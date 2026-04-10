import streamlit as st
import anthropic
import re
import threading
import requests
from datetime import datetime, timezone, timedelta
from supabase import create_client
from tavily import TavilyClient
from exa_py import Exa

# ─── PAGE CONFIG ───────────────────────────────────────────────────────────────
st.set_page_config(page_title="시장 및 종목 내러티브 분석 엔진", page_icon="⚡", layout="wide")

st.markdown("""
<style>
body, .stApp { background: #f8f9fc !important; color: #1a1a2e !important; }
.block-container { padding-top: 1.5rem; max-width: 1100px; }
.stButton > button {
    background: #fff; border: 1.5px solid #d0d5e0;
    color: #2d3a5e; border-radius: 7px; font-size: 13px;
    font-weight: 500; transition: all 0.18s;
}
.stButton > button:hover { border-color: #4a7cf7; color: #4a7cf7; background: #eef2ff; }
h1, h2, h3 { color: #1a1a2e !important; }
.stExpander { background: #fff !important; border: 1.5px solid #e2e6ef !important; border-radius: 8px !important; }
div[data-testid="stExpander"] > div { background: #fff !important; }
div[data-testid="stExpander"] summary { color: #2d3a5e !important; font-weight: 600; }
.stTextInput > div > div > input { background: #fff; border: 1.5px solid #d0d5e0; color: #1a1a2e; border-radius: 7px; }
.stSelectbox > div > div { background: #fff !important; color: #1a1a2e !important; border: 1.5px solid #d0d5e0 !important; border-radius: 7px !important; }
div[data-testid="metric-container"] { background: #fff; border: 1.5px solid #e2e6ef; border-radius: 10px; padding: 14px; box-shadow: 0 1px 4px rgba(0,0,0,0.06); }
div[data-testid="metric-container"] label { color: #6b7a9e !important; }
div[data-testid="metric-container"] div[data-testid="stMetricValue"] { color: #1a1a2e !important; }
.stRadio label { color: #2d3a5e !important; }
.stCaption { color: #8892ab !important; }
div[data-testid="stAlert"] { border-radius: 8px !important; }
.stProgress > div > div { background: #4a7cf7 !important; }
hr { border-color: #e2e6ef !important; }
section[data-testid="stSidebar"] { display: none; }
</style>
""", unsafe_allow_html=True)

# ─── CONSTANTS ─────────────────────────────────────────────────────────────────
CACHE_TTL_HOURS = 48

# ─── 로컬 LLM 설정 ──────────────────────────────────────────────────────────────
# ngrok URL은 Streamlit Secrets에서 관리 (재시작마다 바뀌므로)
# secrets.toml: OLLAMA_URL = "https://xxx.ngrok-free.app"

DEFAULT_OLLAMA_URL = "http://localhost:11434"

# 시장별 기본 모델 (secrets 미설정 시 fallback)
DEFAULT_MODELS = {
    "kospi200":   "qwen2.5:32b",     # 한국어 1티어, VRAM에 완벽히 들어감
    "sp500":      "gemma3:27b",      # (또는 qwen2.5:32b) 영어 추론 능력 최상급
    "nikkei225":  "gemma3:27b",      # 일본어/다국어 능력이 매우 뛰어남
}

def get_ollama_url() -> str:
    return st.secrets.get("OLLAMA_URL", DEFAULT_OLLAMA_URL).rstrip("/")

def get_ollama_model(market_id: str = "sp500") -> str:
    """시장별 최적 LLM 모델 반환"""
    key_map = {
        "kospi200":  "OLLAMA_MODEL_KOSPI",
        "sp500":     "OLLAMA_MODEL_SP500",
        "nikkei225": "OLLAMA_MODEL_NIKKEI",
    }
    secret_key = key_map.get(market_id, "OLLAMA_MODEL_SP500")
    return st.secrets.get(secret_key, DEFAULT_MODELS.get(market_id, "gemma3:27b"))

MARKETS = {
    "🇰🇷 한국 시장": {
        "id": "kospi200",
        "flag": "🇰🇷",
        "color": "#4fc3f7",
        "index": "KOSPI 200",
        "region": "한국",
        "central_bank": "한국은행(BOK)",
        "currency": "원화(KRW)",
        "analysts": "국내외 증권사 애널리스트",
    },
    "🇺🇸 미국 시장": {
        "id": "sp500",
        "flag": "🇺🇸",
        "color": "#00e87a",
        "index": "S&P 500",
        "region": "미국",
        "central_bank": "연준(Fed)",
        "currency": "달러(USD)",
        "analysts": "월스트리트 애널리스트",
    },
    "🇯🇵 일본 시장": {
        "id": "nikkei225",
        "flag": "🇯🇵",
        "color": "#ff7043",
        "index": "닛케이 225",
        "region": "일본",
        "central_bank": "일본은행(BOJ)",
        "currency": "엔화(JPY)",
        "analysts": "일본 및 글로벌 증권사 애널리스트",
    },
}

INDEX_OPTIONS = {
    "kospi200": [
        ("index_kospi200", "KOSPI 200"),
        ("index_kosdaq150", "KOSDAQ 150"),
    ],
    "sp500": [
        ("index_sp500", "S&P 500"),
        ("index_nasdaq100", "NASDAQ 100"),
    ],
    "nikkei225": [
        ("index_nikkei225", "닛케이 225"),
    ],
}

STOCKS = {
    "kospi200": [
        ("005930","삼성전자","반도체"),
        ("000660","SK하이닉스","반도체"),
        ("207940","삼성바이오로직스","바이오"),
        ("005380","현대자동차","자동차"),
        ("000270","기아","자동차"),
        ("051910","LG화학","화학"),
        ("035420","NAVER","인터넷"),
        ("035720","카카오","인터넷"),
        ("068270","셀트리온","바이오"),
        ("105560","KB금융","금융"),
        ("055550","신한지주","금융"),
        ("032830","삼성생명","보험"),
        ("012330","현대모비스","자동차부품"),
        ("003550","LG","지주사"),
        ("066570","LG전자","전자"),
        ("028260","삼성물산","건설"),
        ("096770","SK이노베이션","에너지"),
        ("034730","SK","지주사"),
        ("003490","대한항공","항공"),
        ("009830","한화솔루션","에너지"),
        ("017670","SK텔레콤","통신"),
        ("030200","KT","통신"),
        ("086790","하나금융지주","금융"),
        ("010950","S-Oil","정유"),
        ("015760","한국전력","유틸리티"),
        ("018260","삼성에스디에스","IT서비스"),
        ("011200","HMM","해운"),
        ("259960","크래프톤","게임"),
        ("329180","HD현대중공업","조선"),
        ("042700","한미반도체","반도체장비"),
        ("064350","현대로템","방산/철도"),
        ("034020","두산에너빌리티","에너지설비"),
        ("010140","삼성중공업","조선"),
        ("267260","HD현대일렉트릭","전력기기"),
        ("009540","HD한국조선해양","조선"),
        ("000810","삼성화재","보험"),
        ("316140","우리금융지주","금융"),
        ("024110","기업은행","은행"),
        ("006400","삼성SDI","배터리"),
        ("373220","LG에너지솔루션","배터리"),
        ("251270","넷마블","게임"),
        ("047050","포스코인터내셔널","상사/에너지"),
        ("005490","POSCO홀딩스","철강"),
        ("000100","유한양행","제약"),
        ("196170","알테오젠","바이오"),
        ("145020","휴젤","바이오"),
        ("090430","아모레퍼시픽","화장품"),
        ("035900","JYP Ent.","엔터테인먼트"),
        ("352820","하이브","엔터테인먼트"),
        ("041510","에스엠","엔터테인먼트"),
        ("053800","안랩","소프트웨어/보안"),
        ("112040","위메이드","게임"),
        ("263750","펄어비스","게임"),
        ("161390","한국타이어앤테크놀로지","타이어"),
        ("071050","한국금융지주","증권")
    ],
    "sp500": [
        ("AAPL","Apple","Technology"),
        ("MSFT","Microsoft","Technology"),
        ("NVDA","NVIDIA","Semiconductors"),
        ("AMZN","Amazon","Consumer/Cloud"),
        ("GOOGL","Alphabet","Internet"),
        ("META","Meta","Social Media"),
        ("TSLA","Tesla","EV/Energy"),
        ("BRK.B","Berkshire","Financials"),
        ("JPM","JPMorgan","Banking"),
        ("V","Visa","Payments"),
    
        ("UNH","UnitedHealth","Healthcare"),
        ("XOM","ExxonMobil","Energy"),
        ("JNJ","J&J","Healthcare"),
        ("WMT","Walmart","Retail"),
        ("MA","Mastercard","Payments"),
        ("PG","P&G","Consumer"),
        ("HD","Home Depot","Retail"),
        ("BAC","Bank of America","Banking"),
        ("AVGO","Broadcom","Semiconductors"),
        ("LLY","Eli Lilly","Pharma"),
    
        ("COST","Costco","Retail"),
        ("ABBV","AbbVie","Pharma"),
        ("MRK","Merck","Pharma"),
        ("PEP","PepsiCo","Consumer"),
        ("KO","Coca-Cola","Consumer"),
        ("ADBE","Adobe","Software"),
        ("CRM","Salesforce","Software"),
        ("NFLX","Netflix","Media"),
        ("AMD","AMD","Semiconductors"),
        ("ORCL","Oracle","Software"),
    
        ("TMO","Thermo Fisher","Life Science"),
        ("MCD","McDonald's","Consumer"),
        ("ACN","Accenture","IT Services"),
        ("LIN","Linde","Materials"),
        ("DHR","Danaher","Life Science"),
        ("TXN","Texas Instruments","Semiconductors"),
        ("QCOM","Qualcomm","Semiconductors"),
        ("INTU","Intuit","Software"),
        ("AMGN","Amgen","Biotech"),
        ("GE","GE Aerospace","Aerospace"),
        ("CSCO","Cisco","Networking"),
        ("NKE","Nike","Apparel"),
        ("PM","Philip Morris","Consumer"),
        ("IBM","IBM","Technology"),
        ("INTC","Intel","Semiconductors"),
        ("CAT","Caterpillar","Industrials"),
        ("NOW","ServiceNow","Software"),
        ("GS","Goldman Sachs","Financials"),
        ("PLTR","Palantir","Software"),
        ("UBER","Uber","Platform"),
    
        ("AMAT","Applied Materials","Semiconductor Equipment"),
        ("ETN","Eaton","Power Infrastructure"),
        ("RTX","RTX","Defense/Aerospace"),
        ("BKNG","Booking Holdings","Travel Platform"),
        ("SPGI","S&P Global","Financial Data"),
    ],

    "nikkei225": [
        ("7203","토요타자동차","자동차"),
        ("6758","소니그룹","전자/엔터"),
        ("9984","소프트뱅크그룹","통신/투자"),
        ("8306","미쓰비시UFJ파이낸셜그룹","은행"),
        ("6861","키엔스","전자기기"),
        ("6367","다이킨공업","공조"),
        ("4063","신에츠화학","화학"),
        ("7974","닌텐도","게임"),
        ("6501","히타치제작소","전기/인프라"),
        ("6702","후지쯔","IT"),
        ("8035","도쿄일렉트론","반도체장비"),
        ("7267","혼다","자동차"),
        ("2914","일본담배산업","소비재"),
        ("9432","NTT","통신"),
        ("8411","미즈호파이낸셜그룹","은행"),
        ("4502","다케다약품공업","제약"),
        ("6971","교세라","전자부품"),
        ("7751","캐논","광학/전자"),
        ("6954","화낙","로봇"),
        ("3382","세븐앤아이홀딩스","소매"),
        ("9983","패스트리테일링","의류"),
        ("6098","리크루트홀딩스","인력/플랫폼"),
        ("8766","도쿄해상홀딩스","보험"),
        ("8058","미쓰비시상사","종합상사"),
        ("8001","이토추상사","종합상사"),
        ("8031","미쓰이물산","종합상사"),
        ("8053","스미토모상사","종합상사"),
        ("9433","KDDI","통신"),
        ("9434","소프트뱅크","통신"),
        ("9020","JR동일본","철도"),
        ("9022","JR도카이","철도"),
        ("9021","JR서일본","철도"),
        ("2802","아지노모토","식품"),
        ("4543","데루모","의료기기"),
        ("4519","주가이제약","제약"),
        ("4568","다이이치산쿄","제약"),
        ("6594","니덱","전기모터"),
        ("6723","르네사스일렉트로닉스","반도체"),
        ("7741","HOYA","광학/의료"),
        ("7733","올림푸스","의료기기"),
        ("4901","후지필름홀딩스","헬스케어/소재"),
        ("6503","미쓰비시전기","전기"),
        ("8015","도요타통상","종합상사"),
        ("8801","미쓰이부동산","부동산"),
        ("6857","어드반테스트","반도체장비/테스트"),
        ("6762","TDK","전자부품"),
        ("7201","닛산자동차","자동차"),
        ("7269","스즈키","자동차"),
        ("2502","아사히그룹홀딩스","식음료"),
        ("4452","가오","생활용품")
    ],
}

AGENT_LABELS = {
    "bull":"📈 강세 애널리스트", "neutral":"➡️ 중립 애널리스트", "bear":"📉 약세 애널리스트",
    "bull_critic":"🔥 강세 비판", "neutral_critic":"🔥 중립 비판", "bear_critic":"🔥 약세 비판",
    "judge":"⚡ 최종 판정자",
}

DIRECTIONAL_TERMS = {
    "bull": {
        "sp500": [
            "upside", "outperform", "overweight", "price target raise",
            "earnings upside", "margin expansion", "multiple rerating",
            "AI monetization", "market share gains", "cycle recovery",
            "pricing power", "strong guidance", "demand resilience"
        ],
        "kospi200": [
            "상승 여력", "매수", "목표주가 상향", "실적 개선",
            "이익 성장", "마진 개선", "밸류에이션 재평가",
            "업황 회복", "점유율 확대", "수요 견조", "가이던스 상향"
        ],
        "nikkei225": [
            "上値余地", "買い", "目標株価引き上げ", "業績上振れ",
            "利益率改善", "リレーティング", "需要堅調",
            "シェア拡大", "構造的成長", "ガイダンス上方修正"
        ]
    },
    "neutral": {
        "sp500": [
            "mixed outlook", "fairly valued", "balanced risk reward",
            "range-bound", "uncertainty", "wait and see",
            "limited upside", "macro dependent", "sideways"
        ],
        "kospi200": [
            "중립", "보합", "관망", "불확실성", "박스권",
            "제한적 상승", "밸류 적정", "혼조", "방향성 부재"
        ],
        "nikkei225": [
            "中立", "様子見", "ボックス圏", "不透明感",
            "方向感乏しい", "割安でも割高でもない", "均衡"
        ]
    },
    "bear": {
        "sp500": [
            "downside", "underperform", "sell rating", "price target cut",
            "earnings risk", "margin pressure", "valuation compression",
            "demand slowdown", "inventory correction", "regulatory risk",
            "execution risk", "macro headwinds", "overvalued"
        ],
        "kospi200": [
            "하락 여력", "매도", "목표주가 하향", "실적 부진",
            "마진 압박", "수요 둔화", "밸류 부담", "재고 조정",
            "규제 리스크", "거시 역풍", "과대평가"
        ],
        "nikkei225": [
            "下値余地", "売り", "目標株価引き下げ", "業績下振れ",
            "利益率悪化", "需要減速", "在庫調整", "規制リスク",
            "マクロ逆風", "割高"
        ]
    }
}

# ─── SEARCH CLIENTS ────────────────────────────────────────────────────────────
@st.cache_resource
def get_tavily():
    return TavilyClient(api_key=st.secrets["TAVILY_API_KEY"])

@st.cache_resource
def get_exa():
    return Exa(api_key=st.secrets["EXA_API_KEY"])

EXA_FINANCIAL_DOMAINS = [
    "bloomberg.com", "ft.com", "reuters.com", "wsj.com",
    "seekingalpha.com", "barrons.com", "marketwatch.com",
    "cnbc.com", "economist.com", "morningstar.com",
    "investopedia.com", "fool.com", "zacks.com",
    "hankyung.com", "mk.co.kr", "edaily.co.kr", "thebell.co.kr",
    "fnguide.com", "investing.com", "nikkei.com", "toyokeizai.net",
]



# ─── SNS 도메인·쿼리 (시장별) ────────────────────────────────────────────────
def _get_sns_domains(market_id: str) -> list[str]:
    if market_id == "kospi200":
        return ["finance.naver.com","hankyung.com","mk.co.kr","investing.com","stockplus.com","ppomppu.co.kr","clien.net","fmkorea.com"]
    elif market_id == "nikkei225":
        return ["minkabu.jp","kabutan.jp","finance.yahoo.co.jp","stockvoice.jp"]
    else:
        return ["reddit.com","stocktwits.com","x.com","twitter.com"]

def _build_sns_queries(target: str, direction: str, market_id: str) -> list[str]:
    # ✅ 수정 1: 불필요한 year 파라미터 삭제. SNS 검색어는 최대한 단순화하여 히트율 극대화
    dir_en = "bullish" if direction == "bull" else ("bearish" if direction == "bear" else "outlook")
    
    if market_id == "kospi200":
        return [f"{target} 주식 종토방", f"{target} 매수 매도 여론"]
    elif market_id == "nikkei225":
        return [f"{target} 株 掲示板", f"{target} 個人投資家"]
    else:
        return [f"{target} stock {dir_en} reddit", f"{target} stock retail sentiment"]
        
def _detect_platform(url: str) -> str:
    if "reddit.com" in url: return "Reddit"
    if "stocktwits.com" in url: return "StockTwits"
    if "x.com" in url or "twitter.com" in url: return "X"
    if "naver.com" in url: return "네이버금융"
    if "minkabu" in url: return "みんかぶ"
    if "kabutan" in url: return "株探"
    return "커뮤니티"

def build_stock_lookup():
    """
    STOCKS 딕셔너리를 기반으로
    ticker / name / market_id 정보를 빠르게 찾을 수 있는 lookup 생성
    """
    lookup = {}

    for market_id, items in STOCKS.items():
        for ticker, name, sector in items:
            lookup[ticker.upper()] = {
                "ticker": ticker,
                "name": name,
                "sector": sector,
                "market_id": market_id,
            }
            lookup[name.strip().lower()] = {
                "ticker": ticker,
                "name": name,
                "sector": sector,
                "market_id": market_id,
            }

    return lookup

ENTITY_OVERRIDE = {
    # 정말 필요한 예외만
    "META": {
        "canonical": "Meta Platforms",
        "aliases": ["Meta Platforms", "Meta", "NASDAQ:META", "META"]
    },
    "GOOGL": {
        "canonical": "Alphabet",
        "aliases": ["Alphabet", "Google", "NASDAQ:GOOGL", "GOOGL"]
    },
    "BRK.B": {
        "canonical": "Berkshire Hathaway",
        "aliases": ["Berkshire Hathaway", "NYSE:BRK.B", "BRK.B"]
    },
}

FMP_TICKER_MAP = {
    "005930":"005930.KS","000660":"000660.KS","207940":"207940.KS","005380":"005380.KS",
    "000270":"000270.KS","051910":"051910.KS","035420":"035420.KS","035720":"035720.KS",
    "068270":"068270.KS","105560":"105560.KS","055550":"055550.KS","032830":"032830.KS",
    "012330":"012330.KS","003550":"003550.KS","066570":"066570.KS","028260":"028260.KS",
    "096770":"096770.KS","034730":"034730.KS","003490":"003490.KS","009830":"009830.KS",
    "7203":"7203.T","6758":"6758.T","9984":"9984.T","8306":"8306.T","6861":"6861.T",
    "6367":"6367.T","4063":"4063.T","7974":"7974.T","6501":"6501.T","6702":"6702.T",
    "8035":"8035.T","7267":"7267.T","2914":"2914.T","9432":"9432.T","8411":"8411.T",
    "4502":"4502.T","6971":"6971.T","7751":"7751.T","6954":"6954.T","3382":"3382.T",
}

def normalize_entity(target: str, ticker_raw: str = "", market_id: str = "sp500") -> dict:
    """
    전체 시장 종목에 대해 동적으로 alias 생성.
    일부 특수 기업만 ENTITY_OVERRIDE로 보정.
    """
    target = (target or "").strip()
    ticker_raw = (ticker_raw or "").strip()

    lookup = build_stock_lookup()

    # 1) 우선 ticker_raw 기준 탐색
    stock_info = None
    key_candidates = []

    if ticker_raw:
        key_candidates.append(ticker_raw.upper())
    if target:
        key_candidates.append(target.upper())
        key_candidates.append(target.lower())

    for key in key_candidates:
        if key in lookup:
            stock_info = lookup[key]
            break

    # 2) 지수 처리
    target_l = target.lower()
    if market_id == "sp500" and target_l in ["s&p 500", "sp500", "s&p500"]:
        return {
            "canonical": "S&P 500",
            "aliases": ["S&P 500", "SP500", "SPX", "US large cap equities"]
        }
    if market_id == "kospi200" and target_l in ["kospi 200", "kospi200", "코스피200"]:
        return {
            "canonical": "KOSPI 200",
            "aliases": ["KOSPI 200", "코스피200", "Korean large cap equities"]
        }
    if market_id == "nikkei225" and target_l in ["nikkei 225", "nikkei225", "닛케이225", "日経225"]:
        return {
            "canonical": "Nikkei 225",
            "aliases": ["Nikkei 225", "日経225", "Japanese large cap equities"]
        }

    # 3) 종목 처리
    if stock_info:
        ticker = stock_info["ticker"]
        name = stock_info["name"]
        resolved_market = stock_info["market_id"]

        # 특수 예외 우선
        if ticker.upper() in ENTITY_OVERRIDE:
            base = ENTITY_OVERRIDE[ticker.upper()].copy()
            aliases = list(dict.fromkeys(base["aliases"] + [name, ticker]))
            return {
                "canonical": base["canonical"],
                "aliases": aliases,
                "ticker": ticker,
                "name": name,
                "market_id": resolved_market,
                "sector": stock_info["sector"],
            }

        aliases = [name, ticker]

        # 시장별 거래소 suffix 자동 부여
        if resolved_market == "kospi200":
            aliases.append(f"{ticker}.KS")
        elif resolved_market == "nikkei225":
            aliases.append(f"{ticker}.T")
        elif resolved_market == "sp500":
            aliases.append(f"NASDAQ:{ticker}")
            aliases.append(f"NYSE:{ticker}")

        return {
            "canonical": name,
            "aliases": list(dict.fromkeys([a for a in aliases if a])),
            "ticker": ticker,
            "name": name,
            "market_id": resolved_market,
            "sector": stock_info["sector"],
        }

    # 4) fallback
    aliases = [a for a in [target, ticker_raw] if a]
    return {
        "canonical": target or ticker_raw,
        "aliases": list(dict.fromkeys(aliases)),
        "ticker": ticker_raw,
        "name": target,
        "market_id": market_id,
        "sector": "",
    }
    
# ─── QUERY BUILDER ─────────────────────────────────────────────────────────────
def build_naver_queries(target, ticker):
    return [
        f"{target} 매수 목표주가 상향 강세",
        f"{target} 중립 보합 관망 횡보",
        f"{target} 매도 목표주가 하향 리스크",
        f"{target} 실적 영업이익 매출 전망",
        f"{target} 증권사 주가 전망",
        f"{ticker} {target} 공시 IR"
    ]

def build_queries(target, direction, market_index, sector="", market_id="sp500", ticker_raw=""):
    entity = normalize_entity(target, ticker_raw, market_id)
    canonical = entity["canonical"]
    aliases = entity["aliases"][:4]
    alias_main = canonical
    alias_or = " OR ".join([f'"{a}"' for a in aliases])
    sector_txt = f" {sector}" if sector else ""

    if market_id == "kospi200":
        tavily_queries = [
            f'{alias_or} 최근 뉴스 사업 전략 경쟁 리스크',
            f'{alias_or}{sector_txt} 실적 발표 수익성 수요 마진',
            f'{alias_or} 투자 포인트 우려 요인',
            f'{market_index} 최근 전망 거시 수급 금리 환율'
        ]
        exa_queries = [
            f'{alias_main} investment thesis strategy risk catalyst',
            f'{alias_main} earnings release investor relations profitability',
            f'{alias_main}{sector_txt} competition demand margin capex',
            f'{alias_main} regulatory risk execution narrative'
        ]
        quant_queries = [
            f'{alias_or} 매출 영업이익 영업이익률 순이익 EPS 전망',
            f'{alias_or} 가이던스 CAPEX 수주 backlog 신규수주',
            f'{alias_or} 공시 실적 발표 수치 YoY QoQ',
            f'{alias_or} 사업보고서 분기보고서 주석 수치'
        ]
    elif market_id == "nikkei225":
        tavily_queries = [
            f'{alias_or} 最新ニュース 事業戦略 競争 リスク',
            f'{alias_or}{sector_txt} 決算 収益性 需要 マージン',
            f'{alias_or} 投資ポイント 懸念材料',
            f'{market_index} 見通し 金利 為替 マクロ'
        ]
        exa_queries = [
            f'{alias_main} investment thesis strategy risk catalyst',
            f'{alias_main} earnings release investor relations profitability',
            f'{alias_main}{sector_txt} competition demand margin capex',
            f'{alias_main} regulatory risk execution narrative'
        ]
        quant_queries = [
            f'{alias_or} 売上 営業利益 EPS ガイダンス',
            f'{alias_or} CAPEX 受注 backlog 需要',
            f'{alias_or} 決算短信 数値 YoY QoQ',
            f'{alias_or} 有価証券報告書 注記 数値'
        ]
    else:
        tavily_queries = [
            f'{alias_or} latest news strategy competition risk',
            f'{alias_or}{sector_txt} earnings profitability demand margins',
            f'{alias_or} key debate catalysts concerns',
            f'{market_index} latest outlook macro rates positioning'
        ]
        exa_queries = [
            f'{alias_main} investment thesis strategy risk catalyst',
            f'{alias_main} earnings release investor relations profitability',
            f'{alias_main}{sector_txt} competition demand margin capex',
            f'{alias_main} regulatory risk execution narrative'
        ]
        quant_queries = [
            f'{alias_or} revenue operating margin EPS guidance YoY QoQ',
            f'{alias_or} capex bookings backlog order demand numbers',
            f'{alias_or} annual report 10-Q 10-K financial metrics',
            f'{alias_or} notes to financial statements quantitative disclosure'
        ]

    sns_queries = _build_sns_queries(alias_main, "neutral", market_id)

    return {
        "entity": entity,
        "tavily": tavily_queries,
        "exa_report": exa_queries,
        "exa_sns": sns_queries,
        "quant": quant_queries
    }

def collect_quant_evidence(queries, entity_info=None, market_id="sp500"):
    tavily_items = search_tavily(queries)
    exa_items = search_exa_reports(
        queries,
        entity_info=entity_info,
        recent_days=120,
        market_id=market_id
    )

    merged = []
    seen = set()

    for r in tavily_items + exa_items:
        url = r.get("url", "") or ""
        title = r.get("title", "") or ""
        key = (url, title)
        if key in seen:
            continue
        seen.add(key)

        num_sents = extract_numeric_sentences(r.get("content", "") or "", max_sentences=5)
        if not num_sents:
            continue

        merged.append({
            "title": title,
            "url": url,
            "date": r.get("date", "") or "",
            "numeric_evidence": num_sents,
            "engine": r.get("engine", "mixed")
        })

    return merged

# ─── SEARCH FUNCTIONS ──────────────────────────────────────────────────────────
def search_tavily(queries):
    results = []
    try:
        client = get_tavily()
        seen = set()
        for q in queries:
            try:
                resp = client.search(q, max_results=4, search_depth="advanced", include_answer=False)
                for r in resp.get("results", []):
                    url = r.get("url","")
                    if url in seen: continue
                    seen.add(url)
                    results.append({"title":r.get("title",""),"url":url,"content":r.get("content","")[:700],"date":r.get("published_date","")})
            except Exception as e:
                # 🚨 에러를 기사 내용인 것처럼 위장해서 LLM과 화면에 전달
                results.append({"title": "🚨 Tavily 검색 에러", "url": "", "content": f"상세: {str(e)}", "date": datetime.now().strftime("%Y-%m-%d")})
    except Exception as e:
        results.append({"title": "🚨 Tavily 클라이언트 에러", "url": "", "content": str(e), "date": ""})
    return results



def search_exa_reports(queries, entity_info=None, recent_days=120, market_id="sp500"):
    results = []
    seen = set()

    try:
        client = get_exa()
        start_date = (datetime.now() - timedelta(days=recent_days)).strftime("%Y-%m-%dT00:00:00.000Z")

        domain_map = {
            "sp500": ["sec.gov", "reuters.com", "bloomberg.com", "wsj.com", "seekingalpha.com", "marketwatch.com"],
            "kospi200": ["dart.fss.or.kr", "fnguide.com", "hankyung.com", "mk.co.kr", "edaily.co.kr", "thebell.co.kr"],
            "nikkei225": ["nikkei.com", "kabutan.jp", "minkabu.jp", "toyokeizai.net"]
        }
        include_domains = domain_map.get(market_id, [])

        additional = []
        if entity_info:
            for a in entity_info.get("aliases", [])[:3]:
                additional.extend([
                    f"{a} investment thesis",
                    f"{a} analyst report",
                    f"{a} earnings release"
                ])

        for q in queries:
            try:
                resp = client.search_and_contents(
                    q,
                    type="deep",
                    num_results=8,
                    start_published_date=start_date,
                    include_domains=include_domains if include_domains else None,
                    additional_queries=additional[:6],
                    highlights={"max_characters": 700},
                    text={"max_characters": 1800},
                )

                if not resp or not getattr(resp, "results", None):
                    continue

                for r in resp.results:
                    url = getattr(r, "url", "") or ""
                    if not url or url in seen:
                        continue
                    seen.add(url)

                    highlights = getattr(r, "highlights", None) or []
                    text = getattr(r, "text", "") or ""
                    summary = " … ".join(highlights) if highlights else text[:1000]

                    results.append({
                        "title": getattr(r, "title", "") or "검색 결과",
                        "url": url,
                        "content": summary,
                        "date": getattr(r, "published_date", "") or "",
                        "engine": "exa"
                    })

            except Exception as e:
                results.append({
                    "title": f"🚨 Exa 개별 쿼리 오류",
                    "url": "debug",
                    "content": str(e),
                    "date": datetime.now().strftime("%Y-%m-%d"),
                    "engine": "exa"
                })

    except Exception as e:
        results.append({
            "title": "🚨 Exa 클라이언트 초기화 실패",
            "url": "",
            "content": str(e),
            "date": "",
            "engine": "exa"
        })

    return results


def search_tavily_sns(queries, market_id="sp500"):
    results = []
    try:
        client = get_tavily()
        seen = set()
        domains = _get_sns_domains(market_id)

        for q in queries:
            try:
                resp = client.search(
                    q,
                    max_results=4,
                    search_depth="basic",
                    include_domains=domains
                )
                for r in resp.get("results", []):
                    url = r.get("url") or ""
                    if not url or url in seen:
                        continue
                    seen.add(url)
                    results.append({
                        "title": r.get("title") or "",
                        "url": url,
                        "content": (r.get("content") or "")[:600],
                        "date": r.get("published_date") or "",
                        "platform": _detect_platform(url)
                    })
            except Exception as e:
                results.append({
                    "title": "🚨 Tavily SNS 에러",
                    "url": "",
                    "content": f"상세: {str(e)}",
                    "date": datetime.now().strftime("%Y-%m-%d")
                })
    except Exception as e:
        results.append({
            "title": "🚨 Tavily SNS 클라이언트 에러",
            "url": "",
            "content": str(e),
            "date": ""
        })
    return results

def fetch_current_price(target, ticker_raw, market_id):
    client = get_tavily()
    if market_id == "kospi200":
        queries = [f"{target} 현재 주가", f"{ticker_raw} 주가 추이"]
    elif market_id == "nikkei225":
        queries = [f"{target} 株価 現在", f"{ticker_raw} 株価"]
    else:
        queries = [f"{target} stock current price", f"{ticker_raw} stock price today"]
        
    snippets = []
    for q in queries:
        try:
            resp = client.search(q, max_results=3, search_depth="basic")
            for r in resp.get("results",[]):
                c = (r.get("content") or "")[:300]
                d = (r.get("published_date") or "")[:10]
                if c: snippets.append(f"■ {r.get('title','')} ({d})\n  {r.get('url','')}\n  {c}")
        except Exception as e: 
            snippets.append(f"🚨 현재가 검색 에러: {str(e)}")
            
    if not snippets: return "[현재가 검색 실패]"
    return f"【현재 주가 ({datetime.now().strftime('%Y-%m-%d %H:%M')})】\n" + "\n\n".join(snippets)

# ─── 어닝콜 수집 ─────────────────────────────────────────────────────────────
# (어닝콜 관련 _fetch 함수들은 기존 코드 유지, API 에러가 나면 여기서도 에러를 뱉지만, 이 부분은 검색 엔진과는 별개입니다.)
def fetch_earnings_transcript(ticker_raw, target_name="", market_id="sp500"):
    now = datetime.now()
    yr = now.year
    q_c = (now.month-1)//3+1
    q_p = q_c-1 if q_c>1 else 4
    yr_p = yr if q_c>1 else yr-1
    if market_id == "kospi200":
        return _fetch_earnings_tavily_kr(target_name, ticker_raw, yr, q_c, q_p)
    if market_id == "nikkei225":
        return _fetch_earnings_tavily_jp(target_name, ticker_raw, yr)
    fmp_key = st.secrets.get("FMP_API_KEY","")
    if fmp_key and fmp_key.strip() not in ("","...","여기에_FMP_키"):
        r = _fetch_fmp(_sanitize_fmp_ticker(ticker_raw), fmp_key, yr, yr-1)
        if r: return r
    return _fetch_earnings_tavily_us(target_name, ticker_raw, yr, q_c, q_p)

def _sanitize_fmp_ticker(t): return t.replace(".","‑")

def _fetch_earnings_tavily_kr(name, ticker, yr, q, q_p):
    c = get_tavily()
    qs = [f"{name} {yr}년 {q}분기 실적발표 컨퍼런스콜 경영진 가이던스",
          f"{name} 실적 영업이익 매출 전망 경영진 발언 {yr}"]
    snippets, seen = [], set()
    for query in qs:
        try:
            resp = c.search(query, max_results=4, search_depth="advanced",
                include_domains=["hankyung.com","mk.co.kr","edaily.co.kr","thebell.co.kr","sedaily.com"])
            for r in resp.get("results",[]):
                url=r.get("url","")
                if url in seen: continue
                seen.add(url)
                snippets.append(f"■ {r.get('title','')} ({r.get('published_date','')[:10]})\n  {url}\n  {r.get('content','')[:500]}")
        except: pass
    if not snippets: return f"[{name} 실적발표 뉴스 없음]"
    return f"【{name} 최신 실적발표·컨퍼런스콜】\n\n" + "\n\n".join(snippets[:5])

def _fetch_earnings_tavily_jp(name, ticker, yr):
    c = get_tavily()
    qs = [f"{name} {yr}年 決算発表 業績 経営者コメント", f"{ticker} 決算 売上 ガイダンス {yr}"]
    snippets, seen = [], set()
    for query in qs:
        try:
            resp = c.search(query, max_results=4, search_depth="advanced",
                include_domains=["nikkei.com","minkabu.jp","kabutan.jp","toyokeizai.net"])
            for r in resp.get("results",[]):
                url=r.get("url","")
                if url in seen: continue
                seen.add(url)
                snippets.append(f"■ {r.get('title','')} ({r.get('published_date','')[:10]})\n  {url}\n  {r.get('content','')[:500]}")
        except: pass
    if not snippets: return f"[{name} 決算 뉴스 없음]"
    return f"【{name} 最新決算】\n\n" + "\n\n".join(snippets[:5])

def _fetch_earnings_tavily_us(name, ticker, yr, q, q_p):
    c = get_tavily()
    qs = [f"{name} {ticker} earnings call Q{q_p} {yr} CEO CFO guidance",
          f"{ticker} quarterly earnings management commentary {yr}"]
    snippets, seen = [], set()
    for query in qs:
        try:
            resp = c.search(query, max_results=4, search_depth="advanced",
                include_domains=["seekingalpha.com","fool.com","cnbc.com","reuters.com","bloomberg.com"])
            for r in resp.get("results",[]):
                url=r.get("url","")
                if url in seen: continue
                seen.add(url)
                snippets.append(f"■ {r.get('title','')} ({r.get('published_date','')[:10]})\n  {url}\n  {r.get('content','')[:500]}")
        except: pass
    if not snippets: return f"[{name} 어닝콜 뉴스 없음]"
    return f"【{name} 어닝콜·실적발표】\n\n" + "\n\n".join(snippets[:5])

def _fetch_fmp(ticker, fmp_key, yr, yr_p):
    import urllib.request, json as _j
    found = None
    for y in [yr, yr_p]:
        for q in [4,3,2,1]:
            url = f"https://financialmodelingprep.com/api/v3/earning_call_transcript/{ticker}?quarter={q}&year={y}&apikey={fmp_key}"
            try:
                req = urllib.request.Request(url, headers={"User-Agent":"Mozilla/5.0"})
                with urllib.request.urlopen(req, timeout=8) as r:
                    data = _j.loads(r.read())
                if data and isinstance(data,list) and data[0].get("content"):
                    found=(data[0],q,y); break
            except: continue
        if found: break
    if not found: return None
    row,q,y = found
    text = row.get("content","")
    if len(text)>6000: text=text[:3500]+"\n[중략]\n"+text[-2000:]
    return f"【어닝콜: {ticker} Q{q} {y}】\n(Financial Modeling Prep)\n\n{text}"

def fetch_fmp_financial_snapshot(ticker_raw: str, market_id: str = "sp500") -> str:
    """
    미국 종목에 한해 FMP에서 최근 재무/추정/가이던스 관련 숫자 스냅샷을 가져옴.
    실패하면 None 반환.
    """
    if not ticker_raw or market_id != "sp500":
        return None

    fmp_key = st.secrets.get("FMP_API_KEY", "")
    if not fmp_key or fmp_key.strip() in ("", "...", "여기에_FMP_키"):
        return None

    ticker = FMP_TICKER_MAP.get(ticker_raw, ticker_raw).replace(".", "-")
    headers = {"User-Agent": "Mozilla/5.0"}

    endpoints = {
        "income_statement": f"https://financialmodelingprep.com/api/v3/income-statement/{ticker}?limit=2&apikey={fmp_key}",
        "ratios": f"https://financialmodelingprep.com/api/v3/ratios/{ticker}?limit=2&apikey={fmp_key}",
        "analyst_estimates": f"https://financialmodelingprep.com/api/v3/analyst-estimates/{ticker}?limit=2&apikey={fmp_key}",
    }

    out = []

    for label, url in endpoints.items():
        try:
            r = requests.get(url, headers=headers, timeout=10)
            if r.status_code != 200:
                continue

            data = r.json()
            if not data or not isinstance(data, list):
                continue

            row = data[0]

            if label == "income_statement":
                out.append(
                    f"■ 최근 손익계산서: "
                    f"매출={row.get('revenue')}, "
                    f"영업이익={row.get('operatingIncome')}, "
                    f"순이익={row.get('netIncome')}, "
                    f"EPS={row.get('eps')}, "
                    f"발표일={row.get('date')}"
                )

            elif label == "ratios":
                out.append(
                    f"■ 최근 주요 비율: "
                    f"grossMargin={row.get('grossProfitMargin')}, "
                    f"operatingMargin={row.get('operatingProfitMargin')}, "
                    f"netMargin={row.get('netProfitMargin')}, "
                    f"ROE={row.get('returnOnEquity')}, "
                    f"date={row.get('date')}"
                )

            elif label == "analyst_estimates":
                out.append(
                    f"■ 최근 추정치: "
                    f"estimatedRevenueAvg={row.get('estimatedRevenueAvg')}, "
                    f"estimatedEbitdaAvg={row.get('estimatedEbitdaAvg')}, "
                    f"estimatedEpsAvg={row.get('estimatedEpsAvg')}, "
                    f"date={row.get('date')}"
                )

        except Exception:
            continue

    if not out:
        return None

    return "【FMP 구조화 정량 스냅샷】\n" + "\n".join(out)
        
    
def combined_search(target, direction, market_index, sector="", ticker_raw="", market_id="sp500"):
    qs = build_queries(target, direction, market_index, sector, market_id, ticker_raw=ticker_raw)

    tr = search_tavily(qs["tavily"])
    er = search_exa_reports(qs["exa_report"], entity_info=qs.get("entity"), recent_days=120, market_id=market_id)
    sr = search_tavily_sns(qs["exa_sns"], market_id=market_id)
    qr = collect_quant_evidence(qs["quant"], entity_info=qs.get("entity"), market_id=market_id)
    et = fetch_earnings_transcript(ticker_raw, target_name=target, market_id=market_id) if ticker_raw else "[지수 — 어닝콜 해당 없음]"
    fs = fetch_fmp_financial_snapshot(ticker_raw, market_id=market_id) if ticker_raw else None

    nr = []
    if market_id == "kospi200":
        naver_qs = build_naver_queries(target, ticker_raw)
        nr = search_naver_news(naver_qs)

    cutoff_str = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d")

    def fmt(items, label, show_p=False):
        if not items: return f"【{label}】\n결과 없음\n"
        lines = [f"【{label}】"]
        valid_count = 0
        for r in items:
            date_str = r.get("date") or ""
            if date_str and len(date_str) >= 10 and date_str[:10] < cutoff_str: continue
            valid_count += 1
            ds = f" ({date_str[:10]})" if date_str else ""
            pl = f"[{r.get('platform','')}] " if show_p and r.get("platform") else ""
            lines.append(f"■ {pl}{r.get('title','')}{ds}")
            if r.get("url"): lines.append(f"  {r['url']}")
            if r.get("content"): lines.append(f"  {r['content']}")
            lines.append("")
        if valid_count == 0: return f"【{label}】\n최근 3개월 내 유의미한 결과 없음\n"
        return "\n".join(lines)

    def fmt_quant(items, label):
        if not items: return f"【{label}】\n결과 없음\n"
        lines = [f"【{label}】"]
        valid_count = 0
        for r in items:
            date_str = r.get("date") or ""
            if date_str and len(date_str) >= 10 and date_str[:10] < cutoff_str: continue
            valid_count += 1
            ds = f" ({date_str[:10]})" if date_str else ""
            lines.append(f"■ {r.get('title','')}{ds}")
            if r.get("url"): lines.append(f"  {r['url']}")
            for sent in r.get("numeric_evidence", []): lines.append(f"  - {sent}")
            lines.append("")
        if valid_count == 0: return f"【{label}】\n최근 3개월 내 유의미한 결과 없음\n"
        return "\n".join(lines)

    hdr = (
        f"=== {target} [중립 수집] ({datetime.now().strftime('%Y-%m-%d')}) ===\n"
        f"소스: Tavily + Exa + SNS + 어닝콜 + 정량근거 + 네이버(한국 한정)\n"
        f"주의: 아래 자료는 방향성 유도 없이 수집된 원자료이며, 강세/중립/약세 해석은 이후 에이전트가 수행한다.\n"
    )

    blocks = [
        fmt(tr, "① 최근 뉴스·핵심 논점"),
        fmt(er, "② IR·리포트·공시·장문 자료"),
        fmt(sr, "③ SNS·커뮤니티 반응", show_p=True),
        f"【④ 어닝콜·실적발표】\n{et}"
    ]
    
    if market_id == "kospi200":
        blocks.append(fmt(nr, "⑤ 네이버 금융·뉴스 (강세/중립/약세/실적 종합)"))

    idx_q = "⑥" if market_id == "kospi200" else "⑤"
    idx_f = "⑦" if market_id == "kospi200" else "⑥"

    blocks.extend([
        fmt_quant(qr, f"{idx_q} 내러티브를 지지/반박하는 정량 근거"),
        f"{fs}" if fs else f"【{idx_f} FMP 구조화 정량 스냅샷】\n가용 데이터 없음\n"
    ])

    return hdr + "\n\n".join(blocks)
    
# ─── LLM 호출 (로컬 Ollama 우선, 없으면 Anthropic API fallback) ──────────────
def call_llm(system: str, user_content: str, max_tokens: int = 4000, market_id: str = "sp500") -> str:
    """
    시장별 최적 LLM 자동 선택:
      🇰🇷 KOSPI  → qwen2.5:32b  
      🇺🇸 S&P500 → gemma3:27b  
      🇯🇵 Nikkei → gemma3:27b    
    1순위: 로컬 Ollama / 2순위: Anthropic API fallback
    """
    ollama_url = get_ollama_url()
    model = get_ollama_model(market_id)
    ollama_error_msg = ""  # ✅ 수정 1: 에러를 담아둘 안전한 변수를 만듭니다.

    # ── Ollama 시도 ────────────────────────────────────────────────────────────
    try:
        payload = {
            "model": model,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user",   "content": user_content},
            ],
            "stream": False,
            "options": {"num_predict": max_tokens, "temperature": 0.6},
        }
        headers = {"ngrok-skip-browser-warning": "true", "Content-Type": "application/json"}
        resp = requests.post(f"{ollama_url}/api/chat", json=payload, headers=headers, timeout=300)
        
        # ✅ 수정 2: 404 에러가 나면 Ollama가 보낸 진짜 원인(예: 모델 없음)을 잡아냅니다.
        if resp.status_code != 200:
            raise Exception(f"HTTP {resp.status_code}: {resp.text}")
            
        resp.raise_for_status()
        return resp.json()["message"]["content"]
        
    except Exception as e: 
        # ✅ 수정 3: 파이썬 버그를 피해 에러 내용을 미리 안전한 변수에 복사해 둡니다.
        ollama_error_msg = str(e)
        print(f"[Ollama 실패: {ollama_error_msg}] → Anthropic API로 fallback")

    # ── Anthropic API fallback ─────────────────────────────────────────────────
    api_key = st.secrets.get("ANTHROPIC_API_KEY","") or next(iter(_BG_KEYS.values()), "")
    if not api_key:
        # ✅ 수정 4: 안전하게 복사해둔 에러 메시지를 출력합니다.
        return f"⚠️ LLM 호출 실패: Ollama 연결 불가 + Anthropic API 키 없음\nOllama 상세 오류: {ollama_error_msg}"
        
    try:
        client = anthropic.Anthropic(api_key=api_key)
        resp = client.messages.create(
            model="claude-sonnet-4-5-20250929",
            max_tokens=max_tokens, system=system,
            messages=[{"role":"user","content":user_content}],
        )
        return "".join(b.text for b in resp.content if hasattr(b,"text"))
    except Exception as e:
        return f"⚠️ LLM 오류 (Ollama+Anthropic 모두 실패): {e}"

# ─── SUPABASE ──────────────────────────────────────────────────────────────────
@st.cache_resource
def get_supabase():
    return create_client(st.secrets["SUPABASE_URL"], st.secrets["SUPABASE_KEY"])

_BG_KEYS: dict = {}

def cache_get(target_id):
    try:
        resp = get_supabase().table("analyses").select("*").eq("target_id",target_id).execute()
        if not resp.data: return None
        row = resp.data[0]
        if row.get("status") == "running": return row
        at = datetime.fromisoformat(row["analyzed_at"].replace("Z","")).replace(tzinfo=timezone.utc)
        if (datetime.now(timezone.utc)-at).total_seconds()/3600 > CACHE_TTL_HOURS: return None
        return row
    except: return None

def cache_set(target_id, market_id, target_label, results, winner,
              bull_prob=50, neutral_prob=30, bear_prob=20, status="done"):
    try:
        get_supabase().table("analyses").upsert({
            "target_id":target_id,"market_id":market_id,"target_label":target_label,
            "results":results,"winner":winner,"bull_prob":bull_prob,
            "neutral_prob":neutral_prob,"bear_prob":bear_prob,"status":status,
            "analyzed_at":datetime.now(timezone.utc).isoformat(),
        }, on_conflict="target_id").execute()
    except Exception as e:
        print(f"캐시 저장 오류: {e}")

def cache_set_running(target_id, market_id, target_label):
    try:
        get_supabase().table("analyses").upsert({
            "target_id":target_id,"market_id":market_id,"target_label":target_label,
            "results":{},"winner":"","status":"running","progress":0.0,"status_msg":"분석 준비 중...",
            "analyzed_at":datetime.now(timezone.utc).isoformat(),
        }, on_conflict="target_id").execute()
    except: pass

def update_progress(target_id, pct, msg):
    try:
        get_supabase().table("analyses").update(
            {"progress":float(pct),"status_msg":msg}
        ).eq("target_id",target_id).execute()
    except Exception as e:
        print(f"진행 업데이트 오류: {e}")

def cache_delete(target_id):
    try: get_supabase().table("analyses").delete().eq("target_id",target_id).execute()
    except: pass

def load_leaderboard():
    try:
        resp = get_supabase().table("analyses").select(
            "target_id,market_id,target_label,winner,bull_prob,neutral_prob,bear_prob,status,analyzed_at"
        ).execute()
        rows = []
        for r in resp.data:
            if r.get("status") == "running":
                rows.append({**r,"age_hours":0})
                continue
            at = datetime.fromisoformat(r["analyzed_at"].replace("Z","")).replace(tzinfo=timezone.utc)
            age_h = (datetime.now(timezone.utc)-at).total_seconds()/3600
            if age_h <= CACHE_TTL_HOURS:
                rows.append({**r,"age_hours":round(age_h,1)})
        rows.sort(key=lambda r:(-(r.get("bull_prob") or 0),(r.get("bear_prob") or 0)))
        return rows
    except: return []

def strip_duplicate_translation(text: str) -> str:
    markers = ["번역 (한국어):", "번역:", "Translation:", "Translated version:"]
    for m in markers:
        if m in text:
            parts = text.split(m)
            # 보통 뒤쪽이 한국어 최종본일 가능성이 큼
            return parts[-1].strip()
    return text.strip()
    
# ─── PROMPT BUILDERS ───────────────────────────────────────────────────────────
# ─── PROMPT BUILDERS ───────────────────────────────────────────────────────────
def build_system_prompts(market, stock=None):
    idx = market["index"]
    cb  = market["central_bank"]
    target = f"{stock[1]} ({stock[0]})" if stock else idx
    sn = f" (섹터: {stock[2]}, {idx} 상장)" if stock else ""

    # ✅ 3개월(90일) 커트라인 날짜 계산
    cutoff_date = (datetime.now() - timedelta(days=90))
    cutoff_str_ko = cutoff_date.strftime("%Y년 %m월 %d일")
    cutoff_str_en = cutoff_date.strftime("%B %d, %Y")
    cutoff_str_jp = cutoff_date.strftime("%Y年%m月%d日")

    if market["id"] == "sp500":
        lang_instruction = (
            f"You are a top-tier Wall Street investment analyst. "
            f"You must deeply analyze the provided English financial reports, Reddit/StockTwits sentiment, and earnings calls in English to capture the exact market nuances. "
            f"🚨 CRITICAL RULE: Strictly ignore any data, target prices, ratings, or news older than 3 months (before {cutoff_str_en}). Only use the most recent information. "
            f"Final answer must be written ONLY in Korean. "
            f"Do NOT output English first. "
            f"Do NOT include a separate translation section. "
            f"Do NOT repeat the same content in multiple languages."
        )
    elif market["id"] == "nikkei225":
        lang_instruction = (
            f"あなたは日本市場の専門アナリストです。"
            f"提供された日本語のニュース、みんかぶや株探の掲示板の意見、決算情報を日本語のまま深く分析し、日本市場特有の細かいニュアンスを正確に把握してください。"
            f"🚨 重要ルール: 3ヶ月前（{cutoff_str_jp}以前）の古いデータ、目標株価、ニュースは絶対に無視し、直近の情報のみを使用してください。"
            f"ただし、**最終的な出力はすべて韓国語（한국어）で翻訳して作成**し、以下の韓国語の見出しに必ず従ってください。"
        )
    else:  # kospi200
        lang_instruction = (
            f"당신은 여의도 최고의 한국 시장 전문 애널리스트입니다. "
            f"주어진 한국어 뉴스, 네이버 종토방 여론, 실적발표 자료를 바탕으로 시장의 숨겨진 의도와 방향성을 깊이 있게 분석하십시오. "
            f"🚨 절대 규칙: 3개월 전({cutoff_str_ko}) 기준 과거의 낡은 데이터, 목표가, 투자의견은 절대 무시하고 오직 최신 정보만 인용하십시오. "
            f"**모든 출력은 한국어**로 작성하십시오."
        )

    base_warn = f"\n⚠️ 절대 금지: 검색 결과에 없는 수치 조작 금지. {cutoff_str_ko} 이전 데이터(오래된 목표가)는 철저히 배제할 것."

    return {
"bull": f"""{lang_instruction}
## 📈 {target} 강세 내러티브 수집 (향후 3개월)
### 강세 내러티브의 핵심 주장
### 그 주장을 떠받치는 인과 구조 [왜 이 기업/지수가 좋아질 것인가]
### 핵심 근거 [논리와 메커니즘 중심]
### 정량 근거 점검
[재무제표 수치, 주석 수치, 가이던스 수치, 수주/계약/백로그 수치 중 강세 내러티브를 지지하는 숫자를 제시하시오.
반드시 구체 숫자를 포함하고, 숫자가 부족하면 “정량 근거 부족”이라고 명시하시오.]
### 반대 증거에 대한 강세 측의 반론
### SNS·커뮤니티에서 확인되는 정서
### 어닝콜 핵심 포인트 [강세 해석 가능 구절]
### 강세 내러티브의 전제 조건
### 강세 내러티브 강도 평가 [1-10점]
### 내러티브-정량 일치도 평가 [1-10점]
### 강세 내러티브 3줄 요약
중요: 목표가·투자의견 자체를 평가의 중심에 두지 말고, 내러티브의 설득력과 그것을 뒷받침하는 숫자의 일치 여부를 함께 평가하시오.
출처(기관명, 날짜, URL)를 반드시 명시하시오.{base_warn}""",

"neutral": f"""{lang_instruction}
## ➡️ {target} 중립 내러티브 수집 (향후 3개월)
### 중립 내러티브의 핵심 주장
### 그 주장을 떠받치는 인과 구조
### 핵심 근거 [논리와 메커니즘 중심]
### 정량 근거 점검
[재무제표 수치, 주석 수치, 가이던스 수치, 수주/계약/백로그 수치 중 강세 내러티브를 지지하는 숫자를 제시하시오.
반드시 구체 숫자를 포함하고, 숫자가 부족하면 “정량 근거 부족”이라고 명시하시오.]
### 강세·약세 양측이 모두 놓치고 있는 점
### SNS·커뮤니티에서 확인되는 정서
### 어닝콜 핵심 포인트 [불확실성·균형 신호]
### 중립 내러티브의 전제 조건
### 중립 내러티브 강도 평가 [1-10점]
### 내러티브-정량 일치도 평가 [1-10점]
### 중립 내러티브 3줄 요약
중요: 목표가·투자의견 자체를 평가의 중심에 두지 말고, 내러티브의 설득력과 그것을 뒷받침하는 숫자의 일치 여부를 함께 평가하시오.
출처(기관명, 날짜, URL)를 반드시 명시하시오.{base_warn}""",

"bear": f"""{lang_instruction}
## 📉 {target} 약세 내러티브 수집 (향후 3개월)
### 약세 내러티브의 핵심 주장
### 그 주장을 떠받치는 인과 구조 [왜 악화될 것인가]
### 핵심 근거 [논리와 메커니즘 중심]
### 정량 근거 점검
[재무제표 수치, 주석 수치, 가이던스 수치, 수주/계약/백로그 수치 중 강세 내러티브를 지지하는 숫자를 제시하시오.
반드시 구체 숫자를 포함하고, 숫자가 부족하면 “정량 근거 부족”이라고 명시하시오.]
### 반대 증거에 대한 약세 측의 반론
### SNS·커뮤니티에서 확인되는 정서
### 어닝콜 핵심 포인트 [리스크·악화 시그널]
### 약세 내러티브의 전제 조건
### 약세 내러티브 강도 평가 [1-10점]
### 내러티브-정량 일치도 평가 [1-10점]
### 약세 내러티브 3줄 요약
중요: 목표가·투자의견 자체를 평가의 중심에 두지 말고, 내러티브의 설득력과 그것을 뒷받침하는 숫자의 일치 여부를 함께 평가하시오.
출처(기관명, 날짜, URL)를 반드시 명시하시오.{base_warn}""",

"bull_critic": f"""{lang_instruction}
You are an adversarial analyst stress-testing bullish narratives.
Output only one final Korean version.
Do not include English source text.
Do not add '번역' or 'Translation' sections.
## 🔥 강세 내러티브 비판
### 서사의 약한 고리 [인과 연결의 약함]
### 정량 근거의 취약점
### 강세가 무시한 반대 증거
### 강세 논리의 비약 또는 과장
### 향후 3개월 내 강세 서사가 무너질 조건
### 강세 내러티브 신뢰도 [1-10점 및 2줄 평가]
중요: 목표가나 투자의견 수준이 아니라 내러티브의 설득력과 숫자 근거의 정합성을 함께 비판하시오.""",

        "neutral_critic": f"""{lang_instruction}
You are an adversarial analyst stress-testing neutral narratives.
Output only one final Korean version.
Do not include English source text.
Do not add '번역' or 'Translation' sections.
## 🔥 중립 내러티브 비판
### 서사의 약한 고리 [인과 연결의 약함]
### 정량 근거의 취약점
### 중립이 무시한 반대 증거
### 중립 논리의 비약 또는 과장
### 향후 3개월 내 중립 서사가 무너질 조건
### 중립 내러티브 신뢰도 [1-10점 및 2줄 평가]
중요: 목표가나 투자의견 수준이 아니라 내러티브의 설득력과 숫자 근거의 정합성을 함께 비판하시오.""",

        "bear_critic": f"""{lang_instruction}
You are an adversarial analyst stress-testing bearish narratives.
Output only one final Korean version.
Do not include English source text.
Do not add '번역' or 'Translation' sections.
## 🔥 약세 내러티브 비판
### 서사의 약한 고리 [인과 연결의 약함]
### 정량 근거의 취약점
### 약세가 무시한 반대 증거
### 약세 논리의 비약 또는 과장
### 향후 3개월 내 약세 서사가 무너질 조건
### 약세 내러티브 신뢰도 [1-10점 및 2줄 평가]
중요: 목표가나 투자의견 수준이 아니라 내러티브의 설득력과 숫자 근거의 정합성을 함께 비판하시오.""",

"judge": f"""{lang_instruction}
You are the Chief Investment Strategist reviewing a 6-agent debate.
Output only one final Korean version.
Do not include English source text.
Do not add '번역' or 'Translation' sections.

- 내러티브가 설득력 있어도 정량 근거가 빈약하면 신뢰도를 낮출 것
- 정량 지표가 좋아도 그것이 서사와 연결되지 않으면 높은 점수를 주지 말 것
- 가장 높은 평가는 서사와 숫자가 동일한 방향으로 정합적으로 결합될 때만 부여할 것

⚠️ 절대 금지:
- 목표가 자체를 근거의 중심으로 삼지 말 것
- 【실시간 현재가】를 참고 정보 이상으로 과대평가하지 말 것
- {cutoff_str_ko} 이전의 과거 자료 채택 금지
- 목표가와 투자의견은 보조 참고사항일 뿐, 판정의 주된 근거로 사용하지 말 것

## 핵심 요약
[정확히 4문장. 1:가장 강한 내러티브. 2:그 서사의 인과 구조. 3:가장 취약한 경쟁 내러티브. 4:판단을 뒤집을 변수.]

## ⚡ 최종 판정

### 가장 그럴듯한 내러티브: [강세 / 중립 / 약세]

### 내러티브 강도 평가
- 강세 내러티브 강도: [1-10]
- 중립 내러티브 강도: [1-10]
- 약세 내러티브 강도: [1-10]

### 정량 뒷받침 강도 평가
- 강세 내러티브의 정량 뒷받침 강도: [1-10]
- 중립 내러티브의 정량 뒷받침 강도: [1-10]
- 약세 내러티브의 정량 뒷받침 강도: [1-10]

### 숫자가 실제로 지지하는 방향
[재무제표 수치, 가이던스, 수주/백로그, 마진, EPS, CAPEX 등 수치가
실제로 어느 내러티브를 가장 강하게 지지하는지 간결히 판단]

### 선택 이유
[왜 가장 설득력 있는지. 서사의 구조적 완결성, 인과 논리, 반증 대응력뿐 아니라
그 서사가 실제 정량 지표로 얼마나 뒷받침되는지를 함께 서술]

### 현재 가격 기준 상황
[보조 정보로 짧게 요약]

### 최신 핵심 근거
**근거 1:** [출처 + 사실]
**근거 2:** [출처 + 사실]
**근거 3:** [출처 + 사실]
**근거 4:** [출처 + 사실]
**근거 5:** [출처 + 사실]

### 경쟁 내러티브 탈락 이유
[왜 다른 내러티브들이 덜 설득력 있었는지 간결하게 서술]

### 해당 내러티브를 지지한 애널리스트들의 평균 TP (참고용)
[최종 판정이 강세이면 강세를 지지한 애널리스트들의 TP 평균,
중립이면 중립 진영의 TP 평균,
약세이면 약세 진영의 TP 평균을 제시하시오.
TP가 확인된 경우에만 계산하고, 표본 수(n)도 함께 적을 것.
목표가의 절대 수준보다, 현재가 대비 평균 implied upside/downside를 함께 적을 것.
예: 평균 TP 742달러 (n=5), 현재가 대비 +11.8%.
확인 가능한 TP가 부족하면 “확인 가능한 TP 부족”이라고만 적을 것.]

### 확률 분포
**강세장 (유의미한 상승): XX%**
**보합장 (박스권): XX%**
**약세장 (유의미한 하락): XX%**

### 핵심 변수 (상위 3개)
[향후 3개월 내 판정을 바꿀 수 있는 변수만 간결히 제시]""",
    }

# ─── HELPERS ───────────────────────────────────────────────────────────────────
def extract_winner(text):
    m = re.search(r"가장 그럴듯한 내러티브[^:：\n]*[：:]\s*\[?([^\]\n]+)\]?", text)
    if not m: return None
    raw = m.group(1).strip()
    if "강세" in raw and "약세" not in raw: return "bull"
    if "약세" in raw and "강세" not in raw: return "bear"
    if "중립" in raw or "보합" in raw: return "neutral"
    return None

def winner_from_probs(b, n, r):
    probs = {"bull":b or 0,"neutral":n or 0,"bear":r or 0}
    return max(probs, key=probs.get)

def extract_probs(text):
    b = re.search(r"강세장[^:\n*]*[:\*]+\s*(\d+)%", text)
    n = re.search(r"보합장[^:\n*]*[:\*]+\s*(\d+)%", text)
    r = re.search(r"약세장[^:\n*]*[:\*]+\s*(\d+)%", text)
    if b and n and r: return int(b.group(1)),int(n.group(1)),int(r.group(1))
    return None,None,None

def winner_badge(w):
    return {"bull":"📈 강세","neutral":"➡️ 중립","bear":"📉 약세"}.get(w,"❓")

def age_label(hours):
    if hours < 1: return "방금"
    if hours < 24: return f"{int(hours)}시간 전"
    return f"{int(hours/24)}일 전"

# ─── CORE ANALYSIS (백그라운드 실행 가능) ─────────────────────────────────────
def _run_analysis_core(target_id, target_label, market, stock, prompts):
    results = {}
    target_short = stock[1] if stock else market["index"]
    sector = stock[2] if stock else ""
    ticker_raw = stock[0] if stock else ""

    # ── Phase 0: 공통 증거 수집 ───────────────────────────────────────────────
    update_progress(target_id, 0.05, "🔍 공통 증거 수집 중...")
    try:
        shared_search_results = combined_search(
            target_short,
            "neutral",
            market["index"],
            sector=sector,
            ticker_raw=ticker_raw,
            market_id=market["id"],
        )
    except Exception as e:
        shared_search_results = f"⚠️ 공통 검색 오류: {e}"

    # ── Phase 1: 동일 증거 기반 내러티브 수집 ────────────────────────────────
    agents_p1 = [("bull", "강세"), ("neutral", "중립"), ("bear", "약세")]
    for i, (agent, dir_label) in enumerate(agents_p1):
        pct = 0.12 + i * 0.11
        update_progress(target_id, pct, f"🤖 {AGENT_LABELS[agent]} — 내러티브 구성 중...")
        try:
            user_content = (
                f"다음은 오늘({datetime.now().strftime('%Y년 %m월 %d일')}) 기준 "
                f"{target_label}에 관한 공통 원자료입니다.\n\n"
                f"{shared_search_results}\n\n"
                f"이 자료만 바탕으로 {dir_label} 내러티브를 가장 설득력 있게 구성하십시오.\n"
                f"중요: 목표가나 투자의견 자체보다, 왜 그런 해석이 가능한지의 서사적 구조와 인과 논리에 집중하십시오."
            )
            raw = call_llm(prompts[agent], user_content, market_id=market["id"])
            results[agent] = strip_duplicate_translation(raw)
        except Exception as e:
            results[agent] = f"⚠️ 오류: {e}"

    # ── Phase 2: 비판 ────────────────────────────────────────────────────────
    update_progress(target_id, 0.45, "Phase 2 · 비판 검증 시작.")
    critic_map = {"bull_critic":("bull","강세"),"neutral_critic":("neutral","중립"),"bear_critic":("bear","약세")}
    for i,agent in enumerate(["bull_critic","neutral_critic","bear_critic"]):
        src, label = critic_map[agent]
        pct = 0.45 + i*0.12
        update_progress(target_id, pct, f"🔥 {AGENT_LABELS[agent]} — 비판 중.")
        try:
            user_content = (
                f"[{label} 내러티브]:\n{results.get(src,'')}\n\n"
                f"위 내러티브를 냉정하게 비판하시오.\n"
                f"중요: 목표가 수준이 아니라 서사의 빈약함, 인과 연결의 약함, 누락된 반대 증거를 중심으로 비판하시오."
            )
            raw = call_llm(prompts[agent], user_content, market_id=market["id"])
            results[agent] = strip_duplicate_translation(raw)
        except Exception as e:
            results[agent] = f"⚠️ 오류: {e}"

    # ── Phase 3: 최종 판정 ───────────────────────────────────────────────────
    update_progress(target_id, 0.82, "📡 현재 주가 조회 + 최종 판정 중.")
    try:
        price_ctx = fetch_current_price(target_short, ticker_raw, market["id"])
        judge_input = (
            f"【실시간 현재가 — 참고 정보】\n{price_ctx}\n\n"
            f"{'='*50}\n\n"
            + "\n\n".join(
                f"[{AGENT_LABELS[a]}]:\n{results.get(a,'')}"
                for a in ["bull","neutral","bear","bull_critic","neutral_critic","bear_critic"]
            )
            + "\n\n최종 판정을 내리시오. 단, 현재가는 보조 정보일 뿐이며, 판단의 중심은 내러티브의 설득력·인과 구조·반증 대응력이어야 한다."
        )
        raw = call_llm(prompts["judge"], judge_input, max_tokens=8000, market_id=market["id"])
        results["judge"] = strip_duplicate_translation(raw)
    except Exception as e:
        results["judge"] = f"⚠️ 오류: {e}"

    update_progress(target_id, 0.98, "✅ 저장 중...")
    bp, np_, rp = extract_probs(results.get("judge",""))
    bp = bp or 50
    np_ = np_ or 30
    rp = rp or 20
    winner = winner_from_probs(bp, np_, rp)
    if bp == 50 and np_ == 30 and rp == 20:
        winner = extract_winner(results.get("judge","")) or "neutral"

    cache_set(target_id, market["id"], target_label, results, winner,
              bull_prob=bp, neutral_prob=np_, bear_prob=rp, status="done")
    update_progress(target_id, 1.0, "✅ 분석 완료!")
    return results, winner

# ─── DISPLAY ───────────────────────────────────────────────────────────────────
def display_results(results, winner, cached_at=None):
    if cached_at:
        at = datetime.fromisoformat(cached_at.replace("Z","")).replace(tzinfo=timezone.utc)
        age_h = (datetime.now(timezone.utc)-at).total_seconds()/3600
        st.info(f"🗄 캐시 결과 · {at.strftime('%Y-%m-%d %H:%M')} UTC · {CACHE_TTL_HOURS-age_h:.0f}시간 후 만료")

    w_map={"bull":("📈 강세","#00e87a"),"neutral":("➡️ 중립","#f5c518"),"bear":("📉 약세","#ff3c4e")}
    w_label,w_color=w_map.get(winner,("❓","#888"))
    st.markdown(f"""<div style='text-align:center;padding:16px;
    background:linear-gradient(135deg,{w_color}18,transparent);
    border:2px solid {w_color}66;border-radius:10px;margin:12px 0'>
    <div style='color:#6b7a9e;font-size:11px;letter-spacing:2px;margin-bottom:6px'>가장 그럴듯한 내러티브</div>
    <div style='color:{w_color};font-size:24px;font-weight:900'>{w_label}</div>
    </div>""", unsafe_allow_html=True)

    bp,np_,rp = extract_probs(results.get("judge",""))
    if bp is not None:
        st.markdown("#### 확률 분포")
        c1,c2,c3=st.columns(3)
        c1.metric("📈 강세장",f"{bp}%"); c1.progress(bp/100)
        c2.metric("➡️ 보합장",f"{np_}%"); c2.progress(np_/100)
        c3.metric("📉 약세장",f"{rp}%"); c3.progress(rp/100)

    st.markdown("---")
    st.markdown("### Phase 1 · 내러티브 수집")
    for a in ["bull","neutral","bear"]:
        with st.expander(AGENT_LABELS[a]): st.markdown(results.get(a,"결과 없음"))
    st.markdown("### Phase 2 · 비판 검증")
    for a in ["bull_critic","neutral_critic","bear_critic"]:
        with st.expander(AGENT_LABELS[a]): st.markdown(results.get(a,"결과 없음"))
    st.markdown("### Phase 3 · 최종 판정")
    with st.expander("⚡ 최종 판정자 전문", expanded=True):
        st.markdown(results.get("judge","결과 없음"))

def display_leaderboard():
    rows = load_leaderboard()

    done_rows = [r for r in rows if r.get("status") != "running"]
    running_rows = [r for r in rows if r.get("status") == "running"]

    st.markdown("### 📊 분석 현황")
    c1, c2, c3 = st.columns(3)
    c1.metric("완료된 분석", f"{len(done_rows)}개")
    c2.metric("진행 중", f"{len(running_rows)}개")
    c3.metric("전체 캐시", f"{len(rows)}개")

    if not rows:
        st.caption("아직 분석 없음.")
        return

    with st.expander("랭킹 보기 / 접기", expanded=False):
        st.markdown("#### 추천 강도 랭킹 (48시간 내 · 강세 확률 높은 순)")

        mf = {"kospi200":"🇰🇷","sp500":"🇺🇸","nikkei225":"🇯🇵"}
        h1,h2,h3,h4,h5,h6 = st.columns([0.4,0.3,2.2,1.0,2.5,0.8])

        for h,t in zip([h1,h2,h3,h4,h5,h6], ["순위","시장","종목/지수","판정","확률 분포","분석"]):
            h.markdown(f"<span style='color:#4a5568;font-size:11px'>{t}</span>", unsafe_allow_html=True)

        st.markdown("<hr style='margin:4px 0;border-color:#e2e6ef'>", unsafe_allow_html=True)

        for rank, row in enumerate(rows, 1):
            bp = row.get("bull_prob") or 0
            np_ = row.get("neutral_prob") or 0
            rp = row.get("bear_prob") or 0
            w = row.get("winner", "")
            flag = mf.get(row.get("market_id", ""), "")
            is_running = row.get("status") == "running"
            rank_color = "#00e87a" if bp >= 55 else "#f5c518" if bp >= 45 else "#ff3c4e"

            c1,c2,c3,c4,c5,c6 = st.columns([0.4,0.3,2.2,1.0,2.5,0.8])
            c1.markdown(f"<div style='color:{rank_color};font-weight:900;font-size:14px;padding-top:4px'>#{rank}</div>", unsafe_allow_html=True)
            c2.markdown(f"<div style='font-size:18px;padding-top:2px'>{flag}</div>", unsafe_allow_html=True)
            c3.markdown(f"<div style='color:#4a5568;font-size:13px;padding-top:4px'>{row['target_label']}</div>", unsafe_allow_html=True)

            if is_running:
                c4.markdown("🔄 분석중")
                c5.markdown("<div style='color:#6b7a9e;font-size:11px;padding-top:6px'>진행 중...</div>", unsafe_allow_html=True)
            else:
                c4.markdown(winner_badge(w))
                bar = f"""
                <div style='display:flex;gap:2px;align-items:center;margin-top:6px'>
                    <div style='width:{bp}%;height:8px;background:#00e87a;border-radius:2px 0 0 2px'></div>
                    <div style='width:{np_}%;height:8px;background:#f5c518'></div>
                    <div style='width:{rp}%;height:8px;background:#ff3c4e;border-radius:0 2px 2px 0'></div>
                </div>
                <div style='display:flex;gap:8px;font-size:9px;color:#6b7a9e;margin-top:2px'>
                    <span style='color:#00e87a'>↑{bp}%</span>
                    <span style='color:#f5c518'>→{np_}%</span>
                    <span style='color:#ff3c4e'>↓{rp}%</span>
                </div>
                """
                c5.markdown(bar, unsafe_allow_html=True)

            c6.markdown(
                f"<div style='color:#374151;font-size:10px;padding-top:6px'>{'진행중' if is_running else age_label(row['age_hours'])}</div>",
                unsafe_allow_html=True
            )
            st.markdown("<hr style='margin:2px 0;border-color:#f0f0f0'>", unsafe_allow_html=True)

# ─── MAIN ──────────────────────────────────────────────────────────────────────
def main():
   
    col_title, col_info = st.columns([5,1])
    with col_title:
        qw = get_ollama_model('kospi200')
        ll = get_ollama_model('sp500')
        gm = get_ollama_model('nikkei225')
        st.markdown(f"""<h1 style='background:linear-gradient(90deg,#4fc3f7,#00e87a,#f5c518,#ff3c4e,#e040fb);
        -webkit-background-clip:text;-webkit-text-fill-color:transparent;font-size:26px;margin:0'>
        ⚡ [시장/종목] 내러티브 앤 넘버스 수집 및 분석</h1>
        <p style='color:#4a5568;font-size:11px;letter-spacing:2px;margin:2px 0 0'>
        7-AGENT AI · 향후 3개월 판정 · 🇰🇷{qw} / 🇺🇸{ll} / 🇯🇵{gm}</p>""", unsafe_allow_html=True)
    with col_info:
        ollama_url = get_ollama_url()
        st.markdown(f"<div style='color:#374151;font-size:10px;text-align:right;margin-top:8px'>🖥 {ollama_url[:30]}...</div>",unsafe_allow_html=True)
        if st.button("새로고침", use_container_width=True):
            st.session_state.clear(); st.rerun()

    st.markdown("---")
    display_leaderboard()
    st.markdown("---")

    st.markdown("### STEP 1 · 시장 선택")
    mc = st.radio("",list(MARKETS.keys()),horizontal=True,label_visibility="collapsed")
    market = MARKETS[mc]

    st.markdown("### STEP 2 · 분석 대상")
    
    stocks = STOCKS[market["id"]]
    indices = INDEX_OPTIONS.get(market["id"], [])
    
    index_options = [f"📊 {label} Index" for _, label in indices]
    stock_options = [f"{n} · {t} ({s})" for t, n, s in stocks]
    options = index_options + stock_options
    
    choice = st.selectbox(
        f"선택 가능한 대상: 지수 {len(indices)}개 + 종목 {len(stocks)}개",
        options,
        label_visibility="collapsed"
    )
    
    selected_market = market.copy()
    
    if choice in index_options:
        idx = index_options.index(choice)
        index_code, index_label = indices[idx]
    
        stock = None
        target_id = f"{market['id']}_{index_code}"
        target_label = f"{market['flag']} {index_label}"
        selected_market["index"] = index_label
    
    else:
        idx = stock_options.index(choice)
        stock = stocks[idx]
        target_id = f"{market['id']}_{stock[0]}"
        target_label = f"{stock[1]} ({stock[0]})"

    st.markdown(f"**선택:** {target_label}")
    cached = cache_get(target_id)
    col_a, col_b = st.columns([3,1])

    if cached:
        status = cached.get("status", "done")
    
        if status == "running":
            at = datetime.fromisoformat(cached["analyzed_at"].replace("Z","")).replace(tzinfo=timezone.utc)
            elapsed = int((datetime.now(timezone.utc)-at).total_seconds()/60)
            pct = float(cached.get("progress") or 0.0)
            msg = cached.get("status_msg") or "분석 준비 중..."
    
            st.info(f"⏳ **{target_label}** 백그라운드 분석 중 ({elapsed}분 경과) — 브라우저 꺼도 계속 진행됩니다")
            st.progress(pct, text=msg)
    
            cr, cc = st.columns([1,1])
            with cr:
                if st.button("🔄 새로고침", use_container_width=True):
                    st.rerun()
            with cc:
                if elapsed > 30 and st.button("⚠️ 재시작", use_container_width=True):
                    cache_delete(target_id)
                    st.rerun()
    
            import time
            time.sleep(3)
            st.rerun()
    
        else:
            # ✅ 완료된 결과가 있고, 아직 화면에 안 띄운 상태면 자동 표시
            if (
                cached.get("results")
                and st.session_state.get("loaded_target_id") != target_id
            ):
                bp = cached.get("bull_prob") or 50
                np_ = cached.get("neutral_prob") or 30
                rp = cached.get("bear_prob") or 20
    
                st.session_state.update({
                    "res_results": cached["results"],
                    "res_winner": winner_from_probs(bp, np_, rp),
                    "res_cached_at": cached["analyzed_at"],
                    "show_results": True,
                    "loaded_target_id": target_id,
                })
                st.rerun()
    
            at = datetime.fromisoformat(cached["analyzed_at"].replace("Z","")).replace(tzinfo=timezone.utc)
            age_h = (datetime.now(timezone.utc)-at).total_seconds()/3600
            remaining = CACHE_TTL_HOURS-age_h
    
            with col_a:
                if st.button(f"🗄 캐시 불러오기 ({remaining:.0f}시간 남음, 0 토큰)", type="primary", use_container_width=True):
                    bp = cached.get("bull_prob") or 50
                    np_ = cached.get("neutral_prob") or 30
                    rp = cached.get("bear_prob") or 20
                    st.session_state.update({
                        "res_results": cached["results"],
                        "res_winner": winner_from_probs(bp, np_, rp),
                        "res_cached_at": cached["analyzed_at"],
                        "show_results": True,
                        "loaded_target_id": target_id,
                    })
                    st.rerun()
    
            with col_b:
                if st.button("🗑 재분석", use_container_width=True):
                    cache_delete(target_id)
                    st.session_state.pop("show_results", None)
                    st.session_state.pop("loaded_target_id", None)
                    st.success("캐시 삭제됨.")
                    st.rerun()
    else:
        with col_a:
            if st.button(f"▶ {target_label} 분석 시작", type="primary", use_container_width=True):
                st.session_state.pop("show_results",None)
                st.session_state.pop("loaded_target_id", None)
                prompts = build_system_prompts(selected_market, stock)
                cache_set_running(target_id, selected_market["id"], target_label)

                def _bg_task():
                    _BG_KEYS[target_id] = ""
                    try:
                        _run_analysis_core(target_id, target_label, selected_market, stock, prompts)
                    except Exception as e:
                        print(f"백그라운드 오류 [{target_id}]: {e}")
                        try:
                            cache_set(target_id, selected_market["id"], target_label, {}, "unknown",
                                      status="done")
                        except: pass
                    finally:
                        _BG_KEYS.pop(target_id, None)

                threading.Thread(target=_bg_task, daemon=True).start()
                st.rerun()

    if st.session_state.get("show_results"):
        st.markdown("---")
        display_results(st.session_state["res_results"], st.session_state["res_winner"],
                        st.session_state.get("res_cached_at"))

    st.markdown("---")
    st.caption("AI 생성 콘텐츠 · 투자 조언 아님 · 연구 목적 전용")

if __name__ == "__main__":
    main()
