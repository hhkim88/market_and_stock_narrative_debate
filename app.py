import streamlit as st
import anthropic
import re
import threading
import requests
from datetime import datetime, timezone, timedelta
from supabase import create_client
from tavily import TavilyClient
from exa_py import Exa

st.set_page_config(page_title="시장 및 종목 내러티브 분석 엔진", page_icon="⚡", layout="wide")
st.markdown("""
<style>
body, .stApp { background: #f8f9fc !important; color: #1a1a2e !important; }
.block-container { padding-top: 1.5rem; max-width: 1100px; }
.stButton > button { background: #fff; border: 1.5px solid #d0d5e0; color: #2d3a5e; border-radius: 7px; font-size: 13px; font-weight: 500; transition: all 0.18s; }
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

CACHE_TTL_HOURS = 720  # 30일 (24 * 30)

DEFAULT_OLLAMA_URL = "http://localhost:11434"
DEFAULT_MODELS = {
    "kospi200":  "gemma3:27b",   # gemma는 한국어 지시 준수율 높음 (qwen은 중국어 혼입 위험)
    "sp500":     "gemma3:27b",
    "nikkei225": "gemma3:27b",
}

def get_ollama_url():
    return st.secrets.get("OLLAMA_URL", DEFAULT_OLLAMA_URL).rstrip("/")

def get_ollama_model(market_id="sp500"):
    key_map = {"kospi200":"OLLAMA_MODEL_KOSPI","sp500":"OLLAMA_MODEL_SP500","nikkei225":"OLLAMA_MODEL_NIKKEI"}
    return st.secrets.get(key_map.get(market_id,"OLLAMA_MODEL_SP500"), DEFAULT_MODELS.get(market_id,"gemma3:27b"))

MARKETS = {
    "🇰🇷 한국 시장": {"id":"kospi200","flag":"🇰🇷","color":"#4fc3f7","index":"KOSPI 200","region":"한국","central_bank":"한국은행(BOK)","currency":"원화(KRW)","analysts":"국내외 증권사 애널리스트"},
    "🇺🇸 미국 시장": {"id":"sp500","flag":"🇺🇸","color":"#00e87a","index":"S&P 500","region":"미국","central_bank":"연준(Fed)","currency":"달러(USD)","analysts":"월스트리트 애널리스트"},
    "🇯🇵 일본 시장": {"id":"nikkei225","flag":"🇯🇵","color":"#ff7043","index":"닛케이 225","region":"일본","central_bank":"일본은행(BOJ)","currency":"엔화(JPY)","analysts":"일본 및 글로벌 증권사 애널리스트"},
}

INDEX_OPTIONS = {
    "kospi200":  [("index_kospi200","KOSPI 200"),("index_kosdaq150","KOSDAQ 150")],
    "sp500":     [("index_sp500","S&P 500"),("index_nasdaq100","NASDAQ 100")],
    "nikkei225": [("index_nikkei225","닛케이 225")],
}

STOCKS = {
    "kospi200": [
        ("005930","삼성전자","반도체"),("000660","SK하이닉스","반도체"),("207940","삼성바이오로직스","바이오"),
        ("005380","현대자동차","자동차"),("000270","기아","자동차"),("051910","LG화학","화학"),
        ("035420","NAVER","인터넷"),("035720","카카오","인터넷"),("068270","셀트리온","바이오"),
        ("105560","KB금융","금융"),("055550","신한지주","금융"),("032830","삼성생명","보험"),
        ("012330","현대모비스","자동차부품"),("003550","LG","지주사"),("066570","LG전자","전자"),
        ("028260","삼성물산","건설"),("096770","SK이노베이션","에너지"),("034730","SK","지주사"),
        ("003490","대한항공","항공"),("009830","한화솔루션","에너지"),("017670","SK텔레콤","통신"),
        ("030200","KT","통신"),("086790","하나금융지주","금융"),("010950","S-Oil","정유"),
        ("015760","한국전력","유틸리티"),("018260","삼성에스디에스","IT서비스"),("011200","HMM","해운"),
        ("259960","크래프톤","게임"),("329180","HD현대중공업","조선"),("042700","한미반도체","반도체장비"),
        ("064350","현대로템","방산/철도"),("034020","두산에너빌리티","에너지설비"),("010140","삼성중공업","조선"),
        ("267260","HD현대일렉트릭","전력기기"),("009540","HD한국조선해양","조선"),("000810","삼성화재","보험"),
        ("316140","우리금융지주","금융"),("024110","기업은행","은행"),("006400","삼성SDI","배터리"),
        ("373220","LG에너지솔루션","배터리"),("251270","넷마블","게임"),("047050","포스코인터내셔널","상사/에너지"),
        ("005490","POSCO홀딩스","철강"),("000100","유한양행","제약"),("196170","알테오젠","바이오"),
        ("145020","휴젤","바이오"),("090430","아모레퍼시픽","화장품"),("035900","JYP Ent.","엔터테인먼트"),
        ("352820","하이브","엔터테인먼트"),("041510","에스엠","엔터테인먼트"),("053800","안랩","소프트웨어/보안"),
        ("112040","위메이드","게임"),("263750","펄어비스","게임"),("161390","한국타이어앤테크놀로지","타이어"),
        ("071050","한국금융지주","증권"),
    ],
    "sp500": [
        ("AAPL","Apple","Technology"),("MSFT","Microsoft","Technology"),("NVDA","NVIDIA","Semiconductors"),
        ("AMZN","Amazon","Consumer/Cloud"),("GOOGL","Alphabet","Internet"),("META","Meta","Social Media"),
        ("TSLA","Tesla","EV/Energy"),("BRK.B","Berkshire","Financials"),("JPM","JPMorgan","Banking"),
        ("V","Visa","Payments"),("UNH","UnitedHealth","Healthcare"),("XOM","ExxonMobil","Energy"),
        ("JNJ","J&J","Healthcare"),("WMT","Walmart","Retail"),("MA","Mastercard","Payments"),
        ("PG","P&G","Consumer"),("HD","Home Depot","Retail"),("BAC","Bank of America","Banking"),
        ("AVGO","Broadcom","Semiconductors"),("LLY","Eli Lilly","Pharma"),("COST","Costco","Retail"),
        ("ABBV","AbbVie","Pharma"),("MRK","Merck","Pharma"),("PEP","PepsiCo","Consumer"),
        ("KO","Coca-Cola","Consumer"),("ADBE","Adobe","Software"),("CRM","Salesforce","Software"),
        ("NFLX","Netflix","Media"),("AMD","AMD","Semiconductors"),("ORCL","Oracle","Software"),
        ("TMO","Thermo Fisher","Life Science"),("MCD","McDonald's","Consumer"),("ACN","Accenture","IT Services"),
        ("LIN","Linde","Materials"),("DHR","Danaher","Life Science"),("TXN","Texas Instruments","Semiconductors"),
        ("QCOM","Qualcomm","Semiconductors"),("INTU","Intuit","Software"),("AMGN","Amgen","Biotech"),
        ("GE","GE Aerospace","Aerospace"),("CSCO","Cisco","Networking"),("NKE","Nike","Apparel"),
        ("PM","Philip Morris","Consumer"),("IBM","IBM","Technology"),("INTC","Intel","Semiconductors"),
        ("CAT","Caterpillar","Industrials"),("NOW","ServiceNow","Software"),("GS","Goldman Sachs","Financials"),
        ("PLTR","Palantir","Software"),("UBER","Uber","Platform"),("AMAT","Applied Materials","Semiconductor Equipment"),
        ("ETN","Eaton","Power Infrastructure"),("RTX","RTX","Defense/Aerospace"),("BKNG","Booking Holdings","Travel Platform"),
        ("SPGI","S&P Global","Financial Data"),
    ],
    "nikkei225": [
        ("7203","토요타자동차","자동차"),("6758","소니그룹","전자/엔터"),("9984","소프트뱅크그룹","통신/투자"),
        ("8306","미쓰비시UFJ파이낸셜그룹","은행"),("6861","키엔스","전자기기"),("6367","다이킨공업","공조"),
        ("4063","신에츠화학","화학"),("7974","닌텐도","게임"),("6501","히타치제작소","전기/인프라"),
        ("6702","후지쯔","IT"),("8035","도쿄일렉트론","반도체장비"),("7267","혼다","자동차"),
        ("2914","일본담배산업","소비재"),("9432","NTT","통신"),("8411","미즈호파이낸셜그룹","은행"),
        ("4502","다케다약품공업","제약"),("6971","교세라","전자부품"),("7751","캐논","광학/전자"),
        ("6954","화낙","로봇"),("3382","세븐앤아이홀딩스","소매"),("9983","패스트리테일링","의류"),
        ("6098","리크루트홀딩스","인력/플랫폼"),("8766","도쿄해상홀딩스","보험"),("8058","미쓰비시상사","종합상사"),
        ("8001","이토추상사","종합상사"),("8031","미쓰이물산","종합상사"),("8053","스미토모상사","종합상사"),
        ("9433","KDDI","통신"),("9434","소프트뱅크","통신"),("9020","JR동일본","철도"),
        ("9022","JR도카이","철도"),("9021","JR서일본","철도"),("2802","아지노모토","식품"),
        ("4543","데루모","의료기기"),("4519","주가이제약","제약"),("4568","다이이치산쿄","제약"),
        ("6594","니덱","전기모터"),("6723","르네사스일렉트로닉스","반도체"),("7741","HOYA","광학/의료"),
        ("7733","올림푸스","의료기기"),("4901","후지필름홀딩스","헬스케어/소재"),("6503","미쓰비시전기","전기"),
        ("8015","도요타통상","종합상사"),("8801","미쓰이부동산","부동산"),("6857","어드반테스트","반도체장비/테스트"),
        ("6762","TDK","전자부품"),("7201","닛산자동차","자동차"),("7269","스즈키","자동차"),
        ("2502","아사히그룹홀딩스","식음료"),("4452","가오","생활용품"),
    ],
}

AGENT_LABELS = {
    "bull":"📈 강세 애널리스트","neutral":"➡️ 중립 애널리스트","bear":"📉 약세 애널리스트",
    "bull_critic":"🔥 강세 비판","neutral_critic":"🔥 중립 비판","bear_critic":"🔥 약세 비판",
    "judge":"⚡ 최종 판정자",
}

@st.cache_resource
def get_tavily():
    return TavilyClient(api_key=st.secrets["TAVILY_API_KEY"])

@st.cache_resource
def get_exa():
    return Exa(api_key=st.secrets["EXA_API_KEY"])

EXA_FINANCIAL_DOMAINS = [
    "bloomberg.com","ft.com","reuters.com","wsj.com","seekingalpha.com","barrons.com","marketwatch.com",
    "cnbc.com","economist.com","morningstar.com","investopedia.com","fool.com","zacks.com",
    "hankyung.com","mk.co.kr","edaily.co.kr","thebell.co.kr","fnguide.com","investing.com",
    "nikkei.com","toyokeizai.net",
]

def _get_sns_domains(market_id):
    if market_id == "kospi200":
        return ["finance.naver.com","hankyung.com","mk.co.kr","investing.com","stockplus.com","ppomppu.co.kr","clien.net","fmkorea.com"]
    elif market_id == "nikkei225":
        return ["minkabu.jp","kabutan.jp","finance.yahoo.co.jp","stockvoice.jp"]
    else:
        return ["reddit.com","stocktwits.com","x.com","twitter.com"]

def _build_sns_queries(target, direction, market_id):
    dir_en = "bullish" if direction=="bull" else ("bearish" if direction=="bear" else "outlook")
    if market_id == "kospi200":
        return [f"{target} 주식 종토방", f"{target} 매수 매도 여론"]
    elif market_id == "nikkei225":
        return [f"{target} 株 掲示板", f"{target} 個人投資家"]
    else:
        return [f"{target} stock {dir_en} reddit", f"{target} stock retail sentiment"]

def _detect_platform(url):
    if "reddit.com" in url: return "Reddit"
    if "stocktwits.com" in url: return "StockTwits"
    if "x.com" in url or "twitter.com" in url: return "X"
    if "naver.com" in url: return "네이버금융"
    if "minkabu" in url: return "みんかぶ"
    if "kabutan" in url: return "株探"
    return "커뮤니티"

def build_stock_lookup():
    lookup = {}
    for market_id, items in STOCKS.items():
        for ticker, name, sector in items:
            lookup[ticker.upper()] = {"ticker":ticker,"name":name,"sector":sector,"market_id":market_id}
            lookup[name.strip().lower()] = {"ticker":ticker,"name":name,"sector":sector,"market_id":market_id}
    return lookup

ENTITY_OVERRIDE = {
    "META":  {"canonical":"Meta Platforms","aliases":["Meta Platforms","Meta","NASDAQ:META","META"]},
    "GOOGL": {"canonical":"Alphabet","aliases":["Alphabet","Google","NASDAQ:GOOGL","GOOGL"]},
    "BRK.B":{"canonical":"Berkshire Hathaway","aliases":["Berkshire Hathaway","NYSE:BRK.B","BRK.B"]},
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

def normalize_entity(target, ticker_raw="", market_id="sp500"):
    target = (target or "").strip()
    ticker_raw = (ticker_raw or "").strip()
    lookup = build_stock_lookup()
    stock_info = None
    for key in [ticker_raw.upper(), target.upper(), target.lower()]:
        if key and key in lookup:
            stock_info = lookup[key]; break
    target_l = target.lower()
    if market_id=="sp500" and target_l in ["s&p 500","sp500","s&p500"]:
        return {"canonical":"S&P 500","aliases":["S&P 500","SP500","SPX","US large cap equities"]}
    if market_id=="kospi200" and target_l in ["kospi 200","kospi200","코스피200"]:
        return {"canonical":"KOSPI 200","aliases":["KOSPI 200","코스피200","Korean large cap equities"]}
    if market_id=="nikkei225" and target_l in ["nikkei 225","nikkei225","닛케이225","日経225"]:
        return {"canonical":"Nikkei 225","aliases":["Nikkei 225","日経225","Japanese large cap equities"]}
    if stock_info:
        ticker = stock_info["ticker"]; name = stock_info["name"]; rm = stock_info["market_id"]
        if ticker.upper() in ENTITY_OVERRIDE:
            base = ENTITY_OVERRIDE[ticker.upper()].copy()
            aliases = list(dict.fromkeys(base["aliases"]+[name,ticker]))
            return {"canonical":base["canonical"],"aliases":aliases,"ticker":ticker,"name":name,"market_id":rm,"sector":stock_info["sector"]}
        aliases = [name, ticker]
        if rm=="kospi200": aliases.append(f"{ticker}.KS")
        elif rm=="nikkei225": aliases.append(f"{ticker}.T")
        elif rm=="sp500": aliases+=[f"NASDAQ:{ticker}",f"NYSE:{ticker}"]
        return {"canonical":name,"aliases":list(dict.fromkeys([a for a in aliases if a])),"ticker":ticker,"name":name,"market_id":rm,"sector":stock_info["sector"]}
    aliases=[a for a in [target,ticker_raw] if a]
    return {"canonical":target or ticker_raw,"aliases":list(dict.fromkeys(aliases)),"ticker":ticker_raw,"name":target,"market_id":market_id,"sector":""}

def build_queries(target, direction, market_index, sector="", market_id="sp500", ticker_raw=""):
    entity = normalize_entity(target, ticker_raw, market_id)
    canonical = entity["canonical"]
    aliases = entity["aliases"][:4]
    alias_main = canonical
    alias_or = " OR ".join([f'"{a}"' for a in aliases])
    sector_txt = f" {sector}" if sector else ""

    # 한국 기업용 영문 검색어 (티커.KS 또는 종목명 영문)
    # 예: "Samsung Electronics" "005930.KS" "Samsung Electronics KOSPI"
    ko_en_aliases = [a for a in aliases if re.search(r'[A-Za-z]', a)]  # 영문 포함 alias
    ko_en_main = ko_en_aliases[0] if ko_en_aliases else alias_main
    # 티커 KS suffix
    ks_ticker = f"{ticker_raw}.KS" if ticker_raw and market_id=="kospi200" else ""

    if market_id == "kospi200":
        # ── 국내 한국어 쿼리 ───────────────────────────────────────────────────
        tq = [
            f'{alias_or} 최근 뉴스 사업 전략 경쟁 리스크',
            f'{alias_or}{sector_txt} 실적 발표 수익성 수요 마진',
            f'{alias_or} 투자 포인트 우려 요인',
            f'{market_index} 최근 전망 거시 수급 금리 환율',
        ]
        # ── 해외 시각 영문 쿼리 (신규) ─────────────────────────────────────────
        tq_global = [
            f'{ko_en_main} stock analyst rating target price Wall Street',
            f'{ko_en_main} {sector_txt} foreign investor outlook global',
            f'{ks_ticker or ko_en_main} Bloomberg Reuters Morgan Stanley Goldman',
            f'Korea {sector_txt} sector outlook global fund institutional view',
        ]
        # ── Exa: 국내 IR·공시 ─────────────────────────────────────────────────
        eq = [
            f'{alias_main} investment thesis strategy risk catalyst',
            f'{alias_main} earnings release investor relations profitability',
            f'{alias_main}{sector_txt} competition demand margin capex',
            f'{alias_main} regulatory risk execution narrative',
        ]
        # ── Exa: 해외 리포트 전용 (신규) ──────────────────────────────────────
        eq_global = [
            f'{ko_en_main} buy sell hold analyst report {sector_txt}',
            f'{ko_en_main} Korea stock ADR institutional investor',
            f'{ko_en_main} earnings growth valuation peer comparison',
        ]
        quant = [
            f'{alias_or} 매출 영업이익 영업이익률 순이익 EPS 전망',
            f'{alias_or} 가이던스 CAPEX 수주 backlog 신규수주',
            f'{alias_or} 공시 실적 발표 수치 YoY QoQ',
            f'{alias_or} 사업보고서 분기보고서 주석 수치',
        ]

    elif market_id == "nikkei225":
        tq = [
            f'{alias_or} 最新ニュース 事業戦略 競争 リスク',
            f'{alias_or}{sector_txt} 決算 収益性 需要 マージン',
            f'{alias_or} 投資ポイント 懸念材料',
            f'{market_index} 見通し 金利 為替 マクロ',
        ]
        tq_global = [
            f'{alias_main} Japan stock analyst rating global investor',
            f'{alias_main} {sector_txt} institutional view Bloomberg Reuters',
        ]
        eq = [
            f'{alias_main} investment thesis strategy risk catalyst',
            f'{alias_main} earnings release investor relations profitability',
            f'{alias_main}{sector_txt} competition demand margin capex',
            f'{alias_main} regulatory risk execution narrative',
        ]
        eq_global = [
            f'{alias_main} Japan ADR foreign investor analyst report',
            f'{alias_main} earnings growth valuation peer comparison global',
        ]
        quant = [
            f'{alias_or} 売上 営業利益 EPS ガイダンス',
            f'{alias_or} CAPEX 受注 backlog 需要',
            f'{alias_or} 決算短信 数値 YoY QoQ',
            f'{alias_or} 有価証券報告書 注記 数値',
        ]

    else:  # sp500
        tq = [
            f'{alias_or} latest news strategy competition risk',
            f'{alias_or}{sector_txt} earnings profitability demand margins',
            f'{alias_or} key debate catalysts concerns',
            f'{market_index} latest outlook macro rates positioning',
        ]
        tq_global = []  # 미국은 이미 글로벌 시각이 기본
        eq = [
            f'{alias_main} investment thesis strategy risk catalyst',
            f'{alias_main} earnings release investor relations profitability',
            f'{alias_main}{sector_txt} competition demand margin capex',
            f'{alias_main} regulatory risk execution narrative',
        ]
        eq_global = []
        quant = [
            f'{alias_or} revenue operating margin EPS guidance YoY QoQ',
            f'{alias_or} capex bookings backlog order demand numbers',
            f'{alias_or} annual report 10-Q 10-K financial metrics',
            f'{alias_or} notes to financial statements quantitative disclosure',
        ]

    return {
        "entity":     entity,
        "tavily":     tq,
        "tavily_global": tq_global,       # 해외 시각 Tavily 쿼리
        "exa_report": eq,
        "exa_global": eq_global,          # 해외 시각 Exa 쿼리
        "exa_sns":    _build_sns_queries(alias_main, "neutral", market_id),
        "quant":      quant,
    }

def extract_numeric_sentences(text, max_sentences=5):
    if not text: return []
    sentences = re.split(r'[.。！？\n]', text)
    numeric = [s.strip() for s in sentences if s.strip() and re.search(r'\d+[\.,]?\d*[%억만원달러$€£\+\-]', s)]
    return numeric[:max_sentences]

def collect_quant_evidence(queries, entity_info=None, market_id="sp500", target="", ticker_raw=""):
    tavily_items = search_tavily(queries)
    exa_items = search_exa_reports(queries, entity_info=entity_info, recent_days=120, market_id=market_id)
    naver_items = []
    if market_id == "kospi200" and target:
        naver_items = search_naver_quant(target, ticker_raw)
    merged, seen = [], set()
    for r in tavily_items + exa_items + naver_items:
        url=r.get("url","") or ""; title=r.get("title","") or ""
        key=(url,title)
        if key in seen: continue
        seen.add(key)
        num_sents = extract_numeric_sentences(r.get("content","") or "", max_sentences=5)
        if not num_sents: continue
        merged.append({"title":title,"url":url,"date":r.get("date","") or "","numeric_evidence":num_sents,"engine":r.get("engine","mixed")})
    return merged

def search_tavily(queries):
    results = []
    try:
        client = get_tavily(); seen = set()
        for q in queries:
            try:
                resp = client.search(q, max_results=4, search_depth="advanced", include_answer=False)
                for r in resp.get("results",[]):
                    url=r.get("url","")
                    if url in seen: continue
                    seen.add(url)
                    results.append({"title":r.get("title",""),"url":url,"content":r.get("content","")[:700],"date":r.get("published_date","")})
            except Exception as e:
                results.append({"title":"🚨 Tavily 검색 에러","url":"","content":str(e),"date":datetime.now().strftime("%Y-%m-%d")})
    except Exception as e:
        results.append({"title":"🚨 Tavily 클라이언트 에러","url":"","content":str(e),"date":""})
    return results

def search_exa_reports(queries, entity_info=None, recent_days=120, market_id="sp500"):
    results, seen = [], set()
    try:
        client = get_exa()
        start_date = (datetime.now()-timedelta(days=recent_days)).strftime("%Y-%m-%dT00:00:00.000Z")
        domain_map = {
            "sp500":    ["sec.gov","reuters.com","bloomberg.com","wsj.com","seekingalpha.com","marketwatch.com","ft.com","barrons.com"],
            # KOSPI: 국내 IR·공시 도메인 (해외 리포트는 eq_global 별도 처리)
            "kospi200": ["dart.fss.or.kr","fnguide.com","hankyung.com","mk.co.kr","edaily.co.kr","thebell.co.kr"],
            "nikkei225":["nikkei.com","kabutan.jp","minkabu.jp","toyokeizai.net"],
        }
        # 한국·일본 기업 해외 시각 전용 도메인
        global_domains = [
            "bloomberg.com","reuters.com","ft.com","wsj.com",
            "seekingalpha.com","barrons.com","marketwatch.com",
            "morningstar.com","fool.com","cnbc.com",
        ]
        include_domains = domain_map.get(market_id, [])
        additional = []
        if entity_info:
            for a in entity_info.get("aliases",[])[:3]:
                additional.extend([f"{a} investment thesis",f"{a} analyst report",f"{a} earnings release"])
        for q in queries:
            try:
                resp = client.search_and_contents(q, type="deep", num_results=8, start_published_date=start_date, include_domains=include_domains if include_domains else None, additional_queries=additional[:6], highlights={"max_characters":700}, text={"max_characters":1800})
                if not resp or not getattr(resp,"results",None): continue
                for r in resp.results:
                    url=getattr(r,"url","") or ""
                    if not url or url in seen: continue
                    seen.add(url)
                    hl=getattr(r,"highlights",None) or []; text=getattr(r,"text","") or ""
                    summary=" … ".join(hl) if hl else text[:1000]
                    results.append({"title":getattr(r,"title","") or "검색 결과","url":url,"content":summary,"date":getattr(r,"published_date","") or "","engine":"exa"})
            except Exception as e:
                results.append({"title":"🚨 Exa 오류","url":"debug","content":str(e),"date":datetime.now().strftime("%Y-%m-%d"),"engine":"exa"})
    except Exception as e:
        results.append({"title":"🚨 Exa 초기화 실패","url":"","content":str(e),"date":"","engine":"exa"})
    return results

def search_tavily_sns(queries, market_id="sp500"):
    results = []
    try:
        client = get_tavily(); seen = set(); domains = _get_sns_domains(market_id)
        for q in queries:
            try:
                resp = client.search(q, max_results=4, search_depth="basic", include_domains=domains)
                for r in resp.get("results",[]):
                    url=r.get("url") or ""
                    if not url or url in seen: continue
                    seen.add(url)
                    results.append({"title":r.get("title") or "","url":url,"content":(r.get("content") or "")[:600],"date":r.get("published_date") or "","platform":_detect_platform(url)})
            except Exception as e:
                results.append({"title":"🚨 SNS 에러","url":"","content":str(e),"date":datetime.now().strftime("%Y-%m-%d")})
    except Exception as e:
        results.append({"title":"🚨 SNS 클라이언트 에러","url":"","content":str(e),"date":""})
    return results

def fetch_current_price(target, ticker_raw, market_id):
    client = get_tavily()
    if market_id=="kospi200": queries=[f"{target} 현재 주가",f"{ticker_raw} 주가 추이"]
    elif market_id=="nikkei225": queries=[f"{target} 株価 現在",f"{ticker_raw} 株価"]
    else: queries=[f"{target} stock current price",f"{ticker_raw} stock price today"]
    snippets=[]
    for q in queries:
        try:
            resp=client.search(q,max_results=3,search_depth="basic")
            for r in resp.get("results",[]):
                c=(r.get("content") or "")[:300]; d=(r.get("published_date") or "")[:10]
                if c: snippets.append(f"■ {r.get('title','')} ({d})\n  {r.get('url','')}\n  {c}")
        except Exception as e: snippets.append(f"🚨 현재가 검색 에러: {e}")
    if not snippets: return "[현재가 검색 실패]"
    return f"【현재 주가 ({datetime.now().strftime('%Y-%m-%d %H:%M')})】\n" + "\n\n".join(snippets)

def fetch_earnings_transcript(ticker_raw, target_name="", market_id="sp500"):
    now=datetime.now(); yr=now.year; q_c=(now.month-1)//3+1; q_p=q_c-1 if q_c>1 else 4; yr_p=yr if q_c>1 else yr-1
    if market_id=="kospi200": return _fetch_earnings_tavily_kr(target_name,ticker_raw,yr,q_c,q_p)
    if market_id=="nikkei225": return _fetch_earnings_tavily_jp(target_name,ticker_raw,yr)
    fmp_key=st.secrets.get("FMP_API_KEY","")
    if fmp_key and fmp_key.strip() not in ("","...","여기에_FMP_키"):
        r=_fetch_fmp(_sanitize_fmp_ticker(ticker_raw),fmp_key,yr,yr-1)
        if r: return r
    return _fetch_earnings_tavily_us(target_name,ticker_raw,yr,q_c,q_p)

def _sanitize_fmp_ticker(t): return t.replace(".","‑")

def _fetch_earnings_tavily_kr(name, ticker, yr, q, q_p):
    """한국 어닝콜: 네이버 API(우선) + Tavily(보완) 병렬 수집"""
    snippets, seen = [], set()

    # 1) 네이버 API — 한국어 실적 뉴스 가장 정확
    naver_items = search_naver_earnings(name, ticker, yr, q)
    for r in naver_items:
        url = r.get("url","")
        if not url or url in seen: continue
        seen.add(url)
        snippets.append(f"■ [네이버] {r['title']} ({r['date']})\n  {url}\n  {r['content'][:500]}")

    # 2) Tavily 보완 (한경·매경·이데일리·더벨)
    c = get_tavily()
    for query in [f"{name} {yr}년 {q}분기 실적발표 컨퍼런스콜 경영진",
                  f"{name} 실적 영업이익 매출 경영진 발언 {yr}"]:
        try:
            resp = c.search(query, max_results=3, search_depth="advanced",
                include_domains=["hankyung.com","mk.co.kr","edaily.co.kr","thebell.co.kr","sedaily.com"])
            for r in resp.get("results",[]):
                url = r.get("url","")
                if not url or url in seen: continue
                seen.add(url)
                snippets.append(f"■ [Tavily] {r.get('title','')} ({r.get('published_date','')[:10]})\n  {url}\n  {r.get('content','')[:500]}")
        except: pass

    if not snippets: return f"[{name} 실적발표 뉴스 없음]"
    return f"【{name} 최신 실적발표·컨퍼런스콜 (네이버+Tavily)】\n\n" + "\n\n".join(snippets[:8])

def _fetch_earnings_tavily_jp(name,ticker,yr):
    c=get_tavily(); qs=[f"{name} {yr}年 決算発表 業績 経営者コメント",f"{ticker} 決算 売上 ガイダンス {yr}"]
    snippets,seen=[],set()
    for query in qs:
        try:
            resp=c.search(query,max_results=4,search_depth="advanced",include_domains=["nikkei.com","minkabu.jp","kabutan.jp","toyokeizai.net"])
            for r in resp.get("results",[]):
                url=r.get("url","")
                if url in seen: continue
                seen.add(url)
                snippets.append(f"■ {r.get('title','')} ({r.get('published_date','')[:10]})\n  {url}\n  {r.get('content','')[:500]}")
        except: pass
    if not snippets: return f"[{name} 決算 뉴스 없음]"
    return f"【{name} 最新決算】\n\n" + "\n\n".join(snippets[:5])

def _fetch_earnings_tavily_us(name,ticker,yr,q,q_p):
    c=get_tavily(); qs=[f"{name} {ticker} earnings call Q{q_p} {yr} CEO CFO guidance",f"{ticker} quarterly earnings management commentary {yr}"]
    snippets,seen=[],set()
    for query in qs:
        try:
            resp=c.search(query,max_results=4,search_depth="advanced",include_domains=["seekingalpha.com","fool.com","cnbc.com","reuters.com","bloomberg.com"])
            for r in resp.get("results",[]):
                url=r.get("url","")
                if url in seen: continue
                seen.add(url)
                snippets.append(f"■ {r.get('title','')} ({r.get('published_date','')[:10]})\n  {url}\n  {r.get('content','')[:500]}")
        except: pass
    if not snippets: return f"[{name} 어닝콜 뉴스 없음]"
    return f"【{name} 어닝콜·실적발표】\n\n" + "\n\n".join(snippets[:5])

def _fetch_fmp(ticker,fmp_key,yr,yr_p):
    import urllib.request,json as _j; found=None
    for y in [yr,yr_p]:
        for q in [4,3,2,1]:
            url=f"https://financialmodelingprep.com/api/v3/earning_call_transcript/{ticker}?quarter={q}&year={y}&apikey={fmp_key}"
            try:
                req=urllib.request.Request(url,headers={"User-Agent":"Mozilla/5.0"})
                with urllib.request.urlopen(req,timeout=8) as r: data=_j.loads(r.read())
                if data and isinstance(data,list) and data[0].get("content"): found=(data[0],q,y); break
            except: continue
        if found: break
    if not found: return None
    row,q,y=found; text=row.get("content","")
    if len(text)>6000: text=text[:3500]+"\n[중략]\n"+text[-2000:]
    return f"【어닝콜: {ticker} Q{q} {y}】\n(Financial Modeling Prep)\n\n{text}"

def fetch_fmp_financial_snapshot(ticker_raw, market_id="sp500"):
    if not ticker_raw or market_id!="sp500": return None
    fmp_key=st.secrets.get("FMP_API_KEY","")
    if not fmp_key or fmp_key.strip() in ("","...","여기에_FMP_키"): return None
    ticker=FMP_TICKER_MAP.get(ticker_raw,ticker_raw).replace(".","‑")
    headers={"User-Agent":"Mozilla/5.0"}
    endpoints={"income_statement":f"https://financialmodelingprep.com/api/v3/income-statement/{ticker}?limit=2&apikey={fmp_key}","ratios":f"https://financialmodelingprep.com/api/v3/ratios/{ticker}?limit=2&apikey={fmp_key}","analyst_estimates":f"https://financialmodelingprep.com/api/v3/analyst-estimates/{ticker}?limit=2&apikey={fmp_key}"}
    out=[]
    for label,url in endpoints.items():
        try:
            r=requests.get(url,headers=headers,timeout=10)
            if r.status_code!=200: continue
            data=r.json()
            if not data or not isinstance(data,list): continue
            row=data[0]
            if label=="income_statement": out.append(f"■ 손익: 매출={row.get('revenue')}, 영업이익={row.get('operatingIncome')}, 순이익={row.get('netIncome')}, EPS={row.get('eps')}, 일={row.get('date')}")
            elif label=="ratios": out.append(f"■ 비율: grossMargin={row.get('grossProfitMargin')}, opMargin={row.get('operatingProfitMargin')}, netMargin={row.get('netProfitMargin')}, ROE={row.get('returnOnEquity')}")
            elif label=="analyst_estimates": out.append(f"■ 추정: revAvg={row.get('estimatedRevenueAvg')}, epsAvg={row.get('estimatedEpsAvg')}, date={row.get('date')}")
        except: continue
    if not out: return None
    return "【FMP 구조화 정량 스냅샷】\n" + "\n".join(out)


# ─── 네이버 뉴스 API (KOSPI 한국 기업 전용) ─────────────────────────────────────
# ─── 네이버 API 공통 헬퍼 ─────────────────────────────────────────────────────
def _naver_search_raw(queries: list[str], display: int = 8, max_total: int = 30) -> list[dict]:
    """
    네이버 뉴스 검색 API 저수준 함수.
    쿼리 목록을 받아 중복 제거된 결과 리스트 반환.
    NAVER_CLIENT_ID / NAVER_CLIENT_SECRET 없으면 빈 리스트.
    """
    client_id     = st.secrets.get("NAVER_CLIENT_ID", "")
    client_secret = st.secrets.get("NAVER_CLIENT_SECRET", "")
    if not client_id or not client_secret:
        return []

    import html as _html, re as _re
    from email.utils import parsedate_to_datetime

    headers = {
        "X-Naver-Client-Id":     client_id,
        "X-Naver-Client-Secret": client_secret,
    }
    seen, results = set(), []
    for query in queries:
        try:
            resp = requests.get(
                "https://openapi.naver.com/v1/search/news.json",
                headers=headers,
                params={"query": query, "display": display, "sort": "date"},
                timeout=8,
            )
            resp.raise_for_status()
            for item in resp.json().get("items", []):
                link = item.get("link", "") or item.get("originallink", "")
                if not link or link in seen: continue
                seen.add(link)
                title = _html.unescape(_re.sub(r"<[^>]+>", "", item.get("title", "")))
                desc  = _html.unescape(_re.sub(r"<[^>]+>", "", item.get("description", "")))
                try:
                    date_str = parsedate_to_datetime(item.get("pubDate","")).strftime("%Y-%m-%d")
                except:
                    date_str = item.get("pubDate","")[:10]
                results.append({"title":title,"url":link,"content":desc[:500],"date":date_str})
        except Exception as e:
            print(f"Naver API 오류 [{query[:40]}]: {e}")
        if len(results) >= max_total:
            break
    return results[:max_total]


def search_naver(target: str, direction: str, ticker: str = "") -> list[dict]:
    """
    내러티브 수집용 네이버 뉴스 검색.
    방향(강세/중립/약세)별 키워드 + 실적·리포트·공시 쿼리.
    """
    dir_kw = {
        "bull":    ["매수", "목표주가 상향", "강세", "상승 여력"],
        "neutral": ["중립", "보합", "관망", "횡보"],
        "bear":    ["매도", "목표주가 하향", "약세", "하락 리스크"],
    }
    kw_str = " OR ".join(dir_kw.get(direction, ["전망"])[:3])
    queries = [
        f"{target} {kw_str}",
        f"{target} 실적 영업이익 매출 전망",
        f"{target} 증권사 리포트 목표주가",
        f"{target} 업황 경쟁사 시장점유율",
    ]
    if ticker:
        queries.append(f"{ticker} {target} 공시 IR 사업보고서")
    return _naver_search_raw(queries, display=8, max_total=25)


def search_naver_price(target: str, ticker: str = "") -> list[dict]:
    """현재가·주가 흐름 전용 네이버 검색"""
    queries = [
        f"{target} 현재 주가 오늘",
        f"{target} 주가 등락 상승 하락",
    ]
    if ticker:
        queries.append(f"{ticker} 주가 추이")
    return _naver_search_raw(queries, display=5, max_total=10)


def search_naver_price_action(target: str, ticker: str = "") -> list[dict]:
    """주가 컨텍스트(52주 고저가, 수익률, 이벤트 반응) 전용 네이버 검색"""
    queries = [
        f"{target} 주가 수익률 1개월 3개월 6개월",
        f"{target} 52주 신고가 신저가 현재 위치",
        f"{target} 실적발표 후 주가 반응 상승 하락",
        f"{target} 목표주가 상향 하향 추세 최근",
        f"{target} 외국인 기관 수급 매수 매도",
    ]
    if ticker:
        queries.append(f"{ticker} 주가 밸류에이션 PER PBR")
    return _naver_search_raw(queries, display=6, max_total=20)


def search_naver_earnings(target: str, ticker: str, yr: int, q: int) -> list[dict]:
    """실적발표·컨퍼런스콜 전용 네이버 검색"""
    queries = [
        f"{target} {yr}년 {q}분기 실적발표 컨퍼런스콜",
        f"{target} {yr}년 영업이익 매출 가이던스 경영진",
        f"{target} 실적 어닝콜 CEO CFO 발언",
        f"{target} {yr} 분기 수주 backlog 신규",
    ]
    if ticker:
        queries.append(f"{ticker} 공시 실적 수치")
    return _naver_search_raw(queries, display=8, max_total=20)


def search_naver_quant(target: str, ticker: str = "") -> list[dict]:
    """정량 근거(재무수치, EPS, 마진) 전용 네이버 검색"""
    queries = [
        f"{target} 매출 영업이익 영업이익률 순이익 YoY",
        f"{target} EPS 추정 컨센서스 상향 하향",
        f"{target} CAPEX 투자 수주잔고 백로그",
        f"{target} 마진 개선 악화 구조적",
    ]
    if ticker:
        queries.append(f"{ticker} 재무 수치 공시")
    return _naver_search_raw(queries, display=6, max_total=15)


def search_naver_sns(target: str, ticker: str = "") -> list[dict]:
    """종토방·커뮤니티 여론 전용 네이버 검색"""
    queries = [
        f"{target} 주식 종토방 여론",
        f"{target} 개인투자자 매수 매도 의견",
        f"{target} 소액주주 반응",
    ]
    if ticker:
        queries.append(f"{ticker} 주주 커뮤니티")
    return _naver_search_raw(queries, display=5, max_total=12)



def search_naver_ir(target: str, ticker: str = "") -> list[dict]:
    """
    IR·공시·사업보고서·증권사 리포트 전용 네이버 검색.
    Exa domestic(dart·fnguide)을 대체.
    """
    queries = [
        f"{target} IR 투자자설명회 기업설명회",
        f"{target} 사업보고서 분기보고서 반기보고서",
        f"{target} 증권사 리포트 분석 투자의견",
        f"{target} 공시 주요사항 보고서 신규수주",
        f"{target} 애널리스트 분석 목표주가 리포트",
    ]
    if ticker:
        queries.append(f"{ticker} DART 공시 사업")
    return _naver_search_raw(queries, display=8, max_total=25)


def combined_search(target, direction, market_index, sector="", ticker_raw="", market_id="sp500"):
    qs = build_queries(target, direction, market_index, sector, market_id, ticker_raw=ticker_raw)
    entity = qs.get("entity")

    if market_id == "kospi200":
        # ══ 한국 기업: 국내 뉴스·IR → 네이버 API가 훨씬 정확 ══════════════════
        # ① 국내 뉴스 (Tavily 대체)
        naver_news    = search_naver(target, direction, ticker_raw)
        # ② 국내 IR·공시·증권사 리포트 (Exa domestic 대체)
        naver_ir      = search_naver_ir(target, ticker_raw)
        # ③ 재무 정량
        naver_quant_r = search_naver_quant(target, ticker_raw)
        # ④ 커뮤니티·종토방 (Tavily SNS 대체)
        naver_sns_r   = search_naver_sns(target, ticker_raw)
        # ⑤ 어닝콜·실적 (네이버+Tavily 혼합, 기존 유지)
        et = fetch_earnings_transcript(ticker_raw, target_name=target, market_id=market_id) if ticker_raw else "[지수 — 어닝콜 해당 없음]"
        # ⑥ 해외 시각: Tavily global + Exa global (Bloomberg·Reuters·SA)
        tr_global = search_tavily(qs["tavily_global"]) if qs.get("tavily_global") else []
        er_global = (
            search_exa_reports(qs["exa_global"], entity_info=entity, recent_days=120, market_id="sp500")
            if qs.get("exa_global") else []
        )
        # 정량 근거: 네이버 quant만 사용 (Tavily/Exa quant 불필요)
        qr = collect_quant_evidence([], entity_info=entity, market_id=market_id, target=target, ticker_raw=ticker_raw)
        tr, er, sr, fs = [], [], [], None  # 국내 Tavily/Exa 비활성화

    else:
        # ══ 미국·일본: 기존 Tavily + Exa 사용 ═══════════════════════════════
        tr = search_tavily(qs["tavily"])
        er = search_exa_reports(qs["exa_report"], entity_info=entity, recent_days=120, market_id=market_id)
        sr = search_tavily_sns(qs["exa_sns"], market_id=market_id)
        qr = collect_quant_evidence(qs["quant"], entity_info=entity, market_id=market_id, target=target, ticker_raw=ticker_raw)
        et = fetch_earnings_transcript(ticker_raw, target_name=target, market_id=market_id) if ticker_raw else "[지수 — 어닝콜 해당 없음]"
        fs = fetch_fmp_financial_snapshot(ticker_raw, market_id=market_id) if ticker_raw else None
        tr_global = search_tavily(qs["tavily_global"]) if qs.get("tavily_global") else []
        er_global = (
            search_exa_reports(qs["exa_global"], entity_info=entity, recent_days=120, market_id="sp500")
            if qs.get("exa_global") else []
        )
        naver_news, naver_ir, naver_quant_r, naver_sns_r = [], [], [], []

    cutoff_str = (datetime.now()-timedelta(days=90)).strftime("%Y-%m-%d")

    def fmt(items, label, show_p=False):
        if not items: return f"【{label}】\n결과 없음\n"
        lines=[f"【{label}】"]; valid=0
        for r in items:
            ds=r.get("date") or ""
            if ds and len(ds)>=10 and ds[:10]<cutoff_str: continue
            valid+=1
            d=f" ({ds[:10]})" if ds else ""
            pl=f"[{r.get('platform','')}] " if show_p and r.get("platform") else ""
            lines.append(f"■ {pl}{r.get('title','')}{d}")
            if r.get("url"): lines.append(f"  {r['url']}")
            if r.get("content"): lines.append(f"  {r['content']}")
            lines.append("")
        if valid==0: return f"【{label}】\n최근 3개월 내 유의미한 결과 없음\n"
        return "\n".join(lines)

    def fmt_quant(items, label):
        if not items: return f"【{label}】\n결과 없음\n"
        lines=[f"【{label}】"]; valid=0
        for r in items:
            ds=r.get("date") or ""
            if ds and len(ds)>=10 and ds[:10]<cutoff_str: continue
            valid+=1; d=f" ({ds[:10]})" if ds else ""
            lines.append(f"■ {r.get('title','')}{d}")
            if r.get("url"): lines.append(f"  {r['url']}")
            for sent in r.get("numeric_evidence",[]): lines.append(f"  - {sent}")
            lines.append("")
        if valid==0: return f"【{label}】\n최근 3개월 내 유의미한 결과 없음\n"
        return "\n".join(lines)

    hdr = f"=== {target} [{direction}] ({datetime.now().strftime('%Y-%m-%d')}) ===\n"

    if market_id == "kospi200":
        # ── KOSPI: 국내 → 네이버 API, 해외 → Tavily/Exa global ────────────
        hdr += "소스: 네이버API(뉴스·IR·정량·종토방) + Tavily/Exa(해외시각) + 어닝콜\n"
        sections = [
            fmt(naver_news,    "① 국내 최신 뉴스·방향별 논점 (Naver API)"),
            fmt(naver_ir,      "② 국내 IR·공시·증권사 리포트 (Naver API)"),
            fmt(naver_sns_r,   "③ 종토방·커뮤니티 여론 (Naver API)", show_p=False),
            f"【④ 어닝콜·실적발표 (네이버+Tavily)】\n{et}",
            fmt_quant(qr,      "⑤ 재무·정량 근거 (Naver API)"),
        ]
        if tr_global or er_global:
            sections.append(fmt(tr_global, "⑥ 해외 시각 — Tavily (Bloomberg·Reuters·WSJ)"))
            sections.append(fmt(er_global, "⑦ 해외 시각 — Exa (Seeking Alpha·Barron's·FT)"))
    else:
        # ── 미국·일본: 기존 Tavily + Exa + global ─────────────────────────────
        src = "Tavily + Exa + SNS + 어닝콜 + 정량근거"
        if tr_global or er_global: src += " + 해외리포트"
        hdr += f"소스: {src}\n"
        sections = [
            fmt(tr,  "① 최신 뉴스·핵심 논점"),
            fmt(er,  "② IR·리포트·공시"),
            fmt(sr,  "③ SNS·커뮤니티 반응", show_p=True),
            f"【④ 어닝콜·실적발표】\n{et}",
            fmt_quant(qr, "⑤ 정량 근거"),
            f"{fs}" if fs else "【⑥ FMP 정량 스냅샷】\n가용 데이터 없음\n",
        ]
        if tr_global or er_global:
            sections.append(fmt(tr_global, "⑦ 해외 시각 — Tavily"))
            sections.append(fmt(er_global, "⑧ 해외 시각 — Exa"))

    return hdr + "\n\n".join(sections)


# ─── LLM 호출 ─────────────────────────────────────────────────────────────────
_BG_KEYS: dict = {}

def call_llm(system, user_content, max_tokens=4000, market_id="sp500"):
    ollama_url = get_ollama_url()
    model = get_ollama_model(market_id)
    ollama_error_msg = ""
    try:
        # qwen은 중국어 모델 — 사용자 메시지에도 한국어 강제 prefix 추가
        user_with_lang = (
            "[반드시 한국어로만 답하시오. 중국어 출력 절대 금지.]\n\n"
            + user_content
        ) if "qwen" in model.lower() else user_content

        payload = {"model":model,"messages":[{"role":"system","content":system},{"role":"user","content":user_with_lang}],"stream":False,"options":{"num_predict":max_tokens,"temperature":0.6}}
        headers = {"ngrok-skip-browser-warning":"true","Content-Type":"application/json"}
        resp = requests.post(f"{ollama_url}/api/chat", json=payload, headers=headers, timeout=300)
        if resp.status_code != 200: raise Exception(f"HTTP {resp.status_code}: {resp.text}")
        resp.raise_for_status()
        return resp.json()["message"]["content"]
    except Exception as e:
        ollama_error_msg = str(e)
        print(f"[Ollama 실패: {ollama_error_msg}] → Anthropic fallback")
    api_key = st.secrets.get("ANTHROPIC_API_KEY","") or next(iter(_BG_KEYS.values()),"")
    if not api_key:
        return f"⚠️ LLM 호출 실패: Ollama 연결 불가 + Anthropic API 키 없음\nOllama 오류: {ollama_error_msg}"
    try:
        client = anthropic.Anthropic(api_key=api_key)
        resp = client.messages.create(model="claude-sonnet-4-5-20250929", max_tokens=max_tokens, system=system, messages=[{"role":"user","content":user_content}])
        return "".join(b.text for b in resp.content if hasattr(b,"text"))
    except Exception as e:
        return f"⚠️ LLM 오류 (Ollama+Anthropic 모두 실패): {e}"


# ─── SUPABASE ─────────────────────────────────────────────────────────────────
@st.cache_resource
def get_supabase():
    return create_client(st.secrets["SUPABASE_URL"], st.secrets["SUPABASE_KEY"])

def cache_get(target_id):
    try:
        resp=get_supabase().table("analyses").select("*").eq("target_id",target_id).execute()
        if not resp.data: return None
        row=resp.data[0]
        if row.get("status")=="running": return row
        at=datetime.fromisoformat(row["analyzed_at"].replace("Z","")).replace(tzinfo=timezone.utc)
        if (datetime.now(timezone.utc)-at).total_seconds()/3600>CACHE_TTL_HOURS: return None
        return row
    except: return None

def cache_set(target_id, market_id, target_label, results, winner, bull_prob=50, neutral_prob=30, bear_prob=20, status="done", consensus_tp=""):
    try:
        get_supabase().table("analyses").upsert({"target_id":target_id,"market_id":market_id,"target_label":target_label,"results":results,"winner":winner,"bull_prob":bull_prob,"neutral_prob":neutral_prob,"bear_prob":bear_prob,"status":status,"consensus_tp":consensus_tp,"analyzed_at":datetime.now(timezone.utc).isoformat()},on_conflict="target_id").execute()
    except Exception as e: print(f"캐시 저장 오류: {e}")

def cache_set_running(target_id, market_id, target_label):
    try:
        get_supabase().table("analyses").upsert({"target_id":target_id,"market_id":market_id,"target_label":target_label,"results":{},"winner":"","status":"running","progress":0.0,"status_msg":"분석 준비 중...","analyzed_at":datetime.now(timezone.utc).isoformat()},on_conflict="target_id").execute()
    except: pass

def update_progress(target_id, pct, msg):
    try: get_supabase().table("analyses").update({"progress":float(pct),"status_msg":msg}).eq("target_id",target_id).execute()
    except Exception as e: print(f"진행 업데이트 오류: {e}")

def cache_delete(target_id):
    try: get_supabase().table("analyses").delete().eq("target_id",target_id).execute()
    except: pass

def load_leaderboard():
    try:
        # consensus_tp 컬럼이 없을 경우를 대비해 먼저 기본 컬럼으로 시도
        try:
            resp = get_supabase().table("analyses").select(
                "target_id,market_id,target_label,winner,bull_prob,neutral_prob,bear_prob,status,analyzed_at,consensus_tp"
            ).execute()
        except Exception:
            # consensus_tp 컬럼 없을 때 fallback
            resp = get_supabase().table("analyses").select(
                "target_id,market_id,target_label,winner,bull_prob,neutral_prob,bear_prob,status,analyzed_at"
            ).execute()
        rows = []
        for r in resp.data:
            if r.get("status") == "running":
                rows.append({**r, "age_hours": 0})
                continue
            at    = datetime.fromisoformat(r["analyzed_at"].replace("Z","")).replace(tzinfo=timezone.utc)
            age_h = (datetime.now(timezone.utc)-at).total_seconds()/3600
            if age_h <= CACHE_TTL_HOURS:
                rows.append({**r, "age_hours": round(age_h, 1)})
        rows.sort(key=lambda r: (-(r.get("bull_prob") or 0), (r.get("bear_prob") or 0)))
        return rows
    except Exception as e:
        print(f"load_leaderboard 오류: {e}")
        return []

def _has_chinese(text: str) -> bool:
    """중국어(한자) 문자 비율이 3% 이상이면 True"""
    if not text: return False
    chinese = sum(1 for c in text if '\u4e00' <= c <= '\u9fff')
    return chinese / max(len(text), 1) > 0.03

def _force_korean(text: str, market_id: str = "sp500") -> str:
    """
    중국어가 섞인 출력을 감지하면 LLM에 한국어 번역을 재요청.
    재번역 비용(토큰)은 작지만 품질 보장을 위해 필요.
    """
    if not _has_chinese(text):
        return text
    print(f"[중국어 감지] 한국어 번역 재호출 중... ({len(text)}자)")
    try:
        translate_prompt = (
            "아래 텍스트에 중국어가 포함되어 있습니다. "
            "모든 중국어 문장을 한국어로 번역하되, "
            "이미 한국어인 부분은 그대로 유지하십시오. "
            "형식(마크다운, 번호, 섹션 헤더)은 그대로 보존하십시오.\n\n"
            f"{text}"
        )
        translated = call_llm(
            "당신은 중국어→한국어 번역 전문가입니다. 출력은 반드시 한국어로만 작성하십시오.",
            translate_prompt,
            max_tokens=len(text) // 2 + 1000,  # 원문 길이 기반 토큰 추정
            market_id=market_id,
        )
        return translated if translated and not translated.startswith("⚠️") else text
    except Exception as e:
        print(f"[번역 재호출 실패]: {e}")
        return text

def strip_duplicate_translation(text: str, market_id: str = "sp500") -> str:
    """번역 섹션 중복 제거 + 중국어 자동 번역"""
    for m in ["번역 (한국어):","번역:","Translation:","Translated version:"]:
        if m in text:
            text = text.split(m)[-1].strip()
    # 중국어 감지 시 자동 번역
    text = _force_korean(text, market_id)
    return text.strip()


# ─── PROMPTS ──────────────────────────────────────────────────────────────────
def build_system_prompts(market, stock=None):
    idx=market["index"]; cb=market["central_bank"]
    target=f"{stock[1]} ({stock[0]})" if stock else idx
    sn=f" (섹터: {stock[2]}, {idx} 상장)" if stock else ""
    cutoff_date=(datetime.now()-timedelta(days=90))
    cutoff_str_ko=cutoff_date.strftime("%Y년 %m월 %d일")
    cutoff_str_en=cutoff_date.strftime("%B %d, %Y")
    cutoff_str_jp=cutoff_date.strftime("%Y年%m月%d日")

    # ── 언어 강제 지시 ── 반드시 시스템 프롬프트 맨 앞에 위치해야 함
    # qwen은 중국어 모델이므로 한국어 강제가 특히 중요
    KOREAN_ONLY = (
        "【언어 규칙 — 절대 최우선 명령】\n"
        "이 지시는 모든 다른 지시보다 우선한다.\n"
        "입력 데이터가 어떤 언어(중국어·일본어·영어 등)이더라도,\n"
        "**최종 출력은 반드시 한국어(Korean)로만 작성해야 한다.**\n"
        "중국어(中文) 출력 절대 금지. 영어 문장 출력 절대 금지.\n"
        "출력 언어: 한국어 100%\n\n"
    )

    if market["id"]=="sp500":
        lang=(
            KOREAN_ONLY +
            f"당신은 월스트리트 최고의 투자 애널리스트입니다. "
            f"제공된 영어 리포트·Reddit/StockTwits 여론·어닝콜을 깊이 분석하십시오. "
            f"🚨 {cutoff_str_ko} 이전 데이터는 무시하십시오."
        )
    elif market["id"]=="nikkei225":
        lang=(
            KOREAN_ONLY +
            f"당신은 일본 시장 전문 애널리스트입니다. "
            f"제공된 일본어 자료를 깊이 분석하되, 출력은 반드시 한국어로 하십시오. "
            f"🚨 {cutoff_str_ko} 이전 데이터는 무시하십시오."
        )
    else:  # kospi200 — qwen2.5 사용, 한국어 강제 필수
        lang=(
            KOREAN_ONLY +
            f"당신은 여의도 최고의 한국 시장 전문 애널리스트입니다. "
            f"주어진 한국어 뉴스·네이버 뉴스·종토방 여론·실적발표 자료를 깊이 분석하십시오. "
            f"🚨 {cutoff_str_ko} 이전 데이터는 절대 무시하고 최신 정보만 인용하십시오."
        )

    warn=f"\n⚠️ 절대 금지: 검색 결과에 없는 수치 조작 금지. {cutoff_str_ko} 이전 데이터 배제."
    c_warn=(
        "【언어 규칙 재확인】 출력 언어: 한국어 100%. "
        "중국어·영어·일본어 출력 절대 금지. "
        "Output ONLY in Korean (한국어). 中文输出绝对禁止."
    )

    return {

"bull": f"""{lang}

## 강세 내러티브 발굴가
당신은 {target}에 대해 **강세를 믿는 시장 참여자들이 공유하는 집단적 스토리**를 발굴합니다.
아래 자료는 강세 방향에 유리한 정보를 타깃 수집한 결과입니다.
뉴스를 나열하지 말고, "왜 이 기업이 앞으로 더 많이 팔게 될 것인가"의 믿음 구조를 드러내십시오.

## 📈 {target} 강세 내러티브

### ① 지배적 강세 내러티브 (집단 믿음 한 문장)
[강세 투자자들이 공유하는 핵심 스토리]

### ② 내러티브 인과 사슬
[믿음이 어떻게 연결되는가. 예: "AI 수요 급증 → HBM 공급 부족 → 독점 가격결정력 → 실적 급등"]

### ③ 🔒 이미 주가에 반영된 강세 요인 (Priced-In)
[수개월째 공유된 믿음. 현재 주가에 이미 녹아 있음. 2-3줄. 반영 근거 포함]

### ④ 🚀 아직 주가에 반영되지 않은 강세 촉매 (Not Yet Priced-In) ← 핵심
[시장이 아직 충분히 믿지 않아 미반영된 새로운 스토리. 각 촉매별:]
- 새로운 믿음 / 왜 시장이 과소평가하는가 / 실현 조건·트리거 / 실현 확률 [높음/중간/낮음]

### ⑤ 정량 근거 [강세 내러티브를 뒷받침하는 수치. 없으면 "정량 근거 부족"]

### ⑥ 집단 정서 [SNS·커뮤니티·어닝콜에서 확인되는 공유 믿음 수준]

### ⑦ 강세 내러티브 강도 [1-10]: 집단 믿음의 깊이와 확산 정도
### ⑧ 정량 정합성 [1-10]: 스토리가 숫자로 뒷받침되는가
### ⑨ 강세 내러티브 3줄 요약

출처(기관명, 날짜, URL) 명시.{warn}
🚨 반드시 한국어로만 출력할 것.""",


"neutral": f"""{lang}

## 중립 내러티브 발굴가
**{target}에 대해 시장이 방향을 정하지 못하는 이유**를 집단 믿음 관점에서 포착합니다.
"중립"은 믿음 부재가 아니라 **서로 충돌하는 두 강한 내러티브가 팽팽히 맞서는 상태**입니다.
아래 자료는 중립·균형 방향을 타깃 수집한 결과입니다.

## ➡️ {target} 중립 내러티브

### ① 충돌하는 두 집단 믿음
- 강세 측 믿음: [한 문장] / 약세 측 믿음: [한 문장]
- 왜 어느 쪽도 아직 지배적이지 않은가

### ② 내러티브 교착 구조
[왜 시장이 한 방향으로 확신을 형성하지 못하는가]

### ③ 🔒 이미 반영된 균형 요인 (Priced-In)
[2-3줄. 현재 박스권 주가를 설명하는 이미 알려진 불확실성]

### ④ 🚀 아직 반영되지 않은 방향 결정 요인 (Not Yet Priced-In) ← 핵심
[방향을 결정할 아직 시장이 소화하지 못한 정보. 각 요인별:]
- 요인 내용 / 왜 시장이 아직 이를 소화 못했는가 / 해소 트리거·시점

### ⑤ 정량 근거 / ⑥ 집단 정서

### ⑦ 중립 내러티브 강도 [1-10] / ⑧ 정량 정합성 [1-10]
### ⑨ 중립 내러티브 3줄 요약

출처 명시.{warn}
🚨 반드시 한국어로만 출력할 것.""",


"bear": f"""{lang}

## 약세 내러티브 발굴가
**{target}의 미래 매출·이익이 기대를 하회할 것이라는 집단적 불신의 구조**를 포착합니다.
아래 자료는 약세 방향에 유리한 정보를 타깃 수집한 결과입니다.
리스크 나열이 아닌, "왜 이 기업이 앞으로 덜 팔게 될 것인가"의 믿음 흐름을 드러내십시오.

## 📉 {target} 약세 내러티브

### ① 지배적 약세 내러티브 (집단 불신 한 문장)
[약세 투자자들이 공유하는 핵심 스토리]

### ② 내러티브 인과 사슬
[불신이 어떻게 연결되는가. 예: "경쟁사 진입 → 점유율 잠식 → ASP 하락 → 마진 압박 → 실적 하회"]

### ③ 🔒 이미 주가에 반영된 약세 요인 (Priced-In)
[2-3줄. 이미 하락에 녹아든 우려들. 추가 하락 압력 되기 어려움]

### ④ 🚀 아직 주가에 반영되지 않은 약세 리스크 (Not Yet Priced-In) ← 핵심
[시장이 아직 과소평가하는 새로운 불신의 씨앗. 각 리스크별:]
- 새로운 불신 / 왜 시장이 아직 이 리스크를 두려워하지 않는가 / 현실화 조건·트리거 / 현실화 확률 [높음/중간/낮음]

### ⑤ 정량 근거 / ⑥ 집단 정서

### ⑦ 약세 내러티브 강도 [1-10] / ⑧ 정량 정합성 [1-10]
### ⑨ 약세 내러티브 3줄 요약

출처 명시.{warn}
🚨 반드시 한국어로만 출력할 것.""",


"judge": f"""{lang}
{c_warn}

⚠️ 절대 명령: 아래 내용을 어떤 언어로 받더라도 반드시 **한국어**로만 출력하시오.

## 수석 내러티브 판정관

### 판정 철학
세 개의 독립 내러티브(각자 타깃 검색 기반)를 비교 평가합니다.
핵심 질문: **"향후 3개월간 어떤 집단 믿음이 가장 강하게 확산될 것인가?"**

- Priced-In 내러티브 → 현재 주가에 이미 반영, 추가 alpha 없음
- Not Yet Priced-In → 주가를 움직임, 판정의 핵심

⚠️ 금지: Priced-In 요인을 판정 주근거로 삼지 말 것 / 목표가 중심 판정 금지 / {cutoff_str_ko} 이전 자료 금지

## 핵심 요약
[4문장:
 1. 향후 3개월 가장 강하게 확산될 집단 믿음
 2. 그 믿음의 인과 사슬 (A→B→C→주가 방향)
 3. 경쟁 내러티브의 결정적 약점
 4. 이 판정을 뒤집을 역(逆)내러티브]

## ⚡ 최종 판정

### 가장 그럴듯한 내러티브: [강세 / 중립 / 약세]

### 세 내러티브 비교 평가
| 항목 | 강세 | 중립 | 약세 |
|---|---|---|---|
| 내러티브 강도 | /10 | /10 | /10 |
| 정량 정합성 | /10 | /10 | /10 |
| Priced-In 비중 | - | - | - |
| Not-Yet 질 | 높음/중간/낮음 | - | - |
| 집단 확산 속도 | 빠름/중간/느림 | - | - |

### 지배적 집단 믿음 (한 문장)
[향후 3개월 시장이 가장 강하게 공유할 경제적 믿음]

### Not Yet Priced-In 핵심 (판정 근거)
**선택 내러티브의 미반영 요인:**
[3-5개. 믿음 내용 / 왜 미반영인가 / [강세 촉매 / 약세 리스크]]

**탈락 내러티브의 Not Yet 주장이 약한 이유:**
[왜 경쟁 내러티브의 미반영 주장이 덜 설득력 있는가]

### 판정 이유
[왜 이 방향의 집단 믿음이 가장 강하게 확산될 것인가.
 내러티브 인과 사슬 완결성 + 정량 정합성 + 집단 확산 속도 함께 서술.]

### 현재 가격 기준 상황 [보조 정보. 짧게.]

### 핵심 근거 (Not Yet Priced-In 중심)
**근거 1:** / **근거 2:** / **근거 3:** / **근거 4:** / **근거 5:**

### 해당 내러티브 지지 애널리스트 평균 TP (참고용)
[확인된 경우만. n수 + implied upside/downside. 없으면 "확인 가능한 TP 부족"]

### 확률 분포
**강세장 (유의미한 상승): XX%**
**보합장 (박스권): XX%**
**약세장 (유의미한 하락): XX%**

### 내러티브 실현 트리거 (상위 3개)
아래 형식을 반드시 준수하시오:

**트리거 1: [이벤트명]**
- 예상 시점: [시점]
- 집단 믿음에 미치는 영향: [영향]

**트리거 2: [이벤트명]**
- 예상 시점: [시점]
- 집단 믿음에 미치는 영향: [영향]

**트리거 3: [이벤트명]**
- 예상 시점: [시점]
- 집단 믿음에 미치는 영향: [영향]""",
    }





# ─── HELPERS ──────────────────────────────────────────────────────────────────
def extract_winner(text):
    m=re.search(r"가장 그럴듯한 내러티브[^:：\n]*[：:]\s*\[?([^\]\n]+)\]?",text)
    if not m: return None
    raw=m.group(1).strip()
    if "강세" in raw and "약세" not in raw: return "bull"
    if "약세" in raw and "강세" not in raw: return "bear"
    if "중립" in raw or "보합" in raw: return "neutral"
    return None

def winner_from_probs(b,n,r):
    probs={"bull":b or 0,"neutral":n or 0,"bear":r or 0}
    return max(probs,key=probs.get)

def extract_tp(judge_text: str) -> str:
    """
    Judge 텍스트에서 애널리스트 평균 TP 추출.
    예: "평균 TP 369달러 (n=5), 현재가 대비 +21.6%"  →  "369달러 +21.6%"
        "평균 TP 220,000원 (n=3), 현재가 대비 +18.2%" →  "22만원 +18.2%"
    """
    if not judge_text: return ""
    # 확인 가능한 TP 부족 케이스
    if "확인 가능한 TP 부족" in judge_text or "TP 부족" in judge_text:
        return ""
    # 숫자+단위 패턴 (달러/원/엔)
    patterns = [
        r"평균\s*TP\s*[:\s]*([0-9,]+(?:\.[0-9]+)?(?:달러|원|엔|USD|\$)?).*?([+-]\d+\.?\d*%)",
        r"TP\s*[:\s]*([0-9,]+(?:\.[0-9]+)?(?:달러|원|엔|USD|\$)?).*?([+-]\d+\.?\d*%)",
        r"implied\s*(?:upside|downside)[:\s]*([+-]?\d+\.?\d*%)",
    ]
    import re as _re
    for pat in patterns:
        m = _re.search(pat, judge_text, _re.IGNORECASE)
        if m:
            groups = [g for g in m.groups() if g]
            return " / ".join(groups[:2]) if len(groups) >= 2 else groups[0] if groups else ""
    return ""

def extract_probs(text):
    b=re.search(r"강세장[^:\n*]*[:\*]+\s*(\d+)%",text)
    n=re.search(r"보합장[^:\n*]*[:\*]+\s*(\d+)%",text)
    r=re.search(r"약세장[^:\n*]*[:\*]+\s*(\d+)%",text)
    if b and n and r: return int(b.group(1)),int(n.group(1)),int(r.group(1))
    return None,None,None

def winner_badge(w): return {"bull":"📈 강세","neutral":"➡️ 중립","bear":"📉 약세"}.get(w,"❓")

def age_label(hours):
    if hours < 1:    return "방금"
    if hours < 2:    return "1시간 전"
    if hours < 24:   return f"{int(hours)}시간 전"
    days = int(hours / 24)
    if days == 1:    return "1일 전"
    if days < 7:     return f"{days}일 전"
    weeks = days // 7
    rem   = days % 7
    if weeks == 1 and rem == 0: return "1주일 전"
    if weeks == 1:              return f"1주 {rem}일 전"
    if rem == 0:                return f"{weeks}주일 전"
    return f"{weeks}주 {rem}일 전"


# ─── 주가 컨텍스트 수집 (분류 정확도 향상) ───────────────────────────────────
def _fetch_price_action_context(target: str, ticker_raw: str, market_id: str) -> str:
    """
    Priced-In vs Not-Yet-Priced-In 분류 정확도를 높이기 위한 주가 컨텍스트 수집.

    수집 항목:
    1) 현재가 + 최근 1개월·3개월·6개월 수익률 추이
    2) 52주 고저가 대비 현재 위치 (고점 대비 몇 % 아래?)
    3) 최근 주요 이벤트(실적발표, 제품 발표) 후 주가 반응
    4) 최근 애널리스트 컨센서스 변화 (목표가 상향/하향 추세)

    이 정보로 LLM이 "이미 주가에 반영됐는가"를 훨씬 정확하게 판단할 수 있음.
    """
    client = get_tavily()
    now_str = datetime.now().strftime("%Y-%m-%d")
    snippets = []

    # 한국: 네이버 API로 주가 컨텍스트 먼저 수집
    if market_id == "kospi200":
        naver_pa = search_naver_price_action(target, ticker_raw)
        for r in naver_pa:
            c = r.get("content","")[:400]; d = r.get("date","")
            if c: snippets.append(f"■ [네이버] {r['title']} ({d})\n  {r['url']}\n  {c}")
        # Tavily로 보완
        queries = [
            f"{target} 주가 등락률 1개월 3개월 수익률",
            f"{target} 52주 신고가 신저가 현재 주가 위치",
            f"{target} 실적발표 주가 반응 상승 하락",
            f"{target} 외국인 기관 수급 매수 매도",
        ]
    elif market_id == "nikkei225":
        queries = [
            f"{target} 株価 騰落率 1ヶ月 3ヶ月 推移",
            f"{target} 52週 高値 安値 現在株価 位置",
            f"{target} 決算発表後 株価 反応 上昇 下落",
            f"{target} アナリスト 目標株価 引き上げ 引き下げ 最近",
        ]
    else:  # sp500
        queries = [
            f"{target} stock price return 1 month 3 month 6 month performance",
            f"{target} 52 week high low current price position",
            f"{target} stock reaction after earnings release price move",
            f"{target} analyst price target upgrade downgrade consensus change recent",
        ]

    seen = set()
    for q in queries:
        try:
            resp = client.search(q, max_results=3, search_depth="basic")
            for r in resp.get("results", []):
                url = r.get("url","")
                if url in seen: continue
                seen.add(url)
                c = (r.get("content") or "")[:400]
                d = (r.get("published_date") or "")[:10]
                if c:
                    snippets.append(f"■ {r.get('title','')} ({d})\n  {url}\n  {c}")
        except:
            pass

    if not snippets:
        return f"[{target} 주가 컨텍스트 수집 실패 — 현재가 기준 분류 불확실성 높음]"

    header = (
        f"【{target} 주가 컨텍스트 ({now_str} 기준)】\n"
        f"⚠️ 아래 주가 흐름을 반드시 참고하여 각 내러티브의 반영 여부를 판단하시오:\n"
        f"- 주가가 이미 많이 올랐다면 → 좋은 소식은 이미 반영됐을 가능성 높음\n"
        f"- 실적 발표 후 주가가 반응하지 않았다면 → 이미 컨센서스에 반영됐던 것\n"
        f"- 최근 목표가 상향이 잇따른다면 → 해당 내러티브는 이미 가격에 녹아드는 중\n\n"
    )
    return header + "\n\n".join(snippets[:8])


# ─── CORE ANALYSIS ────────────────────────────────────────────────────────────
def _run_analysis_core(target_id, target_label, market, stock, prompts):
    """
    새 구조 (4단계):
    Phase 1: 방향별 독립 타깃 검색 → 내러티브 직접 추출 (강세/중립/약세 각각)
    Phase 2: Judge가 세 내러티브 비교 평가 → 최종 판정
    (비판 에이전트 제거 — 내러티브 자체에 Priced-In/Not 판단 포함)
    """
    results = {}
    target_short = stock[1] if stock else market["index"]
    sector = stock[2] if stock else ""
    ticker_raw = stock[0] if stock else ""

    # ── 공통: 주가 컨텍스트 수집 (Priced-In 판단 보조) ──────────────────────
    update_progress(target_id, 0.05, "📊 주가 컨텍스트 수집 중...")
    price_action_ctx = _fetch_price_action_context(target_short, ticker_raw, market["id"])

    today = datetime.now().strftime("%Y년 %m월 %d일")

    # ── Phase 1: 방향별 독립 타깃 검색 → 내러티브 추출 ──────────────────────
    direction_map = [
        ("bull",    "강세", 0.12),
        ("neutral", "중립", 0.38),
        ("bear",    "약세", 0.62),
    ]

    for agent, dir_label, pct in direction_map:
        update_progress(target_id, pct, f"🔍 {dir_label} 방향 타깃 검색 + 내러티브 구성 중...")
        try:
            # 방향별 타깃 검색 (강세는 강세 자료, 약세는 약세 자료를 수집)
            targeted_data = combined_search(
                target_short, agent, market["index"],
                sector=sector, ticker_raw=ticker_raw, market_id=market["id"]
            )
            update_progress(target_id, pct + 0.10,
                            f"🤖 {AGENT_LABELS[agent]} — 내러티브 발굴 중...")

            uc = (
                f"오늘({today}) 기준 {target_label}의 {dir_label} 방향을 지지하는 자료를 타깃 수집했습니다.\n\n"
                f"【주가 컨텍스트 — Priced-In 판단 보조】\n{price_action_ctx}\n\n"
                f"{'='*50}\n\n"
                f"【{dir_label} 방향 타깃 수집 자료】\n{targeted_data}\n\n"
                f"위 자료를 바탕으로 {dir_label} 내러티브를 발굴하십시오.\n"
                f"핵심: ④항 'Not Yet Priced-In'에 집중하십시오 — 이것이 향후 주가를 움직입니다."
            )
            results[agent] = strip_duplicate_translation(
                call_llm(prompts[agent], uc, market_id=market["id"]),
                market_id=market["id"],
            )
        except Exception as e:
            results[agent] = f"⚠️ 오류: {e}"

    # ── Phase 2: Judge — 세 내러티브 비교 판정 ───────────────────────────────
    update_progress(target_id, 0.88, "📡 현재 주가 조회 + 최종 판정 중...")
    try:
        price_ctx = fetch_current_price(target_short, ticker_raw, market["id"])
        judge_input = (
            f"【실시간 현재가 — 참고 정보】\n{price_ctx}\n\n"
            f"{'='*60}\n\n"
            f"아래는 각 방향별 타깃 검색을 통해 독립적으로 발굴된 세 개의 내러티브입니다.\n"
            f"각 내러티브는 Priced-In(이미 반영)과 Not Yet Priced-In(미반영)을 자체적으로 구분하고 있습니다.\n\n"
            + "\n\n".join(
                f"{'='*40}\n[{AGENT_LABELS[a]}]:\n{results.get(a, '')}"
                for a in ["bull", "neutral", "bear"]
            )
            + f"\n\n{'='*60}\n\n"
            f"세 내러티브를 비교 평가하여 최종 판정을 내리십시오.\n"
            f"판단의 핵심: 어느 방향의 'Not Yet Priced-In' 내러티브가 가장 설득력 있고 실현 가능성이 높은가?"
        )
        results["judge"] = strip_duplicate_translation(
            call_llm(prompts["judge"], judge_input, max_tokens=8000, market_id=market["id"]),
            market_id=market["id"],
        )
    except Exception as e:
        results["judge"] = f"⚠️ 오류: {e}"

    update_progress(target_id, 0.98, "✅ 저장 중...")
    bp, np_, rp = extract_probs(results.get("judge", ""))
    bp = bp or 50; np_ = np_ or 30; rp = rp or 20
    winner = winner_from_probs(bp, np_, rp)
    if bp == 50 and np_ == 30 and rp == 20:
        winner = extract_winner(results.get("judge", "")) or "neutral"
    tp_str = extract_tp(results.get("judge",""))
    cache_set(target_id, market["id"], target_label, results, winner,
              bull_prob=bp, neutral_prob=np_, bear_prob=rp, status="done", consensus_tp=tp_str)
    update_progress(target_id, 1.0, "✅ 분석 완료!")
    return results, winner


# ─── DISPLAY ──────────────────────────────────────────────────────────────────
def _extract_section(text: str, heading: str) -> str:
    """Judge 텍스트에서 ### 헤딩 기준으로 섹션 추출."""
    if not text or not heading: return ""
    escaped = re.escape(heading)
    pattern = "###[^\n]*" + escaped + "[^\n]*\n(.*?)(?=\n###|$)"
    m = re.search(pattern, text, re.DOTALL)
    return m.group(1).strip() if m else ""

def display_results(results, winner, cached_at=None):
    judge_text = results.get("judge", "") or ""
    w_map = {
        "bull":    ("📈 강세", "#00e87a", "#e6fff4"),
        "neutral": ("➡️ 중립", "#f5c518", "#fffde6"),
        "bear":    ("📉 약세", "#ff3c4e", "#fff0f2"),
    }
    w_label, w_color, w_bg = w_map.get(winner, ("❓", "#888", "#f8f9fc"))
    bp, np_, rp = extract_probs(judge_text)
    bp = bp or 0; np_ = np_ or 0; rp = rp or 0

    # ── 메타 정보 ─────────────────────────────────────────────────────────────
    if cached_at:
        at    = datetime.fromisoformat(cached_at.replace("Z","")).replace(tzinfo=timezone.utc)
        age_h = (datetime.now(timezone.utc)-at).total_seconds()/3600
        st.caption(
            f"🕐 분석일시: {at.strftime('%Y-%m-%d %H:%M')} UTC  ·  "
            f"{age_label(age_h)} 분석  ·  ⚠️ AI 생성 콘텐츠 · 투자 조언 아님"
        )

    # ══ 1. 판정 배너 ═════════════════════════════════════════════════════════
    st.markdown(f"""
<div style='background:linear-gradient(135deg,{w_color}28,{w_bg});
border:2px solid {w_color};border-radius:16px;padding:22px 28px;margin:4px 0 20px'>
  <div style='display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:16px'>
    <div>
      <div style='color:#6b7a9e;font-size:11px;letter-spacing:2px;margin-bottom:6px'>⚡ 향후 3개월 내러티브 판정</div>
      <div style='color:{w_color};font-size:36px;font-weight:900;line-height:1'>{w_label}</div>
    </div>
    <div style='display:flex;gap:24px'>
      <div style='text-align:center'>
        <div style='color:#00e87a;font-size:22px;font-weight:800'>↑{bp}%</div>
        <div style='color:#6b7a9e;font-size:10px;margin-top:2px'>강세장</div>
      </div>
      <div style='text-align:center'>
        <div style='color:#f5c518;font-size:22px;font-weight:800'>→{np_}%</div>
        <div style='color:#6b7a9e;font-size:10px;margin-top:2px'>보합장</div>
      </div>
      <div style='text-align:center'>
        <div style='color:#ff3c4e;font-size:22px;font-weight:800'>↓{rp}%</div>
        <div style='color:#6b7a9e;font-size:10px;margin-top:2px'>약세장</div>
      </div>
    </div>
  </div>
  <div style='margin-top:14px;display:flex;height:8px;border-radius:4px;overflow:hidden;gap:2px'>
    <div style='width:{bp}%;background:#00e87a;border-radius:4px 0 0 4px'></div>
    <div style='width:{np_}%;background:#f5c518'></div>
    <div style='width:{rp}%;background:#ff3c4e;border-radius:0 4px 4px 0'></div>
  </div>
</div>""", unsafe_allow_html=True)

    # ══ 2. 핵심 요약 + 우측 정보 패널 ═══════════════════════════════════════
    summary = _extract_section(judge_text, "핵심 요약")
    belief  = _extract_section(judge_text, "지배적 집단 믿음")
    price   = _extract_section(judge_text, "현재 가격 기준 상황")
    tp_sec  = _extract_section(judge_text, "해당 내러티브 지지 애널리스트 평균 TP")
    reason  = _extract_section(judge_text, "판정 이유")

    col_l, col_r = st.columns([3, 2])

    with col_l:
        if summary:
            st.markdown("**💡 핵심 요약**")
            st.markdown(
                f"<div style='background:#f8f9fc;border-left:4px solid {w_color};"
                f"border-radius:0 10px 10px 0;padding:14px 16px;"
                f"color:#2d3a5e;font-size:13.5px;line-height:1.9'>{summary}</div>",
                unsafe_allow_html=True)

        if belief:
            st.markdown("")
            st.markdown("**🧠 시장의 지배적 집단 믿음**")
            st.markdown(
                f"<div style='background:#fff9e6;border:1.5px solid #f5c518;"
                f"border-radius:10px;padding:12px 16px;"
                f"color:#2d3a5e;font-size:13.5px;font-style:italic;line-height:1.7'>"
                f"&#8220;{belief}&#8221;</div>",
                unsafe_allow_html=True)

    with col_r:
        if price:
            st.markdown("**📍 현재가 기준 상황**")
            st.markdown(
                f"<div style='background:#fff;border:1.5px solid #e2e6ef;"
                f"border-radius:10px;padding:12px 16px;"
                f"color:#374151;font-size:13px;line-height:1.7'>{price}</div>",
                unsafe_allow_html=True)
            st.markdown("")

        if tp_sec and "부족" not in tp_sec:
            tp_color = "#00e87a" if "+" in tp_sec else "#ff3c4e" if "-" in tp_sec else "#6b7a9e"
            st.markdown("**🎯 컨센서스 목표주가 (TP)**")
            st.markdown(
                f"<div style='background:#fff;border:1.5px solid {tp_color}88;"
                f"border-radius:10px;padding:12px 16px;"
                f"color:#374151;font-size:13px;line-height:1.7'>"
                f"<span style='color:{tp_color};font-weight:700'>{tp_sec}</span></div>",
                unsafe_allow_html=True)
            st.markdown("")

        if reason:
            st.markdown("**📝 판정 이유**")
            sentences = [s.strip() for s in reason.replace("\n"," ").split(". ") if s.strip()]
            brief = ". ".join(sentences[:2]).strip()
            if brief and not brief.endswith("."): brief += "."
            st.markdown(
                f"<div style='background:#fff;border:1.5px solid #e2e6ef;"
                f"border-radius:10px;padding:12px 16px;"
                f"color:#374151;font-size:13px;line-height:1.7'>{brief}</div>",
                unsafe_allow_html=True)

    # ══ 3. Not Yet Priced-In 핵심 요인 ═══════════════════════════════════════
    nyt = _extract_section(judge_text, "Not Yet Priced-In 핵심")
    if not nyt:
        nyt = _extract_section(judge_text, "Not Yet Priced")
    if nyt:
        st.markdown("")
        st.markdown("**🚀 아직 주가에 반영되지 않은 핵심 요인**")
        nyt_main = nyt.split("탈락 내러티브")[0].strip()
        st.markdown(
            f"<div style='background:linear-gradient(135deg,#e6fff4,#f0fff8);"
            f"border:1.5px solid #00e87a;border-radius:10px;padding:16px 18px;"
            f"color:#1a3a2a;font-size:13.5px;line-height:1.85'>{nyt_main}</div>",
            unsafe_allow_html=True)

    # ══ 4. 트리거 카드 ════════════════════════════════════════════════════════
    trigger_text = _extract_section(judge_text, "내러티브 실현 트리거")
    if trigger_text:
        st.markdown("")
        st.markdown("**⏰ 주시해야 할 실현 트리거**")
        blocks = re.split(r"\*\*트리거\s*\d+\s*:", trigger_text)
        valid_blocks = [b.strip() for b in blocks[1:4] if b.strip()]
        if valid_blocks:
            cols_t = st.columns(len(valid_blocks))
            icons = ["🔵","🟡","🔴"]
            for i, (col_t, block) in enumerate(zip(cols_t, valid_blocks)):
                title = block.split("**")[0].strip().rstrip("*").strip()
                시점_m = re.search("예상 시점[^\n*]{0,3}([^\n*-]+)", block)
                시점   = 시점_m.group(1).strip() if 시점_m else ""
                영향_m = re.search("집단 믿음에 미치는 영향[^\n*]{0,3}([^\n*-]+)", block)
                영향   = 영향_m.group(1).strip()[:90] if 영향_m else ""
                col_t.markdown(
                    f"<div style='background:#fff;border:1.5px solid #e2e6ef;"
                    f"border-radius:10px;padding:14px;min-height:120px'>"
                    f"<div style='font-size:10px;color:#6b7a9e;font-weight:700;letter-spacing:1px;margin-bottom:6px'>"
                    f"{icons[i]} 트리거 {i+1}</div>"
                    f"<div style='color:#2d3a5e;font-size:13px;font-weight:700;line-height:1.4;margin-bottom:8px'>{title}</div>"
                    + (f"<div style='color:#4a7cf7;font-size:11px;margin-bottom:4px'>📅 {시점}</div>" if 시점 else "")
                    + (f"<div style='color:#6b7a9e;font-size:11px;line-height:1.5'>{영향}</div>" if 영향 else "")
                    + "</div>",
                    unsafe_allow_html=True)
        else:
            st.markdown(trigger_text)

    # ══ 5. 상세 내러티브 탭 ══════════════════════════════════════════════════
    st.markdown("---")
    st.markdown("**📖 상세 내러티브 보기**")
    tab_bull, tab_neutral, tab_bear, tab_judge = st.tabs(
        ["📈 강세 내러티브", "➡️ 중립 내러티브", "📉 약세 내러티브", "📋 최종 판정 전문"]
    )
    with tab_bull:
        st.markdown(results.get("bull") or "결과 없음")
    with tab_neutral:
        st.markdown(results.get("neutral") or "결과 없음")
    with tab_bear:
        st.markdown(results.get("bear") or "결과 없음")
    with tab_judge:
        st.markdown(judge_text or "결과 없음")


def display_leaderboard():
    rows=load_leaderboard()
    done_rows=[r for r in rows if r.get("status")!="running"]
    running_rows=[r for r in rows if r.get("status")=="running"]

    st.markdown("### 📊 분석 현황")
    m1,m2,m3 = st.columns(3)
    m1.metric("완료된 분석",f"{len(done_rows)}개")
    m2.metric("진행 중",f"{len(running_rows)}개")
    m3.metric("전체 캐시",f"{len(rows)}개")

    if not rows:
        st.caption("아직 분석 없음.")
        return None

    # ── 접기/펼치기 토글 버튼 (session_state 직접 제어) ───────────────────────
    lb_open = st.session_state.get("lb_open", True)
    toggle_label = "▲ 랭킹 접기" if lb_open else "▼ 랭킹 펼치기"
    if st.button(toggle_label, key="lb_toggle"):
        st.session_state["lb_open"] = not lb_open
        st.rerun()

    if not lb_open:
        return st.session_state.get("lb_selected_id")

    # ── 랭킹 테이블 ───────────────────────────────────────────────────────────
    st.caption("💡 기업명을 클릭하면 분석 결과가 아래에 표시됩니다.")
    mf = {"kospi200":"🇰🇷","sp500":"🇺🇸","nikkei225":"🇯🇵"}

    h1,h2,h3,h4,h5,h6,h7 = st.columns([0.4,0.3,2.0,1.0,2.2,1.0,0.7])
    for h,t in zip([h1,h2,h3,h4,h5,h6,h7],["순위","시장","종목/지수","판정","확률 분포","컨센서스 TP","분석"]):
        h.markdown(f"<span style='color:#4a5568;font-size:11px'>{t}</span>",unsafe_allow_html=True)
    st.markdown("<hr style='margin:4px 0;border-color:#e2e6ef'>",unsafe_allow_html=True)

    selected_id = st.session_state.get("lb_selected_id")

    for rank,row in enumerate(rows,1):
        tid        = row.get("target_id","")
        bp         = row.get("bull_prob") or 0
        np_        = row.get("neutral_prob") or 0
        rp         = row.get("bear_prob") or 0
        w          = row.get("winner","")
        flag       = mf.get(row.get("market_id",""),"")
        is_running = row.get("status") == "running"
        is_selected= (selected_id == tid)
        rc         = "#00e87a" if bp>=55 else "#f5c518" if bp>=45 else "#ff3c4e"
        tp_raw     = row.get("consensus_tp") or ""

        c1,c2,c3,c4,c5,c6,c7 = st.columns([0.4,0.3,2.0,1.0,2.2,1.0,0.7])

        c1.markdown(
            f"<div style='color:{rc};font-weight:900;font-size:14px;padding-top:8px'>#{rank}</div>",
            unsafe_allow_html=True)
        c2.markdown(
            f"<div style='font-size:18px;padding-top:6px'>{flag}</div>",
            unsafe_allow_html=True)

        # 종목명 버튼 — winner가 있으면(=분석 완료) 버튼으로 표시
        if not is_running and row.get("winner"):
            btn_label = f"▶ {row['target_label']}" if is_selected else row['target_label']
            if c3.button(btn_label, key=f"lb_{tid}", use_container_width=True,
                         type="primary" if is_selected else "secondary"):
                if is_selected:
                    st.session_state.pop("lb_selected_id", None)
                else:
                    st.session_state["lb_selected_id"] = tid
                st.rerun()
        else:
            c3.markdown(
                f"<div style='color:#4a5568;font-size:13px;padding-top:8px'>{row['target_label']}</div>",
                unsafe_allow_html=True)

        if is_running:
            c4.markdown("🔄 분석중")
            c5.markdown(
                "<div style='color:#6b7a9e;font-size:11px;padding-top:8px'>진행 중...</div>",
                unsafe_allow_html=True)
            c6.markdown("")
        else:
            c4.markdown(winner_badge(w))
            bar = (
                f"<div style='display:flex;gap:2px;align-items:center;margin-top:6px'>"
                f"<div style='width:{bp}%;height:8px;background:#00e87a;border-radius:2px 0 0 2px'></div>"
                f"<div style='width:{np_}%;height:8px;background:#f5c518'></div>"
                f"<div style='width:{rp}%;height:8px;background:#ff3c4e;border-radius:0 2px 2px 0'></div></div>"
                f"<div style='display:flex;gap:8px;font-size:9px;color:#6b7a9e;margin-top:2px'>"
                f"<span style='color:#00e87a'>↑{bp}%</span>"
                f"<span style='color:#f5c518'>→{np_}%</span>"
                f"<span style='color:#ff3c4e'>↓{rp}%</span></div>"
            )
            c5.markdown(bar, unsafe_allow_html=True)

            # TP 컬럼: upside는 색상으로 강조
            if tp_raw:
                tp_color = "#00e87a" if "+" in tp_raw else "#ff3c4e" if "-" in tp_raw else "#6b7a9e"
                # TP값과 upside를 분리해서 표시
                parts = tp_raw.split(" / ")
                tp_val = parts[0] if parts else tp_raw
                tp_upside = parts[1] if len(parts) > 1 else ""
                tp_html = (
                    f"<div style='font-size:11px;font-weight:600;color:#2d3a5e;padding-top:4px'>{tp_val}</div>"
                    f"<div style='font-size:11px;color:{tp_color};font-weight:700'>{tp_upside}</div>"
                ) if tp_upside else (
                    f"<div style='font-size:11px;color:{tp_color};font-weight:600;padding-top:6px'>{tp_val}</div>"
                )
                c6.markdown(tp_html, unsafe_allow_html=True)
            else:
                c6.markdown(
                    "<div style='color:#d0d5e0;font-size:11px;padding-top:8px'>—</div>",
                    unsafe_allow_html=True)

        c7.markdown(
            f"<div style='color:#374151;font-size:10px;padding-top:8px'>"
            f"{'진행중' if is_running else age_label(row['age_hours'])}</div>",
            unsafe_allow_html=True)
        st.markdown("<hr style='margin:2px 0;border-color:#f0f0f0'>",unsafe_allow_html=True)

    return st.session_state.get("lb_selected_id")


# ─── MAIN ─────────────────────────────────────────────────────────────────────
def main():
    col_title,col_info=st.columns([5,1])
    with col_title:
        qw=get_ollama_model('kospi200'); ll=get_ollama_model('sp500'); gm=get_ollama_model('nikkei225')
        st.markdown(f"""<h1 style='background:linear-gradient(90deg,#4fc3f7,#00e87a,#f5c518,#ff3c4e,#e040fb);-webkit-background-clip:text;-webkit-text-fill-color:transparent;font-size:26px;margin:0'>
        ⚡ [시장/종목] 내러티브 앤 넘버스 수집 및 분석</h1>
        <p style='color:#4a5568;font-size:11px;letter-spacing:2px;margin:2px 0 0'>7-AGENT AI · 향후 3개월 판정 · 🇰🇷{qw} / 🇺🇸{ll} / 🇯🇵{gm}</p>""",unsafe_allow_html=True)
    with col_info:
        ollama_url=get_ollama_url()
        st.markdown(f"<div style='color:#374151;font-size:10px;text-align:right;margin-top:8px'>🖥 {ollama_url[:30]}...</div>",unsafe_allow_html=True)
        if st.button("새로고침",use_container_width=True): st.session_state.clear(); st.rerun()
    st.markdown("---")
    lb_selected = display_leaderboard()

    # ── 랭킹에서 선택된 종목 결과를 랭킹 바로 아래에 표시 ────────────────────
    if lb_selected:
        lb_cached = cache_get(lb_selected)
        if lb_cached and lb_cached.get("results") and lb_cached.get("status") != "running":
            bp_l  = lb_cached.get("bull_prob") or 50
            np_l  = lb_cached.get("neutral_prob") or 30
            rp_l  = lb_cached.get("bear_prob") or 20
            label_l = lb_cached.get("target_label", lb_selected)
            st.markdown(f"""
            <div style='background:#f0f7ff;border:2px solid #4a7cf7;border-radius:12px;
            padding:12px 18px;margin:8px 0'>
            <span style='color:#2d3a5e;font-size:14px;font-weight:700'>
            📊 {label_l} 분석 결과</span>
            <span style='color:#6b7a9e;font-size:11px;margin-left:10px'>
            (랭킹에서 선택됨 · 다른 기업 클릭 시 교체)</span>
            </div>""", unsafe_allow_html=True)
            display_results(
                lb_cached["results"],
                winner_from_probs(bp_l, np_l, rp_l),
                lb_cached.get("analyzed_at"),
            )

    st.markdown("---")
    st.markdown("### STEP 1 · 시장 선택")
    mc=st.radio("",list(MARKETS.keys()),horizontal=True,label_visibility="collapsed")
    market=MARKETS[mc]
    st.markdown("### STEP 2 · 분석 대상")
    stocks=STOCKS[market["id"]]; indices=INDEX_OPTIONS.get(market["id"],[])
    index_options=[f"📊 {label} Index" for _,label in indices]
    stock_options=[f"{n} · {t} ({s})" for t,n,s in stocks]
    options=index_options+stock_options
    choice=st.selectbox(f"선택 가능한 대상: 지수 {len(indices)}개 + 종목 {len(stocks)}개",options,label_visibility="collapsed")
    selected_market=market.copy()
    if choice in index_options:
        idx=index_options.index(choice); index_code,index_label=indices[idx]
        stock=None; target_id=f"{market['id']}_{index_code}"; target_label=f"{market['flag']} {index_label}"
        selected_market["index"]=index_label
    else:
        idx=stock_options.index(choice); stock=stocks[idx]
        target_id=f"{market['id']}_{stock[0]}"; target_label=f"{stock[1]} ({stock[0]})"
    st.markdown(f"**선택:** {target_label}")
    cached=cache_get(target_id)
    col_a,col_b=st.columns([3,1])
    if cached:
        status=cached.get("status","done")
        if status=="running":
            at=datetime.fromisoformat(cached["analyzed_at"].replace("Z","")).replace(tzinfo=timezone.utc)
            elapsed=int((datetime.now(timezone.utc)-at).total_seconds()/60)
            pct=float(cached.get("progress") or 0.0); msg=cached.get("status_msg") or "분석 준비 중..."
            st.info(f"⏳ **{target_label}** 백그라운드 분석 중 ({elapsed}분 경과) — 브라우저 꺼도 계속 진행됩니다")
            st.progress(pct,text=msg)
            cr,cc=st.columns([1,1])
            with cr:
                if st.button("🔄 새로고침",use_container_width=True): st.rerun()
            with cc:
                if elapsed>30 and st.button("⚠️ 재시작",use_container_width=True): cache_delete(target_id); st.rerun()
            import time; time.sleep(3); st.rerun()
        else:
            if cached.get("results") and st.session_state.get("loaded_target_id")!=target_id:
                bp=cached.get("bull_prob") or 50; np_=cached.get("neutral_prob") or 30; rp=cached.get("bear_prob") or 20
                st.session_state.update({"res_results":cached["results"],"res_winner":winner_from_probs(bp,np_,rp),"res_cached_at":cached["analyzed_at"],"show_results":True,"loaded_target_id":target_id})
                st.rerun()
            at=datetime.fromisoformat(cached["analyzed_at"].replace("Z","")).replace(tzinfo=timezone.utc)
            age_h=(datetime.now(timezone.utc)-at).total_seconds()/3600; remaining=CACHE_TTL_HOURS-age_h
            remain_label = f"{int(remaining/24)}일 후 만료" if remaining > 48 else f"{int(remaining)}시간 후 만료"
            with col_a:
                if st.button(f"🗄 캐시 불러오기 ({age_label(age_h)} 분석 · {remain_label})",type="primary",use_container_width=True):
                    bp=cached.get("bull_prob") or 50; np_=cached.get("neutral_prob") or 30; rp=cached.get("bear_prob") or 20
                    st.session_state.update({"res_results":cached["results"],"res_winner":winner_from_probs(bp,np_,rp),"res_cached_at":cached["analyzed_at"],"show_results":True,"loaded_target_id":target_id})
                    st.rerun()
            with col_b:
                if st.button("🗑 재분석",use_container_width=True):
                    cache_delete(target_id); st.session_state.pop("show_results",None); st.session_state.pop("loaded_target_id",None)
                    st.success("캐시 삭제됨."); st.rerun()
    else:
        with col_a:
            if st.button(f"▶ {target_label} 분석 시작",type="primary",use_container_width=True):
                st.session_state.pop("show_results",None); st.session_state.pop("loaded_target_id",None)
                prompts=build_system_prompts(selected_market,stock)
                cache_set_running(target_id,selected_market["id"],target_label)
                def _bg_task():
                    _BG_KEYS[target_id]=""
                    try: _run_analysis_core(target_id,target_label,selected_market,stock,prompts)
                    except Exception as e:
                        print(f"백그라운드 오류 [{target_id}]: {e}")
                        try: cache_set(target_id,selected_market["id"],target_label,{},"unknown",status="done")
                        except: pass
                    finally: _BG_KEYS.pop(target_id,None)
                threading.Thread(target=_bg_task,daemon=True).start()
                st.rerun()
    if st.session_state.get("show_results"):
        st.markdown("---")
        display_results(st.session_state["res_results"],st.session_state["res_winner"],st.session_state.get("res_cached_at"))
    st.markdown("---")
    st.caption("AI 생성 콘텐츠 · 투자 조언 아님 · 연구 목적 전용")

if __name__ == "__main__":
    main()
