import streamlit as st
import anthropic
import re
import threading
from datetime import datetime, timezone, timedelta
from supabase import create_client
from tavily import TavilyClient
from exa_py import Exa

# ─── PAGE CONFIG ───────────────────────────────────────────────────────────────
st.set_page_config(page_title="시장 방향 판정 엔진", page_icon="⚡", layout="wide")

st.markdown("""
<style>
body, .stApp { background: #f8f9fc !important; color: #1a1a2e !important; }
.block-container { padding-top: 1.5rem; max-width: 1100px; }
/* 버튼 */
.stButton > button {
    background: #fff; border: 1.5px solid #d0d5e0;
    color: #2d3a5e; border-radius: 7px; font-size: 13px;
    font-weight: 500; transition: all 0.18s;
}
.stButton > button:hover {
    border-color: #4a7cf7; color: #4a7cf7; background: #eef2ff;
}
/* 헤딩 */
h1, h2, h3 { color: #1a1a2e !important; }
/* 확장 패널 */
.stExpander {
    background: #fff !important;
    border: 1.5px solid #e2e6ef !important;
    border-radius: 8px !important;
}
div[data-testid="stExpander"] > div { background: #fff !important; }
div[data-testid="stExpander"] summary { color: #2d3a5e !important; font-weight: 600; }
/* 입력창 */
.stTextInput > div > div > input {
    background: #fff; border: 1.5px solid #d0d5e0;
    color: #1a1a2e; border-radius: 7px;
}
/* 셀렉트박스 */
.stSelectbox > div > div {
    background: #fff !important; color: #1a1a2e !important;
    border: 1.5px solid #d0d5e0 !important; border-radius: 7px !important;
}
/* 메트릭 카드 */
div[data-testid="metric-container"] {
    background: #fff; border: 1.5px solid #e2e6ef;
    border-radius: 10px; padding: 14px;
    box-shadow: 0 1px 4px rgba(0,0,0,0.06);
}
div[data-testid="metric-container"] label { color: #6b7a9e !important; }
div[data-testid="metric-container"] div[data-testid="stMetricValue"] { color: #1a1a2e !important; }
/* 라디오 */
.stRadio label { color: #2d3a5e !important; }
/* 캡션 */
.stCaption { color: #8892ab !important; }
/* 성공/정보 박스 */
div[data-testid="stAlert"] { border-radius: 8px !important; }
/* Progress bar */
.stProgress > div > div { background: #4a7cf7 !important; }
/* 구분선 */
hr { border-color: #e2e6ef !important; }
/* 사이드바 없을 때 여백 */
section[data-testid="stSidebar"] { display: none; }
</style>
""", unsafe_allow_html=True)

# ─── CONSTANTS ─────────────────────────────────────────────────────────────────
CACHE_TTL_HOURS = 48

MARKETS = {
    "🇰🇷 KOSPI 200": {
        "id": "kospi200", "flag": "🇰🇷", "color": "#4fc3f7",
        "index": "KOSPI 200", "region": "한국",
        "central_bank": "한국은행(BOK)", "currency": "원화(KRW)",
        "analysts": "국내외 증권사 애널리스트",
    },
    "🇺🇸 S&P 500": {
        "id": "sp500", "flag": "🇺🇸", "color": "#00e87a",
        "index": "S&P 500", "region": "미국",
        "central_bank": "연준(Fed)", "currency": "달러(USD)",
        "analysts": "월스트리트 애널리스트",
    },
    "🇯🇵 닛케이 225": {
        "id": "nikkei225", "flag": "🇯🇵", "color": "#ff7043",
        "index": "닛케이 225", "region": "일본",
        "central_bank": "일본은행(BOJ)", "currency": "엔화(JPY)",
        "analysts": "일본 및 글로벌 증권사 애널리스트",
    },
}

STOCKS = {
    "kospi200": [
        ("005930","삼성전자","반도체"), ("000660","SK하이닉스","반도체"),
        ("207940","삼성바이오로직스","바이오"), ("005380","현대자동차","자동차"),
        ("000270","기아","자동차"), ("051910","LG화학","화학"),
        ("035420","NAVER","인터넷"), ("035720","카카오","인터넷"),
        ("068270","셀트리온","바이오"), ("105560","KB금융","금융"),
        ("055550","신한지주","금융"), ("032830","삼성생명","보험"),
        ("012330","현대모비스","자동차부품"), ("003550","LG","지주사"),
        ("066570","LG전자","전자"), ("028260","삼성물산","건설"),
        ("096770","SK이노베이션","에너지"), ("034730","SK","지주사"),
        ("003490","대한항공","항공"), ("009830","한화솔루션","에너지"),
    ],
    "sp500": [
        ("AAPL","Apple","Technology"), ("MSFT","Microsoft","Technology"),
        ("NVDA","NVIDIA","Semiconductors"), ("AMZN","Amazon","Consumer/Cloud"),
        ("GOOGL","Alphabet","Internet"), ("META","Meta","Social Media"),
        ("TSLA","Tesla","EV/Energy"), ("BRK.B","Berkshire","Financials"),
        ("JPM","JPMorgan","Banking"), ("V","Visa","Payments"),
        ("UNH","UnitedHealth","Healthcare"), ("XOM","ExxonMobil","Energy"),
        ("JNJ","J&J","Healthcare"), ("WMT","Walmart","Retail"),
        ("MA","Mastercard","Payments"), ("PG","P&G","Consumer"),
        ("HD","Home Depot","Retail"), ("BAC","Bank of America","Banking"),
        ("AVGO","Broadcom","Semiconductors"), ("LLY","Eli Lilly","Pharma"),
    ],
    "nikkei225": [
        ("7203","トヨタ自動車","自動車"), ("6758","ソニーグループ","電子"),
        ("9984","ソフトバンクG","通信/投資"), ("8306","三菱UFJ FG","銀行"),
        ("6861","キーエンス","電子機器"), ("6367","ダイキン工業","空調"),
        ("4063","信越化学","化学"), ("7974","任天堂","ゲーム"),
        ("6501","日立製作所","電機"), ("6702","富士通","IT"),
        ("8035","東京エレクトロン","半導体装置"), ("7267","ホンダ","自動車"),
        ("2914","日本たばこ","食品"), ("9432","NTT","通信"),
        ("8411","みずほFG","銀行"), ("4502","武田薬品","製薬"),
        ("6971","京セラ","電子部品"), ("7751","キヤノン","光学"),
        ("6954","ファナック","ロボット"), ("3382","セブン&アイ","小売"),
    ],
}

AGENT_LABELS = {
    "bull":"📈 강세 애널리스트", "neutral":"➡️ 중립 애널리스트", "bear":"📉 약세 애널리스트",
    "bull_critic":"🔥 강세 비판", "neutral_critic":"🔥 중립 비판", "bear_critic":"🔥 약세 비판",
    "judge":"⚡ 최종 판정자",
}

# ─── SEARCH CLIENTS ────────────────────────────────────────────────────────────
@st.cache_resource
def get_tavily():
    return TavilyClient(api_key=st.secrets["TAVILY_API_KEY"])

@st.cache_resource
def get_exa():
    return Exa(api_key=st.secrets["EXA_API_KEY"])

# ─── DOMAIN LISTS ──────────────────────────────────────────────────────────────
EXA_FINANCIAL_DOMAINS = [
    "bloomberg.com", "ft.com", "reuters.com", "wsj.com",
    "seekingalpha.com", "barrons.com", "marketwatch.com",
    "cnbc.com", "economist.com", "morningstar.com",
    "investopedia.com", "fool.com", "zacks.com",
    "hankyung.com", "mk.co.kr", "edaily.co.kr", "thebell.co.kr",
    "fnguide.com", "investing.com",
    "nikkei.com", "toyokeizai.net",
]

EXA_SNS_DOMAINS = [
    # 영문 투자 커뮤니티
    "reddit.com",
    "stocktwits.com",
    "x.com", "twitter.com",
    "news.ycombinator.com",
    "fool.com/community",
    # 한국 투자 커뮤니티
    "finance.naver.com",
    "stock.kakao.com",
    "ppomppu.co.kr",
    "clien.net",
    "mlbpark.donga.com",
    "fmkorea.com",
    # 일본 투자 커뮤니티
    "minkabu.jp",
    "kabutan.jp",
    "stockvoice.jp",
]

# ─── SNS 쿼리 시장별 분기 ─────────────────────────────────────────────────────
def _build_sns_queries(target: str, direction: str, market_id: str, year: int) -> list[str]:
    """
    시장별로 SNS 쿼리와 도메인을 다르게 구성.

    한국(KOSPI):
      - 네이버 종토방, 한경 커뮤니티, 인베스팅 등 한국어 커뮤니티
      - 한국어 쿼리로 감성 검색
    일본(Nikkei):
      - みんかぶ, 株探, Yahoo Japan Finance 게시판
      - 일본어 쿼리
    미국(S&P 500):
      - Reddit r/stocks r/investing, StockTwits
      - 영어 쿼리
    """
    dir_en = {"bull": "bullish buy positive", "neutral": "hold wait uncertain", "bear": "bearish sell risk"}[direction]

    if market_id == "kospi200":
        # 한국어 커뮤니티 — Tavily가 네이버·한경 등 인덱스 보유
        return [
            f"{target} 주식 투자자 반응 {direction} 커뮤니티 종토방 {year}",
            f"{target} 매수 매도 개인투자자 여론 주식 게시판 {year}",
        ]
    elif market_id == "nikkei225":
        # 일본어 커뮤니티
        return [
            f"{target} 株 投資家 掲示板 {direction} 意見 みんかぶ {year}",
            f"{target} 株価 個人投資家 口コミ 買い 売り {year}",
        ]
    else:
        # 영어권 (S&P 500 기본)
        return [
            f"{target} stock {dir_en} investors community Reddit StockTwits {year}",
            f"{target} stock discussion sentiment retail investors {year}",
        ]

def _get_sns_domains(market_id: str) -> list[str]:
    """시장별 SNS 도메인"""
    if market_id == "kospi200":
        return [
            "finance.naver.com", "hankyung.com", "mk.co.kr",
            "investing.com", "stockplus.com", "therich.io",
            "ppomppu.co.kr", "clien.net", "fmkorea.com",
        ]
    elif market_id == "nikkei225":
        return [
            "minkabu.jp", "kabutan.jp", "finance.yahoo.co.jp",
            "stockvoice.jp", "nikkeibp.co.jp",
        ]
    else:
        return ["reddit.com", "stocktwits.com", "x.com", "twitter.com"]


# ─── TICKER MAPPING (FMP용: 종목코드 → 영문 티커) ─────────────────────────────
# FMP는 미국 티커 기준. 한국·일본은 종목코드.XXX 형식 사용
FMP_TICKER_MAP = {
    # KOSPI — FMP는 한국 종목 지원 제한적, 005930.KS 등으로 시도
    "005930": "005930.KS",  "000660": "000660.KS",  "035420": "035420.KS",
    "005380": "005380.KS",  "000270": "000270.KS",  "207940": "207940.KS",
    "035720": "035720.KS",  "051910": "051910.KS",  "068270": "068270.KS",
    "105560": "105560.KS",  "055550": "055550.KS",  "032830": "032830.KS",
    "012330": "012330.KS",  "003550": "003550.KS",  "066570": "066570.KS",
    "028260": "028260.KS",  "096770": "096770.KS",  "034730": "034730.KS",
    "003490": "003490.KS",  "009830": "009830.KS",
    # Nikkei — FMP는 7203.T 형식 지원
    "7203": "7203.T",  "6758": "6758.T",  "9984": "9984.T",  "8306": "8306.T",
    "6861": "6861.T",  "6367": "6367.T",  "4063": "4063.T",  "7974": "7974.T",
    "6501": "6501.T",  "6702": "6702.T",  "8035": "8035.T",  "7267": "7267.T",
    "2914": "2914.T",  "9432": "9432.T",  "8411": "8411.T",  "4502": "4502.T",
    "6971": "6971.T",  "7751": "7751.T",  "6954": "6954.T",  "3382": "3382.T",
}

# ─── QUERY BUILDER ─────────────────────────────────────────────────────────────
def build_queries(target: str, direction: str, market_index: str, sector: str = "", market_id: str = "sp500") -> dict:
    now    = datetime.now()
    year   = now.year
    month  = now.strftime("%B %Y")
    sector_note = f" {sector}" if sector else ""

    if direction == "bull":
        tavily_q = [
            f"{target} stock buy rating target price upgrade analyst {month}",
            f"{target}{sector_note} earnings beat revenue growth bullish outlook {year}",
            f"{market_index} bull market rally forecast {month}",
        ]
        exa_report_q = [
            f"Investment research report bullish case for {target} price target upside",
            f"Expert analysis why {target} stock will outperform in {year}",
            f"{target}{sector_note} undervalued catalyst growth story analyst recommendation",
        ]
        exa_sns_q = _build_sns_queries(target, "bull", market_id, year)
    elif direction == "neutral":
        tavily_q = [
            f"{target} stock hold neutral rating mixed outlook analyst {month}",
            f"{target} sideways range-bound consolidation uncertainty {year}",
            f"{market_index} flat market uncertainty {month}",
        ]
        exa_report_q = [
            f"Why {target} stock is fairly valued neutral outlook balanced risks",
            f"{target}{sector_note} wait and see cautious analyst note {year}",
            f"{market_index} range-bound sideways market analysis competing forces",
        ]
        exa_sns_q = _build_sns_queries(target, "neutral", market_id, year)
    else:  # bear
        tavily_q = [
            f"{target} stock sell downgrade target price cut analyst {month}",
            f"{target}{sector_note} earnings miss revenue decline bearish risk {year}",
            f"{market_index} market correction crash risk warning {month}",
        ]
        exa_report_q = [
            f"Investment research bearish case short thesis {target} downside risk",
            f"Expert column why {target} stock is overvalued or facing headwinds {year}",
            f"{target}{sector_note} structural decline competition disruption analysis",
        ]
        exa_sns_q = _build_sns_queries(target, "bear", market_id, year)

    return {
        "tavily":      tavily_q,
        "exa_report":  exa_report_q,
        "exa_sns":     exa_sns_q,
    }

# ─── 현재가 실시간 조회 ────────────────────────────────────────────────────────
def fetch_current_price(target: str, ticker_raw: str, market_id: str) -> str:
    """
    Tavily로 현재 주가를 실시간 검색.
    Judge에게 실제 가격 컨텍스트를 제공해 환각 방지.
    """
    client = get_tavily()

    # 시장별 쿼리 최적화
    if market_id == "kospi200":
        queries = [
            f"{target} 현재 주가 오늘 {ticker_raw} 코스피",
            f"삼성전자 SK하이닉스 {target} 주가 실시간",
        ] if ticker_raw else [f"KOSPI 200 지수 현재 오늘"]
    elif market_id == "nikkei225":
        queries = [
            f"{target} 株価 現在 今日 {ticker_raw}",
            f"日経225 {target} 最新株価",
        ] if ticker_raw else [f"日経225 指数 現在値 今日"]
    else:  # sp500
        queries = [
            f"{target} stock price today current {ticker_raw}",
            f"S&P 500 {target} share price live",
        ] if ticker_raw else [f"S&P 500 index current level today"]

    price_snippets = []
    for query in queries[:2]:
        try:
            resp = client.search(query, max_results=3, search_depth="basic")
            for r in resp.get("results", []):
                content = r.get("content", "")[:300]
                title   = r.get("title", "")
                url     = r.get("url", "")
                date    = r.get("published_date", "")[:10]
                if content:
                    price_snippets.append(f"■ {title} ({date})\n  출처: {url}\n  {content}")
        except:
            pass

    if not price_snippets:
        return "[현재가 검색 실패 — 가격 언급 시 반드시 출처 명시 필요]"

    return (
        f"【현재 주가 검색 결과 ({datetime.now().strftime('%Y-%m-%d %H:%M')})】\n"
        + "\n\n".join(price_snippets)
    )

# ─── TAVILY SEARCH ─────────────────────────────────────────────────────────────
def search_tavily(queries: list[str]) -> list[dict]:
    client = get_tavily()
    seen, results = set(), []
    for query in queries:
        try:
            resp = client.search(query, max_results=4, search_depth="advanced", include_answer=False)
            for r in resp.get("results", []):
                url = r.get("url", "")
                if url in seen: continue
                seen.add(url)
                results.append({
                    "title":   r.get("title", ""),
                    "url":     url,
                    "content": r.get("content", "")[:700],
                    "date":    r.get("published_date", ""),
                })
        except Exception as e:
            results.append({"title": f"검색 실패: {query}", "url": "", "content": str(e)[:100], "date": ""})
    return results

# ─── EXA: 금융 리포트·칼럼 ─────────────────────────────────────────────────────
def search_exa_reports(queries: list[str], recent_days: int = 90) -> list[dict]:
    client = get_exa()
    start  = (datetime.now() - timedelta(days=recent_days)).strftime("%Y-%m-%dT00:00:00.000Z")
    seen, results = set(), []
    for query in queries:
        try:
            resp = client.search_and_contents(
                query, num_results=4, use_autoprompt=True,
                text={"max_characters": 800},
                highlights={"num_sentences": 3, "highlights_per_url": 2},
                start_published_date=start,
                include_domains=EXA_FINANCIAL_DOMAINS,
            )
            for r in resp.results:
                url = r.url or ""
                if url in seen: continue
                seen.add(url)
                hl = getattr(r, "highlights", []) or []
                content = " … ".join(hl) if hl else (getattr(r, "text", "") or "")[:800]
                results.append({
                    "title":   r.title or "",
                    "url":     url,
                    "content": content[:800],
                    "date":    r.published_date or "",
                })
        except Exception as e:
            results.append({"title": f"리포트 검색 실패: {query}", "url": "", "content": str(e)[:100], "date": ""})
    return results

# ─── TAVILY: SNS·커뮤니티 여론 ───────────────────────────────────────────────
def search_tavily_sns(queries: list[str], market_id: str = "sp500") -> list[dict]:
    """
    Tavily로 Reddit·StockTwits·커뮤니티 여론 수집.
    (Exa는 Reddit 크롤러 차단 → Tavily는 Google 인덱스 경유 접근 가능)
    """
    client = get_tavily()
    seen, results = set(), []
    for query in queries:
        try:
            domains = _get_sns_domains(market_id)
            resp = client.search(
                query,
                max_results=5,
                search_depth="advanced",
                include_domains=domains,
            )
            for r in resp.get("results", []):
                url = r.get("url", "")
                if url in seen: continue
                seen.add(url)
                results.append({
                    "title":    r.get("title", ""),
                    "url":      url,
                    "content":  r.get("content", "")[:600],
                    "date":     r.get("published_date", ""),
                    "platform": _detect_platform(url),
                })
        except Exception as e:
            # include_domains 미지원 시 도메인 없이 재시도
            try:
                resp2 = client.search(query, max_results=4, search_depth="basic")
                for r in resp2.get("results", []):
                    url = r.get("url", "")
                    if url in seen or not any(d in url for d in
                        ["reddit","stocktwits","naver","minkabu","kabutan"]): continue
                    seen.add(url)
                    results.append({
                        "title": r.get("title",""), "url": url,
                        "content": r.get("content","")[:600],
                        "date": r.get("published_date",""),
                        "platform": _detect_platform(url),
                    })
            except:
                results.append({"title": f"SNS 검색 실패: {query}", "url": "",
                                "content": str(e)[:100], "date": "", "platform": "?"})
    return results

def _detect_platform(url: str) -> str:
    if "reddit.com" in url:   return "Reddit"
    if "stocktwits.com" in url: return "StockTwits"
    if "x.com" in url or "twitter.com" in url: return "X(Twitter)"
    if "naver.com" in url:    return "네이버금융"
    if "kakao.com" in url:    return "카카오"
    if "minkabu" in url:      return "みんかぶ"
    if "kabutan" in url:      return "株探"
    return "커뮤니티"

# ─── 어닝콜 / 실적발표 정보 수집 ─────────────────────────────────────────────
def fetch_earnings_transcript(ticker_raw: str, target_name: str = "", market_id: str = "sp500") -> str:
    """
    시장별 최적 전략으로 최신 실적발표·어닝콜 내용 수집.

    한국(KOSPI): Tavily로 한국 경제지 실적 보도 검색
      → 한경·매경·이데일리 등이 컨퍼런스콜 내용을 상세 보도
    일본(Nikkei): Tavily로 일본어 決算 뉴스 검색
    미국(S&P500): FMP API 먼저 시도 → 실패 시 Tavily fallback
    """
    now = datetime.now()
    yr  = now.year
    # 최근 분기 추정
    q_current = (now.month - 1) // 3 + 1
    q_prev    = q_current - 1 if q_current > 1 else 4
    yr_prev   = yr if q_current > 1 else yr - 1

    # ── 한국 종목: Tavily로 실적 뉴스 검색 ──────────────────────────────────
    if market_id == "kospi200":
        return _fetch_earnings_tavily_kr(target_name, ticker_raw, yr, q_current, q_prev)

    # ── 일본 종목: Tavily로 決算 뉴스 검색 ──────────────────────────────────
    if market_id == "nikkei225":
        return _fetch_earnings_tavily_jp(target_name, ticker_raw, yr)

    # ── 미국 종목: FMP 먼저, 실패 시 Tavily fallback ─────────────────────────
    fmp_key = st.secrets.get("FMP_API_KEY", "")
    if fmp_key and fmp_key.strip() not in ("", "...", "여기에_FMP_키"):
        fmp_result = _fetch_fmp(ticker_raw, fmp_key, yr, yr - 1)
        if fmp_result:
            return fmp_result
    # FMP 실패 또는 키 없음 → Tavily fallback
    return _fetch_earnings_tavily_us(target_name, ticker_raw, yr, q_current, q_prev)


def _fetch_earnings_tavily_kr(name: str, ticker: str, yr: int, q: int, q_prev: int) -> str:
    """한국 종목 실적발표·컨퍼런스콜 뉴스 Tavily 검색"""
    client = get_tavily()
    queries = [
        f"{name} {yr}년 {q}분기 실적발표 컨퍼런스콜 경영진 가이던스",
        f"{name} 실적 영업이익 매출 전망 경영진 발언 {yr}",
        f"{ticker} {name} 어닝콜 CEO CFO 발언 {yr}년",
    ]
    snippets = []
    seen = set()
    for query in queries:
        try:
            resp = client.search(
                query, max_results=4, search_depth="advanced",
                include_domains=["hankyung.com","mk.co.kr","edaily.co.kr",
                                 "thebell.co.kr","newsway.co.kr","inews24.com",
                                 "sedaily.com","etnews.com","biz.chosun.com"],
            )
            for r in resp.get("results", []):
                url = r.get("url","")
                if url in seen: continue
                seen.add(url)
                snippets.append(
                    f"■ {r.get('title','')} ({r.get('published_date','')[:10]})\n"
                    f"  출처: {url}\n"
                    f"  {r.get('content','')[:500]}"
                )
        except: pass
    if not snippets:
        return f"[{name} 실적발표 뉴스 검색 결과 없음 — 아직 발표 전일 수 있음]"
    return (
        f"【{name} 최신 실적발표 및 컨퍼런스콜 내용 (Tavily 검색)】\n"
        f"(출처: 한국경제·매일경제·이데일리 등 경제지)\n\n"
        + "\n\n".join(snippets[:6])
    )


def _fetch_earnings_tavily_jp(name: str, ticker: str, yr: int) -> str:
    """일본 종목 決算 뉴스 Tavily 검색"""
    client = get_tavily()
    queries = [
        f"{name} {yr}年 決算発表 業績 経営者コメント 見通し",
        f"{ticker} {name} 決算 売上 営業利益 ガイダンス {yr}",
    ]
    snippets = []
    seen = set()
    for query in queries:
        try:
            resp = client.search(
                query, max_results=4, search_depth="advanced",
                include_domains=["nikkei.com","minkabu.jp","kabutan.jp",
                                 "toyokeizai.net","diamond.jp","reuters.com"],
            )
            for r in resp.get("results", []):
                url = r.get("url","")
                if url in seen: continue
                seen.add(url)
                snippets.append(
                    f"■ {r.get('title','')} ({r.get('published_date','')[:10]})\n"
                    f"  출처: {url}\n"
                    f"  {r.get('content','')[:500]}"
                )
        except: pass
    if not snippets:
        return f"[{name} 決算 뉴스 검색 결과 없음]"
    return (
        f"【{name} 最新決算発表・経営者コメント (Tavily検索)】\n\n"
        + "\n\n".join(snippets[:5])
    )


def _fetch_earnings_tavily_us(name: str, ticker: str, yr: int, q: int, q_prev: int) -> str:
    """미국 종목 어닝콜 뉴스 Tavily fallback"""
    client = get_tavily()
    queries = [
        f"{name} {ticker} earnings call Q{q_prev} {yr} CEO CFO guidance transcript",
        f"{ticker} quarterly earnings results management commentary {yr}",
    ]
    snippets = []
    seen = set()
    for query in queries:
        try:
            resp = client.search(
                query, max_results=4, search_depth="advanced",
                include_domains=["seekingalpha.com","fool.com","cnbc.com",
                                 "reuters.com","bloomberg.com","wsj.com",
                                 "marketwatch.com","thestreet.com"],
            )
            for r in resp.get("results", []):
                url = r.get("url","")
                if url in seen: continue
                seen.add(url)
                snippets.append(
                    f"■ {r.get('title','')} ({r.get('published_date','')[:10]})\n"
                    f"  출처: {url}\n"
                    f"  {r.get('content','')[:500]}"
                )
        except: pass
    if not snippets:
        return f"[{name}({ticker}) 어닝콜 뉴스 검색 결과 없음]"
    return (
        f"【{name} 최신 어닝콜·실적발표 내용 (Tavily 검색)】\n"
        f"(출처: Seeking Alpha·Reuters·Bloomberg 등)\n\n"
        + "\n\n".join(snippets[:5])
    )


def _sanitize_fmp_ticker(ticker: str) -> str:
    """FMP API용 티커 정규화. BRK.B → BRK-B, BF.B → BF-B 등"""
    return ticker.replace(".", "-")

def _fetch_fmp(ticker: str, fmp_key: str, yr: int, yr_prev: int):
    """FMP API로 미국 종목 어닝콜 트랜스크립트 직접 수집. 실패 시 None 반환 (조용히)."""
    import urllib.request, urllib.error, json as _json

    fmp_ticker = _sanitize_fmp_ticker(ticker)   # BRK.B → BRK-B
    found = None

    for y in [yr, yr_prev]:
        for q in [4, 3, 2, 1]:
            url = (
                f"https://financialmodelingprep.com/api/v3/earning_call_transcript"
                f"/{fmp_ticker}?quarter={q}&year={y}&apikey={fmp_key}"
            )
            try:
                req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
                with urllib.request.urlopen(req, timeout=8) as r:
                    data = _json.loads(r.read())
                if data and isinstance(data, list) and data[0].get("content"):
                    found = (data[0], q, y)
                    break
            except:
                continue   # 403, timeout, parse error 모두 조용히 넘어감
        if found:
            break

    if not found:
        return None   # Tavily fallback으로 위임

    row, q, y = found
    text = row.get("content", "")
    if len(text) > 6000:
        text = text[:3500] + "\n\n[... 중략 ...]\n\n" + text[-2000:]
    return (
        f"【어닝콜 트랜스크립트: {fmp_ticker} Q{q} {y} ({row.get('date','')[:10]})】\n"
        f"(출처: Financial Modeling Prep)\n\n{text}"
    )

# ─── COMBINED SEARCH ───────────────────────────────────────────────────────────
def combined_search(
    target: str,
    direction: str,
    market_index: str,
    sector: str = "",
    ticker_raw: str = "",
    market_id: str = "sp500",
) -> str:
    """
    4개 소스 통합:
    1) Tavily   — 최신 뉴스·속보·애널리스트 코멘트
    2) Exa      — 금융 리포트·IB 분석·전문가 칼럼
    3) Exa SNS  — Reddit·StockTwits·커뮤니티 Raw 여론
    4) FMP      — 최신 어닝콜 트랜스크립트 (종목일 때만)
    """
    queries = build_queries(target, direction, market_index, sector, market_id)

    tavily_res  = search_tavily(queries["tavily"])
    report_res  = search_exa_reports(queries["exa_report"])
    sns_res     = search_tavily_sns(queries["exa_sns"], market_id=market_id)
    earnings_tx = (
        fetch_earnings_transcript(ticker_raw, target_name=target, market_id=market_id)
        if ticker_raw else "[지수 분석 — 어닝콜 해당 없음]"
    )

    def fmt_items(items: list[dict], label: str, show_platform: bool = False) -> str:
        if not items:
            return f"【{label}】\n결과 없음\n"
        lines = [f"【{label}】"]
        for r in items:
            date_str = f" ({r['date'][:10]})" if r.get("date") else ""
            platform = f"[{r.get('platform','')}] " if show_platform and r.get("platform") else ""
            lines.append(f"■ {platform}{r['title']}{date_str}")
            if r.get("url"):
                lines.append(f"  출처: {r['url']}")
            if r.get("content"):
                lines.append(f"  내용: {r['content']}")
            lines.append("")
        return "\n".join(lines)

    dir_ko  = "강세" if direction=="bull" else "중립" if direction=="neutral" else "약세"
    total   = len(tavily_res) + len(report_res) + len(sns_res)
    header  = (
        f"=== {target} [{dir_ko}] 검색 결과 "
        f"(총 {total}건, {datetime.now().strftime('%Y-%m-%d')}) ===\n"
        f"소스: Tavily(뉴스) + Exa(리포트) + Exa(SNS여론) + FMP(어닝콜)\n"
    )

    sections = [
        fmt_items(tavily_res,  "① 최신 뉴스·애널리스트 코멘트 (Tavily)"),
        fmt_items(report_res,  "② 금융 리포트·전문가 칼럼 (Exa)"),
        fmt_items(sns_res,     "③ SNS·커뮤니티 Raw 여론 (Exa + Reddit/StockTwits/네이버)", show_platform=True),
        f"④ 최신 어닝콜 트랜스크립트 (FMP)\n{earnings_tx}",
    ]

    return header + "\n\n".join(sections)

def build_search_queries(target: str, direction: str, market_index: str) -> list[str]:
    """하위 호환용"""
    return build_queries(target, direction, market_index)["tavily"]


# ─── LOGIN ─────────────────────────────────────────────────────────────────────
def get_user_api_key() -> str | None:
    return st.session_state.get("user_api_key")

def validate_api_key(key: str) -> tuple[bool, str]:
    key = key.strip()
    if not key:
        return False, "API 키를 입력해 주세요."
    if not key.startswith("sk-ant-"):
        return False, "Anthropic API 키는 'sk-ant-'로 시작해야 합니다."
    if len(key) < 40:
        return False, "API 키가 너무 짧습니다. 전체 키를 복사했는지 확인해 주세요."
    return True, "OK"

def show_login_page():
    st.markdown("""
    <h1 style='text-align:center; background:linear-gradient(90deg,#4fc3f7,#00e87a,#f5c518,#ff3c4e,#e040fb);
    -webkit-background-clip:text; -webkit-text-fill-color:transparent; font-size:28px; margin-bottom:4px'>
    ⚡ 시장 방향 판정 엔진</h1>
    <p style='text-align:center; color:#4a5568; font-size:12px; letter-spacing:2px; margin-bottom:40px'>
    7-AGENT AI · 강세/중립/약세 내러티브 분석 · 향후 3개월 판정</p>
    """, unsafe_allow_html=True)

    _, col_c, _ = st.columns([1, 2, 1])
    with col_c:
        st.markdown("""
        <div style='background:#ffffff; border:1px solid #e2e6ef; border-radius:12px; padding:32px 36px;'>
        <div style='color:#8892ab; font-size:11px; letter-spacing:2px; margin-bottom:20px; text-align:center'>
        🔑 ANTHROPIC API 키로 로그인</div>
        """, unsafe_allow_html=True)

        api_key = st.text_input("API 키", type="password",
                                placeholder="sk-ant-api03-...",
                                label_visibility="collapsed")

        if st.button("▶ 로그인 및 시작", type="primary", use_container_width=True):
            if not api_key:
                st.error("API 키를 입력해 주세요.")
            else:
                valid, msg = validate_api_key(api_key.strip())
                if valid:
                    st.session_state["user_api_key"] = api_key.strip()
                    st.rerun()
                else:
                    st.error(msg)

        st.markdown("</div>", unsafe_allow_html=True)
        st.markdown("""
        <div style='margin-top:20px; color:#374151; font-size:11px; line-height:1.9; text-align:center'>
        API 키가 없으신가요?<br>
        <a href='https://console.anthropic.com/settings/keys' target='_blank'
           style='color:#4fc3f7'>console.anthropic.com</a> 에서 무료 발급<br><br>
        ✅ API 키는 서버에 저장되지 않습니다<br>
        ✅ 세션 종료 시 자동 삭제됩니다<br>
        ✅ 분석 결과는 모든 사용자와 48시간 공유
        </div>""", unsafe_allow_html=True)

# ─── SUPABASE ──────────────────────────────────────────────────────────────────
@st.cache_resource
def get_supabase():
    return create_client(st.secrets["SUPABASE_URL"], st.secrets["SUPABASE_KEY"])

def cache_get(target_id: str):
    try:
        resp = get_supabase().table("analyses").select("*").eq("target_id", target_id).execute()
        if not resp.data: return None
        row = resp.data[0]
        # running 상태는 TTL 무관하게 반환 (진행 중 표시 위해)
        if row.get("status") == "running":
            return row
        at  = datetime.fromisoformat(row["analyzed_at"].replace("Z","")).replace(tzinfo=timezone.utc)
        if (datetime.now(timezone.utc) - at).total_seconds() / 3600 > CACHE_TTL_HOURS:
            return None
        return row
    except: return None

def cache_set(target_id, market_id, target_label, results, winner, bull_prob=50, neutral_prob=30, bear_prob=20, status="done"):
    try:
        get_supabase().table("analyses").upsert({
            "target_id": target_id, "market_id": market_id,
            "target_label": target_label, "results": results,
            "winner": winner,
            "bull_prob": bull_prob,
            "neutral_prob": neutral_prob,
            "bear_prob": bear_prob,
            "status": status,
            "analyzed_at": datetime.now(timezone.utc).isoformat(),
        }, on_conflict="target_id").execute()
    except Exception as e:
        print(f"캐시 저장 오류: {e}")   # 백그라운드 스레드에서는 st.warning 사용 불가

# 백그라운드 스레드에서 API 키를 전달하기 위한 thread-safe 딕셔너리
_BG_KEYS: dict = {}

def cache_set_running(target_id, market_id, target_label):
    """분석 시작 시 'running' 상태를 DB에 기록 — 다른 사용자도 진행 중임을 알 수 있음"""
    try:
        get_supabase().table("analyses").upsert({
            "target_id": target_id, "market_id": market_id,
            "target_label": target_label,
            "results": {}, "winner": "", "status": "running",
            "analyzed_at": datetime.now(timezone.utc).isoformat(),
        }, on_conflict="target_id").execute()
    except: pass

def cache_delete(target_id):
    try: get_supabase().table("analyses").delete().eq("target_id", target_id).execute()
    except: pass

def load_leaderboard():
    try:
        resp = get_supabase().table("analyses").select(
            "target_id,market_id,target_label,winner,bull_prob,neutral_prob,bear_prob,analyzed_at"
        ).execute()
        rows = []
        for r in resp.data:
            at = datetime.fromisoformat(r["analyzed_at"].replace("Z","")).replace(tzinfo=timezone.utc)
            age_h = (datetime.now(timezone.utc) - at).total_seconds() / 3600
            if age_h <= CACHE_TTL_HOURS:
                rows.append({**r, "age_hours": round(age_h, 1)})
        # 강세 확률 높은 순 → 동점 시 약세 확률 낮은 순 정렬
        rows.sort(key=lambda r: (-(r.get("bull_prob") or 0), (r.get("bear_prob") or 0)))
        return rows
    except: return []

# ─── PROMPT BUILDERS ───────────────────────────────────────────────────────────
def build_system_prompts(market: dict, stock: tuple = None):
    idx = market["index"]
    cb  = market["central_bank"]
    kr  = "**CRITICAL: Write your ENTIRE response in Korean (한국어).**"
    target = f"{stock[1]} ({stock[0]})" if stock else idx
    sector_note = f" (섹터: {stock[2]}, {idx} 상장)" if stock else ""

    return {
        "bull": f"""You are a research analyst. You will receive results from 4 sources about {target}{sector_note}. {kr}
## 📈 {target} 강세 내러티브 수집 (향후 3개월)
### 주요 강세론자 및 기관 [실명·기관명·목표가 포함]
### 지배적인 강세 스토리라인 [누가, 왜, 어떤 근거로]
### 핵심 데이터 및 근거 [수치·지표 직접 인용]
### SNS·커뮤니티 강세 여론 [Reddit·커뮤니티의 실제 분위기를 Raw하게 요약]
### 어닝콜 핵심 포인트 [경영진 발언·가이던스 중 강세 근거]
### 강세 전제 조건
### 강세 내러티브 3줄 요약
출처(기관명, 날짜, URL, 커뮤니티명)를 반드시 명시하시오.
⚠️ 검색 결과에 없는 주가·목표가·손절가를 절대 만들어내지 말 것. 수치는 출처가 있는 것만 인용.""",

        "neutral": f"""You are a research analyst. You will receive results from 4 sources about {target}{sector_note}. {kr}
## ➡️ {target} 중립 내러티브 수집 (향후 3개월)
### 주요 중립론자 및 기관 [실명·기관명 포함]
### 지배적인 중립 스토리라인 [누가, 왜, 어떤 근거로]
### 핵심 데이터 및 근거 [상충 신호, 불확실성 지표]
### SNS·커뮤니티 중립 여론 [관망·혼재된 의견의 실제 분위기]
### 어닝콜 핵심 포인트 [경영진 발언 중 불확실성·중립 신호]
### 중립 전제 조건
### 중립 내러티브 3줄 요약
출처(기관명, 날짜, URL, 커뮤니티명)를 반드시 명시하시오.
⚠️ 검색 결과에 없는 주가·목표가·손절가를 절대 만들어내지 말 것. 수치는 출처가 있는 것만 인용.""",

        "bear": f"""You are a research analyst. You will receive results from 4 sources about {target}{sector_note}. {kr}
## 📉 {target} 약세 내러티브 수집 (향후 3개월)
### 주요 약세론자 및 기관 [실명·기관명 포함]
### 지배적인 약세 스토리라인 [누가, 왜, 어떤 근거로]
### 핵심 데이터 및 근거 [리스크 지표, 경고 신호]
### SNS·커뮤니티 약세 여론 [Reddit·커뮤니티의 우려·공포 분위기를 Raw하게 반영]
### 어닝콜 핵심 포인트 [경영진 발언 중 리스크·약세 시그널]
### 약세 전제 조건
### 약세 내러티브 3줄 요약
출처(기관명, 날짜, URL, 커뮤니티명)를 반드시 명시하시오.
⚠️ 검색 결과에 없는 주가·목표가·손절가를 절대 만들어내지 말 것. 수치는 출처가 있는 것만 인용.""",

        "bull_critic": f"""You are an adversarial analyst stress-testing bullish narratives about {target}. {kr}
## 🔥 강세 내러티브 비판
### 근거의 취약점 [데이터 오독, 체리피킹]
### 강세가 외면한 반대 증거
### 논리적 허점
### 향후 3개월 강세 붕괴 리스크
### 강세 신뢰도 [1-10점 및 2줄 평가]""",

        "neutral_critic": f"""You are an adversarial analyst stress-testing neutral narratives about {target}. {kr}
## 🔥 중립 내러티브 비판
### 거짓 균형의 함정
### 중립이 외면한 방향성 신호
### 역사적 실패 사례
### 방향성 강제 촉매
### 중립 신뢰도 [1-10점 및 2줄 평가]""",

        "bear_critic": f"""You are an adversarial analyst stress-testing bearish narratives about {target}. {kr}
## 🔥 약세 내러티브 비판
### 과거 패턴 오남용
### 약세가 외면한 회복력 근거
### 같은 약세 논리의 실패 전례
### 과소평가한 정책 대응 [{cb}]
### 약세 신뢰도 [1-10점 및 2줄 평가]""",

        "judge": f"""You are a Chief Investment Strategist reviewing a structured 6-agent debate about {target}. {kr}

⚠️ 절대 금지 사항 (hallucination 방지):
- 입력된 【실시간 현재가 정보】에 없는 주가 수치를 절대 만들어내지 말 것
- 검색 결과에 명시되지 않은 손절가·익절가·목표가를 생성 금지
- 수치를 인용할 때는 반드시 "○○기관에 따르면" 등 출처를 명시할 것
- 실시간 현재가 섹션에 가격이 있다면, 그 가격을 기준으로 % 등락을 서술할 것

## 핵심 요약
[정확히 4문장.
1문장: 세 내러티브 중 가장 그럴듯한 것과 핵심 이유.
2문장: 검색 결과에서 확인된 가장 강력한 지지 근거 (현재가 포함).
3문장: 경쟁 내러티브의 치명적 약점.
4문장: 이 판단을 뒤집을 수 있는 핵심 변수.]

## ⚡ 최종 판정

### 가장 그럴듯한 내러티브: [강세 / 중립 / 약세]
[이 방향이 가장 설득력 있는 이유. 현재가를 기준으로 서술. 검색 결과 사실만 활용.]

### 현재 가격 기준 상황
[실시간 현재가 정보에서 확인된 가격과 최근 주가 흐름 요약]

### 핵심 근거 (검색 결과·현재가에서 확인된 사실만)
**근거 1:** [출처 명시 + 구체적 사실]
**근거 2:** [출처 명시 + 구체적 사실]
**근거 3:** [출처 명시 + 구체적 사실]
**근거 4:** [출처 명시 + 구체적 사실]
**근거 5:** [출처 명시 + 구체적 사실]

### 경쟁 내러티브 탈락 이유
[강세/중립/약세 중 탈락한 둘의 약점. 검색 결과 근거로 설명.]

### 확률 분포
**강세장 (유의미한 상승): XX%**
**보합장 (박스권): XX%**
**약세장 (유의미한 하락): XX%**
[반드시 합계 100%]

### 주시해야 할 핵심 변수 (상위 3개)
[이 판단을 바꿀 수 있는 향후 이벤트·데이터 발표]

결단하라. 단, 모르는 수치는 만들지 말라.""",
    }

# ─── CLAUDE API ────────────────────────────────────────────────────────────────
def call_claude(system: str, user_content: str, max_tokens: int = 4000, _target_id: str = '') -> str:
    api_key = get_user_api_key() or next(iter(_BG_KEYS.values()), '')
    if not api_key:
        raise RuntimeError("로그인이 필요합니다.")
    client = anthropic.Anthropic(api_key=api_key)
    resp = client.messages.create(
        model="claude-sonnet-4-5-20250929",
        max_tokens=max_tokens,
        system=system,
        messages=[{"role": "user", "content": user_content}],
    )
    return "".join(b.text for b in resp.content if hasattr(b, "text"))

# ─── HELPERS ───────────────────────────────────────────────────────────────────
def extract_winner(text):
    """텍스트에서 '가장 그럴듯한 내러티브' 라인을 파싱. 보조 수단으로만 사용."""
    m = re.search(r"가장 그럴듯한 내러티브[^:：\n]*[：:]\s*\[?([^\]\n]+)\]?", text)
    if not m: return None   # None 반환 → 확률로 결정
    raw = m.group(1).strip()
    if "강세" in raw and "약세" not in raw: return "bull"
    if "약세" in raw and "강세" not in raw: return "bear"
    if "중립" in raw or "보합" in raw: return "neutral"
    return None

def winner_from_probs(bull_p, neutral_p, bear_p):
    """확률 기반으로 winner 결정 — 항상 정확."""
    probs = {"bull": bull_p or 0, "neutral": neutral_p or 0, "bear": bear_p or 0}
    return max(probs, key=probs.get)

def extract_probs(text):
    b = re.search(r"강세장[^:\n*]*[:\*]+\s*(\d+)%", text)
    n = re.search(r"보합장[^:\n*]*[:\*]+\s*(\d+)%", text)
    r = re.search(r"약세장[^:\n*]*[:\*]+\s*(\d+)%", text)
    if b and n and r:
        return int(b.group(1)), int(n.group(1)), int(r.group(1))
    return None, None, None

def winner_badge(w):
    return {"bull":"📈 강세","neutral":"➡️ 중립","bear":"📉 약세"}.get(w,"❓")

def age_label(hours):
    if hours < 1: return "방금"
    if hours < 24: return f"{int(hours)}시간 전"
    return f"{int(hours/24)}일 전"

# ─── RUN ANALYSIS ──────────────────────────────────────────────────────────────
def _run_analysis_core(target_id, target_label, market, stock, prompts, user_api_key):
    results  = {}
    progress = st.progress(0)
    status   = st.empty()
    target_short = stock[1] if stock else market["index"]

    # ── Phase 1: 듀얼 엔진 검색 → Claude 분석 ────────────────────────────────
    st.markdown("**Phase 1 · 내러티브 수집 (Tavily 뉴스 + Exa 리포트·칼럼 → Claude 분석)**")
    cols = st.columns(3)
    areas = {a: cols[i].empty() for i, a in enumerate(["bull","neutral","bear"])}

    sector     = stock[2] if stock else ""
    ticker_raw = stock[0] if stock else ""

    for i, (agent, direction) in enumerate([("bull","bull"),("neutral","neutral"),("bear","bear")]):
        dir_label = "강세" if direction=="bull" else ("중립" if direction=="neutral" else "약세")
        status.markdown(f"🔍 **{AGENT_LABELS[agent]}** — 4-소스 검색 중...")
        areas[agent].info(f"{AGENT_LABELS[agent]}\n🔍 Tavily+Exa+FMP 수집 중...")

        try:
            search_results = combined_search(
                target_short, direction, market["index"],
                sector=sector, ticker_raw=ticker_raw,
                market_id=market["id"],
            )

            areas[agent].info(f"{AGENT_LABELS[agent]}\n🤖 Claude 분석 중...")
            status.markdown(f"🤖 **{AGENT_LABELS[agent]}** — Claude 분석 중...")

            user_content = f"""다음은 오늘({datetime.now().strftime('%Y년 %m월 %d일')}) 기준 {target_label}에 관한 4개 소스 검색 결과입니다:

{search_results}

위 검색 결과를 바탕으로:
- ①②: 기관·전문가의 공식 {dir_label} 내러티브를 수집·정리하십시오
- ③: SNS·커뮤니티의 Raw 여론(감성·분위기)을 객관적으로 요약하십시오
- ④: 어닝콜에서 경영진이 언급한 핵심 가이던스와 리스크를 반영하십시오
구체적인 수치·날짜·출처·발언자를 반드시 인용하십시오."""

            results[agent] = call_claude(prompts[agent], user_content)
            areas[agent].success(f"{AGENT_LABELS[agent]}\n✅ 완료")
        except Exception as e:
            results[agent] = f"⚠️ 오류: {e}"
            areas[agent].warning(f"{AGENT_LABELS[agent]}\n⚠️ 오류")
        progress.progress((i + 1) / 7)

    # ── Phase 2: 비판 ─────────────────────────────────────────────────────────
    st.markdown("**Phase 2 · 비판 검증**")
    cols2 = st.columns(3)
    areas2 = {a: cols2[i].empty() for i, a in enumerate(["bull_critic","neutral_critic","bear_critic"])}

    critic_map = {
        "bull_critic":    ("bull",    "강세"),
        "neutral_critic": ("neutral", "중립"),
        "bear_critic":    ("bear",    "약세"),
    }
    for i, agent in enumerate(["bull_critic","neutral_critic","bear_critic"]):
        src, label = critic_map[agent]
        status.markdown(f"🔥 **{AGENT_LABELS[agent]}** 비판 중...")
        areas2[agent].info(f"{AGENT_LABELS[agent]}\n⏳ 분석 중...")
        try:
            user_content = f"[{label} 내러티브]:\n{results.get(src,'')}\n\n위 내러티브를 냉정하고 구체적으로 비판하시오."
            results[agent] = call_claude(prompts[agent], user_content)
            areas2[agent].success(f"{AGENT_LABELS[agent]}\n✅ 완료")
        except Exception as e:
            results[agent] = f"⚠️ 오류: {e}"
            areas2[agent].warning(f"{AGENT_LABELS[agent]}\n⚠️ 오류")
        progress.progress((4 + i) / 7)

    # ── Phase 3: Judge (현재가 실시간 검색 후 주입) ──────────────────────────
    st.markdown("**Phase 3 · 최종 판정**")
    status.markdown("📡 **현재 주가 실시간 조회 중...**")

    price_context = fetch_current_price(target_short, ticker_raw, market["id"])

    status.markdown("⚡ **최종 판정자** 종합 분석 중...")
    try:
        judge_input = (
            f"【실시간 현재가 정보 — 반드시 이 가격을 기준으로 판단하시오】\n"
            f"{price_context}\n\n"
            f"{'='*60}\n\n"
            + "\n\n".join([
                f"[{AGENT_LABELS[a]}]:\n{results.get(a,'')}"
                for a in ["bull","neutral","bear","bull_critic","neutral_critic","bear_critic"]
            ])
            + "\n\n위 토론과 실시간 현재가를 종합하여 최종 판정을 내리시오."
        )
        results["judge"] = call_claude(prompts["judge"], judge_input, max_tokens=8000)
    except Exception as e:
        results["judge"] = f"⚠️ 오류: {e}"

    progress.progress(1.0)
    status.success("✅ 분석 완료!")

    # ★ winner는 확률 기반으로 결정 (텍스트 파싱은 보조)
    bull_p, neutral_p, bear_p = extract_probs(results.get("judge",""))
    bull_p   = bull_p   or 50
    neutral_p = neutral_p or 30
    bear_p   = bear_p   or 20
    winner = winner_from_probs(bull_p, neutral_p, bear_p)
    # 확률 파싱 실패 시 텍스트 파싱으로 보완
    if bull_p == 50 and neutral_p == 30 and bear_p == 20:
        winner = extract_winner(results.get("judge","")) or "neutral"
    cache_set(
        target_id, market["id"], target_label, results, winner,
        bull_prob=bull_p, neutral_prob=neutral_p, bear_prob=bear_p,
        status="done",
    )
    return results, winner

# ─── DISPLAY RESULTS ───────────────────────────────────────────────────────────
def display_results(results, winner, cached_at=None):
    if cached_at:
        at = datetime.fromisoformat(cached_at.replace("Z","")).replace(tzinfo=timezone.utc)
        age_h = (datetime.now(timezone.utc) - at).total_seconds() / 3600
        st.info(f"🗄 공유 캐시 결과 · {at.strftime('%Y-%m-%d %H:%M')} UTC 분석 · {CACHE_TTL_HOURS-age_h:.0f}시간 후 만료")

    w_map = {"bull":("📈 강세","#00e87a"),"neutral":("➡️ 중립","#f5c518"),"bear":("📉 약세","#ff3c4e")}
    w_label, w_color = w_map.get(winner, ("❓","#888"))
    st.markdown(f"""
    <div style='text-align:center; padding:16px;
    background:linear-gradient(135deg,{w_color}18,transparent);
    border:2px solid {w_color}66; border-radius:10px; margin:12px 0'>
        <div style='color:#6b7a9e; font-size:11px; letter-spacing:2px; margin-bottom:6px'>가장 그럴듯한 내러티브</div>
        <div style='color:{w_color}; font-size:24px; font-weight:900'>{w_label}</div>
    </div>""", unsafe_allow_html=True)

    bp, np_, rp = extract_probs(results.get("judge",""))
    if bp is not None:
        st.markdown("#### 확률 분포")
        c1,c2,c3 = st.columns(3)
        c1.metric("📈 강세장", f"{bp}%"); c1.progress(bp/100)
        c2.metric("➡️ 보합장", f"{np_}%"); c2.progress(np_/100)
        c3.metric("📉 약세장", f"{rp}%"); c3.progress(rp/100)

    st.markdown("---")
    st.markdown("### Phase 1 · 내러티브 수집")
    for a in ["bull","neutral","bear"]:
        with st.expander(AGENT_LABELS[a]):
            st.markdown(results.get(a,"결과 없음"))
    st.markdown("### Phase 2 · 비판 검증")
    for a in ["bull_critic","neutral_critic","bear_critic"]:
        with st.expander(AGENT_LABELS[a]):
            st.markdown(results.get(a,"결과 없음"))
    st.markdown("### Phase 3 · 최종 판정")
    with st.expander("⚡ 최종 판정자 전문", expanded=True):
        st.markdown(results.get("judge","결과 없음"))

# ─── LEADERBOARD ───────────────────────────────────────────────────────────────
def display_leaderboard():
    rows  = load_leaderboard()
    total = 3 + 20 * 3  # 63
    done  = len(rows)
    pct   = int(done / total * 100)

    st.markdown("### 📊 추천 강도 랭킹 (48시간 내 · 강세 확률 높은 순)")
    st.progress(pct / 100, text=f"{done} / {total} 분석 완료 ({pct}%) — 모든 사용자 공유")

    if not rows:
        st.caption("아직 분석 없음. 첫 분석을 시작해보세요!")
        return

    market_flag = {"kospi200":"🇰🇷","sp500":"🇺🇸","nikkei225":"🇯🇵"}

    # 컬럼 헤더
    h1,h2,h3,h4,h5,h6 = st.columns([0.4, 0.3, 2.2, 1.0, 2.5, 0.8])
    h1.markdown("<span style='color:#4a5568;font-size:11px'>순위</span>", unsafe_allow_html=True)
    h2.markdown("<span style='color:#4a5568;font-size:11px'>시장</span>", unsafe_allow_html=True)
    h3.markdown("<span style='color:#4a5568;font-size:11px'>종목/지수</span>", unsafe_allow_html=True)
    h4.markdown("<span style='color:#4a5568;font-size:11px'>판정</span>", unsafe_allow_html=True)
    h5.markdown("<span style='color:#4a5568;font-size:11px'>확률 분포</span>", unsafe_allow_html=True)
    h6.markdown("<span style='color:#4a5568;font-size:11px'>분석</span>", unsafe_allow_html=True)
    st.markdown("<hr style='margin:4px 0; border-color:#e2e6ef'>", unsafe_allow_html=True)

    for rank, row in enumerate(rows, 1):
        bp = row.get("bull_prob") or 0
        np_ = row.get("neutral_prob") or 0
        rp = row.get("bear_prob") or 0
        w  = row.get("winner","")
        flag = market_flag.get(row.get("market_id",""), "")

        # 순위 색상: 상위 강세 초록, 하위 약세 빨강
        if bp >= 55:
            rank_color = "#00e87a"
        elif bp >= 45:
            rank_color = "#f5c518"
        else:
            rank_color = "#ff3c4e"

        c1,c2,c3,c4,c5,c6 = st.columns([0.4, 0.3, 2.2, 1.0, 2.5, 0.8])

        c1.markdown(
            f"<div style='color:{rank_color};font-weight:900;font-size:14px;padding-top:4px'>#{rank}</div>",
            unsafe_allow_html=True,
        )
        c2.markdown(
            f"<div style='font-size:18px;padding-top:2px'>{flag}</div>",
            unsafe_allow_html=True,
        )
        c3.markdown(
            f"<div style='color:#4a5568;font-size:13px;padding-top:4px'>{row['target_label']}</div>",
            unsafe_allow_html=True,
        )
        c4.markdown(winner_badge(w), unsafe_allow_html=False)

        # 미니 확률 바
        bar_html = f"""
        <div style='display:flex;gap:2px;align-items:center;margin-top:6px'>
          <div style='width:{bp}%;height:8px;background:#00e87a;border-radius:2px 0 0 2px' title='강세 {bp}%'></div>
          <div style='width:{np_}%;height:8px;background:#f5c518' title='중립 {np_}%'></div>
          <div style='width:{rp}%;height:8px;background:#ff3c4e;border-radius:0 2px 2px 0' title='약세 {rp}%'></div>
        </div>
        <div style='display:flex;gap:8px;font-size:9px;color:#6b7a9e;margin-top:2px'>
          <span style='color:#00e87a'>↑{bp}%</span>
          <span style='color:#f5c518'>→{np_}%</span>
          <span style='color:#ff3c4e'>↓{rp}%</span>
        </div>"""
        c5.markdown(bar_html, unsafe_allow_html=True)

        c6.markdown(
            f"<div style='color:#374151;font-size:10px;padding-top:6px'>{age_label(row['age_hours'])}</div>",
            unsafe_allow_html=True,
        )

        st.markdown("<hr style='margin:2px 0; border-color:#f0f0f0'>", unsafe_allow_html=True)

# ─── MAIN ──────────────────────────────────────────────────────────────────────
def main():
    if not get_user_api_key():
        show_login_page()
        return

    col_title, col_logout = st.columns([5,1])
    with col_title:
        st.markdown("""
        <h1 style='background:linear-gradient(90deg,#4fc3f7,#00e87a,#f5c518,#ff3c4e,#e040fb);
        -webkit-background-clip:text; -webkit-text-fill-color:transparent; font-size:26px; margin:0'>
        ⚡ 시장 방향 판정 엔진</h1>
        <p style='color:#4a5568; font-size:11px; letter-spacing:2px; margin:2px 0 0'>
        7-AGENT AI · Tavily 웹검색 + Claude 분석 · 향후 3개월 판정</p>
        """, unsafe_allow_html=True)
    with col_logout:
        key = get_user_api_key()
        st.markdown(f"<div style='color:#374151;font-size:10px;text-align:right;margin-top:6px'>🔑 ...{key[-4:]}</div>", unsafe_allow_html=True)
        if st.button("로그아웃", use_container_width=True):
            st.session_state.clear()
            st.rerun()

    st.markdown("---")
    display_leaderboard()
    st.markdown("---")

    st.markdown("### STEP 1 · 시장 선택")
    market_choice = st.radio("", list(MARKETS.keys()), horizontal=True, label_visibility="collapsed")
    market = MARKETS[market_choice]

    st.markdown("### STEP 2 · 분석 대상")
    stocks  = STOCKS[market["id"]]
    options = ["📊 지수 전체"] + [f"{t} · {n} ({s})" for t,n,s in stocks]
    choice  = st.selectbox("", options, label_visibility="collapsed")

    if choice == "📊 지수 전체":
        stock, target_id = None, market["id"]
        target_label = f"{market['flag']} {market['index']}"
    else:
        idx = options.index(choice) - 1
        stock = stocks[idx]
        target_id    = f"{market['id']}_{stock[0]}"
        target_label = f"{stock[1]} ({stock[0]})"

    st.markdown(f"**선택:** {target_label}")

    cached = cache_get(target_id)
    col_a, col_b = st.columns([3,1])

    if cached:
        status = cached.get("status", "done")

        if status == "running":
            # ── 백그라운드 실행 중 ──────────────────────────────────────────
            at = datetime.fromisoformat(cached["analyzed_at"].replace("Z","")).replace(tzinfo=timezone.utc)
            elapsed = int((datetime.now(timezone.utc) - at).total_seconds() / 60)
            st.info(
                f"⏳ **{target_label} 분석이 백그라운드에서 실행 중입니다** "
                f"({elapsed}분 경과)\n\n"
                f"브라우저를 닫아도 서버에서 계속 진행됩니다. "
                f"이 페이지를 30초 후 새로고침하면 진행 상황을 확인할 수 있습니다."
            )
            col_r, col_c = st.columns([1, 1])
            with col_r:
                if st.button("🔄 새로고침 (결과 확인)", use_container_width=True):
                    st.rerun()
            with col_c:
                if elapsed > 20 and st.button("⚠️ 실패로 간주하고 재시작", use_container_width=True):
                    cache_delete(target_id)
                    st.rerun()
            # 30초마다 자동 새로고침
            import time as _time
            _time.sleep(1)
            st.rerun()

        else:
            # ── 완료된 캐시 ─────────────────────────────────────────────────
            at = datetime.fromisoformat(cached["analyzed_at"].replace("Z","")).replace(tzinfo=timezone.utc)
            age_h = (datetime.now(timezone.utc) - at).total_seconds() / 3600
            remaining = CACHE_TTL_HOURS - age_h
            with col_a:
                if st.button(f"🗄 공유 캐시 불러오기 ({remaining:.0f}시간 남음, 토큰 0 소모)", type="primary", use_container_width=True):
                    bp  = cached.get("bull_prob")    or 50
                    np_ = cached.get("neutral_prob") or 30
                    rp  = cached.get("bear_prob")    or 20
                    recalc_winner = winner_from_probs(bp, np_, rp)
                    st.session_state.update({
                        "res_results":   cached["results"],
                        "res_winner":    recalc_winner,
                        "res_cached_at": cached["analyzed_at"],
                        "show_results":  True,
                    })
            with col_b:
                if st.button("🗑 재분석", use_container_width=True):
                    cache_delete(target_id)
                    st.session_state.pop("show_results", None)
                    st.success("캐시 삭제. 아래 버튼으로 재분석하세요.")
                    st.rerun()
    else:
        with col_a:
            if st.button(f"▶ {target_label} 분석 시작", type="primary", use_container_width=True):
                st.session_state.pop("show_results", None)
                api_key = get_user_api_key()
                prompts = build_system_prompts(market, stock)
                # DB에 "실행 중" 상태 선점 기록
                cache_set_running(target_id, market["id"], target_label)
                # 백그라운드 스레드로 실행 — 브라우저 꺼도 계속 진행
                def _bg_task():
                    import streamlit as _st
                    # 스레드에서 session_state 접근 불가 → API 키를 클로저로 전달
                    # call_claude 내부에서 _bg_api_key를 참조하도록 임시 저장
                    try:
                        import contextvars
                    except: pass
                    # API 키를 전역 딕셔너리로 전달 (스레드 안전)
                    _BG_KEYS[target_id] = api_key
                    try:
                        _run_analysis_core(target_id, target_label, market, stock, prompts, api_key)
                    except Exception as e:
                        print(f"백그라운드 분석 오류 [{target_id}]: {e}")
                    finally:
                        _BG_KEYS.pop(target_id, None)

                t = threading.Thread(target=_bg_task, daemon=True)
                t.start()
                st.session_state["bg_running"] = target_id
                st.rerun()

    if st.session_state.get("show_results"):
        st.markdown("---")
        display_results(
            st.session_state["res_results"],
            st.session_state["res_winner"],
            st.session_state.get("res_cached_at"),
        )

    st.markdown("---")
    st.caption("AI 생성 콘텐츠 · 투자 조언 아님 · 연구 목적 전용")

if __name__ == "__main__":
    main()
