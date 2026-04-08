import streamlit as st
import anthropic
import re
from datetime import datetime, timezone, timedelta
from supabase import create_client
from tavily import TavilyClient
from exa_py import Exa

# ─── PAGE CONFIG ───────────────────────────────────────────────────────────────
st.set_page_config(page_title="시장 방향 판정 엔진", page_icon="⚡", layout="wide")

st.markdown("""
<style>
body, .stApp { background: #07070d !important; color: #fff !important; }
.block-container { padding-top: 1.5rem; }
.stButton > button {
    background: transparent; border: 1px solid #444;
    color: #ccc; border-radius: 6px; font-size: 13px;
}
.stButton > button:hover { border-color: #888; color: #fff; }
h1, h2, h3 { color: #fff !important; }
.stExpander { background: #0c0c18 !important; border: 1px solid #1a1a2a !important; }
div[data-testid="stExpander"] > div { background: #09090f !important; }
.stTextInput > div > div > input {
    background: #0d0d14; border: 1px solid #1e1e2a;
    color: #ccc; border-radius: 6px;
}
.stSelectbox > div > div { background: #0d0d14 !important; color: #ccc !important; }
div[data-testid="metric-container"] {
    background: #0c0c18; border: 1px solid #1a1a2a;
    border-radius: 8px; padding: 12px;
}
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
def build_queries(target: str, direction: str, market_index: str, sector: str = "") -> dict:
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
        exa_sns_q = [
            f"{target} stock bullish investors excited positive sentiment Reddit StockTwits",
            f"Why I'm buying {target} stock community discussion {year}",
        ]
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
        exa_sns_q = [
            f"{target} stock mixed opinion community debate uncertain Reddit {year}",
            f"Is {target} worth holding investors discussion {month}",
        ]
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
        exa_sns_q = [
            f"{target} stock bearish investors worried concern Reddit StockTwits {year}",
            f"Why I sold {target} stock community discussion risk {month}",
        ]

    return {
        "tavily":      tavily_q,
        "exa_report":  exa_report_q,
        "exa_sns":     exa_sns_q,
    }

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

# ─── EXA: SNS·커뮤니티 여론 ──────────────────────────────────────────────────
def search_exa_sns(queries: list[str], recent_days: int = 60) -> list[dict]:
    """
    Reddit·StockTwits·X·네이버 금융 등 커뮤니티의 Raw 여론 수집.
    필터링 없는 투자자 감성을 그대로 반영.
    """
    client = get_exa()
    start  = (datetime.now() - timedelta(days=recent_days)).strftime("%Y-%m-%dT00:00:00.000Z")
    seen, results = set(), []
    for query in queries:
        try:
            resp = client.search_and_contents(
                query, num_results=5, use_autoprompt=True,
                text={"max_characters": 600},
                highlights={"num_sentences": 2, "highlights_per_url": 3},
                start_published_date=start,
                include_domains=EXA_SNS_DOMAINS,
            )
            for r in resp.results:
                url = r.url or ""
                if url in seen: continue
                seen.add(url)
                hl = getattr(r, "highlights", []) or []
                content = " … ".join(hl) if hl else (getattr(r, "text", "") or "")[:600]
                results.append({
                    "title":   r.title or url,
                    "url":     url,
                    "content": content[:600],
                    "date":    r.published_date or "",
                    "platform": _detect_platform(url),
                })
        except Exception as e:
            results.append({"title": f"SNS 검색 실패: {query}", "url": "", "content": str(e)[:100], "date": "", "platform": "?"})
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

# ─── FMP: 어닝콜 트랜스크립트 ─────────────────────────────────────────────────
def fetch_earnings_transcript(ticker_raw: str) -> str:
    """
    Financial Modeling Prep API로 최신 어닝콜 트랜스크립트 수집.
    경영진의 실제 발언·가이던스를 직접 반영.
    """
    import urllib.request, json as _json

    fmp_key = st.secrets.get("FMP_API_KEY", "")
    if not fmp_key:
        return "[FMP API 키 없음 — 어닝콜 데이터 미포함]"

    # 티커 변환 (한국·일본 종목)
    ticker = FMP_TICKER_MAP.get(ticker_raw, ticker_raw)

    try:
        # 최근 4개 분기 목록 조회
        url_list = (
            f"https://financialmodelingprep.com/api/v4/earning_call_transcript"
            f"?symbol={ticker}&apikey={fmp_key}"
        )
        with urllib.request.urlopen(url_list, timeout=8) as r:
            data = _json.loads(r.read())

        if not data:
            return f"[{ticker}: 어닝콜 트랜스크립트 없음 (FMP 미지원 종목일 수 있음)]"

        # 가장 최신 분기
        latest = data[0]
        quarter, year_ec = latest.get("quarter", ""), latest.get("year", "")

        # 트랜스크립트 본문 조회
        url_transcript = (
            f"https://financialmodelingprep.com/api/v3/earning_call_transcript"
            f"/{ticker}?quarter={quarter}&year={year_ec}&apikey={fmp_key}"
        )
        with urllib.request.urlopen(url_transcript, timeout=10) as r:
            t_data = _json.loads(r.read())

        if not t_data:
            return f"[{ticker} Q{quarter} {year_ec}: 트랜스크립트 본문 없음]"

        transcript_text = t_data[0].get("content", "")
        if not transcript_text:
            return f"[{ticker}: 트랜스크립트 내용 비어 있음]"

        # 핵심 섹션 추출 (너무 길면 앞 3,000자 + 뒤 1,000자)
        if len(transcript_text) > 5000:
            excerpt = transcript_text[:3000] + "\n\n[... 중략 ...]\n\n" + transcript_text[-1500:]
        else:
            excerpt = transcript_text

        return (
            f"【어닝콜 트랜스크립트: {ticker} Q{quarter} {year_ec}】\n"
            f"(출처: Financial Modeling Prep)\n\n"
            f"{excerpt}"
        )

    except Exception as e:
        return f"[어닝콜 데이터 수집 실패: {ticker} — {str(e)[:120]}]"

# ─── COMBINED SEARCH ───────────────────────────────────────────────────────────
def combined_search(
    target: str,
    direction: str,
    market_index: str,
    sector: str = "",
    ticker_raw: str = "",        # FMP용 원본 종목코드
) -> str:
    """
    4개 소스 통합:
    1) Tavily   — 최신 뉴스·속보·애널리스트 코멘트
    2) Exa      — 금융 리포트·IB 분석·전문가 칼럼
    3) Exa SNS  — Reddit·StockTwits·커뮤니티 Raw 여론
    4) FMP      — 최신 어닝콜 트랜스크립트 (종목일 때만)
    """
    queries = build_queries(target, direction, market_index, sector)

    tavily_res  = search_tavily(queries["tavily"])
    report_res  = search_exa_reports(queries["exa_report"])
    sns_res     = search_exa_sns(queries["exa_sns"])
    earnings_tx = fetch_earnings_transcript(ticker_raw) if ticker_raw else "[지수 분석 — 어닝콜 해당 없음]"

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
    <p style='text-align:center; color:#444; font-size:12px; letter-spacing:2px; margin-bottom:40px'>
    7-AGENT AI · 강세/중립/약세 내러티브 분석 · 향후 3개월 판정</p>
    """, unsafe_allow_html=True)

    _, col_c, _ = st.columns([1, 2, 1])
    with col_c:
        st.markdown("""
        <div style='background:#0c0c18; border:1px solid #1a1a2a; border-radius:12px; padding:32px 36px;'>
        <div style='color:#888; font-size:11px; letter-spacing:2px; margin-bottom:20px; text-align:center'>
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
        <div style='margin-top:20px; color:#333; font-size:11px; line-height:1.9; text-align:center'>
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
        at  = datetime.fromisoformat(row["analyzed_at"].replace("Z","")).replace(tzinfo=timezone.utc)
        if (datetime.now(timezone.utc) - at).total_seconds() / 3600 > CACHE_TTL_HOURS:
            return None
        return row
    except: return None

def cache_set(target_id, market_id, target_label, results, winner, bull_prob=50, neutral_prob=30, bear_prob=20):
    try:
        get_supabase().table("analyses").upsert({
            "target_id": target_id, "market_id": market_id,
            "target_label": target_label, "results": results,
            "winner": winner,
            "bull_prob": bull_prob,
            "neutral_prob": neutral_prob,
            "bear_prob": bear_prob,
            "analyzed_at": datetime.now(timezone.utc).isoformat(),
        }, on_conflict="target_id").execute()
    except Exception as e:
        st.warning(f"캐시 저장 오류: {e}")

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
출처(기관명, 날짜, URL, 커뮤니티명)를 반드시 명시하시오.""",

        "neutral": f"""You are a research analyst. You will receive results from 4 sources about {target}{sector_note}. {kr}
## ➡️ {target} 중립 내러티브 수집 (향후 3개월)
### 주요 중립론자 및 기관 [실명·기관명 포함]
### 지배적인 중립 스토리라인 [누가, 왜, 어떤 근거로]
### 핵심 데이터 및 근거 [상충 신호, 불확실성 지표]
### SNS·커뮤니티 중립 여론 [관망·혼재된 의견의 실제 분위기]
### 어닝콜 핵심 포인트 [경영진 발언 중 불확실성·중립 신호]
### 중립 전제 조건
### 중립 내러티브 3줄 요약
출처(기관명, 날짜, URL, 커뮤니티명)를 반드시 명시하시오.""",

        "bear": f"""You are a research analyst. You will receive results from 4 sources about {target}{sector_note}. {kr}
## 📉 {target} 약세 내러티브 수집 (향후 3개월)
### 주요 약세론자 및 기관 [실명·기관명 포함]
### 지배적인 약세 스토리라인 [누가, 왜, 어떤 근거로]
### 핵심 데이터 및 근거 [리스크 지표, 경고 신호]
### SNS·커뮤니티 약세 여론 [Reddit·커뮤니티의 우려·공포 분위기를 Raw하게 반영]
### 어닝콜 핵심 포인트 [경영진 발언 중 리스크·약세 시그널]
### 약세 전제 조건
### 약세 내러티브 3줄 요약
출처(기관명, 날짜, URL, 커뮤니티명)를 반드시 명시하시오.""",

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

        "judge": f"""You are a Chief Investment Strategist. You will review all 6 analyst/critic outputs and select the most plausible narrative with supporting evidence. {kr}

## 핵심 요약
[정확히 4문장. 1:가장 그럴듯한 내러티브. 2:가장 강력한 지지 증거. 3:경쟁 내러티브의 치명적 약점. 4:이 판단을 뒤집을 핵심 변수.]

## ⚡ 최종 판정

### 가장 그럴듯한 내러티브: [강세 / 중립 / 약세]
[설득력 있는 스토리 서술]

### 핵심 근거
**근거 1:** [구체적 수치·기관·데이터]
**근거 2:** [두 번째 지지 근거]
**근거 3:** [세 번째 지지 근거]
**근거 4:** [네 번째 지지 근거]
**근거 5:** [다섯 번째 지지 근거]

### 경쟁 내러티브 탈락 이유

### 확률 분포
**강세장 (유의미한 상승): XX%**
**보합장 (박스권): XX%**
**약세장 (유의미한 하락): XX%**

### 이 판단을 뒤집을 핵심 변수 (상위 3개)
결단하라.""",
    }

# ─── CLAUDE API ────────────────────────────────────────────────────────────────
def call_claude(system: str, user_content: str, max_tokens: int = 4000) -> str:
    api_key = get_user_api_key()
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
    m = re.search(r"가장 그럴듯한 내러티브[^:：]*[：:]\s*\[?([^\]\n]+)\]?", text)
    if not m: return "unknown"
    raw = m.group(1)
    if "강세" in raw: return "bull"
    if "약세" in raw: return "bear"
    return "neutral"

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
def run_analysis(target_id, target_label, market, stock, prompts):
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

    # ── Phase 3: Judge ────────────────────────────────────────────────────────
    st.markdown("**Phase 3 · 최종 판정**")
    status.markdown("⚡ **최종 판정자** 종합 분석 중...")
    try:
        judge_input = "\n\n".join([
            f"[{AGENT_LABELS[a]}]:\n{results.get(a,'')}"
            for a in ["bull","neutral","bear","bull_critic","neutral_critic","bear_critic"]
        ]) + "\n\n가장 그럴듯한 내러티브를 선정하고 근거를 제시하시오."
        results["judge"] = call_claude(prompts["judge"], judge_input, max_tokens=8000)
    except Exception as e:
        results["judge"] = f"⚠️ 오류: {e}"

    progress.progress(1.0)
    status.success("✅ 분석 완료!")

    winner = extract_winner(results.get("judge",""))
    bull_p, neutral_p, bear_p = extract_probs(results.get("judge",""))
    cache_set(
        target_id, market["id"], target_label, results, winner,
        bull_prob=bull_p or 50, neutral_prob=neutral_p or 30, bear_prob=bear_p or 20,
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
        <div style='color:#666; font-size:11px; letter-spacing:2px; margin-bottom:6px'>가장 그럴듯한 내러티브</div>
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
    h1.markdown("<span style='color:#444;font-size:11px'>순위</span>", unsafe_allow_html=True)
    h2.markdown("<span style='color:#444;font-size:11px'>시장</span>", unsafe_allow_html=True)
    h3.markdown("<span style='color:#444;font-size:11px'>종목/지수</span>", unsafe_allow_html=True)
    h4.markdown("<span style='color:#444;font-size:11px'>판정</span>", unsafe_allow_html=True)
    h5.markdown("<span style='color:#444;font-size:11px'>확률 분포</span>", unsafe_allow_html=True)
    h6.markdown("<span style='color:#444;font-size:11px'>분석</span>", unsafe_allow_html=True)
    st.markdown("<hr style='margin:4px 0; border-color:#1a1a2a'>", unsafe_allow_html=True)

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
            f"<div style='color:#ccc;font-size:13px;padding-top:4px'>{row['target_label']}</div>",
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
        <div style='display:flex;gap:8px;font-size:9px;color:#555;margin-top:2px'>
          <span style='color:#00e87a'>↑{bp}%</span>
          <span style='color:#f5c518'>→{np_}%</span>
          <span style='color:#ff3c4e'>↓{rp}%</span>
        </div>"""
        c5.markdown(bar_html, unsafe_allow_html=True)

        c6.markdown(
            f"<div style='color:#333;font-size:10px;padding-top:6px'>{age_label(row['age_hours'])}</div>",
            unsafe_allow_html=True,
        )

        st.markdown("<hr style='margin:2px 0; border-color:#0d0d0d'>", unsafe_allow_html=True)

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
        <p style='color:#444; font-size:11px; letter-spacing:2px; margin:2px 0 0'>
        7-AGENT AI · Tavily 웹검색 + Claude 분석 · 향후 3개월 판정</p>
        """, unsafe_allow_html=True)
    with col_logout:
        key = get_user_api_key()
        st.markdown(f"<div style='color:#333;font-size:10px;text-align:right;margin-top:6px'>🔑 ...{key[-4:]}</div>", unsafe_allow_html=True)
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
        at = datetime.fromisoformat(cached["analyzed_at"].replace("Z","")).replace(tzinfo=timezone.utc)
        age_h = (datetime.now(timezone.utc) - at).total_seconds() / 3600
        remaining = CACHE_TTL_HOURS - age_h
        with col_a:
            if st.button(f"🗄 공유 캐시 불러오기 ({remaining:.0f}시간 남음, 토큰 0 소모)", type="primary", use_container_width=True):
                st.session_state.update({
                    "res_results": cached["results"],
                    "res_winner":  cached.get("winner","unknown"),
                    "res_cached_at": cached["analyzed_at"],
                    "show_results": True,
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
                prompts = build_system_prompts(market, stock)
                results, winner = run_analysis(target_id, target_label, market, stock, prompts)
                st.session_state.update({
                    "res_results":   results,
                    "res_winner":    winner,
                    "res_cached_at": None,
                    "show_results":  True,
                })
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
