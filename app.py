import streamlit as st
import anthropic
import re
from datetime import datetime, timezone
from supabase import create_client
from tavily import TavilyClient

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

# ─── WEB SEARCH (Tavily) ──────────────────────────────────────────────────────
@st.cache_resource
def get_tavily():
    return TavilyClient(api_key=st.secrets["TAVILY_API_KEY"])

def web_search(queries: list[str]) -> str:
    """Tavily로 복수 쿼리 검색 후 결과를 하나의 텍스트로 합산"""
    client = get_tavily()
    all_results = []
    for query in queries:
        try:
            resp = client.search(query, max_results=4, search_depth="advanced")
            for r in resp.get("results", []):
                title   = r.get("title", "")
                url     = r.get("url", "")
                content = r.get("content", "")[:600]
                all_results.append(f"■ {title}\n출처: {url}\n{content}")
        except Exception as e:
            all_results.append(f"[검색 실패: {query} — {e}]")
    return "\n\n---\n\n".join(all_results)

def build_search_queries(target: str, direction: str, market_index: str) -> list[str]:
    year = datetime.now().year
    if direction == "bull":
        return [
            f"{target} {market_index} bull case buy recommendation analyst target price {year}",
            f"{target} upside forecast earnings growth positive outlook {year}",
        ]
    elif direction == "neutral":
        return [
            f"{target} {market_index} neutral hold sideways range-bound forecast {year}",
            f"{target} mixed outlook uncertainty analyst cautious {year}",
        ]
    else:
        return [
            f"{target} {market_index} bear case sell recommendation downside risk {year}",
            f"{target} correction warning negative outlook analyst {year}",
        ]

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

def cache_set(target_id, market_id, target_label, results, winner):
    try:
        get_supabase().table("analyses").upsert({
            "target_id": target_id, "market_id": market_id,
            "target_label": target_label, "results": results,
            "winner": winner,
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
            "target_id,market_id,target_label,winner,analyzed_at"
        ).order("analyzed_at", desc=True).execute()
        rows = []
        for r in resp.data:
            at = datetime.fromisoformat(r["analyzed_at"].replace("Z","")).replace(tzinfo=timezone.utc)
            age_h = (datetime.now(timezone.utc) - at).total_seconds() / 3600
            if age_h <= CACHE_TTL_HOURS:
                rows.append({**r, "age_hours": round(age_h, 1)})
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
        "bull": f"""You are a research analyst. You will receive recent web search results about {target}{sector_note}.
Based on these results, compile and organize the REAL bullish narratives from actual analysts and institutions. {kr}
## 📈 {target} 강세 내러티브 수집 (향후 3개월)
### 주요 강세론자 및 기관 [실명·기관명·목표가 포함]
### 지배적인 강세 스토리라인 [누가, 왜, 어떤 근거로]
### 핵심 데이터 및 근거 [수치·지표 직접 인용]
### 강세 전제 조건
### 강세 내러티브 3줄 요약
출처(기관명, 날짜, URL)를 반드시 명시하시오.""",

        "neutral": f"""You are a research analyst. You will receive recent web search results about {target}{sector_note}.
Based on these results, compile and organize the REAL neutral/sideways narratives from actual analysts. {kr}
## ➡️ {target} 중립 내러티브 수집 (향후 3개월)
### 주요 중립론자 및 기관 [실명·기관명 포함]
### 지배적인 중립 스토리라인 [누가, 왜, 어떤 근거로]
### 핵심 데이터 및 근거 [상충 신호, 불확실성 지표]
### 중립 전제 조건
### 중립 내러티브 3줄 요약
출처(기관명, 날짜, URL)를 반드시 명시하시오.""",

        "bear": f"""You are a research analyst. You will receive recent web search results about {target}{sector_note}.
Based on these results, compile and organize the REAL bearish narratives from actual analysts and institutions. {kr}
## 📉 {target} 약세 내러티브 수집 (향후 3개월)
### 주요 약세론자 및 기관 [실명·기관명 포함]
### 지배적인 약세 스토리라인 [누가, 왜, 어떤 근거로]
### 핵심 데이터 및 근거 [리스크 지표, 경고 신호]
### 약세 전제 조건
### 약세 내러티브 3줄 요약
출처(기관명, 날짜, URL)를 반드시 명시하시오.""",

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
def call_claude(system: str, user_content: str) -> str:
    api_key = get_user_api_key()
    if not api_key:
        raise RuntimeError("로그인이 필요합니다.")
    client = anthropic.Anthropic(api_key=api_key)
    resp = client.messages.create(
        model="claude-3-7-sonnet-20250219",
        max_tokens=4000,
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

    # ── Phase 1: 웹검색 → Claude 분석 ─────────────────────────────────────────
    st.markdown("**Phase 1 · 내러티브 수집 (Tavily 웹검색 + Claude 분석)**")
    cols = st.columns(3)
    areas = {a: cols[i].empty() for i, a in enumerate(["bull","neutral","bear"])}

    for i, (agent, direction) in enumerate([("bull","bull"),("neutral","neutral"),("bear","bear")]):
        status.markdown(f"🔍 **{AGENT_LABELS[agent]}** — 웹 검색 중...")
        areas[agent].info(f"{AGENT_LABELS[agent]}\n🔍 Tavily 검색 중...")

        try:
            # 1) 웹 검색
            queries = build_search_queries(target_short, direction, market["index"])
            search_results = web_search(queries)

            # 2) 검색 결과를 Claude에게 전달
            areas[agent].info(f"{AGENT_LABELS[agent]}\n🤖 Claude 분석 중...")
            status.markdown(f"🤖 **{AGENT_LABELS[agent]}** — Claude 분석 중...")

            user_content = f"""다음은 오늘({datetime.now().strftime('%Y년 %m월 %d일')}) 기준 {target_label}에 관한 최신 웹 검색 결과입니다:

{search_results}

위 검색 결과를 바탕으로, 실제 시장에서 {'상승' if direction=='bull' else ('보합' if direction=='neutral' else '하락')} 방향성을 주장하는 애널리스트와 기관의 내러티브를 수집·정리하십시오."""

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
        results["judge"] = call_claude(prompts["judge"], judge_input)
    except Exception as e:
        results["judge"] = f"⚠️ 오류: {e}"

    progress.progress(1.0)
    status.success("✅ 분석 완료!")

    winner = extract_winner(results.get("judge",""))
    cache_set(target_id, market["id"], target_label, results, winner)
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
    total = 3 + 20 * 3
    done  = len(rows)
    pct   = int(done / total * 100)
    st.markdown("### 📊 공유 분석 현황 (48시간 내)")
    st.progress(pct/100, text=f"{done} / {total} 완료 ({pct}%) — 모든 사용자 공유")
    if not rows:
        st.caption("아직 분석 없음. 첫 분석을 시작해보세요!")
        return
    market_names = {"kospi200":"🇰🇷 KOSPI 200","sp500":"🇺🇸 S&P 500","nikkei225":"🇯🇵 닛케이 225"}
    for mid in ["kospi200","sp500","nikkei225"]:
        group = [r for r in rows if r["market_id"] == mid]
        if not group: continue
        with st.expander(f"{market_names[mid]} · {len(group)}개 완료", expanded=False):
            for i, row in enumerate(group, 1):
                c1,c2,c3,c4 = st.columns([0.5,2.5,1,1])
                c1.markdown(f"`#{i}`")
                c2.markdown(row["target_label"])
                c3.markdown(winner_badge(row.get("winner","")))
                c4.markdown(age_label(row["age_hours"]))

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
