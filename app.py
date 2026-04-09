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

# ─── QUERY BUILDER ─────────────────────────────────────────────────────────────
# ─── QUERY BUILDER ─────────────────────────────────────────────────────────────
def build_queries(target, direction, market_index, sector="", market_id="sp500"):
    # ✅ 수정 1: 검색어에서 year, month를 제거하여 검색 엔진의 자유도를 높임
    sn = f" {sector}" if sector else ""
    
    if direction == "bull":
        tq = [f"{target} stock buy rating target price upgrade analyst",
              f"{target}{sn} earnings beat revenue growth bullish",
              f"{market_index} bull market rally forecast"]
        eq = [f"Investment research bullish case {target} price target upside",
              f"Expert analysis {target} stock outperform",
              f"{target}{sn} undervalued catalyst growth analyst recommendation"]
              
    elif direction == "neutral":
        tq = [f"{target} stock hold neutral rating mixed outlook",
              f"{target} sideways range-bound uncertainty",
              f"{market_index} flat market uncertainty"]
        eq = [f"Why {target} fairly valued neutral balanced risks",
              f"{target}{sn} wait cautious analyst",
              f"{market_index} sideways market competing forces"]
              
    else:
        tq = [f"{target} stock sell downgrade target price cut analyst",
              f"{target}{sn} earnings miss decline bearish risk",
              f"{market_index} market correction crash risk"]
        eq = [f"Investment research bearish short thesis {target} downside",
              f"Expert column {target} overvalued headwinds",
              f"{target}{sn} structural decline disruption analysis"]
              
    return {"tavily": tq, "exa_report": eq, "exa_sns": _build_sns_queries(target, direction, market_id)}


# ─── SEARCH FUNCTIONS ──────────────────────────────────────────────────────────
def search_tavily(queries):
    client = get_tavily()
    seen, results = set(), []
    for q in queries:
        try:
            resp = client.search(q, max_results=4, search_depth="advanced", include_answer=False)
            for r in resp.get("results", []):
                url = r.get("url","")
                if url in seen: continue
                seen.add(url)
                results.append({"title":r.get("title",""),"url":url,"content":r.get("content","")[:700],"date":r.get("published_date","")})
        except Exception as e: 
            # 🚨 터미널에 에러 출력
            print(f"[Tavily 뉴스 검색 실패] 원인: {e}")
    return results

def search_exa_reports(queries, recent_days=90):
    client = get_exa()
    start = (datetime.now() - timedelta(days=recent_days)).strftime("%Y-%m-%dT00:00:00.000Z")
    seen, results = set(), []
    
    for q in queries:
        try:
            # ✅ 수정 2: Exa API 파라미터 에러 해결 (dict -> True)
            resp = client.search_and_contents(
                q, 
                num_results=4, 
                use_autoprompt=True,
                text=True,         # 변경됨
                highlights=True,   # 변경됨
                start_published_date=start, 
                include_domains=EXA_FINANCIAL_DOMAINS
            )
            
            for r in resp.results:
                url = getattr(r, "url", "") or ""
                if not url or url in seen: continue
                seen.add(url)
                
                hl = getattr(r, "highlights", []) or []
                content = " … ".join(hl) if hl else (getattr(r, "text", "") or "")[:800]
                
                results.append({
                    "title": getattr(r, "title", "") or "",
                    "url": url,
                    "content": content[:800],
                    "date": getattr(r, "published_date", "") or ""
                })
        except Exception as e:
            # 🚨 터미널에 에러 출력 (Exa 잔액이 깎이지 않는 진짜 이유를 여기서 확인 가능)
            print(f"[Exa 리포트 검색 실패] 쿼리: {q} \n에러 상세: {e}")
            
    return results

def search_tavily_sns(queries, market_id="sp500"):
    client = get_tavily()
    seen, results = set(), []
    domains = _get_sns_domains(market_id)
    for q in queries:
        try:
            resp = client.search(q, max_results=5, search_depth="advanced", include_domains=domains)
            for r in resp.get("results",[]):
                url = r.get("url") or ""
                if url in seen: continue
                seen.add(url)
                results.append({
                    "title": r.get("title") or "",
                    "url": url,
                    "content": (r.get("content") or "")[:600],
                    "date": r.get("published_date") or "",
                    "platform": _detect_platform(url)
                })
        except Exception as e:
            print(f"[Tavily SNS 고급검색 실패] {e}")
            # fallback basic search
            try:
                resp2 = client.search(q, max_results=4, search_depth="basic")
                for r in resp2.get("results",[]):
                    url = r.get("url") or ""
                    if url in seen or not any(d in url for d in ["reddit","stocktwits","naver","minkabu","kabutan"]): continue
                    seen.add(url)
                    results.append({
                        "title": r.get("title") or "",
                        "url": url,
                        "content": (r.get("content") or "")[:600],
                        "date": r.get("published_date") or "",
                        "platform": _detect_platform(url)
                    })
            except Exception as e2: 
                print(f"[Tavily SNS 기본검색 실패] {e2}")
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
            print(f"[현재가 검색 실패] {e}")
            
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

def combined_search(target, direction, market_index, sector="", ticker_raw="", market_id="sp500"):
    qs = build_queries(target, direction, market_index, sector, market_id)
    tr = search_tavily(qs["tavily"])
    er = search_exa_reports(qs["exa_report"])
    sr = search_tavily_sns(qs["exa_sns"], market_id=market_id)
    et = fetch_earnings_transcript(ticker_raw, target_name=target, market_id=market_id) if ticker_raw else "[지수 — 어닝콜 해당 없음]"

    # 90일 커트라인 유지 (파이썬 날짜 파싱 에러 완벽 차단)
    cutoff_str = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d")

    def fmt(items, label, show_p=False):
        if not items: return f"【{label}】\n결과 없음\n"
        lines=[f"【{label}】"]
        valid_count = 0
        
        for r in items:
            date_str = r.get("date") or ""  
            
            # 90일 이전 데이터 깔끔하게 무시
            if date_str and len(date_str) >= 10 and date_str[:10] < cutoff_str:
                continue 

            valid_count += 1
            ds = f" ({date_str[:10]})" if date_str else ""
            pl = f"[{r.get('platform','')}] " if show_p and r.get("platform") else ""
            lines.append(f"■ {pl}{r.get('title','')}{ds}")
            if r.get("url"): lines.append(f"  {r['url']}")
            if r.get("content"): lines.append(f"  {r['content']}")
            lines.append("")
            
        if valid_count == 0:
            return f"【{label}】\n최근 3개월 내 유의미한 결과 없음\n"
            
        return "\n".join(lines)

    dir_ko="강세" if direction=="bull" else "중립" if direction=="neutral" else "약세"
    hdr=f"=== {target}[{dir_ko}] ({datetime.now().strftime('%Y-%m-%d')}) ===\n소스: Tavily+Exa+SNS+어닝콜\n"
    return hdr + "\n\n".join([
        fmt(tr, "① 최신 뉴스·애널리스트"),
        fmt(er, "② 금융 리포트·칼럼"),
        fmt(sr, "③ SNS·커뮤니티 여론", show_p=True),
        f"④ 어닝콜·실적발표\n{et}",
    ])

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
            f"However, **YOUR ENTIRE FINAL OUTPUT MUST BE TRANSLATED TO AND WRITTEN IN KOREAN (한국어)** following the exact Korean headers provided below."
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
### 주요 강세론자 및 기관 [실명·기관명·최신 목표가 포함]
### 지배적인 강세 스토리라인 [누가, 왜, 어떤 근거로]
### 핵심 데이터 및 근거 [수치·지표 직접 인용]
### SNS·커뮤니티 강세 여론 [실제 분위기 Raw 요약]
### 어닝콜 핵심 포인트 [경영진 발언 중 강세 근거]
### 강세 전제 조건
### 강세 내러티브 3줄 요약
출처(기관명, 날짜, URL)를 반드시 명시하시오.{base_warn}""",

        "neutral": f"""{lang_instruction}
## ➡️ {target} 중립 내러티브 수집 (향후 3개월)
### 주요 중립론자 및 기관
### 지배적인 중립 스토리라인
### 핵심 데이터 및 근거
### SNS·커뮤니티 중립 여론
### 어닝콜 핵심 포인트 [불확실성·중립 신호]
### 중립 전제 조건
### 중립 내러티브 3줄 요약
출처를 반드시 명시하시오.{base_warn}""",

        "bear": f"""{lang_instruction}
## 📉 {target} 약세 내러티브 수집 (향후 3개월)
### 주요 약세론자 및 기관
### 지배적인 약세 스토리라인
### 핵심 데이터 및 근거
### SNS·커뮤니티 약세 여론 [우려·공포 분위기]
### 어닝콜 핵심 포인트 [리스크·약세 시그널]
### 약세 전제 조건
### 약세 내러티브 3줄 요약
출처를 반드시 명시하시오.{base_warn}""",

        "bull_critic": f"""{lang_instruction}
You are an adversarial analyst stress-testing bullish narratives.
## 🔥 강세 내러티브 비판
### 근거의 취약점 [데이터 오독, 과거 데이터 사용 여부]
### 강세가 외면한 반대 증거
### 논리적 허점
### 향후 3개월 강세 붕괴 리스크
### 강세 신뢰도 [1-10점 및 2줄 평가]""",

        "neutral_critic": f"""{lang_instruction}
You are an adversarial analyst stress-testing neutral narratives.
## 🔥 중립 내러티브 비판
### 거짓 균형의 함정
### 중립이 외면한 방향성 신호
### 역사적 실패 사례
### 방향성 강제 촉매
### 중립 신뢰도 [1-10점 및 2줄 평가]""",

        "bear_critic": f"""{lang_instruction}
You are an adversarial analyst stress-testing bearish narratives.
## 🔥 약세 내러티브 비판
### 과거 패턴 오남용
### 약세가 외면한 회복력 근거
### 같은 약세 논리의 실패 전례
### 과소평가한 정책 대응 [{cb}]
### 약세 신뢰도 [1-10점 및 2줄 평가]""",

        "judge": f"""{lang_instruction}
You are the Chief Investment Strategist reviewing a 6-agent debate.

⚠️ 절대 금지:
- 【실시간 현재가】에 없는 주가 수치 조작 금지
- {cutoff_str_ko} 이전의 과거 목표가 및 분석 내용 채택 엄격히 금지

## 핵심 요약
[정확히 4문장. 1:가장 그럴듯한 최신 내러티브. 2:강력한 지지 근거. 3:경쟁 내러티브의 약점. 4:판단을 뒤집을 핵심 변수.]

## ⚡ 최종 판정

### 가장 그럴듯한 내러티브: [강세 / 중립 / 약세]
[설득력 있는 스토리. 현재가 기준. 검색 결과 사실만.]

### 현재 가격 기준 상황
[실시간 현재가 정보 요약]

### 최신 핵심 근거
**근거 1:** [출처 + 사실]
**근거 2:** [출처 + 사실]
**근거 3:** [출처 + 사실]
**근거 4:** [출처 + 사실]
**근거 5:** [출처 + 사실]

### 경쟁 내러티브 탈락 이유

### 확률 분포
**강세장 (유의미한 상승): XX%**
**보합장 (박스권): XX%**
**약세장 (유의미한 하락): XX%**

### 핵심 변수 (상위 3개)
결단하라.""",
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
    sector     = stock[2] if stock else ""
    ticker_raw = stock[0] if stock else ""

    # ── Phase 1: 내러티브 수집 ─────────────────────────────────────────────────
    agents_p1 = [("bull","bull"),("neutral","neutral"),("bear","bear")]
    for i,(agent,direction) in enumerate(agents_p1):
        dir_label = {"bull":"강세","neutral":"중립","bear":"약세"}[direction]
        pct = 0.05 + i*0.13
        update_progress(target_id, pct, f"🔍 {AGENT_LABELS[agent]} — 검색 중...")
        try:
            search_results = combined_search(
                target_short, direction, market["index"],
                sector=sector, ticker_raw=ticker_raw, market_id=market["id"],
            )
            update_progress(target_id, pct+0.06, f"🤖 {AGENT_LABELS[agent]} — LLM 분석 중...")
            user_content = (
                f"다음은 오늘({datetime.now().strftime('%Y년 %m월 %d일')}) 기준 "
                f"{target_label} 검색 결과입니다:\n\n{search_results}\n\n"
                f"위 결과를 바탕으로 {dir_label} 내러티브를 수집·정리하십시오."
            )
            results[agent] = call_llm(prompts[agent], user_content, market_id=market["id"])
        except Exception as e:
            results[agent] = f"⚠️ 오류: {e}"

    # ── Phase 2: 비판 ──────────────────────────────────────────────────────────
    update_progress(target_id, 0.45, "Phase 2 · 비판 검증 시작...")
    critic_map = {"bull_critic":("bull","강세"),"neutral_critic":("neutral","중립"),"bear_critic":("bear","약세")}
    for i,agent in enumerate(["bull_critic","neutral_critic","bear_critic"]):
        src, label = critic_map[agent]
        pct = 0.45 + i*0.12
        update_progress(target_id, pct, f"🔥 {AGENT_LABELS[agent]} — 비판 중...")
        try:
            user_content = f"[{label} 내러티브]:\n{results.get(src,'')}\n\n위 내러티브를 냉정하게 비판하시오."
            results[agent] = call_llm(prompts[agent], user_content, market_id=market["id"])
        except Exception as e:
            results[agent] = f"⚠️ 오류: {e}"

    # ── Phase 3: 최종 판정 ─────────────────────────────────────────────────────
    update_progress(target_id, 0.82, "📡 현재 주가 조회 + 최종 판정 중...")
    try:
        price_ctx = fetch_current_price(target_short, ticker_raw, market["id"])
        judge_input = (
            f"【실시간 현재가 — 반드시 이 가격 기준으로 판단하시오】\n{price_ctx}\n\n{'='*50}\n\n"
            + "\n\n".join(f"[{AGENT_LABELS[a]}]:\n{results.get(a,'')}"
                         for a in ["bull","neutral","bear","bull_critic","neutral_critic","bear_critic"])
            + "\n\n위 토론과 현재가를 종합하여 최종 판정을 내리시오."
        )
        results["judge"] = call_llm(prompts["judge"], judge_input, max_tokens=8000, market_id=market["id"])
    except Exception as e:
        results["judge"] = f"⚠️ 오류: {e}"

    # ── 저장 ───────────────────────────────────────────────────────────────────
    update_progress(target_id, 0.98, "✅ 저장 중...")
    bp, np_, rp = extract_probs(results.get("judge",""))
    bp = bp or 50; np_ = np_ or 30; rp = rp or 20
    winner = winner_from_probs(bp, np_, rp)
    if bp==50 and np_==30 and rp==20:
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
    rows  = load_leaderboard()
    total = 3 + 20*3
    done  = len([r for r in rows if r.get("status","done")!="running"])
    running = len([r for r in rows if r.get("status")=="running"])
    pct = int(done/total*100)

    st.markdown("### 📊 추천 강도 랭킹 (48시간 내 · 강세 확률 높은 순)")
    label = f"{done}/{total} 완료 ({pct}%)"
    if running: label += f" · 🔄 {running}개 분석 진행 중"
    st.progress(pct/100, text=label)
    if not rows: st.caption("아직 분석 없음."); return

    mf={"kospi200":"🇰🇷","sp500":"🇺🇸","nikkei225":"🇯🇵"}
    h1,h2,h3,h4,h5,h6=st.columns([0.4,0.3,2.2,1.0,2.5,0.8])
    for h,t in zip([h1,h2,h3,h4,h5,h6],["순위","시장","종목/지수","판정","확률 분포","분석"]):
        h.markdown(f"<span style='color:#4a5568;font-size:11px'>{t}</span>",unsafe_allow_html=True)
    st.markdown("<hr style='margin:4px 0;border-color:#e2e6ef'>",unsafe_allow_html=True)

    for rank,row in enumerate(rows,1):
        bp=row.get("bull_prob") or 0
        np_=row.get("neutral_prob") or 0
        rp=row.get("bear_prob") or 0
        w=row.get("winner","")
        flag=mf.get(row.get("market_id",""),"")
        is_running = row.get("status")=="running"
        rank_color="#00e87a" if bp>=55 else "#f5c518" if bp>=45 else "#ff3c4e"

        c1,c2,c3,c4,c5,c6=st.columns([0.4,0.3,2.2,1.0,2.5,0.8])
        c1.markdown(f"<div style='color:{rank_color};font-weight:900;font-size:14px;padding-top:4px'>#{rank}</div>",unsafe_allow_html=True)
        c2.markdown(f"<div style='font-size:18px;padding-top:2px'>{flag}</div>",unsafe_allow_html=True)
        c3.markdown(f"<div style='color:#4a5568;font-size:13px;padding-top:4px'>{row['target_label']}</div>",unsafe_allow_html=True)
        if is_running:
            c4.markdown("🔄 분석중")
        else:
            c4.markdown(winner_badge(w))

        if is_running:
            c5.markdown("<div style='color:#6b7a9e;font-size:11px;padding-top:6px'>진행 중...</div>",unsafe_allow_html=True)
        else:
            bar=f"""<div style='display:flex;gap:2px;align-items:center;margin-top:6px'>
            <div style='width:{bp}%;height:8px;background:#00e87a;border-radius:2px 0 0 2px'></div>
            <div style='width:{np_}%;height:8px;background:#f5c518'></div>
            <div style='width:{rp}%;height:8px;background:#ff3c4e;border-radius:0 2px 2px 0'></div>
            </div><div style='display:flex;gap:8px;font-size:9px;color:#6b7a9e;margin-top:2px'>
            <span style='color:#00e87a'>↑{bp}%</span><span style='color:#f5c518'>→{np_}%</span><span style='color:#ff3c4e'>↓{rp}%</span>
            </div>"""
            c5.markdown(bar,unsafe_allow_html=True)
        c6.markdown(f"<div style='color:#374151;font-size:10px;padding-top:6px'>{'진행중' if is_running else age_label(row['age_hours'])}</div>",unsafe_allow_html=True)
        st.markdown("<hr style='margin:2px 0;border-color:#f0f0f0'>",unsafe_allow_html=True)

# ─── MAIN ──────────────────────────────────────────────────────────────────────
def main():
    col_title, col_info = st.columns([5,1])
    with col_title:
        qw = get_ollama_model('kospi200')
        ll = get_ollama_model('sp500')
        gm = get_ollama_model('nikkei225')
        st.markdown(f"""<h1 style='background:linear-gradient(90deg,#4fc3f7,#00e87a,#f5c518,#ff3c4e,#e040fb);
        -webkit-background-clip:text;-webkit-text-fill-color:transparent;font-size:26px;margin:0'>
        ⚡ 시장 방향 판정 엔진</h1>
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
    options = ["📊 지수 전체"] + [f"{t} · {n} ({s})" for t,n,s in stocks]
    choice = st.selectbox("",options,label_visibility="collapsed")

    if choice == "📊 지수 전체":
        stock, target_id = None, market["id"]
        target_label = f"{market['flag']} {market['index']}"
    else:
        idx = options.index(choice)-1
        stock = stocks[idx]
        target_id = f"{market['id']}_{stock[0]}"
        target_label = f"{stock[1]} ({stock[0]})"

    st.markdown(f"**선택:** {target_label}")
    cached = cache_get(target_id)
    col_a, col_b = st.columns([3,1])

    if cached:
        status = cached.get("status","done")
        if status == "running":
            at = datetime.fromisoformat(cached["analyzed_at"].replace("Z","")).replace(tzinfo=timezone.utc)
            elapsed = int((datetime.now(timezone.utc)-at).total_seconds()/60)
            pct = float(cached.get("progress") or 0.0)
            msg = cached.get("status_msg") or "분석 준비 중..."
            st.info(f"⏳ **{target_label}** 백그라운드 분석 중 ({elapsed}분 경과) — 브라우저 꺼도 계속 진행됩니다")
            st.progress(pct, text=msg)
            cr, cc = st.columns([1,1])
            with cr:
                if st.button("🔄 새로고침", use_container_width=True): st.rerun()
            with cc:
                if elapsed > 30 and st.button("⚠️ 재시작", use_container_width=True):
                    cache_delete(target_id); st.rerun()
            import time; time.sleep(3); st.rerun()
        else:
            at = datetime.fromisoformat(cached["analyzed_at"].replace("Z","")).replace(tzinfo=timezone.utc)
            age_h = (datetime.now(timezone.utc)-at).total_seconds()/3600
            remaining = CACHE_TTL_HOURS-age_h
            with col_a:
                if st.button(f"🗄 캐시 불러오기 ({remaining:.0f}시간 남음, 0 토큰)", type="primary", use_container_width=True):
                    bp=cached.get("bull_prob") or 50
                    np_=cached.get("neutral_prob") or 30
                    rp=cached.get("bear_prob") or 20
                    st.session_state.update({
                        "res_results":cached["results"],
                        "res_winner":winner_from_probs(bp,np_,rp),
                        "res_cached_at":cached["analyzed_at"],
                        "show_results":True,
                    })
            with col_b:
                if st.button("🗑 재분석", use_container_width=True):
                    cache_delete(target_id); st.session_state.pop("show_results",None)
                    st.success("캐시 삭제됨."); st.rerun()
    else:
        with col_a:
            if st.button(f"▶ {target_label} 분석 시작", type="primary", use_container_width=True):
                st.session_state.pop("show_results",None)
                prompts = build_system_prompts(market, stock)
                cache_set_running(target_id, market["id"], target_label)

                def _bg_task():
                    _BG_KEYS[target_id] = ""
                    try:
                        _run_analysis_core(target_id, target_label, market, stock, prompts)
                    except Exception as e:
                        print(f"백그라운드 오류 [{target_id}]: {e}")
                        try:
                            cache_set(target_id, market["id"], target_label, {}, "unknown",
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
