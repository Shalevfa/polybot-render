#!/usr/bin/env python3
"""Live dashboard HTTP server for the BTC 5m paper bot.

Reads status.json + trades.db and makes light, cached public API calls.
Does not import or touch the trading loop.
"""
from __future__ import annotations

import json
import math
import os
import re
import sqlite3
import threading
import time
import traceback
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from zoneinfo import ZoneInfo

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
WEB = os.path.dirname(os.path.abspath(__file__))
STATUS_PATH = os.path.join(ROOT, "status.json")
DB_PATH = os.path.join(ROOT, "trades.db")
LOG_PATH = os.path.join(ROOT, "bot.log")
HIST_PATH = os.path.join(WEB, "price_hist.json")
INDEX_PATH = os.path.join(WEB, "index.html")
SIM1000_PATH = os.path.join(WEB, "sim1000.html")
BOT_PATH = os.path.join(WEB, "bot.html")

sys.path.insert(0, ROOT)
import bot_settings
from bot_settings import LIVE

def estimates_of(row) -> tuple:
    """Purchase-time PnL if the trade wins or loses. Prefer stored values; otherwise derive like the engine."""
    def _get(k):
        if isinstance(row, dict):
            return row.get(k)
        try:
            return row[k]
        except Exception:
            return None
    ep, el = _get("est_profit"), _get("est_loss")
    sh, cost, fe, px = _get("shares"), _get("cost"), _get("fee"), _get("avg_price")
    if fe is None and sh is not None and px is not None:
        try:
            fe = round(float(sh) * 0.07 * float(px) * (1.0 - float(px)), 5)
        except Exception:
            fe = 0.0
    if ep is None and sh is not None and cost is not None:
        ep = round(float(sh) - float(cost) - float(fe or 0), 5)
    if el is None and cost is not None:
        el = round(-(float(cost) + float(fe or 0)), 5)
    return ep, el

def fmt_estimate(ep, el) -> str:
    bits = []
    if ep is not None:
        sign = "+" if ep >= 0 else "-"
        bits.append(f"רווח משוער {sign}${abs(ep):.2f}")
    if el is not None:
        sign = "+" if el >= 0 else "-"
        bits.append(f"הפסד אם מפסיד {sign}${abs(el):.2f}")
    return " · ".join(bits)

HOST = "127.0.0.1"
PORT = int(os.environ.get("POLYBOT_DASH_PORT", "8765"))
WIN = 300
TZ = ZoneInfo("Asia/Jerusalem")
GAMMA = "https://gamma-api.polymarket.com"
CLOB = "https://clob.polymarket.com"
CRYPTO_PRICE = "https://polymarket.com/api/crypto/crypto-price"

STRATEGY_ORDER = [
    "late_model",
    "late_model_raw",
    "early_value",
    "naive_momentum",
    "every_round",
    "momentum_late",
    "latency_hunter",
    "genetic",
    "manual",
]

# left_lo/hi = seconds remaining; el_lo/hi = seconds elapsed
ACTIVE = {
    "late_model": {"left_lo": 5, "left_hi": 75},
    "late_model_raw": {"left_lo": 5, "left_hi": 75},
    "early_value": {"el_lo": 60, "el_hi": 210},
    "naive_momentum": {"left_lo": 50, "left_hi": 65},
    "every_round": {"left_lo": 5, "left_hi": 150},
    "late_add": {"left_lo": 2, "left_hi": 10},
    "momentum_late": {"left_lo": 10, "left_hi": 90},
    "latency_hunter": {"left_lo": 20, "left_hi": 300},
    "genetic": {"left_lo": 5, "left_hi": 75},
    "manual": {},
}

DOC_HE = {
    "late_model": "הראשית. בדקה האחרונה קונה כשיש פער של לפחות 4 סנט מול השווי ההוגן.",
    "late_model_raw": "צל של הראשית: אותה כניסה לפי המודל בלבד, בלי לערבב עם ספר הפקודות.",
    "early_value": "באמצע הסיבוב קונה את הצד שהשוק הגזים במחירו, אם יש פער של 6 סנט.",
    "naive_momentum": "קונה את הצד שעליו הביטקוין נמצא כרגע, כ-60 שניות לסוף, במחיר עד 85 סנט.",
    "every_round": "נכנסת לכל סיבוב, בערך שתי דקות וחצי לפני הסוף, בצד שהמודל מעדיף.",
    "late_add": "ב-10 השניות האחרונות מוסיפה לעסקה מרוויחה, אם המחיר עדיין עד 95 סנט.",
    "momentum_late": "ב-90 השניות האחרונות מסתכלת על התנועה של 30–60 השניות הקודמות, ונכנסת רק בכיוון המומנטום כשיש פער של 4 סנט לפחות.",
    "latency_hunter": "צייד ההשהיה. מודד מומנטום של 15/30/60 שניות, מעריך את הסיכוי ל-Up, ונכנס רק כשיש יתרון נטו של 6 סנט אחרי עמלה והחלקה.",
    "genetic": "בוט גנטי. מחזיק אוכלוסייה קטנה של פרמטרים, בודק כל גרסה בצל, והאלופה סוחרת בקופה דמו של 100 דולר. מתפתח כל כמה סיבובים.",
    "manual": "בוט ידני. לא נכנס לבד — קונים Up או Down מעמוד הניהול שלו, בכסף דמו בלבד.",
}

SKIP_HE = {
    "mom_weak": "המומנטום חלש מדי",
    "mom_reversal": "המומנטום מתהפך",
    "mom_nodata": "אין מספיק היסטוריית מחיר",
    "no_edge": "אין יתרון במחיר",
    "warmup": "חימום אחרי התחברות מחדש",
    "sources_disagree": "המקורות לא מסכימים",
    "stale_chainlink": "Chainlink לא טרי",
    "nowcast_fallback": "נתוני עזר בלבד",
    "no_model": "אין מודל",
    "no_book": "אין ספר פקודות",
    "price_too_high": "המחיר גבוה מדי",
    "strategy_disagree": "המודל והספר לא מסכימים",
    "book_disagree": "אי-הסכמה עם הספר",
    "strategy_other": "התנאים לא התקיימו",
    "no_chainlink": "אין נתון Chainlink",
    "stale_feed": "הזנה לא טרייה",
    "no_asks": "אין מוכרים",
    "ask too high": "המחיר גבוה מדי",
    "no upside": "המחיר גבוה מדי",
    "no asks": "אין מוכרים",
    "not winning": "העסקה הקיימת כבר לא מובילה",
    "spot not clear": "אין כיוון ברור",
    "no late pos": "אין פוזיציה להוסיף עליה",
}

GATE_HE = {
    "ok": "תקינה",
    "stale_chainlink": "Chainlink לא טרי",
    "sources_disagree": "המקורות לא מסכימים",
    "warmup": "חימום אחרי התחברות מחדש",
    "nowcast_fallback": "נתוני עזר בלבד",
    "no_model": "אין מודל",
    "stale_feed": "הזנה לא טרייה",
    "no_chainlink": "אין נתון Chainlink",
    "too_few_sources": "אין מספיק מקורות",
    "stale_nowcast": "תחזית לא טרייה",
    "gate_error": "שגיאת שער",
}

_lock = threading.RLock()
_hist_slug = None
_hist = []  # list of {t, btc, nowcast}
_tokens = {}  # slug -> {Up: id, Down: id, ts}
_book_cache = {"slug": None, "ts": 0.0, "data": None}
_open_api_cache = {}  # slug -> (px, ts)
_http = None


def session() -> requests.Session:
    global _http
    if _http is None:
        s = requests.Session()
        retry = Retry(total=1, backoff_factor=0.15, status_forcelist=(502, 503, 504))
        ad = HTTPAdapter(max_retries=retry, pool_connections=4, pool_maxsize=8)
        s.mount("https://", ad)
        s.mount("http://", ad)
        s.headers["User-Agent"] = "polybot-live-dashboard/1.0"
        _http = s
    return _http


def now_ts() -> float:
    return time.time()


def window_start(t: float | None = None) -> int:
    t = now_ts() if t is None else t
    return int(t // WIN * WIN)


def iso_utc(ts: float) -> str:
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def ncdf(x: float) -> float:
    return 0.5 * (1 + math.erf(x / math.sqrt(2)))


def read_status() -> tuple[dict | None, float | None]:
    try:
        st = os.stat(STATUS_PATH)
        age = now_ts() - st.st_mtime
        with open(STATUS_PATH, "r", encoding="utf-8") as f:
            raw = f.read()
        return json.loads(raw), age
    except (OSError, json.JSONDecodeError):
        return None, None


def db_connect() -> sqlite3.Connection:
    try:
        c = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True, timeout=8)
    except sqlite3.Error:
        c = sqlite3.connect(DB_PATH, timeout=8)
    c.row_factory = sqlite3.Row
    return c

def live_sql(con: sqlite3.Connection) -> str:
    """Filter out archived trades when the column exists (read-only safe)."""
    try:
        cols = {r[1] for r in con.execute("pragma table_info(trades)").fetchall()}
    except Exception:
        return "1=1"
    return LIVE if "archived" in cols else "1=1"


def he_skip(detail: str | None) -> str:
    if not detail:
        return "דילגה"
    low = detail.lower()
    for k, v in SKIP_HE.items():
        if k.lower() in low or k.replace("_", " ") in low:
            return v
    # strip python-dict tails
    short = re.split(r"\{", detail, maxsplit=1)[0].strip()
    short = re.sub(r"^\[.*?\]\s*", "", short)
    return short[:90] if short else "דילגה"


def _window_spec(name: str, settings: dict | None = None) -> dict:
    spec = dict(ACTIVE.get(name) or {})
    cfg = settings or {}
    for k in ("left_lo", "left_hi", "el_lo", "el_hi"):
        if k in cfg and cfg[k] is not None:
            try:
                spec[k] = float(cfg[k])
            except (TypeError, ValueError):
                pass
    return spec

def in_active(name: str, left: float, elapsed: float, settings: dict | None = None) -> bool:
    if name == "manual":
        return False
    spec = _window_spec(name, settings)
    if not spec:
        return False
    if "left_lo" in spec and not (spec["left_lo"] <= left <= spec["left_hi"]):
        return False
    if "el_lo" in spec and not (spec["el_lo"] <= elapsed <= spec["el_hi"]):
        return False
    if "left_lo" in spec or "el_lo" in spec:
        return True
    return False


def before_active(name: str, left: float, elapsed: float, settings: dict | None = None) -> bool:
    if name == "manual":
        return False
    spec = _window_spec(name, settings)
    if not spec:
        return True
    if "el_lo" in spec and elapsed < spec["el_lo"]:
        return True
    if "left_hi" in spec and left > spec["left_hi"]:
        return True
    return False


def load_hist_file():
    global _hist_slug, _hist
    try:
        with open(HIST_PATH, "r", encoding="utf-8") as f:
            d = json.load(f)
        if d.get("slug") == f"btc-updown-5m-{window_start()}":
            _hist_slug = d["slug"]
            _hist = list(d.get("points") or [])
    except (OSError, json.JSONDecodeError, TypeError):
        pass


def save_hist_file():
    with _lock:
        slug, pts = _hist_slug, list(_hist)
    if not slug:
        return
    tmp = HIST_PATH + ".tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump({"slug": slug, "points": pts[-400:]}, f)
        os.replace(tmp, HIST_PATH)
    except OSError:
        pass


def seed_from_db(slug: str, start: int, con: sqlite3.Connection):
    """Merge window snaps + trade spots into the in-memory series."""
    points = []
    row = con.execute("select snaps from windows where slug=?", (slug,)).fetchone()
    if row and row["snaps"]:
        try:
            snaps = json.loads(row["snaps"])
        except json.JSONDecodeError:
            snaps = []
        for s in snaps or []:
            left = s.get("left")
            px = s.get("cl") or s.get("spot")
            if left is None or not px:
                continue
            t = start + WIN - float(left)
            points.append({"t": t, "btc": float(s.get("cl") or px), "nowcast": float(s.get("spot") or px)})
    for tr in con.execute(
        "select ts, spot, open_px from trades where slug=? and spot is not null", (slug,)
    ):
        points.append({"t": float(tr["ts"]), "btc": float(tr["spot"]), "nowcast": float(tr["spot"])})
    if not points:
        return
    with _lock:
        existing = {(round(p["t"], 1), round(p["btc"], 2)) for p in _hist}
        for p in points:
            key = (round(p["t"], 1), round(p["btc"], 2))
            if key not in existing:
                _hist.append(p)
                existing.add(key)
        _hist.sort(key=lambda x: x["t"])


def reset_hist(slug: str):
    global _hist_slug, _hist
    with _lock:
        if _hist_slug != slug:
            _hist_slug = slug
            _hist = []


def sample_price():
    status, _age = read_status()
    if not status:
        return
    start = window_start()
    slug = f"btc-updown-5m-{start}"
    reset_hist(slug)
    feed = status.get("feed") or {}
    btc = feed.get("chainlink_btc")
    nc = feed.get("nowcast")
    if btc is None and nc is None:
        return
    pt = {"t": now_ts(), "btc": float(btc or nc), "nowcast": float(nc or btc)}
    with _lock:
        if _hist and abs(_hist[-1]["t"] - pt["t"]) < 0.4 and abs(_hist[-1]["btc"] - pt["btc"]) < 0.05:
            _hist[-1] = pt
        else:
            _hist.append(pt)
        if len(_hist) > 800:
            del _hist[:-600]


def sampler_loop():
    n = 0
    while True:
        try:
            sample_price()
            n += 1
            if n % 5 == 0:
                save_hist_file()
        except Exception:
            traceback.print_exc()
        time.sleep(1.0)


def parse_updated(text: str | None) -> float | None:
    if not text:
        return None
    raw = text.split("(")[0].strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            return datetime.strptime(raw, fmt).replace(tzinfo=TZ).timestamp()
        except ValueError:
            continue
    return None


def open_from_log(slug: str) -> float | None:
    try:
        # read last ~80kb only
        size = os.path.getsize(LOG_PATH)
        with open(LOG_PATH, "rb") as f:
            if size > 80000:
                f.seek(size - 80000)
            data = f.read().decode("utf-8", errors="replace")
    except OSError:
        return None
    pat = re.compile(rf"open {re.escape(slug)} (?:own chainlink tick|from API) ([0-9.]+)")
    found = pat.findall(data)
    return float(found[-1]) if found else None


def resolve_open_px(slug: str, start: int, con: sqlite3.Connection) -> tuple[float | None, str | None]:
    row = con.execute("select open_px from windows where slug=?", (slug,)).fetchone()
    if row and row["open_px"]:
        return float(row["open_px"]), "windows"
    row = con.execute(
        "select open_px from trades where slug=? and open_px is not null order by id desc limit 1",
        (slug,),
    ).fetchone()
    if row and row["open_px"]:
        return float(row["open_px"]), "trade"
    prev = f"btc-updown-5m-{start - WIN}"
    row = con.execute("select close_px from windows where slug=?", (prev,)).fetchone()
    if row and row["close_px"]:
        return float(row["close_px"]), "prev_close"
    logged = open_from_log(slug)
    if logged:
        return logged, "bot_log"
    with _lock:
        pts = [p for p in _hist if abs(p["t"] - start) <= 2.5]
    if pts:
        # point closest to window start
        p = min(pts, key=lambda x: abs(x["t"] - start))
        return float(p["btc"]), "hist_tick"
    # API fallback after 15s into the window
    if now_ts() >= start + 15:
        cached = _open_api_cache.get(slug)
        if cached and now_ts() - cached[1] < 60:
            return cached[0], "polymarket_api"
        try:
            r = session().get(
                CRYPTO_PRICE,
                params={
                    "symbol": "BTC",
                    "eventStartTime": iso_utc(start),
                    "variant": "fiveminute",
                    "endDate": iso_utc(start + WIN),
                },
                timeout=4,
            )
            if r.ok:
                d = r.json()
                if d.get("openPrice"):
                    px = float(d["openPrice"])
                    _open_api_cache[slug] = (px, now_ts())
                    return px, "polymarket_api"
        except Exception:
            pass
    return None, None


def get_tokens(slug: str) -> dict | None:
    rec = _tokens.get(slug)
    if rec and now_ts() - rec.get("_ts", 0) < 120:
        return rec
    try:
        r = session().get(f"{GAMMA}/events", params={"slug": slug}, timeout=4)
        ev = r.json() if r.ok else None
        if ev:
            m = ev[0]["markets"][0]
            outcomes = json.loads(m["outcomes"]) if isinstance(m["outcomes"], str) else m["outcomes"]
            ids = json.loads(m["clobTokenIds"]) if isinstance(m["clobTokenIds"], str) else m["clobTokenIds"]
            toks = dict(zip(outcomes, ids))
            prices = m.get("outcomePrices")
            if isinstance(prices, str):
                try:
                    prices = [float(x) for x in json.loads(prices)]
                except Exception:
                    prices = None
            toks["_ts"] = now_ts()
            toks["_outcome_prices"] = dict(zip(outcomes, prices)) if prices else None
            _tokens.clear()
            _tokens[slug] = toks
            return toks
    except Exception:
        pass
    return rec  # maybe stale but usable


def fetch_book(token_id: str) -> dict | None:
    try:
        r = session().get(f"{CLOB}/book", params={"token_id": token_id}, timeout=3)
        if not r.ok:
            return None
        b = r.json()
        asks = sorted(((float(x["price"]), float(x["size"])) for x in b.get("asks") or []))
        bids = sorted(((float(x["price"]), float(x["size"])) for x in b.get("bids") or []), reverse=True)
        return {
            "bid": bids[0][0] if bids else None,
            "ask": asks[0][0] if asks else None,
            "bid_sz": bids[0][1] if bids else None,
            "ask_sz": asks[0][1] if asks else None,
        }
    except Exception:
        return None


def get_book(slug: str) -> dict | None:
    c = _book_cache
    if c["slug"] == slug and now_ts() - c["ts"] < 0.9 and c["data"] is not None:
        return c["data"]
    toks = get_tokens(slug)
    if not toks or "Up" not in toks or "Down" not in toks:
        # fall back to gamma outcomePrices
        if toks and toks.get("_outcome_prices"):
            op = toks["_outcome_prices"]
            data = {
                "up": {"bid": op.get("Up"), "ask": op.get("Up"), "bid_sz": None, "ask_sz": None},
                "down": {"bid": op.get("Down"), "ask": op.get("Down"), "bid_sz": None, "ask_sz": None},
                "ok": False,
                "src": "gamma_prices",
            }
            return data
        return None
    data = {"up": None, "down": None, "ok": False, "src": "clob"}
    with ThreadPoolExecutor(max_workers=2) as ex:
        futs = {ex.submit(fetch_book, toks["Up"]): "up", ex.submit(fetch_book, toks["Down"]): "down"}
        for fut in as_completed(futs):
            data[futs[fut]] = fut.result()
    data["ok"] = bool(data["up"] and data["down"])
    if data["ok"]:
        _book_cache.update({"slug": slug, "ts": now_ts(), "data": data})
    return data


def model_p(feed: dict, start: int, open_px: float | None) -> float | None:
    if open_px is None:
        return None
    sig = feed.get("sigma_per_s")
    cl = feed.get("chainlink_btc")
    nc = feed.get("nowcast")
    if sig is None or (cl is None and nc is None):
        return None
    age = feed.get("age_s") or 0
    if age and age > 10:
        return None
    spot = nc if nc is not None else cl
    src = feed.get("nowcast_src") or "chainlink"
    lag = feed.get("chainlink_lag_s") or 0
    tau = max(0.0, start + WIN - now_ts())
    tau_eff = max(0.0, tau - (lag if src != "chainlink" else 0))
    if src == "chainlink":
        tau_eff += max(0.0, float(age or 0))
    sig_i = float(sig) * 1.2
    var = sig_i ** 2 * tau_eff
    if var <= 1e-9:
        return 1.0 if spot >= open_px else 0.0
    p = ncdf((float(spot) - float(open_px)) / math.sqrt(var))
    return min(max(p, 0.02), 0.98)


def list_strategies(status: dict, con: sqlite3.Connection) -> list[str]:
    names = []
    seen = set()
    status_names = list((status.get("strategies") or {}).keys())
    db_names = [r[0] for r in con.execute("select distinct strategy from trades").fetchall()]
    for n in STRATEGY_ORDER + status_names + db_names:
        if n and n not in seen:
            names.append(n)
            seen.add(n)
    return names


def build_payload() -> dict:
    tnow = now_ts()
    start = window_start(tnow)
    slug = f"btc-updown-5m-{start}"
    left = start + WIN - tnow
    elapsed = tnow - start
    status, file_age = read_status()
    status = status or {}
    feed = status.get("feed") or {}
    gate = (feed.get("health") or {}).get("gate") or {}

    reset_hist(slug)
    con = db_connect()
    trades_by_strategy = {}
    try:
        seed_from_db(slug, start, con)
        open_px, open_src = resolve_open_px(slug, start, con)

        btc = feed.get("chainlink_btc")
        nowcast = feed.get("nowcast")
        px = nowcast if nowcast is not None else btc
        diff = (float(px) - float(open_px)) if (px is not None and open_px is not None) else None
        leading = None
        if diff is not None:
            leading = "Up" if diff >= 0 else "Down"

        p_up = model_p(feed, start, open_px)
        book = get_book(slug)

        updated_ts = parse_updated(status.get("updated"))
        status_age = (tnow - updated_ts) if updated_ts else file_age
        reason = gate.get("reason") or ("missing" if not status else "ok")
        frozen = (status_age is not None and status_age > 15) or (reason not in ("ok", None) and not gate.get("ok", True) and (status_age or 0) > 8)
        # red if gate explicitly not ok OR status stale
        feed_ok = bool(gate.get("ok", False)) and not (status_age is not None and status_age > 12)
        if status_age is not None and status_age > 12:
            frozen = True
            feed_ok = False
            if reason == "ok":
                reason = "stale_feed"

        # trades
        recent_rows = con.execute(
            """select id, ts, time, slug, strategy, side, shares, avg_price, cost, fee,
                      model_p, edge, secs_left, outcome, pnl, settled, settled_time, open_px, spot
               from trades where {live} order by id desc limit 15""".format(live=live_sql(con))
        ).fetchall()
        tcols = {r[1] for r in con.execute("pragma table_info(trades)").fetchall()}
        has_pending = "pending" in tcols
        has_est = "est_profit" in tcols
        extra_recent = (", pending" if has_pending else "") + (", est_profit, est_loss" if has_est else "")
        if extra_recent:
            recent_rows = con.execute(
                """select id, ts, time, slug, strategy, side, shares, avg_price, cost, fee,
                          model_p, edge, secs_left, outcome, pnl, settled, settled_time, open_px, spot
                          {extra}
                   from trades where {live} order by id desc limit 15""".format(live=live_sql(con), extra=extra_recent)
            ).fetchall()
        recent = []
        for r in recent_rows:
            tm = r["time"] or ""
            recent.append({
                "id": r["id"],
                "ts": r["ts"],
                "time": tm[11:19] if len(tm) >= 19 else tm,
                "slug": r["slug"],
                "strategy": r["strategy"],
                "side": r["side"],
                "shares": r["shares"],
                "avg_price": r["avg_price"],
                "cost": r["cost"],
                "pnl": r["pnl"],
                "settled": int(r["settled"] or 0),
                "pending": int(r["pending"] or 0) if has_pending else 0,
                "est_profit": estimates_of(r)[0],
                "est_loss": estimates_of(r)[1],
                "outcome": r["outcome"],
                "secs_left": r["secs_left"],
            })

        open_rows = con.execute(
            """select id, strategy, side, shares, avg_price, cost, fee, model_p, secs_left, ts, time
                      {est}
               from trades where slug=? and settled=0 and {live} order by id""".format(
                live=live_sql(con), est=", est_profit, est_loss" if has_est else ""),
            (slug,),
        ).fetchall()
        open_by = {}
        for r in open_rows:
            open_by.setdefault(r["strategy"], []).append(dict(r))

        # latest decisions this window
        dec_rows = con.execute(
            "select strategy, action, detail, ts from decisions where slug=? order by id desc",
            (slug,),
        ).fetchall()
        latest_dec = {}
        for r in dec_rows:
            if r["strategy"] not in latest_dec:
                latest_dec[r["strategy"]] = {"action": r["action"], "detail": r["detail"], "ts": r["ts"]}

        markers = []
        for r in con.execute(
            """select ts, strategy, side, avg_price, cost, shares, spot
               from trades where slug=? and {live} order by ts""".format(live=live_sql(con)),
            (slug,),
        ):
            markers.append({
                "t": r["ts"],
                "strategy": r["strategy"],
                "side": r["side"],
                "price": r["avg_price"],
                "cost": r["cost"],
                "shares": r["shares"],
                "spot": r["spot"],
            })

        stmap = status.get("strategies") or {}
        team = []
        for name in list_strategies(status, con):
            s = stmap.get(name) or {}
            cash = s.get("cash")
            expo = s.get("open_exposure") or 0
            bankroll = None
            if cash is not None:
                bankroll = round(float(cash) + float(expo), 2)
            elif name in STRATEGY_ORDER and not s:
                bankroll = 100.0
                cash = 100.0
            realized = s.get("realized_pnl") if s else (0.0 if name in STRATEGY_ORDER else None)
            settled = s.get("settled") or 0
            wins = s.get("wins") or 0
            wr = (wins / settled) if settled else None

            opens = open_by.get(name) or []
            # also consider already-settled trades of this same (current) slug — rare mid-window
            bought = opens
            if not bought:
                # trade this slug even if somehow settled
                tr = con.execute(
                    f"select id, side, shares, avg_price, cost, fee{', est_profit, est_loss' if has_est else ''} from trades where slug=? and strategy=? and {live_sql(con)} order by id desc limit 1",
                    (slug, name),
                ).fetchone()
                if tr:
                    bought = [dict(tr)]

            run_mode = s.get("run_mode") or ("active" if s else "active")
            cfg = s.get("settings") or {}
            if bought:
                bits = []
                for o in bought:
                    est = fmt_estimate(*estimates_of(o))
                    line = f"קנתה {o['side']} ב-{o['avg_price']*100:.0f}¢ · ${o['cost']:.2f}"
                    if est:
                        line += " · " + est
                    bits.append(line)
                state = "bought"
                state_he = " · ".join(bits)
                if run_mode == "off":
                    state_he = "כבוי · " + state_he
                elif run_mode == "paused":
                    state_he = "מושהה · " + state_he
            elif run_mode == "off":
                state, state_he = "off", "כבוי"
            elif run_mode == "paused":
                state, state_he = "paused", "מושהה"
            elif name == "manual":
                state, state_he = "watching", "ידני · ממתין לפקודה"
            else:
                dec = latest_dec.get(name)
                cfg = s.get("settings") or {}
                active_now = in_active(name, left, elapsed, cfg)
                waiting = before_active(name, left, elapsed, cfg)
                if not s and name in STRATEGY_ORDER:
                    # known but not yet in this process's status
                    if waiting:
                        state, state_he = "waiting", "ממתינה (עדיין לא בסטטוס)"
                    else:
                        state, state_he = "waiting", "עדיין לא באוויר"
                elif active_now:
                    state, state_he = "watching", "עוקבת אחרי הסיבוב"
                    if dec and dec["action"] == "skip":
                        # still in window, last tick skipped — show as watching with last reason
                        state_he = f"עוקבת · {he_skip(dec['detail'])}"
                elif waiting:
                    state, state_he = "waiting", "ממתינה לחלון הכניסה שלה"
                else:
                    # active window already passed, no trade
                    if dec and dec["action"] == "skip":
                        state, state_he = "skipped", f"דילגה: {he_skip(dec['detail'])}"
                    else:
                        state, state_he = "skipped", "דילגה בסיבוב הזה"

            team.append({
                "name": name,
                "doc": s.get("doc") or "",
                "doc_he": DOC_HE.get(name) or (s.get("doc") or "אסטרטגיה חדשה."),
                "cash": cash,
                "open_exposure": expo,
                "bankroll": bankroll,
                "realized_pnl": realized,
                "trades": s.get("trades") or 0,
                "settled": settled,
                "wins": wins,
                "win_rate": wr,
                "state": state,
                "state_he": state_he,
                "live": bool(s),
                "run_mode": s.get("run_mode") or run_mode,
                "enabled": s.get("enabled", True),
                "auto_trading": s.get("auto_trading", name != "manual"),
                "settings": cfg if cfg else (s.get("settings") or None),
                "schema": bot_settings.schema_for(name) if name in bot_settings.KNOWN else [],
                "defaults": bot_settings.defaults_for(name) if name in bot_settings.KNOWN else {},
                "open": [
                    {
                        "side": o["side"],
                        "avg_price": o["avg_price"],
                        "cost": o["cost"],
                        "shares": o["shares"],
                        "fee": (o.get("fee") if isinstance(o, dict) else (o["fee"] if "fee" in o.keys() else None)),
                        "est_profit": estimates_of(o)[0],
                        "est_loss": estimates_of(o)[1],
                    }
                    for o in opens
                ],
                "decision": s.get("decision"),
                "decision_reason": s.get("decision_reason"),
                "genetic": s.get("genetic"),
            })

        trades_by_strategy = {n: [] for n in list_strategies(status, con)}
        live = live_sql(con)
        for r in con.execute(
            f"""select id, ts, time, slug, strategy, side, shares, avg_price, cost, fee,
                       model_p, edge, secs_left, outcome, pnl, settled, settled_time
                       {", pending" if has_pending else ""}
                       {", est_profit, est_loss" if has_est else ""}
                from trades where {live} order by id desc limit 800"""
        ):
            lst = trades_by_strategy.setdefault(r["strategy"], [])
            if len(lst) >= 80:
                continue
            tm = r["time"] or ""
            lst.append({
                "id": r["id"], "ts": r["ts"],
                "time": tm[11:19] if len(tm) >= 19 else tm,
                "slug": r["slug"], "strategy": r["strategy"], "side": r["side"],
                "shares": r["shares"], "avg_price": r["avg_price"], "cost": r["cost"],
                "fee": r["fee"], "pnl": r["pnl"], "settled": int(r["settled"] or 0),
                "pending": int(r["pending"] or 0) if has_pending else 0,
                "est_profit": estimates_of(r)[0],
                "est_loss": estimates_of(r)[1],
                "outcome": r["outcome"], "secs_left": r["secs_left"],
            })

        with _lock:
            series = [p for p in _hist if p["t"] >= start - 1]
        # start the line at the official open so the chart isn't empty early in the window
        if open_px is not None and (not series or series[0]["t"] > start + 3):
            series = [{"t": float(start), "btc": float(open_px), "nowcast": float(open_px)}] + series
        # always include a live point
        if px is not None:
            series = series + [{"t": tnow, "btc": float(btc or px), "nowcast": float(nowcast or px)}]
            if len(series) >= 2 and abs(series[-1]["t"] - series[-2]["t"]) < 0.4:
                series[-2] = series[-1]
                series.pop()

    finally:
        con.close()

    clock = datetime.fromtimestamp(tnow, TZ).strftime("%H:%M:%S")
    return {
        "updated": status.get("updated"),
        "clock": clock,
        "now": tnow,
        "tz": "Asia/Jerusalem",
        "mode": status.get("mode") or "PAPER - no real money",
        "primary": status.get("primary") or "late_model",
        "window": {
            "start": start,
            "end": start + WIN,
            "slug": slug,
            "left": round(left, 2),
            "elapsed": round(elapsed, 2),
            "open_px": open_px,
            "open_src": open_src,
            "btc": btc,
            "nowcast": nowcast,
            "diff": None if diff is None else round(diff, 2),
            "leading": leading,
        },
        "model": {"p_up": p_up, "sigma": feed.get("sigma_per_s")},
        "book": book,
        "feed": {
            "ok": feed_ok,
            "frozen": bool(frozen or not feed_ok),
            "reason": reason,
            "reason_he": GATE_HE.get(reason, reason or "?"),
            "age_s": feed.get("age_s"),
            "status_age_s": None if status_age is None else round(status_age, 1),
        },
        "series": series,
        "markers": markers,
        "strategies": team,
        "recent": recent,
        "trades_by_strategy": trades_by_strategy,
        "html_version": str(int(os.path.getmtime(INDEX_PATH))),
    }


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        # keep access log small
        if "/api/live" in (args[0] if args else ""):
            return
        sys_stderr_write = super().log_message
        sys_stderr_write(fmt, *args)

    def _send(self, code: int, body: bytes, ctype: str):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store, no-cache, must-revalidate")
        self.send_header("Pragma", "no-cache")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        path = urlparse(self.path).path
        try:
            if path in ("/", "/index.html"):
                with open(INDEX_PATH, "rb") as f:
                    self._send(200, f.read(), "text/html; charset=utf-8")
                return
            if path in ("/sim1000", "/sim1000.html"):
                with open(SIM1000_PATH, "rb") as f:
                    self._send(200, f.read(), "text/html; charset=utf-8")
                return
            if path in ("/bot", "/bot.html") or path.startswith("/bot/"):
                with open(BOT_PATH, "rb") as f:
                    self._send(200, f.read(), "text/html; charset=utf-8")
                return
            if path == "/api/live":
                payload = build_payload()
                body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
                self._send(200, body, "application/json; charset=utf-8")
                return
            if path == "/health":
                self._send(200, b'{"ok":true}', "application/json")
                return
            self._send(404, b"not found", "text/plain")
        except Exception:
            traceback.print_exc()
            self._send(500, b'{"error":"internal"}', "application/json")


def main():
    load_hist_file()
    threading.Thread(target=sampler_loop, name="price-sampler", daemon=True).start()
    httpd = ThreadingHTTPServer((HOST, PORT), Handler)
    print(f"live dashboard at http://{HOST}:{PORT}/", flush=True)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        save_hist_file()
        httpd.server_close()


if __name__ == "__main__":
    main()
