#!/usr/bin/env python3
"""Paper-trading bot for Polymarket 'Bitcoin Up or Down' 5-minute markets.
NO real money, NO wallet, NO keys. Reads public data only and simulates taker fills
against the live CLOB order book.
"""
import json, math, os, signal, sqlite3, threading, time, traceback, csv
from types import SimpleNamespace
from datetime import datetime, timezone
import requests, websocket
from latency_hunter import LatencyHunter, HunterConfig
import bot_settings
from bot_settings import LIVE
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE = os.path.dirname(os.path.abspath(__file__))
DB = os.path.join(BASE, "trades.db")
CSV = os.path.join(BASE, "trades.csv")
STATUS = os.path.join(BASE, "status.json")
LOG = os.path.join(BASE, "bot.log")
GAMMA = "https://gamma-api.polymarket.com"
CLOB = "https://clob.polymarket.com"
CRYPTO_PRICE = "https://polymarket.com/api/crypto/crypto-price"
RTDS = "wss://ws-live-data.polymarket.com"
# Space patch: Binance endpoints are env-configurable (binance.com geo-blocks US IPs -> binance.us)
BINANCE_REST = os.environ.get("BINANCE_REST", "https://api.binance.com")
BINANCE_WS = os.environ.get("BINANCE_WS", "wss://stream.binance.com:443")
WIN = 300
FEE_RATE = 0.07          # crypto taker fee rate: fee = C * rate * p * (1-p)
TWAP_LB = 60             # resolution uses 60s TWAP (cryptoMarketConfig twapLookbackSeconds)
START_BANK = 100.0
MIN_SHARES = 5.0         # orderMinSize

# ---- data-quality gates (added after the stale-data longshot losses, trades #46-52)
CL_FRESH_S = 3.5         # chainlink tick age (floor-second based; normal is 1.0-2.2s)
SRC_FRESH_S = 3.0        # an exchange mid counts as fresh if received within this
WARMUP_S = 25.0          # continuous fresh+agreeing data required after any reconnect / freeze
AGREE_USD = 15.0         # offset-adjusted exchange estimates must agree within max(this, 4*sigma)
AGREE_CL_USD = 25.0      # chainlink tick (lags ~1-2s) vs nowcast within max(this, 6*sigma)
STALE_RESET_N = 3        # consecutive 0.5s watchdog checks of staleness that restart the warm-up clock
FREEZE_GAP_S = 4.0       # a watchdog tick gap this large = process/VM was frozen
MAX_DISAGREE = 0.25      # |model P(up) - book mid| above this -> skip (all strategies)
BAND_LO, BAND_HI = 0.12, 0.92   # don't buy a side priced outside this band...
BAND_CONF = 0.90         # ...unless model gives that side >= this, and data has been warm >= BAND_WARM_S
BAND_WARM_S = 60.0
CONTRA_LEFT_S = 90       # with < this left, skip if the book strongly contradicts spot-vs-open
CONTRA_P = 0.15          # 'strongly' = book P(up) < 0.15 while spot > open (or > 0.85 while spot < open)

# Latency-critical (books / current-window load) vs settlement / metadata
FAST = 6
SLOW = 15

def _session(retry):
    s = requests.Session()
    s.headers["User-Agent"] = "polybot-paper/1.0"
    adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s

S_FAST = _session(Retry(total=1, connect=1, read=0, backoff_factor=0.3,
                        status_forcelist=(429, 500, 502, 503, 504),
                        allowed_methods=frozenset(["GET", "HEAD"]),
                        raise_on_status=False))
S_SLOW = _session(Retry(total=3, connect=3, read=2, backoff_factor=0.5,
                        status_forcelist=(429, 500, 502, 503, 504),
                        allowed_methods=frozenset(["GET", "HEAD"]),
                        raise_on_status=False))

def reset_http_sessions():
    """after a VM/process freeze every pooled keep-alive connection is dead; start fresh pools"""
    global S_FAST, S_SLOW
    old = (S_FAST, S_SLOW)
    S_FAST = _session(Retry(total=1, connect=1, read=0, backoff_factor=0.3, status_forcelist=(429, 500, 502, 503, 504),
                            allowed_methods=frozenset(["GET", "HEAD"]), raise_on_status=False))
    S_SLOW = _session(Retry(total=3, connect=3, read=2, backoff_factor=0.5, status_forcelist=(429, 500, 502, 503, 504),
                            allowed_methods=frozenset(["GET", "HEAD"]), raise_on_status=False))
    for o in old:
        try: o.close()
        except Exception: pass

_http = {"ok": 0, "timeout": 0, "err": 0}
_http_lock = threading.Lock()
_log_lock = threading.Lock()

def log(*a):
    line = datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " " + " ".join(str(x) for x in a)
    print(line, flush=True)
    with _log_lock:
        with open(LOG, "a") as f: f.write(line + "\n")

def http_stats():
    with _http_lock:
        return dict(_http)

def get(url, timeout=None, slow=False, **params):
    if timeout is None:
        timeout = SLOW if slow else FAST
    sess = S_SLOW if slow else S_FAST
    try:
        r = sess.get(url, params=params, timeout=timeout)
        r.raise_for_status()
        with _http_lock:
            _http["ok"] += 1
        return r.json()
    except Exception as e:
        msg = str(e).lower()
        with _http_lock:
            _http["err"] += 1
            if isinstance(e, requests.Timeout) or "timed out" in msg or "timeout" in msg:
                _http["timeout"] += 1
        raise

def iso(ts): return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
def local(ts): return datetime.fromtimestamp(ts).strftime("%H:%M")

def ncdf(x): return 0.5 * (1 + math.erf(x / math.sqrt(2)))

# ---------------------------------------------------------------- price feed
class Feed:
    """Chainlink BTC/USD per-second stream via Polymarket RTDS (the resolution source family).
    Exchange mids (Binance primary, Coinbase/Kraken fallback) are used only for the nowcast."""
    EXCHANGES = ("binance", "coinbase", "kraken")

    def __init__(self):
        self.px = {}            # unix sec -> price
        self.lock = threading.Lock()
        self.last_msg = 0
        self.ex_px = {n: {} for n in self.EXCHANGES}
        self.ex_last = {n: (0, None) for n in self.EXCHANGES}
        self.lag = 2            # seconds chainlink stamps lag exchange mid (estimated live)
        self.lag_corr = None
        self.health = {n: {"msgs": 0, "errors": 0, "reconnects": 0, "connects": 0} for n in ("chainlink",) + self.EXCHANGES}
        self._stop = threading.Event()
        self.ws = {}                      # name -> live websocket (so the watchdog can abort it)
        self.last_disrupt = time.time()   # last reconnect / socket error / freeze
        self.fresh_since = None           # start of current continuous fresh+agreeing stretch
        self.freezes = []                 # (wall time resumed, gap seconds), last 50
        self.last_gate = (False, "starting")
        self.instant_counts = {}          # watchdog 2Hz data-quality reasons (since start)
        self.last_bad = None
        threading.Thread(target=self.watchdog, daemon=True, name="feed-watchdog").start()
        threading.Thread(target=self.run, daemon=True, name="feed-chainlink").start()
        threading.Thread(target=self.run_binance, daemon=True, name="feed-binance").start()
        threading.Thread(target=self.run_coinbase, daemon=True, name="feed-coinbase").start()
        threading.Thread(target=self.run_kraken, daemon=True, name="feed-kraken").start()
        threading.Thread(target=self.run_rest_fallback, daemon=True, name="feed-rest").start()

    def _bump(self, name, key, n=1):
        h = self.health[name]
        h[key] = h.get(key, 0) + n

    def _record_ex(self, name, mid):
        t = time.time()
        with self.lock:
            self.ex_px[name][int(t)] = mid
            self.ex_last[name] = (t, mid)
            bucket = self.ex_px[name]
            if len(bucket) > 4000:
                for k in sorted(bucket)[:-3600]: del bucket[k]
        self._bump(name, "msgs")
        cb = getattr(self, "on_ex_tick", None)
        if cb:
            try:
                cb(name, mid, t)
            except Exception:
                pass

    def best_exchg(self):
        """Prefer Binance if fresh (<5s), else Coinbase, else Kraken."""
        now = time.time()
        with self.lock:
            ordered = ("binance", "coinbase", "kraken")
            for name in ordered:
                t, mid = self.ex_last[name]
                if mid is not None and now - t <= 5:
                    return name, t, mid
            # return the least-stale anyway (caller decides)
            best = min(ordered, key=lambda n: 1e12 if self.ex_last[n][1] is None else now - self.ex_last[n][0])
            t, mid = self.ex_last[best]
            return best, t, mid

    def nowcast(self):
        """estimate of the Chainlink value 'now': exchange mid + median(chainlink[k] - exch[k-lag]) over last 120s.
        Returns (value, source)."""
        k, cl = self.latest()
        if k is None: return None, None
        name, t, mid = self.best_exchg()
        with self.lock:
            hist = self.ex_px[name]
            offs = [self.px[j] - hist[j - self.lag] for j in range(k - 120, k + 1) if j in self.px and (j - self.lag) in hist]
        if mid is None or time.time() - t > 5 or len(offs) < 20:
            return cl, "chainlink"
        offs.sort()
        return mid + offs[len(offs) // 2], f"{name}+offset"

    def estimate_lag(self):
        """pick lag in 0..8s maximizing correlation of 1s changes (chainlink vs exchange)"""
        k, _ = self.latest()
        if k is None: return
        name, _, mid = self.best_exchg()
        best = None
        with self.lock:
            hist = self.ex_px[name] if self.ex_px[name] else self.ex_px["binance"]
            for L in range(0, 9):
                a = []; b = []
                for j in range(k - 600, k + 1):
                    if j in self.px and j - 1 in self.px and j - L in hist and j - L - 1 in hist:
                        a.append(self.px[j] - self.px[j - 1]); b.append(hist[j - L] - hist[j - L - 1])
                if len(a) < 60: return
                ma, mb = sum(a) / len(a), sum(b) / len(b)
                cov = sum((x - ma) * (y - mb) for x, y in zip(a, b))
                va = math.sqrt(sum((x - ma) ** 2 for x in a)); vb = math.sqrt(sum((y - mb) ** 2 for y in b))
                r = cov / (va * vb) if va and vb else 0
                if best is None or r > best[1]: best = (L, r)
        if best: self.lag = best[0]; self.lag_corr = round(best[1], 3)

    def add(self, ts_ms, v):
        with self.lock:
            self.px[int(ts_ms // 1000)] = float(v)
            if len(self.px) > 4000:
                for k in sorted(self.px)[:-3600]: del self.px[k]
        self.last_msg = time.time()
        self._bump("chainlink", "msgs")

    def _ws_loop(self, name, url, on_open, on_msg, recv_timeout=20, connect_timeout=15):
        while not self._stop.is_set():
            ws = None
            try:
                ws = websocket.create_connection(url, timeout=connect_timeout, enable_multithread=True)
                ws.settimeout(recv_timeout)
                if on_open:
                    on_open(ws)
                self.ws[name] = ws
                self.last_disrupt = time.time()
                self._bump(name, "connects")
                if self.health[name]["connects"] > 1:
                    self._bump(name, "reconnects")
                    log(f"{name} feed reconnected")
                last_ping = time.time()
                while not self._stop.is_set():
                    if time.time() - last_ping > 10:
                        try:
                            ws.ping()
                        except Exception:
                            break
                        last_ping = time.time()
                    try:
                        m = ws.recv()
                    except websocket.WebSocketTimeoutException:
                        # silence past recv_timeout: treat as dead socket
                        raise
                    if m is None or m == "":
                        continue
                    if isinstance(m, (bytes, bytearray)):
                        continue
                    if isinstance(m, str) and (not m or m[0] not in "{["):
                        continue
                    on_msg(m)
            except Exception as e:
                self._bump(name, "errors")
                self.last_disrupt = time.time()
                log(f"{name} feed error", repr(e)[:200])
                self._stop.wait(1 if getattr(self, "_fast_reconnect", False) else 2)
            finally:
                if self.ws.get(name) is ws:
                    self.ws.pop(name, None)
                if ws is not None:
                    try: ws.close()
                    except Exception: pass

    def run(self):
        def on_open(ws):
            ws.send(json.dumps({"action": "subscribe", "subscriptions": [{"topic": "crypto_prices_chainlink", "type": "*", "filters": "{\"symbol\":\"btc/usd\"}"}]}))
        def on_msg(m):
            d = json.loads(m); p = d.get("payload", {})
            if "data" in p:
                for x in p["data"]: self.add(x["timestamp"], x["value"])
            elif "value" in p and p.get("symbol") == "btc/usd":
                self.add(p["timestamp"], p["value"])
        self._ws_loop("chainlink", RTDS, on_open, on_msg)

    def run_binance(self):
        def on_msg(m):
            d = json.loads(m)
            mid = (float(d["b"]) + float(d["a"])) / 2
            self._record_ex("binance", mid)
        self._ws_loop("binance", BINANCE_WS + "/ws/btcusdt@bookTicker", None, on_msg)

    def run_coinbase(self):
        def on_open(ws):
            ws.send(json.dumps({"type": "subscribe", "product_ids": ["BTC-USD"], "channels": ["ticker"]}))
        def on_msg(m):
            d = json.loads(m)
            if d.get("type") != "ticker":
                return
            bid, ask = d.get("best_bid"), d.get("best_ask")
            if bid and ask:
                self._record_ex("coinbase", (float(bid) + float(ask)) / 2)
            elif d.get("price"):
                self._record_ex("coinbase", float(d["price"]))
        self._ws_loop("coinbase", "wss://ws-feed.exchange.coinbase.com", on_open, on_msg)

    def run_kraken(self):
        def on_open(ws):
            ws.send(json.dumps({"event": "subscribe", "pair": ["XBT/USD"], "subscription": {"name": "ticker"}}))
        def on_msg(m):
            d = json.loads(m)
            if not isinstance(d, list) or len(d) < 2 or not isinstance(d[1], dict):
                return
            tick = d[1]
            if "a" in tick and "b" in tick:
                self._record_ex("kraken", (float(tick["a"][0]) + float(tick["b"][0])) / 2)
        self._ws_loop("kraken", "wss://ws.kraken.com", on_open, on_msg)

    def run_rest_fallback(self):
        sources = [
            ("coinbase", "https://api.exchange.coinbase.com/products/BTC-USD/ticker",
             lambda d: (float(d["bid"]) + float(d["ask"])) / 2),
            ("kraken", "https://api.kraken.com/0/public/Ticker?pair=XBTUSD",
             lambda d: (float(next(iter(d["result"].values()))["b"][0]) + float(next(iter(d["result"].values()))["a"][0])) / 2),
            ("binance", BINANCE_REST + "/api/v3/ticker/bookTicker?symbol=BTCUSDT",
             lambda d: (float(d["bidPrice"]) + float(d["askPrice"])) / 2),
        ]
        while not self._stop.is_set():
            try:
                _, t, mid = self.best_exchg()
                if mid is not None and time.time() - t <= 4:
                    self._stop.wait(1)
                    continue
                got = False
                for name, url, fn in sources:
                    lt, lmid = self.ex_last[name]
                    if lmid is not None and time.time() - lt <= 4:
                        continue
                    try:
                        d = get(url, timeout=5, slow=False)
                        self._record_ex(name, fn(d))
                        got = True
                        break
                    except Exception:
                        self._bump(name, "errors")
                self._stop.wait(1 if got else 2)
            except Exception as e:
                log("rest fallback error", repr(e))
                self._stop.wait(2)

    # ------------------------------------------------ data-quality gate
    def source_estimates(self):
        """Chainlink-equivalent spot per fresh exchange: own mid + own median(chainlink - mid) offset over 120s.
        Each exchange is an independent estimate (USDT basis etc. removed by its own offset)."""
        now = time.time()
        k, cl = self.latest()
        out = {}
        if k is None: return out
        with self.lock:
            for name in self.EXCHANGES:
                t, mid = self.ex_last[name]
                if mid is None or now - t > SRC_FRESH_S: continue
                hist = self.ex_px[name]
                offs = [self.px[j] - hist[j - self.lag] for j in range(k - 120, k + 1) if j in self.px and (j - self.lag) in hist]
                if len(offs) < 20: continue
                offs.sort()
                out[name] = mid + offs[len(offs) // 2]
        return out

    def _instant_ok(self):
        """(ok, reason, detail) for the data right now, ignoring warm-up"""
        now = time.time()
        k, cl = self.latest()
        if k is None: return False, "no_chainlink", {}
        cl_age = now - k
        if cl_age > CL_FRESH_S or now - self.last_msg > SRC_FRESH_S:
            return False, "stale_chainlink", {"cl_age": round(cl_age, 1)}
        nc, src = self.nowcast()
        if src is None or src == "chainlink":
            return False, "nowcast_fallback", {"src": src}
        name = src.split("+")[0]
        t, _ = self.ex_last[name]
        if now - t > SRC_FRESH_S:
            return False, "stale_nowcast", {"src": src, "age": round(now - t, 1)}
        ests = self.source_estimates()
        if len(ests) < 1:
            return False, "too_few_sources", {"n": len(ests)}
        sig = self.sigma_local() or 2.0
        tol_ex = max(AGREE_USD, 4 * sig); tol_cl = max(AGREE_CL_USD, 6 * sig)
        srcs = {**{n: round(v, 1) for n, v in ests.items()}, "chainlink": round(cl, 1)}
        ex_spread = (max(ests.values()) - min(ests.values())) if len(ests) >= 2 else 0.0
        cl_gap = abs(cl - nc)
        # need >= 2 independent agreeing sources: two exchanges within tol_ex, or (only one exchange) it plus chainlink within tol_cl
        if len(ests) >= 2 and ex_spread > tol_ex:
            return False, "sources_disagree", {"ex_spread": round(ex_spread, 1), "tol": round(tol_ex, 1), "srcs": srcs}
        if cl_gap > tol_cl:
            return False, "sources_disagree", {"cl_vs_nowcast": round(cl_gap, 1), "tol": round(tol_cl, 1), "srcs": srcs}
        return True, "ok", {"n_sources": len(ests) + 1, "ex_spread": round(ex_spread, 1), "cl_gap": round(cl_gap, 1)}

    def watchdog(self):
        """1Hz: detect process/VM freezes (wall-clock gap), force immediate reconnect + fresh HTTP pools,
        and maintain the warm-up clock (fresh_since)."""
        last = time.time()
        while not self._stop.is_set():
            self._stop.wait(0.5)
            now = time.time()
            gap = now - last
            last = now
            if gap > FREEZE_GAP_S:
                self.freezes = (self.freezes + [(datetime.now().strftime("%H:%M:%S"), round(gap, 1))])[-50:]
                self.last_disrupt = now
                self.fresh_since = None
                log(f"FREEZE detected: process/VM was suspended ~{gap:.0f}s -> aborting all websockets and HTTP pools for immediate reconnect")
                self._fast_reconnect = True
                for name, ws in list(self.ws.items()):
                    try: ws.abort()
                    except Exception: pass
                    try: ws.sock and ws.sock.close()
                    except Exception: pass
                try: reset_http_sessions()
                except Exception as e: log("http reset err", repr(e))
                continue
            try:
                ok, why, det = self._instant_ok()
            except Exception:
                ok, why, det = False, "gate_error", {}
            self.instant_counts[why] = self.instant_counts.get(why, 0) + 1
            if not ok: self.last_bad = (datetime.now().strftime("%H:%M:%S"), why, det)
            # warm-up clock: restarted by reconnects/freezes (last_disrupt) and by sustained staleness;
            # a momentary disagreement or a single late tick only blocks that instant.
            stale = why in ("no_chainlink", "stale_chainlink", "nowcast_fallback", "stale_nowcast", "too_few_sources", "gate_error")
            self._stale_n = (getattr(self, "_stale_n", 0) + 1) if stale else 0
            if self._stale_n >= STALE_RESET_N:
                self.fresh_since = None
            elif ok and (self.fresh_since is None or self.fresh_since < self.last_disrupt):
                self.fresh_since = now
            if ok: self._fast_reconnect = False

    def warm_for(self):
        fs = self.fresh_since
        if fs is None or fs < self.last_disrupt: return 0.0
        return time.time() - fs

    def gate(self):
        """(ok, reason, detail): hard freshness + agreement + warm-up gate for ANY new entry"""
        ok, why, det = self._instant_ok()
        if not ok:
            self.last_gate = (False, why); return False, why, det
        w = self.warm_for()
        if w < WARMUP_S:
            self.last_gate = (False, "warmup"); return False, "warmup", {"warm_s": round(w, 1), "since_disrupt_s": round(time.time() - self.last_disrupt, 1)}
        self.last_gate = (True, "ok")
        det["warm_s"] = round(w, 1)
        return True, "ok", det

    def series(self, a, b):
        """forward-filled per-second prices for [a, b]"""
        with self.lock:
            keys = sorted(self.px)
            if not keys: return []
            out, last = [], None
            for k in keys:
                if k <= a: last = self.px[k]
            for t in range(a, b + 1):
                if t in self.px: last = self.px[t]
                if last is not None: out.append(last)
            return out

    def ex_series(self, name, a, b):
        """forward-filled per-second exchange mids for [a, b] (raw mids; offset cancels in velocities)"""
        with self.lock:
            hist = self.ex_px.get(name) or {}
            last = None
            for t in range(a - 10, a + 1):
                if t in hist: last = hist[t]
            out = []
            for t in range(a, b + 1):
                if t in hist: last = hist[t]
                out.append(last)
        return out

    def latest(self):
        with self.lock:
            if not self.px: return None, None
            k = max(self.px); return k, self.px[k]

    _bs = (0, None)
    def binance_sigma(self):
        """bootstrap vol from Binance 1s klines while our Chainlink history is short (vols match closely)"""
        if time.time() - self._bs[0] < 60: return self._bs[1]
        try:
            k = get(BINANCE_REST + "/api/v3/klines", symbol="BTCUSDT", interval="1s", limit=900, slow=True, timeout=SLOW)
            c = [float(x[4]) for x in k]; d = [c[i + 1] - c[i] for i in range(len(c) - 1)]
            v = max(math.sqrt(sum(x * x for x in d) / len(d)), 0.5)
        except Exception: v = None
        self._bs = (time.time(), v); return v

    def sigma_local(self, lookback=900):
        """like sigma() but never does HTTP (safe for the watchdog thread)"""
        k, _ = self.latest()
        if k is None: return None
        s = self.series(k - lookback, k)
        if len(s) < 120: return None
        d = [s[i + 1] - s[i] for i in range(len(s) - 1)]
        return max(math.sqrt(sum(x * x for x in d) / len(d)), 0.5)

    def sigma(self, lookback=900):
        """per-second volatility in $ from 1s diffs (robust, floored)"""
        k, _ = self.latest()
        if k is None: return None
        s = self.series(k - lookback, k)
        if len(s) < 120: return self.binance_sigma()
        d = [s[i + 1] - s[i] for i in range(len(s) - 1)]
        mad = sorted(abs(x) for x in d)[len(d) // 2] * 1.4826
        sd = math.sqrt(sum(x * x for x in d) / len(d))
        return max(sd, mad, 0.5)

    def health_snapshot(self):
        now = time.time()
        out = {}
        k, _ = self.latest()
        out["chainlink"] = dict(self.health["chainlink"], last_age_s=round(now - self.last_msg, 1) if self.last_msg else None,
                                tick_age_s=round(now - k, 1) if k else None)
        for name in self.EXCHANGES:
            t, mid = self.ex_last[name]
            out[name] = dict(self.health[name], last_age_s=round(now - t, 1) if mid is not None else None, mid=mid)
        out["http"] = http_stats()
        out["gate"] = dict(ok=self.last_gate[0], reason=self.last_gate[1], warm_s=round(self.warm_for(), 1),
                           since_disrupt_s=round(now - self.last_disrupt, 1), recent_freezes=self.freezes[-10:], freezes_total=len(self.freezes),
                           data_quality_2hz_counts=dict(self.instant_counts), last_bad=self.last_bad)
        return out

    def health_line(self):
        h = self.health_snapshot()
        parts = []
        for name in ("chainlink",) + self.EXCHANGES:
            d = h[name]
            age = d.get("last_age_s"); age_s = "n/a" if age is None else f"{age}s"; parts.append(f"{name} age={age_s} rec={d.get('reconnects',0)} err={d.get('errors',0)} msgs={d.get('msgs',0)}")
        http = h["http"]
        parts.append(f"http ok={http['ok']} timeout={http['timeout']} err={http['err']}")
        return "feed health " + " | ".join(parts)

# ---------------------------------------------------------------- model
def p_up(feed, start, open_px, now=None):
    """P(Chainlink BTC/USD at window end >= open) under Brownian motion.
    Verified live: official close == Chainlink RTDS tick at the end second (not a 60s average),
    so final ~ N(nowcast, sigma^2 * tau_eff), tau_eff = seconds until the end-second tick, minus lag already seen via binance."""
    end = start + WIN
    sig = feed.sigma()
    k, cl = feed.latest()
    if cl is None or sig is None or open_px is None: return None
    if time.time() - feed.last_msg > 10: return None   # stale feed
    spot, src = feed.nowcast()
    tau = max(0.0, end - time.time())
    tau_eff = max(0.0, tau - (feed.lag if src != "chainlink" else 0)) + (0 if src != "chainlink" else max(0, time.time() - k))
    sig_i = sig * 1.2   # fat-tail / model-risk inflation
    var = sig_i ** 2 * tau_eff
    if var <= 1e-9: p = 1.0 if spot >= open_px else 0.0
    else: p = ncdf((spot - open_px) / math.sqrt(var))
    return dict(p=min(max(p, 0.02), 0.98), spot=spot, src=src, cl=cl, sd=math.sqrt(var), sig=sig, tau=tau)

# ---------------------------------------------------------------- market data
FEED = None

class Market:
    def __init__(self, start):
        self.start = start; self.end = start + WIN
        self.slug = f"btc-updown-5m-{start}"
        self.tokens = None; self.open_px = None; self.accepting = False; self.loaded = 0; self.snapped = set(); self.next_cp = 0; self.cp_fail = 0; self.open_src = None
        self._load_lock = threading.Lock()

    def load(self, slow=False):
        if not self._load_lock.acquire(blocking=False):
            return
        try:
            self._load(slow=slow)
        finally:
            self._load_lock.release()

    def _load(self, slow=False):
        try:
            ev = get(f"{GAMMA}/events", slug=self.slug, slow=slow)
            if ev:
                m = ev[0]["markets"][0]
                self.tokens = dict(zip(json.loads(m["outcomes"]), json.loads(m["clobTokenIds"])))
                self.accepting = bool(m.get("acceptingOrders"))
        except Exception as e: log("gamma load err", self.slug, e)
        # Open price = Chainlink tick at the window-start second (verified == official openPrice / previous close).
        # Polymarket's crypto-price API can return a provisional (wrong) openPrice in the first seconds, so we
        # prefer our own tick and only use the API (fetched >= 15s after start) when we don't have it.
        if self.open_px is None and FEED and time.time() >= self.start + 3:
            with FEED.lock: v = FEED.px.get(self.start)
            if v is not None:
                self.open_px = v; self.open_src = "own_chainlink_tick"; log(f"open {self.slug} own chainlink tick {v:.3f}")
        if self.open_px is None and time.time() >= self.start + 15 and time.time() >= self.next_cp:
            try:
                d = get(CRYPTO_PRICE, symbol="BTC", eventStartTime=iso(self.start), variant="fiveminute", endDate=iso(self.end), slow=True)
                if d.get("openPrice"):
                    self.open_px = float(d["openPrice"]); self.open_src = "polymarket_api"
                    log(f"open {self.slug} from API {self.open_px:.3f}")
            except Exception as e:
                self.cp_fail += 1; self.next_cp = time.time() + min(60, 5 * 2 ** self.cp_fail); log("openPrice err", e)
        self.loaded = time.time()

def book(token):
    b = get(f"{CLOB}/book", token_id=token, slow=False, timeout=FAST)
    asks = sorted(((float(x["price"]), float(x["size"])) for x in b.get("asks", [])))
    bids = sorted(((float(x["price"]), float(x["size"])) for x in b.get("bids", [])), reverse=True)
    return asks, bids, b.get("timestamp")

def fee(shares, p): return round(shares * FEE_RATE * p * (1 - p), 5)

def trade_estimates(shares, cost, fe):
    """Purchase-time PnL if the trade wins or loses. Fixed at fill; never revised later."""
    sh = float(shares or 0)
    cost = float(cost or 0)
    fe = float(fe or 0)
    return round(sh - cost - fe, 5), round(-(cost + fe), 5)


def simulate_buy(asks, budget, max_price, min_shares=MIN_SHARES):
    """walk ask levels up to max_price, spending <= budget incl. fees. returns shares, cost, fee, avg"""
    sh = cost = fe = 0.0
    for p, sz in asks:
        if p > max_price: break
        per = p + FEE_RATE * p * (1 - p)
        can = min(sz, (budget - cost - fe) / per)
        can = math.floor(can * 100) / 100
        if can <= 0: break
        sh += can; cost += can * p; fe += fee(can, p)
        if can < sz: break
    if sh < min_shares: return None
    return dict(shares=round(sh, 2), cost=round(cost, 5), fee=round(fe, 5), avg=cost / sh)

def simulate_sell(bids, shares):
    """Walk the bid book to sell up to `shares`. Returns proceeds, fee, avg, shares sold."""
    left = float(shares)
    proceeds = fe = 0.0
    for p, sz in bids:
        can = min(float(sz), left)
        can = math.floor(can * 100) / 100
        if can <= 0:
            break
        proceeds += can * p
        fe += fee(can, p)
        left -= can
        if left <= 1e-9:
            break
    sold = float(shares) - left
    if sold < 0.01:
        return None
    return dict(shares=round(sold, 2), proceeds=round(proceeds, 5), fee=round(fe, 5), avg=proceeds / sold)

# ---------------------------------------------------------------- storage
def db():
    c = sqlite3.connect(DB, timeout=30, check_same_thread=False)
    try:
        c.execute("PRAGMA journal_mode=WAL")
    except Exception:
        pass
    c.execute("""create table if not exists trades(id integer primary key, ts real, time text, slug text, strategy text,
        side text, shares real, avg_price real, cost real, fee real, model_p real, edge real, spot real, open_px real,
        secs_left real, outcome text, payout real, pnl real, settled integer default 0, settled_time text)""")
    c.execute("""create table if not exists windows(slug text primary key, start integer, open_px real, close_px real,
        official text, my_twap real, my_spot_end real, check_ok integer, snaps text)""")
    c.execute("""create table if not exists decisions(id integer primary key, ts real, slug text, strategy text, action text, detail text)""")
    bot_settings.ensure_schema(c)
    c.commit(); return c

CSV_FIELDS = ["id", "time", "slug", "strategy", "side", "shares", "avg_price", "cost", "fee", "model_p", "edge", "spot", "open_px", "secs_left", "outcome", "payout", "pnl", "est_profit", "est_loss", "settled_time"]
def write_csv(c):
    rows = c.execute(f"select {','.join(CSV_FIELDS)} from trades where {LIVE} order by id").fetchall()
    with open(CSV, "w", newline="") as f:
        w = csv.writer(f); w.writerow(CSV_FIELDS); w.writerows(rows)

# ---------------------------------------------------------------- strategies
class Strategy:
    name = "base"; kelly = 0.25; cap = 10.0; min_edge = 0.04
    def __init__(self): self.done = set()
    def active(self, el, left): return False
    def side_choice(self, m, mp, books): return None

    @staticmethod
    def eff(p): return p + FEE_RATE * p * (1 - p)

def mkt_mid(books, side):
    asks, bids = books[side][0], books[side][1]
    if asks and bids: return (asks[0][0] + bids[0][0]) / 2
    return None

def blended(mp, books, w):
    """blend model P(up) with the market's own P(up) (Up mid). The book carries info we don't have (faster feeds, flow)."""
    m = mkt_mid(books, "Up")
    if m is None: return mp["p"], None
    return w * mp["p"] + (1 - w) * m, m

def pick(s, mp, books, w, lo, hi):
    pu, mm = blended(mp, books, w)
    if mm is not None and abs(mp["p"] - mm) > s.max_disagree:
        return None, f"model {mp['p']:.2f} vs market {mm:.2f}: disagreement too large, assume we're missing info"
    best = None
    for side, prob in (("Up", pu), ("Down", 1 - pu)):
        asks = books[side][0]
        if not asks: continue
        a = asks[0][0]; edge = prob - Strategy.eff(a)
        if edge >= s.min_edge and lo <= a <= hi and (best is None or edge > best[2]): best = (side, prob, edge, a)
    if best is None:
        info = ", ".join(f"{sd} fair {pr:.2f} ask {books[sd][0][0][0] if books[sd][0] else '-'}" for sd, pr in (("Up", pu), ("Down", 1 - pu)))
        return None, f"no edge: {info}"
    return best, None

class LateModel(Strategy):
    """PRIMARY. Last 75s..5s: vol model (Binance-nowcast of the Chainlink price) blended 60/40 with the book's own price;
    buy the side whose ask+fee is >= 4c below that fair value."""
    name = "late_model"; min_edge = 0.04; max_disagree = 0.30; w = 0.6; kelly = 0.50; cash_frac_cap = 0.12; min_stake = 3.0
    left_lo = 5; left_hi = 75
    def active(self, el, left): return self.left_lo <= left <= self.left_hi
    def side_choice(self, m, mp, books): return pick(self, mp, books, self.w, 0.03, 0.97)

class LateModelRaw(Strategy):
    """SHADOW of late_model using the raw model only (no blending). Shows if the model alone beats the book.
    Since 11-Nov 08:50 also subject to the global data-quality gates (freshness, |model-book|<=0.25, price band)."""
    name = "late_model_raw"; min_edge = 0.05; max_disagree = 9; w = 1.0; kelly = 0.50; cash_frac_cap = 0.12; min_stake = 3.0
    left_lo = 5; left_hi = 75
    def active(self, el, left): return self.left_lo <= left <= self.left_hi
    def side_choice(self, m, mp, books): return pick(self, mp, books, self.w, 0.03, 0.97)

class EarlyValue(Strategy):
    """Mid-window (60s..210s elapsed): fade an over-reacting book - buy the side priced >= 6c under blended fair value."""
    name = "early_value"; min_edge = 0.06; kelly = 0.4; cap = 6.0; cash_frac_cap = 0.08; max_disagree = 0.25; w = 0.5
    el_lo = 60; el_hi = 210
    def active(self, el, left): return self.el_lo <= el <= self.el_hi
    def side_choice(self, m, mp, books): return pick(self, mp, books, self.w, 0.15, 0.85)

class NaiveMomentum(Strategy):
    """Baseline (no model): with ~60s left, buy whichever side BTC (nowcast) is currently on if its ask <= 0.85. Flat $5."""
    name = "naive_momentum"; cap = 5.0; left_lo = 50; left_hi = 65; MAX_ASK = 0.85
    def active(self, el, left): return self.left_lo <= left <= self.left_hi
    def side_choice(self, m, mp, books):
        side = "Up" if mp["spot"] >= m.open_px else "Down"
        asks = books[side][0]
        if not asks or asks[0][0] > self.MAX_ASK: return None, f"{side} ask too high"
        prob = mp["p"] if side == "Up" else 1 - mp["p"]
        return (side, prob, prob - self.eff(asks[0][0]), asks[0][0]), None

class EveryRound(Strategy):
    """EVERY WINDOW. One entry per 5-min BTC Up/Down window. Starts trying with ~150s left:
    buy the side the vol model favors (same CLOB book as late_model), paying ask+fee even
    with small edge. Fixed $10 stake (scales to 10% of cash if cash <$100). Minimal gates only
    (fresh Chainlink <5s, not in post-reconnect warm-up). Skip only if the favoured side's
    ask is still >0.95. If the 150-20s window is missed due to a freeze, enter once feeds
    are healthy with time left."""
    name = "every_round"
    cap = 10.0
    min_edge = 0.0
    PREFERRED_LEFT_HI = 150
    PREFERRED_LEFT_LO = 20
    FALLBACK_LEFT_MIN = 5
    MAX_ASK = 0.95

    def active(self, el, left):
        # start at ~150s left, plus leftover-time fallback after a missed preferred window
        return self.FALLBACK_LEFT_MIN <= left <= self.PREFERRED_LEFT_HI

    def side_choice(self, m, mp, books):
        # same books dict / book() path as late_model (fetched once per tick in tick_market)
        side = "Up" if mp["p"] >= 0.5 else "Down"
        asks = books.get(side, ([], [], None))[0]
        if not asks:
            return None, f"{side} no asks"
        a = asks[0][0]
        if a > self.MAX_ASK:
            return None, f"{side} ask {a:.2f} > {self.MAX_ASK} (no upside)"
        prob = mp["p"] if side == "Up" else 1 - mp["p"]
        edge = prob - self.eff(a)
        return (side, prob, edge, a), None


class LateAdd(Strategy):
    """Last 10s..2s: if late_model or late_model_raw holds a winning position (spot vs open
    on that side), buy the same side at the ask if ask <= 0.95. If neither holds a position
    but spot is clearly on one side, enter that side if ask <= 0.93. Own $100 bankroll.
    Stake 10% of cash (min $3, max $15). One entry per window. Minimal gates only
    (fresh Chainlink <5s, not in post-reconnect warm-up)."""
    name = "late_add"
    cap = 15.0
    min_stake = 3.0
    min_edge = 0.0
    ADD_MAX_ASK = 0.95
    SOLO_MAX_ASK = 0.93
    CLEAR_USD = 10.0  # |spot-open| must be at least this for a standalone entry

    def active(self, el, left):
        return 2 <= left <= 10

    def side_choice(self, m, mp, books):
        held, src = (None, None)
        lookup = getattr(self, "open_late_side", None)
        if callable(lookup):
            held, src = lookup(m.slug)
        spot, open_px = mp["spot"], m.open_px
        self._entry_kind = "solo"
        self._maxp = self.SOLO_MAX_ASK
        if held:
            winning = (held == "Up" and spot >= open_px) or (held == "Down" and spot < open_px)
            if not winning:
                return None, f"held {held} via {src} is not winning spot={spot:.2f} open={open_px:.2f}"
            side = held
            max_ask = self.ADD_MAX_ASK
            self._entry_kind = f"add:{src}"
            self._maxp = max_ask
        else:
            gap = spot - open_px
            if abs(gap) < self.CLEAR_USD:
                return None, f"no late pos and spot not clear ({gap:+.2f} vs open)"
            side = "Up" if gap >= 0 else "Down"
            max_ask = self.SOLO_MAX_ASK
        asks = books.get(side, ([], [], None))[0]
        if not asks:
            return None, f"{side} no asks"
        a = asks[0][0]
        if a > max_ask:
            return None, f"{side} ask {a:.2f} > {max_ask} (no upside)"
        prob = mp["p"] if side == "Up" else 1 - mp["p"]
        return (side, prob, prob - self.eff(a), a), None

def momentum_features(feed, mp, open_px):
    """price-path features over the last ~60s from the nowcast exchange (Binance primary), with Chainlink as fallback.
    Velocities in $/s. dist = nowcast spot - open."""
    now_s = int(time.time())
    src = (mp.get("src") or "").split("+")[0]
    xs = None; used = src
    if src in Feed.EXCHANGES:
        xs = feed.ex_series(src, now_s - 62, now_s)
        if sum(1 for v in xs if v is None) > 5: xs = None
    if xs is None:
        k, _ = feed.latest()
        if k is None: return None
        xs = feed.series(k - 62, k); used = "chainlink"
    if len(xs) < 63 or any(v is None for v in xs[-63:]): return None
    xs = xs[-63:]
    def avg(seq): return sum(seq) / len(seq)
    cur = avg(xs[-3:])                       # 3s smoothed "now"
    v10 = (cur - avg(xs[-13:-10])) / 10.0
    v30 = (cur - avg(xs[-33:-30])) / 30.0
    v60 = (cur - avg(xs[:3])) / 60.0
    d = [xs[i + 1] - xs[i] for i in range(len(xs) - 1)]
    rv60 = math.sqrt(sum(x * x for x in d) / len(d))
    dist = mp["spot"] - open_px
    toward_open = (dist > 0 and v30 < 0) or (dist < 0 and v30 > 0)
    return dict(src=used, dist=dist, v10=v10, v30=v30, v60=v60, acc=v30 - v60, rv60=rv60, toward_open=toward_open)

class MomentumLate(Strategy):
    """Last 90s..10s: uses the PRICE PATH of the preceding seconds, not just distance-from-open + time left.
    Features from the Binance nowcast: v10/v30/v60 velocity ($/s), acceleration (v30-v60), toward/away from open.
    Projects spot to expiry as spot + 0.4*v30*tau (drift capped at 0.75 model-sd), then P(up) from the same vol model
    on the projected distance. Buys the side whose ask+fee is >= 4c below that probability (6c if betting on a
    cross of the open), only if v30 agrees with the side (|v30| >= max($0.20/s, 0.6*sigma/sqrt30)) and the last
    10s are not sharply reversing. Ask 0.03..0.95. Global data gates apply. Kelly 0.5, cap 12% of cash, min $3."""
    name = "momentum_late"; min_edge = 0.04; cross_edge = 0.06; kelly = 0.50; cash_frac_cap = 0.12; min_stake = 3.0
    max_disagree = 0.30
    K = 0.4; DRIFT_CAP_SD = 0.75; VMIN = 0.20; VMIN_SIG = 0.6; REV_MULT = 1.0; MAX_ASK = 0.95; MIN_ASK = 0.03
    left_lo = 10; left_hi = 90
    def active(self, el, left): return self.left_lo <= left <= self.left_hi

    def side_choice(self, m, mp, books):
        self._skip_key = None; self._feat = None
        left = m.end - time.time()
        tag = f"L{int(left // 10) * 10:02d}"
        def skip(key, msg):
            self._skip_key = key
            return None, f"{tag} {key}: {msg}"
        f = momentum_features(FEED, mp, m.open_px) if FEED else None
        if not f: return skip("mom_nodata", "not enough 60s price history")
        sd = mp.get("sd") or 0.0; sig = mp.get("sig") or 1.0
        tau_eff = (sd / (sig * 1.2)) ** 2 if sig else 0.0
        drift = self.K * f["v30"] * tau_eff
        cap = self.DRIFT_CAP_SD * sd
        drift = max(-cap, min(cap, drift))
        proj = mp["spot"] + drift
        if sd <= 1e-6: pm = 1.0 if proj >= m.open_px else 0.0
        else: pm = ncdf((proj - m.open_px) / sd)
        pm = min(max(pm, 0.02), 0.98)
        vmin = max(self.VMIN, self.VMIN_SIG * sig / math.sqrt(30))
        f.update(p_model=round(mp["p"], 4), p_mom=round(pm, 4), drift=round(drift, 2), proj_dist=round(proj - m.open_px, 2),
                 tau_eff=round(tau_eff, 1), sd=round(sd, 2), sig=round(sig, 3), vmin=round(vmin, 3))
        self._feat = f
        feat = (f"dist={f['dist']:+.2f} v10={f['v10']:+.3f} v30={f['v30']:+.3f} v60={f['v60']:+.3f} acc={f['acc']:+.3f} "
                f"rv60={f['rv60']:.2f} toward_open={int(f['toward_open'])} drift={drift:+.2f} p_model={mp['p']:.3f} p_mom={pm:.3f} "
                f"vmin={vmin:.2f} src={f['src']}")
        self._feat_str = feat
        mm = mkt_mid(books, "Up")
        if mm is not None and abs(pm - mm) > self.max_disagree:
            return skip("strategy_disagree", f"p_mom {pm:.2f} vs book {mm:.2f} | {feat}")
        if abs(f["v30"]) < vmin:
            return skip("mom_weak", f"|v30| < {vmin:.2f} | {feat}")
        side = "Up" if f["v30"] > 0 else "Down"
        sgn = 1 if side == "Up" else -1
        if sgn * f["v10"] < 0 and abs(f["v10"]) >= max(2 * vmin, self.REV_MULT * abs(f["v30"])):
            return skip("mom_reversal", f"{side}: last-10s v10 {f['v10']:+.3f} reversing vs v30 | {feat}")
        prob = pm if side == "Up" else 1 - pm
        cross = (sgn * f["dist"]) < 0           # betting price crosses the open
        need = self.cross_edge if cross else self.min_edge
        asks = books.get(side, ([], [], None))[0]
        if not asks: return skip("no_book", f"{side} no asks | {feat}")
        a = asks[0][0]
        if a > self.MAX_ASK: return skip("price_too_high", f"{side} ask {a:.2f} > {self.MAX_ASK} | {feat}")
        if a < self.MIN_ASK: return skip("price_band", f"{side} ask {a:.2f} < {self.MIN_ASK} | {feat}")
        edge = prob - self.eff(a)
        if edge < need:
            return skip("no_edge", f"{side} p {prob:.3f} ask {a:.2f} edge {edge:+.3f} < {need}{' (cross)' if cross else ''} | {feat}")
        self._entry_kind = ("cross " if cross else "") + feat
        return (side, prob, edge, a), None


class LatencyHunterStrategy(Strategy):
    """Latency hunter (PAPER ONLY). Measures 15s/30s/60s BTC momentum, estimates P(up) from
    recent log-return volatility, and buys the side whose executable ask is at least 6c below
    that probability after taker fees and expected book slippage. Own $100 demo bankroll.
    At most one entry per window. Fed with raw Binance ticks (fastest available);
    the open reference is the market's Chainlink open. Does not place real orders."""
    name = "latency_hunter"
    min_edge = 0.06
    kelly = 0.50
    cash_frac_cap = 0.08
    min_stake = 3.0
    left_lo = 20
    left_hi = 300
    TYPICAL_STAKE = 8.0  # dollars used only to estimate walk-the-book slippage

    def __init__(self):
        super().__init__()
        self.hunter = LatencyHunter()
        self.last_signal = None
        self._last_log_key = None
        self._lock = threading.Lock()

    def on_price(self, price, timestamp):
        with self._lock:
            self.hunter.update_price(price, timestamp=timestamp)

    def seed(self, series):
        with self._lock:
            for ts, px in series:
                self.hunter.update_price(px, timestamp=ts)

    def on_fill(self, timestamp):
        with self._lock:
            self.hunter.mark_trade(timestamp=timestamp)

    def active(self, el, left):
        # After ~65s of BTC history the engine can fire; it refuses the last 20s itself.
        return left >= self.left_lo

    @staticmethod
    def _best(books, side, leg):
        levels = books.get(side, ([], [], None))[leg]
        return levels[0][0] if levels else None

    def _estimated_cost(self, books):
        """Fee model per share at the best ask, plus expected slippage from walking the ask book."""
        costs = []
        for side in ("Up", "Down"):
            asks = books.get(side, ([], [], None))[0]
            if not asks:
                continue
            best = asks[0][0]
            if not (0 < best < 1):
                continue
            fee_ps = FEE_RATE * best * (1.0 - best)
            fill = simulate_buy(asks, self.TYPICAL_STAKE, min(0.97, best + 0.03), min_shares=1.0)
            slip = (fill["avg"] - best) if fill else 0.01
            costs.append(fee_ps + max(0.0, slip))
        return max(costs) if costs else 0.0

    def side_choice(self, m, mp, books):
        up_bid = self._best(books, "Up", 1)
        up_ask = self._best(books, "Up", 0)
        down_bid = self._best(books, "Down", 1)
        down_ask = self._best(books, "Down", 0)
        cost = self._estimated_cost(books)
        with self._lock:
            sig = self.hunter.evaluate(
                market_open_price=m.open_px,  # official Chainlink window open
                seconds_remaining=m.end - time.time(),
                up_bid=up_bid, up_ask=up_ask,
                down_bid=down_bid, down_ask=down_ask,
                estimated_cost=cost,
            )
            self.last_signal = sig
        key = (sig.action, sig.reason)
        if key != self._last_log_key:
            self._last_log_key = key
            d = sig.diagnostics or {}
            log(f"HUNTER {sig.action} {sig.reason} net={sig.net_edge:+.3f} p={sig.confidence:.3f} "
                f"cost={d.get('estimated_cost')} left={d.get('seconds_remaining')} "
                f"spot={d.get('btc_price')} open={d.get('market_open_price')}")
        # Include ACTION in the skip/wait detail so the existing site state_he can show it.
        if sig.action == "WAIT":
            return None, f"{sig.action}: {sig.reason} net={sig.net_edge:+.3f} p={sig.confidence:.3f}"
        side = "Up" if sig.action == "BUY_UP" else "Down"
        asks = books.get(side, ([], [], None))[0]
        if not asks:
            return None, f"{sig.action}: no asks after signal"
        a = asks[0][0]
        return (side, sig.confidence, sig.net_edge, a), None



class Manual(Strategy):
    """MANUAL (PAPER ONLY). Does not enter on its own. Place Up/Down paper buys from its page."""
    name = "manual"
    min_edge = 0.0
    kelly = 0.0
    cap = 20.0
    cash_frac_cap = 1.0
    min_stake = 1.0
    MAX_ASK = 0.99
    def active(self, el, left):
        return False
    def side_choice(self, m, mp, books):
        return None, "manual: no auto entries"


class Genetic(Strategy):
    """GENETIC (PAPER ONLY). Keeps a small population of parameter genomes,
    tracks each virtually with the same fees and book-walk as live paper trading,
    and lets the current champion trade this bot's own $100 bankroll.
    Evolves every N settled rounds. Does not change the other bots."""
    name = "genetic"
    min_edge = 0.04
    max_disagree = 0.30
    w = 0.6
    kelly = 0.50
    cash_frac_cap = 0.12
    min_stake = 3.0
    cap = 10.0
    left_lo = 5
    left_hi = 75
    MIN_ASK = 0.30
    MAX_ASK = 0.97

    def __init__(self):
        super().__init__()
        self.engine = None
        self.last_signal = None

    def active(self, el, left):
        eng = self.engine
        if eng and eng.champion():
            g = eng.champion()["genome"]
            return g["left_lo"] <= left <= g["left_hi"]
        return self.left_lo <= left <= self.left_hi

    def on_observe(self, m, mp, books, el, left):
        if self.engine:
            self.engine.observe(m, mp, books, el, left)

    def on_window_settled(self, slug, official):
        if self.engine:
            self.engine.on_window_settled(slug, official)

    def side_choice(self, m, mp, books):
        if not self.engine:
            return None, "genetic: no engine"
        left = m.end - time.time()
        ch, why = self.engine.champion_choice(m, mp, books, left)
        self.last_signal = SimpleNamespace(
            action=self.engine.last_decision,
            reason=self.engine.last_reason,
            net_edge=(ch[2] if ch else 0.0),
            confidence=(ch[1] if ch else 0.0),
        )
        return ch, why


STRATS = [LateModel(), LateModelRaw(), EarlyValue(), NaiveMomentum(), EveryRound(), MomentumLate(), LatencyHunterStrategy(), Genetic(), Manual()]
PRIMARY = "late_model"

# ---------------------------------------------------------------- bot
class Bot:
    def __init__(self):
        global FEED
        self.dblock = threading.RLock()
        self.mkt_lock = threading.Lock()
        self.c = db(); self.feed = Feed(); FEED = self.feed; self.feed.on_ex_tick = self._on_hunter_ex_tick; self.mkts = {}
        self._stop = threading.Event()
        self._settle_next = {}
        self._settle_fail = {}
        self._last_health_log = 0
        self.skips = {s.name: {} for s in STRATS}      # strategy -> reason -> evaluation ticks skipped (since start)
        self.skip_windows = {s.name: {} for s in STRATS}  # strategy -> reason -> distinct windows
        self._skip_seen = set()
        self.started = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.settings = {}
        self._settings_rev = None
        with self.dblock:
            bot_settings.ensure_schema(self.c)
            for s in STRATS:
                for (slug,) in self.c.execute(f"select slug from trades where strategy=? and {LIVE}", (s.name,)):
                    s.done.add(slug)
        self._reload_settings(force=True)
        for s in STRATS:
            if s.name == "late_add":
                s.open_late_side = self.open_late_side
            if s.name == "genetic":
                from genetic_engine import GeneticEngine
                s.engine = GeneticEngine(os.path.join(BASE, "genetic_state.json"))
                s.engine.load()
                s.engine.configure((self.settings or {}).get("genetic"))

    def open_late_side(self, slug):
        """Prefer an unsettled late_model position in this window; else late_model_raw."""
        with self.dblock:
            for name in ("late_model", "late_model_raw"):
                row = self.c.execute(
                    f"select side from trades where slug=? and strategy=? and settled=0 and {LIVE} order by id desc limit 1",
                    (slug, name),
                ).fetchone()
                if row:
                    return row[0], name
        return None, None

    def ensure_mkt(self, st):
        with self.mkt_lock:
            if st not in self.mkts:
                self.mkts[st] = Market(st)
            return self.mkts[st]


    _ATTR_MAP = {
        "min_edge": "min_edge", "kelly": "kelly", "cap": "cap",
        "cash_frac_cap": "cash_frac_cap", "min_stake": "min_stake",
        "max_disagree": "max_disagree", "w": "w", "cross_edge": "cross_edge",
        "max_ask": "MAX_ASK", "min_ask": "MIN_ASK",
        "preferred_left_hi": "PREFERRED_LEFT_HI", "preferred_left_lo": "PREFERRED_LEFT_LO",
        "fallback_left_min": "FALLBACK_LEFT_MIN", "typical_stake": "TYPICAL_STAKE",
        "left_lo": "left_lo", "left_hi": "left_hi", "el_lo": "el_lo", "el_hi": "el_hi",
    }

    def _reload_settings(self, force=False):
        with self.dblock:
            bot_settings.ensure_schema(self.c)
            rev = bot_settings.settings_rev(self.c)
            if not force and rev == getattr(self, "_settings_rev", None):
                return
            self.settings = bot_settings.load_all(self.c)
            self._settings_rev = rev
        for st in STRATS:
            self._apply_settings(st)

    def _apply_settings(self, st):
        try:
            cfg = self.settings.get(st.name) or bot_settings.defaults_for(st.name)
        except ValueError:
            return
        for key, attr in self._ATTR_MAP.items():
            if key in cfg:
                setattr(st, attr, cfg[key])
        if st.name == "latency_hunter" and hasattr(st, "hunter"):
            kw = {k: cfg[k] for k in bot_settings.HUNTER_CONFIG_KEYS if k in cfg}
            base = HunterConfig()
            merged = {**{f: getattr(base, f) for f in base.__dataclass_fields__}, **kw}
            st.hunter.config = HunterConfig(**{k: merged[k] for k in base.__dataclass_fields__})
        if st.name == "genetic":
            migrated = bot_settings.migrate_genetic_cfg(cfg)
            if migrated:
                cfg = migrated
                self.settings[st.name] = cfg
                try:
                    with self.dblock:
                        bot_settings.save_one(self.c, "genetic", cfg)
                    bot_settings.request_sync(os.path.dirname(os.path.abspath(STATUS)) if STATUS else ".")
                except Exception:
                    pass
            if getattr(st, "engine", None):
                st.engine.configure(cfg)

    def _can_open(self, st, auto=False):
        try:
            cfg = self.settings.get(st.name) or bot_settings.defaults_for(st.name)
        except ValueError:
            cfg = {"enabled": True, "auto_trading": True}
        if not cfg.get("enabled", True):
            return False
        if auto and st.name != "manual" and not cfg.get("auto_trading", True):
            return False
        if auto and st.name == "manual":
            return False
        return True

    def _cfg(self, name):
        return (self.settings or {}).get(name) or bot_settings.defaults_for(name)

    def _alert(self, name, msg):
        cfg = self._cfg(name)
        if cfg.get("alerts"):
            log(f"ALERT {name}: {msg}")

    def _current_market(self):
        now = time.time()
        cur = int(now // WIN * WIN)
        m = self.mkts.get(cur)
        if not m or not m.tokens or m.open_px is None or not m.accepting or now >= m.end:
            return None, now
        return m, now

    def _process_admin_commands(self):
        with self.dblock:
            cmds = bot_settings.pending_commands(self.c)
        for cmd in cmds:
            try:
                result = self._exec_command(cmd)
                with self.dblock:
                    bot_settings.finish_command(self.c, cmd["id"], "done", result or {"ok": True})
            except Exception as e:
                log("admin cmd err", cmd.get("kind"), cmd.get("strategy"), repr(e)[:200])
                with self.dblock:
                    bot_settings.finish_command(self.c, cmd["id"], "error", {"ok": False, "error": str(e)[:300]})

    def _exec_command(self, cmd):
        kind, name, payload = cmd["kind"], cmd.get("strategy"), cmd.get("payload") or {}
        if kind in ("set_settings", "reset_defaults", "toggle"):
            if name not in bot_settings.KNOWN:
                raise ValueError(f"unknown strategy: {name}")
            if kind == "reset_defaults":
                clean = bot_settings.defaults_for(name)
            elif kind == "toggle":
                cur = self._cfg(name)
                if "enabled" in payload:
                    cur["enabled"] = bool(payload["enabled"])
                if "auto_trading" in payload:
                    cur["auto_trading"] = bool(payload["auto_trading"])
                clean = bot_settings.validate(name, cur)
            else:
                cur = self._cfg(name)
                clean = bot_settings.validate(name, payload, base=cur)
            with self.dblock:
                bot_settings.save_one(self.c, name, clean)
            self._reload_settings(force=True)
            bot_settings.request_sync(os.path.dirname(os.path.abspath(DB)) if os.path.isabs(DB) else os.getcwd())
            # ROOT is the bot working dir
            bot_settings.request_sync(os.path.dirname(os.path.abspath(STATUS)) if STATUS else ".")
            log(f"settings {kind} {name} mode={bot_settings.run_mode(clean, name)}")
            return {"ok": True, "settings": clean, "run_mode": bot_settings.run_mode(clean, name)}
        if kind == "buy":
            return self._manual_buy(payload.get("side"), payload.get("amount"))
        if kind == "reset_bankroll":
            return self._reset_bankroll(name, payload.get("confirm"))
        raise ValueError(f"unknown command: {kind}")

    def _manual_buy(self, side, amount):
        if side not in ("Up", "Down"):
            raise ValueError("side must be Up or Down")
        try:
            amount = float(amount)
        except (TypeError, ValueError):
            raise ValueError("amount must be a number") from None
        if amount < 1.0 or amount > 100.0:
            raise ValueError("amount must be between $1 and $100")
        if not self._can_open(next(st for st in STRATS if st.name == "manual"), auto=False):
            raise ValueError("manual bot is off")
        m, now = self._current_market()
        if m is None:
            raise ValueError("no live market currently accepting paper orders")
        try:
            books = {sd: book(tok) for sd, tok in m.tokens.items()}
        except Exception as e:
            raise ValueError(f"order book unavailable: {e}") from e
        asks = books.get(side, ([], [], None))[0]
        if not asks:
            raise ValueError(f"no {side} asks")
        cfg = self._cfg("manual")
        a = asks[0][0]
        maxp = min(float(cfg.get("max_ask") or 0.99), a + 0.03)
        with self.dblock:
            bank = self.bankroll("manual")
        stake = min(amount, bank, float(cfg.get("cap") or 20), bank)
        if stake < 1.0:
            raise ValueError(f"insufficient cash (${bank:.2f})")
        fill = simulate_buy(asks, stake, maxp, min_shares=1.0)
        if not fill:
            raise ValueError(f"could not fill at least 1 share under {maxp:.2f}")
        mp = p_up(self.feed, m.start, m.open_px) or {}
        left = m.end - now
        with self.dblock:
            est_p, est_l = trade_estimates(fill["shares"], fill["cost"], fill["fee"])
            self.c.execute("""insert into trades(ts,time,slug,strategy,side,shares,avg_price,cost,fee,model_p,edge,spot,open_px,secs_left,est_profit,est_loss)
                values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", (
                now, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), m.slug, "manual", side,
                fill["shares"], round(fill["avg"], 4), fill["cost"], fill["fee"],
                round(float(mp["p"]), 4) if mp.get("p") is not None else None,
                None, mp.get("spot"), m.open_px, left, est_p, est_l))
            self.c.commit(); write_csv(self.c)
        log(f"TRADE manual {m.slug} {side} {fill['shares']}sh @{fill['avg']:.3f} cost ${fill['cost']:.2f} fee ${fill['fee']:.3f} left={left:.0f}s (manual paper buy)")
        self._alert("manual", f"bought {side} {fill['shares']}sh cost ${fill['cost']:.2f}")
        bot_settings.request_sync(os.path.dirname(os.path.abspath(STATUS)) if STATUS else ".")
        return {"ok": True, "side": side, "shares": fill["shares"], "avg": fill["avg"],
                "cost": fill["cost"], "fee": fill["fee"], "slug": m.slug}

    def _reset_bankroll(self, name, confirm):
        if confirm != "RESET":
            raise ValueError("confirmation required")
        if name not in bot_settings.KNOWN:
            raise ValueError(f"unknown strategy: {name}")
        stamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with self.dblock:
            bot_settings.ensure_schema(self.c)
            open_n = self.c.execute(
                f"select count(*) from trades where strategy=? and settled=0 and {LIVE}", (name,)
            ).fetchone()[0]
            if open_n:
                raise ValueError("cannot reset while positions are open; wait for them to settle")
            cols = "id,ts,time,slug,side,shares,avg_price,cost,fee,model_p,edge,spot,open_px,secs_left,outcome,payout,pnl,settled,settled_time"
            rows = self.c.execute(
                f"select {cols} from trades where strategy=? and {LIVE}", (name,)
            ).fetchall()
            for r in rows:
                self.c.execute(
                    f"insert into trades_archive(archived_at,reason,strategy,{cols}) values(?,?,?,{','.join(['?']*19)})",
                    (stamp, "reset_bankroll", name) + tuple(r),
                )
            self.c.execute(
                f"update trades set archived=1, archived_at=?, archive_reason='reset_bankroll' where strategy=? and {LIVE}",
                (stamp, name),
            )
            n = self.c.execute("select changes()").fetchone()
            self.c.commit(); write_csv(self.c)
            # allow re-entry in a later window; clear done for this strat
            for st in STRATS:
                if st.name == name:
                    st.done.clear()
            cash = self.bankroll(name)
        log(f"RESET bankroll {name}: archived {len(rows)} trades, cash now ${cash:.2f}")
        self._alert(name, f"bankroll reset to ${cash:.2f}; archived {len(rows)} trades")
        bot_settings.request_sync(os.path.dirname(os.path.abspath(STATUS)) if STATUS else ".")
        return {"ok": True, "archived": len(rows), "cash": cash}

    def _risk_exits(self):
        m, now = self._current_market()
        if m is None:
            return
        with self.dblock:
            rows = self.c.execute(
                f"select id, strategy, side, shares, avg_price, cost, fee from trades where settled=0 and {LIVE}"
            ).fetchall()
        if not rows:
            return
        by = {}
        for r in rows:
            by.setdefault(r[1], []).append(r)
        need = False
        for st in STRATS:
            cfg = self._cfg(st.name)
            if cfg.get("risk_management") and (float(cfg.get("stop_loss") or 0) > 0 or float(cfg.get("take_profit") or 0) > 0):
                if by.get(st.name):
                    need = True
                    break
        if not need:
            return
        try:
            books = {sd: book(tok) for sd, tok in m.tokens.items()}
        except Exception as e:
            log("risk-exit book err", e)
            return
        for st in STRATS:
            cfg = self._cfg(st.name)
            if not cfg.get("risk_management"):
                continue
            sl = float(cfg.get("stop_loss") or 0)
            tp = float(cfg.get("take_profit") or 0)
            if sl <= 0 and tp <= 0:
                continue
            for tid, name, side, shares, avg, cost, fe in by.get(st.name, []):
                bids = books.get(side, ([], [], None))[1]
                if not bids:
                    continue
                bid = bids[0][0]
                why = None
                if sl > 0 and bid <= avg * (1.0 - sl):
                    why = "stop_loss"
                elif tp > 0 and bid >= min(0.99, avg * (1.0 + tp)):
                    why = "take_profit"
                if not why:
                    continue
                fill = simulate_sell(bids, shares)
                if not fill or fill["shares"] < shares * 0.99:
                    log(f"risk-exit {name} #{tid} {why} skipped: not enough bid depth")
                    continue
                payout = fill["proceeds"]
                sell_fee = fill["fee"]
                pnl = payout - sell_fee - cost - fe
                with self.dblock:
                    self.c.execute(
                        "update trades set outcome=?, payout=?, pnl=?, settled=1, settled_time=?, exit_reason=? where id=? and settled=0",
                        (f"sold_{why}", payout, round(pnl, 5), datetime.now().strftime("%Y-%m-%d %H:%M:%S"), why, tid),
                    )
                    self.c.commit(); write_csv(self.c)
                log(f"EXIT {why} {name} #{tid} {side} {fill['shares']}sh bid~{fill['avg']:.3f} proceeds ${payout:.2f} fee ${sell_fee:.3f} pnl ${pnl:+.2f}")
                self._alert(name, f"{why}: sold {side} pnl ${pnl:+.2f}")
        bot_settings.request_sync(os.path.dirname(os.path.abspath(STATUS)) if STATUS else ".")

    def bankroll(self, strat):

        r = self.c.execute(f"select coalesce(sum(case when settled=1 then pnl else -(cost+fee) end),0) from trades where strategy=? and {LIVE}", (strat,)).fetchone()[0]
        return START_BANK + r

    def count_skip(self, m, s, reason):
        d = self.skips[s.name]; d[reason] = d.get(reason, 0) + 1
        key = (m.slug, s.name, reason)
        if key not in self._skip_seen:
            self._skip_seen.add(key)
            w = self.skip_windows[s.name]; w[reason] = w.get(reason, 0) + 1
            if len(self._skip_seen) > 20000: self._skip_seen.clear()

    def every_round_pre(self):
        """minimal gates for every_round: fresh Chainlink (<5s) and not in post-reconnect warm-up.
        Does not require exchange agreement or the full data-quality gate used by the other strategies."""
        now = time.time()
        k, _ = self.feed.latest()
        if k is None:
            return "no_chainlink", "data gate: no_chainlink {}"
        cl_age = now - k
        msg_age = now - self.feed.last_msg if self.feed.last_msg else 1e9
        if cl_age > 5.0 or msg_age > 5.0:
            return "stale_chainlink", f"data gate: stale_chainlink {{'cl_age': {round(cl_age, 1)}}}"
        since = now - self.feed.last_disrupt
        if since < WARMUP_S:
            return "warmup", f"data gate: warmup {{'warm_s': {round(since, 1)}, 'since_disrupt_s': {round(since, 1)}}}"
        return None

    def guard(self, m, s, mp, books, left, side, prob, a):
        """global post-choice guards; returns (reason_key, detail) or None"""
        mm = mkt_mid(books, "Up")
        if mm is not None and abs(mp["p"] - mm) > MAX_DISAGREE:
            return "book_disagree", f"model P(up) {mp['p']:.2f} vs book mid {mm:.2f} (> {MAX_DISAGREE})"
        if a < BAND_LO or a > BAND_HI:
            if not (prob >= BAND_CONF and self.feed.warm_for() >= BAND_WARM_S):
                return "price_band", f"{side} ask {a:.2f} outside [{BAND_LO},{BAND_HI}] and model {prob:.2f} not confident/fresh enough"
        return None

    def decide(self, m, s, mp, books, el, left, pre=None):
        if pre:
            key, why = pre
            self.count_skip(m, s, key); self.dec(m, s, "skip", f"[{key}] {why}"); return
        ch, why = s.side_choice(m, mp, books)
        if not ch:
            if why:
                if s.name == "momentum_late" and getattr(s, "_skip_key", None): key = s._skip_key
                elif "disagreement" in why: key = "strategy_disagree"
                elif why.startswith("no edge"): key = "no_edge"
                elif "no upside" in why: key = "price_too_high"
                elif "no asks" in why: key = "no_book"
                elif why.startswith("wait_late_window"): key = "wait_late_window"
                elif "not winning" in why: key = "not_winning"
                elif "not clear" in why: key = "not_clear"
                else: key = "strategy_other"
                self.count_skip(m, s, key)
                self.dec(m, s, "skip", why)
            return
        side, prob, edge, a = ch
        if s.name != "manual":
            cfg = self._cfg(s.name)
            if cfg.get("max_price_on"):
                try:
                    cap = float(cfg.get("max_price") if cfg.get("max_price") is not None else 0.48)
                except (TypeError, ValueError):
                    cap = 0.48
                eff = Strategy.eff(a)
                if eff > cap:
                    self.count_skip(m, s, "max_price")
                    self.dec(m, s, "skip", f"[max_price] מחיר מעל המקסימום ask={a:.3f} eff={eff:.3f} > {cap:.3f}")
                    return
        if s.name not in ("every_round", "latency_hunter"):
            g = self.guard(m, s, mp, books, left, side, prob, a)
            if g:
                self.count_skip(m, s, g[0]); self.dec(m, s, "skip", f"[{g[0]}] {g[1]}"); return
        with self.dblock:
            bank = self.bankroll(s.name)
        if s.name == "every_round":
            # fixed $10; scale to 10% of cash if bankroll is under $100
            stake = s.cap if bank >= 100 else max(0.0, bank * 0.10)
        elif s.name == "late_add":
            # 10% of cash, floored at $3 and capped at $15
            stake = min(s.cap, max(s.min_stake, 0.10 * bank), bank)
        elif s.name == "naive_momentum":
            stake = min(s.cap, bank)
        elif s.name == "genetic":
            from genetic_engine import size_stake as _genetic_size
            genome = None
            if getattr(s, "engine", None) and s.engine.champion():
                genome = s.engine.champion()["genome"]
            if not genome:
                genome = {
                    "cap": getattr(s, "cap", 10), "cash_frac_cap": getattr(s, "cash_frac_cap", 0.12),
                    "kelly": getattr(s, "kelly", 0.5), "min_stake": 3.0, "min_edge": getattr(s, "min_edge", 0.04),
                }
            stake = _genetic_size(genome, bank, prob, a)
        else:
            q = Strategy.eff(a)
            b = (1 - q) / q
            kf = max(0.0, (prob * b - (1 - prob)) / b)    # Kelly fraction
            frac_cap = getattr(s, "cash_frac_cap", None)
            cap = (frac_cap * bank) if frac_cap is not None else s.cap
            stake = min(cap, s.kelly * kf * bank, bank)
            min_stake = getattr(s, "min_stake", 0.0)
            if min_stake and 0 < stake < min_stake:
                stake = min(min_stake, bank)
        if stake < 1.0:
            self.count_skip(m, s, "stake_small")
            self.dec(m, s, "skip", f"stake too small {stake:.2f} side={side} p={prob:.3f} ask={a}"); return
        # don't pay more than model fair value minus a small margin when walking the book
        if s.name == "every_round":
            maxp = min(0.95, a + 0.03)
            # paper $3 stake often cannot buy the live orderMinSize of 5 at late-window favorites
            fill = simulate_buy(books[side][0], stake, maxp, min_shares=1.0)
        elif s.name == "late_add":
            maxp = min(getattr(s, "_maxp", 0.95), a + 0.03)
            fill = simulate_buy(books[side][0], stake, maxp, min_shares=1.0)
        elif s.name == "momentum_late":
            maxp = min(s.MAX_ASK, prob - s.min_edge / 2, a + 0.03)
            fill = simulate_buy(books[side][0], stake, maxp)
        else:
            min_edge = s.min_edge
            if s.name == "genetic" and getattr(s, "engine", None) and s.engine.champion():
                min_edge = s.engine.champion()["genome"].get("min_edge", min_edge)
            maxp = 0.86 if s.name == "naive_momentum" else min(0.97, prob - min_edge / 2, a + 0.03)
            fill = simulate_buy(books[side][0], stake, maxp)
        if not fill:
            self.count_skip(m, s, "no_fill")
            self.dec(m, s, "skip", f"no fill >= {MIN_SHARES} sh under {maxp:.2f} side={side}"); return
        now = time.time()
        with self.dblock:
            est_p, est_l = trade_estimates(fill["shares"], fill["cost"], fill["fee"])
            self.c.execute("""insert into trades(ts,time,slug,strategy,side,shares,avg_price,cost,fee,model_p,edge,spot,open_px,secs_left,est_profit,est_loss)
                values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", (now, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), m.slug, s.name, side,
                fill["shares"], round(fill["avg"], 4), fill["cost"], fill["fee"], round(prob, 4), round(edge, 4), mp["spot"], m.open_px, left, est_p, est_l))
            self.c.commit(); write_csv(self.c)
        s.done.add(m.slug)
        on_fill = getattr(s, "on_fill", None)
        if callable(on_fill):
            on_fill(now)
        fb = " fallback=1" if s.name == "every_round" and left < 20 else ""
        kind = f" {getattr(s, '_entry_kind', '')}" if s.name in ("late_add", "momentum_late") else ""
        if s.name == "momentum_late":
            self.dec(m, s, "enter", f"L{int(left // 10) * 10:02d} {side} {fill['shares']}sh @{fill['avg']:.3f} stake ${fill['cost'] + fill['fee']:.2f} p_mom={prob:.3f} edge={edge:.3f} left={left:.0f}s | {getattr(s, '_entry_kind', '')}")
        log(f"TRADE {s.name} {m.slug} {side} {fill['shares']}sh @{fill['avg']:.3f} cost ${fill['cost']:.2f} fee ${fill['fee']:.3f} p={prob:.3f} edge={edge:.3f} left={left:.0f}s spot={mp['spot']:.2f}({mp['src']}) cl={mp['cl']:.2f} open={m.open_px:.2f} gate={mp.get('gate')}{fb}{kind}")

    def dec(self, m, s, action, detail):
        key = (m.slug, s.name, action, detail[:12])
        seen = self.__dict__.setdefault("_seen", set())
        if key in seen: return
        seen.add(key)
        with self.dblock:
            self.c.execute("insert into decisions(ts,slug,strategy,action,detail) values(?,?,?,?,?)", (time.time(), m.slug, s.name, action, detail)); self.c.commit()

    def snap(self, m, mp, books, left):
        try:
            with self.dblock:
                row = self.c.execute("select snaps from windows where slug=?", (m.slug,)).fetchone()
                snaps = json.loads(row[0]) if row and row[0] else []
                def _lvls(side, leg, n=8):
                    arr = books.get(side, ([], [], None))[leg] or []
                    out = []
                    for p, sz in arr[:n]:
                        try:
                            out.append([round(float(p), 4), round(float(sz), 4)])
                        except (TypeError, ValueError):
                            pass
                    return out
                ua = books["Up"][0][0][0] if books["Up"][0] else None
                da = books["Down"][0][0][0] if books["Down"][0] else None
                ub = books["Up"][1][0][0] if books["Up"][1] else None
                db = books["Down"][1][0][0] if books["Down"][1] else None
                px_path, px_src = [], None
                try:
                    src0 = (mp.get("src") or "").split("+")[0]
                    now_s = int(time.time())
                    xs = None
                    if src0 in Feed.EXCHANGES:
                        xs = self.feed.ex_series(src0, now_s - 70, now_s)
                        px_src = src0
                    if xs is None or sum(1 for v in xs if v is None) > 15:
                        k, _ = self.feed.latest()
                        if k:
                            xs = self.feed.series(k - 70, k)
                            px_src = "chainlink"
                    if xs:
                        px_path = [None if v is None else round(float(v), 2) for v in xs]
                except Exception:
                    px_path, px_src = [], None
                snaps.append(dict(
                    left=round(left), p=round(mp["p"], 4),
                    sd=None if mp.get("sd") is None else round(float(mp["sd"]), 4),
                    sig=None if mp.get("sig") is None else round(float(mp["sig"]), 6),
                    up_ask=ua, down_ask=da, up_bid=ub, down_bid=db,
                    up_asks=_lvls("Up", 0), down_asks=_lvls("Down", 0),
                    up_bids=_lvls("Up", 1), down_bids=_lvls("Down", 1),
                    spot=round(mp["spot"], 2), cl=round(mp["cl"], 2), src=mp["src"], lag=self.feed.lag,
                    px=px_path, px_src=px_src,
                ))
                self.c.execute("insert into windows(slug,start,open_px,snaps) values(?,?,?,?) on conflict(slug) do update set open_px=excluded.open_px, snaps=excluded.snaps",
                               (m.slug, m.start, m.open_px, json.dumps(snaps))); self.c.commit()
        except Exception as e: log("snap err", e)

    def tick_market(self, m):
        now = time.time(); el = now - m.start; left = m.end - now
        if left <= 0: return
        if not m.tokens or m.open_px is None or now - m.loaded > 60:
            if now - m.loaded > 3: m.load(slow=False)
        if not m.tokens or m.open_px is None or not m.accepting: return
        act = [s for s in STRATS if m.slug not in s.done and s.active(el, left) and self._can_open(s, auto=True)]
        due = [x for x in (270, 240, 210, 180, 150, 120, 90, 75, 60, 45, 30, 20, 15, 10) if left <= x and x not in m.snapped and left > x - 8]
        want_snap = bool(due)
        if due: m.snapped.update(due)
        if not act and not want_snap: return
        g_ok, g_why, g_det = self.feed.gate()
        mp = p_up(self.feed, m.start, m.open_px)
        if not mp:
            for s in act: self.count_skip(m, s, "no_model")
            return
        try:
            books = {side: book(tok) for side, tok in m.tokens.items()}
        except Exception as e:
            for s in act: self.count_skip(m, s, "book_error")
            log("book err", e); return
        if want_snap: self.snap(m, mp, books, left)
        for s in STRATS:
            obs = getattr(s, "on_observe", None)
            if callable(obs):
                try:
                    obs(m, mp, books, el, left)
                except Exception:
                    log("observe err", traceback.format_exc())
        pre = None
        if not g_ok:
            pre = (g_why, f"data gate: {g_why} {g_det}")
        else:
            mp["gate"] = g_det.get("warm_s")
            mm = mkt_mid(books, "Up")
            if mm is not None and left < CONTRA_LEFT_S:
                if (mp["spot"] > m.open_px and mm < CONTRA_P) or (mp["spot"] < m.open_px and mm > 1 - CONTRA_P):
                    pre = ("book_contradicts_spot", f"spot-open {mp['spot'] - m.open_px:+.2f} but book P(up) {mm:.2f} with {left:.0f}s left")
        for s in act:
            try:
                pre_s = pre if s.name not in ("every_round", "late_add", "latency_hunter") else self.every_round_pre()
                self.decide(m, s, mp, books, el, left, pre_s)
            except Exception: log("decide err", traceback.format_exc())

    def prefetch(self):
        now = time.time(); cur = int(now // WIN * WIN)
        self.ensure_mkt(cur); self.ensure_mkt(cur + WIN)
        nxt = self.mkts.get(cur + WIN)
        if nxt and (not nxt.tokens or now - nxt.loaded > 30):
            if now - nxt.loaded > 3:
                nxt.load(slow=True)
        # also keep current-window tokens warm with the longer budget if still missing
        curm = self.mkts.get(cur)
        if curm and (not curm.tokens or curm.open_px is None) and now - curm.loaded > 3:
            curm.load(slow=True)

    def _chainlink_end_tick(self, start):
        """Chainlink (or fallback) tick at the window-end second, trying nearby seconds if needed."""
        for off in (0, -1, 1, -2):
            se = self.feed.series(start + WIN + off, start + WIN + off)
            if se and se[0] is not None:
                return se[0]
        return None

    def _apply_trade_results(self, slug, winner, pending):
        """Write payout/pnl for this slug. pending=1 is a Chainlink provisional; 0 is official."""
        stamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        newly, corrected = [], []
        # Unsettled rows (first application)
        for tid, side, sh, cost, fe in self.c.execute(
            f"select id,side,shares,cost,fee from trades where slug=? and settled=0 and {LIVE}", (slug,)
        ).fetchall():
            payout = sh if side == winner else 0.0
            pnl = payout - cost - fe
            self.c.execute(
                "update trades set outcome=?, payout=?, pnl=?, settled=1, pending=?, settled_time=? where id=?",
                (winner, payout, round(pnl, 5), 1 if pending else 0, stamp, tid),
            )
            newly.append((tid, side, pnl, pending))
        if not pending:
            # Confirm or correct provisionally settled rows
            for tid, side, sh, cost, fe, prev_out, prev_pnl in self.c.execute(
                f"select id,side,shares,cost,fee,outcome,pnl from trades where slug=? and settled=1 and coalesce(pending,0)=1 and {LIVE}",
                (slug,),
            ).fetchall():
                payout = sh if side == winner else 0.0
                pnl = payout - cost - fe
                self.c.execute(
                    "update trades set outcome=?, payout=?, pnl=?, settled=1, pending=0, settled_time=? where id=?",
                    (winner, payout, round(pnl, 5), stamp, tid),
                )
                if prev_out != winner:
                    corrected.append((tid, side, prev_out, winner, prev_pnl, pnl))
                else:
                    newly.append((tid, side, pnl, False))
        return newly, corrected

    def settle(self):
        now = time.time()
        with self.dblock:
            rows = self.c.execute(f"select distinct slug from trades where (settled=0 or coalesce(pending,0)=1) and {LIVE}").fetchall()
            wins = self.c.execute("select slug,start from windows where official is null or (official is not null and check_ok is null)").fetchall()
            slugs = {r[0] for r in rows} | {w[0] for w in wins}
        urgent = False
        for slug in slugs:
            start = int(slug.rsplit("-", 1)[1])
            age = now - (start + WIN)
            if age < 1:
                if age > -5:
                    urgent = True
                continue
            if age < 90:
                urgent = True

            tw = self.feed.series(start + WIN - TWAP_LB, start + WIN)
            my_twap = sum(tw) / len(tw) if len(tw) > 30 else None
            my_spot = self._chainlink_end_tick(start)

            # Provisional result from Chainlink close vs open, as soon as the end tick exists.
            with self.dblock:
                existing = self.c.execute("select official,open_px,close_px,check_ok from windows where slug=?", (slug,)).fetchone()
            prev_official = existing[0] if existing else None
            prev_open = existing[1] if existing else None
            if prev_open is None:
                m = self.mkts.get(start)
                if m is not None and getattr(m, "open_px", None) is not None:
                    prev_open = m.open_px
            if prev_official not in ("Up", "Down") and my_spot is not None and prev_open is not None:
                prov = "Up" if my_spot >= prev_open else "Down"
                with self.dblock:
                    self.c.execute(
                        "insert into windows(slug,start,open_px,my_twap,my_spot_end) values(?,?,?,?,?) "
                        "on conflict(slug) do update set my_twap=coalesce(excluded.my_twap, windows.my_twap), "
                        "my_spot_end=coalesce(excluded.my_spot_end, windows.my_spot_end), "
                        "open_px=coalesce(windows.open_px, excluded.open_px)",
                        (slug, start, prev_open, my_twap, my_spot),
                    )
                    newly, _ = self._apply_trade_results(slug, prov, pending=True)
                    self.c.commit()
                    write_csv(self.c)
                for tid, side, pnl, _ in newly:
                    log(f"PROVISIONAL #{tid} {slug} {side} -> {prov} (Chainlink close {my_spot} vs open {prev_open}) pnl ${pnl:+.2f} pending")

            # Official Polymarket resolution: try immediately after the window ends (no 20s delay).
            if now < self._settle_next.get(slug, 0):
                continue
            winner = close_px = open_px = None
            try:
                ev = get(f"{GAMMA}/events", slug=slug, slow=True)
                mk = ev[0]["markets"][0]
                prices = [float(x) for x in json.loads(mk["outcomePrices"])]
                outs = json.loads(mk["outcomes"])
                if mk.get("closed") and sorted(prices) == [0.0, 1.0]:
                    winner = outs[prices.index(1.0)]
            except Exception as e:
                n = self._settle_fail.get(slug, 0) + 1
                self._settle_fail[slug] = n
                delay = 2 if age < 45 else min(60, 5 * 2 ** min(n, 4))
                self._settle_next[slug] = time.time() + delay
                log("settle fetch err", slug, e)
                continue
            if winner not in ("Up", "Down"):
                self._settle_next[slug] = time.time() + (2 if age < 45 else 10)
                continue
            try:
                d = get(CRYPTO_PRICE, symbol="BTC", eventStartTime=iso(start), variant="fiveminute", endDate=iso(start + WIN), slow=True)
                close_px, open_px = d.get("closePrice"), d.get("openPrice")
            except Exception as e:
                log("settle crypto-price err", slug, e)
            ox = open_px if open_px is not None else prev_open
            check = None
            if close_px and ox:
                check = int((("Up" if close_px >= ox else "Down")) == winner)
            elif my_spot is not None and ox is not None:
                check = int((("Up" if my_spot >= ox else "Down")) == winner)
                log(f"cross-check {slug} used local chainlink ticks close={my_spot} open={ox} check={check}")
            with self.dblock:
                self.c.execute("insert into windows(slug,start,open_px,close_px,official,my_twap,my_spot_end,check_ok) values(?,?,?,?,?,?,?,?) "
                               "on conflict(slug) do update set close_px=coalesce(excluded.close_px, windows.close_px), official=coalesce(excluded.official, windows.official), my_twap=coalesce(excluded.my_twap, windows.my_twap), my_spot_end=coalesce(excluded.my_spot_end, windows.my_spot_end), check_ok=coalesce(excluded.check_ok, windows.check_ok), open_px=coalesce(windows.open_px, excluded.open_px)",
                               (slug, start, open_px if open_px is not None else ox, close_px, winner, my_twap, my_spot, check))
                newly, corrected = self._apply_trade_results(slug, winner, pending=False)
                self.c.commit(); write_csv(self.c)
            for tid, side, pnl, _ in newly:
                log(f"SETTLE #{tid} {slug} {side} -> winner {winner} pnl ${pnl:+.2f}")
            for tid, side, prev_out, new_out, prev_pnl, pnl in corrected:
                log(f"SETTLE CORRECTED #{tid} {slug} {side} {prev_out} -> {new_out} pnl {prev_pnl:+.2f} -> {pnl:+.2f}")
            if prev_official is None or (check is not None and existing and existing[3] is None) or close_px is not None:
                log(f"RESOLVED {slug} official={winner} close={close_px} open={open_px if open_px is not None else ox} my_twap60={my_twap} my_spot_end={my_spot} check={check}")
            if prev_official is None and winner in ("Up", "Down"):
                for st in STRATS:
                    fn = getattr(st, "on_window_settled", None)
                    if callable(fn):
                        try:
                            fn(slug, winner)
                        except Exception:
                            log("settle hook err", traceback.format_exc())
            self._settle_fail.pop(slug, None)
            self._settle_next.pop(slug, None)
        return urgent


    def status(self):
        with self.dblock:
            st = {"updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S %Z").strip() + " (Asia/Jerusalem)", "mode": "PAPER - no real money",
                  "primary": PRIMARY, "start_bankroll": START_BANK, "fee_model": f"taker fee = shares*{FEE_RATE}*p*(1-p), rebates ignored", "strategies": {}}
            for s in STRATS:
                r = self.c.execute(f"""select count(*), sum(settled), sum(case when settled=1 and pnl>0 then 1 else 0 end),
                    coalesce(sum(case when settled=1 then pnl end),0), coalesce(sum(fee),0), coalesce(sum(case when settled=0 then cost+fee end),0),
                    coalesce(sum(cost+fee),0) from trades where strategy=? and {LIVE}""", (s.name,)).fetchone()
                n, ns, w, pnl, fees, openc, vol = r; ns = ns or 0
                rec = dict(doc=(s.__doc__ or '').strip(), trades=n, settled=ns, wins=w or 0, win_rate=round(w / ns, 3) if ns else None,
                    realized_pnl=round(pnl, 2), fees_paid=round(fees, 3), open_exposure=round(openc, 2), turnover=round(vol, 2),
                    cash=round(self.bankroll(s.name), 2), equity_if_open_at_cost=round(START_BANK + pnl, 2), roi_on_turnover=round(pnl / vol, 3) if vol else None)
                sig = getattr(s, "last_signal", None)
                if sig is not None:
                    rec["decision"] = sig.action
                    rec["decision_reason"] = sig.reason
                    rec["decision_net_edge"] = round(float(sig.net_edge), 4)
                    rec["decision_confidence"] = round(float(sig.confidence), 4)
                if s.name == "genetic" and getattr(s, "engine", None):
                    rec["genetic"] = s.engine.public_state()
                    rec["decision"] = rec["genetic"].get("decision") or rec.get("decision")
                    rec["decision_reason"] = rec["genetic"].get("decision_reason") or rec.get("decision_reason")
                cfg = (self.settings or {}).get(s.name) or bot_settings.defaults_for(s.name)
                rec["enabled"] = bool(cfg.get("enabled", True))
                rec["auto_trading"] = bool(cfg.get("auto_trading", s.name != "manual"))
                rec["risk_management"] = bool(cfg.get("risk_management", False))
                rec["alerts"] = bool(cfg.get("alerts", False))
                rec["run_mode"] = bot_settings.run_mode(cfg, s.name)
                rec["settings"] = cfg
                st["strategies"][s.name] = rec
            p = st["strategies"][PRIMARY]
            st.update(bankroll=p["equity_if_open_at_cost"], pnl=p["realized_pnl"], trades=p["trades"], win_rate=p["win_rate"])
            wr = self.c.execute("select count(*), sum(check_ok) from windows where official is not null").fetchone()
            st["windows_resolved_seen"] = wr[0]; st["settlement_cross_checks_ok"] = wr[1]
        st["skips_since_start"] = dict(since=self.started, note="ticks = 1Hz evaluations skipped per reason; windows = distinct 5-min windows where that reason fired",
                                       ticks={k: dict(sorted(v.items(), key=lambda x: -x[1])) for k, v in self.skips.items()},
                                       windows={k: dict(sorted(v.items(), key=lambda x: -x[1])) for k, v in self.skip_windows.items()})
        st["gates"] = dict(chainlink_fresh_s=CL_FRESH_S, source_fresh_s=SRC_FRESH_S, warmup_s=WARMUP_S, agree_usd_min=AGREE_USD,
                           max_model_book_disagree=MAX_DISAGREE, price_band=[BAND_LO, BAND_HI], band_override_conf=BAND_CONF,
                           contradiction=dict(left_lt_s=CONTRA_LEFT_S, book_p=CONTRA_P))
        k, px = self.feed.latest()
        nc, src = self.feed.nowcast()
        st["feed"] = dict(chainlink_btc=px, age_s=round(time.time() - k, 1) if k else None, sigma_per_s=self.feed.sigma(),
                          nowcast=nc, nowcast_src=src, chainlink_lag_s=self.feed.lag, lag_corr=getattr(self.feed, "lag_corr", None),
                          health=self.feed.health_snapshot())
        tmp = STATUS + ".tmp"
        with open(tmp, "w") as f: json.dump(st, f, indent=2)
        os.replace(tmp, STATUS)

    def run_settle_loop(self):
        # first pass soon so a restart can settle a just-ended window promptly
        first = True
        while not self._stop.is_set():
            try:
                self.prefetch()
                urgent = self.settle()
            except Exception:
                log("settle loop err", traceback.format_exc())
                urgent = False
            # Poll every second for a short window after a 5-minute market ends, otherwise every 8s.
            self._stop.wait(1 if (first or urgent) else 8)
            first = False

    def run_status_loop(self):
        while not self._stop.is_set():
            try:
                self.status()
                if time.time() - self._last_health_log > 60:
                    log(self.feed.health_line())
                    self._last_health_log = time.time()
            except Exception:
                log("status loop err", traceback.format_exc())
            self._stop.wait(3)

    def _on_hunter_ex_tick(self, name, mid, ts):
        """Every raw exchange mid. Prefer Binance; other venues only if Binance is stale (>3s)."""
        if name != "binance":
            with self.feed.lock:
                bt, bm = self.feed.ex_last["binance"]
            if bm is not None and ts - bt <= 3.0:
                return
        for s in STRATS:
            fn = getattr(s, "on_price", None)
            if callable(fn):
                fn(mid, ts)

    def _seed_hunter_history(self):
        """Backfill ~120s of 1s Binance mids so LatencyHunter is warm after a restart."""
        now = int(time.time())
        pts, src = [], None
        for name in ("binance", "coinbase", "kraken"):
            xs = self.feed.ex_series(name, now - 120, now)
            if xs and sum(1 for v in xs if v is not None) >= 65:
                pts = [(float(now - 120 + i), float(px)) for i, px in enumerate(xs) if px]
                src = name
                break
        if len(pts) < 65:
            return False
        for s in STRATS:
            seed = getattr(s, "seed", None)
            if callable(seed):
                seed(pts)
        log(f"hunter seeded {len(pts)} {src} ticks from {pts[0][0]:.0f} to {pts[-1][0]:.0f}")
        return True

    def _feed_hunter_prices(self):
        # Primary path is the Binance websocket callback. 1Hz: seed, then backstop if the socket is quiet.
        if not getattr(self, "_hunter_seeded", False):
            self._hunter_seeded = bool(self._seed_hunter_history())
        now = time.time()
        with self.feed.lock:
            bt, bm = self.feed.ex_last["binance"]
        if bm is not None and now - bt <= 3.0:
            return
        name, t, mid = self.feed.best_exchg()
        if mid is None or t is None or now - t > 3.0:
            return
        for s in STRATS:
            fn = getattr(s, "on_price", None)
            if callable(fn):
                fn(mid, t)

    def run(self):
        log("polybot starting (PAPER MODE)")
        self._stop = threading.Event()
        self.feed._stop = self._stop
        def _sig(*_):
            log("shutdown signal")
            self._stop.set()
        signal.signal(signal.SIGTERM, _sig)
        signal.signal(signal.SIGINT, _sig)
        threading.Thread(target=self.run_settle_loop, daemon=True, name="settle").start()
        threading.Thread(target=self.run_status_loop, daemon=True, name="status").start()
        last_lag = 0
        while not self._stop.is_set():
            try:
                now = time.time(); cur = int(now // WIN * WIN)
                self.ensure_mkt(cur); self.ensure_mkt(cur + WIN)
                with self.mkt_lock:
                    for st in list(self.mkts):
                        if st + WIN < now - 5: del self.mkts[st]
                self._feed_hunter_prices()
                self._reload_settings()
                self._process_admin_commands()
                self._risk_exits()
                self.tick_market(self.mkts[cur])
                if now - last_lag > 120: self.feed.estimate_lag(); last_lag = now
            except Exception:
                log("loop err", traceback.format_exc())
            self._stop.wait(1.0)
        log("polybot stopped")

if __name__ == "__main__":
    Bot().run()
