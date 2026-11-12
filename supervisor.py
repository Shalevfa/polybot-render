#!/usr/bin/env python3
"""Hugging Face Space supervisor for the PAPER-only polybot (no wallet, no keys, no real funds).

* serves GET /health (JSON) and GET /stats (dashboard_data.py JSON) on :7860 (HF app port)
* probes connectivity; switches Binance to binance.us if binance.com is geo-blocked
* restores trades.db (+trades.csv, price_hist.json) from a private HF dataset repo on boot
* runs polybot.py, web/server.py and web_push.py, restarting any that exit
* primary mode: snapshots trades.db to the dataset on every settled trade / change (debounced)
  and at least every 5 min when changed; daily history squash; final sync on SIGTERM
* shadow mode (MODE=shadow): trades on a private copy, never uploads state, never pushes to the site
* self keep-alive ping of the Space URL every 6h (free Spaces sleep after 48h without traffic)
"""
from __future__ import annotations

import gzip, hmac, hashlib, json, os, shutil, signal, sqlite3, subprocess, sys, threading, time, traceback
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import requests

APP = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, APP)
import bot_settings
DB = os.path.join(APP, "trades.db")
CSV = os.path.join(APP, "trades.csv")
HIST = os.path.join(APP, "web", "price_hist.json")
STATUS = os.path.join(APP, "status.json")
BOTLOG = os.path.join(APP, "bot.log")
PUSH_OK = os.path.join(APP, "web_push.ok")

HF_TOKEN = os.environ.get("HF_TOKEN", "").strip()
STATE_REPO = os.environ.get("STATE_REPO", "shalevnavon/polybot-state")
MODE = os.environ.get("MODE", "shadow").strip().lower()          # primary | shadow
ALLOW_FRESH = os.environ.get("ALLOW_FRESH", "0") == "1"           # start with empty DB if dataset has none
SPACE_HOST = os.environ.get("SPACE_HOST", "")                     # set by HF, e.g. user-name.hf.space
PRIMARY = MODE == "primary"
T0 = time.time()

state = {
    "mode": MODE, "paper_only": True, "started": time.strftime("%F %T %Z"), "phase": "booting",
    "restore": None, "binance": None, "probes": {}, "children": {},
    "sync": {"last_ok": None, "last_err": None, "uploads": 0, "last_sig": None, "last_squash": None},
    "keepalive": {"last": None, "status": None},
}
lock = threading.Lock()
stopping = threading.Event()
procs: dict[str, subprocess.Popen] = {}


def log(*a):
    print(time.strftime("%F %T"), "[supervisor]", *a, flush=True)


# ------------------------------------------------------------------ health server
def health() -> dict:
    now = time.time()
    with lock:
        h = json.loads(json.dumps(state, default=str))
    h["uptime_s"] = round(now - T0)
    try:
        st = json.load(open(STATUS))
        h["status_age_s"] = round(now - os.path.getmtime(STATUS), 1)
        f = st.get("feed") or {}
        h["feed"] = {"chainlink_btc": f.get("chainlink_btc"), "age_s": f.get("age_s"), "nowcast_src": f.get("nowcast_src"),
                     "sources": {k: {"last_age_s": (v or {}).get("last_age_s"), "errors": (v or {}).get("errors")}
                                 for k, v in (f.get("health") or {}).items() if isinstance(v, dict)}}
        h["strategies"] = {k: {x: v.get(x) for x in ("trades", "settled", "wins", "realized_pnl", "open_exposure", "bankroll", "cash")
                               if x in v} for k, v in (st.get("strategies") or {}).items()}
        h["updated"] = st.get("updated")
    except Exception as e:
        h["status_err"] = repr(e)[:200]
    try:
        h["push_age_s"] = round(now - float(open(PUSH_OK).read().strip()), 1)
    except Exception:
        h["push_age_s"] = None
    try:
        c = sqlite3.connect(f"file:{DB}?mode=ro", uri=True, timeout=5)
        h["db"] = dict(zip(("trades", "settled", "max_id"), c.execute(
            "select count(*), coalesce(sum(settled),0), coalesce(max(id),0) from trades").fetchone()))
        c.close()
    except Exception as e:
        h["db"] = {"err": repr(e)[:200]}
    alive = all(p.poll() is None for p in procs.values()) and bool(procs)
    h["ok"] = bool(alive and h.get("status_age_s") is not None and h["status_age_s"] < 30
                   and (not PRIMARY or (h["push_age_s"] is not None and h["push_age_s"] < 60)))
    return h


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def _send(self, code, obj):
        b = json.dumps(obj, ensure_ascii=False, indent=1).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(b)))
        self.end_headers()
        self.wfile.write(b)

    def do_GET(self):
        path = self.path.split("?")[0]
        try:
            if path in ("/", "/health"):
                return self._send(200, health())
            if path == "/stats":
                r = subprocess.run([sys.executable, os.path.join(APP, "dashboard_data.py")], capture_output=True, timeout=30, cwd=APP)
                return self._send(200 if r.returncode == 0 else 500, json.loads(r.stdout) if r.returncode == 0 else {"err": r.stderr.decode()[-500:]})
            if path == "/status":
                return self._send(200, json.load(open(STATUS)))
            if path == "/api/live":
                port = os.environ.get("POLYBOT_DASH_PORT", "8765")
                r = requests.get(f"http://127.0.0.1:{port}/api/live", timeout=10)
                return self._send(r.status_code, r.json())
            self._send(404, {"err": "not found", "paths": ["/health", "/status", "/stats", "/api/live"]})
        except Exception as e:
            self._send(500, {"err": repr(e)[:300]})

    def do_POST(self):
        path = self.path.split("?")[0]
        try:
            if path not in ("/admin", "/admin/"):
                return self._send(404, {"err": "not found"})
            if not _admin_ok(self):
                return self._send(401, {"ok": False, "error": "unauthorized"})
            if not _rate_ok():
                return self._send(429, {"ok": False, "error": "rate limited"})
            length = int(self.headers.get("Content-Length") or 0)
            if length > 50_000:
                return self._send(413, {"ok": False, "error": "too large"})
            raw = self.rfile.read(length) if length else b"{}"
            try:
                body = json.loads(raw.decode("utf-8") or "{}")
            except Exception:
                return self._send(400, {"ok": False, "error": "bad json"})
            code, obj = _handle_admin(self, body)
            return self._send(code, obj)
        except Exception as e:
            log("admin err", repr(e)[:300])
            return self._send(500, {"ok": False, "error": "internal"})



def _admin_ok(handler) -> bool:
    secret = (os.environ.get("ADMIN_TOKEN") or "").strip()
    if not secret:
        return False
    header = handler.headers.get("Authorization") or ""
    if not header.startswith("Bearer "):
        return False
    got = header[7:].encode("utf-8")
    exp = secret.encode("utf-8")
    return hmac.compare_digest(hashlib.sha256(got).digest(), hashlib.sha256(exp).digest())


_admin_hits = []
_admin_lock = threading.Lock()

def _rate_ok() -> bool:
    now = time.time()
    with _admin_lock:
        _admin_hits[:] = [t for t in _admin_hits if now - t < 60]
        if len(_admin_hits) >= 60:
            return False
        _admin_hits.append(now)
        return True


def _handle_admin(handler, body: dict):
    if not isinstance(body, dict):
        return 400, {"ok": False, "error": "payload must be an object"}
    op = body.get("op")
    name = body.get("name")
    if op == "ping":
        return 200, {"ok": True}
    if op not in ("toggle", "save", "reset_defaults", "reset_bankroll", "buy"):
        return 400, {"ok": False, "error": "unknown op"}
    if op == "buy":
        name = "manual"
    if not name or name not in bot_settings.KNOWN:
        return 400, {"ok": False, "error": "unknown strategy"}
    payload = {}
    kind = op
    if op == "toggle":
        kind = "toggle"
        if "enabled" in body:
            payload["enabled"] = bool(body["enabled"])
        if "auto_trading" in body:
            payload["auto_trading"] = bool(body["auto_trading"])
        if not payload:
            return 400, {"ok": False, "error": "enabled or auto_trading required"}
    elif op == "save":
        kind = "set_settings"
        try:
            payload = bot_settings.validate(name, body.get("settings") or {}, base=bot_settings.defaults_for(name))
        except ValueError as e:
            return 400, {"ok": False, "error": str(e)}
    elif op == "reset_defaults":
        kind = "reset_defaults"
        payload = {}
    elif op == "reset_bankroll":
        kind = "reset_bankroll"
        if body.get("confirm") != "RESET":
            return 400, {"ok": False, "error": "confirmation required"}
        payload = {"confirm": "RESET"}
    elif op == "buy":
        kind = "buy"
        payload = {"side": body.get("side"), "amount": body.get("amount")}
        if payload["side"] not in ("Up", "Down"):
            return 400, {"ok": False, "error": "side must be Up or Down"}
        try:
            amt = float(payload["amount"])
        except (TypeError, ValueError):
            return 400, {"ok": False, "error": "amount must be a number"}
        if amt < 1 or amt > 100:
            return 400, {"ok": False, "error": "amount must be between $1 and $100"}
    c = sqlite3.connect(DB, timeout=30)
    try:
        bot_settings.ensure_schema(c)
        cmd_id = bot_settings.enqueue(c, kind, name, payload)
    finally:
        c.close()
    log("admin enqueued", kind, name, "id", cmd_id)
    return 200, {"ok": True, "command_id": cmd_id, "op": op, "name": name}


def serve():
    ThreadingHTTPServer(("0.0.0.0", int(os.environ.get("PORT", "7860"))), Handler).serve_forever()


# ------------------------------------------------------------------ connectivity
def probe(name, url, **kw):
    try:
        r = requests.get(url, timeout=10, **kw)
        res = r.status_code
    except Exception as e:
        res = repr(e)[:120]
    with lock:
        state["probes"][name] = res
    return res


def choose_binance():
    # binance.com geo-blocks US IPs (HF Spaces run in the US). data-*.binance.vision is Binance's public
    # market-data mirror (same prices, supports 1s klines); binance.us is a last resort (no 1s klines).
    choice = "unavailable (bot falls back to coinbase/kraken)"
    for name, rest, ws in [("binance.com", "https://api.binance.com", "wss://stream.binance.com:443"),
                           ("binance.vision", "https://data-api.binance.vision", "wss://data-stream.binance.vision:443"),
                           ("binance.us", "https://api.binance.us", "wss://stream.binance.us:9443")]:
        if probe(name, rest + "/api/v3/ping") == 200:
            os.environ["BINANCE_REST"], os.environ["BINANCE_WS"] = rest, ws
            choice = name
            break
    with lock:
        state["binance"] = choice
    log("binance:", choice)
    for n, u in [("gamma", "https://gamma-api.polymarket.com/events?limit=1"),
                 ("clob", "https://clob.polymarket.com/time"),
                 ("coinbase", "https://api.exchange.coinbase.com/products/BTC-USD/ticker"),
                 ("kraken", "https://api.kraken.com/0/public/Time"),
                 ("crypto_price", "https://polymarket.com/api/crypto/crypto-price?symbol=BTC&variant=fiveminute&eventStartTime="
                  + time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(int(time.time()) // 300 * 300 - 300))
                  + "&endDate=" + time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(int(time.time()) // 300 * 300))),
                 ("vercel_site", "https://polybot-btc5m.vercel.app/api/data")]:
        log("probe", n, probe(n, u))
    import websocket
    for n, u in [("ws_chainlink_rtds", "wss://ws-live-data.polymarket.com"),
                 ("ws_binance", os.environ.get("BINANCE_WS", "wss://stream.binance.com:443") + "/ws/btcusdt@bookTicker"),
                 ("ws_coinbase", "wss://ws-feed.exchange.coinbase.com"),
                 ("ws_kraken", "wss://ws.kraken.com")]:
        try:
            w = websocket.create_connection(u, timeout=10)
            w.close()
            res = "ws_ok"
        except Exception as e:
            res = repr(e)[:120]
        with lock:
            state["probes"][n] = res
        log("probe", n, res)


# ------------------------------------------------------------------ state restore / sync
def api():
    from huggingface_hub import HfApi
    return HfApi(token=HF_TOKEN)


def restore():
    if os.environ.get("SKIP_RESTORE") == "1":  # local testing only
        with lock:
            state["restore"] = {"ok": True, "skipped": True}
        return
    from huggingface_hub import hf_hub_download
    from huggingface_hub.utils import EntryNotFoundError
    delay = 5
    while True:
        try:
            if not HF_TOKEN:
                raise RuntimeError("HF_TOKEN secret missing")
            tmp = os.path.join(APP, ".restore")
            os.makedirs(tmp, exist_ok=True)
            p = hf_hub_download(STATE_REPO, "trades.db.gz", repo_type="dataset", token=HF_TOKEN,
                                local_dir=tmp, force_download=True)
            raw = gzip.decompress(open(p, "rb").read())
            for ext in ("", "-wal", "-shm"):
                if os.path.exists(DB + ext):
                    os.remove(DB + ext)
            with open(DB, "wb") as f:
                f.write(raw)
            c = sqlite3.connect(DB)
            n = c.execute("select count(*), coalesce(sum(settled),0) from trades").fetchone()
            c.close()
            for name, dst in (("trades.csv", CSV), ("price_hist.json", HIST)):
                try:
                    q = hf_hub_download(STATE_REPO, name, repo_type="dataset", token=HF_TOKEN, local_dir=tmp, force_download=True)
                    shutil.copyfile(q, dst)
                except Exception:
                    pass
            info = {"ok": True, "trades": n[0], "settled": n[1], "bytes": len(raw), "at": time.strftime("%F %T")}
            with lock:
                state["restore"] = info
            log("restored", info)
            return
        except EntryNotFoundError:
            if ALLOW_FRESH:
                with lock:
                    state["restore"] = {"ok": True, "fresh": True, "at": time.strftime("%F %T")}
                log("no snapshot in dataset; ALLOW_FRESH=1 -> starting with an empty DB")
                return
            err = "no trades.db.gz in dataset and ALLOW_FRESH!=1"
        except Exception as e:
            err = repr(e)[:300]
        with lock:
            state["restore"] = {"ok": False, "err": err, "retry_in_s": delay}
        log("restore failed (bot NOT started, retrying):", err)
        time.sleep(delay)
        delay = min(delay * 2, 120)


def db_sig():
    c = sqlite3.connect(f"file:{DB}?mode=ro", uri=True, timeout=10)
    try:
        trades = c.execute("select count(*), coalesce(sum(settled),0), coalesce(max(id),0), "
                           "coalesce(round(sum(coalesce(pnl,0)),4),0) from trades").fetchone()
        try:
            rev = c.execute("select coalesce(max(updated_ts),0) from bot_settings").fetchone()[0]
        except Exception:
            rev = 0
        try:
            arch = c.execute("select coalesce(sum(archived),0) from trades").fetchone()[0]
        except Exception:
            arch = 0
        return list(trades) + [rev, arch]
    finally:
        c.close()


def upload_snapshot(reason):
    from huggingface_hub import CommitOperationAdd
    tmpdb = os.path.join(APP, ".snap.db")
    if os.path.exists(tmpdb):
        os.remove(tmpdb)
    src = sqlite3.connect(DB, timeout=30)
    dst = sqlite3.connect(tmpdb)
    with dst:
        src.backup(dst)
    src.close()
    dst.execute("PRAGMA journal_mode=DELETE")
    sig = dst.execute("select count(*), coalesce(sum(settled),0), coalesce(max(id),0), "
                      "coalesce(round(sum(coalesce(pnl,0)),4),0) from trades").fetchone()
    dst.close()
    gz = gzip.compress(open(tmpdb, "rb").read(), 6)
    ops = [CommitOperationAdd("trades.db.gz", gz)]
    for name, p in (("trades.csv", CSV), ("price_hist.json", HIST)):
        if os.path.exists(p):
            ops.append(CommitOperationAdd(name, open(p, "rb").read()))
    ops.append(CommitOperationAdd("meta.json", json.dumps({"sig": sig, "at": time.strftime("%F %T %Z"),
                                                           "reason": reason, "space": SPACE_HOST}).encode()))
    api().create_commit(STATE_REPO, ops, repo_type="dataset", commit_message=f"snapshot {reason} {sig}")
    return list(sig)


def sync_loop():
    last_up = 0.0
    last_sig = None
    try:
        last_sig = list(db_sig())
    except Exception:
        pass
    with lock:
        state["sync"]["last_sig"] = last_sig
    while not stopping.wait(30):
        try:
            sig = list(db_sig())
            changed = sig != last_sig
            settled_changed = last_sig is None or sig[1] != last_sig[1] or sig[0] != last_sig[0]
            want = os.path.join(APP, ".want_sync")
            force = os.path.exists(want)
            if changed and (settled_changed and time.time() - last_up > 45 or time.time() - last_up > 300) or (force and time.time() - last_up > 8):
                up = upload_snapshot("settle" if settled_changed else ("settings" if force else "periodic"))
                last_sig, last_up = up, time.time()
                with lock:
                    state["sync"].update(last_ok=time.strftime("%F %T"), last_sig=up, uploads=state["sync"]["uploads"] + 1, last_err=None)
                if force:
                    try:
                        os.remove(want)
                    except OSError:
                        pass
                log("state synced", up)
            if time.time() - (state["sync"].get("_squash_t") or T0) > 86400:
                api().super_squash_history(STATE_REPO, repo_type="dataset", commit_message="squash")
                with lock:
                    state["sync"]["_squash_t"] = time.time()
                    state["sync"]["last_squash"] = time.strftime("%F %T")
                log("dataset history squashed")
        except Exception as e:
            with lock:
                state["sync"]["last_err"] = f"{time.strftime('%F %T')} {e!r}"[:300]
            log("sync error:", repr(e)[:300])


# ------------------------------------------------------------------ children
CHILDREN = {
    "polybot": [sys.executable, os.path.join(APP, "polybot.py")],
    "server": [sys.executable, os.path.join(APP, "web", "server.py")],
}


def start(name):
    p = subprocess.Popen(CHILDREN[name], cwd=APP, env=os.environ.copy(), stdin=subprocess.DEVNULL)
    procs[name] = p
    with lock:
        c = state["children"].setdefault(name, {"restarts": -1})
        c.update(pid=p.pid, since=time.strftime("%F %T"), restarts=c["restarts"] + 1)
    log("started", name, p.pid)


def trim_log():
    try:
        if os.path.getsize(BOTLOG) > 50_000_000:
            with open(BOTLOG, "rb") as f:
                f.seek(-5_000_000, 2)
                tail = f.read()
            with open(BOTLOG, "wb") as f:
                f.write(tail)
    except OSError:
        pass


def keepalive_loop():
    # Render free web services spin down after 15 min without INBOUND traffic; HF free Spaces after 48h.
    # Ping our own public URL (goes out through the platform edge, so it counts as inbound).
    # Render sets RENDER_EXTERNAL_URL; HF sets SPACE_HOST. External pinger (cron-job.org) is the backup.
    ext = os.environ.get("RENDER_EXTERNAL_URL", "").rstrip("/") or (f"https://{SPACE_HOST}" if SPACE_HOST else "")
    every = int(os.environ.get("KEEPALIVE_S", "600" if os.environ.get("RENDER") else "1200"))
    hdr = {"Authorization": f"Bearer {HF_TOKEN}"} if (SPACE_HOST and HF_TOKEN) else {}
    first = True
    while not stopping.wait(60 if first else every):
        first = False
        if not ext:
            continue
        try:
            r = requests.get(f"{ext}/health", headers=hdr, timeout=30)
            res = r.status_code
        except Exception as e:
            res = repr(e)[:120]
        with lock:
            state["keepalive"] = {"last": time.strftime("%F %T"), "status": res, "url": ext, "every_s": every}


def on_term(*_):
    if stopping.is_set():
        return
    stopping.set()
    log("SIGTERM: stopping children")
    for p in procs.values():
        try:
            p.terminate()
        except Exception:
            pass
    for p in procs.values():
        try:
            p.wait(10)
        except Exception:
            p.kill()
    if PRIMARY:
        try:
            log("final sync", upload_snapshot("shutdown"))
        except Exception as e:
            log("final sync failed", repr(e))
    os._exit(0)


def main():
    signal.signal(signal.SIGTERM, on_term)
    signal.signal(signal.SIGINT, on_term)
    threading.Thread(target=serve, daemon=True, name="http").start()
    log(f"mode={MODE} state_repo={STATE_REPO} space={SPACE_HOST or os.environ.get("RENDER_EXTERNAL_URL") or '-'} PAPER ONLY")
    choose_binance()
    with lock:
        state["phase"] = "restoring"
    restore()
    if os.environ.get("PUSH_SECRET") and PRIMARY:
        CHILDREN["web_push"] = [sys.executable, os.path.join(APP, "web_push.py")]
    for n in CHILDREN:
        start(n)
    with lock:
        state["phase"] = "running"
    if PRIMARY:
        threading.Thread(target=sync_loop, daemon=True, name="sync").start()
    threading.Thread(target=keepalive_loop, daemon=True, name="keepalive").start()
    while not stopping.wait(5):
        for n, p in list(procs.items()):
            if p.poll() is not None and not stopping.is_set():
                log(n, "exited", p.returncode, "-> restart in 5s")
                time.sleep(5)
                start(n)
        trim_log()


if __name__ == "__main__":
    main()
