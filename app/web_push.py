#!/usr/bin/env python3
"""Push the live dashboard payload to the public Vercel site (polybot-btc5m).

Read-only w.r.t. the bot: it fetches the local dashboard API
(http://127.0.0.1:8765/api/live, served by web/server.py), trims it,
gzips it and POSTs it to https://<site>/api/push with a bearer secret
stored in .vercel_push_token (chmod 600). Never touches trading logic.
"""
from __future__ import annotations

import gzip
import json
import os
import sys
import time
import traceback

import subprocess

import requests

ROOT = os.path.dirname(os.path.abspath(__file__))
TOKEN_PATH = os.path.join(ROOT, ".vercel_push_token")
LOCAL = "http://127.0.0.1:%s/api/live" % os.environ.get("POLYBOT_DASH_PORT", "8765")
SITE = os.environ.get("POLYBOT_SITE", "https://polybot-btc5m.vercel.app")
INTERVAL = float(os.environ.get("POLYBOT_PUSH_INTERVAL", "2"))
MAX_SERIES = 120


def trim(d: dict) -> dict:
    d.pop("html_version", None)
    s = d.get("series") or []
    if len(s) > MAX_SERIES:
        step = len(s) / MAX_SERIES
        keep = [s[int(i * step)] for i in range(MAX_SERIES - 1)] + [s[-1]]
        s = keep
    d["series"] = [
        {"t": round(p["t"], 1), "btc": round(p["btc"], 2), "nowcast": round(p.get("nowcast") or p["btc"], 2)}
        for p in s
    ]
    for st in d.get("strategies") or []:
        st.pop("doc", None)
    d["pushed_at"] = time.time()
    return d


def local_server_alive() -> bool:
    try:
        with open(os.path.join(ROOT, "web", "server.pid")) as f:
            os.kill(int(f.read().strip()), 0)
        return True
    except (OSError, ValueError):
        return False


def main():
    token = os.environ.get("PUSH_SECRET", "").strip()  # Space secret
    if not token:
        with open(TOKEN_PATH, "r", encoding="utf-8") as f:
            token = f.read().strip()
    sess = requests.Session()
    sess.headers["User-Agent"] = "polybot-web-push/1.0"
    fails = 0
    oks = 0
    last_loop = time.time()
    while True:
        t0 = time.time()
        gap = t0 - last_loop
        last_loop = t0
        if gap > INTERVAL + 30:
            # box/VM was suspended: pooled TLS connections are dead -> fresh session
            print(time.strftime("%F %T"), f"box freeze detected (~{gap:.0f}s gap), resetting HTTP session", flush=True)
            sess.close()
            sess = requests.Session()
            sess.headers["User-Agent"] = "polybot-web-push/1.0"
        try:
            r = sess.get(LOCAL, timeout=8)
            r.raise_for_status()
            d = trim(r.json())
            body = gzip.compress(json.dumps(d, ensure_ascii=False, separators=(",", ":")).encode("utf-8"), 9)
            pr = sess.post(
                SITE + "/api/push",
                data=body,
                headers={"Authorization": "Bearer " + token, "Content-Type": "application/octet-stream"},
                timeout=(5, 10),
            )
            if pr.status_code != 200:
                raise RuntimeError(f"push http {pr.status_code}: {pr.text[:120]}")
            if fails:
                print(time.strftime("%F %T"), "recovered after", fails, "fails", flush=True)
            fails = 0
            oks += 1
            try:
                open(os.path.join(ROOT, "web_push.ok"), "w").write(str(time.time()))
            except OSError:
                pass
            if oks % 600 == 1:  # heartbeat roughly every hour
                print(time.strftime("%F %T"), "push ok", pr.status_code, pr.text[:80], flush=True)
        except Exception as e:  # keep looping forever
            fails += 1
            if fails >= 2:  # drop possibly-dead pooled connections
                sess.close()
                sess = requests.Session()
                sess.headers["User-Agent"] = "polybot-web-push/1.0"
            if fails >= 3 and not os.environ.get("POLYBOT_SUPERVISED") and isinstance(e, requests.ConnectionError) and not local_server_alive():
                # local dashboard server died: (re)start it; never touches polybot.py
                subprocess.run([os.path.join(ROOT, "web", "start.sh")], timeout=20,
                               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            if fails <= 3 or fails % 30 == 0:
                print(time.strftime("%F %T"), "push failed:", repr(e)[:300], flush=True)
        time.sleep(max(1.0, INTERVAL - (time.time() - t0)))


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--size":
        d = trim(requests.get(LOCAL, timeout=8).json())
        raw = json.dumps(d, ensure_ascii=False, separators=(",", ":")).encode()
        gz = gzip.compress(raw, 9)
        print("raw", len(raw), "gz", len(gz), "b64", len(gz) * 4 // 3)
        sys.exit(0)
    main()
