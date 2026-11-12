#!/bin/bash
# Rebuild app/ from the box sources (/workspace/polybot) and apply Space-only patches.
# The box files are never modified.
set -euo pipefail
cd "$(dirname "$0")"
SRC=/workspace/polybot
rm -rf app && mkdir -p app/web
cp "$SRC/polybot.py" "$SRC/dashboard_data.py" "$SRC/web_push.py" "$SRC/latency_hunter.py" "$SRC/bot_settings.py" app/
cp "$SRC/web/server.py" "$SRC/web/index.html" app/web/
[ -f "$SRC/web/bot.html" ] && cp "$SRC/web/bot.html" app/web/
[ -f "$SRC/web/classic.html" ] && cp "$SRC/web/classic.html" app/web/
[ -d "$SRC/web/fonts" ] && cp -r "$SRC/web/fonts" app/web/
python3 - <<'PY'
import re
p = "app/polybot.py"; s = open(p).read()
s = s.replace('RTDS = "wss://ws-live-data.polymarket.com"',
  'RTDS = "wss://ws-live-data.polymarket.com"\n'
  '# Space patch: Binance endpoints are env-configurable (binance.com geo-blocks US IPs -> binance.us)\n'
  'BINANCE_REST = os.environ.get("BINANCE_REST", "https://api.binance.com")\n'
  'BINANCE_WS = os.environ.get("BINANCE_WS", "wss://stream.binance.com:443")', 1)
n0 = s.count("api.binance.com") + s.count("stream.binance.com")
s = s.replace('"wss://stream.binance.com:443/ws/btcusdt@bookTicker"', 'BINANCE_WS + "/ws/btcusdt@bookTicker"')
s = s.replace('"https://api.binance.com/api/v3/ticker/bookTicker?symbol=BTCUSDT"', 'BINANCE_REST + "/api/v3/ticker/bookTicker?symbol=BTCUSDT"')
s = s.replace('"https://api.binance.com/api/v3/klines"', 'BINANCE_REST + "/api/v3/klines"')
left = [l for l in s.splitlines() if ("api.binance.com" in l or "stream.binance.com" in l) and "os.environ.get" not in l]
assert not left, left
open(p, "w").write(s)

p = "app/web/server.py"; s = open(p).read()
assert "PORT = 8765" in s
s = s.replace("PORT = 8765", 'PORT = int(os.environ.get("POLYBOT_DASH_PORT", "8765"))', 1)
open(p, "w").write(s)

p = "app/web_push.py"; s = open(p).read()
s = s.replace('LOCAL = "http://127.0.0.1:8765/api/live"',
  'LOCAL = "http://127.0.0.1:%s/api/live" % os.environ.get("POLYBOT_DASH_PORT", "8765")', 1)
old = '''    with open(TOKEN_PATH, "r", encoding="utf-8") as f:
        token = f.read().strip()'''
assert old in s
s = s.replace(old, '''    token = os.environ.get("PUSH_SECRET", "").strip()  # Space secret
    if not token:
        with open(TOKEN_PATH, "r", encoding="utf-8") as f:
            token = f.read().strip()''', 1)
old = "            if fails >= 3 and isinstance(e, requests.ConnectionError) and not local_server_alive():"
assert old in s
s = s.replace(old, "            if fails >= 3 and not os.environ.get(\"POLYBOT_SUPERVISED\") and isinstance(e, requests.ConnectionError) and not local_server_alive():", 1)
# write a heartbeat file the supervisor's /health can read
old = "            fails = 0\n            oks += 1"
assert old in s
s = s.replace(old, "            fails = 0\n            oks += 1\n            try:\n                open(os.path.join(ROOT, \"web_push.ok\"), \"w\").write(str(time.time()))\n            except OSError:\n                pass", 1)
open(p, "w").write(s)
PY
find app -name __pycache__ -prune -exec rm -rf {} +
python3 -m py_compile app/polybot.py app/web/server.py app/web_push.py app/dashboard_data.py app/latency_hunter.py app/bot_settings.py supervisor.py
echo "app/ prepared from $SRC"
find app -name __pycache__ -prune -exec rm -rf {} +
