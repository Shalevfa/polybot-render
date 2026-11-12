#!/usr/bin/env python3
"""Build the JSON payload for the BTC 5m demo live dashboard Page.

Prints a single JSON object to stdout (UTF-8). Also writes the same payload
to /workspace/polybot/dashboard_data.json for easier refresh.

Refresh the Page (every ~10 minutes):
  1. python3 /workspace/polybot/dashboard_data.py
     (prints JSON to stdout; also writes /workspace/polybot/dashboard_data.json)
  2. Update the existing Page in place with WriteCanvas or EditCanvas,
     using the same canvas_id 887095d9-9ca4-49ea-a8de-6690a9d84604
     (URL: https://cursor.com/canvas/store-90846792-3ec9-46dc-bfb5-50f82f50727e/887095d9-9ca4-49ea-a8de-6690a9d84604).
     Replace the embedded DATA object with this JSON. Do not create a new Page.
     Prefer EditCanvas when only DATA changed; use WriteCanvas with this
     canvas_id for a full rewrite of the same page.
"""

from __future__ import annotations

import json
import math
import os
import re
import sqlite3
import sys
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

ROOT = os.path.dirname(os.path.abspath(__file__))
STATUS_PATH = os.path.join(ROOT, "status.json")
DB_PATH = os.path.join(ROOT, "trades.db")
OUT_PATH = os.path.join(ROOT, "dashboard_data.json")
TZ = ZoneInfo("Asia/Jerusalem")
ZONE_LABEL = "שעון ישראל"
START_BANKROLL = 100.0
STATUS_STALE_AFTER_S = 60.0
STRATEGY_ORDER = [
    "late_model",
    "late_model_raw",
    "early_value",
    "naive_momentum",
    "every_round",
    "momentum_late",
    "latency_hunter",
]
GATE_REASON_HE = {
    "ok": "תקינה",
    "stale_chainlink": "Chainlink לא טרי",
    "sources_disagree": "המקורות לא מסכימים",
    "warmup": "חימום אחרי התחברות מחדש",
    "nowcast_fallback": "נתוני עזר בלבד",
    "no_model": "אין מודל",
    "stale_feed": "הזנה לא טרייה",
}


def now_local() -> datetime:
    return datetime.now(TZ)


def parse_bot_updated(text: str) -> datetime | None:
    if not text:
        return None
    raw = text.split("(")[0].strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            return datetime.strptime(raw, fmt).replace(tzinfo=TZ)
        except ValueError:
            continue
    return None


def fmt_clock(dt: datetime, seconds: bool = True) -> str:
    return dt.strftime("%H:%M:%S" if seconds else "%H:%M")


def fmt_when(dt: datetime, seconds: bool = True) -> str:
    months = {
        1: "בינואר",
        2: "בפברואר",
        3: "במרץ",
        4: "באפריל",
        5: "במאי",
        6: "ביוני",
        7: "ביולי",
        8: "באוגוסט",
        9: "בספטמבר",
        10: "באוקטובר",
        11: "בנובמבר",
        12: "בדצמבר",
    }
    clock = fmt_clock(dt, seconds=seconds)
    return f"{dt.day} {months[dt.month]} {dt.year}, {clock} ({ZONE_LABEL})"


def parse_naive(text: str | None) -> datetime | None:
    if not text:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            return datetime.strptime(text, fmt).replace(tzinfo=TZ)
        except ValueError:
            continue
    return None


def window_from_slug(slug: str | None) -> dict:
    if not slug:
        return {"slug": slug, "window": "—", "window_start": None, "window_end": None}
    m = re.search(r"(\d{9,})$", slug)
    if not m:
        return {"slug": slug, "window": slug, "window_start": None, "window_end": None}
    start = datetime.fromtimestamp(int(m.group(1)), tz=TZ)
    end = start + timedelta(seconds=300)
    return {
        "slug": slug,
        "window": f"{fmt_clock(start, seconds=False)}–{fmt_clock(end, seconds=False)}",
        "window_start": start.strftime("%Y-%m-%d %H:%M"),
        "window_end": end.strftime("%Y-%m-%d %H:%M"),
    }


def r2(n) -> float | None:
    if n is None:
        return None
    return round(float(n), 2)


def load_status() -> dict:
    with open(STATUS_PATH, encoding="utf-8") as f:
        return json.load(f)


def load_trades() -> list[dict]:
    con = sqlite3.connect(DB_PATH, timeout=10)
    con.row_factory = sqlite3.Row
    try:
        con.execute("PRAGMA busy_timeout=5000")
        rows = con.execute(
            """
            SELECT id, ts, time, slug, strategy, side, shares, avg_price, cost, fee,
                   model_p, edge, secs_left, outcome, pnl, settled, settled_time
            FROM trades
            ORDER BY COALESCE(ts, id), id
            """
        ).fetchall()
    finally:
        con.close()
    return [dict(r) for r in rows]


def equity_series(trades: list[dict], strategies: list[str]) -> dict:
    settled = [
        t
        for t in trades
        if int(t.get("settled") or 0) == 1 and t.get("settled_time") and t.get("pnl") is not None
    ]
    # Chronological unique settlement timestamps, plus a start point at 100.
    times: list[str] = []
    seen: set[str] = set()
    first_trade_time = None
    if trades:
        first_trade_time = trades[0].get("time")
    if first_trade_time:
        times.append(first_trade_time)
        seen.add(first_trade_time)
    for t in sorted(settled, key=lambda x: (x["settled_time"], x["id"])):
        st = t["settled_time"]
        if st not in seen:
            times.append(st)
            seen.add(st)

    running = {s: 0.0 for s in strategies}
    values = {s: [] for s in strategies}
    # Map time -> list of settlements at that time
    by_time: dict[str, list[dict]] = {t: [] for t in times}
    for t in settled:
        by_time.setdefault(t["settled_time"], []).append(t)

    for tm in times:
        for t in by_time.get(tm, []):
            s = t["strategy"]
            if s in running:
                running[s] += float(t["pnl"])
        for s in strategies:
            values[s].append(r2(START_BANKROLL + running[s]))

    # If a strategy has no points (no times at all), keep a flat 100.
    if not times:
        times = [now_local().strftime("%Y-%m-%d %H:%M:%S")]
        for s in strategies:
            values[s] = [r2(START_BANKROLL)]

    n = len(times)
    label_idx = {0, n - 1}
    if n > 2:
        for i in range(1, 5):
            label_idx.add(int(round(i * (n - 1) / 5)))
    labels = []
    for i, tm in enumerate(times):
        dt = parse_naive(tm)
        labels.append(fmt_clock(dt, seconds=False) if dt and i in label_idx else "")

    all_vals = [v for s in strategies for v in values[s]]
    lo = min(all_vals) if all_vals else START_BANKROLL
    hi = max(all_vals) if all_vals else START_BANKROLL
    pad = max(5.0, (hi - lo) * 0.12)
    floor = math.floor((lo - pad) / 10.0) * 10.0
    ceiling = math.ceil((hi + pad) / 10.0) * 10.0
    if floor >= START_BANKROLL:
        floor = START_BANKROLL - 10
    if ceiling <= START_BANKROLL:
        ceiling = START_BANKROLL + 10
    if floor == ceiling:
        floor -= 10
        ceiling += 10

    primary = strategies[0]
    others = strategies[1:]
    return {
        "points": [{"label": labels[i], "value": values[primary][i]} for i in range(n)],
        "lines": [{"label": s, "values": values[s]} for s in others],
        "secondary": {"label": "100$ התחלה", "values": [START_BANKROLL] * n},
        "floor": int(floor),
        "ceiling": int(ceiling),
        "primary": primary,
    }


def trade_view(t: dict) -> dict:
    dt = parse_naive(t.get("time"))
    settled = int(t.get("settled") or 0) == 1
    pnl = t.get("pnl")
    if not settled:
        result = "פתוחה"
        result_tone = "warning"
        pnl_tone = None
    elif pnl is not None and float(pnl) > 0:
        result = "ניצחון"
        result_tone = "success"
        pnl_tone = "success"
    else:
        result = "הפסד"
        result_tone = "danger"
        pnl_tone = "danger"
    win = window_from_slug(t.get("slug"))
    return {
        "time": fmt_clock(dt) if dt else (t.get("time") or "—"),
        "strategy": t.get("strategy"),
        "side": t.get("side"),
        "price": r2(t.get("avg_price")),
        "stake": r2(t.get("cost")),
        "pnl": r2(pnl) if pnl is not None else None,
        "result": result,
        "result_tone": result_tone,
        "pnl_tone": pnl_tone,
        "window": win["window"],
    }


def feed_state(status: dict, generated: datetime, bot_updated: datetime | None) -> dict:
    feed = status.get("feed") or {}
    health = feed.get("health") or {}
    gate = health.get("gate") or {}
    chainlink = health.get("chainlink") or {}
    age = feed.get("age_s")
    if age is None:
        age = chainlink.get("tick_age_s")
    status_age_s = None
    if bot_updated is not None:
        status_age_s = max(0.0, (generated - bot_updated).total_seconds())
    stale = status_age_s is not None and status_age_s > STATUS_STALE_AFTER_S
    gate_ok = bool(gate.get("ok"))
    reason = gate.get("reason") or ("stale" if stale else "unknown")
    reason_he = GATE_REASON_HE.get(reason, reason)
    warm_s = gate.get("warm_s")
    btc = feed.get("chainlink_btc")
    frozen = stale
    healthy = gate_ok and not stale

    if stale:
        mins = max(1, int(round((status_age_s or 0) / 60.0)))
        last = fmt_when(bot_updated) if bot_updated else "לא ידוע"
        state_text_he = (
            f"הזנת המחירים קפואה. קובץ הסטטוס לא עודכן כבר כ-{mins} דקות "
            f"(עדכון אחרון: {last}). הבוט לא יכול לסחור בזמן הזה."
        )
    elif not gate_ok:
        state_text_he = (
            f"הזנת המחירים פעילה, אבל הנתונים לא מספיק טריים או לא עקביים "
            f"(סיבה: {reason_he}). הבוט מדלג על כניסות עד שזה יתייצב."
        )
    elif warm_s is not None and float(warm_s) < 25:
        state_text_he = (
            "הזנת המחירים חזרה לפעול אחרי ניתוק, ועדיין בחימום. "
            "הבוט ממתין מעט לפני כניסה לעסקאות חדשות."
        )
    else:
        price_bit = ""
        if btc is not None:
            price_bit = f"מחיר ה-BTC ב-Chainlink הוא {float(btc):,.0f} דולר, ו"
        age_bit = f"עודכן לפני {float(age):.1f} שניות" if age is not None else "טרי"
        state_text_he = f"הזנת המחירים תקינה. {price_bit}הנתון {age_bit}."

    return {
        "healthy": healthy,
        "frozen": frozen,
        "state_text_he": state_text_he,
        "tone": "danger" if frozen else ("warning" if not healthy else "success"),
    }


def build() -> dict:
    generated = now_local()
    status = load_status()
    trades = load_trades()
    bot_updated = parse_bot_updated(status.get("updated") or "")
    strategies_raw = status.get("strategies") or {}
    start_bankroll = float(status.get("start_bankroll") or START_BANKROLL)

    names = list(STRATEGY_ORDER)
    for name in strategies_raw:
        if name not in names:
            names.append(name)

    strategies = []
    for name in names:
        if name not in strategies_raw:
            continue
        v = strategies_raw[name]
        wr = v.get("win_rate")
        strategies.append(
            {
                "name": name,
                "primary": name == status.get("primary"),
                "bankroll": r2(v.get("equity_if_open_at_cost", v.get("cash"))),
                "cash": r2(v.get("cash")),
                "open_exposure": r2(v.get("open_exposure") or 0),
                "pnl": r2(v.get("realized_pnl")),
                "trades": int(v.get("trades") or 0),
                "settled": int(v.get("settled") or 0),
                "wins": int(v.get("wins") or 0),
                "win_rate": None if wr is None else r2(float(wr) * 100.0),
                "open_trades": max(0, int(v.get("trades") or 0) - int(v.get("settled") or 0)),
            }
        )

    by_name = {s["name"]: s for s in strategies}
    primary = by_name.get("late_model") or (strategies[0] if strategies else None)
    every_round = by_name.get("every_round")

    open_rows = [t for t in trades if int(t.get("settled") or 0) == 0]
    open_rows.sort(key=lambda t: (t.get("ts") or 0, t.get("id") or 0), reverse=True)
    open_positions = [trade_view(t) for t in open_rows]

    views = [trade_view(t) for t in trades]
    recent_all = list(reversed(views))[:20]
    recent_by_strategy = {}
    for name in names:
        recent_by_strategy[name] = [t for t in reversed(views) if t["strategy"] == name][:20]

    feed = feed_state(status, generated, bot_updated)
    last_updated = fmt_when(bot_updated or generated)
    generated_s = fmt_when(generated)

    # Strategies already have the fields the page table needs; drop cash noise.
    strategies_out = [
        {
            "name": s["name"],
            "primary": s["primary"],
            "bankroll": s["bankroll"],
            "pnl": s["pnl"],
            "trades": s["trades"],
            "settled": s["settled"],
            "wins": s["wins"],
            "win_rate": s["win_rate"],
            "open_trades": s["open_trades"],
        }
        for s in strategies
    ]

    return {
        "last_updated_he": last_updated,
        "generated_at_he": generated_s,
        "zone": ZONE_LABEL,
        "mode_he": "מסחר דמו בלבד, בלי כסף אמיתי",
        "primary": status.get("primary") or "late_model",
        "start_bankroll": start_bankroll,
        "metrics": {
            "late_model_bankroll": primary["bankroll"] if primary else start_bankroll,
            "late_model_pnl": primary["pnl"] if primary else 0,
            "late_model_trades": primary["trades"] if primary else 0,
            "late_model_settled": primary["settled"] if primary else 0,
            "late_model_wins": primary["wins"] if primary else 0,
            "late_model_win_rate": primary["win_rate"] if primary else None,
            "every_round_bankroll": every_round["bankroll"] if every_round else start_bankroll,
            "every_round_pnl": every_round["pnl"] if every_round else 0,
            "every_round_trades": every_round["trades"] if every_round else 0,
            "every_round_open": (every_round["open_trades"] if every_round else 0),
        },
        "strategies": strategies_out,
        "open_positions": open_positions,
        "equity_chart": equity_series(trades, [s["name"] for s in strategies] or STRATEGY_ORDER),
        "recent_trades": recent_all,
        "recent_by_strategy": recent_by_strategy,
        "feed": feed,
    }


def main() -> int:
    data = build()
    text = json.dumps(data, ensure_ascii=False, indent=2)
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        f.write(text)
        f.write("\n")
    sys.stdout.write(text)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
