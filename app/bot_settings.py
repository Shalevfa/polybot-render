"""Per-strategy settings, validation, and persistence (paper trading only).

Defaults match the current hardcoded strategy parameters so behavior is
unchanged until a human edits and saves. Settings live in trades.db
(table bot_settings) so they ride along with the HF state snapshot.
"""
from __future__ import annotations

import json
import os
import sqlite3
import time
from typing import Any

# Live (non-archived) trades only.
LIVE = "coalesce(archived,0)=0"

# Ranges for server-side validation.
RANGES = {
    "min_edge": (0.0, 0.50),
    "kelly": (0.0, 1.0),
    "cap": (1.0, 100.0),
    "cash_frac_cap": (0.01, 1.0),
    "min_stake": (1.0, 50.0),
    "stop_loss": (0.0, 0.90),
    "take_profit": (0.0, 0.90),
    "max_disagree": (0.0, 1.0),
    "w": (0.0, 1.0),
    "left_lo": (0.0, 300.0),
    "left_hi": (1.0, 300.0),
    "el_lo": (0.0, 300.0),
    "el_hi": (1.0, 300.0),
    "max_ask": (0.50, 0.99),
    "min_ask": (0.01, 0.50),
    "cross_edge": (0.0, 0.50),
    "preferred_left_hi": (20.0, 300.0),
    "preferred_left_lo": (5.0, 200.0),
    "fallback_left_min": (1.0, 60.0),
    "min_net_edge": (0.0, 0.50),
    "min_confidence": (0.0, 1.0),
    "max_token_spread": (0.0, 0.20),
    "min_seconds_remaining": (0.0, 200.0),
    "cooldown_seconds": (0.0, 300.0),
    "typical_stake": (1.0, 50.0),
    "pop_size": (4.0, 30.0),
    "evolve_every": (4.0, 100.0),
    "mutation_rate": (0.05, 0.80),
}

BOOLS = ("enabled", "auto_trading", "risk_management", "alerts")

# Current windows / params — must equal today's live values.
STRATEGY_DEFAULTS = {
    "late_model": {
        "enabled": True, "auto_trading": True, "risk_management": False, "alerts": False,
        "min_edge": 0.04, "max_disagree": 0.30, "w": 0.6, "kelly": 0.50,
        "cash_frac_cap": 0.12, "min_stake": 3.0, "cap": 10.0,
        "left_lo": 5.0, "left_hi": 75.0, "stop_loss": 0.0, "take_profit": 0.0,
    },
    "late_model_raw": {
        "enabled": True, "auto_trading": True, "risk_management": False, "alerts": False,
        "min_edge": 0.05, "max_disagree": 9.0,  # effectively unused (shadow)
        "w": 1.0, "kelly": 0.50, "cash_frac_cap": 0.12, "min_stake": 3.0, "cap": 10.0,
        "left_lo": 5.0, "left_hi": 75.0, "stop_loss": 0.0, "take_profit": 0.0,
    },
    "early_value": {
        "enabled": True, "auto_trading": True, "risk_management": False, "alerts": False,
        "min_edge": 0.06, "kelly": 0.4, "cap": 6.0, "cash_frac_cap": 0.08,
        "max_disagree": 0.25, "w": 0.5, "min_stake": 3.0,
        "el_lo": 60.0, "el_hi": 210.0, "stop_loss": 0.0, "take_profit": 0.0,
    },
    "naive_momentum": {
        "enabled": True, "auto_trading": True, "risk_management": False, "alerts": False,
        "cap": 5.0, "kelly": 0.25, "min_edge": 0.04, "min_stake": 1.0,
        "left_lo": 50.0, "left_hi": 65.0, "max_ask": 0.85,
        "stop_loss": 0.0, "take_profit": 0.0,
    },
    "every_round": {
        "enabled": True, "auto_trading": True, "risk_management": False, "alerts": False,
        "cap": 10.0, "min_edge": 0.0, "kelly": 0.0, "min_stake": 1.0,
        "preferred_left_hi": 150.0, "preferred_left_lo": 20.0, "fallback_left_min": 5.0,
        "max_ask": 0.95, "left_lo": 5.0, "left_hi": 150.0,
        "stop_loss": 0.0, "take_profit": 0.0,
    },
    "momentum_late": {
        "enabled": True, "auto_trading": True, "risk_management": False, "alerts": False,
        "min_edge": 0.04, "cross_edge": 0.06, "kelly": 0.50, "cash_frac_cap": 0.12,
        "min_stake": 3.0, "max_disagree": 0.30, "max_ask": 0.95, "min_ask": 0.03,
        "left_lo": 10.0, "left_hi": 90.0, "stop_loss": 0.0, "take_profit": 0.0,
    },
    "latency_hunter": {
        "enabled": True, "auto_trading": True, "risk_management": False, "alerts": False,
        "min_edge": 0.06, "kelly": 0.50, "cash_frac_cap": 0.08, "min_stake": 3.0,
        "typical_stake": 8.0, "left_lo": 20.0, "left_hi": 300.0,
        "min_net_edge": 0.06, "min_confidence": 0.60, "max_token_spread": 0.04,
        "min_seconds_remaining": 20.0, "cooldown_seconds": 20.0,
        "stop_loss": 0.0, "take_profit": 0.0,
    },
    "manual": {
        "enabled": True, "auto_trading": False, "risk_management": False, "alerts": False,
        "min_edge": 0.0, "kelly": 0.0, "cap": 20.0, "cash_frac_cap": 1.0, "min_stake": 1.0,
        "max_ask": 0.99, "stop_loss": 0.0, "take_profit": 0.0,
    },
    "genetic": {
        "enabled": True, "auto_trading": True, "risk_management": False, "alerts": False,
        "min_edge": 0.04, "max_disagree": 0.30, "w": 0.6, "kelly": 0.50,
        "cash_frac_cap": 0.12, "min_stake": 3.0, "cap": 10.0,
        "left_lo": 5.0, "left_hi": 75.0, "min_ask": 0.03, "max_ask": 0.97,
        "pop_size": 10.0, "evolve_every": 12.0, "mutation_rate": 0.20,
        "stop_loss": 0.0, "take_profit": 0.0,
    },
}

# late_model_raw.max_disagree is 9 (sentinel). Allow a wider range just for that field when name matches.
RAW_DISAGREE_MAX = 10.0

KNOWN = tuple(STRATEGY_DEFAULTS.keys())

FIELD_LABELS_HE = {
    "enabled": "הפעלה ראשית",
    "auto_trading": "מסחר אוטומטי",
    "risk_management": "ניהול סיכונים (סטופ / טייק)",
    "alerts": "התראות ביומן",
    "min_edge": "סף יתרון מינימלי (כניסה)",
    "kelly": "מקדם קלי",
    "cap": "תקרת סכום ($)",
    "cash_frac_cap": "תקרת חלק מהקופה",
    "min_stake": "סכום מינימלי ($)",
    "stop_loss": "סטופ לוס (0 = כבוי, חלק ממחיר הכניסה)",
    "take_profit": "טייק פרופיט (0 = כבוי, חלק ממחיר הכניסה)",
    "max_disagree": "אי-הסכמה מקסימלית מול הספר",
    "w": "משקל המודל (מול הספר)",
    "left_lo": "שניות מינימום לסיום (חלון כניסה)",
    "left_hi": "שניות מקסימום לסיום (חלון כניסה)",
    "el_lo": "שניות מינימום מתחילת הסיבוב",
    "el_hi": "שניות מקסימום מתחילת הסיבוב",
    "max_ask": "מחיר היצע מקסימלי",
    "min_ask": "מחיר היצע מינימלי",
    "cross_edge": "יתרון נדרש כשחוצים את הפתיחה",
    "preferred_left_hi": "העדפת כניסה: מקסימום שניות לסיום",
    "preferred_left_lo": "העדפת כניסה: מינימום שניות לסיום",
    "fallback_left_min": "כניסת גיבוי: מינימום שניות לסיום",
    "min_net_edge": "יתרון נטו מינימלי",
    "min_confidence": "ביטחון מינימלי",
    "max_token_spread": "מרווח מקסימלי בספר",
    "min_seconds_remaining": "מינימום שניות לסיום",
    "cooldown_seconds": "המתנה בין עסקאות (שניות)",
    "typical_stake": "סכום טיפוסי להערכת החלקה ($)",
    "pop_size": "גודל האוכלוסייה",
    "evolve_every": "אבולוציה כל כמה סיבובים",
    "mutation_rate": "שיעור מוטציה",
}

HUNTER_CONFIG_KEYS = (
    "min_net_edge", "min_confidence", "max_token_spread",
    "min_seconds_remaining", "cooldown_seconds",
)


def defaults_for(name: str) -> dict[str, Any]:
    if name not in STRATEGY_DEFAULTS:
        raise ValueError(f"unknown strategy: {name}")
    return dict(STRATEGY_DEFAULTS[name])


def schema_for(name: str) -> list[dict[str, Any]]:
    d = defaults_for(name)
    out = []
    for key, val in d.items():
        item = {"key": key, "default": val, "label_he": FIELD_LABELS_HE.get(key, key)}
        if key in BOOLS:
            item["type"] = "bool"
        else:
            item["type"] = "number"
            lo, hi = RANGES.get(key, (None, None))
            if name == "late_model_raw" and key == "max_disagree":
                hi = RAW_DISAGREE_MAX
            item["min"] = lo
            item["max"] = hi
        out.append(item)
    return out


def run_mode(cfg: dict[str, Any], name: str | None = None) -> str:
    if not cfg.get("enabled", True):
        return "off"
    if name == "manual":
        return "active" if cfg.get("enabled", True) else "off"
    if not cfg.get("auto_trading", True):
        return "paused"
    return "active"


def validate(name: str, payload: dict[str, Any], base: dict[str, Any] | None = None) -> dict[str, Any]:
    if name not in STRATEGY_DEFAULTS:
        raise ValueError(f"unknown strategy: {name}")
    if not isinstance(payload, dict):
        raise ValueError("payload must be an object")
    allowed = set(STRATEGY_DEFAULTS[name])
    unknown = [k for k in payload if k not in allowed]
    if unknown:
        raise ValueError("unknown fields: " + ", ".join(unknown[:8]))
    cur = defaults_for(name)
    if base:
        for k, v in base.items():
            if k in allowed:
                cur[k] = v
    for k, v in payload.items():
        cur[k] = v
    clean: dict[str, Any] = {}
    for k, default in defaults_for(name).items():
        v = cur.get(k, default)
        if k in BOOLS:
            if not isinstance(v, bool):
                if v in (0, 1, "0", "1", "true", "false", "True", "False"):
                    v = v in (True, 1, "1", "true", "True")
                else:
                    raise ValueError(f"{k} must be a boolean")
            clean[k] = bool(v)
            continue
        try:
            v = float(v)
        except (TypeError, ValueError):
            raise ValueError(f"{k} must be a number") from None
        if not (v == v) or v in (float("inf"), float("-inf")):
            raise ValueError(f"{k} must be finite")
        lo, hi = RANGES.get(k, (None, None))
        if name == "late_model_raw" and k == "max_disagree":
            hi = RAW_DISAGREE_MAX
        if lo is not None and v < lo:
            raise ValueError(f"{k} must be >= {lo}")
        if hi is not None and v > hi:
            raise ValueError(f"{k} must be <= {hi}")
        # keep ints looking like ints for windows, but store floats consistently
        clean[k] = v
    if "left_lo" in clean and "left_hi" in clean and clean["left_lo"] > clean["left_hi"]:
        raise ValueError("left_lo must be <= left_hi")
    if "el_lo" in clean and "el_hi" in clean and clean["el_lo"] > clean["el_hi"]:
        raise ValueError("el_lo must be <= el_hi")
    if "preferred_left_lo" in clean and "preferred_left_hi" in clean and clean["preferred_left_lo"] > clean["preferred_left_hi"]:
        raise ValueError("preferred_left_lo must be <= preferred_left_hi")
    return clean


def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """create table if not exists bot_settings(
            strategy text primary key,
            payload text not null,
            updated_ts real not null,
            updated_at text not null
        )"""
    )
    conn.execute(
        """create table if not exists bot_commands(
            id integer primary key,
            ts real not null,
            kind text not null,
            strategy text,
            payload text,
            status text not null default 'pending',
            result text,
            done_ts real
        )"""
    )
    conn.execute(
        """create table if not exists trades_archive(
            archive_id integer primary key,
            archived_at text not null,
            reason text,
            strategy text,
            id integer, ts real, time text, slug text, side text,
            shares real, avg_price real, cost real, fee real, model_p real, edge real,
            spot real, open_px real, secs_left real, outcome text, payout real, pnl real,
            settled integer, settled_time text
        )"""
    )
    cols = {r[1] for r in conn.execute("pragma table_info(trades)").fetchall()}
    if cols and "archived" not in cols:
        conn.execute("alter table trades add column archived integer not null default 0")
    if cols and "archived_at" not in cols:
        conn.execute("alter table trades add column archived_at text")
    if cols and "archive_reason" not in cols:
        conn.execute("alter table trades add column archive_reason text")
    if cols and "exit_reason" not in cols:
        conn.execute("alter table trades add column exit_reason text")
    conn.commit()


def load_all(conn: sqlite3.Connection) -> dict[str, dict[str, Any]]:
    ensure_schema(conn)
    out = {name: defaults_for(name) for name in KNOWN}
    rows = conn.execute("select strategy, payload from bot_settings").fetchall()
    for name, raw in rows:
        if name not in out:
            continue
        try:
            saved = json.loads(raw)
            out[name] = validate(name, saved, base=defaults_for(name))
        except Exception:
            pass
    return out


def save_one(conn: sqlite3.Connection, name: str, cfg: dict[str, Any]) -> dict[str, Any]:
    clean = validate(name, cfg, base=defaults_for(name))
    now = time.time()
    stamp = time.strftime("%Y-%m-%d %H:%M:%S")
    conn.execute(
        """insert into bot_settings(strategy, payload, updated_ts, updated_at)
           values(?,?,?,?)
           on conflict(strategy) do update set payload=excluded.payload,
             updated_ts=excluded.updated_ts, updated_at=excluded.updated_at""",
        (name, json.dumps(clean, sort_keys=True), now, stamp),
    )
    conn.commit()
    return clean


def enqueue(conn: sqlite3.Connection, kind: str, strategy: str | None, payload: dict[str, Any] | None = None) -> int:
    ensure_schema(conn)
    cur = conn.execute(
        "insert into bot_commands(ts, kind, strategy, payload, status) values(?,?,?,?, 'pending')",
        (time.time(), kind, strategy, json.dumps(payload or {})),
    )
    conn.commit()
    return int(cur.lastrowid)


def pending_commands(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    ensure_schema(conn)
    rows = conn.execute(
        "select id, kind, strategy, payload from bot_commands where status='pending' order by id"
    ).fetchall()
    out = []
    for i, kind, strat, raw in rows:
        try:
            payload = json.loads(raw) if raw else {}
        except Exception:
            payload = {}
        out.append({"id": i, "kind": kind, "strategy": strat, "payload": payload})
    return out


def finish_command(conn: sqlite3.Connection, cmd_id: int, status: str, result: dict[str, Any] | str) -> None:
    if not isinstance(result, str):
        result = json.dumps(result, ensure_ascii=False)
    conn.execute(
        "update bot_commands set status=?, result=?, done_ts=? where id=?",
        (status, result[:2000], time.time(), cmd_id),
    )
    conn.commit()


def settings_rev(conn: sqlite3.Connection) -> float:
    try:
        row = conn.execute("select coalesce(max(updated_ts),0) from bot_settings").fetchone()
        return float(row[0] or 0)
    except Exception:
        return 0.0


def want_sync_path(root: str) -> str:
    return os.path.join(root, ".want_sync")


def request_sync(root: str) -> None:
    try:
        with open(want_sync_path(root), "w") as f:
            f.write(str(time.time()))
    except OSError:
        pass


def public_view(all_cfg: dict[str, dict[str, Any]]) -> dict[str, Any]:
    """Public, non-secret snapshot for the dashboard."""
    out = {}
    for name in KNOWN:
        cfg = all_cfg.get(name) or defaults_for(name)
        out[name] = {
            "settings": cfg,
            "defaults": defaults_for(name),
            "schema": schema_for(name),
            "run_mode": run_mode(cfg, name),
        }
    return out
