#!/usr/bin/env python3
"""Offline replay harness for paper PolyBot strategies.

Replays recorded 5-minute windows using only information available at each
snapshot time (no look-ahead). Fees and book-walk fills use the same
simulate_buy / fee model as live paper trading.

Historical snapshots (until richer recording landed) store only the best ask,
roughly 5 times per window, and no bids or depth. In that case the replay
builds a single-level book at the recorded ask (50 shares) so the fee is exact
and extra slippage beyond the touch is zero. That understates live walk-the-book
slippage; new snapshots record full top-of-book depth, bids, and a short price
path so later runs can use the live fill model faithfully.
"""
from __future__ import annotations

import json
import math
import sqlite3
from types import SimpleNamespace
from typing import Any

import bot_settings
from bot_settings import RANGES
from polybot import (
    BAND_HI,
    BAND_LO,
    EarlyValue,
    EveryRound,
    FEE_RATE,
    LateModel,
    LateModelRaw,
    LatencyHunterStrategy,
    MIN_SHARES,
    MomentumLate,
    NaiveMomentum,
    START_BANK,
    Strategy,
    WIN,
    pick,
    simulate_buy,
)

# Attribute map must stay aligned with Bot._ATTR_MAP in polybot.py.
ATTR_MAP = {
    "min_edge": "min_edge", "kelly": "kelly", "cap": "cap",
    "cash_frac_cap": "cash_frac_cap", "min_stake": "min_stake",
    "max_disagree": "max_disagree", "w": "w", "cross_edge": "cross_edge",
    "max_ask": "MAX_ASK", "min_ask": "MIN_ASK",
    "preferred_left_hi": "PREFERRED_LEFT_HI", "preferred_left_lo": "PREFERRED_LEFT_LO",
    "fallback_left_min": "FALLBACK_LEFT_MIN", "typical_stake": "TYPICAL_STAKE",
    "left_lo": "left_lo", "left_hi": "left_hi", "el_lo": "el_lo", "el_hi": "el_hi",
}

FACTORIES = {
    "late_model": LateModel,
    "late_model_raw": LateModelRaw,
    "early_value": EarlyValue,
    "naive_momentum": NaiveMomentum,
    "every_round": EveryRound,
    "momentum_late": MomentumLate,
    "latency_hunter": LatencyHunterStrategy,
}

# Strategies that only need model p + top-of-book asks (available historically).
SNAP_OK = ("late_model", "late_model_raw", "early_value", "naive_momentum", "every_round")
# These need a 60s+ price path that old snapshots do not contain.
NEEDS_PATH = ("momentum_late", "latency_hunter")

SYNTHETIC_ASK_SIZE = 50.0


def apply_genome(strat: Strategy, genome: dict[str, Any]) -> None:
    for key, attr in ATTR_MAP.items():
        if key in genome:
            setattr(strat, attr, genome[key])
    if strat.name == "latency_hunter" and hasattr(strat, "hunter"):
        from latency_hunter import HunterConfig
        kw = {k: genome[k] for k in bot_settings.HUNTER_CONFIG_KEYS if k in genome}
        base = HunterConfig()
        merged = {**{f: getattr(base, f) for f in base.__dataclass_fields__}, **kw}
        strat.hunter.config = HunterConfig(**{k: merged[k] for k in base.__dataclass_fields__})


def make_strategy(name: str, genome: dict[str, Any] | None = None) -> Strategy:
    if name not in FACTORIES:
        raise ValueError(f"cannot replay {name}")
    s = FACTORIES[name]()
    apply_genome(s, genome or bot_settings.defaults_for(name))
    return s


def _levels(snap: dict, key_levels: str, key_px: str, reverse: bool) -> list[tuple[float, float]]:
    raw = snap.get(key_levels)
    if raw:
        out = []
        for item in raw:
            try:
                p, sz = float(item[0]), float(item[1])
            except (TypeError, ValueError, IndexError):
                continue
            if 0 < p < 1 and sz > 0:
                out.append((p, sz))
        out.sort(reverse=reverse)
        return out
    px = snap.get(key_px)
    if px is None:
        return []
    try:
        p = float(px)
    except (TypeError, ValueError):
        return []
    if not (0 < p < 1):
        return []
    return [(p, SYNTHETIC_ASK_SIZE)]


def books_from_snap(snap: dict) -> dict:
    return {
        "Up": (_levels(snap, "up_asks", "up_ask", False),
               _levels(snap, "up_bids", "up_bid", True), None),
        "Down": (_levels(snap, "down_asks", "down_ask", False),
                 _levels(snap, "down_bids", "down_bid", True), None),
    }


def mp_from_snap(snap: dict) -> dict:
    return {
        "p": float(snap.get("p") or 0.5),
        "spot": float(snap.get("spot") or 0.0),
        "cl": float(snap.get("cl") or snap.get("spot") or 0.0),
        "src": snap.get("src") or "",
        "sd": snap.get("sd"),
        "sig": snap.get("sig"),
        "gate": None,
    }


def snap_has_depth(snap: dict) -> bool:
    for k in ("up_asks", "down_asks"):
        v = snap.get(k)
        if v and len(v) > 1:
            return True
    return False


def snap_has_bids(snap: dict) -> bool:
    return bool(snap.get("up_bids") or snap.get("down_bids") or snap.get("up_bid") or snap.get("down_bid"))


def snap_has_path(snap: dict) -> bool:
    px = snap.get("px") or []
    return sum(1 for v in px if v is not None) >= 50


def describe_data(windows: list[dict]) -> dict:
    n = len(windows)
    snaps = [s for w in windows for s in w.get("snaps") or []]
    n_snaps = len(snaps)
    with_depth = sum(1 for s in snaps if snap_has_depth(s))
    with_bids = sum(1 for s in snaps if snap_has_bids(s))
    with_path = sum(1 for s in snaps if snap_has_path(s))
    official = sum(1 for w in windows if w.get("official") in ("Up", "Down"))
    starts = [w["start"] for w in windows]
    return {
        "windows": n,
        "resolved": official,
        "snaps": n_snaps,
        "snaps_per_window": round(n_snaps / n, 2) if n else 0,
        "snaps_with_depth": with_depth,
        "snaps_with_bids": with_bids,
        "snaps_with_price_path": with_path,
        "start_min": min(starts) if starts else None,
        "start_max": max(starts) if starts else None,
        "book_history_thin": with_depth == 0 or with_bids == 0,
        "note": (
            "Historical snapshots store best ask only (~5 times per 5-minute window), "
            "with no bid or depth and no 1-second price path. Replay fills at the recorded "
            "ask with a 50-share single level (fees exact, slippage beyond the touch = 0). "
            "momentum_late and latency_hunter cannot be evolved fairly on this history. "
            "Richer per-round snapshots (top-of-book levels, bids, short price path) are "
            "now recorded going forward."
        ),
    }


def load_windows(db_path: str, require_official: bool = True) -> list[dict]:
    c = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    c.row_factory = sqlite3.Row
    rows = c.execute(
        "select slug, start, open_px, close_px, official, snaps from windows order by start"
    ).fetchall()
    c.close()
    out = []
    for r in rows:
        official = r["official"]
        if require_official and official not in ("Up", "Down"):
            continue
        try:
            snaps = json.loads(r["snaps"] or "[]")
        except Exception:
            snaps = []
        # Time order: earlier in the window first (larger `left` first).
        snaps = [s for s in snaps if isinstance(s, dict) and s.get("left") is not None]
        snaps.sort(key=lambda s: -float(s["left"]))
        out.append({
            "slug": r["slug"],
            "start": int(r["start"]),
            "end": int(r["start"]) + WIN,
            "open_px": r["open_px"],
            "close_px": r["close_px"],
            "official": official,
            "snaps": snaps,
        })
    return out


def stake_and_fill(name: str, s: Strategy, bank: float, side: str, prob: float, a: float, books: dict):
    """Same sizing and max-price walk as Bot.decide, without live gates or DB writes."""
    if name == "every_round":
        stake = s.cap if bank >= 100 else max(0.0, bank * 0.10)
    elif name == "naive_momentum":
        stake = min(s.cap, bank)
    else:
        q = Strategy.eff(a)
        if q <= 0 or q >= 1:
            return None, "bad_price"
        b = (1 - q) / q
        kf = max(0.0, (prob * b - (1 - prob)) / b)
        frac_cap = getattr(s, "cash_frac_cap", None)
        cap = (frac_cap * bank) if frac_cap is not None else s.cap
        stake = min(cap, s.kelly * kf * bank, bank)
        min_stake = getattr(s, "min_stake", 0.0)
        if min_stake and 0 < stake < min_stake:
            stake = min(min_stake, bank)
    if stake < 1.0:
        return None, "stake_small"
    if name == "every_round":
        maxp = min(0.95, a + 0.03)
        fill = simulate_buy(books[side][0], stake, maxp, min_shares=1.0)
    elif name == "momentum_late":
        maxp = min(getattr(s, "MAX_ASK", 0.95), prob - s.min_edge / 2, a + 0.03)
        fill = simulate_buy(books[side][0], stake, maxp)
    else:
        maxp = 0.86 if name == "naive_momentum" else min(0.97, prob - s.min_edge / 2, a + 0.03)
        fill = simulate_buy(books[side][0], stake, maxp)
    if not fill:
        return None, "no_fill"
    return fill, None


def _in_window(s: Strategy, el: float, left: float) -> bool:
    return bool(s.active(el, left))


def _price_band_ok(name: str, a: float, prob: float) -> bool:
    if name in ("every_round", "latency_hunter"):
        return True
    if a < BAND_LO or a > BAND_HI:
        # Historically we have no warm-for clock; require model confidence only.
        if not (prob >= 0.90):
            return False
    return True


def replay(name: str, genome: dict[str, Any] | None, windows: list[dict],
           start_bank: float = START_BANK) -> dict:
    """Replay one genome over a list of resolved windows. No look-ahead."""
    if name in NEEDS_PATH:
        usable = [w for w in windows if any(snap_has_path(s) for s in w.get("snaps") or [])]
        if not usable:
            return {
                "name": name, "ok": False, "reason": "insufficient_price_path",
                "pnl": 0.0, "trades": 0, "wins": 0, "max_dd": 0.0,
                "bank": start_bank, "fills": [], "equity": [start_bank],
            }
        windows = usable
    s = make_strategy(name, genome)
    bank = float(start_bank)
    peak = bank
    max_dd = 0.0
    fills = []
    equity = [bank]
    wins = 0
    for w in windows:
        official = w.get("official")
        if official not in ("Up", "Down"):
            continue
        open_px = w.get("open_px")
        if open_px is None:
            continue
        m = SimpleNamespace(slug=w["slug"], start=w["start"], end=w["end"], open_px=float(open_px))
        taken = None
        for snap in w.get("snaps") or []:
            left = float(snap["left"])
            el = WIN - left
            if not _in_window(s, el, left):
                continue
            books = books_from_snap(snap)
            mp = mp_from_snap(snap)
            try:
                ch, _why = s.side_choice(m, mp, books)
            except Exception:
                ch = None
            if not ch:
                continue
            side, prob, edge, a = ch
            if not _price_band_ok(name, a, prob):
                continue
            fill, err = stake_and_fill(name, s, bank, side, prob, a, books)
            if not fill:
                continue
            taken = {
                "slug": w["slug"], "start": w["start"], "left": left,
                "side": side, "prob": prob, "edge": edge, "ask": a,
                "shares": fill["shares"], "avg": fill["avg"],
                "cost": fill["cost"], "fee": fill["fee"],
                "p": mp["p"], "spot": mp["spot"],
            }
            break  # one entry per window, like live
        if taken is None:
            equity.append(bank)
            continue
        payout = taken["shares"] if taken["side"] == official else 0.0
        pnl = payout - taken["cost"] - taken["fee"]
        taken["outcome"] = official
        taken["payout"] = payout
        taken["pnl"] = round(pnl, 5)
        bank = round(bank + pnl, 5)
        peak = max(peak, bank)
        max_dd = max(max_dd, peak - bank)
        if pnl > 0:
            wins += 1
        fills.append(taken)
        equity.append(bank)
        if hasattr(s, "on_fill") and callable(s.on_fill):
            # decision time = window end - left, never the official close time
            s.on_fill(w["end"] - taken["left"])
    n = len(fills)
    return {
        "name": name, "ok": True,
        "pnl": round(bank - start_bank, 4),
        "trades": n,
        "wins": wins,
        "win_rate": round(wins / n, 3) if n else None,
        "max_dd": round(max_dd, 4),
        "bank": round(bank, 4),
        "fills": fills,
        "equity": equity,
        "start_bank": start_bank,
    }


def fitness(result: dict, min_trades: int = 8, dd_w: float = 0.35, thin_w: float = 4.0) -> float:
    if not result.get("ok"):
        return -1e6
    pnl = float(result["pnl"])
    dd = float(result["max_dd"])
    n = int(result["trades"])
    thin = max(0, min_trades - n)
    return pnl - dd_w * dd - thin_w * thin


def walk_forward_splits(windows: list[dict], train_frac: float = 0.70) -> list[tuple[str, list, list]]:
    """Time-respecting splits. Train always precedes test."""
    n = len(windows)
    if n < 20:
        cut = max(8, int(n * train_frac))
        return [("70/30", windows[:cut], windows[cut:])]
    a = max(16, int(n * 0.50))
    b = max(a + 8, int(n * 0.75))
    cut70 = max(16, int(n * train_frac))
    return [
        ("70/30", windows[:cut70], windows[cut70:]),
        ("expanding-50/75", windows[:a], windows[a:b]),
        ("expanding-75/100", windows[:b], windows[b:]),
    ]


def gene_space(name: str) -> dict[str, tuple[float, float]]:
    d = bot_settings.defaults_for(name)
    space = {}
    skip = set(bot_settings.BOOLS) | {"stop_loss", "take_profit"}
    for k, v in d.items():
        if k in skip:
            continue
        lo, hi = RANGES.get(k, (None, None))
        if lo is None or hi is None:
            continue
        if name == "late_model_raw" and k == "max_disagree":
            hi = bot_settings.RAW_DISAGREE_MAX
        space[k] = (float(lo), float(hi))
    return space
