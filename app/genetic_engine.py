#!/usr/bin/env python3
"""Live paper genetic bot: a small population of genomes, shadow P&L, periodic evolution.

PAPER ONLY. The champion genome may open paper trades on this bot's own $100 bankroll.
It never writes parameters onto the other live strategies.
"""
from __future__ import annotations

import json
import os
import random
import time
from types import SimpleNamespace
from typing import Any
from uuid import uuid4

import bot_settings
from bot_settings import RANGES

# Lazy import of polybot to avoid a circular import (polybot loads this module).
def _pb():
    import polybot as pb
    return pb

GENE_KEYS = (
    "min_edge", "max_disagree", "w", "kelly", "cap", "cash_frac_cap",
    "min_stake", "left_lo", "left_hi", "min_ask", "max_ask",
)

STATE_VERSION = 1


def default_genome() -> dict[str, float]:
    d = bot_settings.defaults_for("genetic")
    return {k: float(d[k]) for k in GENE_KEYS}


def clamp_genome(g: dict[str, Any]) -> dict[str, float]:
    out = {}
    for k in GENE_KEYS:
        lo, hi = RANGES[k]
        try:
            v = float(g.get(k, default_genome()[k]))
        except (TypeError, ValueError):
            v = default_genome()[k]
        if v != v or v in (float("inf"), float("-inf")):
            v = default_genome()[k]
        out[k] = min(hi, max(lo, v))
    if out["left_lo"] > out["left_hi"]:
        out["left_lo"], out["left_hi"] = out["left_hi"], out["left_lo"]
    if out["min_ask"] > out["max_ask"]:
        out["min_ask"], out["max_ask"] = out["max_ask"], out["min_ask"]
    return out


def genetic_pick(genome: dict[str, float], mp: dict, books: dict, left: float):
    if not (genome["left_lo"] <= left <= genome["left_hi"]):
        return None, "out of window"
    pb = _pb()
    s = SimpleNamespace(min_edge=genome["min_edge"], max_disagree=genome["max_disagree"],
                        w=genome["w"], name="genetic")
    return pb.pick(s, mp, books, genome["w"], genome["min_ask"], genome["max_ask"])


def shadow_stake_and_fill(genome: dict[str, float], bank: float, side: str, prob: float,
                          a: float, books: dict):
    pb = _pb()
    q = pb.Strategy.eff(a)
    if q <= 0 or q >= 1:
        return None
    b = (1 - q) / q
    kf = max(0.0, (prob * b - (1 - prob)) / b)
    cap = min(genome["cap"], genome["cash_frac_cap"] * bank)
    stake = min(cap, genome["kelly"] * kf * bank, bank)
    if 0 < stake < genome["min_stake"]:
        stake = min(genome["min_stake"], bank)
    if stake < 1.0:
        return None
    maxp = min(0.97, prob - genome["min_edge"] / 2, a + 0.03)
    return pb.simulate_buy(books[side][0], stake, maxp)


def _fitness(ind: dict, min_trades: int = 4) -> float:
    pnl = float(ind.get("pnl") or 0.0)
    dd = float(ind.get("max_dd") or 0.0)
    n = int(ind.get("trades") or 0)
    return pnl - 0.35 * dd - 4.0 * max(0, min_trades - n)


class GeneticEngine:
    def __init__(self, path: str):
        self.path = path
        self.generation = 0
        self.rounds_since_evolve = 0
        self.settled_rounds = 0
        self.pop: list[dict] = []
        self.champion_id: str | None = None
        self.last_decision = "WAIT"
        self.last_reason = "starting"
        self.last_slug: str | None = None
        self.cfg = {
            "pop_size": 10,
            "evolve_every": 12,
            "mutation_rate": 0.20,
        }
        self.enabled = True
        self.auto_trading = True
        self.rng = random.Random()
        self._dirty = False

    def configure(self, settings: dict[str, Any] | None):
        s = settings or {}
        self.enabled = bool(s.get("enabled", True))
        self.auto_trading = bool(s.get("auto_trading", True))
        try:
            self.cfg["pop_size"] = int(max(4, min(30, float(s.get("pop_size", 10)))))
        except (TypeError, ValueError):
            pass
        try:
            self.cfg["evolve_every"] = int(max(4, min(100, float(s.get("evolve_every", 12)))))
        except (TypeError, ValueError):
            pass
        try:
            self.cfg["mutation_rate"] = float(max(0.05, min(0.8, float(s.get("mutation_rate", 0.20)))))
        except (TypeError, ValueError):
            pass
        self._ensure_pop()

    def _new_ind(self, genome: dict | None = None, jitter: bool = False) -> dict:
        g = clamp_genome(genome or default_genome())
        if jitter:
            g = self._mutate(g, p=max(0.5, self.cfg["mutation_rate"]))
        return {
            "id": uuid4().hex[:10],
            "genome": g,
            "pnl": 0.0,
            "trades": 0,
            "wins": 0,
            "bank": float(_pb().START_BANK),
            "peak": float(_pb().START_BANK),
            "max_dd": 0.0,
            "pending": None,
            "traded": [],
        }

    def _ensure_pop(self):
        n = int(self.cfg["pop_size"])
        if not self.pop:
            self.pop.append(self._new_ind(default_genome(), jitter=False))
            self.champion_id = self.pop[0]["id"]
        while len(self.pop) < n:
            self.pop.append(self._new_ind(default_genome(), jitter=True))
        if len(self.pop) > n:
            champ = self.champion()
            rest = [p for p in self.pop if p["id"] != (champ or {}).get("id")]
            rest.sort(key=_fitness, reverse=True)
            keep = ([champ] if champ else []) + rest
            self.pop = keep[:n]
        if not any(p["id"] == self.champion_id for p in self.pop):
            self.pick_champion()

    def champion(self) -> dict | None:
        for p in self.pop:
            if p["id"] == self.champion_id:
                return p
        return self.pop[0] if self.pop else None

    def _mutate(self, g: dict, p: float | None = None) -> dict:
        rate = self.cfg["mutation_rate"] if p is None else p
        out = dict(g)
        for k in GENE_KEYS:
            if self.rng.random() < rate:
                lo, hi = RANGES[k]
                out[k] = out[k] + self.rng.gauss(0, 0.18 * (hi - lo))
        return clamp_genome(out)

    def _crossover(self, a: dict, b: dict) -> dict:
        return {k: (a[k] if self.rng.random() < 0.5 else b[k]) for k in GENE_KEYS}

    def observe(self, m, mp, books, el, left):
        """Update shadow trades for every genome. Safe to call every tick/snap."""
        if not self.enabled:
            return
        self._ensure_pop()
        slug = getattr(m, "slug", None)
        if not slug:
            return
        for ind in self.pop:
            if ind.get("pending") and ind["pending"].get("slug") == slug:
                continue
            if slug in (ind.get("traded") or []):
                continue
            ch, why = genetic_pick(ind["genome"], mp, books, left)
            if not ch:
                continue
            side, prob, edge, a = ch
            fill = shadow_stake_and_fill(ind["genome"], ind["bank"], side, prob, a, books)
            if not fill:
                continue
            ind["pending"] = {
                "slug": slug, "side": side, "shares": fill["shares"],
                "cost": fill["cost"], "fee": fill["fee"], "avg": fill["avg"],
                "prob": prob, "edge": edge, "left": left,
            }
            ind["traded"] = (ind.get("traded") or [])[-80:] + [slug]

    def champion_choice(self, m, mp, books, left):
        self._ensure_pop()
        if not self.enabled or not self.auto_trading:
            self.last_decision, self.last_reason = "WAIT", "off or paused"
            self.last_slug = getattr(m, "slug", None)
            return None, self.last_reason
        ch_ind = self.champion()
        if not ch_ind:
            self.last_decision, self.last_reason = "WAIT", "no champion"
            return None, self.last_reason
        ch, why = genetic_pick(ch_ind["genome"], mp, books, left)
        self.last_slug = getattr(m, "slug", None)
        if not ch:
            self.last_decision, self.last_reason = "WAIT", why or "no edge"
            return None, self.last_reason
        side, prob, edge, a = ch
        self.last_decision = "BUY_UP" if side == "Up" else "BUY_DOWN"
        self.last_reason = f"{side} edge={edge:.3f} ask={a:.3f}"
        return ch, None

    def on_window_settled(self, slug: str, official: str):
        if official not in ("Up", "Down"):
            return
        changed = False
        for ind in self.pop:
            pend = ind.get("pending")
            if not pend or pend.get("slug") != slug:
                continue
            payout = pend["shares"] if pend["side"] == official else 0.0
            pnl = payout - pend["cost"] - pend["fee"]
            ind["bank"] = round(ind["bank"] + pnl, 5)
            ind["pnl"] = round(ind["bank"] - _pb().START_BANK, 4)
            ind["trades"] = int(ind["trades"]) + 1
            if pnl > 0:
                ind["wins"] = int(ind.get("wins") or 0) + 1
            ind["peak"] = max(float(ind.get("peak") or _pb().START_BANK), ind["bank"])
            ind["max_dd"] = max(float(ind.get("max_dd") or 0.0), ind["peak"] - ind["bank"])
            ind["pending"] = None
            changed = True
        if not changed and not any(slug in (p.get("traded") or []) for p in self.pop):
            # still count a settled round so evolution keeps a clock
            pass
        self.settled_rounds += 1
        self.rounds_since_evolve += 1
        self.pick_champion()
        if self.rounds_since_evolve >= int(self.cfg["evolve_every"]):
            self.evolve()
        self.save()
        try:
            bot_settings.request_sync(os.path.dirname(os.path.abspath(self.path)))
        except Exception:
            pass

    def pick_champion(self):
        if not self.pop:
            return
        ranked = sorted(self.pop, key=_fitness, reverse=True)
        self.champion_id = ranked[0]["id"]

    def evolve(self):
        self._ensure_pop()
        n = len(self.pop)
        ranked = sorted(self.pop, key=_fitness, reverse=True)
        elite_n = min(2, n)
        nxt = []
        # Elites keep their shadow history.
        for e in ranked[:elite_n]:
            nxt.append(e)
        def tourney():
            k = min(3, len(ranked))
            picks = [ranked[self.rng.randrange(len(ranked))] for _ in range(k)]
            picks.sort(key=_fitness, reverse=True)
            return picks[0]
        while len(nxt) < n:
            a, b = tourney(), tourney()
            child_g = self._mutate(self._crossover(a["genome"], b["genome"]))
            nxt.append(self._new_ind(child_g, jitter=False))
        self.pop = nxt
        self.generation += 1
        self.rounds_since_evolve = 0
        self.pick_champion()
        self._dirty = True

    def public_state(self) -> dict:
        self._ensure_pop()
        rows = []
        for ind in sorted(self.pop, key=_fitness, reverse=True):
            rows.append({
                "id": ind["id"],
                "champion": ind["id"] == self.champion_id,
                "fitness": round(_fitness(ind), 3),
                "pnl": ind["pnl"],
                "trades": ind["trades"],
                "wins": ind.get("wins") or 0,
                "max_dd": round(float(ind.get("max_dd") or 0), 2),
                "bank": round(float(ind["bank"]), 2),
                "genome": {k: round(float(v), 4) for k, v in ind["genome"].items()},
            })
        best = next((r for r in rows if r["champion"]), rows[0] if rows else None)
        return {
            "generation": self.generation,
            "settled_rounds": self.settled_rounds,
            "rounds_since_evolve": self.rounds_since_evolve,
            "evolve_every": int(self.cfg["evolve_every"]),
            "pop_size": len(self.pop),
            "decision": self.last_decision,
            "decision_reason": self.last_reason,
            "best": best,
            "leaderboard": rows,
        }

    def save(self):
        blob = {
            "version": STATE_VERSION,
            "generation": self.generation,
            "rounds_since_evolve": self.rounds_since_evolve,
            "settled_rounds": self.settled_rounds,
            "champion_id": self.champion_id,
            "last_decision": self.last_decision,
            "last_reason": self.last_reason,
            "cfg": self.cfg,
            "pop": self.pop,
            "saved_at": time.time(),
        }
        path = self.path
        tmp = path + ".tmp"
        os.makedirs(os.path.dirname(os.path.abspath(path)) or ".", exist_ok=True)
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(blob, f)
        os.replace(tmp, path)

    def load(self):
        if not os.path.exists(self.path):
            self._ensure_pop()
            return
        try:
            with open(self.path, "r", encoding="utf-8") as fh:
                blob = json.load(fh)
        except Exception:
            self._ensure_pop()
            return
        self.generation = int(blob.get("generation") or 0)
        self.rounds_since_evolve = int(blob.get("rounds_since_evolve") or 0)
        self.settled_rounds = int(blob.get("settled_rounds") or 0)
        self.champion_id = blob.get("champion_id")
        self.last_decision = blob.get("last_decision") or "WAIT"
        self.last_reason = blob.get("last_reason") or ""
        if isinstance(blob.get("cfg"), dict):
            self.cfg.update({k: blob["cfg"][k] for k in self.cfg if k in blob["cfg"]})
        pop = []
        for raw in blob.get("pop") or []:
            try:
                ind = {
                    "id": raw.get("id") or uuid4().hex[:10],
                    "genome": clamp_genome(raw.get("genome") or default_genome()),
                    "pnl": float(raw.get("pnl") or 0),
                    "trades": int(raw.get("trades") or 0),
                    "wins": int(raw.get("wins") or 0),
                    "bank": float(raw.get("bank") or _pb().START_BANK),
                    "peak": float(raw.get("peak") or _pb().START_BANK),
                    "max_dd": float(raw.get("max_dd") or 0),
                    "pending": raw.get("pending"),
                    "traded": list(raw.get("traded") or []),
                }
                pop.append(ind)
            except Exception:
                continue
        self.pop = pop
        self._ensure_pop()
