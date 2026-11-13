#!/usr/bin/env python3
"""Genetic evolution of per-strategy paper parameters on historical windows.

Does not write to live bot settings. Prints and saves a report only.
"""
from __future__ import annotations

import argparse
import json
import os
import random
import time
from datetime import datetime
from typing import Any

import bot_settings
import ga_replay as R

# Keep this run short enough to finish on the box, long enough to search.
POP = 28
GENS = 22
TOURNEY = 3
ELITE = 2
MUT_P = 0.22
MUT_SCALE = 0.18
SEED = 42


def _clamp_genome(name: str, g: dict[str, float]) -> dict[str, float]:
    space = R.gene_space(name)
    out = dict(g)
    for k, (lo, hi) in space.items():
        v = float(out.get(k, lo))
        out[k] = min(hi, max(lo, v))
    if "left_lo" in out and "left_hi" in out and out["left_lo"] > out["left_hi"]:
        out["left_lo"], out["left_hi"] = out["left_hi"], out["left_lo"]
    if "el_lo" in out and "el_hi" in out and out["el_lo"] > out["el_hi"]:
        out["el_lo"], out["el_hi"] = out["el_hi"], out["el_lo"]
    if "preferred_left_lo" in out and "preferred_left_hi" in out and out["preferred_left_lo"] > out["preferred_left_hi"]:
        out["preferred_left_lo"], out["preferred_left_hi"] = out["preferred_left_hi"], out["preferred_left_lo"]
    if "min_ask" in out and "max_ask" in out and out["min_ask"] > out["max_ask"]:
        out["min_ask"], out["max_ask"] = out["max_ask"], out["min_ask"]
    return out


def _defaults_genome(name: str) -> dict[str, float]:
    d = bot_settings.defaults_for(name)
    space = R.gene_space(name)
    return {k: float(d[k]) for k in space}


def _random_genome(name: str, rng: random.Random) -> dict[str, float]:
    space = R.gene_space(name)
    # Mix of uniform samples and perturbations of the current defaults.
    base = _defaults_genome(name)
    g = {}
    for k, (lo, hi) in space.items():
        if rng.random() < 0.45:
            # jitter around current live params
            span = hi - lo
            g[k] = base[k] + rng.gauss(0, 0.15 * span)
        else:
            g[k] = lo + rng.random() * (hi - lo)
    return _clamp_genome(name, g)


def _crossover(a: dict, b: dict, rng: random.Random) -> dict:
    child = {}
    for k in a:
        child[k] = a[k] if rng.random() < 0.5 else b[k]
    return child


def _mutate(name: str, g: dict, rng: random.Random, p: float = MUT_P) -> dict:
    space = R.gene_space(name)
    out = dict(g)
    for k, (lo, hi) in space.items():
        if rng.random() < p:
            span = hi - lo
            out[k] = out[k] + rng.gauss(0, MUT_SCALE * span)
    return _clamp_genome(name, out)


def _tournament(pop: list[tuple[float, dict]], rng: random.Random, k: int = TOURNEY) -> dict:
    picks = [pop[rng.randrange(len(pop))] for _ in range(k)]
    picks.sort(key=lambda x: x[0], reverse=True)
    return picks[0][1]


def evaluate(name: str, genome: dict, windows: list, min_trades: int) -> tuple[float, dict]:
    res = R.replay(name, genome, windows)
    return R.fitness(res, min_trades=min_trades), res


def evolve_one(name: str, train: list, test: list, rng: random.Random,
               pop_size: int = POP, gens: int = GENS) -> dict:
    min_tr_train = max(6, min(12, len(train) // 12))
    min_tr_test = max(3, min(8, len(test) // 12))
    default = _defaults_genome(name)
    def_fit, def_train = evaluate(name, default, train, min_tr_train)
    _, def_test = evaluate(name, default, test, min_tr_test)

    pop: list[tuple[float, dict, dict]] = [(def_fit, default, def_train)]
    seen = {tuple(sorted((k, round(v, 6)) for k, v in default.items()))}
    while len(pop) < pop_size:
        g = _random_genome(name, rng)
        key = tuple(sorted((k, round(v, 6)) for k, v in g.items()))
        if key in seen:
            continue
        seen.add(key)
        fit, res = evaluate(name, g, train, min_tr_train)
        pop.append((fit, g, res))

    history = []
    for gen in range(gens):
        pop.sort(key=lambda x: x[0], reverse=True)
        history.append({"gen": gen, "best_fit": pop[0][0], "best_pnl": pop[0][2]["pnl"],
                        "best_trades": pop[0][2]["trades"]})
        ranked = [(p[0], p[1]) for p in pop]
        nxt: list[tuple[float, dict, dict]] = pop[:ELITE]
        elite_keys = {tuple(sorted((k, round(v, 6)) for k, v in p[1].items())) for p in nxt}
        while len(nxt) < pop_size:
            a = _tournament(ranked, rng)
            b = _tournament(ranked, rng)
            child = _mutate(name, _crossover(a, b, rng), rng)
            key = tuple(sorted((k, round(v, 6)) for k, v in child.items()))
            if key in elite_keys:
                child = _mutate(name, child, rng, p=0.8)
                key = tuple(sorted((k, round(v, 6)) for k, v in child.items()))
            fit, res = evaluate(name, child, train, min_tr_train)
            nxt.append((fit, child, res))
            elite_keys.add(key)
        pop = nxt

    pop.sort(key=lambda x: x[0], reverse=True)
    # Re-score the top few on the held-out test set; pick the reported "best"
    # as the elite with the best train fitness (standard), and also list OOS.
    top = []
    for fit, g, train_res in pop[:5]:
        _, test_res = evaluate(name, g, test, min_tr_test)
        top.append({
            "train_fitness": round(fit, 4),
            "genome": {k: round(float(v), 4) for k, v in g.items()},
            "train": _brief(train_res),
            "test": _brief(test_res),
        })
    best = top[0]
    return {
        "strategy": name,
        "default": {
            "genome": {k: round(float(v), 4) for k, v in default.items()},
            "train": _brief(def_train),
            "test": _brief(def_test),
            "train_fitness": round(def_fit, 4),
        },
        "best": best,
        "top": top,
        "delta_train_pnl": round(best["train"]["pnl"] - def_train["pnl"], 4),
        "delta_test_pnl": round(best["test"]["pnl"] - def_test["pnl"], 4),
        "history": history,
        "min_trades_train": min_tr_train,
        "min_trades_test": min_tr_test,
        "pop": pop_size,
        "gens": gens,
    }


def _brief(res: dict) -> dict:
    return {
        "ok": bool(res.get("ok", True)),
        "reason": res.get("reason"),
        "pnl": res.get("pnl"),
        "trades": res.get("trades"),
        "wins": res.get("wins"),
        "win_rate": res.get("win_rate"),
        "max_dd": res.get("max_dd"),
        "bank": res.get("bank"),
    }


def run(db_path: str, out_path: str, strategies: list[str] | None = None,
        seed: int = SEED, pop: int = POP, gens: int = GENS) -> dict:
    windows = R.load_windows(db_path)
    data = R.describe_data(windows)
    names = strategies or list(R.SNAP_OK)
    rng = random.Random(seed)
    report: dict[str, Any] = {
        "paper_only": True,
        "applied_to_live": False,
        "note": "Top genomes are reported only. They were NOT written to live bot settings.",
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S %Z").strip() + " (Asia/Jerusalem)",
        "seed": seed,
        "pop": pop,
        "gens": gens,
        "data": data,
        "splits": {},
        "strategies": {},
        "skipped": {},
    }
    splits = R.walk_forward_splits(windows)
    # Primary split is 70/30; also keep brief default-vs-best on other folds.
    primary_name, train, test = splits[0]
    report["splits"][primary_name] = {"train_windows": len(train), "test_windows": len(test),
                                      "train_start": train[0]["start"] if train else None,
                                      "train_end": train[-1]["start"] if train else None,
                                      "test_start": test[0]["start"] if test else None,
                                      "test_end": test[-1]["start"] if test else None}
    for extra_name, tr, te in splits[1:]:
        report["splits"][extra_name] = {"train_windows": len(tr), "test_windows": len(te)}

    for name in names:
        if name in R.NEEDS_PATH:
            # Confirm whether any window is usable
            probe = R.replay(name, None, windows)
            if not probe.get("ok"):
                report["skipped"][name] = {
                    "reason": probe.get("reason") or "insufficient_price_path",
                    "detail": "Old snapshots have no 60s price path, so this strategy cannot be evolved fairly yet. Richer snapshots are now recorded.",
                }
                continue
        t0 = time.time()
        print(f"evolving {name} train={len(train)} test={len(test)} ...", flush=True)
        one = evolve_one(name, train, test, rng, pop_size=pop, gens=gens)
        # Extra folds: score the already-found best and the defaults only (no second full search).
        extra = {}
        for extra_name, tr, te in splits[1:]:
            if not te:
                continue
            min_tr = max(3, min(8, len(te) // 12))
            min_tr_tr = max(6, min(12, len(tr) // 12))
            _, dtr = evaluate(name, _defaults_genome(name), tr, min_tr_tr)
            _, dte = evaluate(name, _defaults_genome(name), te, min_tr)
            _, btr = evaluate(name, one["best"]["genome"], tr, min_tr_tr)
            _, bte = evaluate(name, one["best"]["genome"], te, min_tr)
            extra[extra_name] = {
                "default": {"train": _brief(dtr), "test": _brief(dte)},
                "best": {"train": _brief(btr), "test": _brief(bte)},
            }
        one["other_folds"] = extra
        one["seconds"] = round(time.time() - t0, 1)
        report["strategies"][name] = one
        print(f"  {name}: train pnl {one['default']['train']['pnl']} -> {one['best']['train']['pnl']}  "
              f"OOS {one['default']['test']['pnl']} -> {one['best']['test']['pnl']}  "
              f"({one['seconds']}s)", flush=True)

    os.makedirs(os.path.dirname(os.path.abspath(out_path)) or ".", exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)
    return report


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "trades.db"))
    ap.add_argument("--out", default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "ga_results.json"))
    ap.add_argument("--seed", type=int, default=SEED)
    ap.add_argument("--pop", type=int, default=POP)
    ap.add_argument("--gens", type=int, default=GENS)
    ap.add_argument("--strategies", nargs="*", default=None)
    args = ap.parse_args()
    run(args.db, args.out, strategies=args.strategies, seed=args.seed, pop=args.pop, gens=args.gens)
    print("wrote", args.out)


if __name__ == "__main__":
    main()
