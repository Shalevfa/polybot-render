"""
LatencyHunter — BTC 5-minute Polymarket strategy agent (as provided by Shalev, 2026-11-12)
Paper-trading-first decision engine. Does NOT place live orders.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, asdict
from typing import Deque, Optional, Dict, Any
import json
import math
import time


@dataclass(frozen=True)
class HunterConfig:
    min_net_edge: float = 0.06
    min_confidence: float = 0.60
    max_token_spread: float = 0.04
    min_seconds_remaining: float = 20.0
    cooldown_seconds: float = 20.0
    min_history_seconds: float = 65.0
    min_volatility_observations: int = 20
    weight_15s: float = 0.40
    weight_30s: float = 0.35
    weight_60s: float = 0.25
    momentum_probability_scale: float = 0.025
    volatility_floor_bps_sqrt_s: float = 0.05
    max_tick_age_seconds: float = 3.0
    history_seconds: float = 120.0


@dataclass
class HunterSignal:
    action: str
    confidence: float
    gross_edge: float
    net_edge: float
    reason: str
    timestamp: float
    diagnostics: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), separators=(",", ":"))


class LatencyHunter:
    def __init__(self, config: Optional[HunterConfig] = None):
        self.config = config or HunterConfig()
        self.prices: Deque[tuple[float, float]] = deque()
        self.last_confirmed_trade_at: Optional[float] = None

    def update_price(self, price: float, timestamp: Optional[float] = None) -> None:
        ts = time.time() if timestamp is None else float(timestamp)
        px = float(price)
        if not math.isfinite(px) or px <= 0:
            return
        if not math.isfinite(ts):
            return
        if self.prices and ts < self.prices[-1][0]:
            return
        if self.prices and ts == self.prices[-1][0]:
            self.prices[-1] = (ts, px)
        else:
            self.prices.append((ts, px))
        cutoff = ts - self.config.history_seconds
        while self.prices and self.prices[0][0] < cutoff:
            self.prices.popleft()

    def _price_at_or_before(self, target_ts: float) -> Optional[float]:
        for ts, px in reversed(self.prices):
            if ts <= target_ts:
                return px
        return None

    def _momentum_bps(self, seconds: int) -> Optional[float]:
        if not self.prices:
            return None
        now_ts, current = self.prices[-1]
        old = self._price_at_or_before(now_ts - seconds)
        if old is None or old <= 0:
            return None
        return math.log(current / old) * 10000.0

    def _volatility_bps_sqrt_s(self) -> Optional[float]:
        if len(self.prices) < self.config.min_volatility_observations + 1:
            return None
        observations = []
        previous_ts, previous_px = self.prices[0]
        for ts, px in list(self.prices)[1:]:
            dt = ts - previous_ts
            if dt > 0 and previous_px > 0 and px > 0:
                return_bps = math.log(px / previous_px) * 10000.0
                observations.append(return_bps / math.sqrt(dt))
            previous_ts, previous_px = ts, px
        if len(observations) < self.config.min_volatility_observations:
            return None
        mean = sum(observations) / len(observations)
        variance = sum((x - mean) ** 2 for x in observations) / max(1, len(observations) - 1)
        return math.sqrt(max(variance, 0.0))

    @staticmethod
    def _normal_cdf(value: float) -> float:
        return 0.5 * (1.0 + math.erf(value / math.sqrt(2.0)))

    def _base_probability_up(self, market_open_price, current_price, seconds_remaining, volatility_bps_sqrt_s) -> float:
        if seconds_remaining <= 0:
            return 1.0 if current_price > market_open_price else 0.0
        distance_bps = math.log(current_price / market_open_price) * 10000.0
        sigma = max(volatility_bps_sqrt_s, self.config.volatility_floor_bps_sqrt_s) * math.sqrt(seconds_remaining)
        if sigma <= 0:
            return 0.5
        return self._normal_cdf(distance_bps / sigma)

    def _wait(self, reason, timestamp, diagnostics=None) -> HunterSignal:
        return HunterSignal("WAIT", 0.0, 0.0, 0.0, reason, timestamp, diagnostics or {})

    def evaluate(self, market_open_price, seconds_remaining, up_bid, up_ask, down_bid, down_ask,
                 estimated_cost: float = 0.0, now: Optional[float] = None) -> HunterSignal:
        ts = time.time() if now is None else float(now)
        # Only ticks at or before decision time are used (no look-ahead on replay).
        saved_prices = self.prices
        self.prices = deque(px for px in saved_prices if px[0] <= ts)
        try:
            return self._evaluate_at(ts, market_open_price, seconds_remaining,
                                     up_bid, up_ask, down_bid, down_ask, estimated_cost)
        finally:
            self.prices = saved_prices

    def _evaluate_at(self, ts, market_open_price, seconds_remaining,
                     up_bid, up_ask, down_bid, down_ask, estimated_cost) -> HunterSignal:
        if not self.prices:
            return self._wait("No BTC price data", ts)
        current_ts, current_price = self.prices[-1]
        if ts - current_ts > self.config.max_tick_age_seconds:
            return self._wait("BTC price feed is stale", ts)
        if not math.isfinite(market_open_price) or market_open_price <= 0:
            return self._wait("Invalid market open price", ts)
        if seconds_remaining < self.config.min_seconds_remaining:
            return self._wait("Too close to market resolution", ts)
        if self.last_confirmed_trade_at is not None:
            if ts - self.last_confirmed_trade_at < self.config.cooldown_seconds:
                return self._wait("Trade cooldown active", ts)
        if len(self.prices) < 2:
            return self._wait("Insufficient BTC history", ts)
        history_age = current_ts - self.prices[0][0]
        if history_age < self.config.min_history_seconds:
            return self._wait("Warming up price history", ts)
        quotes = (up_bid, up_ask, down_bid, down_ask)
        if any(q is None for q in quotes):
            return self._wait("Missing executable bid/ask quotes", ts)
        try:
            up_bid, up_ask, down_bid, down_ask = map(float, quotes)
            estimated_cost = float(estimated_cost)
        except (TypeError, ValueError):
            return self._wait("Invalid quote or cost format", ts)
        if not all(math.isfinite(q) and 0 < q < 1 for q in quotes):
            return self._wait("Invalid token quote", ts)
        if not math.isfinite(estimated_cost) or estimated_cost < 0:
            return self._wait("Invalid estimated costs", ts)
        up_spread = up_ask - up_bid
        down_spread = down_ask - down_bid
        if up_spread < 0 or down_spread < 0:
            return self._wait("Crossed or inconsistent order book", ts)
        m15 = self._momentum_bps(15)
        m30 = self._momentum_bps(30)
        m60 = self._momentum_bps(60)
        volatility = self._volatility_bps_sqrt_s()
        if None in (m15, m30, m60) or volatility is None:
            return self._wait("Insufficient data for momentum/volatility", ts)
        r15 = m15 / 15.0
        r30 = m30 / 30.0
        r60 = m60 / 60.0
        cfg = self.config
        momentum_score = cfg.weight_15s * r15 + cfg.weight_30s * r30 + cfg.weight_60s * r60
        acceleration = r15 - r60
        p_up_base = self._base_probability_up(market_open_price, current_price, seconds_remaining, volatility)
        momentum_adjustment = cfg.momentum_probability_scale * math.tanh(momentum_score + 0.25 * acceleration)
        p_up = min(0.99, max(0.01, p_up_base + momentum_adjustment))
        p_down = 1.0 - p_up
        gross_edge_up = p_up - up_ask
        gross_edge_down = p_down - down_ask
        net_edge_up = gross_edge_up - estimated_cost
        net_edge_down = gross_edge_down - estimated_cost
        diagnostics = {
            "btc_price": current_price, "market_open_price": market_open_price,
            "seconds_remaining": seconds_remaining,
            "momentum_bps": {"15s": m15, "30s": m30, "60s": m60},
            "momentum_score": momentum_score, "acceleration": acceleration,
            "volatility_bps_sqrt_s": volatility, "probability_up": p_up, "probability_down": p_down,
            "up_bid": up_bid, "up_ask": up_ask, "down_bid": down_bid, "down_ask": down_ask,
            "up_spread": up_spread, "down_spread": down_spread, "estimated_cost": estimated_cost,
            "net_edge_up": net_edge_up, "net_edge_down": net_edge_down,
        }
        if net_edge_up >= net_edge_down:
            action, confidence, gross_edge, net_edge, selected_spread = "BUY_UP", p_up, gross_edge_up, net_edge_up, up_spread
        else:
            action, confidence, gross_edge, net_edge, selected_spread = "BUY_DOWN", p_down, gross_edge_down, net_edge_down, down_spread
        momentum_aligned = (action == "BUY_UP" and momentum_score > 0) or (action == "BUY_DOWN" and momentum_score < 0)
        if not momentum_aligned:
            return HunterSignal("WAIT", confidence, gross_edge, net_edge, "Momentum does not confirm selected direction", ts, diagnostics)
        if selected_spread > cfg.max_token_spread:
            return HunterSignal("WAIT", confidence, gross_edge, net_edge, "Selected token spread is too wide", ts, diagnostics)
        if confidence < cfg.min_confidence:
            return HunterSignal("WAIT", confidence, gross_edge, net_edge, "Model confidence below threshold", ts, diagnostics)
        if net_edge < cfg.min_net_edge:
            return HunterSignal("WAIT", confidence, gross_edge, net_edge, "Net edge below entry threshold", ts, diagnostics)
        return HunterSignal(action, confidence, gross_edge, net_edge, "Momentum and probability divergence", ts, diagnostics)

    def mark_trade(self, timestamp: Optional[float] = None) -> None:
        self.last_confirmed_trade_at = time.time() if timestamp is None else float(timestamp)

    def reset_cooldown(self) -> None:
        self.last_confirmed_trade_at = None


if __name__ == "__main__":
    agent = LatencyHunter()
    start = time.time() - 75.0
    for i in range(76):
        agent.update_price(100000.0 * (1.0 + 0.000002 * i), timestamp=start + i)
    print(agent.evaluate(100000.0, 180.0, 0.48, 0.50, 0.48, 0.50, estimated_cost=0.005, now=time.time()).to_json())
