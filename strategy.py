"""Deterministic confluence strategy for the BTC paper-trading system.

Signals are advisory by default. No claim of profitability is made. Inputs must be
observed/derived from real exchange data; absent or conflicting inputs yield WAIT.
"""
from __future__ import annotations
from dataclasses import dataclass, asdict
from enum import Enum
import math
import time
from typing import Any, Iterable, Mapping, Sequence


class SignalSide(str, Enum):
    BUY = "BUY"
    SELL = "SELL"
    WAIT = "WAIT"


@dataclass(frozen=True)
class StrategyConfig:
    symbol: str = "BTCUSDT"
    min_delta_btc: float = 0.25
    min_delta_percent: float = 12.0
    min_breakout_usd: float = 15.0
    wall_max_age_seconds: float = 300.0
    min_stop_usd: float = 400.0
    max_stop_usd: float = 500.0
    reward_risk: float = 2.0
    cooldown_seconds: float = 300.0
    range_middle_fraction: float = 0.30
    max_entry_distance_usd: float = 250.0

    def validate(self) -> None:
        for name in ("min_delta_btc", "min_delta_percent", "min_breakout_usd", "wall_max_age_seconds", "min_stop_usd", "max_stop_usd", "reward_risk", "cooldown_seconds", "max_entry_distance_usd"):
            v = float(getattr(self, name))
            if not math.isfinite(v) or v < 0:
                raise ValueError(f"{name} must be finite and nonnegative")
        if self.min_stop_usd <= 0 or self.max_stop_usd < self.min_stop_usd:
            raise ValueError("invalid stop-distance bounds")
        if abs(self.reward_risk - 2.0) > 1e-9:
            raise ValueError("this strategy requires strict 1:2 reward/risk")


@dataclass(frozen=True)
class StrategySignal:
    side: SignalSide
    timestamp: float
    symbol: str
    price: float | None
    confidence_score: int
    setup_tag: str
    reason: str
    stop_loss: float | None = None
    take_profit: float | None = None
    risk_distance_usd: float | None = None
    reward_distance_usd: float | None = None
    evidence: Mapping[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        out = asdict(self)
        out["side"] = self.side.value
        return out


class ConfluenceEngine:
    """Requires a negative-gamma regime, an eaten wall, directional delta and a break.

    The engine deliberately returns WAIT when a wall is absent/old, delta is weak,
    inputs are malformed, the price is mid-range, or the available risk geometry is
    unsuitable. It does not infer aggressor flow from candle direction.
    """
    def __init__(self, config: StrategyConfig | None = None):
        self.config = config or StrategyConfig()
        self.config.validate()
        self._last_signal_at = 0.0
        self._last_signal_side: SignalSide | None = None

    @staticmethod
    def _get(obj: Any, key: str, default: Any = None) -> Any:
        return obj.get(key, default) if isinstance(obj, Mapping) else getattr(obj, key, default)

    @staticmethod
    def _number(value: Any) -> float | None:
        try:
            v = float(value)
            return v if math.isfinite(v) else None
        except (TypeError, ValueError):
            return None

    def evaluate(self, *, price: float, gamma: Any, walls: Sequence[Any], delta_bar: Any,
                 candles: Sequence[Any] = (), sr_zones: Sequence[Any] = (),
                 now: float | None = None, has_open_position: bool = False) -> StrategySignal:
        ts = time.time() if now is None else float(now)
        p = self._number(price)
        if p is None or p <= 0:
            return self._wait(ts, None, "No valid market price")
        if has_open_position:
            return self._wait(ts, p, "Position already open; no pyramiding")
        if ts - self._last_signal_at < self.config.cooldown_seconds:
            return self._wait(ts, p, "Entry cooldown active")
        if gamma is None:
            return self._wait(ts, p, "Gamma regime unavailable")
        regime = str(self._get(gamma, "regime", "")).lower()
        if regime not in {"wild", "negative", "negative_gamma", "negative gamma"}:
            return self._wait(ts, p, "No confirmed negative-gamma/Wild regime")
        if delta_bar is None:
            return self._wait(ts, p, "Footprint delta unavailable")
        delta = self._number(self._get(delta_bar, "delta"))
        total = self._number(self._get(delta_bar, "total_volume"))
        delta_pct = self._number(self._get(delta_bar, "delta_percent"))
        if delta is None or total is None or total <= 0:
            return self._wait(ts, p, "Invalid footprint delta")
        if delta_pct is None:
            delta_pct = delta / total * 100.0
        if abs(delta) < self.config.min_delta_btc or abs(delta_pct) < self.config.min_delta_percent:
            return self._wait(ts, p, "Footprint delta below momentum threshold")
        direction = SignalSide.BUY if delta > 0 else SignalSide.SELL
        # The wall must have been explicitly classified as Eaten; missing state is not guessed.
        candidates = []
        for wall in walls or ():
            state = str(self._get(wall, "state", "")).lower().replace("_", " ")
            side = str(self._get(wall, "side", "")).lower()
            level = self._number(self._get(wall, "strike", self._get(wall, "price")))
            age = self._number(self._get(wall, "age_seconds"))
            if "eaten" not in state or level is None or age is None or age > self.config.wall_max_age_seconds:
                continue
            # Buyer delta should consume an ask wall; seller delta should consume a bid wall.
            expected_wall = "ask" if direction == SignalSide.BUY else "bid"
            if side == expected_wall and abs(p - level) <= self.config.max_entry_distance_usd:
                candidates.append((abs(p-level), level, side, age))
        if not candidates:
            return self._wait(ts, p, "No recent eaten wall aligned with delta direction")
        _, wall_price, wall_side, wall_age = min(candidates)
        # Require a confirmed close beyond the wall, not just a touch.
        if not candles:
            return self._wait(ts, p, "No closed candle available to confirm wall break")
        last = candles[-1]
        closed = bool(self._get(last, "closed", True))
        close = self._number(self._get(last, "close"))
        if not closed or close is None:
            return self._wait(ts, p, "Latest candle is not a valid closed candle")
        if direction == SignalSide.BUY and close < wall_price + self.config.min_breakout_usd:
            return self._wait(ts, p, "No bullish close beyond eaten ask wall")
        if direction == SignalSide.SELL and close > wall_price - self.config.min_breakout_usd:
            return self._wait(ts, p, "No bearish close beyond eaten bid wall")
        # Middle-of-range filter: if recent candles define a broad range, avoid its centre.
        # Compare the latest closed candle against the preceding range, excluding it.
        prior = list(candles[-21:-1]) if len(candles) > 1 else []
        highs = [self._number(self._get(c, "high")) for c in prior]
        lows = [self._number(self._get(c, "low")) for c in prior]
        highs = [v for v in highs if v is not None]; lows = [v for v in lows if v is not None]
        if highs and lows:
            hi, lo = max(highs), min(lows)
            if hi > lo and lo + (hi-lo)*self.config.range_middle_fraction < close < hi - (hi-lo)*self.config.range_middle_fraction:
                # Only allow a clear breakout outside the previous range; otherwise wait.
                if not ((direction == SignalSide.BUY and close > hi) or (direction == SignalSide.SELL and close < lo)):
                    return self._wait(ts, p, "Price is chopping in the middle of its recent range")
        # Dynamic stop: behind the broken wall; optionally behind a nearby S/R zone.
        stop_anchor = wall_price
        for zone in sr_zones or ():
            zlow = self._number(self._get(zone, "low")); zhigh = self._number(self._get(zone, "high"))
            if zlow is None or zhigh is None: continue
            if direction == SignalSide.BUY and zlow <= wall_price <= zhigh:
                stop_anchor = min(stop_anchor, zlow)
            elif direction == SignalSide.SELL and zlow <= wall_price <= zhigh:
                stop_anchor = max(stop_anchor, zhigh)
        buffer = max(25.0, p * 0.00025)
        raw_stop = stop_anchor - buffer if direction == SignalSide.BUY else stop_anchor + buffer
        raw_risk = p - raw_stop if direction == SignalSide.BUY else raw_stop - p
        if not math.isfinite(raw_risk) or raw_risk <= 0:
            return self._wait(ts, p, "Stop geometry invalid")
        # Preserve the structural stop when it fits; otherwise constrain to configured $400-$500.
        risk = min(self.config.max_stop_usd, max(self.config.min_stop_usd, raw_risk))
        stop = p - risk if direction == SignalSide.BUY else p + risk
        reward = risk * self.config.reward_risk
        target = p + reward if direction == SignalSide.BUY else p - reward
        self._last_signal_at = ts
        self._last_signal_side = direction
        return StrategySignal(direction, ts, self.config.symbol, p, 4,
            f"WILD_GAMMA+EATEN_{wall_side.upper()}_WALL+FOOTPRINT_{'BUY' if delta > 0 else 'SELL'}_DELTA",
            "Negative gamma, eaten wall, confirmed closed-candle break and directional footprint delta agree.",
            stop, target, risk, reward,
            {"gamma_regime": regime, "delta_btc": delta, "delta_percent": delta_pct,
             "wall_price": wall_price, "wall_side": wall_side, "wall_age_seconds": wall_age,
             "closed_candle_close": close, "risk_reward": "1:2"})

    def _wait(self, ts: float, price: float | None, reason: str) -> StrategySignal:
        return StrategySignal(SignalSide.WAIT, ts, self.config.symbol, price, 0, "NONE", reason)
