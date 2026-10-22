"""Orchestrator entry point for the paper-only BTC dashboard and strategy.

Run: python main.py
Runs the big-move confluence strategy and a filtered scalp strategy together (scalp only fires
when the big-move one is WAIT). Paper only. Set ENABLE_STRATEGY_TRADING=false to stop orders. Even
then, the only supplied broker is PaperBroker; this module has no live order path.
"""
from __future__ import annotations
import asyncio
import logging
import os
import signal
import time
from typing import Any
import uvicorn

import server
from analytics import to_dict
from broker import Order, OrderSide, OrderType
from dataclasses import replace as _dc_replace
from strategy import ConfluenceEngine, StrategyConfig, SignalSide, StrategySignal

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
log = logging.getLogger("btc-orchestrator")
_SYMBOL = os.getenv("TRADING_SYMBOL", "BTCUSDT")
_QTY = float(os.getenv("PAPER_ORDER_BTC", "0.01"))
_ENABLED = os.getenv("ENABLE_STRATEGY_TRADING", "true").lower() in {"1", "true", "yes"}

SCALP_TAG = "SCALP_DELTA_MOMENTUM"


def _candle_ts(candle: dict[str, Any]) -> float | None:
    """Return a candle timestamp in seconds, accepting common feed formats."""
    value = candle.get("time", candle.get("timestamp", candle.get("start_time")))
    if value is None:
        return None
    try:
        ts = float(value)
        return ts / 1000.0 if ts > 10_000_000_000 else ts
    except (TypeError, ValueError):
        return None


def _ema(values: list[float], period: int) -> float:
    alpha = 2.0 / (period + 1.0)
    result = values[0]
    for value in values[1:]:
        result = alpha * value + (1.0 - alpha) * result
    return result


def _five_minute_candles(candles: list[dict[str, Any]], now: float) -> list[dict[str, float]]:
    """Aggregate closed 1m candles into complete, closed 5m candles only."""
    groups: dict[int, list[dict[str, Any]]] = {}
    for candle in candles:
        ts = _candle_ts(candle)
        if ts is None:
            continue
        bucket = int(ts // 300)
        # A group is eligible only after its 5-minute interval has ended.
        if bucket * 300 + 300 > now:
            continue
        groups.setdefault(bucket, []).append(candle)
    result = []
    for bucket, group in sorted(groups.items()):
        group.sort(key=lambda c: _candle_ts(c) or 0.0)
        # Require five distinct minute candles; don't manufacture a bar across gaps.
        minute_ids = {int((_candle_ts(c) or 0.0) // 60) for c in group}
        expected = {bucket * 5 + i for i in range(5)}
        if not expected.issubset(minute_ids):
            continue
        result.append({
            "time": float(bucket * 300),
            "open": float(group[0]["open"]),
            "high": max(float(c["high"]) for c in group),
            "low": min(float(c["low"]) for c in group),
            "close": float(group[-1]["close"]),
        })
    return result


class ScalpEngine:
    """Selective 1m footprint scalper with regime, exhaustion, structure and cost filters.

    The filters are deliberately configurable so paper experiments can isolate their
    effects. They are not claims of profitability; keep the bot paper-only while testing.
    """
    def __init__(self, symbol, min_delta_btc=0.2, min_delta_pct=15.0,
                 min_stop=120.0, range_mult=1.5, cooldown=120.0):
        self.symbol = symbol
        self.min_delta_btc = float(min_delta_btc)
        self.min_delta_pct = float(min_delta_pct)
        self.min_stop = float(min_stop)
        self.range_mult = float(range_mult)
        self.cooldown = float(cooldown)
        self._last = 0.0
        self.trend_filter = os.getenv("SCALP_TREND_FILTER", "true").lower() in {"1", "true", "yes"}
        self.exhaustion_filter = os.getenv("SCALP_EXHAUSTION_FILTER", "true").lower() in {"1", "true", "yes"}
        self.structure_filter = os.getenv("SCALP_STRUCTURE_FILTER", "true").lower() in {"1", "true", "yes"}
        self.cost_filter = os.getenv("SCALP_COST_FILTER", "true").lower() in {"1", "true", "yes"}
        self.fee_rate = max(0.0, float(os.getenv("SCALP_FEE_RATE", "0.0005")))
        self.slippage_bps = max(0.0, float(os.getenv("SCALP_SLIPPAGE_BPS", "1.0")))
        self.min_reward_cost_ratio = max(0.0, float(os.getenv("SCALP_MIN_REWARD_COST_RATIO", "2.0")))
        self.max_extension_ranges = max(1.0, float(os.getenv("SCALP_MAX_EXTENSION_RANGES", "2.0")))
        self.min_close_location = min(0.9, max(0.5, float(os.getenv("SCALP_MIN_CLOSE_LOCATION", "0.65"))))

    def evaluate(self, *, price, delta_bar, candles, now, has_open_position,
                 trend_candles=None, quantity_btc=None):
        def wait(why):
            return StrategySignal(SignalSide.WAIT, now, self.symbol, price, 0, "NONE", why)

        if has_open_position:
            return wait("Position already open")
        if now - self._last < self.cooldown:
            return wait("Cooldown")
        if delta_bar is None or len(candles) < 20:
            return wait("Not enough 1m data (need at least 20 closed candles)")
        try:
            total, delta = float(delta_bar.total_volume), float(delta_bar.delta)
        except (AttributeError, TypeError, ValueError):
            return wait("Invalid footprint bar")
        if not total or total < 0:
            return wait("No volume")
        pct = delta / total * 100.0
        if abs(delta) < self.min_delta_btc or abs(pct) < self.min_delta_pct:
            return wait("Delta too weak")

        last, prev = candles[-1], candles[-2]
        up = delta > 0
        if up and not (float(last["close"]) > float(last["open"]) and float(last["close"]) > float(prev["close"])):
            return wait("Candle does not confirm buy delta")
        if not up and not (float(last["close"]) < float(last["open"]) and float(last["close"]) < float(prev["close"])):
            return wait("Candle does not confirm sell delta")

        # Higher-timeframe regime: aggregate only complete 5m bars from the 1m feed.
        five = trend_candles or []
        if self.trend_filter:
            if len(five) < 13:
                return wait("Trend filter: need 13 complete 5m candles")
            closes = [float(c["close"]) for c in five[-20:]]
            fast_now = _ema(closes[-5:], 5)
            slow_now = _ema(closes[-13:], 13)
            fast_prev = _ema(closes[-6:-1], 5) if len(closes) >= 6 else fast_now
            if fast_now > slow_now and fast_now > fast_prev and closes[-1] >= fast_now:
                regime = "bullish"
            elif fast_now < slow_now and fast_now < fast_prev and closes[-1] <= fast_now:
                regime = "bearish"
            else:
                return wait("Trend filter: 5m regime is neutral/choppy")
            if up and regime != "bullish":
                return wait("Trend filter: reject long against 5m regime")
            if not up and regime != "bearish":
                return wait("Trend filter: reject short against 5m regime")
        else:
            regime = "disabled"

        recent = candles[-20:]
        ranges = [max(0.0, float(c["high"]) - float(c["low"])) for c in recent[-6:-1]]
        avg = sum(ranges) / len(ranges) if ranges else 0.0
        if avg <= 0:
            return wait("Invalid recent candle range")
        last_range = max(0.0, float(last["high"]) - float(last["low"]))
        candle_range = max(last_range, 1e-9)
        close_location = (float(last["close"]) - float(last["low"])) / candle_range

        if self.exhaustion_filter:
            # Reject outsized late candles and delta with very little price progress.
            move = abs(float(last["close"]) - float(prev["close"]))
            body = abs(float(last["close"]) - float(last["open"]))
            if last_range > avg * self.max_extension_ranges:
                return wait("Momentum filter: signal candle is overextended")
            if body < avg * 0.15 and move < avg * 0.15:
                return wait("Momentum filter: strong delta but little price progress (possible absorption)")
            if up and close_location < self.min_close_location:
                return wait("Momentum filter: bullish candle closed too far from its high")
            if not up and close_location > (1.0 - self.min_close_location):
                return wait("Momentum filter: bearish candle closed too far from its low")
            prior_move = abs(float(last["close"]) - float(recent[-4]["close"]))
            if prior_move > avg * self.max_extension_ranges * 1.5:
                return wait("Momentum filter: price already extended over recent candles")

        risk = max(self.min_stop, avg * self.range_mult)
        reward = risk * 2.0
        stop = float(price) - risk if up else float(price) + risk
        target = float(price) + reward if up else float(price) - reward
        qty = float(quantity_btc or _QTY)
        notional = abs(float(price) * qty)
        # Approximate round-trip taker fees + slippage on both entry and exit.
        estimated_cost = notional * (2.0 * self.fee_rate + 2.0 * self.slippage_bps / 10000.0)
        gross_reward = reward * qty
        net_reward = gross_reward - estimated_cost
        if self.cost_filter:
            if net_reward <= 0:
                return wait(f"Cost filter: estimated net target is not positive (${net_reward:.2f})")
            ratio = gross_reward / max(estimated_cost, 1e-9)
            if ratio < self.min_reward_cost_ratio:
                return wait(f"Cost filter: reward/cost {ratio:.2f}x below {self.min_reward_cost_ratio:.2f}x")

        # Structure filter: reject trades whose target runs into a recent local swing.
        if self.structure_filter:
            swing_highs = []
            swing_lows = []
            for i in range(1, len(recent) - 1):
                hi = float(recent[i]["high"])
                lo = float(recent[i]["low"])
                if hi >= float(recent[i - 1]["high"]) and hi >= float(recent[i + 1]["high"]):
                    if hi > float(price):
                        swing_highs.append(hi)
                if lo <= float(recent[i - 1]["low"]) and lo <= float(recent[i + 1]["low"]):
                    if lo < float(price):
                        swing_lows.append(lo)
            if up and swing_highs:
                room = min(swing_highs) - float(price)
                if room < reward * 1.10:
                    return wait("Structure filter: nearby swing resistance leaves insufficient target room")
            if not up and swing_lows:
                room = float(price) - max(swing_lows)
                if room < reward * 1.10:
                    return wait("Structure filter: nearby swing support leaves insufficient target room")

        self._last = now
        return StrategySignal(
            SignalSide.BUY if up else SignalSide.SELL, now, self.symbol, price, 3,
            SCALP_TAG, "Delta + candle + 5m regime + momentum/structure/cost filters",
            stop, target, risk, reward,
            {"delta_btc": delta, "delta_percent": pct, "regime_5m": regime,
             "estimated_round_trip_cost_usd": estimated_cost,
             "estimated_net_target_usd": net_reward,
             "reward_to_cost_ratio": gross_reward / max(estimated_cost, 1e-9),
             "avg_recent_range_usd": avg, "close_location": close_location})

ENGINE = ConfluenceEngine(StrategyConfig(symbol=_SYMBOL))
_SCALP_ON = os.getenv("SCALP_ENABLED", "true").lower() in {"1", "true", "yes"}
SCALP = ScalpEngine(
    _SYMBOL,
    min_delta_btc=float(os.getenv("SCALP_MIN_DELTA_BTC", "0.2")),
    min_delta_pct=float(os.getenv("SCALP_MIN_DELTA_PCT", "15")),
    min_stop=float(os.getenv("SCALP_MIN_STOP", "120")),
    range_mult=float(os.getenv("SCALP_RANGE_MULT", "1.5")),
    cooldown=float(os.getenv("SCALP_COOLDOWN_SECONDS", "120")),
)
SCALP_MAX_HOLD_SECONDS = max(60.0, float(os.getenv("SCALP_MAX_HOLD_SECONDS", "900")))
_original_feed_handler = server.on_feed_event
_last_eval_bar = None


def _position_qty(position) -> float:
    return float(getattr(position, "quantity_btc", 0.0) or 0.0)


async def _journal_position_transition(*, before_position, before_balance, before_fills,
                                       after_position, after_balance, after_fills,
                                       timestamp: float, price: float, exit_reason_override: str | None = None) -> None:
    """Reconcile broker position transitions to the journal, using actual paper fills."""
    before_qty = _position_qty(before_position)
    after_qty = _position_qty(after_position)
    open_trade = await server.DB.open_trade_for_symbol(_SYMBOL)

    # If a position exists but the journal row was lost (e.g. restart between broker
    # snapshot and journal write), reconstruct a clearly tagged recovery row.
    if abs(after_qty) > 1e-12 and open_trade is None:
        direction = "long" if after_qty > 0 else "short"
        opened_at = getattr(after_position, "opened_at", None) or timestamp
        await server.DB.create_trade(
            direction=direction, symbol=_SYMBOL, quantity_btc=abs(after_qty),
            entry_price=float(after_position.average_entry_price), entry_time=opened_at,
            setup_tag="recovered/paper-position", fee_costs=float(getattr(after_position, "fees_paid", 0.0)),
            stop_price=getattr(after_position, "stop_price", None),
            target_price=getattr(after_position, "target_price", None),
            broker_order_id=f"recovered:{_SYMBOL}:{opened_at}",
            metadata={"recovered_from_broker_snapshot": True})
        open_trade = await server.DB.open_trade_for_symbol(_SYMBOL)

    # Close a journal row only when an actual position-to-flat transition occurred.
    # P&L is broker realized P&L minus commissions accrued during this transition.
    if open_trade and abs(before_qty) > 1e-12 and abs(after_qty) <= 1e-12:
        fills_before_ids = {f.order_id + ":" + str(f.timestamp) for f in before_fills}
        new_fills = [f for f in after_fills if f.order_id + ":" + str(f.timestamp) not in fills_before_ids
                     and f.symbol == _SYMBOL]
        exit_fill = new_fills[-1] if new_fills else None
        gross_realized = float(after_balance.realized_pnl) - float(before_balance.realized_pnl)
        new_fees = sum(float(f.commission) for f in new_fills)
        net_realized = gross_realized - new_fees - float(open_trade.get("fee_costs", 0.0))
        exit_price = float(exit_fill.price) if exit_fill else float(price)
        total_fees = float(open_trade.get("fee_costs", 0.0)) + new_fees
        closed = await server.DB.close_trade(
            open_trade["id"], exit_price=exit_price, realized_pnl_usd=net_realized,
            exit_time=exit_fill.timestamp if exit_fill else timestamp,
            fee_costs=total_fees,
            slippage_incurred=float(open_trade.get("slippage_incurred", 0.0)) +
                sum(abs(float(f.slippage_price)) * float(f.quantity_btc) for f in new_fills),
            metadata={"exit_order_id": exit_fill.order_id if exit_fill else None,
                      "exit_reason": exit_reason_override or next((o.reason for o in reversed(await server.BROKER.get_orders(_SYMBOL))
                                            if exit_fill and o.order_id == exit_fill.order_id), "position became flat")})
        await server.broadcast({"type": "trade_closed", "trade": closed})
        await server.DB.add_event("journal_reconciled_close", closed)


async def _strategy_after_event(event: dict[str, Any]) -> None:
    """Handle feed event, reconcile fills, and evaluate once per closed 1-minute candle."""
    global _last_eval_bar
    is_trade = event.get("type") == "trade"
    before_position = before_balance = before_fills = None
    if is_trade:
        before_position = await server.BROKER.get_position(_SYMBOL)
        before_balance = await server.BROKER.get_balance()
        before_fills = await server.BROKER.get_fills()

    await _original_feed_handler(event)

    if is_trade:
        price_now = float(event.get("price") or server.STATE.get("price") or 0.0)
        after_position = await server.BROKER.get_position(_SYMBOL)
        after_balance = await server.BROKER.get_balance()
        after_fills = await server.BROKER.get_fills()
        await _journal_position_transition(
            before_position=before_position, before_balance=before_balance, before_fills=before_fills,
            after_position=after_position, after_balance=after_balance, after_fills=after_fills,
            timestamp=float(event.get("timestamp") or time.time()), price=price_now)

        # Experimental scalp time stop: close only tagged scalp positions after the
        # configured maximum hold. This remains a paper-broker reduce-only order.
        pos_for_time_exit = await server.BROKER.get_position(_SYMBOL)
        open_trade_for_time_exit = await server.DB.open_trade_for_symbol(_SYMBOL)
        opened_at = getattr(pos_for_time_exit, "opened_at", None)
        if (abs(_position_qty(pos_for_time_exit)) > 1e-12 and open_trade_for_time_exit
                and open_trade_for_time_exit.get("setup_tag") == SCALP_TAG and opened_at
                and time.time() - float(opened_at) >= SCALP_MAX_HOLD_SECONDS):
            try:
                exit_before_position = pos_for_time_exit
                exit_before_balance = await server.BROKER.get_balance()
                exit_before_fills = await server.BROKER.get_fills()
                exit_side = OrderSide.SELL if _position_qty(pos_for_time_exit) > 0 else OrderSide.BUY
                exit_order = Order(
                    symbol=_SYMBOL, side=exit_side,
                    quantity_btc=abs(_position_qty(pos_for_time_exit)),
                    order_type=OrderType.MARKET, reduce_only=True,
                    client_order_id=f"scalp-time-exit-{int(time.time())}")
                exit_result = await server.BROKER.place_order(exit_order)
                if exit_result.status.value == "filled":
                    exit_after_position = await server.BROKER.get_position(_SYMBOL)
                    exit_after_balance = await server.BROKER.get_balance()
                    exit_after_fills = await server.BROKER.get_fills()
                    await _journal_position_transition(
                        before_position=exit_before_position, before_balance=exit_before_balance,
                        before_fills=exit_before_fills, after_position=exit_after_position,
                        after_balance=exit_after_balance, after_fills=exit_after_fills,
                        timestamp=time.time(), price=float(event.get("price") or server.STATE.get("price") or 0.0),
                        exit_reason_override=f"scalp max holding time ({SCALP_MAX_HOLD_SECONDS:.0f}s)")
                    log.info("Paper scalp time-exit submitted after %.0fs", SCALP_MAX_HOLD_SECONDS)
                else:
                    log.warning("Paper scalp time-exit not filled: %s", getattr(exit_result, "reason", "unknown reason"))
            except Exception:
                log.exception("Paper scalp time-exit failed")

    if not is_trade:
        return
    price = server.STATE.get("price")
    if not price:
        return
    closed_candles = server.STATE.get("candles", [])
    if not closed_candles:
        return
    candle = closed_candles[-1]
    candle_time = candle.get("time", candle.get("timestamp"))
    if candle_time is None or candle_time == _last_eval_bar:
        return
    try:
        now = time.time()
        bars = server.FOOTPRINT.bars(now, include_open_bar=False)
        if not bars:
            # Do not mark this candle evaluated until its required footprint data exists.
            return
        delta_bar = bars[-1]
        # If the footprint implementation exposes a timestamp, ensure it aligns to
        # the latest completed 1m candle. Older implementations without a timestamp
        # remain supported; delta and candle data are still required to be present.
        candle_ts = _candle_ts(candle)
        footprint_ts = None
        for attr in ("start_time", "start_ts", "timestamp", "time", "bar_start"):
            raw_ts = getattr(delta_bar, attr, None)
            if raw_ts is None and isinstance(delta_bar, dict):
                raw_ts = delta_bar.get(attr)
            if raw_ts is not None:
                try:
                    footprint_ts = float(raw_ts.timestamp() if hasattr(raw_ts, "timestamp") else raw_ts)
                    if footprint_ts > 10_000_000_000:
                        footprint_ts /= 1000.0
                    break
                except (TypeError, ValueError, OSError):
                    footprint_ts = None
        if candle_ts is not None and footprint_ts is not None and int(candle_ts // 60) != int(footprint_ts // 60):
            log.debug("Skipping scalp/strategy evaluation: footprint minute does not align with candle")
            return
        pos = await server.BROKER.get_position(_SYMBOL)
        gamma = server.STATE.get("gamma")
        gamma_updated_at = server.STATE.get("gamma_updated_at")
        gamma_max_age = max(90.0, float(os.getenv("DERIBIT_OPTION_POLL_SECONDS", "30")) * 3.0)
        if (not gamma_updated_at or time.time() - float(gamma_updated_at) > gamma_max_age):
            gamma = None  # stale/absent option snapshots must never authorize a signal
        walls = server.STATE.get("walls") or []
        all_candles = [dict(c, closed=True) for c in closed_candles]
        candles = all_candles[-20:]
        trend_candles = _five_minute_candles(all_candles, now)
        signal = ENGINE.evaluate(price=float(price), gamma=gamma, walls=walls,
            delta_bar=delta_bar, candles=candles, sr_zones=[], now=now,
            has_open_position=abs(_position_qty(pos)) > 1e-12)
        if signal.side == SignalSide.WAIT and _SCALP_ON:
            scalp = SCALP.evaluate(price=float(price), delta_bar=delta_bar, candles=candles,
                                   trend_candles=trend_candles, quantity_btc=_QTY,
                                   now=now, has_open_position=abs(_position_qty(pos)) > 1e-12)
            if scalp.side != SignalSide.WAIT:
                signal = scalp
            else:
                signal = _dc_replace(signal, reason=f"{signal.reason} | Scalp: {scalp.reason}")
        _last_eval_bar = candle_time
        is_scalp = signal.setup_tag == SCALP_TAG
        payload = signal.to_dict()
        server.STATE["last_signal"] = payload
        await server.broadcast({"type": "strategy_signal", "signal": payload})
        await server.DB.add_event("strategy_signal", payload)
        if signal.side == SignalSide.WAIT or not _ENABLED:
            return
        # Fail closed if market structure sources are not actually available/healthy.
        feed_state = server.STATE.get("feed") or {}
        if not feed_state.get("healthy"):
            log.warning("Paper order blocked: market-data feed is not healthy")
            blocked = {"status": "blocked", "reason": "feed unhealthy", "signal": payload}
            server.STATE["last_strategy_order"] = blocked
            await server.broadcast({"type": "strategy_order", "result": blocked})
            await server.DB.add_event("strategy_order_blocked", blocked)
            return
        if not is_scalp and (os.getenv("FEED_EXCHANGE", "deribit").lower() != "deribit" or server.WALL_TRACKER is None):
            log.error("Paper order blocked: strategy trading requires Deribit option data and WALL_STRIKES")
            blocked = {"status": "blocked", "reason": "Deribit option feed and configured WALL_STRIKES are required", "signal": payload}
            server.STATE["last_strategy_order"] = blocked
            await server.broadcast({"type": "strategy_order", "result": blocked})
            await server.DB.add_event("strategy_order_blocked", blocked)
            return
        if not getattr(server.BROKER, "is_paper", False):
            log.error("Refusing to submit: broker does not explicitly identify as paper")
            return
        order = Order(symbol=_SYMBOL,
            side=OrderSide.BUY if signal.side == SignalSide.BUY else OrderSide.SELL,
            quantity_btc=_QTY, order_type=OrderType.MARKET,
            client_order_id=f"strategy-{int(candle_time)}-{signal.side.value.lower()}")
        result = await server.BROKER.place_order(order)
        if result.status.value == "filled" and result.average_fill_price is not None:
            fill_price = float(result.average_fill_price)
            stop_offset = abs(float(signal.price) - float(signal.stop_loss))
            target_offset = abs(float(signal.take_profit) - float(signal.price))
            position = await server.BROKER.get_position(_SYMBOL)
            if abs(_position_qty(position)) > 1e-12:
                stop = fill_price - stop_offset if position.quantity_btc > 0 else fill_price + stop_offset
                target = fill_price + target_offset if position.quantity_btc > 0 else fill_price - target_offset
                position = await server.BROKER.set_position_protection(_SYMBOL, stop_price=stop, target_price=target)
            new_fills = [f for f in await server.BROKER.get_fills() if f.order_id == result.order_id]
            entry_fee = sum(float(f.commission) for f in new_fills)
            entry_slippage = sum(abs(float(f.slippage_price)) * float(f.quantity_btc) for f in new_fills)
            journal = await server.DB.create_trade(
                direction="long" if position.quantity_btc > 0 else "short", symbol=_SYMBOL,
                quantity_btc=abs(float(result.filled_quantity_btc or _QTY)), entry_price=fill_price,
                entry_time=result.updated_at, setup_tag=signal.setup_tag, fee_costs=entry_fee,
                slippage_incurred=entry_slippage, broker_order_id=result.order_id,
                stop_price=position.stop_price, target_price=position.target_price,
                metadata={"signal": payload, "client_order_id": result.client_order_id})
            await server.broadcast({"type": "trade_opened", "trade": journal})
        order_payload = {"signal": payload, "order": to_dict(result)}
        server.STATE["last_strategy_order"] = order_payload
        await server.broadcast({"type": "strategy_order", **order_payload})
        await server.DB.add_event("strategy_order", order_payload)
        await server.refresh_account(float(price))
    except Exception:
        log.exception("Strategy evaluation/reconciliation failed")


async def run() -> None:
    if not getattr(server.BROKER, "is_paper", False):
        raise RuntimeError("Safety check failed: orchestrator requires PaperBroker")
    if _QTY <= 0:
        raise ValueError("PAPER_ORDER_BTC must be > 0")
    if _ENABLED and (os.getenv("FEED_EXCHANGE", "deribit").lower() != "deribit" or server.WALL_TRACKER is None):
        log.warning("Big-move (wall) strategy inactive: needs FEED_EXCHANGE=deribit and WALL_STRIKES. Scalp mode still runs.")
    server.on_feed_event = _strategy_after_event
    config = uvicorn.Config(server.app, host=os.getenv("HOST", "0.0.0.0"),
        port=int(os.getenv("PORT", "8000")), log_level=os.getenv("LOG_LEVEL", "info"),
        lifespan="on", access_log=True)
    app_server = uvicorn.Server(config)
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, lambda s=sig: setattr(app_server, "should_exit", True))
        except (NotImplementedError, RuntimeError):
            pass
    await app_server.serve()

if __name__ == "__main__":
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        pass
