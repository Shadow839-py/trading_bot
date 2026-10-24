"""Orchestrator entry point for the paper-only BTC dashboard and strategy.

Run: python main.py
Runs three engines in priority order, one position at a time: (1) big-move confluence, (2) gamma-level
bounce/rejection (levels.py; levels from Deribit's own greeks, walls from the Coinbase book), (3) scalp
(off by default). Paper only. Set ENABLE_STRATEGY_TRADING=false to stop orders. Even
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
from levels import LevelService, LevelEngine, LEVEL_PREFIX

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
log = logging.getLogger("btc-orchestrator")
_SYMBOL = os.getenv("TRADING_SYMBOL", "BTCUSDT")
_QTY = float(os.getenv("PAPER_ORDER_BTC", "0.01"))
_ENABLED = os.getenv("ENABLE_STRATEGY_TRADING", "true").lower() in {"1", "true", "yes"}

SCALP_PREFIX = "SCALP_"


def _ema(values, n):
    k, e = 2.0 / (n + 1.0), values[0]
    for v in values[1:]:
        e = v * k + e * (1.0 - k)
    return e


class ScalpEngine:
    """Scalp engine v2 (paper only). Two setups, both tagged so the journal can compare them:

    SCALP_V2_PULLBACK - with the 1-minute trend, price dipped to the fast EMA and a candle with
                        matching footprint delta closes back in the trend direction.
    SCALP_V2_MOMO     - footprint delta + confirming candle, but only with the trend (or when
                        there is no trend), never when price is already extended, and never on
                        delta spikes (in the first 40-trade journal, delta >= 40% or >= 8 BTC
                        won 0 of 8: those looked like exhaustion, not continuation).

    Risk: stop = max(min_stop, 2 x ATR14) capped at max_stop; target = 2 x stop.
    Protection: 2 min pause after a win, 5 min after a loss, 30 min after 3 losses in a row.
    """
    def __init__(self, symbol, min_delta_btc=0.2, min_delta_pct=12.0, min_stop=120.0,
                 max_stop=300.0, atr_mult=2.0, cooldown=120.0, loss_cooldown=300.0,
                 streak_limit=3, streak_pause=1800.0):
        self.symbol, self.min_delta_btc, self.min_delta_pct = symbol, min_delta_btc, min_delta_pct
        self.min_stop, self.max_stop, self.atr_mult = min_stop, max_stop, atr_mult
        self.cooldown, self.loss_cooldown = cooldown, loss_cooldown
        self.streak_limit, self.streak_pause = streak_limit, streak_pause
        self._last_exit, self._last_was_loss = 0.0, False
        self._losses, self._pause_until = 0, 0.0

    def record_result(self, net_pnl, timestamp):
        self._last_exit, self._last_was_loss = float(timestamp), net_pnl < 0
        if net_pnl < 0:
            self._losses += 1
            if self._losses >= self.streak_limit:
                self._pause_until, self._losses = float(timestamp) + self.streak_pause, 0
        else:
            self._losses = 0

    def evaluate(self, *, price, delta_bar, candles, now, has_open_position):
        def wait(why):
            return StrategySignal(SignalSide.WAIT, now, self.symbol, price, 0, "NONE", why)
        if has_open_position:
            return wait("Position already open")
        if now < self._pause_until:
            return wait(f"Loss-streak pause ({int((self._pause_until - now) / 60) + 1} min left)")
        gap = self.loss_cooldown if self._last_was_loss else self.cooldown
        if now - self._last_exit < gap:
            return wait("Cooldown after last trade")
        if delta_bar is None or len(candles) < 35:
            return wait("Warming up (needs 35 closed candles)")
        total, delta = delta_bar.total_volume, delta_bar.delta
        if not total:
            return wait("No volume")
        pct = delta / total * 100.0
        vols = [float(c.get("volume") or 0.0) for c in candles[-20:]]
        avg_vol = sum(vols) / len(vols)
        cap_btc = max(4.0, 0.8 * avg_vol) if avg_vol > 0 else 8.0
        if abs(delta) >= cap_btc or abs(pct) >= 40.0:
            return wait("Delta spike: exhaustion risk")
        if abs(delta) < self.min_delta_btc or abs(pct) < self.min_delta_pct:
            return wait("Delta too weak")
        closes = [float(c["close"]) for c in candles[-60:]]
        trs = [max(c["high"] - c["low"], abs(c["high"] - p["close"]), abs(c["low"] - p["close"]))
               for p, c in zip(candles[-15:-1], candles[-14:])]
        atr = max(sum(trs) / len(trs), 1.0)
        ema_f, ema_s = _ema(closes, 10), _ema(closes, 30)
        trend = 1 if ema_f - ema_s > 0.25 * atr else (-1 if ema_f - ema_s < -0.25 * atr else 0)
        d = 1 if delta > 0 else -1
        if trend and trend != d:
            return wait("Counter-trend")
        last, prev = candles[-1], candles[-2]
        if (last["close"] - last["open"]) * d <= 0:
            return wait("Candle does not confirm delta")
        if abs(price - ema_f) > 1.5 * atr:
            return wait("Price extended from EMA")
        if abs(last["close"] - last["open"]) > 2.2 * atr:
            return wait("Blow-off candle")
        recent = candles[-4:-1]
        swing = min(c["low"] for c in recent) if d > 0 else max(c["high"] for c in recent)
        touched = (swing <= ema_f + 0.25 * atr) if d > 0 else (swing >= ema_f - 0.25 * atr)
        if trend == d and touched and (last["close"] - ema_f) * d > 0:
            tag, conf = "SCALP_V2_PULLBACK", 4
            raw_risk = abs(price - swing) + 0.5 * atr
        elif (last["close"] - prev["close"]) * d > 0:
            tag, conf = "SCALP_V2_MOMO", 3
            raw_risk = self.atr_mult * atr
        else:
            return wait("No setup")
        risk = min(self.max_stop, max(self.min_stop, raw_risk))
        reward = risk * 2.0
        stop = price - d * risk
        target = price + d * reward
        return StrategySignal(SignalSide.BUY if d > 0 else SignalSide.SELL, now, self.symbol, price,
            conf, tag, f"{tag}: delta {delta:+.1f} BTC ({pct:+.0f}%), trend {trend:+d}, ATR {atr:.0f}",
            stop, target, risk, reward,
            {"delta_btc": delta, "delta_percent": pct, "atr": atr, "trend": trend,
             "ema_gap": ema_f - ema_s})


ENGINE = ConfluenceEngine(StrategyConfig(symbol=_SYMBOL))
_SCALP_ON = os.getenv("SCALP_ENABLED", "false").lower() in {"1", "true", "yes"}
SCALP = ScalpEngine(_SYMBOL, min_stop=float(os.getenv("SCALP_MIN_STOP", "120")))
_LEVELS_ON = os.getenv("LEVELS_ENABLED", "true").lower() in {"1", "true", "yes"}
LEVELS = LevelService(on_update=lambda snap: server.STATE.__setitem__("levels", snap))
LEVEL_ENGINE = LevelEngine(LEVELS, _SYMBOL)
LEVEL_MAX_HOLD = max(600.0, float(os.getenv("LEVEL_MAX_HOLD_SECONDS", "14400")))
_LEVEL_OPEN: dict | None = None
_level_exit_checked = 0.0
_original_feed_handler = server.on_feed_event
_last_eval_bar = None


def _position_qty(position) -> float:
    return float(getattr(position, "quantity_btc", 0.0) or 0.0)


async def _journal_position_transition(*, before_position, before_balance, before_fills,
                                       after_position, after_balance, after_fills,
                                       timestamp: float, price: float,
                                       exit_reason_override: str | None = None) -> None:
    """Reconcile broker position transitions to the journal, using actual paper fills."""
    global _LEVEL_OPEN
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
        if str(open_trade.get("setup_tag", "")).startswith(SCALP_PREFIX):
            SCALP.record_result(net_realized, float(exit_fill.timestamp if exit_fill else timestamp))
        if str(open_trade.get("setup_tag", "")).startswith(LEVEL_PREFIX):
            ev = ((open_trade.get("metadata") or {}).get("signal") or {}).get("evidence") or {}
            LEVEL_ENGINE.record_result(net_realized, float(exit_fill.timestamp if exit_fill else timestamp), ev.get("level_k"))
            _LEVEL_OPEN = None
        await server.DB.add_event("journal_reconciled_close", closed)


async def _level_exit_check() -> None:
    """Close a level trade early if its wall was pulled/eaten (the reason for the trade is gone) or after the max hold."""
    global _LEVEL_OPEN, _level_exit_checked
    if not _LEVEL_OPEN or time.time() - _level_exit_checked < 2.0:
        return
    _level_exit_checked = time.time()
    pos = await server.BROKER.get_position(_SYMBOL)
    qty = _position_qty(pos)
    if abs(qty) <= 1e-12:
        _LEVEL_OPEN = None
        return
    reason = None
    t = LEVELS.tier(_LEVEL_OPEN["K"])
    if t.get("tier") == "bad":
        reason = f"level wall invalidated ({t.get('why', 'wall gone')})"
    elif time.time() - _LEVEL_OPEN["opened"] >= LEVEL_MAX_HOLD:
        reason = f"level max holding time ({LEVEL_MAX_HOLD:.0f}s)"
    if not reason:
        return
    try:
        before_balance = await server.BROKER.get_balance()
        before_fills = await server.BROKER.get_fills()
        exit_order = Order(symbol=_SYMBOL, side=OrderSide.SELL if qty > 0 else OrderSide.BUY,
                           quantity_btc=abs(qty), order_type=OrderType.MARKET, reduce_only=True,
                           client_order_id=f"level-exit-{int(time.time())}")
        result = await server.BROKER.place_order(exit_order)
        if result.status.value == "filled":
            await _journal_position_transition(
                before_position=pos, before_balance=before_balance, before_fills=before_fills,
                after_position=await server.BROKER.get_position(_SYMBOL),
                after_balance=await server.BROKER.get_balance(), after_fills=await server.BROKER.get_fills(),
                timestamp=time.time(), price=float(server.STATE.get("price") or 0.0), exit_reason_override=reason)
            log.info("Paper level trade closed early: %s", reason)
        else:
            log.warning("Level early-exit not filled: %s", getattr(result, "reason", "unknown"))
    except Exception:
        log.exception("Level early-exit failed")


async def _strategy_after_event(event: dict[str, Any]) -> None:
    """Handle feed event, reconcile fills, and evaluate once per closed 1-minute candle."""
    global _last_eval_bar, _LEVEL_OPEN
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
        await _level_exit_check()

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
    _last_eval_bar = candle_time
    try:
        now = time.time()
        bars = server.FOOTPRINT.bars(now, include_open_bar=False)
        if not bars:
            return
        delta_bar = bars[-1]
        pos = await server.BROKER.get_position(_SYMBOL)
        gamma = server.STATE.get("gamma")
        gamma_updated_at = server.STATE.get("gamma_updated_at")
        gamma_max_age = max(90.0, float(os.getenv("DERIBIT_OPTION_POLL_SECONDS", "30")) * 3.0)
        if (not gamma_updated_at or time.time() - float(gamma_updated_at) > gamma_max_age):
            gamma = None  # stale/absent option snapshots must never authorize a signal
        walls = server.STATE.get("walls") or []
        candles = [dict(c, closed=True) for c in closed_candles[-20:]]
        scalp_candles = [dict(c, closed=True) for c in closed_candles[-80:]]
        signal = ENGINE.evaluate(price=float(price), gamma=gamma, walls=walls,
            delta_bar=delta_bar, candles=candles, sr_zones=[], now=now,
            has_open_position=abs(_position_qty(pos)) > 1e-12)
        if _LEVELS_ON:
            LEVELS.touches.on_candle(LEVELS, closed_candles[-1], float(price), now)
            if signal.side == SignalSide.WAIT:
                lv = LEVEL_ENGINE.evaluate(price=float(price), delta_bar=delta_bar, candles=candles, now=now,
                                           has_open_position=abs(_position_qty(pos)) > 1e-12)
                if lv.side != SignalSide.WAIT:
                    signal = lv
                else:
                    signal = _dc_replace(signal, reason=f"{signal.reason} | Levels: {lv.reason}")
        if signal.side == SignalSide.WAIT and _SCALP_ON:
            scalp = SCALP.evaluate(price=float(price), delta_bar=delta_bar, candles=scalp_candles,
                                   now=now, has_open_position=abs(_position_qty(pos)) > 1e-12)
            if scalp.side != SignalSide.WAIT:
                signal = scalp
            else:
                signal = _dc_replace(signal, reason=f"{signal.reason} | Scalp: {scalp.reason}")
        is_scalp = str(signal.setup_tag).startswith(SCALP_PREFIX)
        is_level = str(signal.setup_tag).startswith(LEVEL_PREFIX)
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
        if not (is_scalp or is_level) and (os.getenv("FEED_EXCHANGE", "deribit").lower() != "deribit" or server.WALL_TRACKER is None):
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
            if is_level and (signal.evidence or {}).get("level_k") is not None:
                _LEVEL_OPEN = {"K": float(signal.evidence["level_k"]), "opened": time.time()}
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
        log.warning("Big-move (wall) strategy inactive: needs FEED_EXCHANGE=deribit and WALL_STRIKES. The level engine is independent of that.")
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
    if _LEVELS_ON:
        LEVELS.start()
    try:
        await app_server.serve()
    finally:
        if _LEVELS_ON:
            await LEVELS.stop()

if __name__ == "__main__":
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        pass
