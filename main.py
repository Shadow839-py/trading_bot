"""Orchestrator entry point for the paper-only BTC dashboard and strategy.

Run: python main.py
Runs the big-move confluence strategy and a scalp strategy together (scalp only fires
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


class ScalpEngine:
    """Fast paper-scalping rule: 1-minute footprint delta + confirming closed candle.

    Needs no gamma or wall data. Stop is max(min_stop, 1.5 x recent average candle
    range); target is exactly 2x the stop (same 1:2 as the big-move strategy).
    """
    def __init__(self, symbol, min_delta_btc=0.2, min_delta_pct=15.0,
                 min_stop=120.0, range_mult=1.5, cooldown=120.0):
        self.symbol, self.min_delta_btc, self.min_delta_pct = symbol, min_delta_btc, min_delta_pct
        self.min_stop, self.range_mult, self.cooldown = min_stop, range_mult, cooldown
        self._last = 0.0

    def evaluate(self, *, price, delta_bar, candles, now, has_open_position):
        def wait(why):
            return StrategySignal(SignalSide.WAIT, now, self.symbol, price, 0, "NONE", why)
        if has_open_position:
            return wait("Position already open")
        if now - self._last < self.cooldown:
            return wait("Cooldown")
        if delta_bar is None or len(candles) < 6:
            return wait("Not enough data")
        total, delta = delta_bar.total_volume, delta_bar.delta
        if not total:
            return wait("No volume")
        pct = delta / total * 100.0
        if abs(delta) < self.min_delta_btc or abs(pct) < self.min_delta_pct:
            return wait("Delta too weak")
        last, prev = candles[-1], candles[-2]
        up = delta > 0
        if up and not (last["close"] > last["open"] and last["close"] > prev["close"]):
            return wait("Candle does not confirm buy delta")
        if not up and not (last["close"] < last["open"] and last["close"] < prev["close"]):
            return wait("Candle does not confirm sell delta")
        avg = sum(c["high"] - c["low"] for c in candles[-6:-1]) / 5.0
        risk = max(self.min_stop, avg * self.range_mult)
        reward = risk * 2.0
        stop = price - risk if up else price + risk
        target = price + reward if up else price - reward
        self._last = now
        return StrategySignal(SignalSide.BUY if up else SignalSide.SELL, now, self.symbol, price, 3,
            SCALP_TAG, "Footprint delta and closed candle agree (scalp)", stop, target, risk, reward,
            {"delta_btc": delta, "delta_percent": pct})

ENGINE = ConfluenceEngine(StrategyConfig(symbol=_SYMBOL))
_SCALP_ON = os.getenv("SCALP_ENABLED", "true").lower() in {"1", "true", "yes"}
SCALP = ScalpEngine(_SYMBOL, min_stop=float(os.getenv("SCALP_MIN_STOP", "120")))
_original_feed_handler = server.on_feed_event
_last_eval_bar = None


def _position_qty(position) -> float:
    return float(getattr(position, "quantity_btc", 0.0) or 0.0)


async def _journal_position_transition(*, before_position, before_balance, before_fills,
                                       after_position, after_balance, after_fills,
                                       timestamp: float, price: float) -> None:
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
                      "exit_reason": next((o.reason for o in reversed(await server.BROKER.get_orders(_SYMBOL))
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
        signal = ENGINE.evaluate(price=float(price), gamma=gamma, walls=walls,
            delta_bar=delta_bar, candles=candles, sr_zones=[], now=now,
            has_open_position=abs(_position_qty(pos)) > 1e-12)
        if signal.side == SignalSide.WAIT and _SCALP_ON:
            scalp = SCALP.evaluate(price=float(price), delta_bar=delta_bar, candles=candles,
                                   now=now, has_open_position=abs(_position_qty(pos)) > 1e-12)
            if scalp.side != SignalSide.WAIT:
                signal = scalp
            else:
                signal = _dc_replace(signal, reason=f"{signal.reason} | Scalp: {scalp.reason}")
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
