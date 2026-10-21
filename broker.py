"""
Deterministic, in-memory paper broker for BTC spot-style quantity accounting.

Safety properties:
- There is no live exchange adapter or credential handling in this module.
- BrokerBase is abstract; PaperBroker is the only implementation supplied.
- Orders are simulated from explicitly supplied market quotes/trade ticks.
- Market fills include half-spread + configurable slippage and commissions.
- State transitions are protected by an asyncio lock.
- Client order IDs are idempotent: duplicate submissions return the existing order.
- Risk defaults are $500 stop distance and $1,000 target distance per BTC price unit.
  These are price distances, not guaranteed dollar P&L; P&L scales with position size.
- Stop moves to entry when unrealized profit reaches $500 (account-currency P&L).
  A break-even stop cannot be assumed to recover fees/slippage, so this is price break-even.

This is paper simulation, not a prediction of actual fills or profitability.
"""
from __future__ import annotations

import asyncio
import math
import time
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, field, asdict
from enum import Enum
from typing import Optional


class BrokerError(RuntimeError):
    pass


class OrderValidationError(BrokerError):
    pass


class InsufficientFunds(BrokerError):
    pass


class OrderSide(str, Enum):
    BUY = "buy"
    SELL = "sell"


class OrderType(str, Enum):
    MARKET = "market"
    LIMIT = "limit"
    STOP_MARKET = "stop_market"


class OrderStatus(str, Enum):
    PENDING = "pending"
    OPEN = "open"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    CANCELED = "canceled"
    REJECTED = "rejected"


@dataclass
class Order:
    symbol: str
    side: OrderSide
    quantity_btc: float
    order_type: OrderType = OrderType.MARKET
    limit_price: Optional[float] = None
    stop_price: Optional[float] = None
    reduce_only: bool = False
    client_order_id: str = ""
    order_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    status: OrderStatus = OrderStatus.PENDING
    filled_quantity_btc: float = 0.0
    average_fill_price: Optional[float] = None
    commission_paid: float = 0.0
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    reason: str = ""


@dataclass
class Position:
    symbol: str
    quantity_btc: float = 0.0  # positive long, negative short
    average_entry_price: float = 0.0
    realized_pnl: float = 0.0
    fees_paid: float = 0.0
    stop_price: Optional[float] = None
    target_price: Optional[float] = None
    break_even_trailed: bool = False
    opened_at: Optional[float] = None
    updated_at: float = field(default_factory=time.time)

    def unrealized_pnl(self, mark_price: float) -> float:
        if self.quantity_btc == 0:
            return 0.0
        return self.quantity_btc * (mark_price - self.average_entry_price)


@dataclass(frozen=True)
class Fill:
    order_id: str
    symbol: str
    side: str
    quantity_btc: float
    price: float
    commission: float
    liquidity: str
    timestamp: float
    slippage_price: float


@dataclass(frozen=True)
class Balance:
    currency: str
    cash: float
    equity: float
    unrealized_pnl: float
    realized_pnl: float
    fees_paid: float


@dataclass
class BrokerConfig:
    starting_cash: float = 100_000.0
    spread_bps: float = 2.0
    slippage_bps: float = 1.0
    maker_fee_rate: float = 0.0002
    taker_fee_rate: float = 0.0005
    stop_distance_usd: float = 500.0
    target_distance_usd: float = 1_000.0
    break_even_trigger_pnl_usd: float = 500.0
    max_position_btc: float = 1.0
    max_order_btc: float = 1.0
    allow_short: bool = True
    currency: str = "USD"
    require_market_data: bool = True

    def validate(self) -> None:
        positive = ("starting_cash", "max_position_btc", "max_order_btc",
                    "stop_distance_usd", "target_distance_usd", "break_even_trigger_pnl_usd")
        for name in positive:
            value = float(getattr(self, name))
            if not math.isfinite(value) or value <= 0:
                raise ValueError(f"{name} must be finite and > 0")
        for name in ("spread_bps", "slippage_bps", "maker_fee_rate", "taker_fee_rate"):
            value = float(getattr(self, name))
            if not math.isfinite(value) or value < 0:
                raise ValueError(f"{name} must be finite and >= 0")
        if abs(self.target_distance_usd / self.stop_distance_usd - 2.0) > 1e-9:
            raise ValueError("Strict 1:2 risk/reward requires target_distance_usd == 2 * stop_distance_usd")


class BrokerBase(ABC):
    """Standard async broker contract. Implementations must declare paper/live status."""

    is_paper: bool

    @abstractmethod
    async def place_order(self, order: Order) -> Order: ...

    @abstractmethod
    async def cancel_order(self, order_id: str) -> Order: ...

    @abstractmethod
    async def modify_order(self, order_id: str, **changes) -> Order: ...

    @abstractmethod
    async def get_position(self, symbol: str) -> Position: ...

    @abstractmethod
    async def get_balance(self) -> Balance: ...


class PaperBroker(BrokerBase):
    """
    Thread-safe async paper broker. No network connection and no live execution path.

    Market updates are supplied with `update_market(price, bid, ask, timestamp)`.
    Open limit orders fill when a later quote crosses their limit. Limit fills are
    simulated as maker fills at the limit price (no queue-position model). Stop-market
    and protective position stops fill as taker orders with slippage.
    """

    is_paper = True

    def __init__(self, config: BrokerConfig | None = None):
        self.config = config or BrokerConfig()
        self.config.validate()
        self._lock = asyncio.Lock()
        self._orders: dict[str, Order] = {}
        self._client_ids: dict[str, str] = {}
        self._fills: list[Fill] = []
        self._positions: dict[str, Position] = {}
        self._mark_prices: dict[str, float] = {}
        self._quotes: dict[str, tuple[float, float]] = {}
        self._cash = float(self.config.starting_cash)
        self._realized_pnl = 0.0
        self._fees_paid = 0.0
        self._halted = False

    @property
    def halted(self) -> bool:
        return self._halted

    @property
    def fills(self) -> tuple[Fill, ...]:
        return tuple(self._fills)

    @property
    def orders(self) -> tuple[Order, ...]:
        return tuple(self._orders.values())

    async def set_trading_halt(self, halted: bool, reason: str = "") -> None:
        async with self._lock:
            self._halted = bool(halted)
            if reason:
                # Kept as broker state for observability; no orders are silently erased.
                self._halt_reason = reason

    async def update_market(self, symbol: str, price: float, bid: float | None = None,
                            ask: float | None = None, timestamp: float | None = None) -> None:
        """Update simulated market and trigger limit/protective orders."""
        price = self._positive("price", price)
        timestamp = time.time() if timestamp is None else float(timestamp)
        if not math.isfinite(timestamp) or timestamp <= 0:
            raise ValueError("timestamp must be finite and > 0")
        bid = price if bid is None else self._positive("bid", bid)
        ask = price if ask is None else self._positive("ask", ask)
        if bid > ask:
            raise ValueError("bid must be <= ask")
        async with self._lock:
            self._mark_prices[symbol] = price
            self._quotes[symbol] = (bid, ask)
            await self._process_open_orders_locked(symbol, timestamp)
            await self._manage_position_locked(symbol, price, bid, ask, timestamp)

    async def place_order(self, order: Order) -> Order:
        async with self._lock:
            # Idempotency check comes before risk validation: a retry of an already
            # accepted client ID must return its original order, not place a second one
            # or fail because the original order changed the current position.
            if order.client_order_id:
                existing_id = self._client_ids.get(order.client_order_id)
                if existing_id:
                    return self._copy_order(self._orders[existing_id])
            if order.order_id in self._orders:
                return self._copy_order(self._orders[order.order_id])
            if self._halted:
                raise BrokerError(f"Trading halted: {getattr(self, '_halt_reason', 'unspecified')}")
            self._validate_order(order)
            order = self._copy_order(order)
            order.status = OrderStatus.OPEN
            order.created_at = time.time()
            order.updated_at = order.created_at
            self._orders[order.order_id] = order
            if order.client_order_id:
                self._client_ids[order.client_order_id] = order.order_id
            if order.order_type == OrderType.MARKET:
                mark = self._mark_prices.get(order.symbol)
                quote = self._quotes.get(order.symbol)
                if mark is None or quote is None:
                    if self.config.require_market_data:
                        order.status = OrderStatus.REJECTED
                        order.reason = "No market quote received; order rejected fail-closed."
                        order.updated_at = time.time()
                        return self._copy_order(order)
                    raise BrokerError("Market quote is required to simulate a fill")
                self._fill_locked(order, mark, quote[0], quote[1], "taker", time.time())
            elif order.order_type == OrderType.LIMIT:
                self._try_limit_fill_locked(order, time.time())
            # Stop-market orders remain OPEN until a subsequent market update crosses
            # their trigger; they must never fill immediately merely because they were placed.
            return self._copy_order(order)

    async def cancel_order(self, order_id: str) -> Order:
        async with self._lock:
            order = self._get_order(order_id)
            if order.status in {OrderStatus.FILLED, OrderStatus.CANCELED, OrderStatus.REJECTED}:
                return self._copy_order(order)
            order.status = OrderStatus.CANCELED
            order.updated_at = time.time()
            return self._copy_order(order)

    async def modify_order(self, order_id: str, **changes) -> Order:
        allowed = {"limit_price", "stop_price", "quantity_btc", "reduce_only"}
        if not changes or any(k not in allowed for k in changes):
            raise OrderValidationError(f"modify supports only {sorted(allowed)}")
        async with self._lock:
            order = self._get_order(order_id)
            if order.status not in {OrderStatus.OPEN, OrderStatus.PARTIALLY_FILLED}:
                raise BrokerError(f"Cannot modify order in {order.status.value} state")
            candidate = self._copy_order(order)
            for key, value in changes.items():
                setattr(candidate, key, value)
            self._validate_order(candidate)
            for key, value in changes.items():
                setattr(order, key, value)
            order.updated_at = time.time()
            quote = self._quotes.get(order.symbol)
            if order.order_type == OrderType.LIMIT and quote:
                self._try_limit_fill_locked(order, time.time())
            return self._copy_order(order)

    async def get_position(self, symbol: str) -> Position:
        async with self._lock:
            return self._copy_position(self._positions.get(symbol, Position(symbol)))

    async def get_balance(self) -> Balance:
        async with self._lock:
            upnl = sum(p.unrealized_pnl(self._mark_prices.get(s, p.average_entry_price))
                       for s, p in self._positions.items())
            return Balance(self.config.currency, self._cash, self._cash + upnl, upnl,
                           self._realized_pnl, self._fees_paid)

    async def get_order(self, order_id: str) -> Order:
        async with self._lock:
            return self._copy_order(self._get_order(order_id))

    async def get_orders(self, symbol: str | None = None) -> tuple[Order, ...]:
        async with self._lock:
            rows = self._orders.values()
            if symbol is not None:
                rows = [o for o in rows if o.symbol == symbol]
            return tuple(self._copy_order(o) for o in rows)

    async def get_fills(self) -> tuple[Fill, ...]:
        async with self._lock:
            return tuple(self._fills)

    async def set_position_protection(self, symbol: str, *, stop_price: float, target_price: float) -> Position:
        """Set custom stop/target prices on an existing paper position under the broker lock."""
        stop = self._positive("stop_price", stop_price)
        target = self._positive("target_price", target_price)
        async with self._lock:
            position = self._positions.get(symbol)
            if position is None or abs(position.quantity_btc) <= 1e-12:
                raise BrokerError(f"No open position for {symbol}")
            entry = float(position.average_entry_price)
            if position.quantity_btc > 0 and not (stop < entry < target):
                raise OrderValidationError("Long protection must satisfy stop < average entry < target")
            if position.quantity_btc < 0 and not (target < entry < stop):
                raise OrderValidationError("Short protection must satisfy target < average entry < stop")
            position.stop_price = stop
            position.target_price = target
            position.updated_at = time.time()
            return self._copy_position(position)

    async def _process_open_orders_locked(self, symbol: str, timestamp: float) -> None:
        bid, ask = self._quotes[symbol]
        mark = self._mark_prices[symbol]
        for order in list(self._orders.values()):
            if order.symbol == symbol and order.status in {OrderStatus.OPEN, OrderStatus.PARTIALLY_FILLED}:
                if order.order_type == OrderType.LIMIT:
                    self._try_limit_fill_locked(order, timestamp)
                elif order.order_type == OrderType.STOP_MARKET and order.stop_price is not None:
                    triggered = (mark <= order.stop_price if order.side == OrderSide.SELL
                                 else mark >= order.stop_price)
                    if triggered:
                        self._fill_locked(order, mark, bid, ask, "taker", timestamp)

    def _try_limit_fill_locked(self, order: Order, timestamp: float) -> None:
        quote = self._quotes.get(order.symbol)
        if not quote or order.status not in {OrderStatus.OPEN, OrderStatus.PARTIALLY_FILLED}:
            return
        bid, ask = quote
        if order.limit_price is None:
            return
        crossed = (ask <= order.limit_price if order.side == OrderSide.BUY
                   else bid >= order.limit_price)
        if crossed:
            self._fill_locked(order, order.limit_price, bid, ask, "maker", timestamp,
                              apply_slippage=False)

    def _fill_locked(self, order: Order, reference_price: float, bid: float, ask: float,
                     liquidity: str, timestamp: float, apply_slippage: bool = True) -> None:
        if order.status not in {OrderStatus.OPEN, OrderStatus.PARTIALLY_FILLED}:
            return
        if order.reduce_only:
            pos = self._positions.get(order.symbol, Position(order.symbol))
            if pos.quantity_btc == 0 or (pos.quantity_btc > 0 and order.side != OrderSide.SELL) or (
                pos.quantity_btc < 0 and order.side != OrderSide.BUY
            ):
                order.status = OrderStatus.REJECTED
                order.reason = "reduce_only order would not reduce the current position"
                order.updated_at = timestamp
                return
        qty = order.quantity_btc - order.filled_quantity_btc
        if qty <= 0:
            order.status = OrderStatus.FILLED
            return
        configured_spread = reference_price * self.config.spread_bps / 10_000.0
        observed_spread = max(0.0, ask - bid)
        half_spread = max(observed_spread, configured_spread) / 2.0
        slippage = reference_price * self.config.slippage_bps / 10_000.0 if apply_slippage else 0.0
        if order.side == OrderSide.BUY:
            fill_price = reference_price + (half_spread if liquidity == "taker" else 0.0) + slippage
        else:
            fill_price = reference_price - (half_spread if liquidity == "taker" else 0.0) - slippage
        if fill_price <= 0:
            order.status = OrderStatus.REJECTED
            order.reason = "computed fill price is invalid"
            order.updated_at = timestamp
            return
        fee_rate = self.config.maker_fee_rate if liquidity == "maker" else self.config.taker_fee_rate
        commission = abs(qty * fill_price) * fee_rate
        position = self._positions.setdefault(order.symbol, Position(order.symbol))
        signed_qty = qty if order.side == OrderSide.BUY else -qty
        realized_before = position.realized_pnl
        self._apply_position_fill(position, signed_qty, fill_price, commission, timestamp)
        realized_delta = position.realized_pnl - realized_before
        # This paper account models a cash-settled derivative: notional is not debited,
        # but realized P&L and commissions are reflected in cash.
        self._cash += realized_delta - commission
        self._fees_paid += commission
        self._realized_pnl += realized_delta
        order.filled_quantity_btc += qty
        prior_notional = (order.average_fill_price or 0.0) * (order.filled_quantity_btc - qty)
        order.average_fill_price = (prior_notional + fill_price * qty) / order.filled_quantity_btc
        order.commission_paid += commission
        order.status = OrderStatus.FILLED if order.filled_quantity_btc >= order.quantity_btc - 1e-12 else OrderStatus.PARTIALLY_FILLED
        order.updated_at = timestamp
        self._fills.append(Fill(order.order_id, order.symbol, order.side.value, qty, fill_price,
                                commission, liquidity, timestamp, slippage))

    def _apply_position_fill(self, position: Position, signed_qty: float, price: float,
                             commission: float, timestamp: float) -> None:
        old_qty = position.quantity_btc
        new_qty = old_qty + signed_qty
        if old_qty == 0 or old_qty * signed_qty > 0:
            total = abs(old_qty) + abs(signed_qty)
            position.average_entry_price = (
                (abs(old_qty) * position.average_entry_price + abs(signed_qty) * price) / total
                if total else 0.0
            )
        else:
            closing = min(abs(old_qty), abs(signed_qty))
            direction = 1.0 if old_qty > 0 else -1.0
            position.realized_pnl += closing * (price - position.average_entry_price) * direction
            if abs(signed_qty) > abs(old_qty):
                position.average_entry_price = price
            elif abs(signed_qty) == abs(old_qty):
                position.average_entry_price = 0.0
        position.quantity_btc = new_qty
        position.fees_paid += commission
        position.updated_at = timestamp
        if old_qty == 0 and new_qty != 0:
            position.opened_at = timestamp
            position.break_even_trailed = False
            if new_qty > 0:
                position.stop_price = price - self.config.stop_distance_usd
                position.target_price = price + self.config.target_distance_usd
            else:
                position.stop_price = price + self.config.stop_distance_usd
                position.target_price = price - self.config.target_distance_usd
        elif new_qty == 0:
            position.stop_price = None
            position.target_price = None
            position.break_even_trailed = False
            position.opened_at = None
        elif old_qty * new_qty < 0:
            # A fill crossed through flat into the opposite direction.
            position.stop_price = price - self.config.stop_distance_usd if new_qty > 0 else price + self.config.stop_distance_usd
            position.target_price = price + self.config.target_distance_usd if new_qty > 0 else price - self.config.target_distance_usd
            position.break_even_trailed = False

    async def _manage_position_locked(self, symbol: str, price: float, bid: float, ask: float,
                                      timestamp: float) -> None:
        position = self._positions.get(symbol)
        if not position or position.quantity_btc == 0:
            return
        upnl = position.unrealized_pnl(price)
        if not position.break_even_trailed and upnl >= self.config.break_even_trigger_pnl_usd:
            if position.quantity_btc > 0:
                position.stop_price = max(position.stop_price or 0.0, position.average_entry_price)
            else:
                position.stop_price = min(position.stop_price or math.inf, position.average_entry_price)
            position.break_even_trailed = True
        # If the same market update reaches stop and target, stop is evaluated first
        # (conservative assumption). The exit fill pays taker fees and slippage.
        stop_hit = (price <= position.stop_price if position.quantity_btc > 0
                    else price >= position.stop_price) if position.stop_price is not None else False
        target_hit = (price >= position.target_price if position.quantity_btc > 0
                      else price <= position.target_price) if position.target_price is not None else False
        if stop_hit or target_hit:
            side = OrderSide.SELL if position.quantity_btc > 0 else OrderSide.BUY
            qty = abs(position.quantity_btc)
            order = Order(symbol=symbol, side=side, quantity_btc=qty, order_type=OrderType.MARKET,
                          reduce_only=True, client_order_id=f"protective:{symbol}:{int(timestamp*1000)}")
            order.status = OrderStatus.OPEN
            self._orders[order.order_id] = order
            self._client_ids[order.client_order_id] = order.order_id
            self._fill_locked(order, price, bid, ask, "taker", timestamp)
            order.reason = "protective stop triggered" if stop_hit else "take-profit target triggered"

    def _validate_order(self, order: Order) -> None:
        if not order.symbol or not isinstance(order.symbol, str):
            raise OrderValidationError("symbol must be a non-empty string")
        if not isinstance(order.side, OrderSide):
            try:
                order.side = OrderSide(str(order.side).lower())
            except ValueError as exc:
                raise OrderValidationError("side must be buy or sell") from exc
        if not isinstance(order.order_type, OrderType):
            try:
                order.order_type = OrderType(str(order.order_type).lower())
            except ValueError as exc:
                raise OrderValidationError("order_type must be market, limit, or stop_market") from exc
        qty = self._positive("quantity_btc", order.quantity_btc)
        if qty > self.config.max_order_btc:
            raise OrderValidationError(f"quantity exceeds max_order_btc={self.config.max_order_btc}")
        current = self._positions.get(order.symbol, Position(order.symbol))
        projected = current.quantity_btc + (qty if order.side == OrderSide.BUY else -qty)
        if abs(projected) > self.config.max_position_btc + 1e-12:
            raise OrderValidationError(f"order would exceed max_position_btc={self.config.max_position_btc}")
        if projected < 0 and not self.config.allow_short:
            raise OrderValidationError("short selling is disabled")
        if order.order_type == OrderType.LIMIT:
            if order.limit_price is None:
                raise OrderValidationError("limit orders require limit_price")
            self._positive("limit_price", order.limit_price)
        if order.order_type == OrderType.STOP_MARKET:
            if order.stop_price is None:
                raise OrderValidationError("stop-market orders require stop_price")
            self._positive("stop_price", order.stop_price)
        if order.reduce_only and (current.quantity_btc == 0 or
            (current.quantity_btc > 0 and order.side != OrderSide.SELL) or
            (current.quantity_btc < 0 and order.side != OrderSide.BUY)):
            raise OrderValidationError("reduce_only order does not oppose the current position")

    @staticmethod
    def _positive(name: str, value: float) -> float:
        value = float(value)
        if not math.isfinite(value) or value <= 0:
            raise OrderValidationError(f"{name} must be finite and > 0")
        return value

    def _get_order(self, order_id: str) -> Order:
        try:
            return self._orders[order_id]
        except KeyError as exc:
            raise BrokerError(f"Unknown order_id {order_id!r}") from exc

    @staticmethod
    def _copy_order(order: Order) -> Order:
        return Order(**{k: getattr(order, k) for k in Order.__dataclass_fields__})

    @staticmethod
    def _copy_position(position: Position) -> Position:
        return Position(**{k: getattr(position, k) for k in Position.__dataclass_fields__})


# Backward-friendly concise alias.
Broker = BrokerBase
