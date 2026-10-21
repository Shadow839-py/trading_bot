"""
Asynchronous BTC market-data feed adapters.

Supported public sources:
- Binance Spot: BTCUSDT trades + partial Level-2 depth snapshots.
- Coinbase Advanced Trade: BTC-USD market_trades + level2.
- Deribit: BTC perpetual trades/book, plus periodic public option book-summary polling
  over the same WebSocket JSON-RPC connection.

Requires Python 3.10+ and `websockets` (`pip install websockets`).
No API credentials are needed. This module never sends trading orders.

The feed routes observed trades into analytics.FootprintDelta and, when configured,
analytics.OrderBookWallTracker. Order-book snapshots are explicitly snapshots; Binance
partial depth is not a full exchange order book. Staleness is surfaced via health state
and optional fail-closed callback. Consumers should not trade while unhealthy.
"""
from __future__ import annotations

import asyncio
import json
import logging
import math
import random
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Optional, Sequence

try:
    import websockets
    from websockets.exceptions import ConnectionClosed
except ImportError as exc:  # fail clearly at construction/use, rather than obscure import errors
    websockets = None
    ConnectionClosed = Exception

from analytics import (
    AggressorTrade,
    BookLevel,
    DataQualityError,
    FootprintDelta,
    OptionObservation,
    OrderBookWallTracker,
)

log = logging.getLogger(__name__)

EventCallback = Callable[[dict[str, Any]], Awaitable[None] | None]
HealthCallback = Callable[["FeedHealth"], Awaitable[None] | None]


@dataclass(frozen=True)
class FeedHealth:
    exchange: str
    connected: bool
    healthy: bool
    last_message_age_seconds: Optional[float]
    last_trade_age_seconds: Optional[float]
    last_book_age_seconds: Optional[float]
    reconnect_count: int
    reason: str = ""


@dataclass
class FeedConfig:
    exchange: str = "binance"  # binance, coinbase, deribit
    stale_after_seconds: float = 10.0
    heartbeat_seconds: float = 2.0
    reconnect_min_seconds: float = 1.0
    reconnect_max_seconds: float = 30.0
    reconnect_jitter_fraction: float = 0.2
    depth_levels: int = 20
    deribit_option_poll_seconds: float = 30.0
    deribit_option_expiry_days: float = 60.0
    deribit_option_moneyness_fraction: float = 0.25

    def validate(self) -> None:
        if self.exchange.lower() not in {"binance", "coinbase", "deribit"}:
            raise ValueError("exchange must be binance, coinbase, or deribit")
        for name in ("stale_after_seconds", "heartbeat_seconds", "reconnect_min_seconds",
                     "reconnect_max_seconds", "deribit_option_poll_seconds"):
            value = float(getattr(self, name))
            if not math.isfinite(value) or value <= 0:
                raise ValueError(f"{name} must be finite and > 0")
        if self.reconnect_max_seconds < self.reconnect_min_seconds:
            raise ValueError("reconnect_max_seconds must be >= reconnect_min_seconds")
        if self.depth_levels < 1:
            raise ValueError("depth_levels must be >= 1")


@dataclass
class FeedState:
    connected: bool = False
    last_message_monotonic: Optional[float] = None
    last_trade_monotonic: Optional[float] = None
    last_book_monotonic: Optional[float] = None
    last_exchange_timestamp: Optional[float] = None
    reconnect_count: int = 0
    last_error: str = ""
    healthy: bool = False


def _utc_datetime(epoch_seconds: float) -> datetime:
    return datetime.fromtimestamp(epoch_seconds, tz=timezone.utc)


def _call_callback(callback: Optional[Callable], value):
    if callback is None:
        return None
    try:
        result = callback(value)
        if asyncio.iscoroutine(result):
            return result
    except Exception:
        log.exception("Feed callback failed")
    return None


class AsyncMarketDataFeed:
    """
    One exchange feed. Call `await start()` and later `await stop()`.

    `on_event` receives normalized dictionaries:
      trade: {type, exchange, symbol, timestamp, price, size_btc, side, raw}
      book:  {type, exchange, symbol, timestamp, bids, asks, raw}
      option: {type, exchange, symbol, timestamp, observation, raw}
      health: {type, health}

    `footprint` receives every exchange-labelled aggressor trade.
    `wall_tracker` receives those trades and samples on each valid book snapshot.
    Wall tracking requires a caller-supplied strike list; no strikes are fabricated.
    """

    BINANCE_WS = "wss://stream.binance.com:9443/stream?streams=btcusdt@trade/btcusdt@depth20@100ms"
    COINBASE_WS = "wss://advanced-trade-ws.coinbase.com"
    DERIBIT_WS = "wss://www.deribit.com/ws/api/v2"

    def __init__(
        self,
        config: FeedConfig | None = None,
        *,
        footprint: FootprintDelta | None = None,
        wall_tracker: OrderBookWallTracker | None = None,
        on_event: EventCallback | None = None,
        on_health: HealthCallback | None = None,
        spot_provider: Callable[[], Optional[float]] | None = None,
    ):
        self.config = config or FeedConfig()
        self.config.validate()
        self.exchange = self.config.exchange.lower()
        self.footprint = footprint or FootprintDelta()
        self.wall_tracker = wall_tracker
        self.on_event = on_event
        self.on_health = on_health
        self.spot_provider = spot_provider
        self.state = FeedState()
        self._stop = asyncio.Event()
        self._runner: Optional[asyncio.Task] = None
        self._monitor: Optional[asyncio.Task] = None
        self._send_lock = asyncio.Lock()
        self._ws = None
        self._rpc_id = 0
        self._pending_rpc: dict[int, asyncio.Future] = {}
        self._deribit_instruments: dict[str, dict[str, Any]] = {}
        self._latest_trade_timestamp: Optional[float] = None
        self._latest_book_timestamp: Optional[float] = None

    async def start(self) -> None:
        if websockets is None:
            raise RuntimeError("Missing dependency: install `websockets` with `pip install websockets`.")
        if self._runner and not self._runner.done():
            return
        self._stop.clear()
        self._runner = asyncio.create_task(self._run_forever(), name=f"{self.exchange}-feed")
        self._monitor = asyncio.create_task(self._health_monitor(), name=f"{self.exchange}-feed-health")

    async def stop(self) -> None:
        self._stop.set()
        if self._ws is not None:
            try:
                await self._ws.close()
            except Exception:
                pass
        tasks = [t for t in (self._runner, self._monitor) if t is not None]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self.state.connected = False
        self.state.healthy = False
        await self._emit_health("stopped")

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.stop()

    def health(self) -> FeedHealth:
        now = time.monotonic()
        age = lambda t: None if t is None else max(0.0, now - t)
        message_age, trade_age, book_age = (
            age(self.state.last_message_monotonic),
            age(self.state.last_trade_monotonic),
            age(self.state.last_book_monotonic),
        )
        healthy = bool(
            self.state.connected
            and message_age is not None and message_age <= self.config.stale_after_seconds
            and trade_age is not None and trade_age <= self.config.stale_after_seconds
            and book_age is not None and book_age <= self.config.stale_after_seconds
        )
        reason = "" if healthy else self.state.last_error or "disconnected or stale trade/book feed"
        return FeedHealth(self.exchange, self.state.connected, healthy, message_age, trade_age,
                          book_age, self.state.reconnect_count, reason)

    async def _emit_health(self, reason: str = "") -> None:
        h = self.health()
        self.state.healthy = h.healthy
        if reason and not h.healthy and not self.state.last_error:
            h = FeedHealth(h.exchange, h.connected, h.healthy, h.last_message_age_seconds,
                           h.last_trade_age_seconds, h.last_book_age_seconds, h.reconnect_count, reason)
        result = _call_callback(self.on_health, h)
        if result:
            await result
        await self._emit({"type": "health", "health": h})

    async def _emit(self, event: dict[str, Any]) -> None:
        result = _call_callback(self.on_event, event)
        if result:
            try:
                await result
            except Exception:
                log.exception("on_event callback failed")

    async def _health_monitor(self) -> None:
        while not self._stop.is_set():
            await asyncio.sleep(self.config.heartbeat_seconds)
            h = self.health()
            self.state.healthy = h.healthy
            if not h.healthy and self.state.connected:
                self.state.last_error = h.reason
                log.warning("%s feed unhealthy: %s", self.exchange, h.reason)
            await self._emit_health()

    async def _run_forever(self) -> None:
        delay = self.config.reconnect_min_seconds
        while not self._stop.is_set():
            try:
                if self.exchange == "binance":
                    await self._run_binance()
                elif self.exchange == "coinbase":
                    await self._run_coinbase()
                else:
                    await self._run_deribit()
                if not self._stop.is_set():
                    raise ConnectionError("WebSocket ended")
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.state.last_error = f"{type(exc).__name__}: {exc}"
                log.warning("%s feed disconnected: %s", self.exchange, self.state.last_error)
            finally:
                self.state.connected = False
                self.state.healthy = False
                if self._ws is not None:
                    try:
                        await self._ws.close()
                    except Exception:
                        pass
                    self._ws = None
                await self._emit_health(self.state.last_error or "disconnected")
            if self._stop.is_set():
                break
            self.state.reconnect_count += 1
            jitter = 1.0 + random.uniform(-self.config.reconnect_jitter_fraction,
                                           self.config.reconnect_jitter_fraction)
            await asyncio.sleep(min(self.config.reconnect_max_seconds, delay) * max(0.0, jitter))
            delay = min(self.config.reconnect_max_seconds, delay * 2.0)

    async def _connected(self, ws) -> None:
        self._ws = ws
        self.state.connected = True
        self.state.last_error = ""
        # Keepalives are also configured in websockets.connect. Exchange-specific ping
        # handling remains enabled; this method only marks transport connected.
        await self._emit_health("connected")

    def _message_seen(self) -> None:
        self.state.last_message_monotonic = time.monotonic()

    async def _run_binance(self) -> None:
        async with websockets.connect(
            self.BINANCE_WS, ping_interval=20, ping_timeout=20, close_timeout=5, max_queue=2048
        ) as ws:
            await self._connected(ws)
            async for raw in ws:
                self._message_seen()
                msg = json.loads(raw)
                data = msg.get("data", msg)
                kind = data.get("e")
                if kind == "trade":
                    # Binance m=true means buyer was maker, therefore sell aggressor.
                    ts = float(data["T"]) / 1000.0
                    side = "sell" if data.get("m") else "buy"
                    await self._route_trade(ts, float(data["p"]), float(data["q"]), side,
                                            "BTCUSDT", data)
                elif kind == "depthUpdate" or ("bids" in data and "asks" in data):
                    ts = float(data.get("E", int(time.time() * 1000))) / 1000.0
                    bids = [BookLevel(float(p), float(q)) for p, q in data.get("bids", [])]
                    asks = [BookLevel(float(p), float(q)) for p, q in data.get("asks", [])]
                    await self._route_book(ts, bids, asks, "BTCUSDT", data)

    async def _run_coinbase(self) -> None:
        async with websockets.connect(
            self.COINBASE_WS, ping_interval=20, ping_timeout=20, close_timeout=5, max_queue=2048
        ) as ws:
            await self._connected(ws)
            # Advanced Trade public channels. `market_trades.side` is the taker/aggressor side.
            await ws.send(json.dumps({
                "type": "subscribe", "product_ids": ["BTC-USD"], "channel": "market_trades"
            }))
            await ws.send(json.dumps({
                "type": "subscribe", "product_ids": ["BTC-USD"], "channel": "level2"
            }))
            bids: dict[float, float] = {}
            asks: dict[float, float] = {}
            async for raw in ws:
                self._message_seen()
                msg = json.loads(raw)
                channel = msg.get("channel")
                events = msg.get("events", [])
                for event in events:
                    if channel == "market_trades":
                        for tr in event.get("trades", []):
                            aggressor_side = str(tr.get("side", "")).upper()
                            if aggressor_side not in {"BUY", "SELL"}:
                                continue
                            side = "buy" if aggressor_side == "BUY" else "sell"
                            ts = datetime.fromisoformat(tr["time"].replace("Z", "+00:00")).timestamp()
                            await self._route_trade(ts, float(tr["price"]), float(tr["size"]), side,
                                                    "BTC-USD", tr)
                    elif channel == "level2":
                        event_type = event.get("type")
                        if event_type == "snapshot":
                            bids.clear()
                            asks.clear()
                        for update in event.get("updates", []):
                            price, size = float(update["price_level"]), float(update["new_quantity"])
                            side = str(update["side"]).lower()
                            book = bids if side == "bid" else asks
                            if size == 0:
                                book.pop(price, None)
                            else:
                                book[price] = size
                        if bids and asks:
                            ts_raw = event.get("timestamp") or msg.get("timestamp")
                            ts = datetime.fromisoformat(ts_raw.replace("Z", "+00:00")).timestamp() if ts_raw else time.time()
                            top_bids = [BookLevel(p, q) for p, q in sorted(bids.items(), reverse=True)[:self.config.depth_levels]]
                            top_asks = [BookLevel(p, q) for p, q in sorted(asks.items())[:self.config.depth_levels]]
                            await self._route_book(ts, top_bids, top_asks, "BTC-USD", event)

    async def _rpc(self, method: str, params: dict[str, Any] | None = None, timeout: float = 20.0):
        self._rpc_id += 1
        req_id = self._rpc_id
        fut = asyncio.get_running_loop().create_future()
        self._pending_rpc[req_id] = fut
        async with self._send_lock:
            await self._ws.send(json.dumps({"jsonrpc": "2.0", "id": req_id, "method": method,
                                            "params": params or {}}))
        try:
            return await asyncio.wait_for(fut, timeout)
        finally:
            self._pending_rpc.pop(req_id, None)

    async def _run_deribit(self) -> None:
        async with websockets.connect(
            self.DERIBIT_WS, ping_interval=20, ping_timeout=20, close_timeout=5, max_queue=4096
        ) as ws:
            await self._connected(ws)
            self._ws = ws
            # The receive loop must run concurrently with RPC requests, otherwise the
            # JSON-RPC request would wait for a response that nobody is reading.
            reader = asyncio.create_task(self._deribit_reader(ws), name="deribit-ws-reader")
            option_poll = None
            try:
                instruments = await self._rpc(
                    "public/get_instruments",
                    {"currency": "BTC", "kind": "option", "expired": False},
                )
                for item in instruments.get("result", []):
                    self._deribit_instruments[item["instrument_name"]] = item
                await self._rpc("public/subscribe", {
                    "channels": ["trades.BTC-PERPETUAL.raw", "book.BTC-PERPETUAL.100ms"]
                })
                option_poll = asyncio.create_task(self._deribit_option_poller())
                await reader
                if not self._stop.is_set():
                    raise ConnectionError("Deribit WebSocket reader ended")
            finally:
                if option_poll:
                    option_poll.cancel()
                    await asyncio.gather(option_poll, return_exceptions=True)
                reader.cancel()
                await asyncio.gather(reader, return_exceptions=True)

    async def _deribit_reader(self, ws) -> None:
        async for raw in ws:
            self._message_seen()
            msg = json.loads(raw)
            if "id" in msg:
                fut = self._pending_rpc.get(int(msg["id"]))
                if fut and not fut.done():
                    if "error" in msg:
                        fut.set_exception(RuntimeError(str(msg["error"])))
                    else:
                        fut.set_result(msg.get("result", {}))
                continue
            params = msg.get("params", {})
            channel = params.get("channel", "")
            data = params.get("data", {})
            if channel.startswith("trades."):
                rows = data if isinstance(data, list) else [data]
                for tr in rows:
                    ts = float(tr.get("timestamp", time.time() * 1000)) / 1000.0
                    side = str(tr.get("direction", "")).lower()
                    amount = float(tr.get("amount", 0))
                    price = float(tr.get("price", 0))
                    # Deribit BTC perpetual trade amount is USD notional; normalize to BTC.
                    size_btc = amount / price if price > 0 else 0.0
                    if side in {"buy", "sell"} and price > 0 and size_btc >= 0:
                        await self._route_trade(ts, price, size_btc, side, "BTC-PERPETUAL", tr)
            elif channel.startswith("book.BTC-PERPETUAL"):
                # Maintain the incremental local book. Deribit sends a snapshot on initial
                # subscription and subsequent new/change/delete updates.
                if not hasattr(self, "_deribit_bids"):
                    self._deribit_bids, self._deribit_asks = {}, {}
                for rows, book in ((data.get("bids", []), self._deribit_bids),
                                   (data.get("asks", []), self._deribit_asks)):
                    for row in rows:
                        if len(row) < 3:
                            continue
                        action, price, amount = row[0], float(row[1]), float(row[2])
                        if action == "delete" or amount == 0:
                            book.pop(price, None)
                        else:
                            book[price] = amount
                if self._deribit_bids and self._deribit_asks:
                    ts = float(data.get("timestamp", time.time() * 1000)) / 1000.0
                    bid_levels = [BookLevel(p, q) for p, q in sorted(self._deribit_bids.items(), reverse=True)[:self.config.depth_levels]]
                    ask_levels = [BookLevel(p, q) for p, q in sorted(self._deribit_asks.items())[:self.config.depth_levels]]
                    await self._route_book(ts, bid_levels, ask_levels, "BTC-PERPETUAL", data)

    async def _deribit_option_poller(self) -> None:
        while not self._stop.is_set() and self._ws is not None:
            try:
                result = await self._rpc("public/get_book_summary_by_currency", {"currency": "BTC", "kind": "option"})
                rows = result.get("result", [])
                now = datetime.now(timezone.utc)
                spot = self.spot_provider() if self.spot_provider else None
                for row in rows:
                    name = row.get("instrument_name")
                    meta = self._deribit_instruments.get(name)
                    if not meta:
                        continue
                    expiry_ms = meta.get("expiration_timestamp")
                    if not expiry_ms:
                        continue
                    expiry = datetime.fromtimestamp(float(expiry_ms) / 1000, tz=timezone.utc)
                    iv = row.get("mark_iv")
                    oi = row.get("open_interest")
                    strike = meta.get("strike")
                    kind = meta.get("option_type")
                    if iv is None or oi is None or strike is None or kind is None:
                        continue
                    if (expiry - now).total_seconds() <= 0 or (expiry - now).total_seconds() > self.config.deribit_option_expiry_days * 86400:
                        continue
                    if spot is not None:
                        if not math.isfinite(float(spot)) or float(spot) <= 0:
                            continue
                        if abs(float(strike) / float(spot) - 1.0) > self.config.deribit_option_moneyness_fraction:
                            continue
                    obs = OptionObservation(float(strike), str(kind), float(iv) / 100.0, float(oi),
                                            expiry, "Deribit", now)
                    await self._emit({"type": "option", "exchange": "deribit", "symbol": name,
                                      "timestamp": now.timestamp(), "observation": obs, "raw": row})
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.warning("Deribit option summary poll failed: %s", exc)
            await asyncio.sleep(self.config.deribit_option_poll_seconds)

    async def _route_trade(self, ts: float, price: float, size: float, side: str,
                           symbol: str, raw: dict[str, Any]) -> None:
        if not all(math.isfinite(x) for x in (ts, price, size)) or ts <= 0 or price <= 0 or size < 0:
            raise DataQualityError("invalid trade received from feed")
        if side not in {"buy", "sell"}:
            raise DataQualityError(f"unknown aggressor side: {side!r}")
        trade = AggressorTrade(ts, price, size, side)
        self.footprint.add_trade(trade)
        if self.wall_tracker:
            self.wall_tracker.add_trade(trade)
        self.state.last_trade_monotonic = time.monotonic()
        self.state.last_exchange_timestamp = ts
        self._latest_trade_timestamp = ts
        await self._emit({"type": "trade", "exchange": self.exchange, "symbol": symbol,
                          "timestamp": ts, "price": price, "size_btc": size, "side": side, "raw": raw})

    async def _route_book(self, ts: float, bids: Sequence[BookLevel], asks: Sequence[BookLevel],
                          symbol: str, raw: dict[str, Any]) -> None:
        if not bids or not asks:
            return
        if not math.isfinite(ts) or ts <= 0:
            raise DataQualityError("invalid book timestamp")
        self.state.last_book_monotonic = time.monotonic()
        self.state.last_exchange_timestamp = ts
        self._latest_book_timestamp = ts
        wall_rows = []
        if self.wall_tracker:
            # Exchange snapshots may share timestamps. The analytics tracker requires
            # strictly increasing times, so skip duplicate/older wall samples rather
            # than fabricating timestamps or terminating the feed.
            last_sample = getattr(self.wall_tracker, "_last_sample", None)
            if last_sample is None or ts > last_sample:
                wall_rows = self.wall_tracker.sample(bids, asks, ts)
        await self._emit({"type": "book", "exchange": self.exchange, "symbol": symbol,
                          "timestamp": ts, "bids": list(bids), "asks": list(asks),
                          "wall_observations": wall_rows, "raw": raw})


async def run_feed(feed: AsyncMarketDataFeed) -> None:
    """Convenience runner that starts a feed until cancelled."""
    await feed.start()
    try:
        while True:
            await asyncio.sleep(3600)
    finally:
        await feed.stop()
