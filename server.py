"""FastAPI paper-trading dashboard server. No strategy or live order execution is enabled.

Install: pip install fastapi uvicorn sqlalchemy aiosqlite asyncpg websockets
Run: uvicorn server:app --host 127.0.0.1 --port 8000
Optional env: FEED_EXCHANGE=binance|coinbase|deribit, DATABASE_URL=...
"""
from __future__ import annotations
import asyncio, logging, os, time
from collections import deque
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Query
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field
from analytics import FootprintDelta, OrderBookWallTracker, OptionObservation, calculate_gamma_exposure, index_option_snapshot, to_dict
from broker import PaperBroker, BrokerConfig
from feed import AsyncMarketDataFeed, FeedConfig
from database import Database, json_safe

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
log = logging.getLogger("paper-dashboard")
DB = Database()
BROKER = PaperBroker(BrokerConfig())
FOOTPRINT = FootprintDelta(timeframe_seconds=60, price_bin=10.0)
# No strike list is fabricated. Wall tracker activates after real strikes are supplied/configured.
_strikes = [float(x) for x in os.getenv("WALL_STRIKES", "").split(",") if x.strip()]
WALL_TRACKER = OrderBookWallTracker(_strikes) if _strikes else None
FEED: AsyncMarketDataFeed | None = None
FEED_TASK: asyncio.Task | None = None
CLIENTS: set[WebSocket] = set()
STATE: dict[str, Any] = {"feed": {"connected": False, "healthy": False}, "price": None,
    "candles": [], "gamma": [], "walls": [], "balance": None, "position": None,
    "trades": [], "last_update": None, "last_signal": None, "last_strategy_order": None, "symbol": os.getenv("TRADING_SYMBOL", "BTCUSDT"), "mode": "PAPER ONLY"}
_candle: dict | None = None
_candle_lock = asyncio.Lock()
_option_rows: dict[str, OptionObservation] = {}

async def broadcast(event: dict):
    payload = json_safe(event)
    dead = []
    for ws in tuple(CLIENTS):
        try: await ws.send_json(payload)
        except Exception: dead.append(ws)
    for ws in dead: CLIENTS.discard(ws)

async def refresh_account(price: float | None = None):
    try:
        bal = await BROKER.get_balance()
        pos = await BROKER.get_position(STATE["symbol"])
        STATE["balance"] = to_dict(bal)
        STATE["position"] = to_dict(pos)
        await DB.snapshot_broker(BROKER)
        await broadcast({"type":"account", "balance":STATE["balance"], "position":STATE["position"]})
    except Exception:
        log.exception("Account snapshot failed")

async def on_feed_event(event: dict):
    global _candle
    kind = event.get("type")
    if kind == "health":
        STATE["feed"] = json_safe(event.get("health", {}))
        await broadcast({"type":"feed_health", "health":STATE["feed"]})
        await DB.add_event("feed_health", event)
        return
    if kind == "trade":
        ts, price = float(event["timestamp"]), float(event["price"])
        STATE["price"] = price; STATE["last_update"] = ts
        # Broker only simulates existing paper orders/positions; this never submits orders.
        await BROKER.update_market(STATE["symbol"], price, timestamp=ts)
        minute = int(ts // 60) * 60
        async with _candle_lock:
            if _candle is None or minute > _candle["time"]:
                if _candle is not None:
                    STATE["candles"].append(dict(_candle)); STATE["candles"] = STATE["candles"][-1500:]
                    await broadcast({"type":"candle", "candle":dict(_candle), "closed":True})
                _candle = {"time":minute,"open":price,"high":price,"low":price,"close":price,"volume":float(event.get("size_btc",0))}
            elif minute == _candle["time"]:
                _candle["high"] = max(_candle["high"], price); _candle["low"] = min(_candle["low"], price)
                _candle["close"] = price; _candle["volume"] += float(event.get("size_btc",0))
            candle = dict(_candle)
        await broadcast({"type":"candle", "candle":candle, "closed":False})
        await broadcast({"type":"trade", "timestamp":ts,"price":price,"size_btc":event.get("size_btc"),"side":event.get("side")})
        await refresh_account(price)
    elif kind == "book":
        obs = event.get("wall_observations") or []
        STATE["walls"] = to_dict(obs)
        await broadcast({"type":"walls", "timestamp":event.get("timestamp"),"walls":STATE["walls"],
                         "bids":to_dict(event.get("bids", [])),"asks":to_dict(event.get("asks", []))})
    elif kind in {"options_snapshot", "option"}:
        # New feed versions send one complete snapshot per poll. Retain compatibility
        # with a single legacy option event, but never append repeated contracts to a
        # historical list: that double-counted open interest/gamma on every poll.
        if kind == "options_snapshot":
            incoming = event.get("observations") or []
            indexed_rows = []
            for row in incoming:
                if isinstance(row, dict):
                    indexed_rows.append((row.get("symbol"), row.get("observation")))
            snapshot = index_option_snapshot(indexed_rows)
            _option_rows.clear()
            _option_rows.update(snapshot)
        else:
            instrument = str(event.get("symbol") or "").strip()
            obs = event.get("observation")
            if instrument and isinstance(obs, OptionObservation):
                _option_rows[instrument] = obs
        spot = STATE.get("price")
        if spot and _option_rows:
            try:
                now = datetime.now(timezone.utc)
                # Exclude old observations even if a legacy feed stops updating one
                # contract. Snapshot mode normally refreshes all rows together.
                max_age = max(90.0, float(os.getenv("DERIBIT_OPTION_POLL_SECONDS", "30")) * 3.0)
                fresh_rows = [obs for obs in _option_rows.values()
                              if obs.observed_at is not None
                              and 0 <= (now - obs.observed_at).total_seconds() <= max_age]
                gamma = calculate_gamma_exposure(fresh_rows, float(spot), now)
                STATE["gamma"] = to_dict(gamma)
                STATE["gamma_updated_at"] = time.time()
                await broadcast({"type":"gamma", "gamma":STATE["gamma"],
                                 "updated_at":STATE["gamma_updated_at"]})
            except Exception as exc:
                # Do not leave an old gamma regime looking current when the latest
                # snapshot has no valid rows or fails data-quality validation.
                STATE["gamma"] = None
                STATE["gamma_updated_at"] = None
                log.debug("Gamma not ready: %s", exc)
        else:
            STATE["gamma"] = None
            STATE["gamma_updated_at"] = None
            await broadcast({"type":"gamma", "gamma":None, "updated_at":None})
    if kind == "options_snapshot":
        # Avoid storing a full multi-contract options payload every poll; retain only
        # operational metadata in the event journal.
        await DB.add_event("options_snapshot", {
            "exchange": event.get("exchange", "deribit"),
            "timestamp": event.get("timestamp"),
            "count": len(event.get("observations") or []),
            "gamma_updated_at": STATE.get("gamma_updated_at"),
        })
    else:
        await DB.add_event(str(kind or "unknown"), event)

async def start_feed():
    global FEED
    exchange = os.getenv("FEED_EXCHANGE", "deribit").lower()
    if WALL_TRACKER is None:
        log.warning("WALL_STRIKES is unset: wall observations and strategy confluence will be unavailable")
    FEED = AsyncMarketDataFeed(FeedConfig(exchange=exchange,
        deribit_option_poll_seconds=float(os.getenv("DERIBIT_OPTION_POLL_SECONDS", "30"))), footprint=FOOTPRINT,
        wall_tracker=WALL_TRACKER, on_event=on_feed_event,
        spot_provider=lambda: STATE.get("price"))
    await FEED.start()
    log.info("Started %s public market data feed (paper broker only)", exchange)

@asynccontextmanager
async def lifespan(app: FastAPI):
    await DB.init()
    restored = await DB.recover_broker(BROKER)
    if restored: log.info("Recovered paper broker state from database")
    # Seed client-visible account state before feed starts.
    try:
        STATE["balance"] = to_dict(await BROKER.get_balance())
        STATE["position"] = to_dict(await BROKER.get_position(STATE["symbol"]))
    except Exception: log.exception("Initial account state failed")
    if os.getenv("START_FEED", "1").lower() not in {"0", "false", "no"}:
        try: await start_feed()
        except Exception as exc: log.exception("Feed failed to start: %s", exc)
    yield
    if FEED: await FEED.stop()
    await DB.snapshot_broker(BROKER)
    await DB.close()

app = FastAPI(title="BTC Paper Trading Dashboard", version="3.0.0", lifespan=lifespan)

@app.get("/", response_class=HTMLResponse)
async def dashboard():
    path = Path(__file__).with_name("dashboard.html")
    if not path.exists(): raise HTTPException(500, "dashboard.html missing next to server.py")
    return HTMLResponse(path.read_text(encoding="utf-8"))

@app.get("/api/health")
async def health():
    database_status = "connected"
    try:
        await DB.ping()
    except Exception as exc:
        database_status = "unavailable" if DB.engine is not None else "not_initialized"
        log.warning("Database health check failed: %s", exc)
        raise HTTPException(status_code=503, detail={
            "ok": False, "mode": "paper_only", "database": database_status,
            "live_execution_enabled": False,
        })
    gamma_updated_at = STATE.get("gamma_updated_at")
    gamma_age = max(0.0, time.time() - float(gamma_updated_at)) if gamma_updated_at else None
    return {"ok": True, "mode":"paper_only", "feed":STATE["feed"], "price":STATE["price"],
            "database":database_status, "gamma_age_seconds":gamma_age,
            "wall_tracker_configured":WALL_TRACKER is not None,
            "strategy_submission_enabled":os.getenv("ENABLE_STRATEGY_TRADING", "false").lower() in {"1", "true", "yes"},
            "live_execution_enabled":False}

def client_state() -> dict[str, Any]:
    result = dict(STATE)
    gamma_updated_at = result.get("gamma_updated_at")
    gamma_age = max(0.0, time.time() - float(gamma_updated_at)) if gamma_updated_at else None
    max_age = max(90.0, float(os.getenv("DERIBIT_OPTION_POLL_SECONDS", "30")) * 3.0)
    result["gamma_age_seconds"] = gamma_age
    result["gamma_stale"] = bool(result.get("gamma") and (gamma_age is None or gamma_age > max_age))
    if result["gamma_stale"]:
        result["gamma"] = None
    return json_safe(result)

@app.get("/api/state")
async def get_state():
    result = client_state()
    if _candle: result["current_candle"] = dict(_candle)
    return result

@app.get("/api/journal")
async def journal(limit: int = Query(200, ge=1, le=1000), offset: int = Query(0, ge=0), status: str | None = None):
    if status not in {None,"open","closed"}: raise HTTPException(400,"status must be open or closed")
    return await DB.journal(limit, offset, status)

@app.get("/api/performance")
async def performance(): return await DB.summary()

class OpenTradeInput(BaseModel):
    direction: str
    symbol: str = "BTCUSDT"
    quantity_btc: float = Field(gt=0)
    entry_price: float = Field(gt=0)
    entry_time: float | None = None
    setup_tag: str = "manual/paper"
    fee_costs: float = Field(default=0, ge=0)
    slippage_incurred: float = Field(default=0, ge=0)
    stop_price: float | None = Field(default=None, gt=0)
    target_price: float | None = Field(default=None, gt=0)
    broker_order_id: str | None = None

class CloseTradeInput(BaseModel):
    exit_price: float = Field(gt=0)
    realized_pnl_usd: float
    exit_time: float | None = None
    fee_costs: float | None = Field(default=None, ge=0)
    slippage_incurred: float | None = Field(default=None, ge=0)

@app.post("/api/journal/open", status_code=201)
async def journal_open(body: OpenTradeInput):
    try:
        row = await DB.create_trade(**body.model_dump())
        await broadcast({"type":"trade_opened", "trade":row})
        return row
    except ValueError as exc: raise HTTPException(400,str(exc))

@app.post("/api/journal/{trade_id}/close")
async def journal_close(trade_id: str, body: CloseTradeInput):
    try: row = await DB.close_trade(trade_id, **body.model_dump())
    except KeyError as exc: raise HTTPException(404,str(exc))
    await broadcast({"type":"trade_closed", "trade":row})
    return row

@app.websocket("/ws")
async def websocket_hub(ws: WebSocket):
    await ws.accept(); CLIENTS.add(ws)
    try:
        await ws.send_json({"type":"snapshot", "state":client_state(), "current_candle":_candle})
        while True:
            # Keep socket alive; accept lightweight client ping messages.
            msg = await ws.receive_text()
            if msg == "ping": await ws.send_json({"type":"pong","timestamp":time.time()})
    except WebSocketDisconnect: pass
    except Exception: log.debug("WebSocket closed", exc_info=True)
    finally: CLIENTS.discard(ws)
