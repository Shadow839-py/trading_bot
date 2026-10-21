"""Async persistent journal/state store for the BTC paper-trading dashboard.

Default: SQLite at ./trading_journal.db. PostgreSQL is supported with
DATABASE_URL=postgresql+asyncpg://user:pass@host:5432/dbname.
Install: pip install sqlalchemy aiosqlite asyncpg
"""
from __future__ import annotations
import json, os, time, uuid
from datetime import datetime, timezone
from typing import Any, Optional
from sqlalchemy import (JSON, Boolean, DateTime, Float, Integer, String, Text,
                        select, func, desc, text)
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


def utcnow() -> datetime: return datetime.now(timezone.utc)
def iso(value: Any) -> Optional[str]:
    if value is None: return None
    if isinstance(value, datetime): return value.isoformat()
    if isinstance(value, (int, float)): return datetime.fromtimestamp(value, timezone.utc).isoformat()
    return str(value)

class Base(DeclarativeBase): pass

class TradeRecord(Base):
    __tablename__ = "trade_journal"
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    broker_order_id: Mapped[Optional[str]] = mapped_column(String(100), index=True, nullable=True)
    entry_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True, default=utcnow)
    exit_time: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    direction: Mapped[str] = mapped_column(String(8))
    symbol: Mapped[str] = mapped_column(String(32), default="BTCUSDT")
    quantity_btc: Mapped[float] = mapped_column(Float)
    entry_price: Mapped[float] = mapped_column(Float)
    exit_price: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    realized_pnl_usd: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    r_multiple: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    fee_costs: Mapped[float] = mapped_column(Float, default=0.0)
    slippage_incurred: Mapped[float] = mapped_column(Float, default=0.0)
    setup_tag: Mapped[str] = mapped_column(String(160), default="manual/paper")
    stop_price: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    target_price: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    status: Mapped[str] = mapped_column(String(12), default="open", index=True)
    metadata_json: Mapped[dict] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)

class StateRecord(Base):
    __tablename__ = "app_state"
    key: Mapped[str] = mapped_column(String(100), primary_key=True)
    payload: Mapped[dict] = mapped_column(JSON)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow)

class EventRecord(Base):
    __tablename__ = "event_log"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, index=True)
    event_type: Mapped[str] = mapped_column(String(40), index=True)
    payload: Mapped[dict] = mapped_column(JSON)

class Database:
    def __init__(self, url: Optional[str] = None):
        url = url or os.getenv("DATABASE_URL", "sqlite+aiosqlite:///./trading_journal.db")
        if url.startswith("postgres://"): url = "postgresql+asyncpg://" + url[len("postgres://"):]
        if url.startswith("postgresql://"): url = "postgresql+asyncpg://" + url[len("postgresql://"):]
        self.url = url
        # Defer driver loading until startup so importing the API module remains safe.
        self.engine = None
        self.sessions = None

    async def init(self):
        if self.engine is None:
            self.engine = create_async_engine(self.url, pool_pre_ping=True)
            self.sessions = async_sessionmaker(self.engine, expire_on_commit=False)
        async with self.engine.begin() as conn: await conn.run_sync(Base.metadata.create_all)

    async def ping(self) -> bool:
        """Run a real round-trip query; engine initialization alone is not health."""
        if self.engine is None:
            raise RuntimeError("database engine is not initialized")
        async with self.engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
        return True

    async def close(self):
        if self.engine is not None:
            await self.engine.dispose()
            self.engine = None
            self.sessions = None

    async def add_event(self, event_type: str, payload: dict):
        async with self.sessions() as s:
            s.add(EventRecord(event_type=event_type, payload=json_safe(payload)))
            await s.commit()

    async def save_state(self, key: str, payload: dict):
        async with self.sessions() as s:
            row = await s.get(StateRecord, key)
            if row: row.payload = json_safe(payload); row.updated_at = utcnow()
            else: s.add(StateRecord(key=key, payload=json_safe(payload)))
            await s.commit()

    async def load_state(self, key: str) -> Optional[dict]:
        async with self.sessions() as s:
            row = await s.get(StateRecord, key)
            return row.payload if row else None

    async def create_trade(self, *, direction: str, symbol: str, quantity_btc: float,
                           entry_price: float, entry_time: Any = None, setup_tag: str = "manual/paper",
                           fee_costs: float = 0.0, slippage_incurred: float = 0.0,
                           broker_order_id: str | None = None, stop_price: float | None = None,
                           target_price: float | None = None, metadata: dict | None = None) -> dict:
        direction = direction.lower()
        if direction not in {"long", "short"}: raise ValueError("direction must be Long or Short")
        if quantity_btc <= 0 or entry_price <= 0: raise ValueError("quantity and entry price must be positive")
        t = as_datetime(entry_time)
        async with self.sessions() as s:
            # Broker order IDs make retries idempotent across reconnects/recovery.
            if broker_order_id:
                existing = (await s.execute(select(TradeRecord).where(
                    TradeRecord.broker_order_id == broker_order_id))).scalars().first()
                if existing:
                    return trade_dict(existing)
            row = TradeRecord(direction=direction, symbol=symbol, quantity_btc=float(quantity_btc),
                entry_price=float(entry_price), entry_time=t, setup_tag=setup_tag or "unclassified",
                fee_costs=float(fee_costs), slippage_incurred=float(slippage_incurred),
                broker_order_id=broker_order_id, stop_price=stop_price, target_price=target_price,
                metadata_json=json_safe(metadata or {}))
            s.add(row); await s.commit(); await s.refresh(row); return trade_dict(row)

    async def close_trade(self, trade_id: str, *, exit_price: float, realized_pnl_usd: float,
                          exit_time: Any = None, fee_costs: float | None = None,
                          slippage_incurred: float | None = None, metadata: dict | None = None) -> dict:
        async with self.sessions() as s:
            row = await s.get(TradeRecord, trade_id)
            if not row: raise KeyError(f"trade {trade_id} not found")
            if row.status == "closed": return trade_dict(row)
            row.exit_time = as_datetime(exit_time); row.exit_price = float(exit_price)
            row.realized_pnl_usd = float(realized_pnl_usd)
            risk = abs(row.entry_price - (row.stop_price or row.entry_price)) * row.quantity_btc
            row.r_multiple = (row.realized_pnl_usd / risk) if risk > 0 else None
            if fee_costs is not None: row.fee_costs = float(fee_costs)
            if slippage_incurred is not None: row.slippage_incurred = float(slippage_incurred)
            if metadata: row.metadata_json = {**(row.metadata_json or {}), **json_safe(metadata)}
            row.status = "closed"; await s.commit(); await s.refresh(row); return trade_dict(row)

    async def open_trade_for_symbol(self, symbol: str) -> dict | None:
        async with self.sessions() as s:
            row = (await s.execute(
                select(TradeRecord).where(
                    TradeRecord.symbol == symbol, TradeRecord.status == "open"
                ).order_by(desc(TradeRecord.entry_time))
            )).scalars().first()
            return trade_dict(row) if row else None

    async def trade_by_broker_order_id(self, broker_order_id: str) -> dict | None:
        async with self.sessions() as s:
            row = (await s.execute(select(TradeRecord).where(
                TradeRecord.broker_order_id == broker_order_id
            ))).scalars().first()
            return trade_dict(row) if row else None

    async def journal(self, limit: int = 200, offset: int = 0, status: str | None = None) -> list[dict]:
        limit = max(1, min(int(limit), 1000)); offset = max(0, int(offset))
        async with self.sessions() as s:
            q = select(TradeRecord).order_by(desc(TradeRecord.entry_time)).offset(offset).limit(limit)
            if status in {"open", "closed"}: q = q.where(TradeRecord.status == status)
            rows = (await s.execute(q)).scalars().all()
            return [trade_dict(x) for x in rows]

    async def summary(self) -> dict:
        async with self.sessions() as s:
            rows = (await s.execute(select(TradeRecord).where(TradeRecord.status == "closed").order_by(TradeRecord.exit_time))).scalars().all()
        pnls = [float(r.realized_pnl_usd or 0) for r in rows]
        wins = sum(x > 0 for x in pnls); losses = sum(x < 0 for x in pnls)
        gross_profit = sum(x for x in pnls if x > 0); gross_loss = -sum(x for x in pnls if x < 0)
        equity = peak = 0.0; max_dd = 0.0
        for pnl in pnls:
            equity += pnl; peak = max(peak, equity); max_dd = max(max_dd, peak - equity)
        return {"closed_trades": len(pnls), "wins": wins, "losses": losses,
                "win_rate": wins / len(pnls) if pnls else 0.0,
                "gross_profit_usd": gross_profit, "gross_loss_usd": gross_loss,
                "profit_factor": gross_profit / gross_loss if gross_loss else (None if gross_profit == 0 else "infinity"),
                "net_pnl_usd": sum(pnls), "max_drawdown_usd": max_dd,
                "average_r": sum((r.r_multiple or 0) for r in rows) / len(rows) if rows else 0.0}

    async def recover_broker(self, broker) -> bool:
        """Restore a PaperBroker snapshot written by snapshot_broker; false if absent.
        Restores internal paper state only; never connects to or enables live execution.
        """
        state = await self.load_state("paper_broker")
        if not state: return False
        from broker import Order, Position, Fill, OrderSide, OrderType, OrderStatus
        async with broker._lock:
            broker._cash = float(state["cash"]); broker._realized_pnl = float(state["realized_pnl"])
            broker._fees_paid = float(state["fees_paid"]); broker._halted = bool(state.get("halted", False))
            broker._positions = {k: Position(**v) for k, v in state.get("positions", {}).items()}
            broker._mark_prices = {k: float(v) for k, v in state.get("mark_prices", {}).items()}
            broker._quotes = {k: tuple(v) for k, v in state.get("quotes", {}).items()}
            broker._orders = {}
            for raw in state.get("orders", []):
                raw = dict(raw); raw["side"] = OrderSide(raw["side"]); raw["order_type"] = OrderType(raw["order_type"]); raw["status"] = OrderStatus(raw["status"])
                o = Order(**raw); broker._orders[o.order_id] = o
            broker._client_ids = {o.client_order_id: o.order_id for o in broker._orders.values() if o.client_order_id}
            broker._fills = [Fill(**x) for x in state.get("fills", [])]
        return True

    async def snapshot_broker(self, broker):
        async with broker._lock:
            state = {"cash": broker._cash, "realized_pnl": broker._realized_pnl, "fees_paid": broker._fees_paid,
                "halted": broker._halted,
                "positions": {k: vars(v).copy() for k,v in broker._positions.items()},
                "mark_prices": dict(broker._mark_prices), "quotes": {k:list(v) for k,v in broker._quotes.items()},
                "orders": [broker_order_dict(o) for o in broker._orders.values()],
                "fills": [json_safe(vars(f)) for f in broker._fills]}
        await self.save_state("paper_broker", state)


def as_datetime(value: Any) -> datetime:
    if value is None: return utcnow()
    if isinstance(value, datetime): return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, (float, int)): return datetime.fromtimestamp(value, timezone.utc)
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)

def json_safe(value: Any):
    if value is None or isinstance(value, (str, int, float, bool)): return value
    if isinstance(value, datetime): return value.isoformat()
    if hasattr(value, "value"): return value.value
    if hasattr(value, "__dataclass_fields__"): return json_safe(vars(value))
    if isinstance(value, dict): return {str(k): json_safe(v) for k,v in value.items()}
    if isinstance(value, (list, tuple, set)): return [json_safe(v) for v in value]
    return str(value)

def broker_order_dict(o):
    d = vars(o).copy()
    for k in ("side", "order_type", "status"): d[k] = getattr(d[k], "value", d[k])
    return json_safe(d)

def trade_dict(r) -> dict:
    return {"id":r.id,"broker_order_id":r.broker_order_id,"entry_time":iso(r.entry_time),"exit_time":iso(r.exit_time),
        "direction":r.direction,"symbol":r.symbol,"quantity_btc":r.quantity_btc,"entry_price":r.entry_price,
        "exit_price":r.exit_price,"realized_pnl_usd":r.realized_pnl_usd,"r_multiple":r.r_multiple,
        "fee_costs":r.fee_costs,"slippage_incurred":r.slippage_incurred,"setup_tag":r.setup_tag,
        "stop_price":r.stop_price,"target_price":r.target_price,"status":r.status,"metadata":r.metadata_json or {}}
