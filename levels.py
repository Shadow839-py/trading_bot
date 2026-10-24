"""levels.py - gamma-level trading for the BTC paper bot (PAPER ONLY).

Where the data comes from (nothing here prices options itself):
  * Levels  - Deribit public API. Open interest from get_book_summary_by_currency and the exchange's
              own gamma (public/ticker -> greeks.gamma). Dollar gamma per 1% move = gamma x OI x S^2 x 0.01.
              If the exchange data is incomplete the level set is flagged "degraded" and nothing trades.
  * Walls   - Coinbase Exchange public BTC-USD level-2 book + trades (the same view as BTC Pulse "BTCUSD").
  * Rules   - copied from the BTC Pulse page: R1-R4 / S1-S4 = top call-gamma strikes above price and top
              put-gamma strikes below (within 15%); wall = resting size at the strike >= 2.5x the median of
              the neighbouring zones; STRONG = gamma >= half of the biggest level AND a wall that has stood
              3+ minutes; bounce/rejection only when STRONG, within 0.6% of price and net gamma is positive.

Honest limit: net gamma SIGN (calm/wild) still assumes dealers are long calls / short puts, because no
public source shows real dealer positions. The level strikes themselves do not depend on that assumption.
"""
from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import re
import time
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Optional

from strategy import SignalSide, StrategySignal

log = logging.getLogger("levels")
LEVEL_PREFIX = "LEVEL_"

DERIBIT = "https://www.deribit.com/api/v2/public/"
COINBASE_WS = "wss://ws-feed.exchange.coinbase.com"

# --- constants copied from the BTC Pulse page (times in milliseconds) ---
WT_HALF, WT_WALL, WT_END, WT_MIN = 50.0, 2.5, 1.6, 10.0
WT_WIN, WT_NEW, WT_EV = 120_000, 180_000, 300_000
GV_NEAR = 0.6          # percent
N_LEVELS = 4
NEAR_FRACTION = 0.15
MAX_EXPIRY_YEARS = 60.0 / 365.0
MAX_MONEYNESS = 0.25

_MONTHS = {m: i for i, m in enumerate(
    ["JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"], 1)}
_NAME = re.compile(r"^BTC-(\d{1,2})([A-Z]{3})(\d{2})-(\d+)-([CP])$")


def _env_f(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except ValueError:
        return default


def _jsround(x: float) -> int:
    """JavaScript Math.round (halves round up) so zone bins match the dashboard exactly."""
    return int(math.floor(x + 0.5))


# =============================================================== Deribit levels
def parse_option_name(name: str) -> Optional[tuple[float, str, float]]:
    """-> (strike, 'C'|'P', expiry_epoch_seconds) for names like BTC-27DEC26-90000-C (expires 08:00 UTC)."""
    m = _NAME.match(name or "")
    if not m or m.group(2) not in _MONTHS:
        return None
    try:
        exp = datetime(2000 + int(m.group(3)), _MONTHS[m.group(2)], int(m.group(1)), 8, tzinfo=timezone.utc)
    except ValueError:
        return None
    return float(m.group(4)), m.group(5), exp.timestamp()


@dataclass
class Level:
    name: str
    K: float
    kind: str            # 'r' (call gamma above price) or 's' (put gamma below price)
    gamma: float         # dollar gamma per 1% move
    share: float         # gamma / biggest level on the card


@dataclass
class LevelSet:
    spot: float
    levels: list[Level]
    flip: Optional[float]
    calm: bool
    total_net: float
    updated_at: float
    coverage: float = 1.0
    n_instruments: int = 0
    source: str = "deribit_exchange_greeks"
    degraded: bool = False
    reason: str = ""


def compute_levels(spot: float, rows: list[dict[str, float]]) -> LevelSet:
    """rows: dicts with K, cp ('C'/'P'), T (years), gamma (exchange, per $), oi. Mirrors the dashboard's loadGEX."""
    by_k: dict[float, dict[str, float]] = {}
    for r in rows:
        T, K, oi, gm = r["T"], r["K"], r["oi"], r["gamma"]
        if T <= 0 or oi <= 0 or gm <= 0:
            continue
        if T > MAX_EXPIRY_YEARS or abs(K / spot - 1.0) > MAX_MONEYNESS:
            continue
        g = gm * oi * spot * spot * 0.01
        e = by_k.setdefault(K, {"K": K, "call": 0.0, "put": 0.0})
        e["call" if r["cp"] == "C" else "put"] += g
    ordered = sorted(by_k.values(), key=lambda e: e["K"])
    for e in ordered:
        e["net"] = e["call"] - e["put"]
    total = sum(e["net"] for e in ordered)
    flip, best, cum = None, 1e9, 0.0
    for i, e in enumerate(ordered):
        prev = cum
        cum += e["net"]
        if i and prev * cum < 0 and abs(e["K"] - spot) < best:
            best, flip = abs(e["K"] - spot), e["K"]
    near = lambda e: abs(e["K"] / spot - 1.0) < NEAR_FRACTION
    R = sorted([e for e in ordered if e["K"] > spot and near(e) and e["call"] > 0],
               key=lambda e: -e["call"])[:N_LEVELS]
    S = sorted([e for e in ordered if e["K"] < spot and near(e) and e["put"] > 0],
               key=lambda e: -e["put"])[:N_LEVELS]
    R = sorted(R, key=lambda e: e["K"])
    S = sorted(S, key=lambda e: -e["K"])
    mx = max([e["call"] for e in R] + [e["put"] for e in S] + [1.0])
    levels = [Level(f"R{i + 1}", e["K"], "r", e["call"], e["call"] / mx) for i, e in enumerate(R)]
    levels += [Level(f"S{i + 1}", e["K"], "s", e["put"], e["put"] / mx) for i, e in enumerate(S)]
    return LevelSet(spot=spot, levels=levels, flip=flip, calm=total >= 0, total_net=total,
                    updated_at=time.time(), n_instruments=len(rows))


def _get_json(url: str, timeout: float = 15.0) -> Any:
    req = urllib.request.Request(url, headers={"User-Agent": "btc-paper-bot/1.0", "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _pick_candidates(summary: list[dict], spot: float, now: float, limit: int) -> list[tuple[str, float, str, float]]:
    """Choose which instruments to query. Ordering only decides what gets fetched; values come from Deribit."""
    cands = []
    for o in summary:
        meta = parse_option_name(o.get("instrument_name", ""))
        oi = float(o.get("open_interest") or 0.0)
        if not meta or oi <= 0:
            continue
        K, cp, exp = meta
        T = (exp - now) / (365.0 * 86400.0)
        if T <= 0 or T > MAX_EXPIRY_YEARS or abs(K / spot - 1.0) > NEAR_FRACTION:
            continue
        cands.append((oi / math.sqrt(max(T, 1.0 / 365.0 / 24.0)), o["instrument_name"], K, cp, T, oi))
    cands.sort(reverse=True)
    return [(n, K, cp, T) for _, n, K, cp, T, _oi in cands[:limit]]


# =============================================================== wall watch (port of the dashboard)
@dataclass
class WallState:
    kind: str = "ok"                     # 'ok' | 'at' | 'out'
    ask: bool = False
    wall: bool = False
    since: float = 0.0
    lb: bool = False
    hist: list = field(default_factory=list)
    ev: Optional[dict] = None
    fresh: bool = True
    z: float = 0.0
    ratio: float = 0.0
    abs: bool = False
    abs_btc: float = 0.0


class WallWatch:
    def __init__(self) -> None:
        self.K: list[float] = []
        self.st: dict[float, WallState] = {}
        self.tr: list[tuple[float, float, float, bool]] = []
        self.last = 0.0

    def set_strikes(self, ks: list[float]) -> None:
        self.K = list(dict.fromkeys(float(k) for k in ks))
        for k in list(self.st):
            if k not in self.K:
                del self.st[k]

    def trade(self, t_ms: float, p: float, q: float, sell: bool) -> None:
        if not self.K:
            return
        for K in self.K:
            if abs(p - K) <= WT_HALF:
                self.tr.append((t_ms, p, q, sell))
                if len(self.tr) > 4000:
                    del self.tr[:1000]
                return

    def traded(self, K: float, ask: bool, frm: float) -> float:
        return sum(q for (T, p, q, sell) in self.tr if T >= frm and sell == (not ask) and abs(p - K) <= WT_HALF)

    def sample(self, bids: list[tuple[float, float]], asks: list[tuple[float, float]], now: float) -> None:
        if not self.K:
            return
        if self.last and now - self.last > 45_000:
            self.st = {}
        self.last = now
        while self.tr and self.tr[0][0] < now - WT_WIN - 5000:
            self.tr.pop(0)
        lo, hi = min(self.K) - 850, max(self.K) + 850
        bA = [[0.0] * 17 for _ in self.K]
        aA = [[0.0] * 17 for _ in self.K]
        bb, eb, ba, ea = -math.inf, math.inf, math.inf, -math.inf
        for p, q in bids:
            bb, eb = max(bb, p), min(eb, p)
            if p < lo or p > hi:
                continue
            for n, K in enumerate(self.K):
                i = _jsround((p - K) / 100.0)
                if -8 <= i <= 8:
                    bA[n][i + 8] += q
        for p, q in asks:
            ba, ea = min(ba, p), max(ea, p)
            if p < lo or p > hi:
                continue
            for n, K in enumerate(self.K):
                i = _jsround((p - K) / 100.0)
                if -8 <= i <= 8:
                    aA[n][i + 8] += q
        if not (bb > 0 and ba < math.inf):
            return
        mid = (bb + ba) / 2.0
        for n, K in enumerate(self.K):
            ask = K > mid
            b = (aA if ask else bA)[n]
            inside = (lambda a, z: a >= ba and z <= ea) if ask else (lambda a, z: z <= bb and a >= eb)
            if abs(K - mid) <= WT_HALF:
                self.st[K] = WallState(kind="at")
                continue
            if not inside(K - WT_HALF, K + WT_HALF):
                self.st[K] = WallState(kind="out")
                continue
            nb = [b[j + 8] for j in range(-8, 9) if j and inside(K + 100 * j - WT_HALF, K + 100 * j + WT_HALF)]
            if len(nb) < 2:
                self.st[K] = WallState(kind="out")
                continue
            nb.sort()
            half = len(nb) // 2
            base = max(0.5, nb[half] if len(nb) % 2 else (nb[half - 1] + nb[half]) / 2.0)
            s = self.st.get(K)
            if s is None or s.kind != "ok" or s.ask != ask:
                s = self.st[K] = WallState(kind="ok", ask=ask)
            z = b[8]
            s.z, s.ratio = z, z / base
            s.hist.append((now, z))
            while now - s.hist[0][0] > WT_WIN:
                s.hist.pop(0)
            peak = max(h[1] for h in s.hist)
            is_wall = z >= WT_MIN and s.ratio >= (WT_END if s.wall else WT_WALL)
            if is_wall and not s.wall:
                s.wall, s.since, s.lb, s.ev = True, now, s.fresh, None
            elif not is_wall and s.wall:
                gone = peak - z
                if gone >= 0.5 * peak:
                    kind = "eaten" if self.traded(K, ask, now - WT_WIN) >= 0.5 * gone else "pulled"
                    s.ev = {"t": now, "was": peak, "type": kind}
                s.wall, s.since = False, 0.0
            s.fresh, s.abs = False, False
            if s.wall:
                tr = self.traded(K, ask, max(now - WT_WIN, s.since))
                if tr >= max(3.0, 0.15 * z) and z >= 0.75 * peak:
                    s.abs, s.abs_btc = True, tr


def _fmt_btc(v: float) -> str:
    return f"{v:,.0f}" if v >= 100 else (f"{v:.0f}" if v >= 10 else f"{v:.1f}")


def _age(ms: float) -> str:
    s = max(0, round(ms / 1000.0))
    return f"{s}s" if s < 60 else (f"{s // 60}m" if s < 3600 else f"{s // 3600}h{(s % 3600) // 60:02d}m")


def wall_text(s: Optional[WallState], now: float) -> str:
    if s is None:
        return "Reading the order book..."
    if s.kind == "at":
        return "Price is at this level"
    if s.kind == "out":
        return "Beyond the order book we can see"
    if s.ev and not s.wall and now - s.ev["t"] < WT_EV:
        return f"{s.ev['type'].capitalize()} {_age(now - s.ev['t'])} ago - was {_fmt_btc(s.ev['was'])} BTC"
    if s.wall:
        if s.abs:
            return f"Absorbing - {_fmt_btc(s.abs_btc)} BTC traded, {_fmt_btc(s.z)} left"
        age = now - s.since
        a = _age(age) + ("+" if s.lb else "")
        return (f"New wall - {_fmt_btc(s.z)} BTC - {a}" if age < WT_NEW and not s.lb
                else f"Wall - {_fmt_btc(s.z)} BTC - {a}")
    return f"No wall - {_fmt_btc(s.z)} BTC"


# =============================================================== the service
class LevelService:
    def __init__(self, on_update: Optional[Callable[[dict], None]] = None) -> None:
        self.refresh_seconds = _env_f("LEVEL_REFRESH_SECONDS", 120.0)
        self.ticker_count = int(_env_f("LEVEL_TICKER_COUNT", 300))
        self.ticker_rps = max(1.0, min(15.0, _env_f("LEVEL_TICKER_RPS", 8.0)))
        self.min_coverage = _env_f("LEVEL_MIN_COVERAGE", 0.8)
        self.max_age = _env_f("LEVEL_MAX_AGE_SECONDS", 600.0)
        self.walls = WallWatch()
        self.levelset: Optional[LevelSet] = None
        self.bids: dict[float, float] = {}
        self.asks: dict[float, float] = {}
        self.book_ready = False
        self.book_updated = 0.0
        self.mid: Optional[float] = None
        self._tasks: list[asyncio.Task] = []
        self._on_update = on_update
        self.touches = TouchLog()

    # ---- lifecycle
    def start(self) -> None:
        loop = asyncio.get_running_loop()
        self._tasks = [loop.create_task(self._levels_loop()), loop.create_task(self._book_loop()),
                       loop.create_task(self._wall_loop())]

    async def stop(self) -> None:
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks = []

    # ---- Deribit levels
    async def refresh_levels(self) -> LevelSet:
        now = time.time()
        idx = await asyncio.to_thread(_get_json, DERIBIT + "get_index_price?index_name=btc_usd")
        spot = float(idx["result"]["index_price"])
        summ = await asyncio.to_thread(_get_json, DERIBIT + "get_book_summary_by_currency?currency=BTC&kind=option")
        cands = _pick_candidates(summ["result"], spot, now, self.ticker_count)
        if not cands:
            raise RuntimeError("no option candidates from Deribit")
        sem = asyncio.Semaphore(6)
        gap = 1.0 / self.ticker_rps

        async def one(name: str, K: float, cp: str, T: float):
            async with sem:
                await asyncio.sleep(gap * 0.5)
                try:
                    d = (await asyncio.to_thread(_get_json, DERIBIT + "ticker?instrument_name=" + name, 12.0))["result"]
                    gm = (d.get("greeks") or {}).get("gamma")
                    oi = d.get("open_interest")
                    if gm is None or oi is None:
                        return None
                    return {"K": K, "cp": cp, "T": T, "gamma": float(gm), "oi": float(oi)}
                except Exception:
                    return None

        results = []
        for i in range(0, len(cands), 24):
            batch = await asyncio.gather(*[one(*c) for c in cands[i:i + 24]])
            results += [r for r in batch if r]
            await asyncio.sleep(gap * 3)
        ls = compute_levels(spot, results)
        ls.coverage = len(results) / len(cands)
        if ls.coverage < self.min_coverage or not ls.levels:
            ls.degraded = True
            ls.reason = f"only {len(results)}/{len(cands)} Deribit tickers returned greeks"
        self.levelset = ls
        self.walls.set_strikes([l.K for l in ls.levels])
        return ls

    async def _levels_loop(self) -> None:
        while True:
            try:
                ls = await self.refresh_levels()
                log.info("Levels updated: %d levels, calm=%s, coverage=%.0f%%%s", len(ls.levels), ls.calm,
                         ls.coverage * 100, " (DEGRADED: " + ls.reason + ")" if ls.degraded else "")
                self._publish()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.warning("Deribit level refresh failed: %s", exc)
            await asyncio.sleep(self.refresh_seconds)

    # ---- Coinbase book
    async def _book_loop(self) -> None:
        import websockets
        tries = 0
        while True:
            try:
                async with websockets.connect(COINBASE_WS, max_size=2 ** 26, ping_interval=20) as ws:
                    await ws.send(json.dumps({"type": "subscribe", "product_ids": ["BTC-USD"],
                                              "channels": ["matches", "level2_batch"]}))
                    async for raw in ws:
                        tries = 0
                        self.handle_cb_message(json.loads(raw), time.time() * 1000.0)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.warning("Coinbase book feed dropped: %s", exc)
            self.book_ready = False
            tries += 1
            await asyncio.sleep(min(30.0, 2.0 * tries))

    def handle_cb_message(self, m: dict, now_ms: float) -> None:
        t = m.get("type")
        if t == "snapshot":
            self.bids = {float(p): float(q) for p, q, *_ in m.get("bids", [])}
            self.asks = {float(p): float(q) for p, q, *_ in m.get("asks", [])}
            self.book_ready = True
        elif t == "l2update" and self.book_ready:
            for side, p, q in m.get("changes", []):
                book, p, q = (self.bids if side == "buy" else self.asks), float(p), float(q)
                if q > 0:
                    book[p] = q
                else:
                    book.pop(p, None)
        elif t == "match":
            # side = the RESTING order's side: a resting buy being hit means the taker sold
            self.walls.trade(now_ms, float(m["price"]), float(m["size"]), m.get("side") == "buy")
            return
        else:
            return
        self.book_updated = now_ms / 1000.0

    def _trim_and_snapshot(self):
        if not (self.book_ready and self.bids and self.asks):
            return None
        bb, ba = max(self.bids), min(self.asks)
        mid = (bb + ba) / 2.0
        lim = mid * 0.08
        if len(self.bids) + len(self.asks) > 30000:      # keep memory bounded
            self.bids = {p: q for p, q in self.bids.items() if p >= mid - mid * 0.1}
            self.asks = {p: q for p, q in self.asks.items() if p <= mid + mid * 0.1}
        self.mid = mid
        return ([(p, q) for p, q in self.bids.items() if p >= mid - lim],
                [(p, q) for p, q in self.asks.items() if p <= mid + lim])

    async def _wall_loop(self) -> None:
        while True:
            try:
                snap = self._trim_and_snapshot()
                if snap:
                    self.walls.sample(snap[0], snap[1], time.time() * 1000.0)
                    self._publish()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("wall sampling failed")
            await asyncio.sleep(2.0)

    # ---- tiers / verdict (the dashboard's gvTier + gvPaint)
    def tier(self, K: float, now_ms: Optional[float] = None) -> dict:
        now_ms = time.time() * 1000.0 if now_ms is None else now_ms
        ls = self.levelset
        lvl = next((l for l in ls.levels if l.K == K), None) if ls else None
        if lvl is None:
            return {"tier": None}
        s = self.walls.st.get(K)
        big = lvl.share >= 0.5
        solid = bool(s and s.kind == "ok" and s.wall and (now_ms - s.since >= WT_NEW or s.lb))
        gone = bool(s and s.kind == "ok" and s.ev and not s.wall and now_ms - s.ev["t"] < WT_EV)
        if gone:
            return {"tier": "bad", "level": lvl, "state": s, "why": f"wall was {s.ev['type']}"}
        tier = "strong" if big and solid else ("mid" if big or solid else "weak")
        return {"tier": tier, "level": lvl, "state": s, "big": big, "solid": solid}

    def verdict(self, price: float, now_ms: Optional[float] = None) -> dict:
        ls = self.levelset
        if not ls or not ls.levels or not price > 0:
            return {"kind": "none", "why": "no levels"}
        n = min(ls.levels, key=lambda l: abs(l.K - price))
        d = abs(n.K - price) / price * 100.0
        above = n.K > price
        g = self.tier(n.K, now_ms)
        base = {"level": n, "d_pct": d, "above": above, "tier": g["tier"], "state": g.get("state"), "calm": ls.calm}
        if d > GV_NEAR:
            return {**base, "kind": "none", "why": f"no level close (nearest {n.name} {n.K:,.0f}, {d:.1f}%)"}
        if g["tier"] == "bad":
            return {**base, "kind": "distrust", "why": f"{n.name} {n.K:,.0f}: {g['why']}"}
        if g["tier"] == "strong":
            if ls.calm:
                return {**base, "kind": "rejection" if above else "bounce", "why": f"{n.name} {n.K:,.0f} strong"}
            return {**base, "kind": "react_wild", "why": f"{n.name} strong but net gamma is negative"}
        return {**base, "kind": "soft", "why": f"{n.name} {n.K:,.0f} is {g['tier']} (needs gamma AND a 3m+ wall)"}

    # ---- state for /api/state
    def snapshot(self) -> dict:
        ls, now_ms = self.levelset, time.time() * 1000.0
        out: dict[str, Any] = {"book_ready": self.book_ready, "mid": self.mid,
                               "book_age_s": (time.time() - self.book_updated) if self.book_updated else None,
                               "touch_stats": self.touches.stats()}
        if ls:
            out.update({"spot": ls.spot, "calm": ls.calm, "total_net_musd": round(ls.total_net / 1e6, 1),
                        "flip": ls.flip, "age_s": round(time.time() - ls.updated_at, 1),
                        "coverage": round(ls.coverage, 3), "degraded": ls.degraded, "reason": ls.reason,
                        "source": ls.source,
                        "levels": [{"name": l.name, "K": l.K, "gamma_musd": round(l.gamma / 1e6, 2),
                                    "share": round(l.share, 2), "tier": self.tier(l.K, now_ms)["tier"],
                                    "wall": wall_text(self.walls.st.get(l.K), now_ms)} for l in ls.levels]})
            if self.mid:
                v = self.verdict(self.mid, now_ms)
                out["verdict"] = {"kind": v["kind"], "why": v.get("why")}
        return out

    def _publish(self) -> None:
        if self._on_update:
            try:
                self._on_update(self.snapshot())
            except Exception:
                log.exception("levels publish failed")


# =============================================================== research log: do STRONG levels actually hold?
class TouchLog:
    """Logs every time price touches ANY gamma level, tagged with its tier, and what price did next.
    This answers 'does STRONG mean anything?' using the bot's own data. Written to level_touches.jsonl."""
    HORIZONS = (15, 30, 60)

    def __init__(self) -> None:
        self.path = os.getenv("LEVEL_LOG_PATH", "level_touches.jsonl")
        self.zone = _env_f("LEVEL_TOUCH_LOG_ZONE", 60.0)
        self.pending: list[dict] = []
        self.last_touch: dict[float, float] = {}
        self.agg: dict[str, dict[str, float]] = {}

    def on_candle(self, svc: LevelService, candle: dict, perp_price: float, now: float) -> None:
        ls = svc.levelset
        if not ls or ls.degraded or not svc.mid:
            return
        basis = perp_price - svc.mid
        for p in list(self.pending):
            p["path"].append((now, float(candle["close"])))
            if now - p["t"] >= 60 * 60:
                self._finish(p)
                self.pending.remove(p)
        hi, lo, close = float(candle["high"]), float(candle["low"]), float(candle["close"])
        for l in ls.levels:
            Kp = l.K + basis
            if lo <= Kp + self.zone and hi >= Kp - self.zone and now - self.last_touch.get(l.K, 0) > 1800:
                t = svc.tier(l.K)
                st = t.get("state")
                self.last_touch[l.K] = now
                self.pending.append({"t": now, "level": l.name, "K": l.K, "tier": t["tier"], "calm": ls.calm,
                                     "share": round(l.share, 2), "wall_btc": round(st.z, 1) if st and st.kind == "ok" else None,
                                     "ref": close, "side": 1 if close >= Kp else -1, "path": []})

    def _finish(self, p: dict) -> None:
        out = {k: p[k] for k in ("level", "K", "tier", "calm", "share", "wall_btc", "side")}
        out["time"] = datetime.fromtimestamp(p["t"], tz=timezone.utc).isoformat()
        for h in self.HORIZONS:
            pts = [c for ts, c in p["path"] if ts - p["t"] >= h * 60 - 30]
            out[f"move_{h}m"] = round((pts[0] - p["ref"]) * p["side"], 1) if pts else None
        try:
            with open(self.path, "a", encoding="utf-8") as fh:
                fh.write(json.dumps(out) + "\n")
        except OSError:
            log.warning("could not write %s", self.path)
        a = self.agg.setdefault(str(out["tier"]), {"n": 0, "sum30": 0.0, "held30": 0})
        if out.get("move_30m") is not None:
            a["n"] += 1
            a["sum30"] += out["move_30m"]
            a["held30"] += 1 if out["move_30m"] > 0 else 0

    def stats(self) -> dict:
        return {k: {"touches": int(v["n"]), "avg_move_30m": round(v["sum30"] / v["n"], 1) if v["n"] else None,
                    "pct_held_30m": round(100.0 * v["held30"] / v["n"]) if v["n"] else None}
                for k, v in self.agg.items()}


# =============================================================== the trading rule
class LevelEngine:
    """Bounce / rejection entries at STRONG gamma levels. Paper only. Evaluated once per closed 1m candle."""

    def __init__(self, svc: LevelService, symbol: str) -> None:
        self.svc, self.symbol = svc, symbol
        self.zone = _env_f("LEVEL_ZONE", 90.0)              # max distance from the level at entry
        self.touch = _env_f("LEVEL_TOUCH", 70.0)            # candle must have come within this of the level
        self.buffer = _env_f("LEVEL_BUFFER", 70.0)          # stop beyond the level
        self.min_stop = _env_f("LEVEL_MIN_STOP", 100.0)
        self.max_stop = _env_f("LEVEL_MAX_STOP", 300.0)
        self.min_rr = _env_f("LEVEL_MIN_RR", 1.5)
        self.min_delta_pct = _env_f("LEVEL_MIN_DELTA_PCT", 3.0)
        self.level_cooldown = _env_f("LEVEL_COOLDOWN_SECONDS", 1800.0)
        self.streak_limit = int(_env_f("LEVEL_STREAK_LIMIT", 2))
        self.streak_pause = _env_f("LEVEL_STREAK_PAUSE_SECONDS", 3600.0)
        self.max_basis = _env_f("LEVEL_MAX_BASIS", 300.0)
        self._level_block: dict[float, float] = {}
        self._losses, self._pause_until = 0, 0.0

    def record_result(self, net_pnl: float, ts: float, level_k: Optional[float]) -> None:
        if level_k is not None:
            self._level_block[float(level_k)] = float(ts) + self.level_cooldown
        if net_pnl < 0:
            self._losses += 1
            if self._losses >= self.streak_limit:
                self._pause_until, self._losses = float(ts) + self.streak_pause, 0
        else:
            self._losses = 0

    def evaluate(self, *, price: float, delta_bar, candles: list[dict], now: float,
                 has_open_position: bool) -> StrategySignal:
        def wait(why: str) -> StrategySignal:
            return StrategySignal(SignalSide.WAIT, now, self.symbol, price, 0, "NONE", why)

        svc, ls = self.svc, self.svc.levelset
        if has_open_position:
            return wait("Position already open")
        if ls is None:
            return wait("Levels: waiting for Deribit option data")
        if ls.degraded:
            return wait(f"Levels degraded: {ls.reason}")
        if time.time() - ls.updated_at > self.svc.max_age:
            return wait("Levels are stale")
        if not svc.book_ready or not svc.mid or time.time() - svc.book_updated > 30:
            return wait("Coinbase order book not live (walls unknown)")
        if now < self._pause_until:
            return wait(f"Loss-streak pause ({int((self._pause_until - now) / 60) + 1} min left)")
        if delta_bar is None or len(candles) < 16:
            return wait("Not enough candle data")
        basis = price - svc.mid
        if abs(basis) > self.max_basis:
            return wait(f"Deribit and Coinbase prices disagree by ${basis:,.0f}")
        v = svc.verdict(svc.mid, now * 1000.0)
        if v["kind"] not in ("bounce", "rejection"):
            return wait(f"Levels: {v.get('why', v['kind'])}")
        lvl: Level = v["level"]
        if now < self._level_block.get(lvl.K, 0.0):
            return wait(f"{lvl.name} {lvl.K:,.0f} on cooldown after a recent trade")
        up = v["kind"] == "bounce"
        K = lvl.K + basis                                       # level expressed in Deribit-perp prices
        last = candles[-1]
        lo, hi, op, cl = (float(last[k]) for k in ("low", "high", "open", "close"))
        if up:
            if not (lo <= K + self.touch and cl > K and cl > op):
                return wait(f"Bounce at {lvl.name}: no bullish rejection candle yet")
            if price - K > self.zone:
                return wait(f"Bounce at {lvl.name}: price already ${price - K:,.0f} above the level")
        else:
            if not (hi >= K - self.touch and cl < K and cl < op):
                return wait(f"Rejection at {lvl.name}: no bearish rejection candle yet")
            if K - price > self.zone:
                return wait(f"Rejection at {lvl.name}: price already ${K - price:,.0f} below the level")
        delta, total = float(delta_bar.delta), float(delta_bar.total_volume or 0.0)
        pct = delta / total * 100.0 if total else 0.0
        if (delta * (1 if up else -1) <= 0) or abs(pct) < self.min_delta_pct:
            return wait(f"{lvl.name}: footprint delta does not support the {'bounce' if up else 'rejection'}")
        trs = [max(float(c["high"]) - float(c["low"]), abs(float(c["high"]) - float(p["close"])),
                   abs(float(c["low"]) - float(p["close"]))) for p, c in zip(candles[-15:-1], candles[-14:])]
        atr = max(sum(trs) / len(trs), 1.0)
        buf = max(self.buffer, 0.8 * atr)
        stop_level = min(K - buf, lo - 0.3 * atr) if up else max(K + buf, hi + 0.3 * atr)
        risk = max(self.min_stop, abs(price - stop_level))
        if risk > self.max_stop:
            return wait(f"{lvl.name}: stop would be ${risk:,.0f} away (max {self.max_stop:,.0f})")
        others = [l.K + basis for l in svc.levelset.levels if l.K != lvl.K]
        if ls.flip:
            others.append(ls.flip + basis)
        beyond = sorted(k for k in others if (k > price if up else k < price))
        if beyond:
            nxt = beyond[0] if up else beyond[-1]
            reward = 0.85 * abs(nxt - price)
        else:
            reward = 2.0 * risk
        reward = min(reward, 4.0 * risk)
        if reward / risk < self.min_rr:
            return wait(f"{lvl.name}: next level too close (reward/risk {reward / risk:.2f} < {self.min_rr})")
        st = v.get("state")
        evidence = {"level": lvl.name, "level_k": lvl.K, "level_perp_equiv": round(K, 1),
                    "gamma_share": round(lvl.share, 2), "gamma_musd": round(lvl.gamma / 1e6, 2),
                    "wall_btc": round(st.z, 1) if st else None,
                    "wall_age_s": round((now * 1000.0 - st.since) / 1000.0) if st and st.since else None,
                    "net_gamma_calm": ls.calm, "delta_btc": delta, "delta_percent": pct, "atr": round(atr, 1),
                    "basis": round(basis, 1), "next_level_reward_rr": round(reward / risk, 2)}
        tag = "LEVEL_BOUNCE" if up else "LEVEL_REJECT"
        return StrategySignal(SignalSide.BUY if up else SignalSide.SELL, now, self.symbol, price, 4, tag,
            f"{tag}: {lvl.name} {lvl.K:,.0f} strong (gamma + {st.z:.0f} BTC wall), calm zone, rejection candle",
            price - risk if up else price + risk, price + reward if up else price - reward,
            risk, reward, evidence)
