"""
BTC Pulse analytics core.

Source audit: index-4-final-v10-1.html
- GEX: lines 1450-1478 (Black-Scholes-style gamma exposure estimate; dealer positioning
  is an explicit assumption, not observed dealer inventory).
- Wall tracking: lines 1555-1604.
- Footprint delta: lines 2258-2289.
- Swing S/R scoring: lines 2584-2636.

This module does not fetch data. Callers must supply timestamped exchange observations.
Do not pass candle bars that were not yet closed at the decision timestamp.
All outputs include provenance/quality information where applicable.
"""
from __future__ import annotations

from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from enum import Enum
import math
from statistics import median
from typing import Iterable, Mapping, Optional, Sequence


class DataQualityError(ValueError):
    """Raised when an observation is malformed or its provenance is ambiguous."""


class Provenance(str, Enum):
    EXCHANGE = "exchange_observed"
    DERIVED = "derived_from_exchange_data"
    MODEL = "modeled_estimate"


def _finite_positive(name: str, value: float) -> float:
    value = float(value)
    if not math.isfinite(value) or value <= 0:
        raise DataQualityError(f"{name} must be finite and > 0")
    return value


def _normal_pdf(x: float) -> float:
    return math.exp(-0.5 * x * x) / math.sqrt(2.0 * math.pi)


def _normal_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


@dataclass(frozen=True)
class OptionObservation:
    """One Deribit option snapshot row; values must come from an actual API response.

    option_type: 'C'/'P' or 'call'/'put'
    mark_iv: volatility as a fraction (e.g. 0.55), NOT percent points.
    open_interest: contracts, as returned by the source. The HTML model uses it
    directly; contract multiplier/unit conversion is intentionally not guessed.
    expiry_utc: timezone-aware UTC expiry timestamp.
    """
    strike: float
    option_type: str
    mark_iv: float
    open_interest: float
    expiry_utc: datetime
    source: str = "Deribit"
    observed_at: Optional[datetime] = None

    def validate(self) -> None:
        _finite_positive("strike", self.strike)
        _finite_positive("mark_iv", self.mark_iv)
        if not math.isfinite(self.open_interest) or self.open_interest < 0:
            raise DataQualityError("open_interest must be finite and >= 0")
        if self.option_type.lower() not in {"c", "p", "call", "put"}:
            raise DataQualityError("option_type must be call/put or C/P")
        if self.expiry_utc.tzinfo is None or self.expiry_utc.utcoffset() is None:
            raise DataQualityError("expiry_utc must be timezone-aware")
        if self.observed_at and (self.observed_at.tzinfo is None or self.observed_at.utcoffset() is None):
            raise DataQualityError("observed_at must be timezone-aware")




def index_option_snapshot(rows: Iterable[tuple[str, OptionObservation]]) -> dict[str, OptionObservation]:
    """Index one complete option poll by instrument name, retaining one row per contract.

    If malformed upstream data repeats an instrument, the last observation wins so
    the same contract cannot contribute gamma more than once to a snapshot.
    """
    indexed: dict[str, OptionObservation] = {}
    for instrument, observation in rows:
        name = str(instrument or "").strip()
        if not name or not isinstance(observation, OptionObservation):
            continue
        indexed[name] = observation
    return indexed


@dataclass(frozen=True)
class GammaLevel:
    strike: float
    call_gamma: float
    put_gamma: float
    net_gamma: float
    provenance: str = Provenance.MODEL.value


@dataclass(frozen=True)
class GammaExposure:
    spot: float
    total_net_gamma: float
    regime: str
    call_wall: Optional[float]
    put_wall: Optional[float]
    flip_level: Optional[float]
    levels: tuple[GammaLevel, ...]
    provenance: str = Provenance.MODEL.value
    assumptions: tuple[str, ...] = (
        "Dealer positioning is assumed long calls and short puts.",
        "Open interest and mark IV are observed inputs; dealer inventory is not observed.",
        "Formula mirrors the HTML model and its unit convention; it is not a verified USD P&L measure.",
    )


def _flip_level(levels: Sequence[GammaLevel], spot: float) -> Optional[float]:
    cumulative = 0.0
    best_distance = math.inf
    flip = None
    for i, level in enumerate(levels):
        previous = cumulative
        cumulative += level.net_gamma
        if i and previous * cumulative < 0:
            distance = abs(level.strike - spot)
            if distance < best_distance:
                best_distance, flip = distance, level.strike
    return flip


def calculate_gamma_exposure(
    options: Iterable[OptionObservation],
    spot: float,
    as_of: datetime,
    max_days: float = 60.0,
    max_moneyness_fraction: float = 0.25,
) -> GammaExposure:
    """Compute the HTML's modeled dealer GEX by strike.

    HTML formula:
      d1 = (ln(S/K) + 0.5 * iv^2 * T) / (iv * sqrt(T))
      g = exp(-d1^2/2)/sqrt(2*pi)/(S*iv*sqrt(T))*OI*S^2*0.01
      net strike exposure = call g - put g
    The HTML includes only expiries <=60 days and strikes within 25% of spot
    for the displayed strike table. Expired/invalid rows are ignored.
    """
    spot = _finite_positive("spot", spot)
    if as_of.tzinfo is None or as_of.utcoffset() is None:
        raise DataQualityError("as_of must be timezone-aware")
    calls: dict[float, float] = {}
    puts: dict[float, float] = {}
    max_seconds = max_days * 86400.0
    for opt in options:
        opt.validate()
        if opt.source.lower() != "deribit":
            # The analytical foundation specifically describes Deribit options.
            raise DataQualityError(f"Expected Deribit option data, got {opt.source!r}")
        seconds = (opt.expiry_utc - as_of).total_seconds()
        if seconds <= 0 or seconds > max_seconds or opt.open_interest <= 0:
            continue
        if abs(opt.strike / spot - 1.0) > max_moneyness_fraction:
            continue
        T = seconds / (365.0 * 86400.0)
        iv = opt.mark_iv
        sq = iv * math.sqrt(T)
        d1 = (math.log(spot / opt.strike) + 0.5 * iv * iv * T) / sq
        # Preserve the HTML's formula and 0.01 multiplier exactly.
        g = _normal_pdf(d1) / (spot * sq) * opt.open_interest * spot * spot * 0.01
        target = calls if opt.option_type.lower() in {"c", "call"} else puts
        target[opt.strike] = target.get(opt.strike, 0.0) + g

    strikes = sorted(set(calls) | set(puts))
    levels = tuple(
        GammaLevel(k, calls.get(k, 0.0), puts.get(k, 0.0),
                   calls.get(k, 0.0) - puts.get(k, 0.0))
        for k in strikes
    )
    if not levels:
        raise DataQualityError("No valid in-range Deribit option rows; GEX unavailable, not zero.")
    total = sum(x.net_gamma for x in levels)
    above = [x for x in levels if x.strike > spot]
    below = [x for x in levels if x.strike < spot]
    call_wall = max(above, key=lambda x: x.call_gamma).strike if above else None
    put_wall = max(below, key=lambda x: x.put_gamma).strike if below else None
    return GammaExposure(
        spot=spot,
        total_net_gamma=total,
        regime="Calm" if total > 0 else "Wild",
        call_wall=call_wall,
        put_wall=put_wall,
        flip_level=_flip_level(levels, spot),
        levels=levels,
    )


@dataclass(frozen=True)
class BookLevel:
    price: float
    size_btc: float


@dataclass(frozen=True)
class AggressorTrade:
    timestamp: float
    price: float
    size_btc: float
    side: str  # "buy" means buyer-initiated; "sell" means seller-initiated
    provenance: str = Provenance.EXCHANGE.value


class WallState(str, Enum):
    UNKNOWN = "Unknown"
    NO_WALL = "No wall"
    NEW = "New"
    WALL = "Wall"
    ABSORBING = "Absorbing"
    PULLED = "Pulled"
    EATEN = "Eaten"
    AT_PRICE = "Price is at this level"
    OUT_OF_BOOK = "Beyond visible order book"


@dataclass
class _WallMemory:
    side: str
    first_seen: float
    history: list[tuple[float, float]] = field(default_factory=list)
    was_wall: bool = False
    event: Optional[tuple[float, str, float]] = None  # timestamp, type, peak size


@dataclass(frozen=True)
class WallObservation:
    strike: float
    side: str
    size_btc: Optional[float]
    relative_size: Optional[float]
    state: WallState
    age_seconds: Optional[float]
    traded_into_btc: float
    provenance: str = Provenance.DERIVED.value
    note: str = ""


class OrderBookWallTracker:
    """Tracks visible walls at specified strikes from repeated live book snapshots.

    HTML thresholds preserved:
      strike zone +/- $50; 17 x $100 buckets; wall >=10 BTC and >=2.5x
      median of visible neighboring buckets; keep existing wall down to 1.6x;
      120s rolling size history; "new" for <180s; event display horizon 300s.
      Absorbing: traded >= max(3 BTC, 15% of remaining wall) and >=75% of
      rolling peak remains. Pulled/eaten: size drops >=50% from rolling peak;
      classify eaten if >=50% of vanished size traded into the level, otherwise pulled.
    Trade side must be true aggressor side from the feed. Do not infer it from
    candle direction. Missing trade history yields zero observed trades, not synthetic volume.
    """
    HALF = 50.0
    WALL_RATIO = 2.5
    KEEP_RATIO = 1.6
    MIN_SIZE = 10.0
    WINDOW = 120.0
    NEW_SECONDS = 180.0
    EVENT_SECONDS = 300.0

    def __init__(self, strikes: Sequence[float]):
        self.strikes = sorted(set(float(k) for k in strikes))
        self._memory: dict[float, _WallMemory] = {}
        self._trades: list[AggressorTrade] = []
        self._last_sample: Optional[float] = None

    def add_trade(self, trade: AggressorTrade) -> None:
        if trade.side not in {"buy", "sell"}:
            raise DataQualityError("trade side must be aggressor-side buy/sell")
        if trade.provenance != Provenance.EXCHANGE.value:
            raise DataQualityError("Wall classification requires exchange-observed trades")
        if trade.timestamp < 0 or trade.price <= 0 or trade.size_btc < 0:
            raise DataQualityError("invalid trade")
        self._trades.append(trade)
        cutoff = trade.timestamp - self.WINDOW - 5.0
        self._trades = [t for t in self._trades if t.timestamp >= cutoff]

    def _traded_into(self, strike: float, book_side: str, since: float, now: float) -> float:
        # Ask walls are consumed by buyer-initiated trades; bid walls by seller-initiated trades.
        aggressor = "buy" if book_side == "ask" else "sell"
        return sum(t.size_btc for t in self._trades
                   if since <= t.timestamp <= now and t.side == aggressor
                   and abs(t.price - strike) <= self.HALF)

    @staticmethod
    def _levels(raw: Iterable[BookLevel | Sequence[float]]) -> list[BookLevel]:
        result = []
        for item in raw:
            if isinstance(item, BookLevel):
                level = item
            else:
                level = BookLevel(float(item[0]), float(item[1]))
            if level.price <= 0 or level.size_btc < 0 or not math.isfinite(level.price + level.size_btc):
                raise DataQualityError("invalid book level")
            result.append(level)
        return result

    def sample(
        self, bids: Iterable[BookLevel | Sequence[float]],
        asks: Iterable[BookLevel | Sequence[float]], timestamp: float,
    ) -> list[WallObservation]:
        bids, asks = self._levels(bids), self._levels(asks)
        if not bids or not asks:
            raise DataQualityError("empty order book: wall state is unknown, not no-wall")
        if self._last_sample is not None and timestamp <= self._last_sample:
            raise DataQualityError("book timestamps must increase monotonically")
        if self._last_sample is not None and timestamp - self._last_sample > 45:
            # Same as HTML: clear history after a long gap; never imply continuous observation.
            self._memory.clear()
        self._last_sample = timestamp
        self._trades = [t for t in self._trades if t.timestamp >= timestamp - self.WINDOW - 5]
        best_bid, best_ask = max(x.price for x in bids), min(x.price for x in asks)
        mid = (best_bid + best_ask) / 2
        out: list[WallObservation] = []
        for strike in self.strikes:
            if abs(strike - mid) <= self.HALF:
                out.append(WallObservation(strike, "unknown", None, None, WallState.AT_PRICE, None, 0))
                continue
            side = "ask" if strike > mid else "bid"
            visible = asks if side == "ask" else bids
            lo, hi = min(x.price for x in visible), max(x.price for x in visible)
            zone_lo, zone_hi = strike - self.HALF, strike + self.HALF
            if zone_lo < lo or zone_hi > hi:
                out.append(WallObservation(strike, side, None, None, WallState.OUT_OF_BOOK, None, 0))
                continue

            buckets = [0.0] * 17
            for level in visible:
                idx = round((level.price - strike) / 100.0)
                if -8 <= idx <= 8:
                    buckets[idx + 8] += level.size_btc
            neighbors = []
            for j in range(-8, 9):
                if j == 0:
                    continue
                a, z = strike + 100 * j - self.HALF, strike + 100 * j + self.HALF
                if a >= lo and z <= hi:
                    neighbors.append(buckets[j + 8])
            if len(neighbors) < 2:
                out.append(WallObservation(strike, side, None, None, WallState.OUT_OF_BOOK, None, 0))
                continue
            base = max(0.5, median(neighbors))
            size = buckets[8]
            mem = self._memory.get(strike)
            if mem is None or mem.side != side:
                mem = _WallMemory(side=side, first_seen=timestamp)
                self._memory[strike] = mem
            mem.history.append((timestamp, size))
            mem.history = [(t, z) for t, z in mem.history if t >= timestamp - self.WINDOW]
            peak = max((z for _, z in mem.history), default=size)
            is_wall = size >= self.MIN_SIZE and size / base >= (self.KEEP_RATIO if mem.was_wall else self.WALL_RATIO)
            if is_wall and not mem.was_wall:
                mem.first_seen = timestamp
                mem.event = None
            elif not is_wall and mem.was_wall:
                gone = peak - size
                if peak > 0 and gone >= 0.5 * peak:
                    traded = self._traded_into(strike, side, timestamp - self.WINDOW, timestamp)
                    mem.event = (timestamp, "eaten" if traded >= 0.5 * gone else "pulled", peak)
            mem.was_wall = is_wall
            since = mem.first_seen if is_wall else timestamp - self.WINDOW
            traded = self._traded_into(strike, side, max(timestamp - self.WINDOW, since), timestamp) if is_wall else 0.0
            age = max(0.0, timestamp - mem.first_seen) if is_wall else None
            if is_wall and traded >= max(3.0, 0.15 * size) and size >= 0.75 * peak:
                state = WallState.ABSORBING
            elif is_wall and age is not None and age < self.NEW_SECONDS:
                state = WallState.NEW
            elif is_wall:
                state = WallState.WALL
            elif mem.event and timestamp - mem.event[0] < self.EVENT_SECONDS:
                state = WallState.EATEN if mem.event[1] == "eaten" else WallState.PULLED
            else:
                state = WallState.NO_WALL
            out.append(WallObservation(strike, side, size, size / base, state, age, traded,
                                       note="Observed book depth plus observed aggressor trades."))
        return out


@dataclass(frozen=True)
class DeltaBar:
    start_timestamp: float
    end_timestamp: float
    buy_volume: float
    sell_volume: float
    delta: float
    total_volume: float
    delta_percent: Optional[float]
    provenance: str = Provenance.DERIVED.value


class FootprintDelta:
    """Builds footprint bars from actual trades with aggressor-side labels."""
    def __init__(self, timeframe_seconds: int = 60, price_bin: float = 10.0):
        if timeframe_seconds <= 0 or price_bin <= 0:
            raise ValueError("timeframe_seconds and price_bin must be > 0")
        self.timeframe_seconds = timeframe_seconds
        self.price_bin = price_bin
        self._bars: dict[int, dict] = {}

    def add_trade(self, trade: AggressorTrade) -> None:
        if trade.provenance != Provenance.EXCHANGE.value:
            raise DataQualityError("footprint delta requires exchange-observed trades")
        if trade.side not in {"buy", "sell"} or trade.price <= 0 or trade.size_btc < 0:
            raise DataQualityError("invalid trade")
        key = int(trade.timestamp // self.timeframe_seconds)
        bar = self._bars.setdefault(key, {"buy": 0.0, "sell": 0.0, "levels": {}})
        side = trade.side
        bar[side] += trade.size_btc
        price_level = math.floor(trade.price / self.price_bin) * self.price_bin
        level = bar["levels"].setdefault(price_level, {"buy": 0.0, "sell": 0.0})
        level[side] += trade.size_btc

    def bars(self, as_of_timestamp: float, include_open_bar: bool = False) -> list[DeltaBar]:
        current_key = int(as_of_timestamp // self.timeframe_seconds)
        result = []
        for key in sorted(self._bars):
            if not include_open_bar and key >= current_key:
                continue
            row = self._bars[key]
            buy, sell = row["buy"], row["sell"]
            total, delta = buy + sell, buy - sell
            result.append(DeltaBar(
                key * self.timeframe_seconds, (key + 1) * self.timeframe_seconds,
                buy, sell, delta, total, (delta / total * 100.0) if total else None
            ))
        return result

    def footprint_levels(self, bar_start_timestamp: float) -> dict[float, dict[str, float]]:
        key = int(bar_start_timestamp // self.timeframe_seconds)
        row = self._bars.get(key)
        return {k: dict(v) for k, v in sorted(row["levels"].items(), reverse=True)} if row else {}


@dataclass(frozen=True)
class Candle:
    timestamp: float
    open: float
    high: float
    low: float
    close: float
    volume: float = 0.0
    closed: bool = True
    provenance: str = Provenance.EXCHANGE.value

    def validate(self) -> None:
        if not self.closed:
            raise DataQualityError("S/R must only use closed candles to avoid look-ahead bias")
        if self.provenance != Provenance.EXCHANGE.value:
            raise DataQualityError("S/R input candles must be exchange-observed")
        if not (self.low > 0 and self.high >= max(self.open, self.close, self.low)
                and self.low <= min(self.open, self.close) and self.volume >= 0):
            raise DataQualityError("invalid OHLCV candle")


@dataclass(frozen=True)
class SRZone:
    low: float
    high: float
    midpoint: float
    score: float
    pivot_count: int
    timeframes: tuple[str, ...]
    touches: int
    breaks: int
    high_volume_node: bool
    round_5000: Optional[float]
    flipped: bool
    role: str
    provenance: str = Provenance.DERIVED.value


# timeframe: name, pivot weight, left/right swing window, minimum excursion %
_SR_CONFIG = (("1D", 3.0, 3, 2.0), ("4H", 2.0, 4, 1.0), ("1H", 1.0, 6, 0.6))


def score_support_resistance(
    candles_by_timeframe: Mapping[str, Sequence[Candle]],
    spot: float,
    now_timestamp: float,
) -> list[SRZone]:
    """Replicates the HTML swing-pivot clustering and scoring model.

    Requires closed, real OHLCV candles for 1D/4H/1H. Input sequences must be
    chronological and timestamps must be consistent with each candle's timeframe.
    """
    spot = _finite_positive("spot", spot)
    pivots: list[dict] = []
    checked: dict[str, list[Candle]] = {}
    for name, weight, window, amp in _SR_CONFIG:
        data = list(candles_by_timeframe.get(name, ()))
        if not data:
            continue
        for candle in data:
            candle.validate()
        if any(data[i].timestamp >= data[i + 1].timestamp for i in range(len(data) - 1)):
            raise DataQualityError(f"{name} candles must be strictly chronological")
        checked[name] = data
        for i in range(window, len(data) - window):
            c = data[i]
            neighbors = data[i-window:i] + data[i+1:i+window+1]
            is_high = all(c.high > x.high for x in neighbors)
            is_low = all(c.low < x.low for x in neighbors)
            mn = min(x.low for x in neighbors)
            mx = max(x.high for x in neighbors)
            # HTML's 45-day half-life, floored at 0.25.
            decay = max(0.25, 0.5 ** (max(0.0, now_timestamp - c.timestamp) / 86400.0 / 45.0))
            if is_high and (c.high - mn) / c.high * 100.0 >= amp:
                pivots.append({"p": c.high, "is_high": True, "w": weight * decay, "lab": name})
            if is_low and (mx - c.low) / c.low * 100.0 >= amp:
                pivots.append({"p": c.low, "is_high": False, "w": weight * decay, "lab": name})
    if not pivots:
        return []
    pivots.sort(key=lambda x: x["p"])
    tolerance = spot * 0.004
    clusters: list[list[dict]] = []
    for pivot in pivots:
        if clusters and pivot["p"] - sum(x["p"] for x in clusters[-1]) / len(clusters[-1]) <= tolerance:
            clusters[-1].append(pivot)
        else:
            clusters.append([pivot])

    h1 = checked.get("1H", [])
    bin_size = spot * 0.0025
    volume_profile: dict[int, float] = {}
    for candle in h1:
        a, b = math.floor(candle.low / bin_size), math.floor(candle.high / bin_size)
        count = b - a + 1
        for idx in range(a, b + 1):
            volume_profile[idx] = volume_profile.get(idx, 0.0) + candle.volume / count
    volume_values = sorted(volume_profile.values())
    threshold = volume_values[min(len(volume_values)-1, int(len(volume_values) * 0.8))] if volume_values else 0.0
    zones: list[SRZone] = []
    for cluster in clusters:
        prices = [x["p"] for x in cluster]
        pad = spot * 0.001
        low, high = min(prices) - pad, max(prices) + pad
        if high - low < spot * 0.002:
            mid = (high + low) / 2
            low, high = mid - spot * 0.001, mid + spot * 0.001
        labels = tuple(name for name in ("1D", "4H", "1H") if any(x["lab"] == name for x in cluster))
        touches = breaks = 0
        last_touch = -99
        for i in range(1, max(1, len(h1) - 1)):
            if i >= len(h1) - 1 or i - last_touch < 6 or h1[i].low > high or h1[i].high < low:
                continue
            previous_close = h1[i - 1].close
            direction = 1 if previous_close < low else (-1 if previous_close > high else 0)
            if not direction:
                continue
            last_touch = i
            touches += 1
            for j in range(i, min(len(h1), i + 13)):
                if (direction > 0 and h1[j].close > high * 1.003) or (direction < 0 and h1[j].close < low * 0.997):
                    breaks += 1
                    break
        hvn = any(volume_profile.get(idx, 0.0) >= threshold
                  for idx in range(math.floor(low / bin_size), math.floor(high / bin_size) + 1))
        r5 = math.ceil(low / 5000.0) * 5000.0
        round_5000 = r5 if r5 <= high else None
        round_1000 = math.ceil(low / 1000.0) * 1000.0 <= high
        flipped = any(x["is_high"] for x in cluster) and any(not x["is_high"] for x in cluster)
        score = (sum(x["w"] for x in cluster) + (1.5 if hvn else 0.0)
                 + (1.5 if round_5000 else (0.5 if round_1000 else 0.0))
                 + (1.5 if flipped else 0.0) + 0.4 * (touches - breaks))
        midpoint = (low + high) / 2
        if not (len(cluster) >= 2 or "1D" in labels):
            continue
        if abs(midpoint / spot - 1) >= 0.15 or score < 2.5:
            continue
        zones.append(SRZone(low, high, midpoint, score, len(cluster), labels, touches, breaks,
                            hvn, round_5000, flipped, "resistance" if midpoint > spot else "support"))
    # Preserve best-ranked four above and four at/below spot, as in HTML.
    resistances = sorted((z for z in zones if z.midpoint > spot), key=lambda z: z.score, reverse=True)[:4]
    supports = sorted((z for z in zones if z.midpoint <= spot), key=lambda z: z.score, reverse=True)[:4]
    return sorted(resistances, key=lambda z: z.midpoint) + sorted(supports, key=lambda z: z.midpoint, reverse=True)


def to_dict(value):
    """Convert dataclasses/enums into JSON-serializable primitive structures."""
    def convert(x):
        if hasattr(x, "__dataclass_fields__"):
            return {k: convert(v) for k, v in asdict(x).items()}
        if isinstance(x, Enum):
            return x.value
        if isinstance(x, tuple):
            return [convert(v) for v in x]
        if isinstance(x, list):
            return [convert(v) for v in x]
        if isinstance(x, dict):
            return {str(k): convert(v) for k, v in x.items()}
        return x
    return convert(value)
