import os, sys, time, math
import logging
import logging.handlers
from dotenv import load_dotenv
load_dotenv()
import schedule
import asyncio
from typing import List, Dict, Tuple, Optional
from bfxapi import Client
from bfxapi.types import Wallet, FundingCredit, FundingOffer
import ssl
import certifi
import aiohttp

_AIOHTTP_SSL = ssl.create_default_context(cafile=certifi.where())

# Log rotation: compressed, keep 30 days
_LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
os.makedirs(_LOG_DIR, exist_ok=True)
_log_handler = logging.handlers.TimedRotatingFileHandler(
    filename=os.path.join(_LOG_DIR, "bot.log"),
    when="midnight",
    interval=1,
    backupCount=30,
    encoding="utf-8",
)
_log_handler.namer = lambda name: name + ".gz"
_log_handler.rotator = lambda source, dest: (
    __import__("gzip").open(dest, "wb").write(__import__("pathlib").Path(source).read_bytes())
    or __import__("pathlib").Path(source).unlink()
)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(message)s",
    handlers=[_log_handler, logging.StreamHandler()],
)
log = logging.info

# =============================================================================
# Constants
# =============================================================================

BITFINEX_PUBLIC_API_URL = "https://api-pub.bitfinex.com"
DEFAULT_FUND_CURRENCY = "fUSD"

_DEFAULT_MIN_FUNDS = 500.0
_MF_RAW = os.getenv("MINIMUM_FUNDS")
MINIMUM_FUNDS = (
    float(str(_MF_RAW).strip())
    if _MF_RAW is not None and str(_MF_RAW).strip()
    else _DEFAULT_MIN_FUNDS
)
_EX_MIN_RAW = os.getenv("BITFINEX_MIN_FUNDING_ORDER_USD")
BITFINEX_MIN_FUNDING_ORDER_USD = (
    float(str(_EX_MIN_RAW).strip())
    if _EX_MIN_RAW is not None and str(_EX_MIN_RAW).strip()
    else 150.0
)

# =============================================================================
# Strategy config (centralized, env-overridable)
# =============================================================================

def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or not str(raw).strip():
        return default
    try:
        return float(str(raw).strip())
    except ValueError:
        return default


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or not str(raw).strip():
        return default
    try:
        return int(float(str(raw).strip()))
    except ValueError:
        return default


def _env_split(name: str, default: Tuple[float, ...], keys: Tuple[int, ...]) -> Dict[int, float]:
    """Parse a comma-separated split and hard-validate sum≈1 (±0.01).
    Silent normalization would let `0.5,0.5,0.5` become `1/3,1/3,1/3` with no
    warning, making the actual allocation disagree with what's written in .env —
    so we raise instead, matching the BASE_SPLIT_* validation philosophy."""
    raw = os.getenv(name)
    if raw is None or not str(raw).strip():
        return dict(zip(keys, default))
    parts = [float(x.strip()) for x in str(raw).split(",")]
    if len(parts) != len(keys):
        raise ValueError(f"{name} expects {len(keys)} comma-separated values, got {len(parts)}")
    for p in parts:
        if p < 0 or p > 1:
            raise ValueError(f"{name} value {p} must be in [0, 1]")
    s = sum(parts)
    if abs(s - 1.0) > 0.01:
        raise ValueError(f"{name} sum={s:.4f}, expected 1.0 (±0.01)")
    return dict(zip(keys, parts))


# Locked high-rate loan definition
LOCKED_MIN_PERIOD_DAYS = _env_int("LOCKED_MIN_PERIOD_DAYS", 60)
LOCKED_MIN_RATE = _env_float("LOCKED_MIN_RATE", 0.00040)

# Base mode split (applied to available_capital when no spike)
#   2d / 120d-preposition / reserve
BASE_SPLIT_2D = _env_float("BASE_SPLIT_2D", 0.70)
BASE_SPLIT_120D_PREPOSITION = _env_float("BASE_SPLIT_120D_PREPOSITION", 0.25)
BASE_SPLIT_RESERVE = _env_float("BASE_SPLIT_RESERVE", 0.05)

# Pre-positioning
#   target_rate = clamp(p99_rate * 0.98, [floor, ceil])
PREPOSITION_PERIOD = _env_int("PREPOSITION_PERIOD", 120)
PREPOSITION_RATE_FLOOR = _env_float("PREPOSITION_RATE_FLOOR", 0.00040)
PREPOSITION_RATE_CEIL = _env_float("PREPOSITION_RATE_CEIL", 0.00048)
PREPOSITION_P99_MULT = _env_float("PREPOSITION_P99_MULT", 0.98)
PREPOSITION_LOOKBACK_DAYS = _env_int("PREPOSITION_LOOKBACK_DAYS", 3)
# Tolerance band around target_rate for keeping existing preposition offers in place.
PREPOSITION_TOLERANCE = _env_float("PREPOSITION_TOLERANCE", 0.00002)

# Spike detection
SPIKE_L1_MULTIPLIER = _env_float("SPIKE_L1_MULTIPLIER", 1.8)  # last-1m avg / 24h avg
SPIKE_L2_MIN_RATE = _env_float("SPIKE_L2_MIN_RATE", 0.00035)  # max rate in last 1m
SPIKE_L1_MIN_LONG_PERIOD = _env_int("SPIKE_L1_MIN_LONG_PERIOD", 30)
SPIKE_L2_MIN_LONG_PERIOD = _env_int("SPIKE_L2_MIN_LONG_PERIOD", 120)
SPIKE_L2_MIN_LONG_TRADES = _env_int("SPIKE_L2_MIN_LONG_TRADES", 2)
SPIKE_RECENT_WINDOW_SEC = _env_int("SPIKE_RECENT_WINDOW_SEC", 60)
SPIKE_BASELINE_WINDOW_SEC = _env_int("SPIKE_BASELINE_WINDOW_SEC", 86400)

# Spike splits (applied to available_capital when a spike is active)
#   keys: 2d / 30d / 120d
SPIKE_SPLIT_L1 = _env_split("SPIKE_SPLIT_L1", (0.40, 0.20, 0.40), (2, 30, 120))
SPIKE_SPLIT_L2 = _env_split("SPIKE_SPLIT_L2", (0.10, 0.20, 0.70), (2, 30, 120))

# Rate ladder shape (used for 2d/30d buckets that need multi-step laddering)
#   rate_low  = max(market_floor, p{LADDER_LOW_PCT} of last-24h trades for that tenor)
#   rate_high = p{LADDER_HIGH_PCT} of last-24h trades for that tenor
LADDER_LOW_PCT = _env_float("LADDER_LOW_PCT", 30.0)
LADDER_HIGH_PCT = _env_float("LADDER_HIGH_PCT", 95.0)
LADDER_MIN_SAMPLES = _env_int("LADDER_MIN_SAMPLES", 20)
_STEPS_MAX_BUCKET_SIZE = 1000.0

# Preposition target rate minimum sample threshold (for p99 calc)
PREPOSITION_MIN_SAMPLES = _env_int("PREPOSITION_MIN_SAMPLES", 50)

# Dry-run: log orders without hitting the exchange
DRY_RUN = str(os.getenv("DRY_RUN", "")).strip().lower() in ("1", "true", "yes", "on")


def _validate_config() -> None:
    """Hard-validate config at startup. Raise ValueError on any misconfiguration
    that could cause the bot to silently misallocate funds."""
    errors: List[str] = []

    base_sum = BASE_SPLIT_2D + BASE_SPLIT_120D_PREPOSITION + BASE_SPLIT_RESERVE
    if abs(base_sum - 1.0) > 0.01:
        errors.append(
            f"BASE_SPLIT_2D ({BASE_SPLIT_2D}) + BASE_SPLIT_120D_PREPOSITION "
            f"({BASE_SPLIT_120D_PREPOSITION}) + BASE_SPLIT_RESERVE "
            f"({BASE_SPLIT_RESERVE}) = {base_sum:.4f}, expected 1.0 (±0.01)"
        )
    for v, name in (
        (BASE_SPLIT_2D, "BASE_SPLIT_2D"),
        (BASE_SPLIT_120D_PREPOSITION, "BASE_SPLIT_120D_PREPOSITION"),
        (BASE_SPLIT_RESERVE, "BASE_SPLIT_RESERVE"),
    ):
        if v < 0 or v > 1:
            errors.append(f"{name}={v} must be in [0, 1]")

    # Spike splits are hard-validated inside _env_split at import time (sum≈1,
    # each value in [0,1]), so no extra check here.

    if PREPOSITION_RATE_FLOOR >= PREPOSITION_RATE_CEIL:
        errors.append(
            f"PREPOSITION_RATE_FLOOR ({PREPOSITION_RATE_FLOOR}) must be < "
            f"PREPOSITION_RATE_CEIL ({PREPOSITION_RATE_CEIL})"
        )
    if not (0 < PREPOSITION_P99_MULT <= 1.5):
        errors.append(f"PREPOSITION_P99_MULT ({PREPOSITION_P99_MULT}) must be in (0, 1.5]")
    if LOCKED_MIN_PERIOD_DAYS < 1 or LOCKED_MIN_PERIOD_DAYS > 120:
        errors.append(f"LOCKED_MIN_PERIOD_DAYS ({LOCKED_MIN_PERIOD_DAYS}) must be in [1, 120]")
    if LOCKED_MIN_RATE <= 0:
        errors.append(f"LOCKED_MIN_RATE ({LOCKED_MIN_RATE}) must be > 0")
    if not (0 < LADDER_LOW_PCT < LADDER_HIGH_PCT <= 100):
        errors.append(
            f"LADDER_LOW_PCT ({LADDER_LOW_PCT}) must be < LADDER_HIGH_PCT "
            f"({LADDER_HIGH_PCT}), both in (0, 100]"
        )
    if SPIKE_RECENT_WINDOW_SEC >= SPIKE_BASELINE_WINDOW_SEC:
        errors.append(
            f"SPIKE_RECENT_WINDOW_SEC ({SPIKE_RECENT_WINDOW_SEC}) must be < "
            f"SPIKE_BASELINE_WINDOW_SEC ({SPIKE_BASELINE_WINDOW_SEC})"
        )
    if SPIKE_L1_MULTIPLIER <= 1.0:
        errors.append(f"SPIKE_L1_MULTIPLIER ({SPIKE_L1_MULTIPLIER}) must be > 1.0")

    if errors:
        raise ValueError("Invalid strategy configuration:\n  - " + "\n  - ".join(errors))


# =============================================================================
# Client (lazy — initialised by setup() so `import start` stays side-effect-free)
# =============================================================================

bfx: Optional[Client] = None


def setup() -> None:
    """Validate config and build the Bitfinex client. Call from main guard."""
    global bfx
    _validate_config()
    log(
        f"Config OK: base_split=({BASE_SPLIT_2D},{BASE_SPLIT_120D_PREPOSITION},"
        f"{BASE_SPLIT_RESERVE}) spike_l1={SPIKE_SPLIT_L1} spike_l2={SPIKE_SPLIT_L2} "
        f"preposition=[{PREPOSITION_RATE_FLOOR},{PREPOSITION_RATE_CEIL}] DRY_RUN={DRY_RUN}"
    )
    bfx = Client(api_key=os.getenv("BF_API_KEY"), api_secret=os.getenv("BF_API_SECRET"))


def _resolve_fund_currency() -> str:
    raw = os.getenv("FUND_CURRENCY")
    if raw is None or not str(raw).strip():
        return DEFAULT_FUND_CURRENCY
    s = str(raw).strip().upper()
    if s.startswith("F") and len(s) > 1:
        return "f" + s[1:]
    return "f" + s


# =============================================================================
# Public market data
# =============================================================================

# Below this USD volume a tenor bucket is treated as "too thin" — we propagate
# the shorter-tenor ravg forward instead of trusting the tiny sample's own avg.
_BOOK_MIN_VOLUME_USD = 1000.0


async def get_market_funding_book(currency: str):
    """Aggregate order book per tenor bucket (2/30/60/120 days).

    Returns (volume_dict, rate_upper_dict, rate_ravg_dict).
    rate_ravg is the volume-weighted mean rate. If a tenor bucket's volume is
    below _BOOK_MIN_VOLUME_USD, its ravg is unreliable and we propagate the
    previous tenor's ravg forward. upper defaults to the previous tenor's upper
    in the same situation.
    """
    market_fday_volume_dict = {2: 0.0, 30: 0.0, 60: 0.0, 120: 0.0}
    market_frate_upper_dict: Dict[int, float] = {2: 0.0, 30: 0.0, 60: 0.0, 120: 0.0}
    market_frate_ravg_dict: Dict[int, float] = {2: 0.0, 30: 0.0, 60: 0.0, 120: 0.0}
    weighted_sum: Dict[int, float] = {2: 0.0, 30: 0.0, 60: 0.0, 120: 0.0}

    connector = aiohttp.TCPConnector(ssl=_AIOHTTP_SSL)
    async with aiohttp.ClientSession(connector=connector) as session:
        for page in range(5):
            url = f"{BITFINEX_PUBLIC_API_URL}/v2/book/{currency}/P{page}?len=250"
            async with session.get(url) as response:
                response.raise_for_status()
                book_data = await response.json()
                for offer in book_data:
                    numdays = offer[2]
                    if numdays == 2:
                        bucket = 2
                    elif 3 <= numdays <= 30:
                        bucket = 30
                    elif 31 <= numdays <= 60:
                        bucket = 60
                    elif 61 <= numdays <= 120:
                        bucket = 120
                    else:
                        continue
                    vol = abs(offer[3])
                    market_fday_volume_dict[bucket] += vol
                    market_frate_upper_dict[bucket] = max(
                        market_frate_upper_dict[bucket], offer[0]
                    )
                    weighted_sum[bucket] += offer[0] * vol

    for p in (2, 30, 60, 120):
        v = market_fday_volume_dict[p]
        market_frate_ravg_dict[p] = (weighted_sum[p] / v) if v > 0 else 0.0

    # Propagate shorter-tenor values forward for thin buckets (volume < threshold).
    # This keeps ladder fallbacks sane when the longer-tenor book is sparse.
    tenors = (2, 30, 60, 120)
    for i in range(1, len(tenors)):
        cur, prev = tenors[i], tenors[i - 1]
        if market_fday_volume_dict[cur] < _BOOK_MIN_VOLUME_USD:
            market_frate_ravg_dict[cur] = market_frate_ravg_dict[prev]
            if market_frate_upper_dict[cur] <= 0:
                market_frate_upper_dict[cur] = market_frate_upper_dict[prev]

    log(f"market_fday_volume_dict: {market_fday_volume_dict}")
    log(f"market_frate_upper_dict: {market_frate_upper_dict}")
    log(f"market_frate_ravg_dict: {market_frate_ravg_dict}")
    return market_fday_volume_dict, market_frate_upper_dict, market_frate_ravg_dict


_PUBLIC_TRADES_LIMIT = 10000


async def _fetch_public_trades(currency: str, since_ms: int) -> list:
    """Public funding trades from `since_ms` until now. Returns list of [id, mts, amount, rate, period]."""
    url = (
        f"{BITFINEX_PUBLIC_API_URL}/v2/trades/{currency}/hist"
        f"?start={since_ms}&limit={_PUBLIC_TRADES_LIMIT}&sort=1"
    )
    connector = aiohttp.TCPConnector(ssl=_AIOHTTP_SSL)
    try:
        async with aiohttp.ClientSession(connector=connector) as session:
            async with session.get(url) as resp:
                resp.raise_for_status()
                data = await resp.json()
                if isinstance(data, list):
                    if len(data) >= _PUBLIC_TRADES_LIMIT:
                        log(
                            f"Warning: public trades fetch hit limit {_PUBLIC_TRADES_LIMIT}; "
                            f"older samples truncated."
                        )
                    return data
                return []
    except Exception as e:
        log(f"Warning: public trades fetch failed: {e}")
        return []


def _filter_trades_by_period(trades: list, period_min: int, period_max: int) -> List[float]:
    """Return the list of rates for trades whose period is in [period_min, period_max]."""
    out: List[float] = []
    for t in trades or []:
        try:
            rate = float(t[3])
            period = int(t[4])
        except (TypeError, ValueError, IndexError):
            continue
        if rate <= 0:
            continue
        if period_min <= period <= period_max:
            out.append(rate)
    return out


def compute_ladder_range(
    trades_24h: list,
    period_min: int,
    period_max: int,
    book_fallback_low: float,
    book_fallback_high: float,
) -> Tuple[float, float]:
    """
    Build a (rate_low, rate_high) ladder range from the last 24h public trades
    whose period falls in [period_min, period_max].

    - rate_low  = p{LADDER_LOW_PCT}  of the selected trades
    - rate_high = p{LADDER_HIGH_PCT} of the selected trades

    If samples are insufficient, fall back to the book-derived values.
    If rate_high <= rate_low for any reason, expand by 10% so the ladder
    actually spans a range.
    """
    rates = _filter_trades_by_period(trades_24h, period_min, period_max)
    if len(rates) < LADDER_MIN_SAMPLES:
        log(
            f"Ladder {period_min}-{period_max}d: only {len(rates)} trade samples "
            f"(< {LADDER_MIN_SAMPLES}); falling back to book "
            f"[{book_fallback_low}, {book_fallback_high}]"
        )
        low, high = book_fallback_low, book_fallback_high
    else:
        low = _percentile_sorted(rates, LADDER_LOW_PCT)
        high = _percentile_sorted(rates, LADDER_HIGH_PCT)
        log(
            f"Ladder {period_min}-{period_max}d: p{LADDER_LOW_PCT}={low:.8f} "
            f"p{LADDER_HIGH_PCT}={high:.8f} (samples={len(rates)})"
        )

    if high <= low:
        high = low * 1.10 if low > 0 else max(high, 0.00001)
        log(
            f"Ladder {period_min}-{period_max}d: high<=low after percentile, "
            f"expanded to [{low:.8f}, {high:.8f}]"
        )
    return low, high


def _percentile_sorted(values: List[float], pct: float) -> float:
    if not values:
        return float("nan")
    xs = sorted(values)
    if pct <= 0:
        return xs[0]
    if pct >= 100:
        return xs[-1]
    k = (len(xs) - 1) * pct / 100.0
    f = int(k)
    c = min(f + 1, len(xs) - 1)
    return xs[f] + (k - f) * (xs[c] - xs[f])


# =============================================================================
# Pre-positioning target rate (p99 of hourly funding candle HIGHs)
# =============================================================================

_PREPOSITION_CANDLE_KEY = "trade:1h:{currency}:a30:p2:p30"


async def _fetch_funding_candles_high(currency: str, since_ms: int) -> List[float]:
    """
    Fetch hourly HIGH funding rates from the aggregated candle endpoint.

    Uses `trade:1h:{currency}:a30:p2:p30` which aggregates trades with
    amount>=30 and period in [2, 30]. For a multi-day window this is a small
    number of candles (e.g. ~72 for 3d, ~168 for 7d), far under any API page
    limit, so no truncation.

    Returns a list of HIGH rates (one per candle). Empty list on failure.
    """
    key = _PREPOSITION_CANDLE_KEY.format(currency=currency)
    url = (
        f"{BITFINEX_PUBLIC_API_URL}/v2/candles/{key}/hist"
        f"?start={since_ms}&limit=10000&sort=1"
    )
    connector = aiohttp.TCPConnector(ssl=_AIOHTTP_SSL)
    try:
        async with aiohttp.ClientSession(connector=connector) as session:
            async with session.get(url) as resp:
                resp.raise_for_status()
                data = await resp.json()
    except Exception as e:
        log(f"Warning: funding candles fetch failed: {e}")
        return []

    if not isinstance(data, list):
        return []

    highs: List[float] = []
    for row in data:
        try:
            high = float(row[3])
        except (TypeError, ValueError, IndexError):
            continue
        if high > 0:
            highs.append(high)
    return highs


async def compute_preposition_target_rate(currency: str) -> Tuple[float, str]:
    """
    target_rate = clamp(p99_of_hourly_highs * PREPOSITION_P99_MULT, [floor, ceil])

    Uses last PREPOSITION_LOOKBACK_DAYS of hourly funding candle HIGHs.
    HIGH is the per-hour peak, which is exactly what we want to track for
    the "preposition at high-rate zone" strategy, and the candle endpoint
    never truncates for our window size.

    Falls back to the floor when samples are insufficient or the API fails.
    Returns (rate, source_label) for logging.
    """
    now_s = time.time()
    since_s = now_s - PREPOSITION_LOOKBACK_DAYS * 86400
    since_ms = int(since_s * 1000)

    def _fmt(ts: float) -> str:
        return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts))

    window_label = (
        f"{_fmt(since_s)} ~ {_fmt(now_s)} "
        f"({PREPOSITION_LOOKBACK_DAYS}d)"
    )

    highs = await _fetch_funding_candles_high(currency, since_ms)

    if len(highs) < PREPOSITION_MIN_SAMPLES:
        target = PREPOSITION_RATE_FLOOR
        log(
            f"Preposition target: window={window_label}, insufficient candle samples "
            f"({len(highs)} < {PREPOSITION_MIN_SAMPLES}), fallback to floor {target}"
        )
        return target, "fallback_floor"

    p99 = _percentile_sorted(highs, 99.0)
    raw = p99 * PREPOSITION_P99_MULT
    target = min(max(PREPOSITION_RATE_FLOOR, raw), PREPOSITION_RATE_CEIL)
    log(
        f"Preposition target: window={window_label}, "
        f"p99_high={p99:.8f} * {PREPOSITION_P99_MULT} = {raw:.8f} "
        f"-> clamp[{PREPOSITION_RATE_FLOOR}, {PREPOSITION_RATE_CEIL}] = {target:.8f} "
        f"(hourly candles={len(highs)})"
    )
    return target, f"p99_high_candles_{len(highs)}"


# =============================================================================
# Spike detection
# =============================================================================

def detect_spike_level(trades: list, now_ms: int) -> Tuple[int, dict]:
    """
    Classify spike level from public trades (assumed to cover ~24h up to now_ms).

    Windows (disjoint):
      recent   = [now - SPIKE_RECENT_WINDOW_SEC, now]
      baseline = [now - SPIKE_BASELINE_WINDOW_SEC, now - SPIKE_RECENT_WINDOW_SEC)

    Returns (level, debug_info):
      0 = no spike
      1 = L1: recent avg > baseline avg * SPIKE_L1_MULTIPLIER, and >=1 trade with
            period >= SPIKE_L1_MIN_LONG_PERIOD in the recent window
      2 = L2: L1 conditions, plus recent max rate >= SPIKE_L2_MIN_RATE, plus
            >= SPIKE_L2_MIN_LONG_TRADES trades with period >= SPIKE_L2_MIN_LONG_PERIOD
    """
    recent_cutoff = now_ms - SPIKE_RECENT_WINDOW_SEC * 1000
    baseline_cutoff = now_ms - SPIKE_BASELINE_WINDOW_SEC * 1000

    recent_rates: List[float] = []
    recent_rate_max = 0.0
    recent_long_l1_count = 0
    recent_long_l2_count = 0
    baseline_rates: List[float] = []

    for t in trades:
        try:
            mts = int(t[1])
            rate = float(t[3])
            period = int(t[4])
        except (TypeError, ValueError, IndexError):
            continue
        if rate <= 0 or mts < baseline_cutoff:
            continue
        if mts >= recent_cutoff:
            recent_rates.append(rate)
            if rate > recent_rate_max:
                recent_rate_max = rate
            if period >= SPIKE_L1_MIN_LONG_PERIOD:
                recent_long_l1_count += 1
            if period >= SPIKE_L2_MIN_LONG_PERIOD:
                recent_long_l2_count += 1
        else:
            baseline_rates.append(rate)

    info = {
        "baseline_n": len(baseline_rates),
        "recent_n": len(recent_rates),
        "recent_max": recent_rate_max,
        "recent_long_l1": recent_long_l1_count,
        "recent_long_l2": recent_long_l2_count,
    }

    if not recent_rates or not baseline_rates:
        info["reason"] = "insufficient samples"
        return 0, info

    recent_avg = sum(recent_rates) / len(recent_rates)
    baseline_avg = sum(baseline_rates) / len(baseline_rates)
    info["recent_avg"] = recent_avg
    info["baseline_avg"] = baseline_avg
    info["ratio"] = recent_avg / baseline_avg if baseline_avg > 0 else 0.0

    if baseline_avg <= 0 or recent_avg < baseline_avg * SPIKE_L1_MULTIPLIER:
        info["reason"] = "ratio below L1 multiplier"
        return 0, info
    if recent_long_l1_count < 1:
        info["reason"] = "no long-tenor trade"
        return 0, info

    if (
        recent_rate_max >= SPIKE_L2_MIN_RATE
        and recent_long_l2_count >= SPIKE_L2_MIN_LONG_TRADES
    ):
        info["reason"] = "L2 conditions met"
        return 2, info

    info["reason"] = "L1 conditions met"
    return 1, info


# =============================================================================
# Account state
# =============================================================================

async def get_active_credits(currency: str):
    """FundingCredit = our funds currently matched and actively earning.
    Returns None on API failure (caller should skip the round)."""
    try:
        return await asyncio.to_thread(bfx.rest.auth.get_funding_credits, symbol=currency)
    except Exception as e:
        log(f"Error getting funding credits: {e}")
        return None


async def list_lending_offers(currency: str):
    """Returns None on API failure (caller should skip the round)."""
    try:
        return await asyncio.to_thread(bfx.rest.auth.get_funding_offers, symbol=currency)
    except Exception as e:
        log(f"Error getting lending offers: {e}")
        return None


async def get_available_balance(currency: str):
    """funding wallet available_balance. Returns None on API failure."""
    try:
        wallets: List[Wallet] = await asyncio.to_thread(bfx.rest.auth.get_wallets)
        for wallet in wallets:
            if str(wallet.wallet_type).lower() != "funding":
                continue
            if f"f{wallet.currency}" == currency:
                return float(wallet.available_balance)
        return 0.0
    except Exception as e:
        log(f"Error getting balance: {e}")
        return None


def classify_loans(credits: List[FundingCredit]) -> Dict[str, List[FundingCredit]]:
    """Split active credits into locked_high_rate vs active_other."""
    locked: List[FundingCredit] = []
    other: List[FundingCredit] = []
    for c in credits or []:
        try:
            period = int(c.period)
            rate = float(c.rate)
        except (TypeError, ValueError, AttributeError):
            other.append(c)
            continue
        if period >= LOCKED_MIN_PERIOD_DAYS and rate >= LOCKED_MIN_RATE:
            locked.append(c)
        else:
            other.append(c)
    return {"locked": locked, "other": other}


def classify_offers(
    offers: List[FundingOffer],
    currency: str,
    preposition_target_rate: float,
) -> Dict[str, List[FundingOffer]]:
    """Split pending offers into 'preposition' (keep) vs 'other' (may cancel).

    Keep rule (asymmetric):
      period == PREPOSITION_PERIOD AND rate + PREPOSITION_TOLERANCE >= target_rate

    An existing preposition offer priced *above* target_rate is preserved — it
    can only earn us more if it fills during a spike, and cancelling it to re-list
    at a lower rate would be self-sabotage. Only offers priced meaningfully
    *below* target_rate are rotated out.
    """
    cur = (currency or "").upper()
    preposition: List[FundingOffer] = []
    other: List[FundingOffer] = []
    for o in offers or []:
        sym = getattr(o, "symbol", "") or ""
        if str(sym).upper() != cur:
            continue
        try:
            period = int(o.period)
            rate = float(o.rate)
        except (TypeError, ValueError, AttributeError):
            other.append(o)
            continue
        if (
            period == PREPOSITION_PERIOD
            and rate + PREPOSITION_TOLERANCE >= preposition_target_rate
        ):
            preposition.append(o)
        else:
            other.append(o)
    return {"preposition": preposition, "other": other}


def _sum_offer_amounts(offers: List[FundingOffer]) -> float:
    total = 0.0
    for o in offers or []:
        try:
            total += abs(float(o.amount))
        except (TypeError, ValueError):
            continue
    return total


# =============================================================================
# Order builders
# =============================================================================

_LADDER_RATE_DECIMALS = 8


def _ladder_orders(
    budget: float,
    period: int,
    rate_low: float,
    rate_high: float,
) -> List[Tuple[float, float, int]]:
    """Build a laddered list of (amount, rate, period) orders across [rate_low, rate_high].

    Design:
      - chunk_floor = max(MINIMUM_FUNDS, BITFINEX_MIN) is the smallest single-order size
      - target_steps is the stylistic preference (~10 steps, more for large budgets)
      - feasible_steps = budget // chunk_floor — the actual max number of orders
      - steps = min(target_steps, feasible_steps); floor at 1 when budget >= min
      - rates are evenly spaced across [rate_low, rate_high] INCLUDING both endpoints
        (steps==1 collapses to the midpoint)
      - any rounding remainder is absorbed into the last order
    """
    if budget + 1e-9 < BITFINEX_MIN_FUNDING_ORDER_USD:
        return []

    chunk_floor = max(MINIMUM_FUNDS, BITFINEX_MIN_FUNDING_ORDER_USD)
    target_steps = max(10, math.ceil(budget / _STEPS_MAX_BUCKET_SIZE))
    feasible_steps = max(1, int(budget // chunk_floor))
    steps = max(1, min(target_steps, feasible_steps))

    per_step = round(budget / steps, 2)
    if per_step + 1e-9 < BITFINEX_MIN_FUNDING_ORDER_USD:
        # Shouldn't happen given feasible_steps, but guard anyway: collapse to one order.
        per_step = round(budget, 2)
        steps = 1

    def _rate_at(i: int) -> float:
        if steps == 1:
            return round((rate_low + rate_high) / 2.0, _LADDER_RATE_DECIMALS)
        frac = i / (steps - 1)
        return round(rate_low + frac * (rate_high - rate_low), _LADDER_RATE_DECIMALS)

    orders: List[Tuple[float, float, int]] = []
    allocated = 0.0
    for i in range(steps):
        is_last = i == steps - 1
        amount = round(budget - allocated, 8) if is_last else round(per_step, 8)
        if amount + 1e-9 < BITFINEX_MIN_FUNDING_ORDER_USD:
            # Fold this slice into the previous order's amount.
            if orders:
                prev_amt, prev_rate, prev_period = orders[-1]
                orders[-1] = (round(prev_amt + amount, 8), prev_rate, prev_period)
            break
        orders.append((amount, _rate_at(i), period))
        allocated += amount
    return orders


def _redistribute_sub_minimum_buckets(
    buckets: List[Tuple[str, float]],
) -> List[Tuple[str, float]]:
    """
    Merge sub-minimum buckets into the preferred destination.

    `buckets[0]` is the preferred destination. Any bucket with amount < Bitfinex
    minimum gets merged into the preferred destination (keeping funds deployed).
    If the preferred destination is itself < minimum, we escalate and pour it +
    everything else into the NEXT bucket in priority order, and so on.

    Returns the filtered list of (name, amount) where every entry is >= minimum.
    Empty list => even the combined total can't reach the minimum; caller must skip.

    Examples (min=150):
      [("2d", 210), ("prep", 75)]        -> [("2d", 285)]      # prep swept up
      [("2d", 140), ("prep", 50)]        -> [("prep", 190)]    # 2d also < min, both escalate
      [("120d", 350), ("30d", 100), ("2d", 50)] -> [("120d", 500)]  # small slices into 120d
    """
    min_order = BITFINEX_MIN_FUNDING_ORDER_USD
    items = [(name, float(amt)) for name, amt in buckets if amt > 0]
    if not items:
        return []

    # Walk from the tail: any sub-min bucket merges into the preferred (index 0).
    i = len(items) - 1
    while i >= 1:
        name, amt = items[i]
        if amt + 1e-9 < min_order:
            pref_name, pref_amt = items[0]
            log(
                f"Redistribute: bucket '{name}' {amt:.2f} < min {min_order} "
                f"-> merged into '{pref_name}'"
            )
            items[0] = (pref_name, pref_amt + amt)
            items.pop(i)
        i -= 1

    # If the preferred destination itself is < min, escalate: drop it to the next
    # surviving bucket, carrying the pooled amount forward.
    while len(items) >= 2 and items[0][1] + 1e-9 < min_order:
        pref_name, pref_amt = items[0]
        next_name, next_amt = items[1]
        log(
            f"Redistribute: preferred '{pref_name}' {pref_amt:.2f} < min {min_order} "
            f"-> escalating into '{next_name}'"
        )
        items[1] = (next_name, next_amt + pref_amt)
        items.pop(0)

    if len(items) == 1 and items[0][1] + 1e-9 < min_order:
        log(
            f"Redistribute: sole bucket '{items[0][0]}' {items[0][1]:.2f} still below "
            f"min {min_order} — caller must skip."
        )
        return []
    return items


def build_base_orders(
    available_capital: float,
    preposition_already_placed: float,
    preposition_target_rate: float,
    ladder_2d: Tuple[float, float],
) -> List[Tuple[float, float, int]]:
    """
    Base mode: targets are computed against `available_capital` (NOT placed-inclusive),
    so already-placed preposition does not swell the pool.

      target_2d   = available_capital * BASE_SPLIT_2D
      target_120d = available_capital * BASE_SPLIT_120D_PREPOSITION
      reserve     = available_capital * BASE_SPLIT_RESERVE  (stays in wallet)

    Already-placed preposition is then deducted from target_120d to get the topup.

    Fallback priority when sub-minimum: 2d first (easiest to fill).
    """
    target_2d = available_capital * BASE_SPLIT_2D
    target_120d = available_capital * BASE_SPLIT_120D_PREPOSITION
    preposition_topup = max(0.0, target_120d - preposition_already_placed)

    # Never overspend available_capital (reserve is what's left).
    if target_2d + preposition_topup > available_capital:
        # Honor preposition topup first, clip 2d.
        target_2d = max(0.0, available_capital - preposition_topup)

    log(
        f"Base mode (pre-redistribute): available={available_capital:.2f} "
        f"target_2d={target_2d:.2f} target_120d={target_120d:.2f} "
        f"already_placed={preposition_already_placed:.2f} topup={preposition_topup:.2f}"
    )

    merged = _redistribute_sub_minimum_buckets([
        ("2d", target_2d),
        ("preposition_topup", preposition_topup),
    ])
    merged_map = dict(merged)

    orders: List[Tuple[float, float, int]] = []
    topup_final = merged_map.get("preposition_topup", 0.0)
    actual_2d_final = merged_map.get("2d", 0.0)

    if topup_final + 1e-9 >= BITFINEX_MIN_FUNDING_ORDER_USD:
        orders.append((round(topup_final, 8), preposition_target_rate, PREPOSITION_PERIOD))
    if actual_2d_final + 1e-9 >= BITFINEX_MIN_FUNDING_ORDER_USD:
        orders.extend(
            _ladder_orders(
                actual_2d_final,
                period=2,
                rate_low=ladder_2d[0],
                rate_high=ladder_2d[1],
            )
        )
    return orders


def build_spike_orders(
    available_capital: float,
    level: int,
    preposition_already_placed: float,
    preposition_target_rate: float,
    ladder_2d: Tuple[float, float],
    ladder_30d: Tuple[float, float],
) -> List[Tuple[float, float, int]]:
    """
    Spike mode: targets computed against `available_capital` by the level split.

      target_Xd = available_capital * SPIKE_SPLIT_L{level}[X]   for X in {2, 30, 120}
      topup_120d = max(0, target_120d - preposition_already_placed)

    If topup_120d < target_120d (preposition already covers some), the savings
    stay as-is — we don't re-inflate 2d/30d. 2d/30d budgets are fresh shares of
    available, capped so 2d+30d+topup_120d <= available.

    Fallback priority when sub-minimum: 120d > 30d > 2d (favor long tenor).
    """
    split = SPIKE_SPLIT_L1 if level == 1 else SPIKE_SPLIT_L2

    target_2d = available_capital * split[2]
    target_30d = available_capital * split[30]
    target_120d = available_capital * split[120]
    topup_120d = max(0.0, target_120d - preposition_already_placed)

    # Cap 2d + 30d so we don't overcommit available_capital.
    slot_for_short = max(0.0, available_capital - topup_120d)
    short_target_sum = target_2d + target_30d
    if short_target_sum > slot_for_short and short_target_sum > 0:
        shrink = slot_for_short / short_target_sum
        target_2d *= shrink
        target_30d *= shrink

    log(
        f"Spike L{level} (pre-redistribute): available={available_capital:.2f} "
        f"split={split} target_2d={target_2d:.2f} target_30d={target_30d:.2f} "
        f"target_120d={target_120d:.2f} topup_120d={topup_120d:.2f} "
        f"preposition_placed={preposition_already_placed:.2f}"
    )

    merged = _redistribute_sub_minimum_buckets([
        ("120d_topup", topup_120d),
        ("30d", target_30d),
        ("2d", target_2d),
    ])
    merged_map = dict(merged)

    orders: List[Tuple[float, float, int]] = []
    topup_120d_final = merged_map.get("120d_topup", 0.0)
    budget_30d_final = merged_map.get("30d", 0.0)
    budget_2d_final = merged_map.get("2d", 0.0)

    if topup_120d_final + 1e-9 >= BITFINEX_MIN_FUNDING_ORDER_USD:
        orders.append((round(topup_120d_final, 8), preposition_target_rate, PREPOSITION_PERIOD))
    if budget_30d_final + 1e-9 >= BITFINEX_MIN_FUNDING_ORDER_USD:
        orders.extend(
            _ladder_orders(
                budget_30d_final,
                period=30,
                rate_low=ladder_30d[0],
                rate_high=ladder_30d[1],
            )
        )
    if budget_2d_final + 1e-9 >= BITFINEX_MIN_FUNDING_ORDER_USD:
        orders.extend(
            _ladder_orders(
                budget_2d_final,
                period=2,
                rate_low=ladder_2d[0],
                rate_high=ladder_2d[1],
            )
        )
    return orders


# =============================================================================
# Order submission
# =============================================================================

async def _submit_offer(currency: str, amount: float, rate: float, period: int) -> bool:
    """Returns True on success, False on failure/skip."""
    if amount + 1e-9 < BITFINEX_MIN_FUNDING_ORDER_USD:
        log(f"Skip offer amount {amount}: below minimum {BITFINEX_MIN_FUNDING_ORDER_USD}")
        return False
    amount_str = f"{amount:.8f}"
    log(
        f"{'[DRY_RUN] ' if DRY_RUN else ''}"
        f"Submit: period={period}d rate={rate:.6f} (~{rate * 365 * 100:.2f}% APY) "
        f"amount={amount_str}"
    )
    if DRY_RUN:
        return True
    try:
        await asyncio.to_thread(
            bfx.rest.auth.submit_funding_offer,
            type="LIMIT", symbol=currency, amount=amount_str, rate=rate, period=period,
        )
        return True
    except Exception as e:
        log(f"Error submitting offer: {e}")
        return False


async def _cancel_offer(offer: FundingOffer) -> bool:
    """Returns True on success, False on failure."""
    try:
        oid = int(offer.id)
    except (TypeError, ValueError, AttributeError):
        return False
    log(
        f"{'[DRY_RUN] ' if DRY_RUN else ''}"
        f"Cancel offer id={oid} period={offer.period}d rate={offer.rate} "
        f"amount={offer.amount}"
    )
    if DRY_RUN:
        return True
    try:
        await asyncio.to_thread(bfx.rest.auth.cancel_funding_offer, id=oid)
        return True
    except Exception as e:
        log(f"Error cancelling offer {oid}: {e}")
        return False


# =============================================================================
# Main strategy loop
# =============================================================================

# Tolerances used by _plan_matches_existing.
_PLAN_DIFF_RATE_DECIMALS = 5       # round rate to this many decimals when bucketing
_PLAN_DIFF_REL_TOLERANCE = 0.05    # 5% per-bucket amount tolerance
_PLAN_DIFF_ABS_TOLERANCE = 20.0    # 20 USD floor, so small absolute drift still triggers merge


def _plan_matches_existing(
    existing_offers: List[FundingOffer],
    new_orders: List[Tuple[float, float, int]],
) -> bool:
    """
    Return True if cancelling existing_offers to place new_orders would be a no-op:
      - Same (period, rounded-rate) bucket set on both sides
      - Each bucket's total amount differs by <= max(5%, 20 USD)

    Used to skip the cancel+resubmit dance when the market hasn't meaningfully
    moved. Preposition offers are classified out before this check, so we're
    only comparing the laddered / spike buckets.
    """
    def _bucketize_offers(offers: List[FundingOffer]) -> Dict[Tuple[int, float], float]:
        out: Dict[Tuple[int, float], float] = {}
        for o in offers or []:
            try:
                period = int(o.period)
                rate = float(o.rate)
                amount = abs(float(o.amount))
            except (TypeError, ValueError, AttributeError):
                return {}  # any malformed offer forces a re-plan
            key = (period, round(rate, _PLAN_DIFF_RATE_DECIMALS))
            out[key] = out.get(key, 0.0) + amount
        return out

    def _bucketize_plan(orders: List[Tuple[float, float, int]]) -> Dict[Tuple[int, float], float]:
        out: Dict[Tuple[int, float], float] = {}
        for amount, rate, period in orders or []:
            key = (period, round(rate, _PLAN_DIFF_RATE_DECIMALS))
            out[key] = out.get(key, 0.0) + amount
        return out

    existing = _bucketize_offers(existing_offers)
    planned = _bucketize_plan(new_orders)

    if existing.keys() != planned.keys():
        return False

    for key, planned_amt in planned.items():
        existing_amt = existing[key]
        diff = abs(existing_amt - planned_amt)
        tol = max(_PLAN_DIFF_ABS_TOLERANCE, planned_amt * _PLAN_DIFF_REL_TOLERANCE)
        if diff > tol:
            return False
    return True


def _scale_orders_to_cap(
    orders: List[Tuple[float, float, int]], cap: float
) -> List[Tuple[float, float, int]]:
    """Proportionally shrink order amounts so their sum <= cap. When a scaled
    amount falls below Bitfinex min, merge it into the last surviving order of
    the same period (or the overall last survivor) so no budget is silently
    dropped. If no order survives (cap itself below min), returns []."""
    total = sum(a for a, _r, _p in orders)
    if total <= cap + 1e-9 or total <= 0:
        return orders
    shrink = cap / total
    scaled: List[Tuple[float, float, int]] = []
    leftover = 0.0
    for amount, rate, period in orders:
        new_amount = round(amount * shrink, 8)
        if new_amount + 1e-9 >= BITFINEX_MIN_FUNDING_ORDER_USD:
            scaled.append((new_amount, rate, period))
        else:
            leftover += new_amount
    if leftover > 0 and scaled:
        # Prefer merging into the last survivor of the same period; fall back to the overall last.
        merge_idx = len(scaled) - 1
        for i in range(len(scaled) - 1, -1, -1):
            if scaled[i][2] == orders[-1][2]:
                merge_idx = i
                break
        amt, rate, period = scaled[merge_idx]
        scaled[merge_idx] = (round(amt + leftover, 8), rate, period)
    log(
        f"Scaled orders: total {total:.2f} -> cap {cap:.2f} (factor {shrink:.4f})"
        + (f", merged leftover {leftover:.2f} into surviving order" if leftover > 0 and scaled else "")
        + (f", dropped {leftover:.2f} (no survivor)" if leftover > 0 and not scaled else "")
    )
    return scaled


async def lending_bot_strategy():
    log("=" * 60)
    log(f"Running lending bot strategy (DRY_RUN={DRY_RUN})")
    currency = _resolve_fund_currency()
    now_ms = int(time.time() * 1000)

    # 1. Snapshot account state — if any of these fail, SKIP this round (don't act blind).
    credits = await get_active_credits(currency)
    offers = await list_lending_offers(currency)
    wallet_avail = await get_available_balance(currency)
    if credits is None or offers is None or wallet_avail is None:
        log("Skip: failed to fetch account state; leaving offers untouched.")
        return

    # 2. Early skip: if no usable capital at all, bail BEFORE hitting candle / book APIs.
    #    Rough upper bound on cancellable offers = everything that's NOT a PREPOSITION_PERIOD
    #    tenor. This is a conservative lower-bound on available_capital (the real
    #    classify_offers later may keep even fewer as preposition, so actual capital
    #    can only be >= this). If even this is below min, no amount of recomputation
    #    will help.
    rough_cancellable = sum(
        abs(float(o.amount))
        for o in (offers or [])
        if getattr(o, "period", None) != PREPOSITION_PERIOD
    )
    rough_capital = wallet_avail + rough_cancellable
    if rough_capital + 1e-9 < BITFINEX_MIN_FUNDING_ORDER_USD:
        log(
            f"Skip (early): rough available_capital {rough_capital:.2f} "
            f"(wallet={wallet_avail:.2f} + non-preposition offers={rough_cancellable:.2f}) "
            f"below minimum {BITFINEX_MIN_FUNDING_ORDER_USD}; leaving offers untouched."
        )
        return

    classified_loans = classify_loans(credits)
    locked = classified_loans["locked"]
    active_other = classified_loans["other"]
    locked_amount = sum(abs(float(c.amount)) for c in locked)
    other_amount = sum(abs(float(c.amount)) for c in active_other)
    log(
        f"Loans: locked_high_rate={len(locked)} ({locked_amount:.2f}) "
        f"active_other={len(active_other)} ({other_amount:.2f})"
    )
    for c in locked:
        log(
            f"  LOCKED id={c.id} period={c.period}d rate={c.rate:.6f} "
            f"amount={abs(float(c.amount)):.2f}"
        )

    # 3. Preposition target rate (hourly candle HIGHs, separate fetch — could be cached later).
    preposition_target_rate, _src = await compute_preposition_target_rate(currency)

    # 4. Classify pending offers — preposition stays in place, others may be cancelled.
    classified_offers = classify_offers(offers, currency, preposition_target_rate)
    preposition_offers = classified_offers["preposition"]
    other_offers = classified_offers["other"]
    preposition_amount = _sum_offer_amounts(preposition_offers)
    other_offers_amount = _sum_offer_amounts(other_offers)
    log(
        f"Offers: preposition(keep)={len(preposition_offers)} ({preposition_amount:.2f}) "
        f"other(cancel-candidates)={len(other_offers)} ({other_offers_amount:.2f})"
    )

    # 5. available_capital = wallet + offers we're about to cancel.
    available_capital = wallet_avail + other_offers_amount
    log(
        f"Capital: wallet={wallet_avail:.2f} cancellable_offers={other_offers_amount:.2f} "
        f"-> available_capital={available_capital:.2f}"
    )
    if available_capital + 1e-9 < BITFINEX_MIN_FUNDING_ORDER_USD:
        log(
            f"Skip: available_capital {available_capital:.2f} below minimum "
            f"{BITFINEX_MIN_FUNDING_ORDER_USD}; leaving existing offers untouched."
        )
        return

    # 6. Shared 24h public trades feed (spike detection + ladder range).
    baseline_since_ms = now_ms - SPIKE_BASELINE_WINDOW_SEC * 1000
    trades_24h = await _fetch_public_trades(currency, baseline_since_ms)

    # 7. Book-derived fallback ranges for ladders (when trade samples are thin).
    _vol, book_rate_upper, book_rate_avg = await get_market_funding_book(currency)
    ladder_2d = compute_ladder_range(
        trades_24h, period_min=2, period_max=2,
        book_fallback_low=book_rate_avg[2], book_fallback_high=book_rate_upper[2],
    )
    ladder_30d = compute_ladder_range(
        trades_24h, period_min=3, period_max=30,
        book_fallback_low=book_rate_avg[30], book_fallback_high=book_rate_upper[30],
    )

    # 8. Spike detection.
    spike_level, spike_info = detect_spike_level(trades_24h, now_ms=now_ms)
    log(f"Spike detection: level={spike_level} info={spike_info}")

    # 9. Build new orders.
    if spike_level == 0:
        new_orders = build_base_orders(
            available_capital=available_capital,
            preposition_already_placed=preposition_amount,
            preposition_target_rate=preposition_target_rate,
            ladder_2d=ladder_2d,
        )
    else:
        new_orders = build_spike_orders(
            available_capital=available_capital,
            level=spike_level,
            preposition_already_placed=preposition_amount,
            preposition_target_rate=preposition_target_rate,
            ladder_2d=ladder_2d,
            ladder_30d=ladder_30d,
        )

    if not new_orders:
        log("No new orders to place this round; keeping existing preposition offers.")
        return

    # 10. No-op check: if current other_offers already match the plan closely,
    #     skip cancel+resubmit to avoid losing queue priority every minute.
    if _plan_matches_existing(other_offers, new_orders):
        log(
            f"Skip: existing {len(other_offers)} offers already match the plan "
            f"(within {int(_PLAN_DIFF_REL_TOLERANCE * 100)}% / {_PLAN_DIFF_ABS_TOLERANCE} USD). "
            f"No cancel/resubmit."
        )
        return

    # 11. Cancel non-preposition offers (preposition is never touched here).
    cancel_attempts = len(other_offers)
    cancel_successes = 0
    for o in other_offers:
        if await _cancel_offer(o):
            cancel_successes += 1
        await asyncio.sleep(0.1)
    cancel_failures = cancel_attempts - cancel_successes
    log(f"Cancel summary: {cancel_successes}/{cancel_attempts} succeeded ({cancel_failures} failed)")

    await asyncio.sleep(0.5)

    # 12. Before submitting, re-fetch wallet to confirm what really freed up.
    #     If some cancels silently failed, wallet will be smaller than expected and we
    #     must scale down the new orders rather than over-commit.
    if not DRY_RUN:
        actual_wallet = await get_available_balance(currency)
    else:
        actual_wallet = available_capital
    if actual_wallet is None:
        log("Skip submit: failed to re-check wallet after cancels.")
        return
    log(f"Post-cancel wallet: {actual_wallet:.2f} (expected ~{available_capital:.2f})")

    submit_cap = actual_wallet - 1.0  # small safety buffer for fees / rounding
    new_orders = _scale_orders_to_cap(new_orders, submit_cap)
    if not new_orders:
        log("No orders remain after scale-down; wallet too small this round.")
        return

    # 13. Submit new orders.
    submit_successes = 0
    submit_attempts = len(new_orders)
    for amount, rate, period in new_orders:
        if await _submit_offer(currency, amount, rate, period):
            submit_successes += 1
        await asyncio.sleep(0.1)
    log(
        f"Round summary: cancelled={cancel_successes}/{cancel_attempts} "
        f"submitted={submit_successes}/{submit_attempts}"
    )


async def run_schedule_task():
    try:
        await lending_bot_strategy()
    except Exception as e:
        log(f"Strategy run failed: {e}")


if __name__ == '__main__':
    setup()
    mode = int(sys.argv[1]) if len(sys.argv) > 1 else 1
    if mode == 0:
        asyncio.run(run_schedule_task())
    else:
        schedule.every(1).minutes.do(lambda: asyncio.run(run_schedule_task()))
        asyncio.run(run_schedule_task())
        while True:
            schedule.run_pending()
            time.sleep(1)
