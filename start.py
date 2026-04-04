import os,sys,time,platform,math
import logging
import logging.handlers
from dotenv import load_dotenv
load_dotenv()
import schedule
import asyncio
from typing import List
from bfxapi import Client
from bfxapi.types import Wallet
import platform
import ssl
import certifi
import aiohttp

_AIOHTTP_SSL = ssl.create_default_context(cafile=certifi.where())

# Log rotation: compressed, keep 30 days (5-min interval ~26 MB raw, ~5 MB compressed)
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
print = lambda *args, **kwargs: logging.info(" ".join(str(a) for a in args))

# API ENDPOINTS
BITFINEX_PUBLIC_API_URL = "https://api-pub.bitfinex.com"
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
DEFAULT_FUND_CURRENCY = "fUSD"


_BITFINEX_FEE_RATE = 0.85  # Bitfinex charges 15% on funding earnings


def _explicit_high_rate_apy_min() -> float | None:
    raw = os.getenv("HIGH_RATE_APY_MIN")
    if raw is None or not str(raw).strip():
        return None
    # env var is gross APY %; convert to net APY for internal comparison
    return float(str(raw).strip()) * _BITFINEX_FEE_RATE


def _default_high_rate_apy_min_for_currency(currency: str) -> float:
    # Thresholds are net APY (after 15% fee): fUSD=17%, fUST~14.875%
    c = (currency or "").strip().lower()
    if c == "fust":
        return 14.875
    return 17.0


def effective_high_rate_apy_min(currency: str) -> float:
    v = _explicit_high_rate_apy_min()
    if v is not None:
        return v
    return _default_high_rate_apy_min_for_currency((currency or "").strip().lower())


def _split_buckets_all_meet_min(funds: float, split: dict, min_o: float) -> bool:
    for r in split.values():
        if r < 0.01:
            continue
        if funds * r + 1e-9 < min_o:
            return False
    return True


def _load_high_rate_margin_split():
    raw = os.getenv("HIGH_RATE_MARGIN_SPLIT")
    default = (0.30, 0.15, 0.0, 0.55)
    keys = (2, 30, 60, 120)
    if raw is None or not str(raw).strip():
        return dict(zip(keys, default))
    try:
        parts = [float(x.strip()) for x in str(raw).split(",")]
        if len(parts) != 4:
            return dict(zip(keys, default))
        s = sum(parts)
        if s <= 0:
            return dict(zip(keys, default))
        return {k: v / s for k, v in zip(keys, parts)}
    except ValueError:
        return dict(zip(keys, default))


HIGH_RATE_MARGIN_SPLIT = _load_high_rate_margin_split()


def _resolve_fund_currency():
    raw = os.getenv("FUND_CURRENCY")
    if raw is None or not str(raw).strip():
        return DEFAULT_FUND_CURRENCY
    s = str(raw).strip().upper()
    if s.startswith("F") and len(s) > 1:
        return "f" + s[1:]
    return "f" + s

""" Strategy Parameters, Modify here"""
_STEPS_MAX_BUCKET_SIZE = 1000.0  # max USD per step; steps = ceil(budget / this), min 10
highest_sentiment = 5 # highest sentiment to adjust from fair rate to market highest rate
_RA_RAW = os.getenv("RATE_ADJUSTMENT_RATIO")
rate_adjustment_ratio = (
    float(str(_RA_RAW).strip())
    if _RA_RAW is not None and str(_RA_RAW).strip()
    else 1.11
)
# interval = 1 # interval one hour


bfx = Client(api_key=os.getenv("BF_API_KEY"), api_secret=os.getenv("BF_API_SECRET"))


"""Get funding book data from Bitfinex"""
async def get_market_funding_book(currency=None):
    if currency is None:
        currency = _resolve_fund_currency()
    #total volume in whole market
    market_fday_volume_dict = {2: 1, 30: 1, 60: 1, 120: 1} # can't be 0
    #highest rate in each day set whole market
    market_frate_upper_dict = {2: -999, 30: -999, 60: -999, 120: -999}
    # weighted average rate in each day set whole market
    market_frate_ravg_dict = {2: 0, 30: 0, 60: 0, 120: 0}

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
                        market_fday_volume_dict[2] += abs(offer[3])
                        market_frate_upper_dict[2] = max(market_frate_upper_dict[2], offer[0])
                        market_frate_ravg_dict[2] += offer[0] * abs(offer[3])
                    elif 3 <= numdays <= 30:
                        market_fday_volume_dict[30] += abs(offer[3])
                        market_frate_upper_dict[30] = max(market_frate_upper_dict[30], offer[0])
                        market_frate_ravg_dict[30] += offer[0] * abs(offer[3])
                    elif 31 <= numdays <= 60:
                        market_fday_volume_dict[60] += abs(offer[3])
                        market_frate_upper_dict[60] = max(market_frate_upper_dict[60], offer[0])
                        market_frate_ravg_dict[60] += offer[0] * abs(offer[3])
                    elif 61 <= numdays <= 120:
                        market_fday_volume_dict[120] += abs(offer[3])
                        market_frate_upper_dict[120] = max(market_frate_upper_dict[120], offer[0])
                        market_frate_ravg_dict[120] += offer[0] * abs(offer[3])

    market_frate_ravg_dict[2] /= market_fday_volume_dict[2]
    market_frate_ravg_dict[30] /= market_fday_volume_dict[30]
    if market_fday_volume_dict[30] < market_frate_ravg_dict[2]*1.5:
        market_frate_ravg_dict[30] = market_frate_ravg_dict[2]
    market_frate_ravg_dict[60] /= market_fday_volume_dict[60]
    if market_fday_volume_dict[60] < market_frate_ravg_dict[30]:
        market_frate_ravg_dict[60] = market_frate_ravg_dict[30]
    market_frate_ravg_dict[120] /= market_fday_volume_dict[120]
    if market_fday_volume_dict[120] < market_frate_ravg_dict[60]:
        market_frate_ravg_dict[120] = market_frate_ravg_dict[60]

    print("market_fday_volume_dict:")
    print(market_fday_volume_dict)
    print("market_frate_upper_dict:")
    print(market_frate_upper_dict)
    print("market_frate_ravg_dict:")
    print(market_frate_ravg_dict)
    # return total volume, highest rate, lowest rate
    return market_fday_volume_dict,market_frate_upper_dict,market_frate_ravg_dict

"""Calculate how FOMO the market is"""
async def get_market_borrow_sentiment(currency=None):
    if currency is None:
        currency = _resolve_fund_currency()
    #TODO: fetch matching book from https://report.bitfinex.com/api/json-rpc
    url = f"{BITFINEX_PUBLIC_API_URL}/v2/funding/stats/{currency}/hist"
    connector = aiohttp.TCPConnector(ssl=_AIOHTTP_SSL)
    async with aiohttp.ClientSession(connector=connector) as session:
        async with session.get(url) as response:
            response.raise_for_status()
            fdata = await response.json()
            need_rows = 13
            if (
                not isinstance(fdata, list)
                or len(fdata) < need_rows
                or not isinstance(fdata[0], list)
                or len(fdata[0]) <= 8
            ):
                print(
                    f"Warning: funding stats missing or short for {currency!r} (got {len(fdata) if isinstance(fdata, list) else type(fdata)} rows), sentiment=1.0"
                )
                return 1.0
            funding_amount_used_today = fdata[0][8]
            funding_amount_used_avg = 0
            for n in range(1, need_rows):
                row = fdata[n]
                if not isinstance(row, list) or len(row) <= 8:
                    print(f"Warning: funding stats row {n} invalid for {currency!r}, sentiment=1.0")
                    return 1.0
                funding_amount_used_avg += row[8]

            funding_amount_used_avg /= 12
            if not funding_amount_used_avg:
                print(f"Warning: zero avg funding used for {currency!r}, sentiment=1.0")
                return 1.0
            sentiment = funding_amount_used_today / funding_amount_used_avg
            print(f"funding_amount_used_today: {funding_amount_used_today}, funding_amount_used_avg: {funding_amount_used_avg}, sentiment: {sentiment}")
            return sentiment
        

def _valid_book_upper(u: float) -> bool:
    try:
        return u is not None and float(u) > 0.0
    except (TypeError, ValueError):
        return False


def _cap_with_book(model_upper: float, book_upper: float) -> float:
    if _valid_book_upper(book_upper):
        return max(model_upper, float(book_upper))
    return model_upper


def _percentile_sorted(values: list[float], pct: float) -> float:
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


def _apply_hist_cap_to_ladder_tops(
    upper: dict,
    avg: dict,
    hist_ref_daily: float,
    slack_mult: float,
) -> dict:
    lim = max(0.0, float(hist_ref_daily)) * max(1.0, float(slack_mult))
    out = {}
    for p in (2, 30, 60, 120):
        u = float(upper[p])
        av = float(avg[p])
        capped = min(u, lim)
        if capped + 1e-12 < av:
            capped = av
        out[p] = capped
    return out


def _margin_split_from_2d_top(currency: str, top_daily: float) -> dict:
    hr_min = effective_high_rate_apy_min(currency)
    top_net_apy_pct = float(top_daily) * 100.0 * 365.0 * _BITFINEX_FEE_RATE
    if top_net_apy_pct + 1e-9 >= hr_min:
        return dict(HIGH_RATE_MARGIN_SPLIT)
    return {2: 1.0, 30: 0.0, 60: 0.0, 120: 0.0}


def _parse_funding_candle_highs(data: list) -> dict[int, float]:
    by_mts: dict[int, float] = {}
    for row in data:
        if not isinstance(row, list) or len(row) <= 3:
            continue
        try:
            mts = int(row[0])
            h = float(row[3])
        except (TypeError, ValueError):
            continue
        if 0 < h < 0.5:
            by_mts[mts] = h
    return by_mts


async def fetch_funding_hist_high_percentile(currency: str) -> float | None:
    if os.getenv("FUNDING_HIST_DISABLE", "").strip().lower() in ("1", "true", "yes", "on"):
        return None

    try:
        pct = float(os.getenv("FUNDING_HIST_PERCENTILE", "92"))
    except ValueError:
        pct = 92.0
    pct = max(50.0, min(pct, 99.9))

    raw_iv = os.getenv("FUNDING_HIST_INTERVAL", "12h").strip().lower()
    interval = "1h" if raw_iv in ("1h", "60m") else "12h"

    sym = f"{currency}:a30:p2:p30"
    end_ms = int(time.time() * 1000)

    if interval == "1h":
        try:
            days = int(float(os.getenv("FUNDING_HIST_LOOKBACK_DAYS", "45")))
        except ValueError:
            days = 45
        days = max(7, min(days, 120))
        start_ms = end_ms - int(days * 86400 * 1000)
        url = (
            f"{BITFINEX_PUBLIC_API_URL}/v2/candles/trade:1h:{sym}/hist"
            f"?start={start_ms}&end={end_ms}&limit=10000&sort=1"
        )
    else:
        try:
            bar_count = int(float(os.getenv("FUNDING_HIST_BAR_COUNT", "8")))
        except ValueError:
            bar_count = 8
        bar_count = max(3, min(bar_count, 500))
        hours_per = 12
        extra = int(bar_count) + 4
        start_ms = end_ms - int(extra * hours_per * 3600 * 1000)
        url = (
            f"{BITFINEX_PUBLIC_API_URL}/v2/candles/trade:12h:{sym}/hist"
            f"?start={start_ms}&end={end_ms}&limit={extra}&sort=1"
        )

    try:
        connector = aiohttp.TCPConnector(ssl=_AIOHTTP_SSL)
        async with aiohttp.ClientSession(
            connector=connector,
            headers={"User-Agent": "bf-lending-bot/1.0"},
        ) as session:
            async with session.get(url) as resp:
                resp.raise_for_status()
                data = await resp.json()
    except Exception as e:
        print(f"Warning: funding history candles failed: {e}")
        return None
    if not isinstance(data, list):
        return None

    by_mts = _parse_funding_candle_highs(data)
    if not by_mts:
        print("Warning: funding history candle parse empty, skip hist cap")
        return None

    if interval == "1h":
        highs = list(by_mts.values())
        if len(highs) < 48:
            print(f"Warning: funding history too few samples ({len(highs)}), skip hist cap")
            return None
        return _percentile_sorted(highs, pct)

    try:
        bar_count = int(float(os.getenv("FUNDING_HIST_BAR_COUNT", "8")))
    except ValueError:
        bar_count = 8
    bar_count = max(3, min(bar_count, 500))

    chron = sorted(by_mts.items(), key=lambda x: x[0])
    drop_forming = os.getenv("FUNDING_HIST_INCLUDE_FORMING", "").strip().lower() not in (
        "1",
        "true",
        "yes",
        "on",
    )
    if drop_forming and len(chron) > 1:
        chron = chron[:-1]
    highs_tail = [h for _, h in chron[-bar_count:]]
    if len(highs_tail) < bar_count:
        print(
            f"Warning: funding 12h history need {bar_count} bars, got {len(highs_tail)} after trim, skip hist cap"
        )
        return None

    ref = _percentile_sorted(highs_tail, pct)
    print(
        f"Hist load: 12h x{bar_count} closed bars (drop_forming={drop_forming}), "
        f"p{pct} ref_daily={ref:.8f}"
    )
    return ref


"""Guess offer rate from funding book data"""
def guess_funding_book(
    currency,
    volume_dict,
    rate_upper_dict,
    rate_avg_dict,
    sentiment,
    hist_cap_daily: float | None = None,
    hist_slack: float = 1.03,
):
    last_step_percentage = 1 + (rate_adjustment_ratio - 1.0) * 10  # use fixed 10 steps for rate estimation
    sentiment_ratio = max(1.0, sentiment / highest_sentiment)
    rate_guess_2 = rate_avg_dict[2] * last_step_percentage * sentiment_ratio
    rate_guess_30 = rate_avg_dict[30] * last_step_percentage * sentiment_ratio
    rate_guess_60 = rate_avg_dict[60] * last_step_percentage * sentiment_ratio
    rate_guess_120 = rate_avg_dict[120] * last_step_percentage * sentiment_ratio
    rate_guess_upper = {
        2: _cap_with_book(rate_guess_2, rate_upper_dict[2]),
        30: _cap_with_book(rate_guess_30, rate_upper_dict[30]),
        60: _cap_with_book(rate_guess_60, rate_upper_dict[60]),
        120: _cap_with_book(rate_guess_120, rate_upper_dict[120]),
    }
    if hist_cap_daily is not None and hist_cap_daily > 0:
        before2 = rate_guess_upper[2]
        rate_guess_upper = _apply_hist_cap_to_ladder_tops(
            rate_guess_upper, rate_avg_dict, hist_cap_daily, hist_slack
        )
        print(
            f"Hist cap: ref_daily={hist_cap_daily:.8f} slack={hist_slack} "
            f"2d_top {before2:.8f} -> {rate_guess_upper[2]:.8f} "
            f"(APY {rate_guess_upper[2] * 365 * 100:.2f}%)"
        )
    margin_split_ratio_dict = _margin_split_from_2d_top(currency, rate_guess_upper[2])
    hr_min = effective_high_rate_apy_min(currency)
    top_net_apy_pct = rate_guess_upper[2] * 100.0 * 365.0 * _BITFINEX_FEE_RATE
    if margin_split_ratio_dict.get(2, 0) >= 0.99:
        print(f"Normal mode: 2d ladder top net APY {top_net_apy_pct:.2f}% < {hr_min}%")
    else:
        print(
            f"High-rate mode: 2d ladder top net APY {top_net_apy_pct:.2f}% >= {hr_min}% "
            f"=> margin_split_ratio_dict={margin_split_ratio_dict}"
        )
    print(f"margin_split_ratio_dict: {margin_split_ratio_dict}, rate_guess_upper: {rate_guess_upper}")
    return margin_split_ratio_dict, rate_guess_upper


""" get all offers in my book """
async def list_lending_offers(currency):
    try:
        return bfx.rest.auth.get_funding_offers(symbol=currency)
    except Exception as e:
        print(f"Error getting lending offers: {e}")
        return []

""" remove current offer in my book """
async def remove_all_lending_offer(currency):
    try:
        return bfx.rest.auth.cancel_all_funding_offers(currency)
    except Exception as e:
        print(f"Error removing lending offers: {e}")
        return None

"""Get available funds"""
async def get_balance(currency):
    try:
        wallets: List[Wallet] = bfx.rest.auth.get_wallets()
        for wallet in wallets:
            if str(wallet.wallet_type).lower() != "funding":
                continue
            if f"f{wallet.currency}" == currency:
                return wallet.available_balance
        return 0
    except Exception as e:
        print(f"Error getting balance: {e}")
        return 0


_CANCEL_REPLACE_THRESHOLD = 0.05  # cancel/replace only if any offer rate deviates >5% from new ladder


def _offers_within_ladder(offers, currency: str, rate_avg_dict: dict, rate_upper_dict: dict) -> bool:
    """Return True if all existing offers are within ±5% of the new ladder range for their bucket."""
    cur = (currency or "").upper()
    bucket_for_period = {}
    for p in (2, 30, 60, 120):
        bucket_for_period[p] = p

    def _bucket(period: int) -> int | None:
        if period == 2:
            return 2
        if 3 <= period <= 30:
            return 30
        if 31 <= period <= 60:
            return 60
        if 61 <= period <= 120:
            return 120
        return None

    for o in offers or []:
        sym = getattr(o, "symbol", "") or ""
        if str(sym).upper() != cur:
            continue
        try:
            rate = float(o.rate)
            period = int(o.period)
        except (TypeError, ValueError):
            return False
        bucket = _bucket(period)
        if bucket is None:
            return False
        lo = rate_avg_dict[bucket] * (1 - _CANCEL_REPLACE_THRESHOLD)
        hi = rate_upper_dict[bucket] * (1 + _CANCEL_REPLACE_THRESHOLD)
        if not (lo - 1e-12 <= rate <= hi + 1e-12):
            print(
                f"Offer period={period}d rate={rate:.6f} outside ladder "
                f"[{lo:.6f}, {hi:.6f}] for bucket {bucket}d — will cancel/replace"
            )
            return False
    return True


def _sum_funding_offer_amounts(offers, currency: str) -> float:
    cur = (currency or "").upper()
    total = 0.0
    for o in offers or []:
        sym = getattr(o, "symbol", "") or ""
        if str(sym).upper() != cur:
            continue
        try:
            total += abs(float(o.amount))
        except (TypeError, ValueError):
            continue
    return total


""" Main Function: Strategically place a lending offer on Bitfinex"""
async def place_lending_offer(currency, margin_split_ratio_dict,rate_avg_dict,offer_rate_guess_upper):
    """
    Args:
        currency (str): The currency to lend (e.g., 'UST', 'USD')
        margin_split_ratio_dict (dict): ratio of each period
        rate_avg_dict (dict): average rate of each period
        offer_rate_guess_upper (dict): upper rate of each period
    
    Returns:
        None
    """
    def _submit(amount: float, rate: float, period: int, tail: bool = False):
        if amount + 1e-9 < BITFINEX_MIN_FUNDING_ORDER_USD:
            print(
                f"Skip offer amount {amount}: below Bitfinex minimum "
                f"{BITFINEX_MIN_FUNDING_ORDER_USD} USD or equivalent per order"
            )
            return None
        tag = " (remainder)" if tail else ""
        print(
            f"offer rate @{round(rate * 100 * 365,2)} % APY, amount: {amount}, period: {period}{tag}"
        )
        try:
            return bfx.rest.auth.submit_funding_offer(
                type="LIMIT", symbol=currency, amount=str(amount), rate=rate, period=period
            )
        except Exception as e:
            print(f"Error submitting funding offer: {e}")
            return None

    funds = await get_balance(currency)
    if funds < 1e-6:
        print(f"Not enough funds to lend, funds: {funds}")
        return
    if funds + 1e-9 < BITFINEX_MIN_FUNDING_ORDER_USD:
        print(
            f"Cannot place funding offers: balance {funds} is below Bitfinex minimum "
            f"({BITFINEX_MIN_FUNDING_ORDER_USD} USD or equivalent per offer). Add funds to the funding wallet."
        )
        return
    time.sleep(0.5)

    chunk_floor = max(MINIMUM_FUNDS, BITFINEX_MIN_FUNDING_ORDER_USD)
    for period in margin_split_ratio_dict.keys():
        ratio = margin_split_ratio_dict[period]
        if ratio < 0.01:
            continue
        period_budget = round(funds * ratio, 8)
        if period_budget + 1e-9 < BITFINEX_MIN_FUNDING_ORDER_USD:
            print(
                f"Skip period {period}d: budget {period_budget} below minimum "
                f"{BITFINEX_MIN_FUNDING_ORDER_USD}"
            )
            continue
        steps = max(10, math.ceil(period_budget / _STEPS_MAX_BUCKET_SIZE))
        available_funds = period_budget
        splited_fund = max(chunk_floor, round(period_budget / steps, 2))
        segment_rate = (offer_rate_guess_upper[period] - rate_avg_dict[period]) / steps
        print(f"Period {period}d: budget={period_budget}, steps={steps}, per_step={splited_fund}")
        for i in range(1, steps + 1):
            if available_funds <= 0:
                break
            rate = round(rate_avg_dict[period] + i * segment_rate, 5)
            if i == steps:
                amt = round(available_funds, 8)
                if amt + 1e-9 >= BITFINEX_MIN_FUNDING_ORDER_USD:
                    _submit(amt, rate, period, tail=True)
                elif amt > 1e-6:
                    print(
                        f"Skip final step {period}d amount {amt}: below Bitfinex minimum order "
                        f"{BITFINEX_MIN_FUNDING_ORDER_USD}"
                    )
                time.sleep(0.1)
                available_funds = 0
                break
            if available_funds < splited_fund:
                amt = round(available_funds, 8)
                _submit(amt, rate, period, tail=True)
                time.sleep(0.1)
                available_funds = 0
                break
            remainder = available_funds - splited_fund
            if remainder == 0:
                _submit(splited_fund, rate, period)
                time.sleep(0.1)
                available_funds = 0
                break
            if remainder + 1e-9 < chunk_floor:
                merged = round(splited_fund + remainder, 8)
                if merged + 1e-9 >= BITFINEX_MIN_FUNDING_ORDER_USD:
                    _submit(merged, rate, period, tail=True)
                elif merged > 1e-6:
                    print(
                        f"Skip merged {merged}: below Bitfinex minimum order "
                        f"{BITFINEX_MIN_FUNDING_ORDER_USD}"
                    )
                time.sleep(0.1)
                available_funds = 0
                break
            _submit(splited_fund, rate, period)
            time.sleep(0.1)
            available_funds = remainder
        if available_funds > 0:
            last_rate = round(rate_avg_dict[period] + steps * segment_rate, 5)
            leftover = round(available_funds, 8)
            if leftover + 1e-9 >= BITFINEX_MIN_FUNDING_ORDER_USD:
                _submit(leftover, last_rate, period, tail=True)
            else:
                print(
                    f"Leftover {leftover} stays in wallet: below Bitfinex minimum order "
                    f"{BITFINEX_MIN_FUNDING_ORDER_USD}"
                )
            time.sleep(0.1)

async def lending_bot_strategy():
    
    print("Running lending bot strategy")
    currency = _resolve_fund_currency()
    # get market sentiment
    sentiment = await get_market_borrow_sentiment(currency)
    # get market rate
    volume_dict,rate_upper_dict,rate_avg_dict = await get_market_funding_book(currency)

    hist_cap = await fetch_funding_hist_high_percentile(currency)
    try:
        hist_slack = float(os.getenv("FUNDING_HIST_SLACK_MULT", "1.03"))
    except ValueError:
        hist_slack = 1.03
    hist_slack = max(1.0, min(hist_slack, 1.25))

    margin_split_ratio_dict, offer_rate_guess_upper = guess_funding_book(
        currency,
        volume_dict,
        rate_upper_dict,
        rate_avg_dict,
        sentiment,
        hist_cap_daily=hist_cap,
        hist_slack=hist_slack,
    )

    # get my offers and remove current offer first
    my_offers = await list_lending_offers(currency)
    print(f"my_offers: {my_offers}")

    funds_avail = await get_balance(currency)
    locked_in_offers = _sum_funding_offer_amounts(my_offers, currency)
    funds_ready = funds_avail + locked_in_offers
    if funds_ready + 1e-9 < BITFINEX_MIN_FUNDING_ORDER_USD:
        print(
            f"Skip cancel/replace: deployable after cancel {funds_ready} "
            f"(available {funds_avail}, in_offers {locked_in_offers}) is below Bitfinex minimum offer "
            f"{BITFINEX_MIN_FUNDING_ORDER_USD} USD equivalent — would leave offers empty with no valid replacement."
        )
        return

    n_active = sum(1 for r in margin_split_ratio_dict.values() if r >= 0.01)
    if n_active > 1 and not _split_buckets_all_meet_min(
        funds_ready, margin_split_ratio_dict, BITFINEX_MIN_FUNDING_ORDER_USD
    ):
        print(
            f"Multi-tenor split would leave each bucket below min order "
            f"{BITFINEX_MIN_FUNDING_ORDER_USD}; fallback 100% 2d (balance {funds_ready})."
        )
        margin_split_ratio_dict = {2: 1.0, 30: 0.0, 60: 0.0, 120: 0.0}

    if my_offers and _offers_within_ladder(my_offers, currency, rate_avg_dict, offer_rate_guess_upper):
        print("All existing offers within ladder range — skip cancel/replace")
        return

    time.sleep(0.5)
    cancel_res = await remove_all_lending_offer(currency[1:])
    print(f"cancel_res: {cancel_res}")

    # place new offer
    time.sleep(0.5)
    await place_lending_offer(currency, margin_split_ratio_dict,rate_avg_dict,offer_rate_guess_upper)
    

async def run_schedule_task():
    await lending_bot_strategy()


if __name__ == '__main__':
    mode = int(sys.argv[1]) if len(sys.argv) > 1 else 1
    if mode == 0:
        asyncio.run(run_schedule_task())
    else:
        schedule.every(5).minutes.do(lambda: asyncio.run(run_schedule_task()))
        asyncio.run(run_schedule_task())
        while True:
            schedule.run_pending()
            time.sleep(1)

