#!/usr/bin/env python3
"""
Fetch Bitfinex public funding candles and summarize APY distributions.
Uses aggregated hourly series trade:1h:{fUSD|fUST}:a30:p2:p30 (UI-style bucket).

This is a market-rate proxy; the bot's HIGH_RATE_APY_MIN compares against a
book-derived 2d ladder top, so calibrate thresholds with live logs if needed.
"""

from __future__ import annotations

import argparse
import json
import ssl
import statistics
import time
import urllib.request
from typing import Any

import certifi

BITFINEX_PUB = "https://api-pub.bitfinex.com"


def _daily_to_apy_pct(daily: float) -> float:
    return float(daily) * 365.0 * 100.0


def _percentile(sorted_xs: list[float], p: float) -> float:
    if not sorted_xs:
        return float("nan")
    xs = sorted_xs
    k = (len(xs) - 1) * p / 100.0
    f = int(k)
    c = min(f + 1, len(xs) - 1)
    return xs[f] + (k - f) * (xs[c] - xs[f])


def _fetch_candles(
    symbol_key: str,
    start_ms: int,
    end_ms: int,
    limit: int = 10_000,
) -> list[list[Any]]:
    url = (
        f"{BITFINEX_PUB}/v2/candles/trade:1h:{symbol_key}/hist"
        f"?start={start_ms}&end={end_ms}&limit={limit}&sort=1"
    )
    req = urllib.request.Request(url, headers={"User-Agent": "bf-lending-bot-analysis/1.0"})
    ctx = ssl.create_default_context(cafile=certifi.where())
    with urllib.request.urlopen(req, context=ctx, timeout=120) as resp:
        return json.loads(resp.read().decode())


def _winsorize(xs: list[float], hi_pct: float) -> list[float]:
    if not xs or hi_pct >= 100.0:
        return list(xs)
    cap = _percentile(sorted(xs), hi_pct)
    return [min(x, cap) for x in xs]


def _streak_stats(flags: list[bool]) -> tuple[int, float, int]:
    if not flags:
        return 0, 0.0, 0
    lengths: list[int] = []
    cur = 0
    for f in flags:
        if f:
            cur += 1
        elif cur:
            lengths.append(cur)
            cur = 0
    if cur:
        lengths.append(cur)
    if not lengths:
        return 0, 0.0, 0
    return len(lengths), sum(lengths) / len(lengths), max(lengths)


def main() -> None:
    p = argparse.ArgumentParser(description="Analyze Bitfinex funding candle history.")
    p.add_argument(
        "--symbol",
        choices=("fUSD", "fUST"),
        default="fUSD",
        help="Funding symbol root (default: fUSD).",
    )
    p.add_argument("--days", type=int, default=366, help="Lookback days (default: 366).")
    p.add_argument(
        "--winsor-apy",
        type=float,
        default=99.5,
        help="Winsorize hourly HIGH APY at this percentile for robust stats (default: 99.5).",
    )
    args = p.parse_args()

    sym = f"{args.symbol}:a30:p2:p30"
    end = int(time.time() * 1000)
    start = end - int(args.days) * 86400 * 1000
    rows = _fetch_candles(sym, start, end)
    apy_high = [_daily_to_apy_pct(r[3]) for r in rows]
    apy_close = [_daily_to_apy_pct(r[2]) for r in rows]
    apy_low = [_daily_to_apy_pct(r[4]) for r in rows]
    apy_high_w = _winsorize(apy_high, args.winsor_apy)

    n = len(rows)
    print(f"symbol={sym} rows={n} days_requested={args.days}")
    print()

    def block(title: str, xs: list[float]) -> None:
        s = sorted(xs)
        print(title)
        print(
            "  mean",
            round(statistics.mean(xs), 3),
            "p50",
            round(_percentile(s, 50), 3),
            "p75",
            round(_percentile(s, 75), 3),
            "p90",
            round(_percentile(s, 90), 3),
            "p95",
            round(_percentile(s, 95), 3),
            "p99",
            round(_percentile(s, 99), 3),
            "max",
            round(max(xs), 3),
        )

    block("APY% from hourly HIGH (daily rate high)", apy_high)
    block(f"APY% HIGH winsor@{args.winsor_apy}%", apy_high_w)
    block("APY% from hourly CLOSE", apy_close)
    block("APY% from hourly LOW", apy_low)
    print()

    thresholds = [15, 18, 20, 25, 30, 35, 40, 45, 50, 60, 80]
    print("Share of hours with HIGH APY >= threshold (raw HIGH):")
    for t in thresholds:
        hit = sum(1 for x in apy_high if x >= t) / max(n, 1) * 100.0
        print(f"  >= {t:>2}% APY : {hit:5.2f}% of hours")
    print()

    for label, xs in ("raw HIGH", apy_high), (f"winsor HIGH@{args.winsor_apy}%", apy_high_w):
        s = sorted(xs)
        for pct in (90, 95, 97, 99):
            v = _percentile(s, pct)
            print(f"{label} percentile p{pct}: {v:.3f}% APY")
    print()

    for t in (25.0, 35.0, 45.0):
        flags = [x >= t for x in apy_high]
        n_ep, mean_len, mx = _streak_stats(flags)
        print(
            f"Streaks HIGH>={t}% APY: episodes={n_ep} mean_len_h={mean_len:.2f} max_len_h={mx}"
        )


if __name__ == "__main__":
    main()
