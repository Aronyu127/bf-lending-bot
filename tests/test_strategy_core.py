"""Unit tests for lending strategy helpers (no network).

Expectations aligned with STRATEGY / start.py behavior:
  - Credits: single rollup (count rows + sum abs(amount)), no loan subclasses.
  - Preposition touch: sliding-window avg hit-count vs threshold r*.
  - Spike: disjoint recent vs baseline windows and level rules.
"""


import ast
import unittest

import start as s


class _Cred:
    __slots__ = ("amount",)

    def __init__(self, amount):
        self.amount = amount


class TestSourceNoLegacyLoanClassifier(unittest.TestCase):
    def test_classify_loans_removed(self):
        root = ast.parse(open(s.__file__, encoding="utf-8").read())
        names = {n.name for n in root.body if isinstance(n, ast.FunctionDef)}
        self.assertNotIn("classify_loans", names)


class TestActiveCreditsRollups(unittest.TestCase):
    def test_sums_absolute_amounts_and_count(self):
        rows = [_Cred("-100"), _Cred(40.5), _Cred(0)]
        n, total = s._active_credits_rollups(rows)
        self.assertEqual(n, 3)
        self.assertAlmostEqual(total, 140.5)

    def test_empty_and_none(self):
        self.assertEqual(s._active_credits_rollups([]), (0, 0.0))
        self.assertEqual(s._active_credits_rollups(None), (0, 0.0))

    def test_bad_amount_skips_value_keeps_row_count(self):
        class Bad:
            amount = None

        n, total = s._active_credits_rollups([_Cred(10), Bad()])
        self.assertEqual(n, 2)
        self.assertAlmostEqual(total, 10.0)


class TestPrepositionTouchMath(unittest.TestCase):
    def test_avg_hits_flat_highs_means_full_window_touch(self):
        w = s.PREPOSITION_TOUCH_WINDOW_HOURS
        highs = [1e-4] * (w + 10)
        avg = s._preposition_avg_hourly_hits(highs, w, 1e-4)
        self.assertAlmostEqual(avg, float(w))

    def test_binary_search_returns_max_feasible_r_on_flat_curve(self):
        h = 0.000731
        w = s.PREPOSITION_TOUCH_WINDOW_HOURS
        need = len(highs_needed := [h] * max(s.PREPOSITION_MIN_SAMPLES + 3, w + 3))
        self.assertGreaterEqual(need, s.PREPOSITION_MIN_SAMPLES)
        rs = s._preposition_max_rate_for_touch_budget(highs_needed, w, s.PREPOSITION_MIN_AVG_TOUCHES)
        self.assertIsNotNone(rs)
        self.assertAlmostEqual(rs, h, delta=1e-12)
        clipped = min(rs * s.PREPOSITION_P99_MULT, s.PREPOSITION_RATE_CEIL)
        self.assertLess(clipped, s.PREPOSITION_RATE_CEIL + 1e-12)


class TestSpikeLevels(unittest.TestCase):
    """Trade rows: [id, mts, amount, rate, period]"""

    def _mk_trades_flat(self, now_ms):
        baseline_start = now_ms - s.SPIKE_BASELINE_WINDOW_SEC * 1000
        recent_start = now_ms - s.SPIKE_RECENT_WINDOW_SEC * 1000
        out = []
        for i in range(80):
            mts = baseline_start + i * max(1, (recent_start - baseline_start - 5000) // 80)
            out.append([0, mts, 0, 0.00025, 2])
        for i in range(15):
            mts = recent_start + i * 100
            out.append([0, mts, 0, 0.00030, 2])
        return out

    def test_ratio_below_multiplier_is_level_zero(self):
        now_ms = 5_000_000_000_000
        trades = self._mk_trades_flat(now_ms)
        level, info = s.detect_spike_level(trades, now_ms)
        self.assertEqual(level, 0)
        self.assertIn("ratio", info or {})

    def test_level_one_when_ratio_and_long_trade_in_recent(self):
        now_ms = 6_000_000_000_000
        baseline_cut = now_ms - s.SPIKE_BASELINE_WINDOW_SEC * 1000
        recent_cut = now_ms - s.SPIKE_RECENT_WINDOW_SEC * 1000
        baseline_end = recent_cut - 1
        span = baseline_end - baseline_cut
        self.assertGreater(span, 0)
        trades = []
        for i in range(60):
            mts = baseline_cut + i * span // max(59, 1)
            trades.append([0, mts, 0, 0.00010, 2])
        for i in range(25):
            mts = recent_cut + i * 100
            trades.append([0, mts, 0, 0.00025, int(s.SPIKE_L1_MIN_LONG_PERIOD)])

        level, info = s.detect_spike_level(trades, now_ms)
        self.assertGreaterEqual(level, 1)
        self.assertAlmostEqual(info["baseline_avg"], 0.00010)
        self.assertAlmostEqual(info["recent_avg"], 0.00025)
        self.assertGreaterEqual(info["recent_avg"], info["baseline_avg"] * s.SPIKE_L1_MULTIPLIER)


if __name__ == "__main__":
    unittest.main()
