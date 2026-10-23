import unittest

from benchmark.xgboost_udf_bench import _latency_stats


class TestXGBoostLatencyStats(unittest.TestCase):
    def test_p95_uses_high_sample_with_two_iterations(self):
        stats = _latency_stats([10.488, 15.428], rows=100)

        self.assertEqual(stats["p50_ms"], 12.958)
        self.assertEqual(stats["p95_ms"], 15.428)

    def test_p95_uses_nearest_rank_for_twenty_iterations(self):
        stats = _latency_stats(list(range(1, 21)), rows=100)

        self.assertEqual(stats["p50_ms"], 10.5)
        self.assertEqual(stats["p95_ms"], 19)

    def test_rejects_empty_samples(self):
        with self.assertRaises(ValueError):
            _latency_stats([], rows=100)


if __name__ == "__main__":
    unittest.main()
