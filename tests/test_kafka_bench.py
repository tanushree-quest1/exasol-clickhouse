import unittest

from benchmark.kafka_bench import parse_sweep_values


class TestKafkaSweepValues(unittest.TestCase):
    def test_parses_rates_including_unthrottled_case(self):
        self.assertEqual(parse_sweep_values("0,2000,5000", allow_zero=True), [0, 2000, 5000])

    def test_rejects_duplicates(self):
        with self.assertRaises(ValueError):
            parse_sweep_values("1000,1000")

    def test_rejects_zero_for_batch_sizes(self):
        with self.assertRaises(ValueError):
            parse_sweep_values("0")


if __name__ == "__main__":
    unittest.main()
