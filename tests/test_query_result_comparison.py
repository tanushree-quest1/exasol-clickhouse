import unittest
import json
from datetime import datetime
from decimal import Decimal

from benchmark.query_bench import compare_result_rows


class TestQueryResultComparison(unittest.TestCase):
    def test_ignores_result_order_and_tolerates_numeric_transport_noise(self):
        result = compare_result_rows(
            [("US", 12, Decimal("101.2500000")), ("CA", 8, 42.0)],
            [("CA", 8, Decimal("42.0000001")), ("US", 12, 101.25)],
        )

        self.assertTrue(result["values_match"])

    def test_reports_value_mismatch(self):
        result = compare_result_rows([("US", 12)], [("US", 13)])

        self.assertFalse(result["values_match"])
        self.assertEqual(result["mismatch_sample"][0]["column_index"], 1)

    def test_reports_datetime_mismatch(self):
        result = compare_result_rows(
            [(datetime(2026, 1, 1, 0, 0),)],
            [(datetime(2026, 1, 1, 5, 30),)],
        )

        self.assertFalse(result["values_match"])

    def test_serializes_bytes_in_mismatch_report(self):
        result = compare_result_rows([(b"US",)], [("CA",)])

        self.assertFalse(result["values_match"])
        self.assertEqual(result["mismatch_sample"][0]["clickhouse"], "US")
        json.dumps(result)


if __name__ == "__main__":
    unittest.main()
