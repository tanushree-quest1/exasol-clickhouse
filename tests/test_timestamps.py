import unittest
from datetime import datetime, timezone

from consumers.timestamps import exasol_timestamp_text, parse_utc_timestamp


class TestEventTimestampNormalization(unittest.TestCase):
    def test_naive_timestamps_are_treated_as_utc(self):
        parsed = parse_utc_timestamp("2026-10-22T20:17:58.619")

        self.assertEqual(parsed, datetime(2026, 10, 22, 20, 17, 58, 619000, tzinfo=timezone.utc))
        self.assertEqual(
            exasol_timestamp_text("2026-10-22T20:17:58.619"),
            "2026-10-22 20:17:58.619000",
        )

    def test_offset_timestamps_are_converted_to_utc(self):
        self.assertEqual(
            exasol_timestamp_text("2026-10-23T01:47:58.619+05:30"),
            "2026-10-22 20:17:58.619000",
        )

    def test_utc_designator_and_datetime_values_are_supported(self):
        parsed = parse_utc_timestamp("2026-10-22T20:17:58.619Z")

        self.assertEqual(parsed.utcoffset().total_seconds(), 0)
        self.assertEqual(
            exasol_timestamp_text(parsed),
            "2026-10-22 20:17:58.619000",
        )


if __name__ == "__main__":
    unittest.main()
