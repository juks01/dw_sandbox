import random
import unittest
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

from mock_source.app.generate import (
    _bulk_row,
    _random_timestamps,
    initialize,
    records_for_target,
)


class GeneratorSizingTests(unittest.TestCase):
    def test_record_count_grows_with_target_size(self):
        self.assertGreater(
            records_for_target(1_000_000),
            records_for_target(100_000),
        )

    def test_invalid_target_size_is_rejected(self):
        for target, payload_size in ((0, 10), (100, 0), (-1, 10)):
            with self.subTest(target=target, payload_size=payload_size):
                with self.assertRaises(ValueError):
                    records_for_target(target, payload_size)

    def test_created_and_updated_timestamps_are_randomized_in_required_ranges(self):
        now = datetime(2026, 10, 8, tzinfo=timezone.utc)
        created_at, updated_at = _random_timestamps(random.Random(7), now)

        self.assertGreaterEqual((now - created_at).days, 365)
        self.assertLessEqual((now - created_at).days, 3 * 365)
        self.assertGreaterEqual((now - updated_at).total_seconds(), 0)
        self.assertLessEqual((now - updated_at).days, 365)
        self.assertLessEqual(created_at, updated_at)
        self.assertNotEqual(
            (created_at, updated_at),
            _random_timestamps(random.Random(8), now),
        )

    def test_bulk_record_has_multiple_fields_and_nested_json(self):
        row = _bulk_row(1234, 20, random.Random(7))

        self.assertEqual(len(row), 8)
        self.assertIn("labels", row[6].obj)
        self.assertIn("context", row[7].obj)
        self.assertIn("identity", row[7].obj["context"])
        self.assertEqual(row[7].obj["context"]["identity"]["id"], 1234)

    def test_init_if_empty_skips_fully_populated_tables(self):
        conn = MagicMock()
        conn.__enter__.return_value = conn
        conn.execute.return_value.fetchone.side_effect = [(3,), (4,), (5,), (6,)]

        with patch("mock_source.app.generate.db.connect", return_value=conn), patch(
            "mock_source.app.generate.db.ensure_schema"
        ):
            result = initialize(if_empty=True, medium_bytes=1, large_bytes=1)

        self.assertEqual(result, {
            "departments": 3,
            "customers": 4,
            "medium_records": 5,
            "large_records": 6,
        })
        conn.commit.assert_not_called()

    def test_init_if_empty_rejects_partially_populated_database(self):
        conn = MagicMock()
        conn.__enter__.return_value = conn
        conn.execute.return_value.fetchone.side_effect = [(3,), (0,), (5,), (6,)]

        with patch("mock_source.app.generate.db.connect", return_value=conn), patch(
            "mock_source.app.generate.db.ensure_schema"
        ):
            with self.assertRaisesRegex(ValueError, "mock data already exists"):
                initialize(if_empty=True, medium_bytes=1, large_bytes=1)
