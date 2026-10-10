import unittest
from unittest.mock import patch

from fastapi import HTTPException

from mock_source.app import main


class MockSourceApiTests(unittest.TestCase):
    def test_page_response_reports_more_rows(self):
        with patch.object(main.db, "list_page", return_value=([{"id": 1}], 3)) as list_page:
            response = main.get_records("customers", skip=0, limit=1)

        self.assertEqual(response, {
            "items": [{"id": 1}],
            "total": 3,
            "skip": 0,
            "limit": 1,
            "has_more": True,
        })
        list_page.assert_called_once_with("customers", 1, 0, None)

    def test_incremental_parameter_parses_iso_timestamp(self):
        with patch.object(main.db, "list_page", return_value=([], 0)) as list_page:
            response = main.get_records(
                "medium_records",
                skip=0,
                limit=1000,
                updated_since="2026-10-08T10:00:00Z",
            )

        self.assertEqual(response["total"], 0)
        self.assertEqual(
            list_page.call_args.args[0:3],
            ("medium_records", 1000, 0),
        )
        self.assertEqual(list_page.call_args.args[3].isoformat(), "2026-10-08T10:00:00+00:00")

    def test_invalid_table_and_timestamp_are_rejected(self):
        with self.assertRaises(HTTPException) as unknown_table:
            main.get_records("nope", skip=0, limit=10)
        self.assertEqual(unknown_table.exception.status_code, 404)

        with self.assertRaises(HTTPException) as invalid_timestamp:
            main.get_records("customers", skip=0, limit=10, updated_since="bad")
        self.assertEqual(invalid_timestamp.exception.status_code, 422)
