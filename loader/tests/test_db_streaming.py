import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from loader.app import db


class StreamingBatchDatabaseTests(unittest.TestCase):
    def test_batch_rows_are_written_in_one_transaction_without_payload_copy(self):
        connection = MagicMock()
        cursor = MagicMock()
        connection.__enter__.return_value = connection
        connection.cursor.return_value.__enter__.return_value = cursor
        cursor.fetchone.return_value = (42,)
        cursor.fetchall.return_value = []

        batches = [
            {
                "items": [{
                    "_row_id": "root-row",
                    "_source_batch_id": 42,
                    "_parent_id": None,
                    "_parent_key": None,
                    "_row_index": 0,
                    "id": 1,
                }],
                "items_tags": [{
                    "_row_id": "child-row",
                    "_source_batch_id": 42,
                    "_parent_id": "root-row",
                    "_parent_key": '["items",1]',
                    "_row_index": 0,
                    "value": "tag",
                }],
            },
        ]
        with (
            patch.object(db, "get_conn", return_value=connection),
            patch.object(db, "iter_flattened_batches", return_value=iter(batches)),
            tempfile.TemporaryDirectory() as directory,
        ):
            result = db.load_payload(
                "items.json",
                "items",
                "https://example.test/items",
                Path(directory) / "items.json",
                "abc123",
                128,
                "array",
                None,
                "full_snapshot",
                "close_on_full_snapshot",
                "run-1",
                None,
                None,
                True,
            )

        self.assertEqual(result["status"], "loaded")
        self.assertEqual(result["batch_id"], 42)
        self.assertEqual(result["tables"], {"items": 1, "items_tags": 1})
        connection.commit.assert_called_once()
        raw_batch_insert = next(
            call for call in cursor.execute.call_args_list
            if "INSERT INTO staging.raw_batches" in str(call.args[0])
        )
        self.assertIn("VALUES (%s, %s, %s, NULL,", str(raw_batch_insert.args[0]))
        self.assertEqual(raw_batch_insert.args[1][3:5], ("items.json", "abc123"))
        self.assertEqual(cursor.executemany.call_count, 2)


if __name__ == "__main__":
    unittest.main()
