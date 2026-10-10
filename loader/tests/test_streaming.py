import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from loader.app.flatten import inspect_payload, iter_flattened_batches


class StreamingPayloadTests(unittest.TestCase):
    def test_inspection_and_flattening_process_paginated_items_in_bounded_batches(self):
        payload = {
            "items": [
                {"id": idx, "attributes": {"labels": [f"label-{idx}", "shared"]}}
                for idx in range(11)
            ],
            "total": 11,
            "skip": 0,
            "limit": 11,
            "has_more": False,
        }
        raw = json.dumps(payload).encode()

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "items.json"
            path.write_bytes(raw)

            info = inspect_payload(path)
            batches = list(iter_flattened_batches(
                "items", path, 19, info.root_type, info.collection_key, batch_size=3,
            ))

        self.assertEqual(info.root_type, "object")
        self.assertEqual(info.collection_key, "items")
        self.assertEqual(info.byte_size, len(raw))
        self.assertEqual(info.sha256, hashlib.sha256(raw).hexdigest())
        self.assertEqual(
            [len(batch["items"]) for batch in batches],
            [3, 3, 3, 2],
        )
        self.assertEqual(
            [row["id"] for batch in batches for row in batch["items"]],
            list(range(11)),
        )
        labels = [row for batch in batches for row in batch["items_attributes_labels"]]
        self.assertEqual(len(labels), 22)
        self.assertTrue(all(row["_source_batch_id"] == 19 for row in labels))

    def test_empty_root_array_yields_an_empty_source_table(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "empty.json"
            path.write_text("[]", encoding="utf-8")
            info = inspect_payload(path)
            batches = list(iter_flattened_batches(
                "empty-source", path, 2, info.root_type, info.collection_key,
            ))

        self.assertEqual(info.root_type, "array")
        self.assertEqual(batches, [{"empty_source": []}])


if __name__ == "__main__":
    unittest.main()
