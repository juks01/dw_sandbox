import unittest

from loader.app.flatten import flatten_payload


class EmptySnapshotFlatteningTests(unittest.TestCase):
    def test_empty_root_snapshot_retains_source_table(self):
        tables = flatten_payload("empty-source", [], 7)
        self.assertEqual(tables, {"empty_source": []})

    def test_empty_nested_array_retains_child_table(self):
        tables = flatten_payload("items", [{"id": 1, "tags": []}], 7)
        self.assertIn("items_tags", tables)
        self.assertEqual(tables["items_tags"], [])


if __name__ == "__main__":
    unittest.main()
