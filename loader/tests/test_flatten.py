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

    def test_paginated_api_envelope_loads_collection_into_source_table(self):
        tables = flatten_payload(
            "mock customers",
            {
                "items": [
                    {"id": 1, "name": "Customer 1"},
                    {"id": 2, "name": "Customer 2"},
                ],
                "total": 2,
                "skip": 0,
                "limit": 1000,
                "has_more": False,
            },
            9,
        )

        self.assertEqual(len(tables), 1)
        self.assertEqual(
            [row["id"] for row in tables["mock_customers"]],
            [1, 2],
        )
        self.assertTrue(all(row["_source_batch_id"] == 9 for row in tables["mock_customers"]))

    def test_nested_scalar_rows_get_stable_parent_key(self):
        payload = [{"id": 17, "attributes": {"labels": ["one", "two"]}}]

        first = flatten_payload("mock medium", payload, 9)["mock_medium_attributes_labels"]
        second = flatten_payload("mock medium", payload, 10)["mock_medium_attributes_labels"]

        self.assertEqual([row["_parent_key"] for row in first], [row["_parent_key"] for row in second])
        self.assertEqual(
            [(row["_parent_key"], row["_row_index"]) for row in first],
            [(row["_parent_key"], row["_row_index"]) for row in second],
        )
        self.assertEqual([row["value"] for row in first], ["one", "two"])


if __name__ == "__main__":
    unittest.main()
