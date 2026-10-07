import tempfile
import sqlite3
import unittest
from contextlib import closing
from pathlib import Path

from orchestrator.app import db


class DemoSourceInitializationTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.original_db_path = db.DB_PATH
        db.DB_PATH = str(Path(self.temp_dir.name) / "orchestrator.db")

    def tearDown(self):
        db.DB_PATH = self.original_db_path
        self.temp_dir.cleanup()

    def test_demo_source_is_seeded_once(self):
        db.init_db()
        db.init_db()

        sources = db.list_sources()
        demo_sources = [source for source in sources if source["name"] == "demo-products"]

        self.assertEqual(len(demo_sources), 1)
        self.assertEqual(demo_sources[0]["url"], "local://demo")
        self.assertEqual(demo_sources[0]["cron"], "*/2 * * * *")
        self.assertEqual(demo_sources[0]["enabled"], 1)

    def test_source_settings_checkpoint_and_run_status_round_trip(self):
        db.init_db()
        source = db.create_source(
            "incremental-items", "https://example.test/items", "0 * * * *", True,
            "incremental_upsert", "close_on_full_snapshot", ["id"], "updated_since", "updated_at",
        )
        db.update_source_checkpoint(source["name"], "2026-10-01T12:00:00+00:00")
        run_id = db.create_run(source["name"])
        db.update_run(
            run_id, status="done", finished="2026-10-01T12:01:00+00:00",
            load_mode="full_snapshot", checkpoint_after="2026-10-01T12:00:00+00:00",
        )

        saved = db.get_source(source["id"])
        listed = next(row for row in db.list_sources() if row["id"] == source["id"])
        self.assertEqual(saved["load_mode"], "incremental_upsert")
        self.assertEqual(saved["delete_policy"], "close_on_full_snapshot")
        self.assertEqual(saved["key_fields"], ["id"])
        self.assertEqual(saved["checkpoint"], "2026-10-01T12:00:00+00:00")
        self.assertEqual(listed["last_successful_run"], "2026-10-01T12:01:00+00:00")
        self.assertEqual(listed["last_error"], None)

    def test_source_error_clears_after_a_later_success(self):
        db.init_db()
        source = db.create_source(
            "items", "https://example.test/items", "0 * * * *", True,
            "full_snapshot", "close_on_full_snapshot", [], "updated_since", "updated_at",
        )
        failed_run = db.create_run(source["name"])
        db.update_run(failed_run, status="failed", step="load", error="loader unavailable")

        listed = next(row for row in db.list_sources() if row["id"] == source["id"])
        self.assertEqual(listed["last_error"], "loader unavailable")
        self.assertEqual(listed["last_error_step"], "load")
        self.assertEqual(listed["last_error_run_id"], failed_run)

        successful_run = db.create_run(source["name"])
        db.update_run(successful_run, status="done", finished=db.now_iso())
        listed = next(row for row in db.list_sources() if row["id"] == source["id"])
        self.assertIsNone(listed["last_error"])

    def test_init_migrates_existing_source_and_run_tables(self):
        with closing(sqlite3.connect(db.DB_PATH)) as conn:
            conn.execute(
                "CREATE TABLE sources (id INTEGER PRIMARY KEY, name TEXT UNIQUE, "
                "url TEXT, cron TEXT, enabled INTEGER, next_run TEXT)"
            )
            conn.execute(
                "CREATE TABLE runs (id INTEGER PRIMARY KEY, source TEXT, started TEXT, "
                "finished TEXT, status TEXT, step TEXT, error TEXT, filename TEXT, batch_id TEXT)"
            )
            conn.execute(
                "INSERT INTO sources (name, url, cron, enabled) VALUES "
                "('existing', 'https://example.test', '* * * * *', 1)"
            )
            conn.commit()

        db.init_db()
        source = db.get_source_by_name("existing")
        self.assertEqual(source["load_mode"], "full_snapshot")
        self.assertEqual(source["delete_policy"], "close_on_full_snapshot")
        self.assertEqual(source["key_fields"], [])
        self.assertIsNone(source["checkpoint"])
        self.assertIn("next_run_timezone", source)
        with closing(sqlite3.connect(db.DB_PATH)) as conn:
            run_columns = {row[1] for row in conn.execute("PRAGMA table_info(runs)")}
        self.assertIn("checkpoint_after", run_columns)

    def test_changing_incremental_contract_resets_checkpoint(self):
        db.init_db()
        source = db.create_source(
            "incremental-items", "https://example.test/items", "0 * * * *", True,
            "incremental_upsert", "close_on_full_snapshot", ["id"],
            "updated_since", "updated_at",
        )
        db.update_source_checkpoint(source["name"], "2026-10-01T12:00:00+00:00")

        self.assertTrue(db.update_source(
            source["id"], source["name"], source["url"], source["cron"], True,
            source["load_mode"], "never_close", ["id"], "updated_since", "updated_at",
        ))
        self.assertEqual(db.get_source(source["id"])["checkpoint"], "2026-10-01T12:00:00+00:00")

        db.update_source_next_run(source["id"], "2026-10-01T13:00:00+00:00", "UTC")
        db.update_source(
            source["id"], source["name"], source["url"], "30 * * * *", True,
            source["load_mode"], "never_close", ["id"], "updated_since", "updated_at",
        )
        source = db.get_source(source["id"])
        self.assertIsNone(source["next_run"])
        self.assertIsNone(source["next_run_timezone"])

        db.update_source(
            source["id"], source["name"], source["url"], source["cron"], True,
            source["load_mode"], "never_close", ["id"], "modified_since", "updated_at",
        )
        self.assertIsNone(db.get_source(source["id"])["checkpoint"])


if __name__ == "__main__":
    unittest.main()
