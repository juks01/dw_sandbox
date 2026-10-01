import tempfile
import unittest
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


if __name__ == "__main__":
    unittest.main()
