import tempfile
import unittest
import httpx
from pathlib import Path
from unittest.mock import patch

from orchestrator.app import db, pipeline


class FakeResponse:
    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self.payload


class PipelineCheckpointTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.original_db_path = db.DB_PATH
        db.DB_PATH = str(Path(self.temp_dir.name) / "orchestrator.db")
        db.init_db()
        self.source = db.create_source(
            "items", "https://example.test/items", "0 * * * *", True,
            "incremental_upsert", "close_on_full_snapshot", ["id"], "updated_since", "updated_at",
        )
        self.checkpoint_before = "2026-10-01T10:00:00+00:00"
        self.checkpoint_after = "2026-10-01T11:00:00+00:00"
        db.update_source_checkpoint("items", self.checkpoint_before)
        self.source = db.get_source(self.source["id"])
        self.run_id = db.create_run("items")
        self.addCleanup(self.temp_dir.cleanup)
        self.addCleanup(setattr, db, "DB_PATH", self.original_db_path)

    def _responses(self, url, **kwargs):
        if url.endswith("/extract"):
            self.extract_request = kwargs["json"]
            return FakeResponse({
                "filename": "items.json",
                "source_url": self.source["url"],
                "checkpoint_after": self.checkpoint_after,
                "pagination_complete": True,
            })
        return FakeResponse({"batch_id": 42, "tables": {"items": 1}})

    def _run(self, mart_refresh):
        with (
            patch.object(pipeline, "full_health", return_value={
                "dependencies": {"extractor": True, "loader": True, "staging": True, "core": True, "mart": True},
            }),
            patch.object(pipeline.httpx, "post", side_effect=self._responses),
            patch.object(pipeline, "call_core_sync", return_value=[
                {"source_table": "items", "dim_table": "dim_items"},
            ]) as core_sync,
            patch.object(pipeline, "call_mart_refresh", side_effect=mart_refresh),
        ):
            pipeline.run_pipeline_steps(self.source, self.run_id)
        return core_sync

    def test_checkpoint_advances_only_after_mart_success(self):
        self._run(lambda expected: [])

        source = db.get_source(self.source["id"])
        run = db.get_run(self.run_id)
        self.assertEqual(source["checkpoint"], self.checkpoint_after)
        self.assertEqual(run["checkpoint_before"], self.checkpoint_before)
        self.assertEqual(run["checkpoint_after"], self.checkpoint_after)
        self.assertEqual(run["load_mode"], "incremental_upsert")
        self.assertEqual(run["status"], "done")
        self.assertEqual(self.extract_request["checkpoint_before"], self.checkpoint_before)
        self.assertEqual(self.extract_request["load_mode"], "incremental_upsert")

    def test_mart_failure_keeps_previous_checkpoint_for_retry(self):
        def fail_refresh(expected):
            raise pipeline.PipelineError("mart", "refresh failed")

        with self.assertRaises(pipeline.PipelineError):
            self._run(fail_refresh)

        source = db.get_source(self.source["id"])
        self.assertEqual(source["checkpoint"], self.checkpoint_before)

    def test_http_error_includes_service_status_and_response_detail(self):
        response = httpx.Response(
            500,
            request=httpx.Request("POST", "http://loader:8000/load"),
            json={"detail": "database permission denied"},
        )

        self.assertEqual(
            pipeline._http_error_message("loader", response),
            "loader returned HTTP 500 Internal Server Error: database permission denied",
        )


if __name__ == "__main__":
    unittest.main()
