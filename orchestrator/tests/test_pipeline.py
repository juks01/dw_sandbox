import logging
import tempfile
import unittest
import httpx
from pathlib import Path
from unittest.mock import patch

from orchestrator.app import db, pipeline
from orchestrator.app.access_log import HealthAccessLogFilter


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

    def test_incremental_source_without_watermark_runs_without_checkpoint(self):
        source = {**self.source, "watermark_field": "", "checkpoint": None}
        requests = []

        def responses(url, **kwargs):
            requests.append((url, kwargs["json"]))
            if url.endswith("/extract"):
                return FakeResponse({
                    "filename": "items.json",
                    "source_url": source["url"],
                    "checkpoint_after": None,
                    "pagination_complete": True,
                })
            return FakeResponse({"batch_id": 42, "tables": {"items": 1}})

        with (
            patch.object(pipeline, "full_health", return_value={
                "dependencies": {"extractor": True, "loader": True, "staging": True, "core": True, "mart": True},
            }),
            patch.object(pipeline.httpx, "post", side_effect=responses),
            patch.object(pipeline, "call_core_sync", return_value=[
                {"source_table": "items", "dim_table": "dim_items"},
            ]),
            patch.object(pipeline, "call_mart_refresh", return_value=[]),
        ):
            pipeline.run_pipeline_steps(source, self.run_id)

        run = db.get_run(self.run_id)
        self.assertEqual(run["load_mode"], "incremental_upsert")
        self.assertIsNone(run["checkpoint_before"])
        self.assertIsNone(run["checkpoint_after"])
        self.assertEqual(requests[0][1]["load_mode"], "incremental_upsert")
        self.assertIsNone(requests[0][1]["checkpoint_before"])
        self.assertFalse(requests[0][1]["watermark_required"])

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


class HealthCheckLoggingTests(unittest.TestCase):
    def setUp(self):
        with pipeline._health_log_lock:
            pipeline._health_log_states.clear()

    def tearDown(self):
        with pipeline._health_log_lock:
            pipeline._health_log_states.clear()

    def test_http_failure_log_identifies_service_and_target_once_until_recovery(self):
        response = httpx.Response(503, request=httpx.Request("GET", "http://extractor:8000/health"))
        with (
            patch.object(pipeline.httpx, "get", return_value=response),
            self.assertLogs("dw.health", level="WARNING") as captured,
        ):
            self.assertFalse(pipeline.check_extractor())
            self.assertFalse(pipeline.check_extractor())

        self.assertEqual(len(captured.records), 1)
        self.assertIn("extractor", captured.output[0])
        self.assertIn("http://extractor:8000/health", captured.output[0])
        self.assertIn("HTTP 503", captured.output[0])

        with (
            patch.object(
                pipeline.httpx,
                "get",
                return_value=httpx.Response(
                    200, request=httpx.Request("GET", "http://extractor:8000/health"),
                ),
            ),
            self.assertLogs("dw.health", level="INFO") as captured,
        ):
            self.assertTrue(pipeline.check_extractor())

        self.assertIn("Health check recovered for extractor", captured.output[0])

    def test_database_failure_log_identifies_target_database(self):
        with (
            patch.object(pipeline.psycopg, "connect", side_effect=RuntimeError("connection refused")),
            self.assertLogs("dw.health", level="WARNING") as captured,
        ):
            self.assertFalse(pipeline.check_staging())

        self.assertIn("staging", captured.output[0])
        self.assertIn("staging:5432/staging", captured.output[0])
        self.assertIn("connection refused", captured.output[0])

    def test_successful_health_checks_log_concise_dependency_status(self):
        response = httpx.Response(
            200, request=httpx.Request("GET", "http://extractor:8000/health"),
        )
        with (
            patch.object(pipeline.httpx, "get", return_value=response),
            self.assertLogs("uvicorn.error", level="INFO") as captured,
        ):
            self.assertTrue(pipeline.check_extractor())

        self.assertEqual(
            captured.records[0].getMessage(),
            'extractor: "GET /health" - 200 OK',
        )

    def test_successful_database_health_check_logs_query(self):
        with (
            patch.object(pipeline.psycopg, "connect"),
            self.assertLogs("uvicorn.error", level="INFO") as captured,
        ):
            self.assertTrue(pipeline.check_staging())

        self.assertEqual(
            captured.records[0].getMessage(),
            'staging: "SELECT 1" - OK',
        )

    def test_health_access_filter_suppresses_only_health_endpoint(self):
        access_filter = HealthAccessLogFilter()
        health_record = logging.LogRecord(
            "uvicorn.access", logging.INFO, __file__, 1, "%s - %s",
            ("127.0.0.1", "GET", "/health", "1.1", 200), None,
        )
        api_record = logging.LogRecord(
            "uvicorn.access", logging.INFO, __file__, 1, "%s - %s",
            ("127.0.0.1", "GET", "/api/sources", "1.1", 200), None,
        )

        self.assertFalse(access_filter.filter(health_record))
        self.assertTrue(access_filter.filter(api_record))


if __name__ == "__main__":
    unittest.main()
