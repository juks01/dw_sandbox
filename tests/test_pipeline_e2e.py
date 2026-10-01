"""Opt-in extractor-to-mart integration tests for a running local DB stack."""
from __future__ import annotations

import hashlib
import importlib
import json
import os
import tempfile
import unittest
import uuid
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _load_dev_env() -> None:
    """Load compose's local .env without adding a dotenv dependency."""
    env_file = ROOT / ".env"
    if not env_file.is_file():
        return
    for line in env_file.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip("\"'"))


def _db_settings(prefix: str, *, user_key: str, password_key: str, default_user: str,
                 default_password: str, db_key: str, default_db: str,
                 port_key: str, default_port: int) -> dict:
    return {
        "host": os.environ.get(f"DW_TEST_{prefix}_HOST", "localhost"),
        "port": int(os.environ.get(f"DW_TEST_{prefix}_PORT", os.environ.get(port_key, default_port))),
        "dbname": os.environ.get(db_key, default_db),
        "user": os.environ.get(user_key, default_user),
        "password": os.environ.get(password_key, default_password),
        "connect_timeout": 5,
    }


@unittest.skipUnless(
    os.environ.get("DW_E2E_TESTS") == "1",
    "set DW_E2E_TESTS=1 to run against the local compose databases",
)
class ExtractorToMartTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        _load_dev_env()

        import httpx
        import psycopg
        from fastapi.testclient import TestClient
        from psycopg import sql

        cls.httpx = httpx
        cls.psycopg = psycopg
        cls.sql = sql
        cls.TestClient = TestClient
        cls.source = f"e2e-{uuid.uuid4().hex[:12]}"
        cls.table = cls.source.replace("-", "_")
        cls.dimension = f"dim_{cls.table}"
        cls.temp_dir = tempfile.TemporaryDirectory(prefix="dw-e2e-")
        cls.addClassCleanup(cls.temp_dir.cleanup)
        cls.addClassCleanup(cls._cleanup_database)

        cls.staging = _db_settings(
            "STAGING", user_key="LOADER_WRITER_USER", password_key="LOADER_WRITER_PASSWORD",
            default_user="loader_writer", default_password="loaderpass", db_key="STAGING_DB",
            default_db="staging", port_key="STAGING_PORT", default_port=5433,
        )
        cls.core_admin = _db_settings(
            "CORE", user_key="ADMIN_USER", password_key="POSTGRES_PASSWORD",
            default_user="admin", default_password="devpassword", db_key="CORE_DB",
            default_db="core", port_key="CORE_PORT", default_port=5434,
        )
        cls.core_reader = _db_settings(
            "CORE", user_key="CORE_READER_USER", password_key="CORE_READER_PASSWORD",
            default_user="core_reader", default_password="corereaderpass", db_key="CORE_DB",
            default_db="core", port_key="CORE_PORT", default_port=5434,
        )
        cls.core_service = _db_settings(
            "CORE", user_key="CORE_SERVICE_USER", password_key="CORE_SERVICE_PASSWORD",
            default_user="core_service", default_password="coreservicepass", db_key="CORE_DB",
            default_db="core", port_key="CORE_PORT", default_port=5434,
        )
        cls.mart_admin = _db_settings(
            "MART", user_key="ADMIN_USER", password_key="POSTGRES_PASSWORD",
            default_user="admin", default_password="devpassword", db_key="MART_DB",
            default_db="mart", port_key="MART_PORT", default_port=5435,
        )
        cls.reporting = _db_settings(
            "MART", user_key="REPORTING_USER", password_key="REPORTING_PASSWORD",
            default_user="reporting", default_password="reportingpass", db_key="MART_DB",
            default_db="mart", port_key="MART_PORT", default_port=5435,
        )

        cls.extractor = importlib.import_module("extractor.app.main")
        cls.loader = importlib.import_module("loader.app.main")
        cls.loader_db = importlib.import_module("loader.app.db")
        cls.extractor.LANDING_DIR = Path(cls.temp_dir.name)
        cls.loader.LANDING_DIR = Path(cls.temp_dir.name)
        cls.loader_db.STAGING_HOST = cls.staging["host"]
        cls.loader_db.STAGING_PORT = cls.staging["port"]
        cls.loader_db.STAGING_DB = cls.staging["dbname"]
        cls.loader_db.LOADER_WRITER_USER = cls.staging["user"]
        cls.loader_db.LOADER_WRITER_PASSWORD = cls.staging["password"]

        cls.payloads = [
            [{"id": 7812391, "title": "Integration item v1", "price": 12.5}],
            [{"id": 7812391, "title": "Integration item v2", "price": 14.0}],
        ]
        cls.requested_urls: list[str] = []
        original_allow = cls.extractor.is_allowed_url
        original_get = httpx.get
        cls.extractor.is_allowed_url = lambda url: url.startswith("https://example.test/")

        def fake_get(url: str, **kwargs):
            cls.requested_urls.append(url)
            revision = int(url.rsplit("=", 1)[1])
            return httpx.Response(
                200,
                json=cls.payloads[revision - 1],
                headers={"content-type": "application/json"},
                request=httpx.Request("GET", url),
            )

        filenames: list[str] = []
        try:
            httpx.get = fake_get
            with TestClient(cls.extractor.app) as client:
                for revision in (1, 2):
                    response = client.post(
                        "/extract",
                        json={
                            "source": cls.source,
                            "url": f"https://example.test/items?revision={revision}",
                            "run_id": f"e2e-{revision}",
                        },
                    )
                    response.raise_for_status()
                    filenames.append(response.json()["filename"])
        finally:
            httpx.get = original_get
            cls.extractor.is_allowed_url = original_allow

        cls.filenames = filenames
        cls.manifests = []
        for index, filename in enumerate(filenames):
            raw_bytes = (Path(cls.temp_dir.name) / filename).read_bytes()
            manifest_path = Path(cls.temp_dir.name) / f"{Path(filename).stem}.manifest.json"
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            if json.loads(raw_bytes) != cls.payloads[index]:
                raise AssertionError("extractor changed the source JSON payload")
            if manifest["sha256"] != hashlib.sha256(raw_bytes).hexdigest():
                raise AssertionError("extractor manifest checksum does not match its payload")
            cls.manifests.append(manifest)

        with TestClient(cls.loader.app) as client:
            cls.load_results = []
            for filename in filenames:
                response = client.post(
                    "/load",
                    json={"filename": filename, "source": cls.source,
                          "source_url": "https://example.test/items"},
                )
                response.raise_for_status()
                cls.load_results.append(response.json())
                cls._sync_core_and_mart()

    @classmethod
    def _sync_core_and_mart(cls) -> None:
        with cls.psycopg.connect(**cls.core_service) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM core.sync_users()")
                cur.fetchall()
                cur.execute("SELECT * FROM core.sync_from_staging()")
                synced = cur.fetchall()
        if cls.table not in {row[0] for row in synced}:
            raise AssertionError(f"staging table {cls.table!r} did not reach core")

        with cls.psycopg.connect(**cls.mart_admin) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM mart.refresh()")
                cls.published = cur.fetchall()

    @classmethod
    def _cleanup_database(cls) -> None:
        if not hasattr(cls, "source") or not hasattr(cls, "psycopg"):
            return
        cleanup = (
            (getattr(cls, "mart_admin", None), "mart", cls.table),
            (getattr(cls, "core_admin", None), "core", cls.dimension),
            (getattr(cls, "core_admin", None), "staging", cls.table),
        )
        for settings, schema, table in cleanup:
            if settings is None:
                continue
            try:
                with cls.psycopg.connect(**settings) as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            cls.sql.SQL("DROP TABLE IF EXISTS {}.{} CASCADE").format(
                                cls.sql.Identifier(schema), cls.sql.Identifier(table)
                            )
                        )
            except cls.psycopg.Error:
                pass
        try:
            with cls.psycopg.connect(**cls.staging) as conn:
                with conn.cursor() as cur:
                    cur.execute("DELETE FROM staging.raw_batches WHERE source = %s", (cls.source,))
        except cls.psycopg.Error:
            pass

    def test_dynamic_extraction_and_loader_pass_through(self):
        self.assertEqual(len(set(self.requested_urls)), 2)
        self.assertEqual([item["status"] for item in self.load_results], ["loaded", "loaded"])
        self.assertNotEqual(self.manifests[0]["sha256"], self.manifests[1]["sha256"])

        with self.psycopg.connect(**self.staging) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT payload FROM staging.raw_batches WHERE filename = ANY(%s) ORDER BY id",
                    (self.filenames,),
                )
                self.assertEqual(cur.fetchall(), [(payload,) for payload in self.payloads])

    def test_scd2_closes_old_version_and_keeps_current(self):
        query = self.sql.SQL(
            "SELECT title, is_current, valid_to FROM core.{} ORDER BY _sk"
        ).format(self.sql.Identifier(self.dimension))
        with self.psycopg.connect(**self.core_admin) as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                versions = cur.fetchall()

        self.assertEqual(len(versions), 2)
        self.assertEqual([(row[0], row[1]) for row in versions], [
            ("Integration item v1", False), ("Integration item v2", True)
        ])
        self.assertIsNotNone(versions[0][2])
        self.assertIsNone(versions[1][2])

    def test_mart_publishes_only_the_latest_source_values(self):
        query = self.sql.SQL("SELECT * FROM mart.{}").format(self.sql.Identifier(self.table))
        with self.psycopg.connect(**self.reporting) as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                rows = cur.fetchall()
                columns = [column.name for column in cur.description]

        self.assertEqual(len(rows), 1)
        published = dict(zip(columns, rows[0]))
        self.assertIn("title", columns)
        self.assertEqual(published["title"], "Integration item v2")
        self.assertTrue({"valid_from", "valid_to", "is_current"}.isdisjoint(columns))

    def test_core_user_rows_are_filtered_by_department_rls(self):
        with self.psycopg.connect(**self.core_reader) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT set_config('core.department_code', 'ALL', false)")
                cur.execute("SELECT count(*) FROM core.dim_user")
                total_users = cur.fetchone()[0]

                cur.execute("SELECT set_config('core.department_code', 'ENG', false)")
                cur.execute(
                    "SELECT DISTINCT d.code FROM core.dim_user u "
                    "JOIN core.dim_department d USING (department_id)"
                )
                engineering_codes = {row[0] for row in cur.fetchall()}
                cur.execute("SELECT count(*) FROM core.dim_user")
                engineering_users = cur.fetchone()[0]

                cur.execute("SELECT set_config('core.department_code', 'SALES', false)")
                cur.execute(
                    "SELECT DISTINCT d.code FROM core.dim_user u "
                    "JOIN core.dim_department d USING (department_id)"
                )
                sales_codes = {row[0] for row in cur.fetchall()}

        self.assertGreater(total_users, engineering_users)
        self.assertEqual(engineering_codes, {"ENG"})
        self.assertEqual(sales_codes, {"SALES"})


if __name__ == "__main__":
    unittest.main()