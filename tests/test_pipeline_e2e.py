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


def _required_setting(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"missing required test setting: {name}")
    return value


def _db_settings(prefix: str, *, user_key: str, password_key: str, db_key: str) -> dict:
    return {
        "host": _required_setting(f"DW_TEST_{prefix}_HOST"),
        "port": int(_required_setting(f"DW_TEST_{prefix}_PORT")),
        "dbname": _required_setting(db_key),
        "user": _required_setting(user_key),
        "password": _required_setting(password_key),
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
            db_key="STAGING_DB",
        )
        cls.staging_admin = _db_settings(
            "STAGING", user_key="ADMIN_USER", password_key="POSTGRES_PASSWORD",
            db_key="STAGING_DB",
        )
        cls.core_admin = _db_settings(
            "CORE", user_key="ADMIN_USER", password_key="POSTGRES_PASSWORD",
            db_key="CORE_DB",
        )
        cls.core_reader = _db_settings(
            "CORE", user_key="CORE_READER_USER", password_key="CORE_READER_PASSWORD",
            db_key="CORE_DB",
        )
        cls.core_service = _db_settings(
            "CORE", user_key="CORE_SERVICE_USER", password_key="CORE_SERVICE_PASSWORD",
            db_key="CORE_DB",
        )
        cls.mart_admin = _db_settings(
            "MART", user_key="ADMIN_USER", password_key="POSTGRES_PASSWORD",
            db_key="MART_DB",
        )
        cls.reporting = _db_settings(
            "MART", user_key="REPORTING_USER", password_key="REPORTING_PASSWORD",
            db_key="MART_DB",
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
            [
                {"id": 7812391, "title": "Integration item v1", "price": 12.5,
                 "updated_at": "2026-10-01T10:00:00+00:00"},
                {"id": 7812392, "title": "Unchanged incremental item", "price": 8.0,
                 "updated_at": "2026-10-01T10:00:00+00:00"},
            ],
            [{"id": 7812391, "title": "Integration item v2", "price": 14.0,
              "updated_at": "2026-10-01T11:00:00+00:00"}],
            [{"id": 7812391, "title": "Integration item v2", "price": 14.0,
              "updated_at": "2026-10-01T11:00:00+00:00"}],
            [{"id": 7812391, "title": "Integration item v2", "price": 14.0,
              "updated_at": "2026-10-01T11:00:00+00:00"}],
        ]
        cls.load_modes = [
            "full_snapshot",
            "incremental_upsert",
            "full_snapshot",
            "full_snapshot",
        ]
        cls.delete_policies = [
            "close_on_full_snapshot",
            "close_on_full_snapshot",
            "never_close",
            "close_on_full_snapshot",
        ]
        cls.requested_urls: list[str] = []
        original_allow = cls.extractor.is_allowed_url
        original_get = httpx.get
        cls.extractor.is_allowed_url = lambda url: url.startswith("https://example.test/")

        def fake_get(url: str, **kwargs):
            cls.requested_urls.append(url)
            revision = int(httpx.URL(url).params["revision"])
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
                checkpoint = None
                for revision in range(1, len(cls.payloads) + 1):
                    load_mode = cls.load_modes[revision - 1]
                    delete_policy = cls.delete_policies[revision - 1]
                    response = client.post(
                        "/extract",
                        json={
                            "source": cls.source,
                            "url": f"https://example.test/items?revision={revision}",
                            "run_id": f"e2e-{revision}",
                            "load_mode": load_mode,
                            "delete_policy": delete_policy,
                            "checkpoint_before": checkpoint,
                            "incremental_param": "updated_since",
                            "watermark_field": "updated_at",
                            "watermark_required": True,
                        },
                    )
                    response.raise_for_status()
                    result = response.json()
                    filenames.append(result["filename"])
                    checkpoint = result["checkpoint_after"]
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
            for index, filename in enumerate(filenames):
                manifest = cls.manifests[index]
                response = client.post(
                    "/load",
                    json={"filename": filename, "source": cls.source,
                          "source_url": "https://example.test/items",
                          "load_mode": manifest["load_mode"],
                          "delete_policy": manifest["delete_policy"],
                          "run_id": manifest["run_id"],
                          "checkpoint_before": manifest["checkpoint_before"],
                          "checkpoint_after": manifest["checkpoint_after"],
                          "pagination_complete": manifest["pagination_complete"]},
                )
                response.raise_for_status()
                cls.load_results.append(response.json())
                cls._sync_core_and_mart()
                if index == 2:
                    with cls.psycopg.connect(**cls.core_admin) as conn:
                        with conn.cursor() as cur:
                            cur.execute(
                                cls.sql.SQL(
                                    "SELECT is_current FROM core.{} "
                                    "WHERE id = %s AND _business_key = %s"
                                ).format(cls.sql.Identifier(cls.dimension)),
                                (7812392, "7812392"),
                            )
                            cls.never_close_kept_absent_key = cur.fetchone() == (True,)

    @classmethod
    def _sync_core_and_mart(cls) -> None:
        batch = cls.load_results[-1]
        mode = cls.manifests[len(cls.load_results) - 1]["load_mode"]
        with cls.psycopg.connect(**cls.core_service) as conn:
            with conn.cursor() as cur:
                delete_policy = cls.manifests[len(cls.load_results) - 1]["delete_policy"]
                cur.execute("SELECT * FROM core.sync_users(%s, %s)", (mode, delete_policy))
                cur.fetchall()
                cur.execute(
                    "SELECT * FROM core.sync_from_staging(%s, %s, %s, %s, %s)",
                    (batch["batch_id"], cls.source, mode, delete_policy, ["id"]),
                )
                synced = cur.fetchall()
        if cls.table not in {row[0] for row in synced}:
            raise AssertionError(f"staging table {cls.table!r} did not reach core")

        with cls.psycopg.connect(**cls.mart_admin) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM mart.refresh()")

    @classmethod
    def _cleanup_database(cls) -> None:
        if not hasattr(cls, "source") or not hasattr(cls, "psycopg"):
            return
        cleanup = (
            (getattr(cls, "mart_admin", None), "mart", cls.table),
            (getattr(cls, "core_admin", None), "core", cls.dimension),
            (getattr(cls, "staging_admin", None), "staging", cls.table),
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
            with cls.psycopg.connect(**cls.staging_admin) as conn:
                with conn.cursor() as cur:
                    cur.execute("DELETE FROM staging.raw_batches WHERE source = %s", (cls.source,))
        except cls.psycopg.Error:
            pass

    def test_dynamic_extraction_and_loader_pass_through(self):
        self.assertEqual(len(set(self.requested_urls)), 4)
        self.assertEqual([item["status"] for item in self.load_results], ["loaded"] * 4)
        self.assertNotEqual(self.manifests[0]["sha256"], self.manifests[1]["sha256"])
        self.assertEqual(
            [manifest["load_mode"] for manifest in self.manifests],
            self.load_modes,
        )
        self.assertEqual(
            [manifest["delete_policy"] for manifest in self.manifests],
            self.delete_policies,
        )

        with self.psycopg.connect(**self.staging_admin) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT payload FROM staging.raw_batches WHERE filename = ANY(%s) ORDER BY id",
                    (self.filenames,),
                )
                self.assertEqual(cur.fetchall(), [(payload,) for payload in self.payloads])

    def test_scd2_closes_old_version_and_keeps_current(self):
        query = self.sql.SQL(
            "SELECT id, title, is_current, valid_to FROM core.{} ORDER BY _sk"
        ).format(self.sql.Identifier(self.dimension))
        with self.psycopg.connect(**self.core_admin) as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                versions = cur.fetchall()

        self.assertEqual(len(versions), 3)
        self.assertEqual(
            [(row[1], row[2]) for row in versions if row[0] == 7812391],
            [("Integration item v1", False), ("Integration item v2", True)],
        )
        self.assertEqual(
            [(row[1], row[2]) for row in versions if row[0] == 7812392],
            [("Unchanged incremental item", False)],
        )
        self.assertIsNotNone(next(row[3] for row in versions if row[1] == "Integration item v1"))
        self.assertIsNone(next(row[3] for row in versions if row[1] == "Integration item v2"))
        self.assertTrue(self.never_close_kept_absent_key)

    def test_mart_publishes_only_the_latest_source_values(self):
        query = self.sql.SQL("SELECT * FROM mart.{}").format(self.sql.Identifier(self.table))
        with self.psycopg.connect(**self.reporting) as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                rows = cur.fetchall()
                columns = [column.name for column in cur.description]

        self.assertEqual(len(rows), 1)
        published = [dict(zip(columns, row)) for row in rows]
        self.assertIn("title", columns)
        self.assertEqual({row["title"] for row in published}, {"Integration item v2"})
        self.assertTrue({"valid_from", "valid_to", "is_current"}.isdisjoint(columns))

    def test_supported_api_json_shapes_reach_mart(self):
        cases = {
            "flat-array": {
                "url": "https://example.test/flat-array",
                "payload": [
                    {"id": 9910101, "name": "Array row one", "active": True},
                    {"id": 9910102, "name": "Array row two", "active": False},
                ],
            },
            "dummyjson-products": {
                "url": "https://example.test/products?limit=2",
                "pages": {
                    0: {
                        "products": [
                            {
                                "id": 9910201,
                                "title": "DummyJSON product one",
                                "price": 12.5,
                                "category": "beauty",
                                "brand": "Test brand",
                                "sku": "E2E-9910201",
                                "meta": {"createdAt": "2026-10-01T10:00:00Z"},
                                "tags": ["beauty", "test"],
                                "images": ["https://example.test/9910201.png"],
                                "reviews": [{
                                    "rating": 5,
                                    "comment": "Test review",
                                    "date": "2026-10-01T11:00:00Z",
                                    "reviewerName": "Test reviewer",
                                    "reviewerEmail": "reviewer@example.test",
                                }],
                            },
                            {
                                "id": 9910202,
                                "title": "DummyJSON product two",
                                "price": 18.0,
                                "category": "furniture",
                                "brand": "Another brand",
                                "sku": "E2E-9910202",
                                "meta": {"createdAt": "2026-10-02T10:00:00Z"},
                                "tags": ["home"],
                                "images": ["https://example.test/9910202.png"],
                                "reviews": [],
                            },
                        ],
                        "total": 3,
                        "skip": 0,
                        "limit": 2,
                    },
                    2: {
                        "products": [
                            {
                                "id": 9910203,
                                "title": "DummyJSON product three",
                                "price": 23.0,
                                "category": "kitchen",
                                "brand": "Third brand",
                                "sku": "E2E-9910203",
                                "meta": {"createdAt": "2026-10-03T10:00:00Z"},
                                "tags": ["kitchen"],
                                "images": ["https://example.test/9910203.png"],
                                "reviews": [],
                            },
                        ],
                        "total": 3,
                        "skip": 2,
                        "limit": 2,
                    },
                },
            },
            "nested-object": {
                "url": "https://example.test/nested-object",
                "payload": {
                    "id": 9910301,
                    "name": "Nested API object",
                    "profile": {"region": "north", "verified": True},
                    "tags": ["priority", "customer"],
                    "orders": [{"id": 9910311, "amount": 27.5}],
                },
            },
        }
        created: list[tuple[str, set[str]]] = []
        original_get = self.httpx.get
        original_allow = self.extractor.is_allowed_url

        def fake_get(url: str, **kwargs):
            parsed_url = self.httpx.URL(url)
            case_name = parsed_url.path.rsplit("/", 1)[-1]
            if case_name == "products":
                case_name = "dummyjson-products"
            case = cases[case_name]
            if "pages" in case:
                offset = int(parsed_url.params.get("skip", "0"))
                payload = case["pages"][offset]
            else:
                payload = case["payload"]
            return self.httpx.Response(
                200,
                json=payload,
                headers={"content-type": "application/json"},
                request=self.httpx.Request("GET", url),
            )

        self.extractor.is_allowed_url = lambda url: url.startswith("https://example.test/")
        self.httpx.get = fake_get
        try:
            with self.TestClient(self.extractor.app) as extractor_client, self.TestClient(self.loader.app) as loader_client:
                for case_name, case in cases.items():
                    source = f"{self.source}-{case_name}"
                    root_table = source.replace("-", "_")
                    table_names: set[str] = {root_table}
                    extract_response = extractor_client.post(
                        "/extract",
                        json={
                            "source": source,
                            "url": case["url"],
                            "run_id": f"{source}-run",
                            "load_mode": "full_snapshot",
                            "delete_policy": "close_on_full_snapshot",
                        },
                    )
                    extract_response.raise_for_status()
                    extracted = extract_response.json()
                    self.assertTrue(extracted["pagination_complete"], case_name)
                    manifest_path = Path(self.temp_dir.name) / extracted["manifest_filename"]
                    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
                    self.assertEqual(manifest["source"], source)

                    load_response = loader_client.post(
                        "/load",
                        json={
                            "filename": extracted["filename"],
                            "source": source,
                            "source_url": case["url"],
                            "load_mode": manifest["load_mode"],
                            "delete_policy": manifest["delete_policy"],
                            "run_id": manifest["run_id"],
                            "checkpoint_before": manifest["checkpoint_before"],
                            "checkpoint_after": manifest["checkpoint_after"],
                            "pagination_complete": manifest["pagination_complete"],
                        },
                    )
                    load_response.raise_for_status()
                    loaded = load_response.json()
                    table_names.update(loaded["tables"])
                    created.append((source, table_names))

                    with self.psycopg.connect(**self.core_service) as conn:
                        with conn.cursor() as cur:
                            cur.execute("SELECT * FROM core.sync_users(%s, %s)", (
                                "full_snapshot", "close_on_full_snapshot",
                            ))
                            cur.fetchall()
                            cur.execute(
                                "SELECT * FROM core.sync_from_staging(%s, %s, %s, %s, %s)",
                                (
                                    loaded["batch_id"], source, "full_snapshot",
                                    "close_on_full_snapshot", ["id"],
                                ),
                            )
                            synced_tables = {row[0] for row in cur.fetchall()}
                    self.assertEqual(synced_tables, table_names, case_name)

                    with self.psycopg.connect(**self.mart_admin) as conn:
                        with conn.cursor() as cur:
                            cur.execute("SELECT * FROM mart.refresh()")
                            cur.fetchall()

                    if case_name == "flat-array":
                        with self.psycopg.connect(**self.reporting) as conn:
                            with conn.cursor() as cur:
                                cur.execute(
                                    self.sql.SQL("SELECT id, name FROM mart.{} ORDER BY id").format(
                                        self.sql.Identifier(root_table),
                                    )
                                )
                                self.assertEqual(cur.fetchall(), [
                                    (9910101, "Array row one"),
                                    (9910102, "Array row two"),
                                ])
                    elif case_name == "dummyjson-products":
                        products_table = f"{root_table}_products"
                        with self.psycopg.connect(**self.reporting) as conn:
                            with conn.cursor() as cur:
                                cur.execute(
                                    self.sql.SQL(
                                        "SELECT id, title, category, sku, meta_createdat "
                                        "FROM mart.{} ORDER BY id"
                                    ).format(
                                        self.sql.Identifier(products_table),
                                    )
                                )
                                self.assertEqual(cur.fetchall(), [
                                    (9910201, "DummyJSON product one", "beauty", "E2E-9910201", "2026-10-01T10:00:00Z"),
                                    (9910202, "DummyJSON product two", "furniture", "E2E-9910202", "2026-10-02T10:00:00Z"),
                                    (9910203, "DummyJSON product three", "kitchen", "E2E-9910203", "2026-10-03T10:00:00Z"),
                                ])
                                cur.execute(
                                    self.sql.SQL(
                                        "SELECT value FROM mart.{}"
                                    ).format(
                                        self.sql.Identifier(f"{products_table}_tags"),
                                    )
                                )
                                self.assertCountEqual(cur.fetchall(), [
                                    ("beauty",), ("test",), ("home",), ("kitchen",),
                                ])
                                cur.execute(
                                    self.sql.SQL(
                                        "SELECT rating, comment, reviewername "
                                        "FROM mart.{}"
                                    ).format(
                                        self.sql.Identifier(f"{products_table}_reviews"),
                                    )
                                )
                                self.assertEqual(cur.fetchall(), [
                                    (5, "Test review", "Test reviewer"),
                                ])
                    else:
                        tags_table = f"{root_table}_tags"
                        orders_table = f"{root_table}_orders"
                        with self.psycopg.connect(**self.reporting) as conn:
                            with conn.cursor() as cur:
                                cur.execute(
                                    self.sql.SQL(
                                        "SELECT id, name, profile_region, profile_verified "
                                        "FROM mart.{}"
                                    ).format(self.sql.Identifier(root_table))
                                )
                                self.assertEqual(cur.fetchall(), [
                                    (9910301, "Nested API object", "north", True),
                                ])
                                cur.execute(
                                    self.sql.SQL("SELECT value FROM mart.{} ORDER BY _row_index").format(
                                        self.sql.Identifier(tags_table),
                                    )
                                )
                                self.assertEqual(cur.fetchall(), [("priority",), ("customer",)])
                                cur.execute(
                                    self.sql.SQL("SELECT id, amount FROM mart.{}").format(
                                        self.sql.Identifier(orders_table),
                                    )
                                )
                                self.assertEqual(cur.fetchall(), [(9910311, 27.5)])
        finally:
            self.httpx.get = original_get
            self.extractor.is_allowed_url = original_allow
            for source, table_names in created:
                for table in table_names:
                    for settings, schema, name in (
                        (self.mart_admin, "mart", table),
                        (self.core_admin, "core", f"dim_{table}"),
                        (self.staging_admin, "staging", table),
                    ):
                        with self.psycopg.connect(**settings) as conn:
                            with conn.cursor() as cur:
                                cur.execute(
                                    self.sql.SQL("DROP TABLE IF EXISTS {}.{} CASCADE").format(
                                        self.sql.Identifier(schema),
                                        self.sql.Identifier(name),
                                    )
                                )
                with self.psycopg.connect(**self.staging_admin) as conn:
                    with conn.cursor() as cur:
                        cur.execute("DELETE FROM staging.raw_batches WHERE source = %s", (source,))

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
