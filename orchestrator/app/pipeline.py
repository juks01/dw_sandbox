from __future__ import annotations

import os
from typing import Any

import httpx
import psycopg

from . import db

EXTRACTOR_URL = os.environ.get("EXTRACTOR_URL", "http://extractor:8000")
LOADER_URL = os.environ.get("LOADER_URL", "http://loader:8000")

STAGING_HOST = os.environ.get("STAGING_HOST", "staging")
STAGING_PORT = 5432 #int(os.environ.get("STAGING_PORT", "5432"))
STAGING_DB = os.environ.get("STAGING_DB", "staging")
STAGING_SERVICE_USER = os.environ.get("STAGING_SERVICE_USER", "staging_service")
STAGING_SERVICE_PASSWORD = os.environ.get("STAGING_SERVICE_PASSWORD", "")
STAGING_READER_USER = os.environ.get("STAGING_READER_USER", "staging_reader")
STAGING_READER_PASSWORD = os.environ.get("STAGING_READER_PASSWORD", "")

CORE_HOST = os.environ.get("CORE_HOST", "core")
CORE_PORT = 5432 #int(os.environ.get("CORE_PORT", "5432"))
CORE_DB = os.environ.get("CORE_DB", "core")
CORE_SERVICE_USER = os.environ.get("CORE_SERVICE_USER", "core_service")
CORE_SERVICE_PASSWORD = os.environ.get("CORE_SERVICE_PASSWORD", "")

MART_HOST = os.environ.get("MART_HOST", "mart")
MART_PORT = 5432 #int(os.environ.get("MART_PORT", "5432"))
MART_DB = os.environ.get("MART_DB", "mart")
MART_SERVICE_USER = os.environ.get("MART_SERVICE_USER", "mart_service")
MART_SERVICE_PASSWORD = os.environ.get("MART_SERVICE_PASSWORD", "")


def _pg_select_1(host: str, port: int, dbname: str, user: str, password: str) -> bool:
    try:
        with psycopg.connect(
            host=host, port=port, dbname=dbname, user=user, password=password, connect_timeout=3
        ) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
        return True
    except Exception:
        return False


def check_extractor() -> bool:
    try:
        r = httpx.get(f"{EXTRACTOR_URL}/health", timeout=3.0)
        return r.status_code == 200
    except Exception:
        return False


def check_loader() -> bool:
    try:
        r = httpx.get(f"{LOADER_URL}/health", timeout=3.0)
        return r.status_code == 200
    except Exception:
        return False


def _http_error_message(service: str, response: httpx.Response) -> str:
    detail = response.text.strip()
    try:
        payload = response.json()
        if isinstance(payload, dict):
            detail = str(payload.get("detail", detail))
    except ValueError:
        pass
    status = f"HTTP {response.status_code}"
    if response.reason_phrase:
        status += f" {response.reason_phrase}"
    return f"{service} returned {status}: {detail or 'no response body'}"


def check_core() -> bool:
    return _pg_select_1(CORE_HOST, CORE_PORT, CORE_DB, CORE_SERVICE_USER, CORE_SERVICE_PASSWORD)


def check_staging() -> bool:
    return _pg_select_1(STAGING_HOST, STAGING_PORT, STAGING_DB, STAGING_READER_USER, STAGING_READER_PASSWORD)


def check_mart() -> bool:
    return _pg_select_1(MART_HOST, MART_PORT, MART_DB, MART_SERVICE_USER, MART_SERVICE_PASSWORD)


def full_health() -> dict[str, Any]:
    checks = {
        "extractor": check_extractor(),
        "loader": check_loader(),
        "staging": check_staging(),
        "core": check_core(),
        "mart": check_mart(),
    }
    return {"status": "ok" if all(checks.values()) else "degraded", "dependencies": checks}


def call_core_sync(
    batch_id: int, source_name: str, load_mode: str, delete_policy: str,
    key_fields: list[str],
    loaded_table_names: set[str],
) -> list[dict]:
    with psycopg.connect(
        host=CORE_HOST, port=CORE_PORT, dbname=CORE_DB,
        user=CORE_SERVICE_USER, password=CORE_SERVICE_PASSWORD, connect_timeout=10,
    ) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM core.sync_users(%s, %s)", (load_mode, delete_policy))
            cur.execute(
                "SELECT * FROM core.sync_from_staging(%s, %s, %s, %s, %s)",
                (batch_id, source_name, load_mode, delete_policy, key_fields),
            )
            rows = cur.fetchall()
        conn.commit()

    synced = [
        {"source_table": r[0], "dim_table": r[1], "inserted_count": r[2], "closed_count": r[3]}
        for r in rows
    ]
    synced_tables = {r["source_table"] for r in synced}
    missing = loaded_table_names - synced_tables
    if missing:
        raise PipelineError(
            "core",
            f"staging table(s) loaded this run never reached core (dropped by "
            f"core.sync_from_staging()): {', '.join(sorted(missing))}",
        )
    return synced


def call_mart_refresh(expected_dim_tables: set[str]) -> list[dict]:
    with psycopg.connect(
        host=MART_HOST, port=MART_PORT, dbname=MART_DB,
        user=MART_SERVICE_USER, password=MART_SERVICE_PASSWORD, connect_timeout=10,
    ) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM mart.refresh()")
            rows = cur.fetchall()
        conn.commit()

    published = [
        {"dim_table": r[0], "mart_table": r[1], "row_count": r[2]}
        for r in rows
    ]
    published_dims = {r["dim_table"] for r in published}
    missing = expected_dim_tables - published_dims
    if missing:
        raise PipelineError(
            "mart",
            f"core dim table(s) updated this run never reached mart "
            f"(stale core_ext mirror in mart.refresh()?): {', '.join(sorted(missing))}",
        )
    return published


class PipelineError(Exception):
    def __init__(self, step: str, message: str):
        super().__init__(message)
        self.step = step
        self.message = message


def run_pipeline_steps(source: dict, run_id: int, force_full: bool = False) -> None:
    """Run one source through extract, load, core and mart."""
    source_name = source["name"]
    source_url = source["url"]
    checkpoint_before = None if force_full else source.get("checkpoint")
    load_mode = (
        "full_snapshot"
        if force_full or not checkpoint_before
        else source["load_mode"]
    )
    key_fields = source.get("key_fields") or []
    db.update_run(run_id, checkpoint_before=checkpoint_before, load_mode=load_mode)

    # Pre-flight dependency health checks: don't start against a known-down dependency.
    health = full_health()
    if not all(health["dependencies"].values()):
        down = [k for k, v in health["dependencies"].items() if not v]
        raise PipelineError("health_check", f"dependencies unavailable: {', '.join(down)}")

    # ---- extract ----
    db.update_run(run_id, status="running", step="extract")
    try:
        resp = httpx.post(
            f"{EXTRACTOR_URL}/extract",
            json={
                "source": source_name,
                "url": source_url,
                "run_id": str(run_id),
                "load_mode": load_mode,
                "delete_policy": source["delete_policy"],
                "checkpoint_before": checkpoint_before,
                "incremental_param": source["incremental_param"],
                "watermark_field": source["watermark_field"],
                "watermark_required": source["load_mode"] == "incremental_upsert",
            },
            timeout=60.0,
        )
        resp.raise_for_status()
        extract_result = resp.json()
    except httpx.HTTPStatusError as exc:
        raise PipelineError("extract", _http_error_message("extractor", exc.response)) from exc
    except httpx.RequestError as exc:
        raise PipelineError("extract", f"extractor request failed: {exc}") from exc
    except Exception as exc:
        raise PipelineError("extract", str(exc)) from exc

    filename = extract_result["filename"]
    checkpoint_after = extract_result.get("checkpoint_after")
    if not extract_result.get("pagination_complete", False):
        raise PipelineError("extract", "source response is paginated; all pages must be fetched before loading")
    if source["load_mode"] == "incremental_upsert" and not checkpoint_after:
        raise PipelineError("extract", "incremental source did not produce a checkpoint")
    db.update_run(run_id, checkpoint_after=checkpoint_after)
    db.update_run(run_id, filename=filename)

    # ---- load ----
    db.update_run(run_id, step="load")
    try:
        resp = httpx.post(
            f"{LOADER_URL}/load",
            json={
                "filename": filename,
                "source": source_name,
                "source_url": extract_result.get("source_url"),
                "load_mode": load_mode,
                "delete_policy": source["delete_policy"],
                "run_id": str(run_id),
                "checkpoint_before": checkpoint_before,
                "checkpoint_after": checkpoint_after,
                "pagination_complete": True,
            },
            timeout=60.0,
        )
        resp.raise_for_status()
        load_result = resp.json()
    except httpx.HTTPStatusError as exc:
        raise PipelineError("load", _http_error_message("loader", exc.response)) from exc
    except Exception as exc:
        raise PipelineError("load", str(exc)) from exc

    if load_result.get("batch_id") is not None:
        db.update_run(run_id, batch_id=str(load_result["batch_id"]))
    if not load_result.get("batch_id"):
        raise PipelineError("load", "loader did not return a staging batch ID")

    loaded_table_names: set[str] = set(load_result.get("tables") or {})

    # ---- core ----
    db.update_run(run_id, step="core")
    try:
        core_synced = call_core_sync(
            int(load_result["batch_id"]), source_name, load_mode,
            source["delete_policy"], key_fields,
            loaded_table_names,
        )
    except PipelineError:
        raise
    except Exception as exc:
        raise PipelineError("core", str(exc)) from exc

    # ---- mart ----
    db.update_run(run_id, step="mart")
    expected_dim_tables = {
        row["dim_table"] for row in core_synced if row["source_table"] in loaded_table_names
    }
    try:
        call_mart_refresh(expected_dim_tables)
    except PipelineError:
        raise
    except Exception as exc:
        raise PipelineError("mart", str(exc)) from exc

    if checkpoint_after:
        db.update_source_checkpoint(source_name, checkpoint_after)
    db.update_run(run_id, status="done", step="done", finished=db.now_iso())
