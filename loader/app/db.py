"""Staging database access for the loader.

Every identifier that reaches SQL is either a hardcoded literal or has
already been through flatten.normalize_identifier() and is always passed
through psycopg.sql.Identifier() -- never string-concatenated.
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Optional

import psycopg
from psycopg import sql
from psycopg.types.json import Json

from .flatten import TECHNICAL_COLUMNS, iter_flattened_batches

STAGING_HOST = os.environ.get("STAGING_HOST", "staging")
STAGING_PORT = int(os.environ.get("STAGING_PORT", "5432"))
STAGING_DB = os.environ.get("STAGING_DB", "staging")
LOADER_WRITER_USER = os.environ.get("LOADER_WRITER_USER", "loader_writer")
LOADER_WRITER_PASSWORD = os.environ.get("LOADER_WRITER_PASSWORD", "")


def get_conn() -> psycopg.Connection:
    return psycopg.connect(
        host=STAGING_HOST,
        port=STAGING_PORT,
        dbname=STAGING_DB,
        user=LOADER_WRITER_USER,
        password=LOADER_WRITER_PASSWORD,
        autocommit=False,
    )


def health_check() -> bool:
    try:
        with psycopg.connect(
            host=STAGING_HOST,
            port=STAGING_PORT,
            dbname=STAGING_DB,
            user=LOADER_WRITER_USER,
            password=LOADER_WRITER_PASSWORD,
            connect_timeout=3,
        ) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
        return True
    except Exception:
        return False


def insert_raw_batch(
    cur: psycopg.Cursor, filename: str, source: str, source_url: Optional[str],
    payload_sha256: str, payload_size: int, load_mode: str, delete_policy: str,
    run_id: Optional[str],
    checkpoint_before: Optional[str], checkpoint_after: Optional[str],
    pagination_complete: bool,
) -> tuple[int, bool, list[str]]:
    """Return the batch ID, whether it is new, and table names for duplicates."""
    cur.execute(
        """
        INSERT INTO staging.raw_batches
            (filename, source, source_url, payload, payload_filename, payload_sha256,
             payload_size, run_id, load_mode, delete_policy, checkpoint_before,
             checkpoint_after, pagination_complete)
        VALUES (%s, %s, %s, NULL, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (filename) DO NOTHING
        RETURNING id
        """,
        (
            filename, source, source_url, filename, payload_sha256, payload_size,
            run_id, load_mode, delete_policy, checkpoint_before, checkpoint_after,
            pagination_complete,
        ),
    )
    row = cur.fetchone()
    if row:
        return row[0], True, []
    cur.execute(
        "SELECT id, table_names FROM staging.raw_batches WHERE filename = %s",
        (filename,),
    )
    existing = cur.fetchone()
    if not existing:
        raise RuntimeError("batch insert conflicted but existing batch was not found")
    return existing[0], False, existing[1] or []


def _pg_type_for(value: Any) -> str:
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "bigint"
    if isinstance(value, float):
        return "double precision"
    if value is None:
        return "text"
    return "text"


def ensure_table(cur: psycopg.Cursor, table_name: str) -> None:
    cur.execute(
        sql.SQL(
            """
            CREATE TABLE IF NOT EXISTS staging.{tbl} (
                _row_id UUID PRIMARY KEY,
                _source_batch_id BIGINT NOT NULL REFERENCES staging.raw_batches(id),
                _parent_id UUID,
                _parent_key TEXT,
                _row_index INTEGER NOT NULL DEFAULT 0
            )
            """
        ).format(tbl=sql.Identifier(table_name))
    )
    cur.execute(
        sql.SQL("ALTER TABLE staging.{tbl} ADD COLUMN IF NOT EXISTS _parent_key TEXT").format(
            tbl=sql.Identifier(table_name)
        )
    )
    cur.execute(
        sql.SQL("GRANT SELECT, INSERT, UPDATE ON staging.{tbl} TO loader_writer").format(
            tbl=sql.Identifier(table_name)
        )
    )
    cur.execute(
        sql.SQL("GRANT SELECT ON staging.{tbl} TO staging_reader").format(tbl=sql.Identifier(table_name))
    )


def existing_columns(cur: psycopg.Cursor, table_name: str) -> set[str]:
    cur.execute(
        """
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = 'staging' AND table_name = %s
        """,
        (table_name,),
    )
    return {r[0] for r in cur.fetchall()}


def ensure_columns(cur: psycopg.Cursor, table_name: str, rows: list[dict]) -> None:
    """Add any columns present in `rows` but missing from the table.
    Column types are a best-effort initial guess (spec section 5: types are
    only preliminary guesses at this stage)."""
    have = existing_columns(cur, table_name)
    seen: dict[str, str] = {}
    for row in rows:
        for col, value in row.items():
            if col in TECHNICAL_COLUMNS or col in have or col in seen:
                continue
            seen[col] = _pg_type_for(value)

    for col, pg_type in seen.items():
        cur.execute(
            sql.SQL("ALTER TABLE staging.{tbl} ADD COLUMN IF NOT EXISTS {col} {typ}").format(
                tbl=sql.Identifier(table_name),
                col=sql.Identifier(col),
                typ=sql.SQL(pg_type),
            )
        )


def insert_rows(cur: psycopg.Cursor, table_name: str, rows: list[dict]) -> int:
    if not rows:
        return 0
    all_cols = list(dict.fromkeys(col for row in rows for col in row))

    col_idents = sql.SQL(", ").join(sql.Identifier(c) for c in all_cols)
    placeholders = sql.SQL(", ").join(sql.Placeholder() for _ in all_cols)
    insert_sql = sql.SQL(
        "INSERT INTO staging.{tbl} ({cols}) VALUES ({vals}) ON CONFLICT (_row_id) DO NOTHING"
    ).format(tbl=sql.Identifier(table_name), cols=col_idents, vals=placeholders)

    cur.executemany(
        insert_sql,
        ([row.get(col) for col in all_cols] for row in rows),
    )
    return len(rows)


def load_payload(
    filename: str, source: str, source_url: Optional[str], payload_path: Path,
    payload_sha256: str, payload_size: int, root_type: str,
    collection_key: Optional[str], load_mode: str, delete_policy: str, run_id: Optional[str],
    checkpoint_before: Optional[str], checkpoint_after: Optional[str],
    pagination_complete: bool,
) -> dict[str, Any]:
    """Stream bounded row groups into one atomic staging batch."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            batch_id, is_new, existing_tables = insert_raw_batch(
                cur, filename, source, source_url, payload_sha256, payload_size,
                load_mode, delete_policy, run_id, checkpoint_before, checkpoint_after,
                pagination_complete,
            )
            if not is_new:
                return {
                    "status": "skipped_duplicate",
                    "filename": filename,
                    "batch_id": batch_id,
                    "tables": {name: 0 for name in existing_tables},
                }

            summary: dict[str, int] = {}
            initialized_tables: set[str] = set()
            table_names: list[str] = []
            for tables in iter_flattened_batches(
                source, payload_path, batch_id, root_type, collection_key,
            ):
                for table_name, rows in tables.items():
                    if table_name not in initialized_tables:
                        ensure_table(cur, table_name)
                        initialized_tables.add(table_name)
                        table_names.append(table_name)
                    ensure_columns(cur, table_name, rows)
                    inserted = insert_rows(cur, table_name, rows)
                    summary[table_name] = summary.get(table_name, 0) + inserted
            cur.execute(
                "UPDATE staging.raw_batches SET table_names = %s WHERE id = %s",
                (Json(table_names), batch_id),
            )

        conn.commit()
    return {"status": "loaded", "filename": filename, "batch_id": batch_id, "tables": summary}
