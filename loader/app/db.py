"""Staging database access for the loader.

Every identifier that reaches SQL is either a hardcoded literal or has
already been through flatten.normalize_identifier() and is always passed
through psycopg.sql.Identifier() -- never string-concatenated.
"""
from __future__ import annotations

import os
from typing import Any, Optional

import psycopg
from psycopg import sql
from psycopg.types.json import Json

from .flatten import TECHNICAL_COLUMNS

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


def insert_raw_batch(cur: psycopg.Cursor, filename: str, source: str, source_url: Optional[str], payload: Any) -> Optional[int]:
    """Insert into staging.raw_batches. Returns the new batch id, or None if
    this filename was already loaded before (idempotency, spec section 16)."""
    cur.execute(
        """
        INSERT INTO staging.raw_batches (filename, source, source_url, payload)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (filename) DO NOTHING
        RETURNING id
        """,
        (filename, source, source_url, Json(payload)),
    )
    row = cur.fetchone()
    return row[0] if row else None


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
                _row_index INTEGER NOT NULL DEFAULT 0
            )
            """
        ).format(tbl=sql.Identifier(table_name))
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
    all_cols: list[str] = []
    seen_cols = set()
    for row in rows:
        for c in row.keys():
            if c not in seen_cols:
                seen_cols.add(c)
                all_cols.append(c)

    col_idents = sql.SQL(", ").join(sql.Identifier(c) for c in all_cols)
    placeholders = sql.SQL(", ").join(sql.Placeholder() for _ in all_cols)
    insert_sql = sql.SQL(
        "INSERT INTO staging.{tbl} ({cols}) VALUES ({vals}) ON CONFLICT (_row_id) DO NOTHING"
    ).format(tbl=sql.Identifier(table_name), cols=col_idents, vals=placeholders)

    count = 0
    for row in rows:
        values = [row.get(c) for c in all_cols]
        cur.execute(insert_sql, values)
        count += 1
    return count


def load_payload(filename: str, source: str, source_url: Optional[str], payload: Any, flatten_fn) -> dict[str, Any]:
    """flatten_fn(batch_id) -> {table_name: [row, ...]}. Flattening happens
    AFTER the batch id is known so every row can carry the real
    _source_batch_id, all inside the same transaction as the raw_batches
    insert (idempotency + consistency)."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            batch_id = insert_raw_batch(cur, filename, source, source_url, payload)
            if batch_id is None:
                conn.rollback()
                return {"status": "skipped_duplicate", "filename": filename, "tables": {}}

            tables = flatten_fn(batch_id)

            summary: dict[str, int] = {}
            for table_name, rows in tables.items():
                ensure_table(cur, table_name)
                ensure_columns(cur, table_name, rows)
                inserted = insert_rows(cur, table_name, rows)
                summary[table_name] = inserted

        conn.commit()
    return {"status": "loaded", "filename": filename, "batch_id": batch_id, "tables": summary}
