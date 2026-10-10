from __future__ import annotations

import json
import os
from datetime import datetime
from typing import Any

import psycopg
from psycopg import sql

DB_HOST = os.environ.get("MOCK_SOURCE_DB_HOST", "localhost")
DB_PORT = int(os.environ.get("MOCK_SOURCE_DB_PORT", "5432"))
DB_NAME = os.environ.get("MOCK_SOURCE_DB", "mock_source")
DB_USER = os.environ.get("MOCK_SOURCE_DB_USER", "mock_source")
DB_PASSWORD = os.environ.get("MOCK_SOURCE_DB_PASSWORD", "")

TABLES = {
    "departments": "departments",
    "customers": "customers",
    "medium_records": "medium_records",
    "large_records": "large_records",
}
BULK_TABLES = ("medium_records", "large_records")


def connect() -> psycopg.Connection:
    return psycopg.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        connect_timeout=5,
    )


def ensure_schema(conn: psycopg.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS departments (
            id BIGINT PRIMARY KEY,
            created_at TIMESTAMPTZ NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL,
            name TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS customers (
            id BIGINT PRIMARY KEY,
            created_at TIMESTAMPTZ NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL,
            name TEXT NOT NULL,
            email TEXT NOT NULL,
            department_id BIGINT NOT NULL REFERENCES departments(id)
        );
        CREATE TABLE IF NOT EXISTS medium_records (
            id BIGINT PRIMARY KEY,
            created_at TIMESTAMPTZ NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL,
            record_type TEXT NOT NULL,
            score INTEGER NOT NULL,
            payload TEXT NOT NULL,
            attributes JSONB NOT NULL,
            metadata JSONB NOT NULL
        );
        CREATE TABLE IF NOT EXISTS large_records (
            id BIGINT PRIMARY KEY,
            created_at TIMESTAMPTZ NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL,
            record_type TEXT NOT NULL,
            score INTEGER NOT NULL,
            payload TEXT NOT NULL,
            attributes JSONB NOT NULL,
            metadata JSONB NOT NULL
        );

        UPDATE departments
        SET created_at = now() - ages.created_age_days * interval '1 day',
            updated_at = now() - random() * 365 * interval '1 day'
        FROM (
            SELECT id, 365 + random() * 730 AS created_age_days
            FROM departments WHERE created_at IS NULL
        ) AS ages
        WHERE departments.id = ages.id;

        UPDATE customers
        SET created_at = now() - ages.created_age_days * interval '1 day',
            updated_at = now() - random() * 365 * interval '1 day'
        FROM (
            SELECT id, 365 + random() * 730 AS created_age_days
            FROM customers WHERE created_at IS NULL
        ) AS ages
        WHERE customers.id = ages.id;

        UPDATE medium_records
        SET created_at = COALESCE(
                medium_records.created_at,
                now() - ages.created_age_days * interval '1 day'
            ),
            updated_at = CASE
                WHEN medium_records.created_at IS NULL
                    THEN now() - random() * 365 * interval '1 day'
                ELSE medium_records.updated_at
            END,
            record_type = COALESCE(
                record_type, (ARRAY['event', 'metric', 'snapshot'])[1 + medium_records.id % 3]
            ),
            score = COALESCE(score, (medium_records.id % 1000)::INTEGER),
            attributes = COALESCE(attributes, jsonb_build_object(
                'source', 'mock', 'labels', jsonb_build_array('synthetic', 'bulk'),
                'active', TRUE
            )),
            metadata = COALESCE(metadata, jsonb_build_object(
                'version', 1, 'context', jsonb_build_object(
                    'batch', medium_records.id / 1000, 'generation', 'fixture'
                )
            ))
        FROM (
            SELECT id, 365 + random() * 730 AS created_age_days
            FROM medium_records
            WHERE created_at IS NULL OR record_type IS NULL OR score IS NULL
                OR attributes IS NULL OR metadata IS NULL
        ) AS ages
        WHERE medium_records.id = ages.id;

        UPDATE large_records
        SET created_at = COALESCE(
                large_records.created_at,
                now() - ages.created_age_days * interval '1 day'
            ),
            updated_at = CASE
                WHEN large_records.created_at IS NULL
                    THEN now() - random() * 365 * interval '1 day'
                ELSE large_records.updated_at
            END,
            record_type = COALESCE(
                record_type, (ARRAY['event', 'metric', 'snapshot'])[1 + large_records.id % 3]
            ),
            score = COALESCE(score, (large_records.id % 1000)::INTEGER),
            attributes = COALESCE(attributes, jsonb_build_object(
                'source', 'mock', 'labels', jsonb_build_array('synthetic', 'bulk'),
                'active', TRUE
            )),
            metadata = COALESCE(metadata, jsonb_build_object(
                'version', 1, 'context', jsonb_build_object(
                    'batch', large_records.id / 1000, 'generation', 'fixture'
                )
            ))
        FROM (
            SELECT id, 365 + random() * 730 AS created_age_days
            FROM large_records
            WHERE created_at IS NULL OR record_type IS NULL OR score IS NULL
                OR attributes IS NULL OR metadata IS NULL
        ) AS ages
        WHERE large_records.id = ages.id;
        CREATE INDEX IF NOT EXISTS customers_updated_at_idx ON customers(updated_at);
        CREATE INDEX IF NOT EXISTS medium_records_updated_at_idx ON medium_records(updated_at);
        CREATE INDEX IF NOT EXISTS large_records_updated_at_idx ON large_records(updated_at);
        """
    )


def list_page(
    table: str, limit: int, offset: int, updated_since: datetime | None,
) -> tuple[list[dict[str, Any]], int]:
    if table not in TABLES:
        raise ValueError(f"unknown mock table: {table}")

    table_name = sql.Identifier(TABLES[table])
    conditions = sql.SQL(" WHERE updated_at > %s") if updated_since else sql.SQL("")
    params: list[Any] = [updated_since] if updated_since else []
    with connect() as conn:
        total = conn.execute(
            sql.SQL("SELECT count(*) FROM {}{}").format(table_name, conditions),
            params,
        ).fetchone()[0]
        with conn.cursor() as cursor:
            cursor.execute(
                sql.SQL("SELECT * FROM {}{} ORDER BY id LIMIT %s OFFSET %s").format(
                    table_name, conditions,
                ),
                [*params, limit, offset],
            )
            rows = cursor.fetchall()
            columns = [description.name for description in cursor.description]
    return [
        {
            key: value.isoformat().replace("+00:00", "Z") if isinstance(value, datetime) else value
            for key, value in zip(columns, row)
        }
        for row in rows
    ], total


def dataset_stats() -> dict[str, dict[str, int]]:
    """Return exact bulk API byte sizes and PostgreSQL relation sizes."""
    result: dict[str, dict[str, int]] = {}
    with connect() as conn:
        for table in TABLES:
            table_name = sql.Identifier(table)
            row_count, disk_bytes = conn.execute(
                sql.SQL(
                    "SELECT count(*), pg_total_relation_size(%s::regclass) FROM {}"
                ).format(table_name),
                (table,),
            ).fetchone()
            stats = {"rows": row_count, "database_bytes": disk_bytes}
            if table in BULK_TABLES:
                sum_values = conn.execute(
                    sql.SQL(
                        """
                        SELECT COALESCE(sum(octet_length(regexp_replace(
                            json_build_object(
                                'id', id,
                                'created_at', to_char(created_at AT TIME ZONE 'UTC',
                                    'YYYY-MM-DD"T"HH24:MI:SS.US') || 'Z',
                                'updated_at', to_char(updated_at AT TIME ZONE 'UTC',
                                    'YYYY-MM-DD"T"HH24:MI:SS.US') || 'Z',
                                'record_type', record_type,
                                'score', score,
                                'payload', payload,
                                'attributes', attributes,
                                'metadata', metadata
                            )::text,
                            '[[:space:]]*([,:])[[:space:]]*', '\\1', 'g'
                        ))), 0)
                        FROM {}
                        """
                    ).format(table_name)
                ).fetchone()[0]
                template_size = len(json.dumps(
                    {
                        "id": 0,
                        "created_at": "",
                        "updated_at": "",
                        "record_type": "",
                        "score": 0,
                        "payload": "",
                        "attributes": {},
                        "metadata": {},
                    },
                    separators=(",", ":"),
                ).encode("utf-8"))
                row_overhead = template_size - 1
                last_offset = max(0, ((row_count - 1) // 1000) * 1000)
                envelope_bytes = len(json.dumps(
                    {
                        "items": [], "total": row_count, "skip": last_offset,
                        "limit": 1000, "has_more": False,
                    },
                    separators=(",", ":"),
                ).encode("utf-8"))
                stats["api_json_bytes"] = (
                    sum_values + row_overhead * row_count
                    + max(0, row_count - 1) + envelope_bytes - 2
                )
            result[table] = stats
    return result
