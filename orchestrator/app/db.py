from __future__ import annotations

import json
import os
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Generator, Optional

DB_PATH = os.environ.get("ORCH_SQLITE_PATH", "/data/orchestrator.db")

_lock = threading.Lock()


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@contextmanager
def get_db() -> Generator[sqlite3.Connection, None, None]:
    os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_db() -> None:
    with _lock, get_db() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sources (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                url TEXT NOT NULL,
                cron TEXT NOT NULL,
                enabled INTEGER NOT NULL DEFAULT 1,
                next_run TEXT,
                load_mode TEXT NOT NULL DEFAULT 'full_snapshot',
                delete_policy TEXT NOT NULL DEFAULT 'close_on_full_snapshot',
                key_fields TEXT NOT NULL DEFAULT '[]',
                incremental_param TEXT NOT NULL DEFAULT 'updated_since',
                watermark_field TEXT NOT NULL DEFAULT 'updated_at',
                checkpoint TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source TEXT NOT NULL,
                started TEXT NOT NULL,
                finished TEXT,
                status TEXT NOT NULL,
                step TEXT,
                error TEXT,
                filename TEXT,
                batch_id TEXT,
                checkpoint_before TEXT,
                checkpoint_after TEXT,
                load_mode TEXT
            )
            """
        )
        _ensure_columns(conn, "sources", {
            "load_mode": "TEXT NOT NULL DEFAULT 'full_snapshot'",
            "delete_policy": "TEXT NOT NULL DEFAULT 'close_on_full_snapshot'",
            "key_fields": "TEXT NOT NULL DEFAULT '[]'",
            "incremental_param": "TEXT NOT NULL DEFAULT 'updated_since'",
            "watermark_field": "TEXT NOT NULL DEFAULT 'updated_at'",
            "checkpoint": "TEXT",
        })
        _ensure_columns(conn, "runs", {
            "checkpoint_before": "TEXT",
            "checkpoint_after": "TEXT",
            "load_mode": "TEXT",
        })
        conn.execute(
            """
            INSERT OR IGNORE INTO sources (name, url, cron, enabled, next_run)
            VALUES (?, ?, ?, 1, NULL)
            """,
            ("demo-products", "local://demo", "*/2 * * * *"),
        )


# ---------------------------------------------------------------------
# Sources
# ---------------------------------------------------------------------
def _ensure_columns(conn: sqlite3.Connection, table: str, columns: dict[str, str]) -> None:
    existing = {row["name"] for row in conn.execute(f"PRAGMA table_info({table})")}
    for name, definition in columns.items():
        if name not in existing:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {name} {definition}")


def list_sources() -> list[dict]:
    with get_db() as conn:
        rows = conn.execute(
            """
            SELECT s.*,
                (SELECT finished FROM runs r
                 WHERE r.source = s.name AND r.status = 'done'
                 ORDER BY r.id DESC LIMIT 1) AS last_successful_run,
                failure.error AS last_error,
                failure.step AS last_error_step,
                failure.id AS last_error_run_id,
                failure.started AS last_error_started
            FROM sources s
            LEFT JOIN runs failure ON failure.id = (
                SELECT r.id FROM runs r
                WHERE r.source = s.name AND r.status = 'failed'
                  AND r.id > COALESCE(
                      (SELECT MAX(success.id) FROM runs success
                       WHERE success.source = s.name AND success.status = 'done'),
                      0
                  )
                ORDER BY r.id DESC LIMIT 1
            )
            ORDER BY s.id
            """
        ).fetchall()
        result = []
        for row in rows:
            source = dict(row)
            source["key_fields"] = json.loads(source["key_fields"])
            result.append(source)
        return result


def get_source(source_id: int) -> Optional[dict]:
    with get_db() as conn:
        row = conn.execute("SELECT * FROM sources WHERE id = ?", (source_id,)).fetchone()
        return _source_dict(row) if row else None


def get_source_by_name(name: str) -> Optional[dict]:
    with get_db() as conn:
        row = conn.execute("SELECT * FROM sources WHERE name = ?", (name,)).fetchone()
        return _source_dict(row) if row else None


def _source_dict(row: Optional[sqlite3.Row]) -> Optional[dict]:
    if row is None:
        return None
    source = dict(row)
    source["key_fields"] = json.loads(source["key_fields"])
    return source


def create_source(
    name: str, url: str, cron: str, enabled: bool, load_mode: str, delete_policy: str,
    key_fields: list[str], incremental_param: str, watermark_field: str,
) -> dict:
    with get_db() as conn:
        cur = conn.execute(
            """
            INSERT INTO sources
                (name, url, cron, enabled, next_run, load_mode, key_fields,
                 incremental_param, watermark_field, delete_policy)
            VALUES (?, ?, ?, ?, NULL, ?, ?, ?, ?, ?)
            """,
            (name, url, cron, 1 if enabled else 0, load_mode, json.dumps(key_fields),
             incremental_param, watermark_field, delete_policy),
        )
        new_id = cur.lastrowid
        row = conn.execute("SELECT * FROM sources WHERE id = ?", (new_id,)).fetchone()
        return _source_dict(row)


def update_source_next_run(source_id: int, next_run_iso: Optional[str]) -> None:
    with get_db() as conn:
        conn.execute(
            "UPDATE sources SET next_run = ? WHERE id = ?",
            (next_run_iso, source_id)
        )


def update_source(
    source_id: int, name: str, url: str, cron: str, enabled: bool,
    load_mode: str, delete_policy: str, key_fields: list[str], incremental_param: str,
    watermark_field: str,
) -> bool:
    with get_db() as conn:
        cur = conn.execute(
            """
            UPDATE sources
            SET name = ?, url = ?, cron = ?, enabled = ?, load_mode = ?, delete_policy = ?,
                key_fields = ?, incremental_param = ?, watermark_field = ?,
                checkpoint = CASE
                    WHEN url <> ? OR load_mode <> ? OR incremental_param <> ?
                         OR watermark_field <> ? THEN NULL
                    ELSE checkpoint
                END
            WHERE id = ?
            """,
            (name, url, cron, 1 if enabled else 0, load_mode, delete_policy, json.dumps(key_fields),
             incremental_param, watermark_field, url, load_mode, incremental_param,
             watermark_field, source_id),
        )
        return cur.rowcount > 0


def update_source_checkpoint(source_name: str, checkpoint: Optional[str]) -> None:
    with get_db() as conn:
        conn.execute("UPDATE sources SET checkpoint = ? WHERE name = ?", (checkpoint, source_name))


def delete_source(source_id: int) -> bool:
    with get_db() as conn:
        cur = conn.execute("DELETE FROM sources WHERE id = ?", (source_id,))
        return cur.rowcount > 0


# ---------------------------------------------------------------------
# Runs
# ---------------------------------------------------------------------
def create_run(source: str) -> int:
    with get_db() as conn:
        cur = conn.execute(
            "INSERT INTO runs (source, started, status, step) VALUES (?, ?, 'queued', 'queued')",
            (source, now_iso()),
        )
        if cur.lastrowid is None:
            raise RuntimeError("Failed to obtain the new run ID")
        return cur.lastrowid


def update_run(run_id: int, **fields: Any) -> None:
    if not fields:
        return
    allowed = {
        "status", "step", "error", "filename", "batch_id", "finished",
        "checkpoint_before", "checkpoint_after", "load_mode",
    }
    unknown = fields.keys() - allowed
    if unknown:
        raise ValueError(f"unsupported run field(s): {', '.join(sorted(unknown))}")
    cols = ", ".join(f"{k} = ?" for k in fields)
    values = list(fields.values()) + [run_id]
    with get_db() as conn:
        conn.execute(f"UPDATE runs SET {cols} WHERE id = ?", values)  # noqa: S608 - fixed column set from this module only


def list_runs(limit: int = 50) -> list[dict]:
    with get_db() as conn:
        rows = conn.execute("SELECT * FROM runs ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
        return [dict(r) for r in rows]


def get_run(run_id: int) -> Optional[dict]:
    with get_db() as conn:
        row = conn.execute("SELECT * FROM runs WHERE id = ?", (run_id,)).fetchone()
        return dict(row) if row else None
