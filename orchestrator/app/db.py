from __future__ import annotations

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
                next_run TEXT
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
                batch_id TEXT
            )
            """
        )
        # Seed one deterministic, fully offline demo source so the whole
        # pipeline can be exercised immediately after startup (spec 18).
        row = conn.execute("SELECT COUNT(*) AS c FROM sources").fetchone()
        if row["c"] == 0:
            conn.execute(
                "INSERT INTO sources (name, url, cron, enabled, next_run) VALUES (?, ?, ?, 1, NULL)",
                ("demo_source", "local://demo", "*/2 * * * *"),
            )


# ---------------------------------------------------------------------
# Sources
# ---------------------------------------------------------------------
def list_sources() -> list[dict]:
    with get_db() as conn:
        rows = conn.execute("SELECT * FROM sources ORDER BY id").fetchall()
        return [dict(r) for r in rows]


def get_source(source_id: int) -> Optional[dict]:
    with get_db() as conn:
        row = conn.execute("SELECT * FROM sources WHERE id = ?", (source_id,)).fetchone()
        return dict(row) if row else None


def get_source_by_name(name: str) -> Optional[dict]:
    with get_db() as conn:
        row = conn.execute("SELECT * FROM sources WHERE name = ?", (name,)).fetchone()
        return dict(row) if row else None


def create_source(name: str, url: str, cron: str, enabled: bool = True) -> dict:
    with get_db() as conn:
        cur = conn.execute(
            "INSERT INTO sources (name, url, cron, enabled, next_run) VALUES (?, ?, ?, ?, NULL)",
            (name, url, cron, 1 if enabled else 0)
        )
        new_id = cur.lastrowid
        row = conn.execute("SELECT * FROM sources WHERE id = ?", (new_id,)).fetchone()
        return dict(row)


def update_source_next_run(source_id: int, next_run_iso: Optional[str]) -> None:
    with get_db() as conn:
        conn.execute(
            "UPDATE sources SET next_run = ? WHERE id = ?",
            (next_run_iso, source_id)
        )


def update_source(source_id: int, name: str, url: str, cron: str, enabled: bool = True) -> bool:
    with get_db() as conn:
        cur = conn.execute(
            "UPDATE sources SET (name, url, cron, enabled) = (?, ?, ?, ?) WHERE id = ?",
            (name, url, cron, 1 if enabled else 0, source_id)
        )
        return cur.rowcount > 0


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
        print("DEBUG: Created run with ID:", cur.lastrowid)
        if cur.lastrowid is None:
            raise RuntimeError("Failed to obtain the new run ID")
        return cur.lastrowid


def update_run(run_id: int, **fields: Any) -> None:
    if not fields:
        return
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
