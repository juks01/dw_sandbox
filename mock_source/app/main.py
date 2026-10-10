from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any

from fastapi import FastAPI, HTTPException, Query

from . import db


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncGenerator[None, None]:
    with db.connect() as conn:
        db.ensure_schema(conn)
    yield


app = FastAPI(title="dw-dev standalone mock source", lifespan=lifespan)


@app.get("/health")
def health() -> dict[str, str]:
    try:
        with db.connect() as conn:
            conn.execute("SELECT 1")
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"mock source database unavailable: {exc}") from exc
    return {"status": "ok"}


@app.get("/api/{table}")
def get_records(
    table: str,
    skip: int = Query(default=0, ge=0),
    limit: int = Query(default=1000, ge=1, le=5000),
    updated_since: str | None = None,
) -> dict[str, Any]:
    if table not in db.TABLES:
        raise HTTPException(status_code=404, detail="unknown mock table")
    timestamp = None
    if updated_since:
        try:
            timestamp = datetime.fromisoformat(updated_since.replace("Z", "+00:00"))
        except ValueError as exc:
            raise HTTPException(status_code=422, detail="updated_since must be an ISO-8601 timestamp") from exc
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=timezone.utc)
    records, total = db.list_page(table, limit, skip, timestamp)
    return {
        "items": records,
        "total": total,
        "skip": skip,
        "limit": limit,
        "has_more": skip + len(records) < total,
    }
