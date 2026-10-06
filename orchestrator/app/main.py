from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Literal
from urllib.parse import urlsplit

from fastapi import Depends, FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

from . import db, pipeline, runner, scheduler
from .auth import require_auth

app = FastAPI(title="dw-dev orchestrator")

STATIC_DIR = Path(__file__).parent / "static"


@app.on_event("startup")
async def on_startup() -> None:
    db.init_db()
    scheduler.start()


@app.on_event("shutdown")
async def on_shutdown() -> None:
    scheduler.stop()


# ---------------------------------------------------------------------
# Health (unauthenticated, for container healthchecks / other services)
# ---------------------------------------------------------------------
@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.get("/api/health")
def api_health(_: str = Depends(require_auth)) -> dict:
    return pipeline.full_health()


# ---------------------------------------------------------------------
# Sources
# ---------------------------------------------------------------------
class SourceCreate(BaseModel):
    name: str
    url: str
    cron: str
    enabled: bool = True
    load_mode: Literal["full_snapshot", "incremental_upsert"] = "full_snapshot"
    delete_policy: Literal["close_on_full_snapshot", "never_close"] | None = None
    key_fields: list[str] = Field(default_factory=list)
    incremental_param: str = "updated_since"
    watermark_field: str = "updated_at"


def _effective_delete_policy(body: SourceCreate) -> str:
    if body.delete_policy:
        return body.delete_policy
    return "close_on_full_snapshot"


def _validate_source_settings(body: SourceCreate) -> None:
    try:
        parsed_url = urlsplit(body.url.strip())
    except ValueError as exc:
        raise HTTPException(status_code=422, detail="url must be an absolute HTTP(S) URL or local://demo") from exc
    is_demo_url = parsed_url.scheme.lower() == "local" and parsed_url.netloc.lower() == "demo"
    if not is_demo_url and (
        parsed_url.scheme.lower() not in {"http", "https"} or not parsed_url.hostname
    ):
        raise HTTPException(status_code=422, detail="url must be an absolute HTTP(S) URL or local://demo")
    if body.load_mode == "incremental_upsert" and (
        not body.incremental_param.strip() or not body.watermark_field.strip()
    ):
        raise HTTPException(
            status_code=422,
            detail="incremental_upsert requires an incremental parameter and watermark field",
        )
    if any(not key.strip() for key in body.key_fields):
        raise HTTPException(status_code=422, detail="key_fields cannot contain empty values")


@app.get("/api/sources")
def api_list_sources(_: str = Depends(require_auth)) -> list[dict]:
    return db.list_sources()


@app.post("/api/sources")
def api_create_source(body: SourceCreate, _: str = Depends(require_auth)) -> dict:
    _validate_source_settings(body)
    if db.get_source_by_name(body.name):
        raise HTTPException(status_code=409, detail=f"source '{body.name}' already exists")
    return db.create_source(
        body.name, body.url, body.cron, body.enabled, body.load_mode,
        _effective_delete_policy(body),
        body.key_fields, body.incremental_param, body.watermark_field,
    )


@app.delete("/api/sources/{source_id}")
def api_delete_source(source_id: int, _: str = Depends(require_auth)) -> dict:
    if not db.delete_source(source_id):
        raise HTTPException(status_code=404, detail="source not found")
    return {"status": "deleted", "id": source_id}


@app.put("/api/sources/{source_id}")
def api_update_source(source_id: int, body: SourceCreate, _: str = Depends(require_auth)) -> dict:
    _validate_source_settings(body)
    if not db.update_source(
        source_id, body.name, body.url, body.cron, body.enabled, body.load_mode,
        _effective_delete_policy(body),
        body.key_fields, body.incremental_param, body.watermark_field,
    ):
        raise HTTPException(status_code=404, detail="update failed")
    return {"status": "updated", "id": source_id}


@app.post("/api/sources/{source_id}/run")
async def api_run_source(source_id: int, _: str = Depends(require_auth)) -> dict:
    source = db.get_source(source_id)
    if not source:
        raise HTTPException(status_code=404, detail="source not found")
    try:
        run_id = await runner.trigger_run(source)
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return {"status": "started", "run_id": run_id, "source": source["name"]}


@app.post("/api/sources/{source_id}/full-reload")
async def api_full_reload(source_id: int, _: str = Depends(require_auth)) -> dict:
    source = db.get_source(source_id)
    if not source:
        raise HTTPException(status_code=404, detail="source not found")
    try:
        run_id = await runner.trigger_run(source, force_full=True)
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return {"status": "started", "run_id": run_id, "source": source["name"], "load_mode": "full_snapshot"}


# ---------------------------------------------------------------------
# Runs
# ---------------------------------------------------------------------
@app.get("/api/runs")
def api_list_runs(limit: int = 50, _: str = Depends(require_auth)) -> list[dict]:
    return db.list_runs(limit=limit)


@app.get("/api/runs/{run_id}")
def api_get_run(run_id: int, _: str = Depends(require_auth)) -> dict:
    run = db.get_run(run_id)
    if not run:
        raise HTTPException(status_code=404, detail="run not found")
    return run


# ---------------------------------------------------------------------
# GUI (plain HTML/JS, no build step, protected by the same Basic Auth)
# ---------------------------------------------------------------------
@app.get("/", response_class=HTMLResponse)
def gui(_: str = Depends(require_auth)) -> str:
    html = (STATIC_DIR / "index.html").read_text(encoding="utf-8")
    return html.replace("__APP_TIME_ZONE__", json.dumps(os.environ.get("TZ", "UTC")))
