from __future__ import annotations

from pathlib import Path

from fastapi import Depends, FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

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


@app.get("/api/sources")
def api_list_sources(_: str = Depends(require_auth)) -> list[dict]:
    return db.list_sources()


@app.post("/api/sources")
def api_create_source(body: SourceCreate, _: str = Depends(require_auth)) -> dict:
    if db.get_source_by_name(body.name):
        raise HTTPException(status_code=409, detail=f"source '{body.name}' already exists")
    return db.create_source(body.name, body.url, body.cron, body.enabled)


@app.delete("/api/sources/{source_id}")
def api_delete_source(source_id: int, _: str = Depends(require_auth)) -> dict:
    if not db.delete_source(source_id):
        raise HTTPException(status_code=404, detail="source not found")
    return {"status": "deleted", "id": source_id}


@app.put("/api/sources/{source_id}")
def api_update_source(source_id: int, body: SourceCreate, _: str = Depends(require_auth)) -> dict:
    if not db.update_source_active(source_id, body.enabled):
        raise HTTPException(status_code=404, detail="source not found")
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
    return (STATIC_DIR / "index.html").read_text(encoding="utf-8")
