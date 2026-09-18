from __future__ import annotations

import functools
import json
import os
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from . import db
from .flatten import flatten_payload

LANDING_DIR = Path(os.environ.get("LANDING_DIR", "/landing"))

app = FastAPI(title="dw-dev loader")


class LoadRequest(BaseModel):
    filename: str
    source: Optional[str] = None
    source_url: Optional[str] = None


@app.get("/health")
def health() -> dict:
    ok = db.health_check()
    if not ok:
        raise HTTPException(status_code=503, detail="staging database unreachable")
    return {"status": "ok"}


@app.post("/load")
def load(req: LoadRequest) -> dict:
    file_path = LANDING_DIR / req.filename
    if not file_path.is_file():
        raise HTTPException(status_code=404, detail=f"landing file not found: {req.filename}")

    try:
        raw_text = file_path.read_text(encoding="utf-8")
        payload = json.loads(raw_text)
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=400, detail=f"invalid JSON in {req.filename}: {exc}") from exc

    source = req.source or req.filename.split("_")[0]

    try:
        flatten_fn = functools.partial(flatten_payload, source, payload)
        result = db.load_payload(req.filename, source, req.source_url, payload, flatten_fn)
    except Exception as exc:  # noqa: BLE001 - surface to orchestrator as a load failure
        raise HTTPException(status_code=500, detail=f"load failed: {exc}") from exc

    return result
