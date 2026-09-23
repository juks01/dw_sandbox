from __future__ import annotations

import functools
import hashlib
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
    req_filename = Path(req.filename)

    if req_filename.name != req.filename:
        raise HTTPException(status_code=400, detail="invalid landing filename")

    file_path = (LANDING_DIR / req_filename).resolve()
    landing_root = LANDING_DIR.resolve()

    if landing_root not in file_path.parents:
        raise HTTPException(status_code=400, detail="invalid landing path")

    if not file_path.is_file():
        raise HTTPException(status_code=404, detail=f"landing file not found: {req.filename}")

    manifest_filename = f"{req_filename.stem}.manifest.json"
    manifest_path = (LANDING_DIR / manifest_filename).resolve()
    if landing_root not in manifest_path.parents:
        raise HTTPException(status_code=400, detail="invalid landing manifest path")
    if not manifest_path.is_file():
        raise HTTPException(status_code=409, detail=f"landing manifest not found: {manifest_filename}")

    try:
        raw_text = file_path.read_text(encoding="utf-8")
        payload = json.loads(raw_text)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise HTTPException(status_code=400, detail=f"invalid JSON in {req.filename}: {exc}") from exc

    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise HTTPException(status_code=400, detail=f"invalid JSON in {manifest_filename}: {exc}") from exc

    if manifest.get("payload_filename") != req.filename:
        raise HTTPException(status_code=400, detail="manifest payload filename does not match request")
    expected_sha256 = manifest.get("sha256")
    actual_sha256 = hashlib.sha256(file_path.read_bytes()).hexdigest()
    if expected_sha256 != actual_sha256:
        raise HTTPException(status_code=409, detail="landing payload checksum does not match manifest")

    source = req.source or manifest.get("source") or req.filename.split("_")[0]
    source_url = req.source_url or manifest.get("requested_url")

    try:
        flatten_fn = functools.partial(flatten_payload, source, payload)
        result = db.load_payload(req.filename, source, source_url, payload, flatten_fn)
    except Exception as exc:  # noqa: BLE001 - surface to orchestrator as a load failure
        raise HTTPException(status_code=500, detail=f"load failed: {exc}") from exc

    return {**result, "manifest_filename": manifest_filename}
