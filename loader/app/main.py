from __future__ import annotations

import json
import os
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Literal, Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from . import db
from .access_log import install_health_access_log_filter
from .flatten import inspect_payload

LANDING_DIR = Path(os.environ.get("LANDING_DIR", "/landing"))


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncGenerator[None, None]:
    install_health_access_log_filter()
    yield


app = FastAPI(title="dw-dev loader", lifespan=lifespan)


class LoadRequest(BaseModel):
    filename: str
    source: Optional[str] = None
    source_url: Optional[str] = None
    load_mode: Literal["full_snapshot", "incremental_upsert"] = "full_snapshot"
    delete_policy: Literal["close_on_full_snapshot", "never_close"] | None = None
    run_id: Optional[str] = None
    checkpoint_before: Optional[str] = None
    checkpoint_after: Optional[str] = None
    pagination_complete: bool = True


@app.get("/health")
def health() -> dict:
    ok = db.health_check()
    if not ok:
        raise HTTPException(status_code=503, detail="staging database unreachable")
    return {"status": "ok"}


@app.post("/load")
def load(req: LoadRequest) -> dict:
    """Validate the extraction manifest before streaming its payload into staging."""
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
        payload_info = inspect_payload(file_path)
    except (UnicodeDecodeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=f"invalid JSON in {req.filename}: {exc}") from exc

    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise HTTPException(status_code=400, detail=f"invalid JSON in {manifest_filename}: {exc}") from exc

    if manifest.get("payload_filename") != req.filename:
        raise HTTPException(status_code=400, detail="manifest payload filename does not match request")
    if req.source and manifest.get("source") != req.source:
        raise HTTPException(status_code=409, detail="source does not match extraction manifest")
    if req.run_id and manifest.get("run_id") != req.run_id:
        raise HTTPException(status_code=409, detail="run ID does not match extraction manifest")
    if manifest.get("load_mode", "full_snapshot") != req.load_mode:
        raise HTTPException(status_code=409, detail="load mode does not match extraction manifest")
    delete_policy = req.delete_policy or manifest.get("delete_policy", "close_on_full_snapshot")
    if delete_policy not in {"close_on_full_snapshot", "never_close"}:
        raise HTTPException(status_code=400, detail="manifest contains an unsupported missing-key policy")
    if manifest.get("delete_policy", "close_on_full_snapshot") != delete_policy:
        raise HTTPException(status_code=409, detail="delete policy does not match extraction manifest")
    for field in ("checkpoint_before", "checkpoint_after", "pagination_complete"):
        requested = getattr(req, field)
        manifested = manifest.get(field, True if field == "pagination_complete" else None)
        if requested != manifested:
            raise HTTPException(status_code=409, detail=f"{field} does not match extraction manifest")
    expected_sha256 = manifest.get("sha256")
    # Reject altered or mismatched landing files before any staging writes occur.
    if expected_sha256 != payload_info.sha256:
        raise HTTPException(status_code=409, detail="landing payload checksum does not match manifest")
    expected_size = manifest.get("byte_size")
    if expected_size is not None and expected_size != payload_info.byte_size:
        raise HTTPException(status_code=409, detail="landing payload size does not match manifest")

    source = req.source or manifest.get("source") or req.filename.split("_")[0]
    source_url = req.source_url or manifest.get("requested_url")

    try:
        result = db.load_payload(
            req.filename,
            source,
            source_url,
            file_path,
            payload_info.sha256,
            payload_info.byte_size,
            payload_info.root_type,
            payload_info.collection_key,
            req.load_mode,
            delete_policy,
            req.run_id or manifest.get("run_id"),
            req.checkpoint_before,
            req.checkpoint_after,
            req.pagination_complete,
        )
    except Exception as exc:  # noqa: BLE001 - surface to orchestrator as a load failure
        raise HTTPException(status_code=500, detail=f"load failed: {exc}") from exc

    return {**result, "manifest_filename": manifest_filename}
