"""Extractor service.

Responsible for fetching configured sources and writing the raw response into
the landing zone. It has no database dependency.
"""
from __future__ import annotations

import json
import os
import hashlib
import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from .config import is_allowed_url

LANDING_DIR = Path(os.environ.get("LANDING_DIR", "/landing"))
USER_AGENT = os.environ.get("EXTRACTOR_USER_AGENT", "dw-dev-extractor/1.0")

app = FastAPI(title="dw-dev extractor")


class ExtractRequest(BaseModel):
    source: str
    url: str
    run_id: Optional[str] = None


class ExtractResponse(BaseModel):
    filename: str
    manifest_filename: str
    source: str
    source_url: str


def _safe_source_slug(source: str) -> str:
    """Normalize a source name so it is always safe to embed in a filename."""
    slug = "".join(c if c.isalnum() or c == "-" else "_" for c in source.strip().lower())
    slug = slug.strip("_") or "source"
    return slug[:64]


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/extract", response_model=ExtractResponse)
def extract(req: ExtractRequest) -> ExtractResponse:
    if not req.source.strip():
        raise HTTPException(status_code=400, detail="source is required")

    if not req.url.strip():
        raise HTTPException(status_code=400, detail="url is required")

    if not is_allowed_url(req.url):
        raise HTTPException(status_code=403, detail="url host is not allowed by extractor allowlist")

    try:
        resp = httpx.get(
            req.url,
            headers={"User-Agent": USER_AGENT},
            timeout=30.0,
            follow_redirects=True,
        )
        resp.raise_for_status()
    except httpx.UnsupportedProtocol as exc:
        raise HTTPException(status_code=400, detail=f"URL scheme is not supported by extractor transport: {exc}") from exc
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=502, detail=f"upstream request failed: {exc}") from exc
    raw_bytes = resp.content

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%f")[:-3] + "Z"
    file_uuid = str(uuid.uuid4())
    slug = _safe_source_slug(req.source)
    filename = f"{slug}_{ts}_{file_uuid}.json"
    manifest_filename = f"{slug}_{ts}_{file_uuid}.manifest.json"

    LANDING_DIR.mkdir(parents=True, exist_ok=True)
    dest = LANDING_DIR / filename
    manifest_dest = LANDING_DIR / manifest_filename

    # Validate it's actually JSON before writing (payload must stay as
    # unmodified as possible, but we do want to fail fast on garbage).
    try:
        json.loads(raw_bytes)
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=502, detail=f"upstream response is not valid JSON: {exc}") from exc

    fetched_at = datetime.now(timezone.utc).isoformat()
    manifest = {
        "manifest_version": 1,
        "status": "extracted",
        "source": req.source,
        "run_id": req.run_id,
        "requested_url": req.url,
        "final_url": str(resp.url),
        "fetched_at": fetched_at,
        "http_status": resp.status_code,
        "content_type": resp.headers.get("content-type"),
        "byte_size": len(raw_bytes),
        "sha256": hashlib.sha256(raw_bytes).hexdigest(),
        "payload_filename": filename,
    }

    _write_atomically(dest, raw_bytes)
    _write_atomically(
        manifest_dest,
        json.dumps(manifest, ensure_ascii=True, indent=2).encode("utf-8") + b"\n",
    )

    return ExtractResponse(
        filename=filename,
        manifest_filename=manifest_filename,
        source=req.source,
        source_url=req.url,
    )


def _write_atomically(destination: Path, content: bytes) -> None:
    """Make a completed file visible only after all bytes have been written."""
    with tempfile.NamedTemporaryFile(dir=destination.parent, prefix=f".{destination.name}.", delete=False) as tmp:
        temporary_path = Path(tmp.name)
        tmp.write(content)
        tmp.flush()
        os.fsync(tmp.fileno())
    try:
        os.replace(temporary_path, destination)
    except Exception:
        temporary_path.unlink(missing_ok=True)
        raise


def _fetch_http_source(url: str) -> bytes:
    try:
        resp = httpx.get(
            url,
            headers={"User-Agent": USER_AGENT},
            timeout=30.0,
            follow_redirects=True,
        )
        resp.raise_for_status()
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=502, detail=f"upstream request failed: {exc}") from exc
    return resp.content
