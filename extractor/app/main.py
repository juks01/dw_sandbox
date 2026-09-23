"""Extractor service.

Responsible for fetching configured sources and writing the raw response into
the landing zone. It has no database dependency.
"""
from __future__ import annotations

import json
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlparse

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

    LANDING_DIR.mkdir(parents=True, exist_ok=True)
    dest = LANDING_DIR / filename

    # Validate it's actually JSON before writing (payload must stay as
    # unmodified as possible, but we do want to fail fast on garbage).
    try:
        json.loads(raw_bytes)
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=502, detail=f"upstream response is not valid JSON: {exc}") from exc

    dest.write_bytes(raw_bytes)

    return ExtractResponse(filename=filename, source=req.source, source_url=req.url)


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
