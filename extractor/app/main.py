"""Extractor service.

Responsible for fetching configured sources and writing the raw response into
the landing zone. It has no database dependency.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
import uuid
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from .access_log import install_health_access_log_filter
from .config import is_allowed_url

LANDING_DIR = Path(os.environ.get("LANDING_DIR", "/landing"))
DEMO_PAYLOAD_PATH = Path(__file__).resolve().parent.parent / "conf" / "demo.json"
USER_AGENT = os.environ.get("EXTRACTOR_USER_AGENT", "dw-dev-extractor/1.0")
MAX_PAGINATION_PAGES = 1000


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncGenerator[None, None]:
    install_health_access_log_filter()
    yield


app = FastAPI(title="dw-dev extractor", lifespan=lifespan)


class ExtractRequest(BaseModel):
    source: str
    url: str
    run_id: Optional[str] = None
    load_mode: str = "full_snapshot"
    delete_policy: Optional[str] = None
    checkpoint_before: Optional[str] = None
    incremental_param: str = "updated_since"
    watermark_field: str = "updated_at"
    watermark_required: bool = False


class ExtractResponse(BaseModel):
    filename: str
    manifest_filename: str
    source: str
    source_url: str
    checkpoint_after: Optional[str] = None
    pagination_complete: bool


def _iter_field_values(value: Any, field: str):
    if isinstance(value, dict):
        for key, child in value.items():
            if key == field:
                yield child
            yield from _iter_field_values(child, field)
    elif isinstance(value, list):
        for child in value:
            yield from _iter_field_values(child, field)


def _checkpoint_after(
    payload: Any, field: str, checkpoint_before: Optional[str] = None,
) -> Optional[str]:
    values = list(_iter_field_values(payload, field))
    if not values:
        return None

    candidates: list[datetime] = []
    if checkpoint_before:
        try:
            previous = datetime.fromisoformat(checkpoint_before.replace("Z", "+00:00"))
        except ValueError:
            raise ValueError("saved checkpoint must be an ISO-8601 timestamp") from None
        if previous.tzinfo is None:
            previous = previous.replace(tzinfo=timezone.utc)
        candidates.append(previous.astimezone(timezone.utc))
    for value in values:
        if not isinstance(value, str):
            raise ValueError(f"watermark field {field!r} must contain ISO-8601 timestamps")
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            raise ValueError(f"watermark field {field!r} must contain ISO-8601 timestamps") from None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        candidates.append(parsed.astimezone(timezone.utc))
    return max(candidates).isoformat() if candidates else None


def _pagination_complete(payload: Any) -> bool:
    def inspect(value: Any) -> bool:
        if isinstance(value, dict):
            if (
                value.get("has_more") is True or value.get("next")
                or value.get("next_cursor") or value.get("nextCursor")
            ):
                return False
            total = value.get("total")
            skip = value.get("skip", value.get("offset", 0))
            limit = value.get("limit")
            if isinstance(total, int) and isinstance(skip, int) and isinstance(limit, int):
                if total > skip + limit:
                    return False
                list_lengths = [len(child) for child in value.values() if isinstance(child, list)]
                if list_lengths and max(list_lengths) + skip < total:
                    return False
            return all(inspect(child) for child in value.values())
        if isinstance(value, list):
            return all(inspect(child) for child in value)
        return True

    return inspect(payload)


def _page_collection(payload: Any) -> tuple[Optional[str], Optional[list]]:
    if isinstance(payload, list):
        return None, payload
    if not isinstance(payload, dict):
        return None, None

    common_names = ("results", "items", "records", "products", "data")
    for name in common_names:
        if isinstance(payload.get(name), list):
            return name, payload[name]
    lists = [(name, value) for name, value in payload.items() if isinstance(value, list)]
    return lists[0] if len(lists) == 1 else (None, None)


def _next_page_url(payload: Any, response: httpx.Response) -> Optional[str]:
    if isinstance(payload, dict):
        next_url = payload.get("next")
        links = payload.get("links")
        if not next_url and isinstance(links, dict):
            next_url = links.get("next")
        if isinstance(next_url, str) and next_url.strip():
            return str(response.url.join(next_url.strip()))

    link_header = response.headers.get("link", "")
    match = re.search(
        r'<([^>]+)>\s*;\s*rel\s*=\s*"?next"?\s*(?:;|,|$)',
        link_header,
        re.IGNORECASE,
    )
    if match:
        return str(response.url.join(match.group(1)))

    if isinstance(payload, dict):
        total = payload.get("total")
        offset_key = "skip" if isinstance(payload.get("skip"), int) else "offset"
        offset = payload.get(offset_key, 0)
        limit = payload.get("limit")
        _, items = _page_collection(payload)
        if (
            isinstance(total, int) and isinstance(offset, int) and offset >= 0
            and isinstance(limit, int) and limit > 0 and items is not None
            and total > offset + len(items)
        ):
            if not items:
                raise HTTPException(status_code=502, detail="pagination made no progress")
            next_offset = offset + len(items)
            return str(response.url.copy_merge_params({offset_key: str(next_offset), "limit": str(limit)}))

        if payload.get("has_more") is True:
            page = payload.get("page")
            if not isinstance(page, int):
                try:
                    page = int(response.url.params.get("page", "1"))
                except ValueError:
                    raise HTTPException(status_code=502, detail="invalid page number in pagination response") from None
            if not items:
                raise HTTPException(status_code=502, detail="pagination made no progress")
            return str(response.url.copy_merge_params({"page": str(page + 1)}))

    return None


def _merge_page_payload(aggregate: Any, page: Any) -> Any:
    aggregate_key, aggregate_items = _page_collection(aggregate)
    page_key, page_items = _page_collection(page)
    if aggregate_items is None or page_items is None or aggregate_key != page_key:
        raise HTTPException(
            status_code=502,
            detail="cannot combine paginated responses with different result shapes",
        )
    aggregate_items.extend(page_items)
    if isinstance(aggregate, dict) and isinstance(page, dict):
        for key, value in page.items():
            if key != aggregate_key:
                aggregate[key] = value
    return aggregate


def _get_http_page(url: str) -> httpx.Response:
    current_url = url
    try:
        for _ in range(11):
            # Revalidate every redirect target so an allowed API cannot redirect outside the allowlist.
            if not is_allowed_url(current_url):
                raise HTTPException(status_code=403, detail="redirect or pagination host is not allowed")
            response = httpx.get(
                current_url,
                headers={"User-Agent": USER_AGENT},
                timeout=30.0,
                follow_redirects=False,
            )
            if not response.is_redirect:
                response.raise_for_status()
                return response
            location = response.headers.get("location")
            if not location:
                raise HTTPException(status_code=502, detail="upstream redirect has no location")
            current_url = str(response.url.join(location))
        raise HTTPException(status_code=502, detail="too many upstream redirects")
    except httpx.UnsupportedProtocol as exc:
        raise HTTPException(status_code=400, detail=f"URL scheme is not supported by extractor transport: {exc}") from exc
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=502, detail=f"upstream request failed: {exc}") from exc


def _empty_payload(payload: Any) -> bool:
    if payload is None or payload == [] or payload == {}:
        return True
    if isinstance(payload, dict):
        lists = [value for value in payload.values() if isinstance(value, list)]
        return bool(lists) and all(not value for value in lists)
    return False


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
    """Fetch every supported page and atomically publish payload plus manifest."""
    if not req.source.strip():
        raise HTTPException(status_code=400, detail="source is required")

    if not req.url.strip():
        raise HTTPException(status_code=400, detail="url is required")
    if req.load_mode not in {"full_snapshot", "incremental_upsert"}:
        raise HTTPException(status_code=400, detail="unsupported load mode")
    delete_policy = req.delete_policy or "close_on_full_snapshot"
    if delete_policy not in {"close_on_full_snapshot", "never_close"}:
        raise HTTPException(status_code=400, detail="unsupported missing-key policy")
    uses_watermark = bool(req.watermark_field.strip())
    if req.load_mode == "incremental_upsert" and uses_watermark and (
        not req.checkpoint_before or not req.incremental_param.strip()
    ):
        raise HTTPException(
            status_code=400,
            detail="timestamp-based incremental extraction requires a checkpoint and query parameter",
        )

    if not is_allowed_url(req.url):
        raise HTTPException(status_code=403, detail="url host is not allowed by extractor allowlist")

    if req.url.strip().lower() == "local://demo":
        try:
            raw_bytes = DEMO_PAYLOAD_PATH.read_bytes()
        except OSError as exc:
            raise HTTPException(status_code=500, detail="offline demo payload is unavailable") from exc
        final_url = req.url
        http_status = 200
        content_type = "application/json"
        page_count = 1
    else:
        current_url = req.url
        if req.load_mode == "incremental_upsert" and uses_watermark:
            current_url = str(
                httpx.URL(current_url).copy_merge_params(
                    {req.incremental_param: req.checkpoint_before}
                )
            )
        seen_urls: set[str] = set()
        aggregate = None
        page_count = 0
        while True:
            if current_url in seen_urls:
                raise HTTPException(status_code=502, detail="pagination returned a repeated URL")
            seen_urls.add(current_url)
            resp = _get_http_page(current_url)
            try:
                page_payload = resp.json()
            except ValueError as exc:
                raise HTTPException(status_code=502, detail="upstream response is not valid JSON") from exc

            if aggregate is None:
                aggregate = page_payload
            else:
                # Keep paginated rows together so downstream stages see one complete source batch.
                aggregate = _merge_page_payload(aggregate, page_payload)
            page_count += 1
            final_url = str(resp.url)
            http_status = resp.status_code
            content_type = resp.headers.get("content-type")

            next_url = _next_page_url(page_payload, resp)
            if not next_url:
                break
            if page_count >= MAX_PAGINATION_PAGES:
                raise HTTPException(
                    status_code=502,
                    detail=f"pagination exceeded the {MAX_PAGINATION_PAGES}-page safety limit",
                )
            current_url = next_url

        if page_count == 1:
            raw_bytes = resp.content
        else:
            raw_bytes = json.dumps(aggregate, ensure_ascii=False).encode("utf-8")

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
        payload = json.loads(raw_bytes)
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=502, detail=f"upstream response is not valid JSON: {exc}") from exc

    fetched_at = datetime.now(timezone.utc).isoformat()
    checkpoint_after = None
    if req.watermark_required or (
        req.load_mode == "incremental_upsert" and uses_watermark
    ):
        try:
            checkpoint_after = _checkpoint_after(
                payload, req.watermark_field, req.checkpoint_before,
            )
        except ValueError as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc
        if (
            checkpoint_after is None
            and req.load_mode == "incremental_upsert"
            and req.checkpoint_before
            and _empty_payload(payload)
        ):
            checkpoint_after = req.checkpoint_before
        if checkpoint_after is None:
            raise HTTPException(
                status_code=502,
                detail=f"response contains no usable watermark field {req.watermark_field!r}",
            )
    pagination_complete = _pagination_complete(payload)
    manifest = {
        "manifest_version": 1,
        "status": "extracted",
        "source": req.source,
        "run_id": req.run_id,
        "load_mode": req.load_mode,
        "delete_policy": delete_policy,
        "checkpoint_before": req.checkpoint_before,
        "checkpoint_after": checkpoint_after,
        "pagination_complete": pagination_complete,
        "requested_url": req.url,
        "final_url": final_url,
        "fetched_at": fetched_at,
        "http_status": http_status,
        "content_type": content_type,
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
        checkpoint_after=checkpoint_after,
        pagination_complete=pagination_complete,
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
