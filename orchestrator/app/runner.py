from __future__ import annotations

import asyncio
import threading

from . import db, pipeline

_lock = threading.Lock()
_running: set[str] = set()


def is_running(source_name: str) -> bool:
    with _lock:
        return source_name in _running


def _try_start(source_name: str) -> bool:
    with _lock:
        if source_name in _running:
            return False
        _running.add(source_name)
        return True


def _finish(source_name: str) -> None:
    with _lock:
        _running.discard(source_name)


async def trigger_run(source: dict) -> int:
    """Starts a pipeline run for `source` in the background.
    Raises RuntimeError if that source already has a run in flight
    (caller is expected to turn this into an HTTP 409, per spec section 9)."""
    if not _try_start(source["name"]):
        raise RuntimeError(f"source '{source['name']}' is already running")

    run_id = db.create_run(source["name"])

    async def _worker() -> None:
        try:
            await asyncio.to_thread(pipeline.run_pipeline_steps, source["name"], source["url"], run_id)
        except pipeline.PipelineError as exc:
            db.update_run(run_id, status="failed", step=exc.step, error=exc.message, finished=db.now_iso())
        except Exception as exc:  # noqa: BLE001 - last-resort catch so a run never hangs "running" forever
            db.update_run(run_id, status="failed", step="unknown", error=str(exc), finished=db.now_iso())
        finally:
            _finish(source["name"])

    asyncio.create_task(_worker())
    return run_id
