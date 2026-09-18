from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime, timezone

from croniter import croniter

from . import db, runner

logger = logging.getLogger("dw.scheduler")

INTERVAL_SECONDS = int(os.environ.get("ORCH_SCHEDULER_INTERVAL_SECONDS", "5"))

_task: asyncio.Task | None = None


def _parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value)


async def _tick() -> None:
    now = datetime.now(timezone.utc)
    for source in db.list_sources():
        if not source["enabled"]:
            continue
        try:
            next_run = _parse_iso(source["next_run"])
            if next_run is None:
                next_run = croniter(source["cron"], now).get_next(datetime)
                db.update_source_next_run(source["id"], next_run.isoformat())
                continue

            if now >= next_run:
                if not runner.is_running(source["name"]):
                    try:
                        await runner.trigger_run(source)
                    except RuntimeError:
                        # Already running via a manual trigger - fine, just skip this tick.
                        pass
                new_next = croniter(source["cron"], now).get_next(datetime)
                db.update_source_next_run(source["id"], new_next.isoformat())
        except Exception:  # noqa: BLE001 - one bad cron expr must not kill the scheduler loop
            logger.exception("scheduler tick failed for source %s", source.get("name"))


async def _loop() -> None:
    while True:
        await _tick()
        await asyncio.sleep(INTERVAL_SECONDS)


def start() -> None:
    global _task
    if _task is None:
        _task = asyncio.get_event_loop().create_task(_loop())


def stop() -> None:
    global _task
    if _task is not None:
        _task.cancel()
        _task = None
