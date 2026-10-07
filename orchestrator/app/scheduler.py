from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime
from zoneinfo import ZoneInfo

from croniter import croniter

from . import db, runner

logger = logging.getLogger("dw.scheduler")

INTERVAL_SECONDS = int(os.environ.get("ORCH_SCHEDULER_INTERVAL_SECONDS", "5"))
TIME_ZONE = ZoneInfo(os.environ.get("TZ", "UTC"))

_task: asyncio.Task | None = None


def _parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value)


def _next_run(cron: str, now: datetime) -> datetime:
    return croniter(cron, now).get_next(datetime)


async def _tick() -> None:
    now = datetime.now(TIME_ZONE)
    for source in db.list_sources():
        if not source["enabled"]:
            continue
        try:
            next_run = _parse_iso(source["next_run"])
            if next_run is None or source.get("next_run_timezone") != TIME_ZONE.key:
                next_run = _next_run(source["cron"], now)
                db.update_source_next_run(
                    source["id"], next_run.isoformat(), TIME_ZONE.key,
                )
                continue

            if now >= next_run:
                if not runner.is_running(source["name"]):
                    try:
                        await runner.trigger_run(source)
                    except RuntimeError:
                        # Already running via a manual trigger - fine, just skip this tick.
                        pass
                new_next = _next_run(source["cron"], now)
                db.update_source_next_run(
                    source["id"], new_next.isoformat(), TIME_ZONE.key,
                )
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
