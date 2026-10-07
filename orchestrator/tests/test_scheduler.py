import asyncio
import unittest
from datetime import datetime, timedelta
from unittest.mock import patch

from orchestrator.app import scheduler
from orchestrator.app.scheduler import TIME_ZONE, _next_run


class SchedulerTimeZoneTests(unittest.TestCase):
    def test_cron_next_run_uses_configured_timezone(self):
        now = datetime(2026, 1, 5, 8, 30, tzinfo=TIME_ZONE)

        next_run = _next_run("0 9 * * *", now)

        self.assertEqual(next_run.hour, 9)
        self.assertEqual(next_run.minute, 0)
        self.assertEqual(next_run.tzinfo, TIME_ZONE)

    def test_existing_schedule_is_recomputed_for_configured_timezone(self):
        source = {
            "id": 7,
            "name": "items",
            "cron": "0 9 * * *",
            "enabled": 1,
            "next_run": "2026-01-05T09:00:00+00:00",
            "next_run_timezone": None,
        }
        calculated = []

        def next_run(cron, now):
            calculated.append(now.tzinfo)
            return now + timedelta(hours=1)

        with (
            patch.object(scheduler.db, "list_sources", return_value=[source]),
            patch.object(scheduler.db, "update_source_next_run") as update_next_run,
            patch.object(scheduler, "_next_run", side_effect=next_run),
        ):
            asyncio.run(scheduler._tick())

        self.assertEqual(calculated, [TIME_ZONE])
        self.assertEqual(update_next_run.call_args.args[0], source["id"])
        self.assertEqual(update_next_run.call_args.args[2], TIME_ZONE.key)


if __name__ == "__main__":
    unittest.main()
