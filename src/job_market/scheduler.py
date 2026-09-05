from __future__ import annotations

import logging
from datetime import UTC, datetime

from apscheduler.schedulers.blocking import BlockingScheduler

from .cli import collect_once
from .config import settings


def main() -> None:
    logging.basicConfig(level=settings.log_level)
    scheduler = BlockingScheduler(timezone="UTC")
    scheduler.add_job(
        collect_once,
        "interval",
        minutes=settings.collect_interval_minutes,
        kwargs={
            "text": settings.hh_search_text,
            "area": settings.hh_area,
            "pages": settings.hh_pages,
        },
        id="collect-hh-vacancies",
        max_instances=1,
        coalesce=True,
        next_run_time=datetime.now(UTC),
    )
    scheduler.start()


if __name__ == "__main__":
    main()
