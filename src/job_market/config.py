from __future__ import annotations

import os
from dataclasses import dataclass


def _integer(name: str, default: int) -> int:
    return int(os.getenv(name, default))


def _floating(name: str, default: float) -> float:
    return float(os.getenv(name, default))


@dataclass(frozen=True, slots=True)
class Settings:
    database_url: str = os.getenv(
        "DATABASE_URL", "postgresql+psycopg://job_market:job_market@localhost:5432/job_market"
    )
    hh_search_text: str = os.getenv("HH_SEARCH_TEXT", "python")
    hh_area: int = _integer("HH_AREA", 113)
    hh_pages: int = _integer("HH_PAGES", 3)
    collect_interval_minutes: int = _integer("COLLECT_INTERVAL_MINUTES", 60)
    request_delay_seconds: float = _floating("REQUEST_DELAY_SECONDS", 0.25)
    user_agent: str = os.getenv(
        "APP_USER_AGENT", "job-market-observer/2.0 (contact: repository-owner)"
    )
    log_level: str = os.getenv("LOG_LEVEL", "INFO")


settings = Settings()

