from __future__ import annotations

import argparse
import logging

from .collector import collect_hh
from .config import settings
from .db import SessionLocal
from .hh_client import HeadHunterClient


def collect_once(text: str, area: int, pages: int) -> None:
    with SessionLocal() as session, HeadHunterClient() as client:
        result = collect_hh(session, client, text=text, area=area, pages=pages)
    logging.getLogger(__name__).info(
        "Collection completed: received=%d created=%d updated=%d unchanged=%d",
        result.received,
        result.created,
        result.updated,
        result.unchanged,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Collect vacancies from the public HH API")
    parser.add_argument("--text", default=settings.hh_search_text)
    parser.add_argument("--area", type=int, default=settings.hh_area)
    parser.add_argument("--pages", type=int, default=settings.hh_pages)
    args = parser.parse_args()
    logging.basicConfig(level=settings.log_level)
    collect_once(args.text, args.area, args.pages)


if __name__ == "__main__":
    main()

