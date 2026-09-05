from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime

from sqlalchemy.orm import Session

from .hh_client import HeadHunterClient
from .ingestion import parse_hh_vacancy, upsert_vacancy
from .models import CollectionRun

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class CollectionResult:
    received: int = 0
    created: int = 0
    updated: int = 0
    unchanged: int = 0


def collect_hh(
    session: Session,
    client: HeadHunterClient,
    *,
    text: str,
    area: int,
    pages: int,
) -> CollectionResult:
    run = CollectionRun(source="hh", query=text, started_at=datetime.now(UTC), status="running")
    session.add(run)
    session.commit()
    result = CollectionResult()
    try:
        for brief in client.search(text=text, area=area, pages=pages):
            result.received += 1
            payload = client.vacancy(str(brief["id"]))
            status = upsert_vacancy(session, parse_hh_vacancy(payload))
            setattr(result, status, getattr(result, status) + 1)
            # Small commits keep a long collection run recoverable.
            session.commit()
        _finish_run(run, result, "completed")
        session.commit()
        return result
    except Exception as exc:
        session.rollback()
        run = session.merge(run)
        _finish_run(run, result, "failed")
        run.error = str(exc)[:2000]
        session.commit()
        logger.exception("Collection run failed")
        raise


def _finish_run(run: CollectionRun, result: CollectionResult, status: str) -> None:
    run.finished_at = datetime.now(UTC)
    run.status = status
    run.received = result.received
    run.created = result.created
    run.updated = result.updated
    run.unchanged = result.unchanged

