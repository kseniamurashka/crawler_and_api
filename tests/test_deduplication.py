import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import Session

from job_market.ingestion import parse_hh_vacancy, upsert_vacancy
from job_market.models import Base, Vacancy, VacancySnapshot

FIXTURE = Path(__file__).parent / "fixtures" / "hh_vacancy.json"


def test_same_vacancy_is_not_duplicated_but_change_is_snapshotted() -> None:
    engine = create_engine("sqlite://")
    Base.metadata.create_all(engine)
    payload = json.loads(FIXTURE.read_text(encoding="utf-8"))
    first_seen = datetime(2026, 9, 1, tzinfo=UTC)

    with Session(engine, expire_on_commit=False) as session:
        assert upsert_vacancy(session, parse_hh_vacancy(payload), first_seen) == "created"
        session.commit()
        assert upsert_vacancy(
            session, parse_hh_vacancy(payload), first_seen + timedelta(hours=1)
        ) == "unchanged"
        session.commit()
        payload["salary"]["to"] = 375000
        assert upsert_vacancy(
            session, parse_hh_vacancy(payload), first_seen + timedelta(hours=2)
        ) == "updated"
        session.commit()

        assert session.scalar(select(func.count(Vacancy.id))) == 1
        assert session.scalar(select(func.count(VacancySnapshot.id))) == 2
        vacancy = session.scalar(select(Vacancy))
        assert vacancy.salary_rub_to == 375000
        # SQLite strips timezone metadata; PostgreSQL preserves it.
        assert vacancy.last_seen_at.replace(tzinfo=UTC) == first_seen + timedelta(hours=2)
