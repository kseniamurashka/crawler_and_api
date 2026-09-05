import json
from pathlib import Path

from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy.pool import StaticPool

from job_market.api import app
from job_market.db import get_session
from job_market.ingestion import parse_hh_vacancy, upsert_vacancy
from job_market.models import Base


FIXTURE = Path(__file__).parent / "fixtures" / "hh_vacancy.json"


def test_vacancies_and_statistics_api() -> None:
    engine = create_engine(
        "sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool
    )
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        upsert_vacancy(session, parse_hh_vacancy(json.loads(FIXTURE.read_text(encoding="utf-8"))))
        session.commit()

    def test_session():
        with Session(engine) as session:
            yield session

    app.dependency_overrides[get_session] = test_session
    try:
        client = TestClient(app)
        response = client.get("/api/v1/vacancies", params={"skill": "Python 3"})
        assert response.status_code == 200
        assert response.json()["total"] == 1
        assert response.json()["items"][0]["skills"] == ["kubernetes", "postgresql", "python"]

        response = client.get("/api/v1/stats/technologies")
        assert response.status_code == 200
        assert {item["name"] for item in response.json()} == {"kubernetes", "postgresql", "python"}
    finally:
        app.dependency_overrides.clear()
