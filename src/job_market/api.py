from __future__ import annotations

from typing import Annotated

import uvicorn
from fastapi import Depends, FastAPI, Query
from fastapi.responses import HTMLResponse
from sqlalchemy import func, select, text
from sqlalchemy.orm import Session

from .analytics import demand_history, grouped_stats
from .dashboard import DASHBOARD_HTML
from .db import get_session
from .models import Vacancy
from .normalization import normalize_skill
from .schemas import DemandPoint, Health, StatItem, VacancyOut, VacancyPage

app = FastAPI(
    title="Job Market Observer",
    version="2.0.0",
    description="Vacancy collection, normalization, history and market analytics.",
)
DbSession = Annotated[Session, Depends(get_session)]


@app.get("/", response_class=HTMLResponse, include_in_schema=False)
def dashboard() -> str:
    return DASHBOARD_HTML


@app.get("/health", response_model=Health, tags=["service"])
def health(session: DbSession) -> Health:
    session.execute(text("SELECT 1"))
    return Health(status="ok", database="ok")


@app.get("/api/v1/vacancies", response_model=VacancyPage, tags=["vacancies"])
def vacancies(
    session: DbSession,
    city: str | None = None,
    grade: str | None = None,
    skill: str | None = None,
    active: bool = True,
    limit: Annotated[int, Query(ge=1, le=100)] = 20,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> VacancyPage:
    query = select(Vacancy).where(Vacancy.is_active.is_(active))
    if city:
        query = query.where(Vacancy.city == city)
    if grade:
        query = query.where(Vacancy.grade == grade)
    if skill:
        query = query.where(Vacancy.skills.any(name=normalize_skill(skill)))
    total = session.scalar(select(func.count()).select_from(query.subquery())) or 0
    rows = session.scalars(
        query.order_by(Vacancy.published_at.desc()).offset(offset).limit(limit)
    ).unique()
    items = [
        VacancyOut.model_validate(
            {
                **{
                    field: getattr(row, field)
                    for field in VacancyOut.model_fields
                    if field != "skills"
                },
                "skills": [item.name for item in row.skills],
            }
        )
        for row in rows
    ]
    return VacancyPage(items=items, total=total, limit=limit, offset=offset)


@app.get("/api/v1/stats/{dimension}", response_model=list[StatItem], tags=["analytics"])
def stats(
    dimension: str,
    session: DbSession,
    limit: Annotated[int, Query(ge=1, le=100)] = 20,
) -> list[dict]:
    if dimension not in {"cities", "grades", "technologies"}:
        from fastapi import HTTPException

        raise HTTPException(404, "Unknown dimension")
    return grouped_stats(session, dimension, limit)


@app.get("/api/v1/stats/demand/history", response_model=list[DemandPoint], tags=["analytics"])
def demand(session: DbSession, days: Annotated[int, Query(ge=1, le=365)] = 30) -> list[dict]:
    return demand_history(session, days)


def run() -> None:
    uvicorn.run("job_market.api:app", host="0.0.0.0", port=8000)
