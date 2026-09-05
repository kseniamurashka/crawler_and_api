from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

from sqlalchemy import Date, cast, func, select
from sqlalchemy.orm import Session

from .models import Skill, Vacancy, vacancy_skills


def grouped_stats(session: Session, dimension: str, limit: int = 20) -> list[dict]:
    if dimension == "technologies":
        rows = session.execute(
            select(Skill.name, func.count(vacancy_skills.c.vacancy_id).label("count"))
            .join(vacancy_skills, vacancy_skills.c.skill_id == Skill.id)
            .join(Vacancy, Vacancy.id == vacancy_skills.c.vacancy_id)
            .where(Vacancy.is_active.is_(True))
            .group_by(Skill.name)
            .order_by(func.count(vacancy_skills.c.vacancy_id).desc(), Skill.name)
            .limit(limit)
        )
    else:
        column = {"cities": Vacancy.city, "grades": Vacancy.grade}[dimension]
        rows = session.execute(
            select(column, func.count(Vacancy.id).label("count"))
            .where(Vacancy.is_active.is_(True), column.is_not(None))
            .group_by(column)
            .order_by(func.count(Vacancy.id).desc(), column)
            .limit(limit)
        )
    return [{"name": name, "count": count} for name, count in rows]


def demand_history(session: Session, days: int = 30) -> list[dict]:
    since = datetime.now(UTC) - timedelta(days=days)
    rows = session.execute(
        select(
            cast(Vacancy.first_seen_at, Date).label("day"),
            func.count(Vacancy.id).label("vacancies"),
        )
        .where(Vacancy.first_seen_at >= since)
        .group_by(cast(Vacancy.first_seen_at, Date))
        .order_by(cast(Vacancy.first_seen_at, Date))
    )
    return [
        {
            "day": day if isinstance(day, date) else date.fromisoformat(day),
            "vacancies": count,
        }
        for day, count in rows
    ]
