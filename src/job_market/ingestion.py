from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Literal

from sqlalchemy import select
from sqlalchemy.orm import Session

from .models import Skill, Vacancy, VacancySnapshot
from .normalization import detect_grade, normalize_salary, normalize_skills


@dataclass(frozen=True, slots=True)
class ParsedVacancy:
    source_id: str
    title: str
    employer_name: str | None
    city: str | None
    grade: str
    experience: str | None
    employment: str | None
    salary_from: float | None
    salary_to: float | None
    salary_currency: str | None
    salary_gross: bool | None
    salary_rub_from: float | None
    salary_rub_to: float | None
    skills: list[str]
    url: str
    published_at: datetime | None
    content_hash: str
    raw_payload: dict[str, Any]


def parse_hh_vacancy(payload: dict[str, Any]) -> ParsedVacancy:
    title = str(payload.get("name") or "").strip()
    experience = payload.get("experience") or {}
    employment = payload.get("employment") or {}
    salary = normalize_salary(payload.get("salary"))
    skill_names = normalize_skills(
        [str(item.get("name", "")) for item in payload.get("key_skills", [])]
    )
    stable = {
        "title": title,
        "employer": (payload.get("employer") or {}).get("name"),
        "city": (payload.get("area") or {}).get("name"),
        "experience": experience.get("id"),
        "employment": employment.get("id"),
        "salary": payload.get("salary"),
        "skills": skill_names,
        "archived": bool(payload.get("archived", False)),
    }
    content_hash = hashlib.sha256(
        json.dumps(stable, sort_keys=True, ensure_ascii=False).encode("utf-8")
    ).hexdigest()
    return ParsedVacancy(
        source_id=str(payload["id"]),
        title=title,
        employer_name=(payload.get("employer") or {}).get("name"),
        city=(payload.get("area") or {}).get("name"),
        grade=detect_grade(title, experience.get("id")),
        experience=experience.get("name"),
        employment=employment.get("name"),
        salary_from=salary.from_amount,
        salary_to=salary.to_amount,
        salary_currency=salary.currency,
        salary_gross=salary.gross,
        salary_rub_from=salary.rub_from,
        salary_rub_to=salary.rub_to,
        skills=skill_names,
        url=str(payload.get("alternate_url") or payload.get("url") or ""),
        published_at=_datetime(payload.get("published_at")),
        content_hash=content_hash,
        raw_payload=payload,
    )


def upsert_vacancy(
    session: Session, parsed: ParsedVacancy, collected_at: datetime | None = None
) -> Literal["created", "updated", "unchanged"]:
    now = collected_at or datetime.now(UTC)
    vacancy = session.scalar(
        select(Vacancy).where(Vacancy.source == "hh", Vacancy.source_id == parsed.source_id)
    )
    if vacancy is None:
        vacancy = Vacancy(
            source="hh",
            source_id=parsed.source_id,
            first_seen_at=now,
            last_seen_at=now,
            is_active=not bool(parsed.raw_payload.get("archived")),
        )
        session.add(vacancy)
        result: Literal["created", "updated", "unchanged"] = "created"
    elif vacancy.content_hash == parsed.content_hash:
        vacancy.last_seen_at = now
        return "unchanged"
    else:
        result = "updated"

    for field in (
        "title",
        "employer_name",
        "city",
        "grade",
        "experience",
        "employment",
        "salary_from",
        "salary_to",
        "salary_currency",
        "salary_gross",
        "salary_rub_from",
        "salary_rub_to",
        "url",
        "published_at",
        "content_hash",
        "raw_payload",
    ):
        setattr(vacancy, field, getattr(parsed, field))
    vacancy.last_seen_at = now
    vacancy.is_active = not bool(parsed.raw_payload.get("archived"))
    vacancy.skills = [_get_or_create_skill(session, name) for name in parsed.skills]
    session.flush()
    session.add(
        VacancySnapshot(
            vacancy_id=vacancy.id,
            collected_at=now,
            content_hash=parsed.content_hash,
            title=parsed.title,
            city=parsed.city,
            grade=parsed.grade,
            salary_rub_from=parsed.salary_rub_from,
            salary_rub_to=parsed.salary_rub_to,
            skills=parsed.skills,
            is_active=vacancy.is_active,
        )
    )
    return result


def _get_or_create_skill(session: Session, name: str) -> Skill:
    skill = session.scalar(select(Skill).where(Skill.name == name))
    if skill is None:
        skill = Skill(name=name)
        session.add(skill)
        session.flush()
    return skill


def _datetime(value: Any) -> datetime | None:
    if not value:
        return None
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)
