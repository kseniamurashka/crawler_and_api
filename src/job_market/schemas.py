from datetime import date, datetime

from pydantic import BaseModel, ConfigDict


class VacancyOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    source: str
    source_id: str
    title: str
    employer_name: str | None
    city: str | None
    grade: str
    experience: str | None
    salary_rub_from: float | None
    salary_rub_to: float | None
    skills: list[str]
    url: str
    published_at: datetime | None
    first_seen_at: datetime
    last_seen_at: datetime
    is_active: bool


class VacancyPage(BaseModel):
    items: list[VacancyOut]
    total: int
    limit: int
    offset: int


class StatItem(BaseModel):
    name: str
    count: int


class DemandPoint(BaseModel):
    day: date
    vacancies: int


class Health(BaseModel):
    status: str
    database: str

