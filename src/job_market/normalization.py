from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any


SKILL_ALIASES = {
    "postgres": "postgresql",
    "postgresql": "postgresql",
    "postgre sql": "postgresql",
    "python 3": "python",
    "py": "python",
    "js": "javascript",
    "node": "node.js",
    "nodejs": "node.js",
    "node js": "node.js",
    "reactjs": "react",
    "react.js": "react",
    "k8s": "kubernetes",
    "ci/cd": "ci/cd",
    "gitlab ci": "gitlab ci",
}

GRADE_PATTERNS = (
    ("intern", re.compile(r"\b(intern|internship|стаж[её]р|стажировка)\b", re.I)),
    ("junior", re.compile(r"\b(junior|jr\.?|младший|начинающий)\b", re.I)),
    ("senior", re.compile(r"\b(senior|sr\.?|старший|ведущий)\b", re.I)),
    ("lead", re.compile(r"\b(lead|teamlead|techlead|руководитель|лид)\b", re.I)),
    ("middle", re.compile(r"\b(middle|mid\.?|мидл)\b", re.I)),
)


@dataclass(frozen=True, slots=True)
class NormalizedSalary:
    from_amount: float | None
    to_amount: float | None
    currency: str | None
    gross: bool | None
    rub_from: float | None
    rub_to: float | None


def normalize_skill(value: str) -> str:
    clean = re.sub(r"\s+", " ", value.strip().lower())
    return SKILL_ALIASES.get(clean, clean)


def normalize_skills(values: list[str]) -> list[str]:
    return sorted({skill for value in values if (skill := normalize_skill(value))})


def detect_grade(title: str, experience_id: str | None = None) -> str:
    # Lead must win over senior in titles such as "Senior / Team Lead".
    matches = {grade for grade, pattern in GRADE_PATTERNS if pattern.search(title)}
    for grade in ("lead", "senior", "middle", "junior", "intern"):
        if grade in matches:
            return grade
    return {
        "noExperience": "intern",
        "between1And3": "junior",
        "between3And6": "middle",
        "moreThan6": "senior",
    }.get(experience_id or "", "unknown")


def normalize_salary(
    salary: dict[str, Any] | None,
    rates_to_rub: dict[str, float] | None = None,
) -> NormalizedSalary:
    if not salary:
        return NormalizedSalary(None, None, None, None, None, None)

    currency = str(salary.get("currency") or "").upper() or None
    amount_from = _number(salary.get("from"))
    amount_to = _number(salary.get("to"))
    gross = salary.get("gross")
    rates = {"RUR": 1.0, "RUB": 1.0, **(rates_to_rub or {})}
    rate = rates.get(currency or "")
    return NormalizedSalary(
        amount_from,
        amount_to,
        currency,
        gross if isinstance(gross, bool) else None,
        round(amount_from * rate, 2) if amount_from is not None and rate else None,
        round(amount_to * rate, 2) if amount_to is not None and rate else None,
    )


def _number(value: Any) -> float | None:
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None

