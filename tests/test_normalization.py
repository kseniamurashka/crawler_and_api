from job_market.normalization import detect_grade, normalize_salary, normalize_skills


def test_grade_title_has_priority_over_experience() -> None:
    assert detect_grade("Junior Python Developer", "between3And6") == "junior"
    assert detect_grade("Senior / Team Lead", "between3And6") == "lead"


def test_salary_is_only_converted_with_known_rate() -> None:
    usd = normalize_salary({"from": 1000, "to": 2000, "currency": "USD", "gross": True})
    converted = normalize_salary(
        {"from": 1000, "to": None, "currency": "USD", "gross": True}, {"USD": 90.0}
    )

    assert usd.rub_from is None
    assert converted.rub_from == 90_000


def test_skill_aliases_are_normalized_and_deduplicated() -> None:
    assert normalize_skills([" Postgres ", "PostgreSQL", "JS"]) == ["javascript", "postgresql"]

