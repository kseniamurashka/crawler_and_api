import json
from pathlib import Path

from job_market.ingestion import parse_hh_vacancy

FIXTURE = Path(__file__).parent / "fixtures" / "hh_vacancy.json"


def test_parse_saved_hh_response() -> None:
    parsed = parse_hh_vacancy(json.loads(FIXTURE.read_text(encoding="utf-8")))

    assert parsed.source_id == "98765432"
    assert parsed.city == "Москва"
    assert parsed.grade == "senior"
    assert parsed.salary_rub_from == 250_000
    assert parsed.salary_rub_to == 350_000
    assert parsed.skills == ["kubernetes", "postgresql", "python"]
    assert len(parsed.content_hash) == 64


def test_content_hash_ignores_unstable_payload_fields() -> None:
    payload = json.loads(FIXTURE.read_text(encoding="utf-8"))
    first = parse_hh_vacancy(payload)
    payload["description"] = "API representation changed, business data did not"

    assert parse_hh_vacancy(payload).content_hash == first.content_hash

