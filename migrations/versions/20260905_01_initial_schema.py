"""Initial normalized vacancy and history schema.

Revision ID: 20260905_01
Revises:
"""
from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa

revision: str = "20260905_01"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "collection_runs",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("source", sa.String(40), nullable=False),
        sa.Column("query", sa.String(255), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True)),
        sa.Column("status", sa.String(20), nullable=False),
        sa.Column("received", sa.Integer(), nullable=False),
        sa.Column("created", sa.Integer(), nullable=False),
        sa.Column("updated", sa.Integer(), nullable=False),
        sa.Column("unchanged", sa.Integer(), nullable=False),
        sa.Column("error", sa.Text()),
    )
    op.create_index("ix_collection_runs_source", "collection_runs", ["source"])
    op.create_index("ix_collection_runs_started_at", "collection_runs", ["started_at"])
    op.create_table(
        "vacancies",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("source", sa.String(40), nullable=False),
        sa.Column("source_id", sa.String(80), nullable=False),
        sa.Column("title", sa.String(500), nullable=False),
        sa.Column("employer_name", sa.String(300)),
        sa.Column("city", sa.String(150)),
        sa.Column("grade", sa.String(20), nullable=False),
        sa.Column("experience", sa.String(100)),
        sa.Column("employment", sa.String(100)),
        sa.Column("salary_from", sa.Float()), sa.Column("salary_to", sa.Float()),
        sa.Column("salary_currency", sa.String(8)), sa.Column("salary_gross", sa.Boolean()),
        sa.Column("salary_rub_from", sa.Float()), sa.Column("salary_rub_to", sa.Float()),
        sa.Column("url", sa.String(1000), nullable=False),
        sa.Column("published_at", sa.DateTime(timezone=True)),
        sa.Column("first_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("is_active", sa.Boolean(), nullable=False),
        sa.Column("content_hash", sa.String(64), nullable=False),
        sa.Column("raw_payload", sa.JSON(), nullable=False),
        sa.UniqueConstraint("source", "source_id", name="uq_vacancy_source_id"),
    )
    for column in ("title", "employer_name", "city", "grade", "published_at", "first_seen_at", "last_seen_at", "is_active"):
        op.create_index(f"ix_vacancies_{column}", "vacancies", [column])
    op.create_index("ix_vacancies_city_grade", "vacancies", ["city", "grade"])
    op.create_table("skills", sa.Column("id", sa.Integer(), primary_key=True), sa.Column("name", sa.String(100), nullable=False, unique=True))
    op.create_index("ix_skills_name", "skills", ["name"], unique=True)
    op.create_table(
        "vacancy_skills",
        sa.Column("vacancy_id", sa.Integer(), sa.ForeignKey("vacancies.id", ondelete="CASCADE"), primary_key=True),
        sa.Column("skill_id", sa.Integer(), sa.ForeignKey("skills.id", ondelete="CASCADE"), primary_key=True),
    )
    op.create_table(
        "vacancy_snapshots",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("vacancy_id", sa.Integer(), sa.ForeignKey("vacancies.id", ondelete="CASCADE"), nullable=False),
        sa.Column("collected_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("content_hash", sa.String(64), nullable=False),
        sa.Column("title", sa.String(500), nullable=False), sa.Column("city", sa.String(150)),
        sa.Column("grade", sa.String(20), nullable=False), sa.Column("salary_rub_from", sa.Float()),
        sa.Column("salary_rub_to", sa.Float()), sa.Column("skills", sa.JSON(), nullable=False),
        sa.Column("is_active", sa.Boolean(), nullable=False),
        sa.UniqueConstraint("vacancy_id", "content_hash", name="uq_snapshot_vacancy_hash"),
    )
    op.create_index("ix_vacancy_snapshots_collected_at", "vacancy_snapshots", ["collected_at"])
    op.create_index("ix_snapshots_vacancy_collected", "vacancy_snapshots", ["vacancy_id", "collected_at"])


def downgrade() -> None:
    op.drop_table("vacancy_snapshots")
    op.drop_table("vacancy_skills")
    op.drop_table("skills")
    op.drop_table("vacancies")
    op.drop_table("collection_runs")

