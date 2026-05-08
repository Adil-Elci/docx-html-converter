"""add jobs.article_format

Revision ID: 0053_add_jobs_article_format
Revises: 0052_extend_seo_research_cache_for_brainstorm
Create Date: 2026-05-08 00:00:00.000000
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op


revision = "0053_add_jobs_article_format"
down_revision = "0052_extend_seo_research_cache_for_brainstorm"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "jobs",
        sa.Column(
            "article_format",
            sa.String(length=24),
            nullable=False,
            server_default=sa.text("'narrative'"),
        ),
    )
    op.create_check_constraint(
        "jobs_article_format_check",
        "jobs",
        "article_format IN ('narrative','listicle')",
    )


def downgrade() -> None:
    op.drop_constraint("jobs_article_format_check", "jobs", type_="check")
    op.drop_column("jobs", "article_format")
