"""add jobs.service_type

Revision ID: 0054_add_jobs_service_type
Revises: 0053_add_jobs_article_format
Create Date: 2026-05-25 00:00:00.000000
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op


revision = "0054_add_jobs_service_type"
down_revision = "0053_add_jobs_article_format"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "jobs",
        sa.Column(
            "service_type",
            sa.String(length=24),
            nullable=False,
            server_default=sa.text("'article'"),
        ),
    )
    op.create_check_constraint(
        "jobs_service_type_check",
        "jobs",
        "service_type IN ('article','brand_mention')",
    )


def downgrade() -> None:
    op.drop_constraint("jobs_service_type_check", "jobs", type_="check")
    op.drop_column("jobs", "service_type")
