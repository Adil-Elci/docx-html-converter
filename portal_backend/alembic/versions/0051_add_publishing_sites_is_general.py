"""add publishing_sites.is_general flag for allgemein-topic fallback

Revision ID: 0051_add_publishing_sites_is_general
Revises: 0050_extend_seo_research_cache_for_derived_topic
Create Date: 2026-05-05 13:00:00.000000
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op


revision = "0051_add_publishing_sites_is_general"
down_revision = "0050_extend_seo_research_cache_for_derived_topic"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "publishing_sites",
        sa.Column(
            "is_general",
            sa.Boolean(),
            nullable=False,
            server_default=sa.false(),
        ),
    )


def downgrade() -> None:
    op.drop_column("publishing_sites", "is_general")
