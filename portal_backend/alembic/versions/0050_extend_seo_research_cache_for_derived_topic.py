"""extend seo_research_cache.cache_kind to allow derived_topic

Revision ID: 0050_extend_seo_research_cache_for_derived_topic
Revises: 0049_extend_seo_research_cache_kinds
Create Date: 2026-05-05 12:00:00.000000
"""

from __future__ import annotations

from alembic import op


revision = "0050_extend_seo_research_cache_for_derived_topic"
down_revision = "0049_extend_seo_research_cache_kinds"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_constraint("seo_research_cache_kind_check", "seo_research_cache", type_="check")
    op.create_check_constraint(
        "seo_research_cache_kind_check",
        "seo_research_cache",
        "cache_kind IN ('keyword_metrics','serp_results','duplicate_check','research_payload','derived_topic')",
    )


def downgrade() -> None:
    op.execute("DELETE FROM seo_research_cache WHERE cache_kind = 'derived_topic'")
    op.drop_constraint("seo_research_cache_kind_check", "seo_research_cache", type_="check")
    op.create_check_constraint(
        "seo_research_cache_kind_check",
        "seo_research_cache",
        "cache_kind IN ('keyword_metrics','serp_results','duplicate_check','research_payload')",
    )
