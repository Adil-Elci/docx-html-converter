"""extend seo_research_cache.cache_kind to allow brainstorm

Revision ID: 0052_extend_seo_research_cache_for_brainstorm
Revises: 0051_add_publishing_sites_is_general
Create Date: 2026-05-05 14:00:00.000000
"""

from __future__ import annotations

from alembic import op


revision = "0052_extend_seo_research_cache_for_brainstorm"
down_revision = "0051_add_publishing_sites_is_general"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_constraint("seo_research_cache_kind_check", "seo_research_cache", type_="check")
    op.create_check_constraint(
        "seo_research_cache_kind_check",
        "seo_research_cache",
        "cache_kind IN ('keyword_metrics','serp_results','duplicate_check','research_payload','derived_topic','brainstorm')",
    )


def downgrade() -> None:
    op.execute("DELETE FROM seo_research_cache WHERE cache_kind = 'brainstorm'")
    op.drop_constraint("seo_research_cache_kind_check", "seo_research_cache", type_="check")
    op.create_check_constraint(
        "seo_research_cache_kind_check",
        "seo_research_cache",
        "cache_kind IN ('keyword_metrics','serp_results','duplicate_check','research_payload','derived_topic')",
    )
