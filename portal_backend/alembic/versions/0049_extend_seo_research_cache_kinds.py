"""extend seo_research_cache.cache_kind to allow research_payload

Revision ID: 0049_extend_seo_research_cache_kinds
Revises: 0048_drop_jobs_pipeline_mode
Create Date: 2026-05-04 12:00:00.000000
"""

from __future__ import annotations

from alembic import op


revision = "0049_extend_seo_research_cache_kinds"
down_revision = "0048_drop_jobs_pipeline_mode"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_constraint("seo_research_cache_kind_check", "seo_research_cache", type_="check")
    op.create_check_constraint(
        "seo_research_cache_kind_check",
        "seo_research_cache",
        "cache_kind IN ('keyword_metrics','serp_results','duplicate_check','research_payload')",
    )


def downgrade() -> None:
    # Purge research_payload rows so the narrowed CHECK can be re-added.
    op.execute("DELETE FROM seo_research_cache WHERE cache_kind = 'research_payload'")
    op.drop_constraint("seo_research_cache_kind_check", "seo_research_cache", type_="check")
    op.create_check_constraint(
        "seo_research_cache_kind_check",
        "seo_research_cache",
        "cache_kind IN ('keyword_metrics','serp_results','duplicate_check')",
    )
