"""add 4llm pipeline tables

Revision ID: 0046_add_4llm_pipeline_tables
Revises: 0045_add_task_board_card_read_states
Create Date: 2026-04-06 12:00:00.000000
"""

from __future__ import annotations

import os

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "0046_add_4llm_pipeline_tables"
down_revision = "0045_add_task_board_card_read_states"
branch_labels = None
depends_on = None


def _vector_dim() -> int:
    raw = os.getenv("EMBEDDING_VECTOR_DIM", "1536").strip()
    try:
        value = int(raw)
    except ValueError:
        return 1536
    return value if value > 0 else 1536


class VectorType(sa.types.UserDefinedType):
    def __init__(self, dim: int):
        self.dim = dim

    def get_col_spec(self, **_kwargs) -> str:
        return f"VECTOR({self.dim})"


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS vector")

    op.add_column("jobs", sa.Column("pipeline_mode", sa.Text(), nullable=False, server_default="legacy"))
    op.add_column(
        "jobs",
        sa.Column(
            "pipeline_state",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
    )
    op.alter_column("jobs", "pipeline_mode", server_default=None)
    op.alter_column("jobs", "pipeline_state", server_default=None)

    op.drop_constraint("job_events_event_type_check", "job_events", type_="check")
    op.create_check_constraint(
        "job_events_event_type_check",
        "job_events",
        "event_type IN ('converter_called','converter_ok','image_prompt_ok','image_generated','wp_post_created','wp_post_updated','failed','creator_phase','canceled','site_understood','site_matched','keyword_research_complete','link_mapping_complete','content_brief_ready','quality_checked','review_ready','published')",
    )

    op.add_column("publishing_site_articles", sa.Column("content_text", sa.Text(), nullable=True))
    op.add_column("publishing_site_articles", sa.Column("content_hash", sa.Text(), nullable=True))
    op.add_column("publishing_site_articles", sa.Column("language", sa.Text(), nullable=True))
    op.add_column("publishing_site_articles", sa.Column("topic", sa.Text(), nullable=True))
    op.add_column("publishing_site_articles", sa.Column("keywords", postgresql.ARRAY(sa.Text()), nullable=True))
    op.add_column("publishing_site_articles", sa.Column("embedding", VectorType(_vector_dim()), nullable=True))
    op.add_column("publishing_site_articles", sa.Column("embedding_model", sa.Text(), nullable=True))
    op.add_column("publishing_site_articles", sa.Column("embedding_updated_at", sa.DateTime(timezone=True), nullable=True))

    op.create_table(
        "target_site_pages",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("client_target_site_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("target_site_url", sa.Text(), nullable=False),
        sa.Column("page_url", sa.Text(), nullable=False),
        sa.Column("title", sa.Text(), nullable=True),
        sa.Column("excerpt", sa.Text(), nullable=True),
        sa.Column("content_text", sa.Text(), nullable=True),
        sa.Column("content_hash", sa.Text(), nullable=True),
        sa.Column("language", sa.Text(), nullable=True),
        sa.Column("embedding", VectorType(_vector_dim()), nullable=True),
        sa.Column("embedding_model", sa.Text(), nullable=True),
        sa.Column("last_scraped_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["client_target_site_id"], ["client_target_sites.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id", name="target_site_pages_pkey"),
        sa.UniqueConstraint("target_site_url", "page_url", name="target_site_pages_target_page_unique"),
    )

    op.create_table(
        "seo_research_cache",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("provider", sa.Text(), nullable=False),
        sa.Column("cache_kind", sa.Text(), nullable=False),
        sa.Column("lookup_key", sa.Text(), nullable=False),
        sa.Column("locale", sa.Text(), nullable=True),
        sa.Column("content_hash", sa.Text(), nullable=True),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("fetched_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint("provider IN ('dataforseo','copyscape')", name="seo_research_cache_provider_check"),
        sa.CheckConstraint(
            "cache_kind IN ('keyword_metrics','serp_results','duplicate_check')",
            name="seo_research_cache_kind_check",
        ),
        sa.PrimaryKeyConstraint("id", name="seo_research_cache_pkey"),
        sa.UniqueConstraint("provider", "cache_kind", "lookup_key", "locale", name="seo_research_cache_lookup_unique"),
    )

    op.create_table(
        "placed_links",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("job_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("publishing_site_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("source_url", sa.Text(), nullable=True),
        sa.Column("target_url", sa.Text(), nullable=False),
        sa.Column("anchor_text", sa.Text(), nullable=False),
        sa.Column("link_type", sa.Text(), nullable=False),
        sa.Column("target_kind", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "link_type IN ('internal','external','target_backlink')",
            name="placed_links_link_type_check",
        ),
        sa.CheckConstraint(
            "target_kind IN ('owned_network','target_site')",
            name="placed_links_target_kind_check",
        ),
        sa.ForeignKeyConstraint(["job_id"], ["jobs.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["publishing_site_id"], ["publishing_sites.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id", name="placed_links_pkey"),
    )


def downgrade() -> None:
    op.drop_table("placed_links")
    op.drop_table("seo_research_cache")
    op.drop_table("target_site_pages")

    op.drop_column("publishing_site_articles", "embedding_updated_at")
    op.drop_column("publishing_site_articles", "embedding_model")
    op.drop_column("publishing_site_articles", "embedding")
    op.drop_column("publishing_site_articles", "keywords")
    op.drop_column("publishing_site_articles", "topic")
    op.drop_column("publishing_site_articles", "language")
    op.drop_column("publishing_site_articles", "content_hash")
    op.drop_column("publishing_site_articles", "content_text")

    op.drop_constraint("job_events_event_type_check", "job_events", type_="check")
    op.create_check_constraint(
        "job_events_event_type_check",
        "job_events",
        "event_type IN ('converter_called','converter_ok','image_prompt_ok','image_generated','wp_post_created','wp_post_updated','failed','creator_phase','canceled')",
    )

    op.drop_column("jobs", "pipeline_state")
    op.drop_column("jobs", "pipeline_mode")
