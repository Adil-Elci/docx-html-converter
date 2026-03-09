"""add keyword trend cache table

Revision ID: 0027_keyword_trend_cache
Revises: 0026_publishing_site_article_index
Create Date: 2026-03-09 00:00:01.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "0027_keyword_trend_cache"
down_revision = "0026_publishing_site_article_index"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "keyword_trend_cache",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("source", sa.Text(), nullable=False, server_default=sa.text("'google_suggest'")),
        sa.Column("locale", sa.Text(), nullable=False, server_default=sa.text("'de-DE'")),
        sa.Column("seed_query", sa.Text(), nullable=False),
        sa.Column("normalized_seed_query", sa.Text(), nullable=False),
        sa.Column("content_hash", sa.Text(), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("fetched_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint("source IN ('google_suggest')", name="keyword_trend_cache_source_check"),
        sa.UniqueConstraint("source", "locale", "normalized_seed_query", name="keyword_trend_cache_lookup_unique"),
    )
    op.create_index(
        "idx_keyword_trend_cache_lookup",
        "keyword_trend_cache",
        ["source", "locale", "normalized_seed_query", "expires_at"],
    )


def downgrade() -> None:
    op.drop_index("idx_keyword_trend_cache_lookup", table_name="keyword_trend_cache")
    op.drop_table("keyword_trend_cache")
