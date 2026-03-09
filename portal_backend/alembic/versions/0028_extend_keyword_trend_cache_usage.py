"""extend keyword trend cache with family and usage metadata

Revision ID: 0028_keyword_trend_cache_usage
Revises: 0027_keyword_trend_cache
Create Date: 2026-03-09 00:00:02.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "0028_keyword_trend_cache_usage"
down_revision = "0027_keyword_trend_cache"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("keyword_trend_cache", sa.Column("query_family", sa.Text(), nullable=False, server_default=""))
    op.add_column("keyword_trend_cache", sa.Column("hit_count", sa.Integer(), nullable=False, server_default="0"))
    op.add_column("keyword_trend_cache", sa.Column("last_used_at", sa.DateTime(timezone=True), nullable=True))
    op.create_index(
        "idx_keyword_trend_cache_family_usage",
        "keyword_trend_cache",
        ["source", "locale", "query_family", "last_used_at"],
    )
    op.alter_column("keyword_trend_cache", "query_family", server_default=None)
    op.alter_column("keyword_trend_cache", "hit_count", server_default=None)


def downgrade() -> None:
    op.drop_index("idx_keyword_trend_cache_family_usage", table_name="keyword_trend_cache")
    op.drop_column("keyword_trend_cache", "last_used_at")
    op.drop_column("keyword_trend_cache", "hit_count")
    op.drop_column("keyword_trend_cache", "query_family")
