"""add publishing site article index tables

Revision ID: 0026_publishing_site_article_index
Revises: 0025_site_analysis_cache_site_type
Create Date: 2026-03-09 00:00:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "0026_publishing_site_article_index"
down_revision = "0025_site_analysis_cache_site_type"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "publishing_site_articles",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("publishing_site_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("wp_post_id", sa.BigInteger(), nullable=False),
        sa.Column("url", sa.Text(), nullable=False),
        sa.Column("slug", sa.Text(), nullable=True),
        sa.Column("title", sa.Text(), nullable=True),
        sa.Column("excerpt", sa.Text(), nullable=True),
        sa.Column("status", sa.Text(), nullable=False, server_default=sa.text("'unknown'")),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("modified_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("source", sa.Text(), nullable=False, server_default=sa.text("'wp_rest'")),
        sa.Column("last_synced_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["publishing_site_id"], ["publishing_sites.id"], ondelete="CASCADE"),
        sa.CheckConstraint(
            "status IN ('publish','draft','pending','future','private','trash','unavailable','unknown')",
            name="publishing_site_articles_status_check",
        ),
        sa.CheckConstraint(
            "source IN ('wp_rest','job')",
            name="publishing_site_articles_source_check",
        ),
        sa.UniqueConstraint("publishing_site_id", "wp_post_id", name="publishing_site_articles_site_post_unique"),
        sa.UniqueConstraint("publishing_site_id", "url", name="publishing_site_articles_site_url_unique"),
    )
    op.create_index(
        "idx_publishing_site_articles_site_status_published_at",
        "publishing_site_articles",
        ["publishing_site_id", "status", "published_at"],
    )

    op.create_table(
        "publishing_site_article_categories",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("publishing_site_article_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("wp_category_id", sa.BigInteger(), nullable=False),
        sa.Column("category_name", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["publishing_site_article_id"], ["publishing_site_articles.id"], ondelete="CASCADE"),
        sa.UniqueConstraint(
            "publishing_site_article_id",
            "wp_category_id",
            name="publishing_site_article_categories_article_category_unique",
        ),
    )
    op.create_index(
        "idx_publishing_site_article_categories_article_id",
        "publishing_site_article_categories",
        ["publishing_site_article_id"],
    )


def downgrade() -> None:
    op.drop_index("idx_publishing_site_article_categories_article_id", table_name="publishing_site_article_categories")
    op.drop_table("publishing_site_article_categories")
    op.drop_index("idx_publishing_site_articles_site_status_published_at", table_name="publishing_site_articles")
    op.drop_table("publishing_site_articles")
