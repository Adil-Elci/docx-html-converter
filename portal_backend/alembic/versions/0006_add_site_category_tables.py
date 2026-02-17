"""add site categories tables

Revision ID: 0006_site_category_tables
Revises: 0005_sitecred_author_fields
Create Date: 2026-02-17 00:00:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "0006_site_category_tables"
down_revision = "0005_sitecred_author_fields"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "site_categories",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("site_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("wp_category_id", sa.BigInteger(), nullable=False),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("slug", sa.Text(), nullable=True),
        sa.Column("parent_wp_category_id", sa.BigInteger(), nullable=True),
        sa.Column("post_count", sa.Integer(), nullable=True),
        sa.Column("enabled", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.ForeignKeyConstraint(["site_id"], ["sites.id"], ondelete="CASCADE"),
        sa.UniqueConstraint("site_id", "wp_category_id", name="site_categories_site_wp_category_unique"),
    )
    op.create_index("idx_site_categories_site_id", "site_categories", ["site_id"])
    op.create_index("idx_site_categories_enabled", "site_categories", ["enabled"])

    op.create_table(
        "site_default_categories",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("site_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("wp_category_id", sa.BigInteger(), nullable=False),
        sa.Column("category_name", sa.Text(), nullable=True),
        sa.Column("position", sa.Integer(), nullable=False, server_default=sa.text("100")),
        sa.Column("enabled", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.ForeignKeyConstraint(["site_id"], ["sites.id"], ondelete="CASCADE"),
        sa.UniqueConstraint("site_id", "wp_category_id", name="site_default_categories_site_wp_category_unique"),
    )
    op.create_index("idx_site_default_categories_site_id", "site_default_categories", ["site_id"])
    op.create_index("idx_site_default_categories_enabled", "site_default_categories", ["enabled"])
    op.create_index("idx_site_default_categories_position", "site_default_categories", ["position"])


def downgrade() -> None:
    op.drop_index("idx_site_default_categories_position", table_name="site_default_categories")
    op.drop_index("idx_site_default_categories_enabled", table_name="site_default_categories")
    op.drop_index("idx_site_default_categories_site_id", table_name="site_default_categories")
    op.drop_table("site_default_categories")

    op.drop_index("idx_site_categories_enabled", table_name="site_categories")
    op.drop_index("idx_site_categories_site_id", table_name="site_categories")
    op.drop_table("site_categories")
