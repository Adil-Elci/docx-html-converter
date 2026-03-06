"""add site analysis cache table

Revision ID: 0024_site_analysis_cache
Revises: 0023_update_request_kind_check
Create Date: 2026-03-06 00:00:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "0024_site_analysis_cache"
down_revision = "0023_request_kind_check"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "site_analysis_cache",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("cache_kind", sa.Text(), nullable=False, server_default=sa.text("'phase2_site_analysis'")),
        sa.Column("site_role", sa.Text(), nullable=False),
        sa.Column("publishing_site_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("client_target_site_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("normalized_url", sa.Text(), nullable=False),
        sa.Column("content_hash", sa.Text(), nullable=False),
        sa.Column("generator_mode", sa.Text(), nullable=False),
        sa.Column("model_name", sa.Text(), nullable=False, server_default=sa.text("''")),
        sa.Column("prompt_version", sa.Text(), nullable=False, server_default=sa.text("'v1'")),
        sa.Column("payload", postgresql.JSONB(), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["publishing_site_id"], ["publishing_sites.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["client_target_site_id"], ["client_target_sites.id"], ondelete="CASCADE"),
        sa.CheckConstraint("cache_kind IN ('phase2_site_analysis')", name="site_analysis_cache_kind_check"),
        sa.CheckConstraint("site_role IN ('host','target')", name="site_analysis_cache_role_check"),
        sa.CheckConstraint("generator_mode IN ('llm','deterministic','hybrid')", name="site_analysis_cache_mode_check"),
        sa.CheckConstraint(
            "(CASE WHEN publishing_site_id IS NULL THEN 0 ELSE 1 END + "
            "CASE WHEN client_target_site_id IS NULL THEN 0 ELSE 1 END) <= 1",
            name="site_analysis_cache_ref_count_check",
        ),
        sa.CheckConstraint(
            "((site_role = 'host' AND client_target_site_id IS NULL) "
            "OR (site_role = 'target' AND publishing_site_id IS NULL))",
            name="site_analysis_cache_role_ref_check",
        ),
        sa.UniqueConstraint(
            "cache_kind",
            "site_role",
            "normalized_url",
            "content_hash",
            "generator_mode",
            "model_name",
            "prompt_version",
            name="site_analysis_cache_lookup_unique",
        ),
    )
    op.create_index(
        "idx_site_analysis_cache_publishing_site_id",
        "site_analysis_cache",
        ["publishing_site_id"],
    )
    op.create_index(
        "idx_site_analysis_cache_client_target_site_id",
        "site_analysis_cache",
        ["client_target_site_id"],
    )


def downgrade() -> None:
    op.drop_index("idx_site_analysis_cache_client_target_site_id", table_name="site_analysis_cache")
    op.drop_index("idx_site_analysis_cache_publishing_site_id", table_name="site_analysis_cache")
    op.drop_table("site_analysis_cache")
