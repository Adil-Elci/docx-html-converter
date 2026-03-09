"""add site profiles and fit cache

Revision ID: 0029_add_site_profiles_and_fit_cache
Revises: 0028_keyword_trend_cache_usage
Create Date: 2026-03-09 13:00:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "0029_add_site_profiles_and_fit_cache"
down_revision = "0028_keyword_trend_cache_usage"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "site_profile_cache",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("profile_kind", sa.Text(), nullable=False),
        sa.Column("publishing_site_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("client_target_site_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("normalized_url", sa.Text(), nullable=False),
        sa.Column("content_hash", sa.Text(), nullable=False),
        sa.Column("generator_mode", sa.Text(), nullable=False, server_default="deterministic"),
        sa.Column("profile_version", sa.Text(), nullable=False, server_default="v1"),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.ForeignKeyConstraint(["client_target_site_id"], ["client_target_sites.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["publishing_site_id"], ["publishing_sites.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint("profile_kind IN ('publishing_site','target_site')", name="site_profile_cache_kind_check"),
        sa.CheckConstraint("generator_mode IN ('deterministic','hybrid')", name="site_profile_cache_mode_check"),
        sa.CheckConstraint(
            "(CASE WHEN publishing_site_id IS NULL THEN 0 ELSE 1 END + CASE WHEN client_target_site_id IS NULL THEN 0 ELSE 1 END) <= 1",
            name="site_profile_cache_ref_count_check",
        ),
        sa.CheckConstraint(
            "((profile_kind = 'publishing_site' AND client_target_site_id IS NULL) OR (profile_kind = 'target_site' AND publishing_site_id IS NULL))",
            name="site_profile_cache_ref_kind_check",
        ),
        sa.UniqueConstraint(
            "profile_kind",
            "normalized_url",
            "content_hash",
            "profile_version",
            name="site_profile_cache_lookup_unique",
        ),
    )
    op.create_index("idx_site_profile_cache_kind_url", "site_profile_cache", ["profile_kind", "normalized_url"])
    op.create_index("idx_site_profile_cache_publishing_site_id", "site_profile_cache", ["publishing_site_id"])
    op.create_index("idx_site_profile_cache_client_target_site_id", "site_profile_cache", ["client_target_site_id"])

    op.create_table(
        "site_fit_cache",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("publishing_site_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("client_target_site_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("target_normalized_url", sa.Text(), nullable=False),
        sa.Column("publishing_profile_hash", sa.Text(), nullable=False),
        sa.Column("target_profile_hash", sa.Text(), nullable=False),
        sa.Column("model_name", sa.Text(), nullable=False, server_default=""),
        sa.Column("prompt_version", sa.Text(), nullable=False, server_default="v1"),
        sa.Column("fit_score", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("decision", sa.Text(), nullable=False, server_default="accepted"),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.ForeignKeyConstraint(["client_target_site_id"], ["client_target_sites.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["publishing_site_id"], ["publishing_sites.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint("decision IN ('accepted','rejected')", name="site_fit_cache_decision_check"),
        sa.CheckConstraint("fit_score >= 0 AND fit_score <= 100", name="site_fit_cache_score_check"),
        sa.UniqueConstraint(
            "publishing_site_id",
            "target_normalized_url",
            "publishing_profile_hash",
            "target_profile_hash",
            "prompt_version",
            name="site_fit_cache_lookup_unique",
        ),
    )
    op.create_index("idx_site_fit_cache_target_url", "site_fit_cache", ["target_normalized_url"])
    op.create_index("idx_site_fit_cache_publishing_site_id", "site_fit_cache", ["publishing_site_id"])
    op.create_index("idx_site_fit_cache_client_target_site_id", "site_fit_cache", ["client_target_site_id"])


def downgrade() -> None:
    op.drop_index("idx_site_fit_cache_client_target_site_id", table_name="site_fit_cache")
    op.drop_index("idx_site_fit_cache_publishing_site_id", table_name="site_fit_cache")
    op.drop_index("idx_site_fit_cache_target_url", table_name="site_fit_cache")
    op.drop_table("site_fit_cache")

    op.drop_index("idx_site_profile_cache_client_target_site_id", table_name="site_profile_cache")
    op.drop_index("idx_site_profile_cache_publishing_site_id", table_name="site_profile_cache")
    op.drop_index("idx_site_profile_cache_kind_url", table_name="site_profile_cache")
    op.drop_table("site_profile_cache")
