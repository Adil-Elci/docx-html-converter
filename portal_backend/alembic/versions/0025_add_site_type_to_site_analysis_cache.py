"""add site_type to site_analysis_cache and allow target phase cache kind

Revision ID: 0025_site_analysis_cache_site_type
Revises: 0024_site_analysis_cache
Create Date: 2026-03-06 00:00:01.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "0025_site_analysis_cache_site_type"
down_revision = "0024_site_analysis_cache"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "site_analysis_cache",
        sa.Column("site_type", sa.Text(), nullable=True, server_default=sa.text("'publishing_site'")),
    )
    op.execute(
        """
        UPDATE site_analysis_cache
        SET site_type = CASE
            WHEN site_role = 'target' THEN 'target_site'
            ELSE 'publishing_site'
        END
        """
    )
    op.alter_column("site_analysis_cache", "site_type", nullable=False, server_default=None)

    op.drop_constraint("site_analysis_cache_kind_check", "site_analysis_cache", type_="check")
    op.create_check_constraint(
        "site_analysis_cache_kind_check",
        "site_analysis_cache",
        "cache_kind IN ('phase1_target_analysis','phase2_site_analysis')",
    )
    op.create_check_constraint(
        "site_analysis_cache_site_type_check",
        "site_analysis_cache",
        "site_type IN ('publishing_site','target_site')",
    )
    op.create_check_constraint(
        "site_analysis_cache_role_type_check",
        "site_analysis_cache",
        "((site_role = 'host' AND site_type = 'publishing_site') OR "
        "(site_role = 'target' AND site_type = 'target_site'))",
    )


def downgrade() -> None:
    # Remove rows using the new cache kind before restoring stricter constraint.
    op.execute("DELETE FROM site_analysis_cache WHERE cache_kind = 'phase1_target_analysis'")

    op.drop_constraint("site_analysis_cache_role_type_check", "site_analysis_cache", type_="check")
    op.drop_constraint("site_analysis_cache_site_type_check", "site_analysis_cache", type_="check")
    op.drop_constraint("site_analysis_cache_kind_check", "site_analysis_cache", type_="check")
    op.create_check_constraint(
        "site_analysis_cache_kind_check",
        "site_analysis_cache",
        "cache_kind IN ('phase2_site_analysis')",
    )
    op.drop_column("site_analysis_cache", "site_type")

