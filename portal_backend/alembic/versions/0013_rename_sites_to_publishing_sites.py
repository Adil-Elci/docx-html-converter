"""rename site tables/columns to publishing site terminology

Revision ID: 0013_publishing_sites_rename
Revises: 0012_widen_alembic_version_num
Create Date: 2026-02-23 00:00:00.000000
"""

from __future__ import annotations

from alembic import op


revision = "0013_publishing_sites_rename"
down_revision = "0012_widen_alembic_version_num"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.rename_table("sites", "publishing_sites")
    op.rename_table("site_credentials", "publishing_site_credentials")
    op.rename_table("site_categories", "publishing_site_categories")
    op.rename_table("site_default_categories", "publishing_site_default_categories")
    op.rename_table("client_site_access", "client_publishing_site_access")

    op.alter_column("publishing_sites", "site_url", new_column_name="publishing_site_url")
    op.alter_column("publishing_site_credentials", "site_id", new_column_name="publishing_site_id")
    op.alter_column("publishing_site_categories", "site_id", new_column_name="publishing_site_id")
    op.alter_column("publishing_site_default_categories", "site_id", new_column_name="publishing_site_id")
    op.alter_column("client_publishing_site_access", "site_id", new_column_name="publishing_site_id")
    op.alter_column("submissions", "site_id", new_column_name="publishing_site_id")
    op.alter_column("jobs", "site_id", new_column_name="publishing_site_id")


def downgrade() -> None:
    op.alter_column("jobs", "publishing_site_id", new_column_name="site_id")
    op.alter_column("submissions", "publishing_site_id", new_column_name="site_id")
    op.alter_column("client_publishing_site_access", "publishing_site_id", new_column_name="site_id")
    op.alter_column("publishing_site_default_categories", "publishing_site_id", new_column_name="site_id")
    op.alter_column("publishing_site_categories", "publishing_site_id", new_column_name="site_id")
    op.alter_column("publishing_site_credentials", "publishing_site_id", new_column_name="site_id")
    op.alter_column("publishing_sites", "publishing_site_url", new_column_name="site_url")

    op.rename_table("client_publishing_site_access", "client_site_access")
    op.rename_table("publishing_site_default_categories", "site_default_categories")
    op.rename_table("publishing_site_categories", "site_categories")
    op.rename_table("publishing_site_credentials", "site_credentials")
    op.rename_table("publishing_sites", "sites")
