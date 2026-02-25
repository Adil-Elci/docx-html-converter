"""rename publishing_sites.name to publishing_site_name

Revision ID: 0019_publishing_site_name_col
Revises: 0018_site_admin_credentials
Create Date: 2026-02-25 00:00:00.000000
"""

from __future__ import annotations

from alembic import op


revision = "0019_publishing_site_name_col"
down_revision = "0018_site_admin_credentials"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column("publishing_sites", "name", new_column_name="publishing_site_name")


def downgrade() -> None:
    op.alter_column("publishing_sites", "publishing_site_name", new_column_name="name")

