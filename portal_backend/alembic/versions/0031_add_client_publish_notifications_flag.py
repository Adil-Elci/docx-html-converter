"""add client publish notifications flag

Revision ID: 0031_add_client_publish_notifications_flag
Revises: 0030_add_client_target_site_root_url
Create Date: 2026-03-11 11:10:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0031_add_client_publish_notifications_flag"
down_revision = "0030_add_client_target_site_root_url"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "clients",
        sa.Column(
            "publish_notifications_enabled",
            sa.Boolean(),
            nullable=False,
            server_default=sa.true(),
        ),
    )


def downgrade() -> None:
    op.drop_column("clients", "publish_notifications_enabled")
