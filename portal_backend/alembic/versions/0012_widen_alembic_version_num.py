"""widen alembic_version.version_num

Revision ID: 0012_widen_alembic_version_num
Revises: 0011_user_name_approver_snap
Create Date: 2026-02-19 00:00:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0012_widen_alembic_version_num"
down_revision = "0011_user_name_approver_snap"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column(
        "alembic_version",
        "version_num",
        existing_type=sa.String(length=32),
        type_=sa.String(length=128),
        existing_nullable=False,
    )


def downgrade() -> None:
    op.alter_column(
        "alembic_version",
        "version_num",
        existing_type=sa.String(length=128),
        type_=sa.String(length=32),
        existing_nullable=False,
    )

