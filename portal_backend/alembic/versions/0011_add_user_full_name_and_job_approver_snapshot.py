"""add user full_name and jobs approver name snapshot

Revision ID: 0011_add_user_full_name_and_job_approver_snapshot
Revises: 0010_add_rejected_job_status
Create Date: 2026-02-19 00:00:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0011_add_user_full_name_and_job_approver_snapshot"
down_revision = "0010_add_rejected_job_status"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("users", sa.Column("full_name", sa.Text(), nullable=True))
    op.add_column("jobs", sa.Column("approved_by_name_snapshot", sa.Text(), nullable=True))


def downgrade() -> None:
    op.drop_column("jobs", "approved_by_name_snapshot")
    op.drop_column("users", "full_name")

