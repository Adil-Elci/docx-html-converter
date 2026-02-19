"""add rejected status to jobs

Revision ID: 0010_add_rejected_job_status
Revises: 0009_admin_publish_approval
Create Date: 2026-02-19 00:00:00.000000
"""

from __future__ import annotations

from alembic import op


revision = "0010_add_rejected_job_status"
down_revision = "0009_admin_publish_approval"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_constraint("jobs_job_status_check", "jobs", type_="check")
    op.create_check_constraint(
        "jobs_job_status_check",
        "jobs",
        "job_status IN ('queued','processing','pending_approval','rejected','succeeded','failed','retrying')",
    )


def downgrade() -> None:
    op.drop_constraint("jobs_job_status_check", "jobs", type_="check")
    op.create_check_constraint(
        "jobs_job_status_check",
        "jobs",
        "job_status IN ('queued','processing','pending_approval','succeeded','failed','retrying')",
    )

