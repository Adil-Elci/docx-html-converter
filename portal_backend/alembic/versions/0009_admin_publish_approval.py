"""add admin approval fields for publish workflow

Revision ID: 0009_admin_publish_approval
Revises: 0008_password_reset
Create Date: 2026-02-19 00:00:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "0009_admin_publish_approval"
down_revision = "0008_password_reset"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "submissions",
        sa.Column("request_kind", sa.Text(), nullable=False, server_default=sa.text("'guest_post'")),
    )
    op.create_check_constraint(
        "submissions_request_kind_check",
        "submissions",
        "request_kind IN ('guest_post','order')",
    )

    op.add_column(
        "jobs",
        sa.Column("requires_admin_approval", sa.Boolean(), nullable=False, server_default=sa.text("false")),
    )
    op.add_column("jobs", sa.Column("approved_by", postgresql.UUID(as_uuid=True), nullable=True))
    op.add_column("jobs", sa.Column("approved_at", sa.TIMESTAMP(timezone=True), nullable=True))
    op.create_foreign_key(
        "jobs_approved_by_fkey",
        "jobs",
        "users",
        ["approved_by"],
        ["id"],
        ondelete="SET NULL",
    )

    op.drop_constraint("jobs_job_status_check", "jobs", type_="check")
    op.create_check_constraint(
        "jobs_job_status_check",
        "jobs",
        "job_status IN ('queued','processing','pending_approval','succeeded','failed','retrying')",
    )

    op.create_index(
        "idx_jobs_pending_approval",
        "jobs",
        ["job_status", "requires_admin_approval", "updated_at"],
    )
    op.create_index("idx_jobs_approved_by", "jobs", ["approved_by"])


def downgrade() -> None:
    op.drop_index("idx_jobs_approved_by", table_name="jobs")
    op.drop_index("idx_jobs_pending_approval", table_name="jobs")

    op.drop_constraint("jobs_job_status_check", "jobs", type_="check")
    op.create_check_constraint(
        "jobs_job_status_check",
        "jobs",
        "job_status IN ('queued','processing','succeeded','failed','retrying')",
    )

    op.drop_constraint("jobs_approved_by_fkey", "jobs", type_="foreignkey")
    op.drop_column("jobs", "approved_at")
    op.drop_column("jobs", "approved_by")
    op.drop_column("jobs", "requires_admin_approval")

    op.drop_constraint("submissions_request_kind_check", "submissions", type_="check")
    op.drop_column("submissions", "request_kind")
