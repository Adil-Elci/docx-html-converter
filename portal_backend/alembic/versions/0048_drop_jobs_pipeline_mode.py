"""drop jobs.pipeline_mode column

Revision ID: 0048_drop_jobs_pipeline_mode
Revises: 0047_add_task_board_improvements_job_type
Create Date: 2026-05-03 12:00:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0048_drop_jobs_pipeline_mode"
down_revision = "0047_add_task_board_improvements_job_type"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_column("jobs", "pipeline_mode")


def downgrade() -> None:
    op.add_column("jobs", sa.Column("pipeline_mode", sa.Text(), nullable=False, server_default="legacy"))
    op.alter_column("jobs", "pipeline_mode", server_default=None)
