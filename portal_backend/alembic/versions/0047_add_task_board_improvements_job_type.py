"""add task board improvements job type

Revision ID: 0047_add_task_board_improvements_job_type
Revises: 0046_add_4llm_pipeline_tables
Create Date: 2026-04-22 11:10:00.000000
"""

from __future__ import annotations

from alembic import op


revision = "0047_add_task_board_improvements_job_type"
down_revision = "0046_add_4llm_pipeline_tables"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_constraint("task_board_cards_job_type_check", "task_board_cards", type_="check")
    op.create_check_constraint(
        "task_board_cards_job_type_check",
        "task_board_cards",
        "job_type IN ('articles','develop','fix','research','improvements')",
    )


def downgrade() -> None:
    op.drop_constraint("task_board_cards_job_type_check", "task_board_cards", type_="check")
    op.create_check_constraint(
        "task_board_cards_job_type_check",
        "task_board_cards",
        "job_type IN ('articles','develop','fix','research')",
    )
