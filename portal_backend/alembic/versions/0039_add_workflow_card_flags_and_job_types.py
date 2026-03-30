"""add workflow card flags and job types

Revision ID: 0039_add_workflow_card_flags_and_job_types
Revises: 0038_add_workflow_manual_cards_and_comments
Create Date: 2026-03-30 19:05:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0039_add_workflow_card_flags_and_job_types"
down_revision = "0038_add_workflow_manual_cards_and_comments"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("workflow_cards", sa.Column("job_type", sa.Text(), nullable=True))
    op.add_column("workflow_cards", sa.Column("flag_type", sa.Text(), nullable=True))
    op.execute(
        """
        UPDATE workflow_cards
        SET job_type = CASE
          WHEN card_kind = 'job' THEN 'articles'
          WHEN job_type IS NULL THEN 'build'
          ELSE job_type
        END
        """
    )
    op.create_check_constraint(
        "workflow_cards_job_type_check",
        "workflow_cards",
        "job_type IN ('articles','develop','fix','build')",
    )
    op.create_check_constraint(
        "workflow_cards_flag_type_check",
        "workflow_cards",
        "flag_type IN ('bug','needs_levent_attention') OR flag_type IS NULL",
    )


def downgrade() -> None:
    op.drop_constraint("workflow_cards_flag_type_check", "workflow_cards", type_="check")
    op.drop_constraint("workflow_cards_job_type_check", "workflow_cards", type_="check")
    op.drop_column("workflow_cards", "flag_type")
    op.drop_column("workflow_cards", "job_type")
