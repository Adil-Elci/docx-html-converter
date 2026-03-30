"""add workflow priority and assignee

Revision ID: 0041_add_workflow_priority_and_assignee
Revises: 0040_reset_workflow_cards_manual_only
Create Date: 2026-03-30 22:10:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0041_add_workflow_priority_and_assignee"
down_revision = "0040_reset_workflow_cards_manual_only"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "workflow_cards",
        sa.Column("priority", sa.Text(), nullable=False, server_default="medium"),
    )
    op.add_column(
        "workflow_cards",
        sa.Column("assignee_user_id", sa.UUID(), nullable=True),
    )
    op.create_foreign_key(
        "workflow_cards_assignee_user_id_fkey",
        "workflow_cards",
        "users",
        ["assignee_user_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.execute(
        """
        UPDATE workflow_cards
        SET assignee_user_id = created_by_user_id
        WHERE assignee_user_id IS NULL AND created_by_user_id IS NOT NULL
        """
    )
    op.execute(
        """
        UPDATE workflow_cards
        SET job_type = 'develop'
        WHERE job_type = 'build'
        """
    )
    op.drop_constraint("workflow_cards_job_type_check", "workflow_cards", type_="check")
    op.create_check_constraint(
        "workflow_cards_job_type_check",
        "workflow_cards",
        "job_type IN ('articles','develop','fix','research')",
    )
    op.create_check_constraint(
        "workflow_cards_priority_check",
        "workflow_cards",
        "priority IN ('urgent','high','medium','low')",
    )
    op.alter_column("workflow_cards", "priority", server_default=None)


def downgrade() -> None:
    op.drop_constraint("workflow_cards_priority_check", "workflow_cards", type_="check")
    op.drop_constraint("workflow_cards_job_type_check", "workflow_cards", type_="check")
    op.create_check_constraint(
        "workflow_cards_job_type_check",
        "workflow_cards",
        "job_type IN ('articles','develop','fix','build')",
    )
    op.execute(
        """
        UPDATE workflow_cards
        SET job_type = 'build'
        WHERE job_type = 'research'
        """
    )
    op.drop_constraint("workflow_cards_assignee_user_id_fkey", "workflow_cards", type_="foreignkey")
    op.drop_column("workflow_cards", "assignee_user_id")
    op.drop_column("workflow_cards", "priority")
