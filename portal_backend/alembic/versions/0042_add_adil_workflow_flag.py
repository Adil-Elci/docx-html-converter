"""add adil workflow flag

Revision ID: 0042_add_adil_workflow_flag
Revises: 0041_add_workflow_priority_and_assignee
Create Date: 2026-03-31 11:45:00.000000
"""

from __future__ import annotations

from alembic import op


revision = "0042_add_adil_workflow_flag"
down_revision = "0041_add_workflow_priority_and_assignee"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_constraint("workflow_cards_flag_type_check", "workflow_cards", type_="check")
    op.create_check_constraint(
        "workflow_cards_flag_type_check",
        "workflow_cards",
        "flag_type IN ('bug','needs_levent_attention','needs_adil_attention') OR flag_type IS NULL",
    )


def downgrade() -> None:
    op.drop_constraint("workflow_cards_flag_type_check", "workflow_cards", type_="check")
    op.create_check_constraint(
        "workflow_cards_flag_type_check",
        "workflow_cards",
        "flag_type IN ('bug','needs_levent_attention') OR flag_type IS NULL",
    )
