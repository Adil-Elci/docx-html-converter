"""reset workflow cards for manual-only board

Revision ID: 0040_reset_workflow_cards_manual_only
Revises: 0039_add_workflow_card_flags_and_job_types
Create Date: 2026-03-30 21:40:00.000000
"""

from __future__ import annotations

from alembic import op


revision = "0040_reset_workflow_cards_manual_only"
down_revision = "0039_add_workflow_card_flags_and_job_types"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("DELETE FROM workflow_card_comments")
    op.execute("DELETE FROM workflow_card_events")
    op.execute("DELETE FROM workflow_cards")


def downgrade() -> None:
    pass
