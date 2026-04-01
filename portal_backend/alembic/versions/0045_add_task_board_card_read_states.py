"""add task board card read states

Revision ID: 0045_add_task_board_card_read_states
Revises: 0044_rename_workflow_tables_to_task_board
Create Date: 2026-04-01 14:20:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "0045_add_task_board_card_read_states"
down_revision = "0044_rename_workflow_tables_to_task_board"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "task_board_card_read_states",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("card_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("user_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["card_id"], ["task_board_cards.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id", name="task_board_card_read_states_pkey"),
        sa.UniqueConstraint("card_id", "user_id", name="task_board_card_read_states_card_user_unique"),
    )


def downgrade() -> None:
    op.drop_table("task_board_card_read_states")
