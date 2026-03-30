"""add workflow manual cards and comments

Revision ID: 0038_add_workflow_manual_cards_and_comments
Revises: 0037_add_super_admin_role
Create Date: 2026-03-30 18:10:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "0038_add_workflow_manual_cards_and_comments"
down_revision = "0037_add_super_admin_role"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("workflow_cards", sa.Column("card_kind", sa.Text(), nullable=False, server_default="job"))
    op.add_column("workflow_cards", sa.Column("description", sa.Text(), nullable=True))
    op.add_column("workflow_cards", sa.Column("created_by_user_id", postgresql.UUID(as_uuid=True), nullable=True))
    op.add_column("workflow_cards", sa.Column("created_by_name_snapshot", sa.Text(), nullable=True))
    op.create_foreign_key(
        "workflow_cards_created_by_user_id_fkey",
        "workflow_cards",
        "users",
        ["created_by_user_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.alter_column("workflow_cards", "job_id", existing_type=postgresql.UUID(as_uuid=True), nullable=True)
    op.alter_column("workflow_cards", "submission_id", existing_type=postgresql.UUID(as_uuid=True), nullable=True)
    op.alter_column("workflow_cards", "client_id", existing_type=postgresql.UUID(as_uuid=True), nullable=True)
    op.alter_column("workflow_cards", "publishing_site_id", existing_type=postgresql.UUID(as_uuid=True), nullable=True)
    op.execute("UPDATE workflow_cards SET card_kind = 'job' WHERE card_kind IS NULL")
    op.alter_column("workflow_cards", "card_kind", server_default=None)
    op.create_check_constraint(
        "workflow_cards_kind_check",
        "workflow_cards",
        "card_kind IN ('job','manual')",
    )

    op.alter_column("workflow_card_events", "job_id", existing_type=postgresql.UUID(as_uuid=True), nullable=True)
    op.drop_constraint("workflow_card_events_type_check", "workflow_card_events", type_="check")
    op.create_check_constraint(
        "workflow_card_events_type_check",
        "workflow_card_events",
        "event_type IN ('created','manual_created','moved','auto_synced','comment_added','comment_updated')",
    )

    op.create_table(
        "workflow_card_comments",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("card_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("author_user_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("author_name_snapshot", sa.Text(), nullable=False),
        sa.Column("body", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.ForeignKeyConstraint(["author_user_id"], ["users.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["card_id"], ["workflow_cards.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.alter_column("workflow_card_comments", "created_at", server_default=None)
    op.alter_column("workflow_card_comments", "updated_at", server_default=None)

    op.execute(
        """
        UPDATE workflow_columns
        SET name = 'TO DO', color = '#5e6c84', position = 100, updated_at = now()
        WHERE column_key = 'backlog'
        """
    )
    op.execute("UPDATE workflow_columns SET column_key = 'todo' WHERE column_key = 'backlog'")
    op.execute(
        """
        UPDATE workflow_columns
        SET name = 'IN PROGRESS', color = '#0c66e4', position = 200, updated_at = now()
        WHERE column_key = 'in_progress'
        """
    )
    op.execute(
        """
        UPDATE workflow_columns
        SET name = 'DONE', color = '#1f845a', position = 300, updated_at = now()
        WHERE column_key = 'done'
        """
    )

    op.execute(
        """
        UPDATE workflow_cards
        SET column_id = (SELECT id FROM workflow_columns WHERE column_key = 'in_progress' LIMIT 1),
            updated_at = now()
        WHERE column_id IN (SELECT id FROM workflow_columns WHERE column_key = 'pending_review')
        """
    )
    op.execute(
        """
        UPDATE workflow_cards
        SET column_id = (SELECT id FROM workflow_columns WHERE column_key = 'todo' LIMIT 1),
            updated_at = now()
        WHERE column_id IN (SELECT id FROM workflow_columns WHERE column_key = 'blocked')
        """
    )
    op.execute(
        """
        UPDATE workflow_card_events
        SET from_column_id = (SELECT id FROM workflow_columns WHERE column_key = 'in_progress' LIMIT 1)
        WHERE from_column_id IN (SELECT id FROM workflow_columns WHERE column_key = 'pending_review')
        """
    )
    op.execute(
        """
        UPDATE workflow_card_events
        SET to_column_id = (SELECT id FROM workflow_columns WHERE column_key = 'in_progress' LIMIT 1)
        WHERE to_column_id IN (SELECT id FROM workflow_columns WHERE column_key = 'pending_review')
        """
    )
    op.execute(
        """
        UPDATE workflow_card_events
        SET from_column_id = (SELECT id FROM workflow_columns WHERE column_key = 'todo' LIMIT 1)
        WHERE from_column_id IN (SELECT id FROM workflow_columns WHERE column_key = 'blocked')
        """
    )
    op.execute(
        """
        UPDATE workflow_card_events
        SET to_column_id = (SELECT id FROM workflow_columns WHERE column_key = 'todo' LIMIT 1)
        WHERE to_column_id IN (SELECT id FROM workflow_columns WHERE column_key = 'blocked')
        """
    )
    op.execute("DELETE FROM workflow_columns WHERE column_key IN ('pending_review', 'blocked')")


def downgrade() -> None:
    op.execute(
        """
        INSERT INTO workflow_columns (id, column_key, name, color, position, created_at, updated_at)
        SELECT gen_random_uuid(), 'pending_review', 'Pending Review', '#b7791f', 300, now(), now()
        WHERE NOT EXISTS (SELECT 1 FROM workflow_columns WHERE column_key = 'pending_review')
        """
    )
    op.execute(
        """
        INSERT INTO workflow_columns (id, column_key, name, color, position, created_at, updated_at)
        SELECT gen_random_uuid(), 'blocked', 'Blocked', '#c53030', 400, now(), now()
        WHERE NOT EXISTS (SELECT 1 FROM workflow_columns WHERE column_key = 'blocked')
        """
    )
    op.execute(
        """
        UPDATE workflow_columns
        SET name = 'Backlog', color = '#7c8aa5', position = 100, updated_at = now()
        WHERE column_key = 'todo'
        """
    )
    op.execute("UPDATE workflow_columns SET column_key = 'backlog' WHERE column_key = 'todo'")
    op.execute(
        """
        UPDATE workflow_columns
        SET name = 'In Progress', color = '#2d7ff9', position = 200, updated_at = now()
        WHERE column_key = 'in_progress'
        """
    )
    op.execute(
        """
        UPDATE workflow_columns
        SET name = 'Done', color = '#2f855a', position = 500, updated_at = now()
        WHERE column_key = 'done'
        """
    )

    op.execute("DELETE FROM workflow_card_events WHERE event_type IN ('manual_created', 'comment_added', 'comment_updated')")
    op.execute("DELETE FROM workflow_cards WHERE card_kind = 'manual'")
    op.drop_table("workflow_card_comments")

    op.drop_constraint("workflow_card_events_type_check", "workflow_card_events", type_="check")
    op.create_check_constraint(
        "workflow_card_events_type_check",
        "workflow_card_events",
        "event_type IN ('created','moved','auto_synced')",
    )
    op.alter_column("workflow_card_events", "job_id", existing_type=postgresql.UUID(as_uuid=True), nullable=False)

    op.drop_constraint("workflow_cards_kind_check", "workflow_cards", type_="check")
    op.drop_constraint("workflow_cards_created_by_user_id_fkey", "workflow_cards", type_="foreignkey")
    op.alter_column("workflow_cards", "publishing_site_id", existing_type=postgresql.UUID(as_uuid=True), nullable=False)
    op.alter_column("workflow_cards", "client_id", existing_type=postgresql.UUID(as_uuid=True), nullable=False)
    op.alter_column("workflow_cards", "submission_id", existing_type=postgresql.UUID(as_uuid=True), nullable=False)
    op.alter_column("workflow_cards", "job_id", existing_type=postgresql.UUID(as_uuid=True), nullable=False)
    op.drop_column("workflow_cards", "created_by_name_snapshot")
    op.drop_column("workflow_cards", "created_by_user_id")
    op.drop_column("workflow_cards", "description")
    op.drop_column("workflow_cards", "card_kind")
