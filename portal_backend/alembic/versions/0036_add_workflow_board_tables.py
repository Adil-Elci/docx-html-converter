"""add workflow board tables

Revision ID: 0036_add_workflow_board_tables
Revises: 0035_add_creator_output_execution_traces
Create Date: 2026-03-30 12:00:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "0036_add_workflow_board_tables"
down_revision = "0035_add_creator_output_execution_traces"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "workflow_columns",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("column_key", sa.Text(), nullable=False),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("color", sa.Text(), nullable=True),
        sa.Column("position", sa.Integer(), nullable=False, server_default="100"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("column_key", name="workflow_columns_key_unique"),
    )
    op.create_table(
        "workflow_cards",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("job_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("submission_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("client_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("publishing_site_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("column_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("column_source", sa.Text(), nullable=False, server_default="auto"),
        sa.Column("position", sa.Integer(), nullable=False, server_default="1000"),
        sa.Column("title_snapshot", sa.Text(), nullable=True),
        sa.Column("request_kind_snapshot", sa.Text(), nullable=True),
        sa.Column("job_status_snapshot", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.CheckConstraint("column_source IN ('auto','manual')", name="workflow_cards_column_source_check"),
        sa.ForeignKeyConstraint(["client_id"], ["clients.id"]),
        sa.ForeignKeyConstraint(["column_id"], ["workflow_columns.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["job_id"], ["jobs.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["publishing_site_id"], ["publishing_sites.id"]),
        sa.ForeignKeyConstraint(["submission_id"], ["submissions.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("job_id", name="workflow_cards_job_unique"),
    )
    op.create_table(
        "workflow_card_events",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("card_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("job_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("actor_user_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("event_type", sa.Text(), nullable=False),
        sa.Column("from_column_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("to_column_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column(
            "payload",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.CheckConstraint(
            "event_type IN ('created','moved','auto_synced')",
            name="workflow_card_events_type_check",
        ),
        sa.ForeignKeyConstraint(["actor_user_id"], ["users.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["card_id"], ["workflow_cards.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["from_column_id"], ["workflow_columns.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["job_id"], ["jobs.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["to_column_id"], ["workflow_columns.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.execute(
        """
        INSERT INTO workflow_columns (id, column_key, name, color, position)
        VALUES
          ('9cccbf64-75fe-47c6-970d-7c2b78f6d8a1', 'backlog', 'Backlog', '#7c8aa5', 100),
          ('65f68ac6-75b9-4f8b-b2b2-c170a8308f17', 'in_progress', 'In Progress', '#2d7ff9', 200),
          ('45502f65-2d6d-46ff-a15f-f29d741907d6', 'pending_review', 'Pending Review', '#b7791f', 300),
          ('38dd53cf-f7dd-4928-ad94-aaea3db10ece', 'blocked', 'Blocked', '#c53030', 400),
          ('e4796885-7d3a-4c57-9854-7d4790f85e58', 'done', 'Done', '#2f855a', 500)
        """
    )
    op.alter_column("workflow_columns", "position", server_default=None)
    op.alter_column("workflow_cards", "column_source", server_default=None)
    op.alter_column("workflow_cards", "position", server_default=None)
    op.alter_column("workflow_card_events", "payload", server_default=None)


def downgrade() -> None:
    op.drop_table("workflow_card_events")
    op.drop_table("workflow_cards")
    op.drop_table("workflow_columns")
