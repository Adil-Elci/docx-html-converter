"""add creator prompt trace columns

Revision ID: 0032_add_creator_prompt_trace_columns
Revises: 0031_add_client_publish_notifications_flag
Create Date: 2026-03-11 16:10:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "0032_add_creator_prompt_trace_columns"
down_revision = "0031_add_client_publish_notifications_flag"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "creator_outputs",
        sa.Column(
            "planner_trace",
            postgresql.JSONB(),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
    )
    op.add_column(
        "creator_outputs",
        sa.Column(
            "writer_prompt_trace",
            postgresql.JSONB(),
            nullable=False,
            server_default=sa.text("'[]'::jsonb"),
        ),
    )

    connection = op.get_bind()
    connection.execute(
        sa.text(
            """
            UPDATE creator_outputs
            SET planner_trace = COALESCE(payload->'debug'->'prompt_trace'->'planner', '{}'::jsonb),
                writer_prompt_trace = COALESCE(payload->'debug'->'prompt_trace'->'writer_attempts', '[]'::jsonb)
            """
        )
    )


def downgrade() -> None:
    op.drop_column("creator_outputs", "writer_prompt_trace")
    op.drop_column("creator_outputs", "planner_trace")
