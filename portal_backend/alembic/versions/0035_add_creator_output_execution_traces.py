"""add creator output execution traces

Revision ID: 0035_add_creator_output_execution_traces
Revises: 0034_add_creator_output_draft_article_html
Create Date: 2026-03-12 17:20:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision = "0035_add_creator_output_execution_traces"
down_revision = "0034_add_creator_output_draft_article_html"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "creator_outputs",
        sa.Column(
            "creator_trace",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'[]'::jsonb"),
        ),
    )
    op.add_column(
        "creator_outputs",
        sa.Column(
            "backend_trace",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'[]'::jsonb"),
        ),
    )
    op.execute(
        """
        UPDATE creator_outputs
        SET
            creator_trace = COALESCE(payload #> '{debug,creator_trace}', '[]'::jsonb),
            backend_trace = COALESCE(payload #> '{debug,backend_trace}', '[]'::jsonb)
        """
    )
    op.alter_column("creator_outputs", "creator_trace", server_default=None)
    op.alter_column("creator_outputs", "backend_trace", server_default=None)


def downgrade() -> None:
    op.drop_column("creator_outputs", "backend_trace")
    op.drop_column("creator_outputs", "creator_trace")
