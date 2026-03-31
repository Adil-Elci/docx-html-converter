"""convert workflow flags to array

Revision ID: 0043_convert_workflow_flags_to_array
Revises: 0042_add_adil_workflow_flag
Create Date: 2026-03-31 14:20:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "0043_convert_workflow_flags_to_array"
down_revision = "0042_add_adil_workflow_flag"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "workflow_cards",
        sa.Column(
            "flag_types",
            postgresql.ARRAY(sa.Text()),
            nullable=False,
            server_default=sa.text("ARRAY[]::text[]"),
        ),
    )
    op.execute(
        """
        UPDATE workflow_cards
        SET flag_types = CASE
          WHEN flag_type IS NULL OR btrim(flag_type) = '' THEN ARRAY[]::text[]
          ELSE ARRAY[flag_type]
        END
        """
    )
    op.drop_constraint("workflow_cards_flag_type_check", "workflow_cards", type_="check")
    op.drop_column("workflow_cards", "flag_type")
    op.create_check_constraint(
        "workflow_cards_flag_types_check",
        "workflow_cards",
        "flag_types <@ ARRAY['bug','needs_levent_attention','needs_adil_attention']::text[]",
    )
    op.alter_column("workflow_cards", "flag_types", server_default=None)


def downgrade() -> None:
    op.add_column("workflow_cards", sa.Column("flag_type", sa.Text(), nullable=True))
    op.execute(
        """
        UPDATE workflow_cards
        SET flag_type = CASE
          WHEN array_position(flag_types, 'bug') IS NOT NULL THEN 'bug'
          WHEN array_position(flag_types, 'needs_levent_attention') IS NOT NULL THEN 'needs_levent_attention'
          WHEN array_position(flag_types, 'needs_adil_attention') IS NOT NULL THEN 'needs_adil_attention'
          ELSE NULL
        END
        """
    )
    op.drop_constraint("workflow_cards_flag_types_check", "workflow_cards", type_="check")
    op.drop_column("workflow_cards", "flag_types")
    op.create_check_constraint(
        "workflow_cards_flag_type_check",
        "workflow_cards",
        "flag_type IN ('bug','needs_levent_attention','needs_adil_attention') OR flag_type IS NULL",
    )
