"""drop client publishing site access table

Revision ID: 0033_drop_client_publishing_site_access
Revises: 0032_add_creator_prompt_trace_columns
Create Date: 2026-03-12 12:30:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "0033_drop_client_publishing_site_access"
down_revision = "0032_add_creator_prompt_trace_columns"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_table("client_publishing_site_access")


def downgrade() -> None:
    op.create_table(
        "client_publishing_site_access",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("client_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("publishing_site_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("enabled", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["client_id"], ["clients.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["publishing_site_id"], ["publishing_sites.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "client_id",
            "publishing_site_id",
            name="client_publishing_site_access_client_publishing_site_unique",
        ),
    )
