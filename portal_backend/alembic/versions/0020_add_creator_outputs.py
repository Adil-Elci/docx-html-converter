"""add creator_outputs table

Revision ID: 0020_creator_outputs
Revises: 0019_publishing_site_name_col
Create Date: 2026-02-26 00:00:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "0020_creator_outputs"
down_revision = "0019_publishing_site_name_col"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "creator_outputs",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("submission_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("job_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("client_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("publishing_site_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("target_site_url", sa.Text(), nullable=False),
        sa.Column("host_site_url", sa.Text(), nullable=False),
        sa.Column("payload", postgresql.JSONB(), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["submission_id"], ["submissions.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["job_id"], ["jobs.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["client_id"], ["clients.id"]),
        sa.ForeignKeyConstraint(["publishing_site_id"], ["publishing_sites.id"]),
    )


def downgrade() -> None:
    op.drop_table("creator_outputs")
