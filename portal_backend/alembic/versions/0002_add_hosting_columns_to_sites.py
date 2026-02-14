"""add hosting columns to sites

Revision ID: 0002_add_hosting_columns_to_sites
Revises: 0001_initial_schema
Create Date: 2026-02-14 00:00:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0002_add_hosting_columns_to_sites"
down_revision = "0001_initial_schema"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("sites", sa.Column("hosting_provider", sa.Text(), nullable=True))
    op.add_column("sites", sa.Column("hosting_panel", sa.Text(), nullable=True))


def downgrade() -> None:
    op.drop_column("sites", "hosting_panel")
    op.drop_column("sites", "hosting_provider")
