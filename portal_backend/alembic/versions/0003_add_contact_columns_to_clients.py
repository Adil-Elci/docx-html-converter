"""add contact columns to clients

Revision ID: 0003_contact_cols_clients
Revises: 0002_hosting_cols_sites
Create Date: 2026-02-16 00:00:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0003_contact_cols_clients"
down_revision = "0002_hosting_cols_sites"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("clients", sa.Column("email", sa.Text(), nullable=True))
    op.add_column("clients", sa.Column("phone_number", sa.Text(), nullable=True))


def downgrade() -> None:
    op.drop_column("clients", "phone_number")
    op.drop_column("clients", "email")
