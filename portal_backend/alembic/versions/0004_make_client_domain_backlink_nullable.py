"""make client domain/backlink nullable

Revision ID: 0004_client_domain_backlink_nullable
Revises: 0003_contact_cols_clients
Create Date: 2026-02-16 00:00:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0004_client_domain_backlink_nullable"
down_revision = "0003_contact_cols_clients"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column("clients", "primary_domain", existing_type=sa.Text(), nullable=True)
    op.alter_column("clients", "backlink_url", existing_type=sa.Text(), nullable=True)


def downgrade() -> None:
    op.execute("UPDATE clients SET primary_domain = '' WHERE primary_domain IS NULL;")
    op.execute("UPDATE clients SET backlink_url = '' WHERE backlink_url IS NULL;")
    op.alter_column("clients", "primary_domain", existing_type=sa.Text(), nullable=False)
    op.alter_column("clients", "backlink_url", existing_type=sa.Text(), nullable=False)
