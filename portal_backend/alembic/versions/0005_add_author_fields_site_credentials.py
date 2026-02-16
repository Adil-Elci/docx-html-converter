"""add author fields to site_credentials

Revision ID: 0005_sitecred_author_fields
Revises: 0004_client_domain_nullable
Create Date: 2026-02-16 00:00:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0005_sitecred_author_fields"
down_revision = "0004_client_domain_nullable"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("site_credentials", sa.Column("author_name", sa.Text(), nullable=True))
    op.add_column("site_credentials", sa.Column("author_id", sa.BigInteger(), nullable=True))


def downgrade() -> None:
    op.drop_column("site_credentials", "author_id")
    op.drop_column("site_credentials", "author_name")
