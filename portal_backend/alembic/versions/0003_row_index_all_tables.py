"""add row_index to all tables

Revision ID: 0003_row_index_all_tables
Revises: 0002_hosting_cols_sites
Create Date: 2026-02-16 00:00:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0003_row_index_all_tables"
down_revision = "0002_hosting_cols_sites"
branch_labels = None
depends_on = None


TABLES = [
    "clients",
    "sites",
    "site_credentials",
    "client_site_access",
    "submissions",
    "jobs",
    "job_events",
    "assets",
]


def _add_row_index(table_name: str) -> None:
    seq_name = f"{table_name}_row_index_seq"
    index_name = f"idx_{table_name}_row_index"

    op.add_column(table_name, sa.Column("row_index", sa.BigInteger(), nullable=True))
    op.execute(f"CREATE SEQUENCE IF NOT EXISTS {seq_name}")
    op.execute(f"ALTER SEQUENCE {seq_name} OWNED BY {table_name}.row_index")
    op.execute(f"ALTER TABLE {table_name} ALTER COLUMN row_index SET DEFAULT nextval('{seq_name}')")
    op.execute(f"UPDATE {table_name} SET row_index = nextval('{seq_name}') WHERE row_index IS NULL")
    op.execute(
        f"""
        SELECT setval(
            '{seq_name}',
            COALESCE((SELECT MAX(row_index) FROM {table_name}), 1),
            (SELECT COUNT(*) > 0 FROM {table_name})
        )
        """
    )
    op.alter_column(table_name, "row_index", nullable=False)
    op.create_index(index_name, table_name, ["row_index"], unique=True)


def _drop_row_index(table_name: str) -> None:
    seq_name = f"{table_name}_row_index_seq"
    index_name = f"idx_{table_name}_row_index"

    op.drop_index(index_name, table_name=table_name)
    op.drop_column(table_name, "row_index")
    op.execute(f"DROP SEQUENCE IF EXISTS {seq_name}")


def upgrade() -> None:
    for table_name in TABLES:
        _add_row_index(table_name)


def downgrade() -> None:
    for table_name in reversed(TABLES):
        _drop_row_index(table_name)
