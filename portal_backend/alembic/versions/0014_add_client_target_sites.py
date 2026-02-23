"""add client target sites table

Revision ID: 0014_client_target_sites
Revises: 0013_publishing_sites_rename
Create Date: 2026-02-23 00:00:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "0014_client_target_sites"
down_revision = "0013_publishing_sites_rename"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "client_target_sites",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("client_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("target_site_domain", sa.Text(), nullable=True),
        sa.Column("target_site_url", sa.Text(), nullable=True),
        sa.Column("is_primary", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.CheckConstraint(
            "(target_site_domain IS NOT NULL) OR (target_site_url IS NOT NULL)",
            name="client_target_sites_target_required_check",
        ),
        sa.ForeignKeyConstraint(["client_id"], ["clients.id"], ondelete="CASCADE"),
        sa.UniqueConstraint(
            "client_id",
            "target_site_domain",
            "target_site_url",
            name="client_target_sites_client_target_unique",
        ),
    )

    op.create_index("idx_client_target_sites_client_id", "client_target_sites", ["client_id"])
    op.create_index("idx_client_target_sites_is_primary", "client_target_sites", ["is_primary"])

    op.execute(
        """
        CREATE TRIGGER trg_client_target_sites_set_updated_at
        BEFORE UPDATE ON client_target_sites
        FOR EACH ROW
        EXECUTE FUNCTION set_updated_at();
        """
    )

    op.execute(
        """
        INSERT INTO client_target_sites (client_id, target_site_domain, target_site_url, is_primary)
        SELECT
            c.id,
            NULLIF(BTRIM(c.primary_domain), ''),
            NULLIF(BTRIM(c.backlink_url), ''),
            TRUE
        FROM clients AS c
        WHERE NULLIF(BTRIM(c.primary_domain), '') IS NOT NULL
           OR NULLIF(BTRIM(c.backlink_url), '') IS NOT NULL;
        """
    )


def downgrade() -> None:
    op.execute("DROP TRIGGER IF EXISTS trg_client_target_sites_set_updated_at ON client_target_sites;")
    op.drop_index("idx_client_target_sites_is_primary", table_name="client_target_sites")
    op.drop_index("idx_client_target_sites_client_id", table_name="client_target_sites")
    op.drop_table("client_target_sites")
