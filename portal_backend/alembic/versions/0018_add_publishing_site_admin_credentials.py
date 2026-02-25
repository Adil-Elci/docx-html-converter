"""add publishing site admin credentials table

Revision ID: 0018_site_admin_credentials
Revises: 0017_db_updater_sync_jobs
Create Date: 2026-02-25 00:00:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "0018_site_admin_credentials"
down_revision = "0017_db_updater_sync_jobs"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("master_site_info", sa.Column("wp_admin_login_url", sa.Text(), nullable=True))
    op.add_column("master_site_info", sa.Column("wp_admin_username", sa.Text(), nullable=True))
    op.add_column("master_site_info", sa.Column("wp_admin_password", sa.Text(), nullable=True))
    op.add_column(
        "master_site_info",
        sa.Column("wp_admin_enabled", sa.Boolean(), nullable=False, server_default=sa.text("true")),
    )

    op.create_table(
        "publishing_site_admin_credentials",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column(
            "publishing_site_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("publishing_sites.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("wp_admin_login_url", sa.Text(), nullable=True),
        sa.Column("wp_admin_username", sa.Text(), nullable=False),
        sa.Column("wp_admin_password", sa.Text(), nullable=False),
        sa.Column("enabled", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.UniqueConstraint("publishing_site_id", name="publishing_site_admin_credentials_site_unique"),
    )
    op.create_index(
        "idx_publishing_site_admin_credentials_enabled",
        "publishing_site_admin_credentials",
        ["enabled"],
    )
    op.execute(
        """
        CREATE TRIGGER trg_publishing_site_admin_credentials_set_updated_at
        BEFORE UPDATE ON publishing_site_admin_credentials
        FOR EACH ROW
        EXECUTE FUNCTION set_updated_at();
        """
    )


def downgrade() -> None:
    op.execute(
        "DROP TRIGGER IF EXISTS trg_publishing_site_admin_credentials_set_updated_at ON publishing_site_admin_credentials;"
    )
    op.drop_index("idx_publishing_site_admin_credentials_enabled", table_name="publishing_site_admin_credentials")
    op.drop_table("publishing_site_admin_credentials")

    op.drop_column("master_site_info", "wp_admin_enabled")
    op.drop_column("master_site_info", "wp_admin_password")
    op.drop_column("master_site_info", "wp_admin_username")
    op.drop_column("master_site_info", "wp_admin_login_url")

