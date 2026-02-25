"""master site info table and site column renames

Revision ID: 0016_master_site_info
Revises: 0015_manual_orders_no_doc
Create Date: 2026-02-24 00:00:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "0016_master_site_info"
down_revision = "0015_manual_orders_no_doc"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Rename publishing_sites columns
    op.alter_column("publishing_sites", "hosting_provider", new_column_name="hosted_by")
    op.alter_column("publishing_sites", "hosting_panel", new_column_name="host_panel")

    # Create master_site_info source-of-truth snapshot table for file-driven sync
    op.create_table(
        "master_site_info",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("publishing_site_url", sa.Text(), nullable=False),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("wp_rest_base", sa.Text(), nullable=False, server_default=sa.text("'/wp-json/wp/v2'")),
        sa.Column("hosted_by", sa.Text(), nullable=True),
        sa.Column("host_panel", sa.Text(), nullable=True),
        sa.Column("status", sa.Text(), nullable=False, server_default=sa.text("'active'")),
        sa.Column("auth_type", sa.Text(), nullable=False, server_default=sa.text("'application_password'")),
        sa.Column("wp_username", sa.Text(), nullable=True),
        sa.Column("wp_app_password", sa.Text(), nullable=True),
        sa.Column("enabled", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.CheckConstraint("status IN ('active','inactive')", name="master_site_info_status_check"),
        sa.CheckConstraint("auth_type IN ('application_password')", name="master_site_info_auth_type_check"),
        sa.UniqueConstraint("publishing_site_url", name="master_site_info_publishing_site_url_key"),
    )
    op.create_index("idx_master_site_info_status", "master_site_info", ["status"])
    op.create_index("idx_master_site_info_enabled", "master_site_info", ["enabled"])
    op.execute(
        """
        CREATE TRIGGER trg_master_site_info_set_updated_at
        BEFORE UPDATE ON master_site_info
        FOR EACH ROW
        EXECUTE FUNCTION set_updated_at();
        """
    )

    # Deduplicate credentials to one row per publishing site, keep latest/preferred row.
    op.execute(
        """
        DELETE FROM publishing_site_credentials psc
        USING (
          SELECT id
          FROM (
            SELECT
              id,
              ROW_NUMBER() OVER (
                PARTITION BY publishing_site_id
                ORDER BY enabled DESC, COALESCE(updated_at, created_at) DESC, created_at DESC, id DESC
              ) AS rn
            FROM publishing_site_credentials
          ) ranked
          WHERE rn > 1
        ) d
        WHERE psc.id = d.id;
        """
    )

    # Enforce one credential per site.
    op.drop_constraint("publishing_site_credentials_site_username_unique", "publishing_site_credentials", type_="unique")
    op.create_unique_constraint("publishing_site_credentials_site_unique", "publishing_site_credentials", ["publishing_site_id"])

    # Seed master table from existing publishing_sites + credentials
    op.execute(
        """
        INSERT INTO master_site_info (
          publishing_site_url,
          name,
          wp_rest_base,
          hosted_by,
          host_panel,
          status,
          auth_type,
          wp_username,
          wp_app_password,
          enabled
        )
        SELECT
          ps.publishing_site_url,
          COALESCE(NULLIF(BTRIM(ps.name), ''), regexp_replace(regexp_replace(ps.publishing_site_url, '^https?://', ''), '/.*$', '')),
          COALESCE(NULLIF(BTRIM(ps.wp_rest_base), ''), '/wp-json/wp/v2'),
          ps.hosted_by,
          ps.host_panel,
          COALESCE(ps.status, 'active'),
          COALESCE(psc.auth_type, 'application_password'),
          psc.wp_username,
          psc.wp_app_password,
          COALESCE(psc.enabled, true)
        FROM publishing_sites ps
        LEFT JOIN publishing_site_credentials psc
          ON psc.publishing_site_id = ps.id
        ON CONFLICT (publishing_site_url) DO NOTHING;
        """
    )


def downgrade() -> None:
    op.drop_constraint("publishing_site_credentials_site_unique", "publishing_site_credentials", type_="unique")
    op.create_unique_constraint(
        "publishing_site_credentials_site_username_unique",
        "publishing_site_credentials",
        ["publishing_site_id", "wp_username"],
    )

    op.execute("DROP TRIGGER IF EXISTS trg_master_site_info_set_updated_at ON master_site_info;")
    op.drop_index("idx_master_site_info_enabled", table_name="master_site_info")
    op.drop_index("idx_master_site_info_status", table_name="master_site_info")
    op.drop_table("master_site_info")

    op.alter_column("publishing_sites", "host_panel", new_column_name="hosting_panel")
    op.alter_column("publishing_sites", "hosted_by", new_column_name="hosting_provider")
