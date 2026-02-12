"""initial schema

Revision ID: 0001_initial_schema
Revises:
Create Date: 2026-02-12 00:00:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "0001_initial_schema"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto;")

    op.create_table(
        "clients",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("primary_domain", sa.Text(), nullable=False),
        sa.Column("backlink_url", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.CheckConstraint("status IN ('active', 'inactive')", name="clients_status_check"),
    )

    op.create_table(
        "sites",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("site_url", sa.Text(), nullable=False),
        sa.Column("wp_rest_base", sa.Text(), nullable=False, server_default=sa.text("'/wp-json/wp/v2'")),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.CheckConstraint("status IN ('active', 'inactive')", name="sites_status_check"),
        sa.UniqueConstraint("site_url", name="sites_site_url_key"),
    )

    op.create_table(
        "site_credentials",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("site_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("auth_type", sa.Text(), nullable=False),
        sa.Column("wp_username", sa.Text(), nullable=False),
        sa.Column("wp_app_password", sa.Text(), nullable=False),
        sa.Column("enabled", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.CheckConstraint("auth_type IN ('application_password')", name="site_credentials_auth_type_check"),
        sa.ForeignKeyConstraint(["site_id"], ["sites.id"], ondelete="CASCADE"),
        sa.UniqueConstraint("site_id", "wp_username", name="site_credentials_site_username_unique"),
    )

    op.create_table(
        "client_site_access",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("client_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("site_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("enabled", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.ForeignKeyConstraint(["client_id"], ["clients.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["site_id"], ["sites.id"], ondelete="CASCADE"),
        sa.UniqueConstraint("client_id", "site_id", name="client_site_access_client_site_unique"),
    )

    op.create_table(
        "submissions",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("client_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("site_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("source_type", sa.Text(), nullable=False),
        sa.Column("doc_url", sa.Text(), nullable=True),
        sa.Column("file_url", sa.Text(), nullable=True),
        sa.Column("backlink_placement", sa.Text(), nullable=False),
        sa.Column("post_status", sa.Text(), nullable=False),
        sa.Column("title", sa.Text(), nullable=True),
        sa.Column("raw_text", sa.Text(), nullable=True),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("rejection_reason", sa.Text(), nullable=True),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.CheckConstraint("source_type IN ('google-doc','docx-upload')", name="submissions_source_type_check"),
        sa.CheckConstraint("backlink_placement IN ('intro','conclusion')", name="submissions_backlink_placement_check"),
        sa.CheckConstraint("post_status IN ('draft','publish')", name="submissions_post_status_check"),
        sa.CheckConstraint("status IN ('received','validated','rejected','queued')", name="submissions_status_check"),
        sa.CheckConstraint(
            "(source_type = 'google-doc' AND doc_url IS NOT NULL AND file_url IS NULL) "
            "OR (source_type = 'docx-upload' AND file_url IS NOT NULL AND doc_url IS NULL)",
            name="submissions_source_payload_check",
        ),
        sa.ForeignKeyConstraint(["client_id"], ["clients.id"]),
        sa.ForeignKeyConstraint(["site_id"], ["sites.id"]),
    )

    op.create_table(
        "jobs",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("submission_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("client_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("site_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("job_status", sa.Text(), nullable=False),
        sa.Column("attempt_count", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("wp_post_id", sa.BigInteger(), nullable=True),
        sa.Column("wp_post_url", sa.Text(), nullable=True),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.CheckConstraint("job_status IN ('queued','processing','succeeded','failed','retrying')", name="jobs_job_status_check"),
        sa.ForeignKeyConstraint(["submission_id"], ["submissions.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["client_id"], ["clients.id"]),
        sa.ForeignKeyConstraint(["site_id"], ["sites.id"]),
    )

    op.create_table(
        "job_events",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("job_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("event_type", sa.Text(), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.CheckConstraint(
            "event_type IN ('converter_called','converter_ok','image_prompt_ok','image_generated','wp_post_created','wp_post_updated','failed')",
            name="job_events_event_type_check",
        ),
        sa.ForeignKeyConstraint(["job_id"], ["jobs.id"], ondelete="CASCADE"),
    )

    op.create_table(
        "assets",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("job_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("asset_type", sa.Text(), nullable=False),
        sa.Column("provider", sa.Text(), nullable=False),
        sa.Column("source_url", sa.Text(), nullable=True),
        sa.Column("storage_url", sa.Text(), nullable=True),
        sa.Column("meta", postgresql.JSONB(astext_type=sa.Text()), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.CheckConstraint("asset_type IN ('featured_image')", name="assets_asset_type_check"),
        sa.CheckConstraint("provider IN ('leonardo','openai','other')", name="assets_provider_check"),
        sa.ForeignKeyConstraint(["job_id"], ["jobs.id"], ondelete="CASCADE"),
    )

    op.create_index("idx_clients_status", "clients", ["status"])
    op.create_index("idx_sites_status", "sites", ["status"])

    op.create_index("idx_site_credentials_site_id", "site_credentials", ["site_id"])
    op.create_index("idx_site_credentials_enabled", "site_credentials", ["enabled"])

    op.create_index("idx_client_site_access_client_id", "client_site_access", ["client_id"])
    op.create_index("idx_client_site_access_site_id", "client_site_access", ["site_id"])
    op.create_index("idx_client_site_access_enabled", "client_site_access", ["enabled"])

    op.create_index("idx_submissions_client_id", "submissions", ["client_id"])
    op.create_index("idx_submissions_site_id", "submissions", ["site_id"])
    op.create_index("idx_submissions_status", "submissions", ["status"])
    op.create_index("idx_submissions_created_at", "submissions", ["created_at"])

    op.create_index("idx_jobs_submission_id", "jobs", ["submission_id"])
    op.create_index("idx_jobs_client_id", "jobs", ["client_id"])
    op.create_index("idx_jobs_site_id", "jobs", ["site_id"])
    op.create_index("idx_jobs_job_status", "jobs", ["job_status"])
    op.create_index("idx_jobs_job_status_created_at", "jobs", ["job_status", "created_at"])

    op.create_index("idx_job_events_job_id_created_at", "job_events", ["job_id", "created_at"])
    op.create_index("idx_assets_job_id", "assets", ["job_id"])

    op.execute(
        """
        CREATE OR REPLACE FUNCTION set_updated_at()
        RETURNS TRIGGER
        LANGUAGE plpgsql
        AS $$
        BEGIN
            NEW.updated_at = NOW();
            RETURN NEW;
        END;
        $$;
        """
    )

    op.execute(
        """
        CREATE TRIGGER trg_clients_set_updated_at
        BEFORE UPDATE ON clients
        FOR EACH ROW
        EXECUTE FUNCTION set_updated_at();
        """
    )
    op.execute(
        """
        CREATE TRIGGER trg_sites_set_updated_at
        BEFORE UPDATE ON sites
        FOR EACH ROW
        EXECUTE FUNCTION set_updated_at();
        """
    )
    op.execute(
        """
        CREATE TRIGGER trg_site_credentials_set_updated_at
        BEFORE UPDATE ON site_credentials
        FOR EACH ROW
        EXECUTE FUNCTION set_updated_at();
        """
    )
    op.execute(
        """
        CREATE TRIGGER trg_client_site_access_set_updated_at
        BEFORE UPDATE ON client_site_access
        FOR EACH ROW
        EXECUTE FUNCTION set_updated_at();
        """
    )
    op.execute(
        """
        CREATE TRIGGER trg_submissions_set_updated_at
        BEFORE UPDATE ON submissions
        FOR EACH ROW
        EXECUTE FUNCTION set_updated_at();
        """
    )
    op.execute(
        """
        CREATE TRIGGER trg_jobs_set_updated_at
        BEFORE UPDATE ON jobs
        FOR EACH ROW
        EXECUTE FUNCTION set_updated_at();
        """
    )


def downgrade() -> None:
    op.execute("DROP TRIGGER IF EXISTS trg_jobs_set_updated_at ON jobs;")
    op.execute("DROP TRIGGER IF EXISTS trg_submissions_set_updated_at ON submissions;")
    op.execute("DROP TRIGGER IF EXISTS trg_client_site_access_set_updated_at ON client_site_access;")
    op.execute("DROP TRIGGER IF EXISTS trg_site_credentials_set_updated_at ON site_credentials;")
    op.execute("DROP TRIGGER IF EXISTS trg_sites_set_updated_at ON sites;")
    op.execute("DROP TRIGGER IF EXISTS trg_clients_set_updated_at ON clients;")
    op.execute("DROP FUNCTION IF EXISTS set_updated_at();")

    op.drop_table("assets")
    op.drop_table("job_events")
    op.drop_table("jobs")
    op.drop_table("submissions")
    op.drop_table("client_site_access")
    op.drop_table("site_credentials")
    op.drop_table("sites")
    op.drop_table("clients")
