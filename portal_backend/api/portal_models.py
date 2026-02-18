from __future__ import annotations

import uuid
from datetime import datetime, timezone

from sqlalchemy import BigInteger, Boolean, CheckConstraint, Column, DateTime, ForeignKey, Integer, Text, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import declarative_base

Base = declarative_base()


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class Client(Base):
    __tablename__ = "clients"
    __table_args__ = (
        CheckConstraint("status IN ('active','inactive')", name="clients_status_check"),
    )

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name = Column(Text, nullable=False)
    primary_domain = Column(Text, nullable=True)
    backlink_url = Column(Text, nullable=True)
    email = Column(Text, nullable=True)
    phone_number = Column(Text, nullable=True)
    status = Column(Text, nullable=False, default="active")
    created_at = Column(DateTime(timezone=True), nullable=False, default=utcnow)
    updated_at = Column(DateTime(timezone=True), nullable=False, default=utcnow)


class User(Base):
    __tablename__ = "users"
    __table_args__ = (
        CheckConstraint("role IN ('admin','client')", name="users_role_check"),
    )

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    email = Column(Text, nullable=False, unique=True)
    password_hash = Column(Text, nullable=False)
    role = Column(Text, nullable=False, default="client")
    is_active = Column(Boolean, nullable=False, default=True)
    created_at = Column(DateTime(timezone=True), nullable=False, default=utcnow)
    updated_at = Column(DateTime(timezone=True), nullable=False, default=utcnow)


class ClientUser(Base):
    __tablename__ = "client_users"
    __table_args__ = (
        UniqueConstraint("client_id", "user_id", name="client_users_client_user_unique"),
    )

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    client_id = Column(UUID(as_uuid=True), ForeignKey("clients.id", ondelete="CASCADE"), nullable=False)
    user_id = Column(UUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    created_at = Column(DateTime(timezone=True), nullable=False, default=utcnow)
    updated_at = Column(DateTime(timezone=True), nullable=False, default=utcnow)


class Site(Base):
    __tablename__ = "sites"
    __table_args__ = (
        CheckConstraint("status IN ('active','inactive')", name="sites_status_check"),
    )

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name = Column(Text, nullable=False)
    site_url = Column(Text, nullable=False, unique=True)
    wp_rest_base = Column(Text, nullable=False, default="/wp-json/wp/v2")
    hosting_provider = Column(Text, nullable=True)
    hosting_panel = Column(Text, nullable=True)
    status = Column(Text, nullable=False, default="active")
    created_at = Column(DateTime(timezone=True), nullable=False, default=utcnow)
    updated_at = Column(DateTime(timezone=True), nullable=False, default=utcnow)


class SiteCredential(Base):
    __tablename__ = "site_credentials"
    __table_args__ = (
        CheckConstraint("auth_type IN ('application_password')", name="site_credentials_auth_type_check"),
        UniqueConstraint("site_id", "wp_username", name="site_credentials_site_username_unique"),
    )

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    site_id = Column(UUID(as_uuid=True), ForeignKey("sites.id", ondelete="CASCADE"), nullable=False)
    auth_type = Column(Text, nullable=False, default="application_password")
    wp_username = Column(Text, nullable=False)
    wp_app_password = Column(Text, nullable=False)
    author_name = Column(Text, nullable=True)
    author_id = Column(BigInteger, nullable=True)
    enabled = Column(Boolean, nullable=False, default=True)
    created_at = Column(DateTime(timezone=True), nullable=False, default=utcnow)
    updated_at = Column(DateTime(timezone=True), nullable=False, default=utcnow)


class SiteCategory(Base):
    __tablename__ = "site_categories"
    __table_args__ = (
        UniqueConstraint("site_id", "wp_category_id", name="site_categories_site_wp_category_unique"),
    )

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    site_id = Column(UUID(as_uuid=True), ForeignKey("sites.id", ondelete="CASCADE"), nullable=False)
    wp_category_id = Column(BigInteger, nullable=False)
    name = Column(Text, nullable=False)
    slug = Column(Text, nullable=True)
    parent_wp_category_id = Column(BigInteger, nullable=True)
    post_count = Column(Integer, nullable=True)
    enabled = Column(Boolean, nullable=False, default=True)
    created_at = Column(DateTime(timezone=True), nullable=False, default=utcnow)
    updated_at = Column(DateTime(timezone=True), nullable=False, default=utcnow)


class SiteDefaultCategory(Base):
    __tablename__ = "site_default_categories"
    __table_args__ = (
        UniqueConstraint("site_id", "wp_category_id", name="site_default_categories_site_wp_category_unique"),
    )

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    site_id = Column(UUID(as_uuid=True), ForeignKey("sites.id", ondelete="CASCADE"), nullable=False)
    wp_category_id = Column(BigInteger, nullable=False)
    category_name = Column(Text, nullable=True)
    position = Column(Integer, nullable=False, default=100)
    enabled = Column(Boolean, nullable=False, default=True)
    created_at = Column(DateTime(timezone=True), nullable=False, default=utcnow)
    updated_at = Column(DateTime(timezone=True), nullable=False, default=utcnow)


class ClientSiteAccess(Base):
    __tablename__ = "client_site_access"
    __table_args__ = (
        UniqueConstraint("client_id", "site_id", name="client_site_access_client_site_unique"),
    )

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    client_id = Column(UUID(as_uuid=True), ForeignKey("clients.id", ondelete="CASCADE"), nullable=False)
    site_id = Column(UUID(as_uuid=True), ForeignKey("sites.id", ondelete="CASCADE"), nullable=False)
    enabled = Column(Boolean, nullable=False, default=True)
    created_at = Column(DateTime(timezone=True), nullable=False, default=utcnow)
    updated_at = Column(DateTime(timezone=True), nullable=False, default=utcnow)


class Submission(Base):
    __tablename__ = "submissions"
    __table_args__ = (
        CheckConstraint("source_type IN ('google-doc','docx-upload')", name="submissions_source_type_check"),
        CheckConstraint("backlink_placement IN ('intro','conclusion')", name="submissions_backlink_placement_check"),
        CheckConstraint("post_status IN ('draft','publish')", name="submissions_post_status_check"),
        CheckConstraint(
            "status IN ('received','validated','rejected','queued')",
            name="submissions_status_check",
        ),
        CheckConstraint(
            "(source_type = 'google-doc' AND doc_url IS NOT NULL AND file_url IS NULL) "
            "OR (source_type = 'docx-upload' AND file_url IS NOT NULL AND doc_url IS NULL)",
            name="submissions_source_payload_check",
        ),
    )

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    client_id = Column(UUID(as_uuid=True), ForeignKey("clients.id"), nullable=False)
    site_id = Column(UUID(as_uuid=True), ForeignKey("sites.id"), nullable=False)
    source_type = Column(Text, nullable=False)
    doc_url = Column(Text, nullable=True)
    file_url = Column(Text, nullable=True)
    backlink_placement = Column(Text, nullable=False)
    post_status = Column(Text, nullable=False)
    title = Column(Text, nullable=True)
    raw_text = Column(Text, nullable=True)
    notes = Column(Text, nullable=True)
    status = Column(Text, nullable=False, default="received")
    rejection_reason = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False, default=utcnow)
    updated_at = Column(DateTime(timezone=True), nullable=False, default=utcnow)


class Job(Base):
    __tablename__ = "jobs"
    __table_args__ = (
        CheckConstraint(
            "job_status IN ('queued','processing','succeeded','failed','retrying')",
            name="jobs_job_status_check",
        ),
    )

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    submission_id = Column(UUID(as_uuid=True), ForeignKey("submissions.id", ondelete="CASCADE"), nullable=False)
    client_id = Column(UUID(as_uuid=True), ForeignKey("clients.id"), nullable=False)
    site_id = Column(UUID(as_uuid=True), ForeignKey("sites.id"), nullable=False)
    job_status = Column(Text, nullable=False, default="queued")
    attempt_count = Column(Integer, nullable=False, default=0)
    last_error = Column(Text, nullable=True)
    wp_post_id = Column(BigInteger, nullable=True)
    wp_post_url = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False, default=utcnow)
    updated_at = Column(DateTime(timezone=True), nullable=False, default=utcnow)


class JobEvent(Base):
    __tablename__ = "job_events"
    __table_args__ = (
        CheckConstraint(
            "event_type IN ('converter_called','converter_ok','image_prompt_ok','image_generated','wp_post_created','wp_post_updated','failed')",
            name="job_events_event_type_check",
        ),
    )

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    job_id = Column(UUID(as_uuid=True), ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False)
    event_type = Column(Text, nullable=False)
    payload = Column(JSONB, nullable=False, default=dict)
    created_at = Column(DateTime(timezone=True), nullable=False, default=utcnow)


class Asset(Base):
    __tablename__ = "assets"
    __table_args__ = (
        CheckConstraint("asset_type IN ('featured_image')", name="assets_asset_type_check"),
        CheckConstraint("provider IN ('leonardo','openai','other')", name="assets_provider_check"),
    )

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    job_id = Column(UUID(as_uuid=True), ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False)
    asset_type = Column(Text, nullable=False)
    provider = Column(Text, nullable=False)
    source_url = Column(Text, nullable=True)
    storage_url = Column(Text, nullable=True)
    meta = Column(JSONB, nullable=False, default=dict)
    created_at = Column(DateTime(timezone=True), nullable=False, default=utcnow)
