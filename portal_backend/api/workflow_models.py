from __future__ import annotations

import uuid

from sqlalchemy import CheckConstraint, Column, DateTime, ForeignKey, Integer, Text, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB, UUID

from .portal_models import Base, utcnow


class WorkflowColumn(Base):
    __tablename__ = "workflow_columns"
    __table_args__ = (
        UniqueConstraint("column_key", name="workflow_columns_key_unique"),
    )

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    column_key = Column(Text, nullable=False)
    name = Column(Text, nullable=False)
    color = Column(Text, nullable=True)
    position = Column(Integer, nullable=False, default=100)
    created_at = Column(DateTime(timezone=True), nullable=False, default=utcnow)
    updated_at = Column(DateTime(timezone=True), nullable=False, default=utcnow)


class WorkflowCard(Base):
    __tablename__ = "workflow_cards"
    __table_args__ = (
        CheckConstraint("column_source IN ('auto','manual')", name="workflow_cards_column_source_check"),
        UniqueConstraint("job_id", name="workflow_cards_job_unique"),
    )

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    job_id = Column(UUID(as_uuid=True), ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False)
    submission_id = Column(UUID(as_uuid=True), ForeignKey("submissions.id", ondelete="CASCADE"), nullable=False)
    client_id = Column(UUID(as_uuid=True), ForeignKey("clients.id"), nullable=False)
    site_id = Column("publishing_site_id", UUID(as_uuid=True), ForeignKey("publishing_sites.id"), nullable=False)
    column_id = Column(UUID(as_uuid=True), ForeignKey("workflow_columns.id", ondelete="CASCADE"), nullable=False)
    column_source = Column(Text, nullable=False, default="auto")
    position = Column(Integer, nullable=False, default=1000)
    title_snapshot = Column(Text, nullable=True)
    request_kind_snapshot = Column(Text, nullable=True)
    job_status_snapshot = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False, default=utcnow)
    updated_at = Column(DateTime(timezone=True), nullable=False, default=utcnow)


class WorkflowCardEvent(Base):
    __tablename__ = "workflow_card_events"
    __table_args__ = (
        CheckConstraint(
            "event_type IN ('created','moved','auto_synced')",
            name="workflow_card_events_type_check",
        ),
    )

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    card_id = Column(UUID(as_uuid=True), ForeignKey("workflow_cards.id", ondelete="CASCADE"), nullable=False)
    job_id = Column(UUID(as_uuid=True), ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False)
    actor_user_id = Column(UUID(as_uuid=True), ForeignKey("users.id", ondelete="SET NULL"), nullable=True)
    event_type = Column(Text, nullable=False)
    from_column_id = Column(UUID(as_uuid=True), ForeignKey("workflow_columns.id", ondelete="SET NULL"), nullable=True)
    to_column_id = Column(UUID(as_uuid=True), ForeignKey("workflow_columns.id", ondelete="SET NULL"), nullable=True)
    payload = Column(JSONB, nullable=False, default=dict)
    created_at = Column(DateTime(timezone=True), nullable=False, default=utcnow)
