from __future__ import annotations

import uuid

from sqlalchemy import CheckConstraint, Column, DateTime, ForeignKey, Integer, Text, UniqueConstraint
from sqlalchemy.dialects.postgresql import ARRAY, JSONB, UUID

from .portal_models import Base, utcnow


class TaskBoardColumn(Base):
    __tablename__ = "task_board_columns"
    __table_args__ = (
        UniqueConstraint("column_key", name="task_board_columns_key_unique"),
    )

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    column_key = Column(Text, nullable=False)
    name = Column(Text, nullable=False)
    color = Column(Text, nullable=True)
    position = Column(Integer, nullable=False, default=100)
    created_at = Column(DateTime(timezone=True), nullable=False, default=utcnow)
    updated_at = Column(DateTime(timezone=True), nullable=False, default=utcnow)


class TaskBoardCard(Base):
    __tablename__ = "task_board_cards"
    __table_args__ = (
        CheckConstraint("column_source IN ('auto','manual')", name="task_board_cards_column_source_check"),
        CheckConstraint("card_kind IN ('job','manual')", name="task_board_cards_kind_check"),
        CheckConstraint("job_type IN ('articles','develop','fix','research')", name="task_board_cards_job_type_check"),
        CheckConstraint("priority IN ('urgent','high','medium','low')", name="task_board_cards_priority_check"),
        CheckConstraint(
            "flag_types <@ ARRAY['bug','needs_levent_attention','needs_adil_attention']::text[]",
            name="task_board_cards_flag_types_check",
        ),
        UniqueConstraint("job_id", name="task_board_cards_job_unique"),
    )

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    job_id = Column(UUID(as_uuid=True), ForeignKey("jobs.id", ondelete="CASCADE"), nullable=True)
    submission_id = Column(UUID(as_uuid=True), ForeignKey("submissions.id", ondelete="CASCADE"), nullable=True)
    client_id = Column(UUID(as_uuid=True), ForeignKey("clients.id"), nullable=True)
    site_id = Column("publishing_site_id", UUID(as_uuid=True), ForeignKey("publishing_sites.id"), nullable=True)
    column_id = Column(UUID(as_uuid=True), ForeignKey("task_board_columns.id", ondelete="CASCADE"), nullable=False)
    card_kind = Column(Text, nullable=False, default="job")
    column_source = Column(Text, nullable=False, default="auto")
    position = Column(Integer, nullable=False, default=1000)
    title_snapshot = Column(Text, nullable=True)
    description = Column(Text, nullable=True)
    job_type = Column(Text, nullable=True)
    priority = Column(Text, nullable=False, default="medium")
    flag_types = Column(ARRAY(Text), nullable=False, default=list)
    request_kind_snapshot = Column(Text, nullable=True)
    job_status_snapshot = Column(Text, nullable=True)
    created_by_user_id = Column(UUID(as_uuid=True), ForeignKey("users.id", ondelete="SET NULL"), nullable=True)
    assignee_user_id = Column(UUID(as_uuid=True), ForeignKey("users.id", ondelete="SET NULL"), nullable=True)
    created_by_name_snapshot = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False, default=utcnow)
    updated_at = Column(DateTime(timezone=True), nullable=False, default=utcnow)


class TaskBoardCardEvent(Base):
    __tablename__ = "task_board_card_events"
    __table_args__ = (
        CheckConstraint(
            "event_type IN ('created','manual_created','moved','auto_synced','comment_added','comment_updated')",
            name="task_board_card_events_type_check",
        ),
    )

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    card_id = Column(UUID(as_uuid=True), ForeignKey("task_board_cards.id", ondelete="CASCADE"), nullable=False)
    job_id = Column(UUID(as_uuid=True), ForeignKey("jobs.id", ondelete="CASCADE"), nullable=True)
    actor_user_id = Column(UUID(as_uuid=True), ForeignKey("users.id", ondelete="SET NULL"), nullable=True)
    event_type = Column(Text, nullable=False)
    from_column_id = Column(UUID(as_uuid=True), ForeignKey("task_board_columns.id", ondelete="SET NULL"), nullable=True)
    to_column_id = Column(UUID(as_uuid=True), ForeignKey("task_board_columns.id", ondelete="SET NULL"), nullable=True)
    payload = Column(JSONB, nullable=False, default=dict)
    created_at = Column(DateTime(timezone=True), nullable=False, default=utcnow)


class TaskBoardCardComment(Base):
    __tablename__ = "task_board_card_comments"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    card_id = Column(UUID(as_uuid=True), ForeignKey("task_board_cards.id", ondelete="CASCADE"), nullable=False)
    author_user_id = Column(UUID(as_uuid=True), ForeignKey("users.id", ondelete="SET NULL"), nullable=True)
    author_name_snapshot = Column(Text, nullable=False)
    body = Column(Text, nullable=False)
    created_at = Column(DateTime(timezone=True), nullable=False, default=utcnow)
    updated_at = Column(DateTime(timezone=True), nullable=False, default=utcnow)
