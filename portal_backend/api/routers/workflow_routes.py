from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta, timezone
import re
from typing import Dict, Iterable, List, Optional, Sequence
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import or_
from sqlalchemy.orm import Session

from ..auth import require_admin, require_super_admin
from ..db import get_db
from ..portal_models import Client, CreatorOutput, Job, JobEvent, Site, Submission, User
from ..workflow_models import WorkflowCard, WorkflowCardEvent, WorkflowColumn
from ..workflow_schemas import (
    WorkflowBoardOut,
    WorkflowCardMoveIn,
    WorkflowCardOut,
    WorkflowColumnCreateIn,
    WorkflowColumnOut,
    WorkflowColumnUpdateIn,
)

router = APIRouter(prefix="/workflow", tags=["workflow"], dependencies=[Depends(require_admin)])

WORKFLOW_RECENT_TERMINAL_DAYS = 14
TERMINAL_JOB_STATUSES = {"succeeded", "failed", "rejected", "canceled"}
DEFAULT_WORKFLOW_COLUMNS = (
    {"key": "backlog", "name": "Backlog", "color": "#7c8aa5", "position": 100},
    {"key": "in_progress", "name": "In Progress", "color": "#2d7ff9", "position": 200},
    {"key": "pending_review", "name": "Pending Review", "color": "#b7791f", "position": 300},
    {"key": "blocked", "name": "Blocked", "color": "#c53030", "position": 400},
    {"key": "done", "name": "Done", "color": "#2f855a", "position": 500},
)
JOB_STATUS_COLUMN_KEYS = {
    "queued": "backlog",
    "processing": "in_progress",
    "retrying": "in_progress",
    "pending_approval": "pending_review",
    "failed": "blocked",
    "rejected": "blocked",
    "canceled": "blocked",
    "succeeded": "done",
}
SYSTEM_WORKFLOW_COLUMN_KEYS = {item["key"] for item in DEFAULT_WORKFLOW_COLUMNS}
CUSTOM_WORKFLOW_COLUMN_COLOR = "#7c8aa5"


def _workflow_column_key_for_status(job_status: str) -> str:
    normalized = (job_status or "").strip().lower()
    return JOB_STATUS_COLUMN_KEYS.get(normalized, "backlog")


def _is_system_workflow_column_key(column_key: str) -> bool:
    return (column_key or "").strip().lower() in SYSTEM_WORKFLOW_COLUMN_KEYS


def _build_custom_workflow_column_key(name: str, existing_keys: Sequence[str]) -> str:
    slug = re.sub(r"[^a-z0-9]+", "_", (name or "").strip().lower()).strip("_")
    if not slug:
        slug = "column"
    base = f"custom_{slug}"
    normalized_existing = {(item or "").strip().lower() for item in existing_keys}
    key = base
    suffix = 2
    while key in normalized_existing or _is_system_workflow_column_key(key):
        key = f"{base}_{suffix}"
        suffix += 1
    return key


def _build_workflow_card_title(submission: Submission, content_title: str) -> str:
    if content_title.strip():
        return content_title.strip()
    if isinstance(submission.title, str) and submission.title.strip():
        return submission.title.strip()
    if submission.request_kind == "create_article":
        return "Created article request"
    return "Submitted article request"


def _get_target_url_from_submission(submission: Submission) -> str:
    notes = str(submission.notes or "")
    for item in notes.split(";"):
        left, sep, right = item.partition("=")
        if not sep:
            continue
        if left.strip().lower() != "target_site_url":
            continue
        return right.strip()
    return ""


def _ensure_default_workflow_columns(db: Session) -> List[WorkflowColumn]:
    existing = db.query(WorkflowColumn).order_by(WorkflowColumn.position.asc(), WorkflowColumn.created_at.asc()).all()
    existing_by_key = {item.column_key: item for item in existing}
    changed = False
    for spec in DEFAULT_WORKFLOW_COLUMNS:
        column = existing_by_key.get(spec["key"])
        if column is None:
            column = WorkflowColumn(
                column_key=spec["key"],
                name=spec["name"],
                color=spec["color"],
                position=spec["position"],
            )
            db.add(column)
            existing.append(column)
            changed = True
            continue
    if changed:
        db.flush()
    return sorted(existing, key=lambda item: (item.position, item.created_at))


def _next_workflow_column_position(columns: Sequence[WorkflowColumn]) -> int:
    if not columns:
        return 100
    return max(int(item.position or 0) for item in columns) + 100


def _get_content_titles_by_job_id(db: Session, job_ids: Sequence[UUID]) -> Dict[UUID, str]:
    if not job_ids:
        return {}
    creator_rows = (
        db.query(CreatorOutput.job_id, CreatorOutput.payload)
        .filter(CreatorOutput.job_id.in_(job_ids))
        .order_by(CreatorOutput.created_at.desc())
        .all()
    )
    title_map: Dict[UUID, str] = {}
    for job_id, payload in creator_rows:
        if job_id in title_map or not isinstance(payload, dict):
            continue
        phase4 = payload.get("phase4") if isinstance(payload.get("phase4"), dict) else {}
        for key in ("h1", "title"):
            raw = phase4.get(key)
            if isinstance(raw, str) and raw.strip():
                title_map[job_id] = raw.strip()
                break

    event_rows = (
        db.query(JobEvent.job_id, JobEvent.payload)
        .filter(JobEvent.job_id.in_(job_ids), JobEvent.event_type == "converter_ok")
        .order_by(JobEvent.created_at.desc())
        .all()
    )
    for job_id, payload in event_rows:
        if job_id in title_map or not isinstance(payload, dict):
            continue
        raw = payload.get("title")
        if isinstance(raw, str) and raw.strip():
            title_map[job_id] = raw.strip()
    return title_map


def _next_position(positions_by_column: Dict[UUID, int], column_id: UUID) -> int:
    next_value = positions_by_column.get(column_id, 0) + 100
    positions_by_column[column_id] = next_value
    return next_value


def _apply_card_job_sync(
    card: WorkflowCard,
    *,
    desired_column_id: UUID,
    job_status: str,
    title_snapshot: str,
    request_kind: str,
    next_position,
) -> dict:
    previous_column_id = card.column_id
    previous_status = (card.job_status_snapshot or "").strip().lower()
    previous_title = str(card.title_snapshot or "")
    previous_request_kind = str(card.request_kind_snapshot or "")
    normalized_status = (job_status or "").strip().lower()
    dirty = (
        previous_status != normalized_status
        or previous_title != title_snapshot
        or previous_request_kind != request_kind
    )
    card.title_snapshot = title_snapshot
    card.request_kind_snapshot = request_kind
    card.job_status_snapshot = normalized_status

    should_move = False
    if normalized_status in TERMINAL_JOB_STATUSES:
        should_move = card.column_id != desired_column_id
        card.column_source = "auto"
    elif card.column_source == "auto" and previous_status != normalized_status and card.column_id != desired_column_id:
        should_move = True

    if not should_move:
        if dirty:
            card.updated_at = datetime.now(timezone.utc)
        return {
            "moved": False,
            "dirty": dirty,
        }

    card.column_id = desired_column_id
    card.position = next_position(desired_column_id)
    card.updated_at = datetime.now(timezone.utc)
    return {
        "moved": True,
        "dirty": True,
        "from_column_id": previous_column_id,
        "to_column_id": desired_column_id,
        "previous_status": previous_status or None,
        "job_status": normalized_status,
    }


def _record_card_event(
    db: Session,
    *,
    card: WorkflowCard,
    actor_user_id: Optional[UUID],
    event_type: str,
    from_column_id: Optional[UUID],
    to_column_id: Optional[UUID],
    payload: dict,
) -> None:
    db.add(
        WorkflowCardEvent(
            card_id=card.id,
            job_id=card.job_id,
            actor_user_id=actor_user_id,
            event_type=event_type,
            from_column_id=from_column_id,
            to_column_id=to_column_id,
            payload=payload,
        )
    )


def _select_workflow_jobs(db: Session) -> list[tuple[Job, Submission, Client, Site]]:
    cutoff = datetime.now(timezone.utc) - timedelta(days=WORKFLOW_RECENT_TERMINAL_DAYS)
    return (
        db.query(Job, Submission, Client, Site)
        .join(Submission, Submission.id == Job.submission_id)
        .join(Client, Client.id == Job.client_id)
        .join(Site, Site.id == Job.site_id)
        .filter(
            or_(
                ~Job.job_status.in_(tuple(TERMINAL_JOB_STATUSES)),
                Job.updated_at >= cutoff,
            )
        )
        .order_by(Job.created_at.desc())
        .all()
    )


def _sync_workflow_cards(
    db: Session,
    *,
    actor_user_id: Optional[UUID] = None,
) -> tuple[list[WorkflowColumn], list[tuple[WorkflowCard, Job, Submission, Client, Site]]]:
    columns = _ensure_default_workflow_columns(db)
    columns_by_key = {item.column_key: item for item in columns}
    rows = _select_workflow_jobs(db)
    job_ids = [job.id for job, *_ in rows]
    existing_cards = (
        db.query(WorkflowCard)
        .filter(WorkflowCard.job_id.in_(job_ids))
        .order_by(WorkflowCard.position.asc(), WorkflowCard.created_at.asc())
        .all()
        if job_ids
        else []
    )
    cards_by_job_id = {item.job_id: item for item in existing_cards}
    positions_by_column: Dict[UUID, int] = defaultdict(int)
    for card in existing_cards:
        positions_by_column[card.column_id] = max(positions_by_column[card.column_id], int(card.position or 0))

    content_titles = _get_content_titles_by_job_id(db, job_ids)
    changed = False

    for job, submission, client, site in rows:
        desired_column = columns_by_key[_workflow_column_key_for_status(job.job_status)]
        title_snapshot = _build_workflow_card_title(submission, content_titles.get(job.id, ""))
        card = cards_by_job_id.get(job.id)
        if card is None:
            card = WorkflowCard(
                job_id=job.id,
                submission_id=submission.id,
                client_id=client.id,
                site_id=site.id,
                column_id=desired_column.id,
                column_source="auto",
                position=_next_position(positions_by_column, desired_column.id),
                title_snapshot=title_snapshot,
                request_kind_snapshot=submission.request_kind,
                job_status_snapshot=job.job_status,
            )
            db.add(card)
            db.flush()
            _record_card_event(
                db,
                card=card,
                actor_user_id=actor_user_id,
                event_type="created",
                from_column_id=None,
                to_column_id=desired_column.id,
                payload={"job_status": job.job_status, "request_kind": submission.request_kind},
            )
            cards_by_job_id[job.id] = card
            changed = True
            continue

        sync_event = _apply_card_job_sync(
            card,
            desired_column_id=desired_column.id,
            job_status=job.job_status,
            title_snapshot=title_snapshot,
            request_kind=submission.request_kind,
            next_position=lambda column_id: _next_position(positions_by_column, column_id),
        )
        card.submission_id = submission.id
        card.client_id = client.id
        card.site_id = site.id
        if sync_event["dirty"]:
            changed = True
        if sync_event["moved"]:
            _record_card_event(
                db,
                card=card,
                actor_user_id=actor_user_id,
                event_type="auto_synced",
                from_column_id=sync_event["from_column_id"],
                to_column_id=sync_event["to_column_id"],
                payload={
                    "previous_status": sync_event["previous_status"],
                    "job_status": sync_event["job_status"],
                },
            )

    if changed:
        db.commit()
        columns = _ensure_default_workflow_columns(db)
        cards_by_job_id = {
            item.job_id: item
            for item in db.query(WorkflowCard)
            .filter(WorkflowCard.job_id.in_(job_ids))
            .order_by(WorkflowCard.position.asc(), WorkflowCard.created_at.asc())
            .all()
        }

    enriched_rows = [
        (cards_by_job_id[job.id], job, submission, client, site)
        for job, submission, client, site in rows
        if job.id in cards_by_job_id
    ]
    return columns, enriched_rows


def _build_workflow_board_payload(
    columns: Sequence[WorkflowColumn],
    rows: Iterable[tuple[WorkflowCard, Job, Submission, Client, Site]],
) -> WorkflowBoardOut:
    cards_by_column: Dict[UUID, List[WorkflowCardOut]] = defaultdict(list)
    open_card_count = 0
    completed_card_count = 0
    updated_at = max((item.updated_at for item in columns), default=datetime.now(timezone.utc))

    for card, job, submission, client, site in rows:
        column_key = next((item.column_key for item in columns if item.id == card.column_id), "")
        title = _build_workflow_card_title(submission, str(card.title_snapshot or ""))
        card_out = WorkflowCardOut(
            id=card.id,
            job_id=job.id,
            submission_id=submission.id,
            client_id=client.id,
            client_name=(client.name or "").strip(),
            site_id=site.id,
            site_name=(site.name or "").strip(),
            site_url=(site.site_url or "").strip(),
            column_id=card.column_id,
            column_key=column_key,
            title=title,
            request_kind=(card.request_kind_snapshot or submission.request_kind or "").strip() or None,
            job_status=(job.job_status or "").strip(),
            wp_post_url=(job.wp_post_url or "").strip() or None,
            last_error=(job.last_error or "").strip() or None,
            position=int(card.position or 0),
            created_at=card.created_at,
            updated_at=card.updated_at,
        )
        cards_by_column[card.column_id].append(card_out)
        if (job.job_status or "").strip().lower() in TERMINAL_JOB_STATUSES:
            completed_card_count += 1
        else:
            open_card_count += 1
        updated_at = max(updated_at, card.updated_at, job.updated_at)

    columns_out = []
    for column in sorted(columns, key=lambda item: (item.position, item.created_at)):
        column_cards = sorted(cards_by_column.get(column.id, []), key=lambda item: (item.position, item.created_at))
        columns_out.append(
            WorkflowColumnOut(
                id=column.id,
                key=column.column_key,
                name=column.name,
                color=column.color,
                is_system=_is_system_workflow_column_key(column.column_key),
                position=column.position,
                cards=column_cards,
            )
        )

    return WorkflowBoardOut(
        columns=columns_out,
        open_card_count=open_card_count,
        completed_card_count=completed_card_count,
        updated_at=updated_at,
    )


def _load_workflow_board(db: Session, *, actor_user_id: Optional[UUID] = None) -> WorkflowBoardOut:
    columns, rows = _sync_workflow_cards(db, actor_user_id=actor_user_id)
    return _build_workflow_board_payload(columns, rows)


@router.get("/board", response_model=WorkflowBoardOut)
def get_workflow_board(
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin),
) -> WorkflowBoardOut:
    return _load_workflow_board(db, actor_user_id=current_user.id)


@router.patch("/cards/{card_id}", response_model=WorkflowBoardOut)
def move_workflow_card(
    card_id: UUID,
    payload: WorkflowCardMoveIn,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin),
) -> WorkflowBoardOut:
    columns = _ensure_default_workflow_columns(db)
    target_column = next((item for item in columns if item.id == payload.column_id), None)
    if target_column is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Workflow column not found.")

    card = db.query(WorkflowCard).filter(WorkflowCard.id == card_id).first()
    if card is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Workflow card not found.")

    if card.column_id != target_column.id:
        positions_by_column = defaultdict(int)
        existing_cards = (
            db.query(WorkflowCard)
            .order_by(WorkflowCard.position.asc(), WorkflowCard.created_at.asc())
            .all()
        )
        for item in existing_cards:
            positions_by_column[item.column_id] = max(positions_by_column[item.column_id], int(item.position or 0))
        previous_column_id = card.column_id
        card.column_id = target_column.id
        card.column_source = "manual"
        card.position = _next_position(positions_by_column, target_column.id)
        card.updated_at = datetime.now(timezone.utc)
        _record_card_event(
            db,
            card=card,
            actor_user_id=current_user.id,
            event_type="moved",
            from_column_id=previous_column_id,
            to_column_id=target_column.id,
            payload={"column_source": "manual"},
        )
        db.commit()

    return _load_workflow_board(db, actor_user_id=current_user.id)


@router.post("/columns", response_model=WorkflowBoardOut)
def create_workflow_column(
    payload: WorkflowColumnCreateIn,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_super_admin),
) -> WorkflowBoardOut:
    columns = _ensure_default_workflow_columns(db)
    column = WorkflowColumn(
        column_key=_build_custom_workflow_column_key(payload.name, [item.column_key for item in columns]),
        name=payload.name,
        color=CUSTOM_WORKFLOW_COLUMN_COLOR,
        position=_next_workflow_column_position(columns),
    )
    db.add(column)
    db.commit()
    return _load_workflow_board(db, actor_user_id=current_user.id)


@router.patch("/columns/{column_id}", response_model=WorkflowBoardOut)
def rename_workflow_column(
    column_id: UUID,
    payload: WorkflowColumnUpdateIn,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_super_admin),
) -> WorkflowBoardOut:
    column = db.query(WorkflowColumn).filter(WorkflowColumn.id == column_id).first()
    if column is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Workflow column not found.")

    if column.name != payload.name:
        column.name = payload.name
        column.updated_at = datetime.now(timezone.utc)
        db.commit()

    return _load_workflow_board(db, actor_user_id=current_user.id)


@router.delete("/columns/{column_id}", response_model=WorkflowBoardOut)
def delete_workflow_column(
    column_id: UUID,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_super_admin),
) -> WorkflowBoardOut:
    columns = _ensure_default_workflow_columns(db)
    column = next((item for item in columns if item.id == column_id), None)
    if column is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Workflow column not found.")
    if _is_system_workflow_column_key(column.column_key):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="System workflow columns cannot be deleted.",
        )

    backlog_column = next((item for item in columns if item.column_key == "backlog"), None)
    if backlog_column is None:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Backlog column is missing.")

    cards = (
        db.query(WorkflowCard)
        .filter(WorkflowCard.column_id == column.id)
        .order_by(WorkflowCard.position.asc(), WorkflowCard.created_at.asc())
        .all()
    )
    positions_by_column: Dict[UUID, int] = defaultdict(int)
    for item in db.query(WorkflowCard).order_by(WorkflowCard.position.asc(), WorkflowCard.created_at.asc()).all():
        positions_by_column[item.column_id] = max(positions_by_column[item.column_id], int(item.position or 0))

    for card in cards:
        previous_column_id = card.column_id
        card.column_id = backlog_column.id
        card.column_source = "manual"
        card.position = _next_position(positions_by_column, backlog_column.id)
        card.updated_at = datetime.now(timezone.utc)
        _record_card_event(
            db,
            card=card,
            actor_user_id=current_user.id,
            event_type="moved",
            from_column_id=previous_column_id,
            to_column_id=backlog_column.id,
            payload={
                "column_source": "manual",
                "reason": "column_deleted",
                "deleted_column_id": str(column.id),
                "deleted_column_name": column.name,
            },
        )

    db.flush()
    db.delete(column)
    db.commit()
    return _load_workflow_board(db, actor_user_id=current_user.id)
