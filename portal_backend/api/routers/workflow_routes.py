from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta, timezone
import os
import re
from typing import Dict, Iterable, List, Optional, Sequence
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
import requests
from sqlalchemy import or_
from sqlalchemy.orm import Session

from ..auth import require_admin, require_super_admin
from ..db import get_db
from ..portal_models import Client, CreatorOutput, Job, JobEvent, Site, Submission, User
from ..workflow_models import WorkflowCard, WorkflowCardComment, WorkflowCardEvent, WorkflowColumn
from ..workflow_schemas import (
    WorkflowBoardOut,
    WorkflowCardCreateIn,
    WorkflowCardMoveIn,
    WorkflowCardOut,
    WorkflowCardUpdateIn,
    WorkflowColumnCreateIn,
    WorkflowColumnOut,
    WorkflowColumnUpdateIn,
    WorkflowCommentCreateIn,
    WorkflowCommentOut,
    WorkflowCommentRewriteIn,
    WorkflowCommentRewriteOut,
    WorkflowCommentUpdateIn,
)

router = APIRouter(prefix="/workflow", tags=["workflow"], dependencies=[Depends(require_admin)])

WORKFLOW_RECENT_TERMINAL_DAYS = 14
TERMINAL_JOB_STATUSES = {"succeeded", "failed", "rejected", "canceled"}
DEFAULT_WORKFLOW_COLUMNS = (
    {"key": "todo", "name": "TO DO", "color": "#5e6c84", "position": 100},
    {"key": "in_progress", "name": "IN PROGRESS", "color": "#0c66e4", "position": 200},
    {"key": "done", "name": "DONE", "color": "#1f845a", "position": 300},
)
JOB_STATUS_COLUMN_KEYS = {
    "queued": "todo",
    "processing": "in_progress",
    "retrying": "in_progress",
    "pending_approval": "in_progress",
    "failed": "todo",
    "rejected": "todo",
    "canceled": "todo",
    "succeeded": "done",
}
SYSTEM_WORKFLOW_COLUMN_KEYS = {item["key"] for item in DEFAULT_WORKFLOW_COLUMNS}
CUSTOM_WORKFLOW_COLUMN_COLOR = "#7c8aa5"
WORKFLOW_COMMENT_ANTHROPIC_BASE_URL = "https://api.anthropic.com/v1"
WORKFLOW_COMMENT_ANTHROPIC_MODEL = "claude-haiku-4-5-20251001"


def _workflow_column_key_for_status(job_status: str) -> str:
    normalized = (job_status or "").strip().lower()
    return JOB_STATUS_COLUMN_KEYS.get(normalized, "todo")


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


def _build_workflow_card_title(
    submission: Optional[Submission],
    content_title: str,
    manual_title: str = "",
) -> str:
    if content_title.strip():
        return content_title.strip()
    if submission is not None and isinstance(submission.title, str) and submission.title.strip():
        return submission.title.strip()
    if manual_title.strip():
        return manual_title.strip()
    if submission is not None and submission.request_kind == "create_article":
        return "Created article request"
    return "Workflow task"


def _build_actor_name(user: Optional[User]) -> str:
    if user is None:
        return "Unknown"
    full_name = str(user.full_name or "").strip()
    if full_name:
        return full_name
    email = str(user.email or "").strip()
    if email:
        return email
    return "Unknown"


def _parse_submission_notes_map(submission: Optional[Submission]) -> dict[str, str]:
    notes = str(submission.notes or "") if submission is not None else ""
    parsed: dict[str, str] = {}
    for item in notes.split(";"):
        left, sep, right = item.partition("=")
        if not sep:
            continue
        key = left.strip().lower()
        if not key:
            continue
        parsed[key] = right.strip()
    return parsed


def _infer_job_type(card: WorkflowCard, submission: Optional[Submission]) -> Optional[str]:
    existing = str(card.job_type or "").strip().lower()
    if existing:
        return existing
    request_kind = str(card.request_kind_snapshot or (submission.request_kind if submission is not None else "")).strip().lower()
    if request_kind in {"submit_article", "create_article"} or card.card_kind == "job":
        return "articles"
    return None


def _extract_submission_actor(
    db: Session,
    submission: Optional[Submission],
) -> tuple[Optional[UUID], Optional[str]]:
    note_map = _parse_submission_notes_map(submission)
    actor_user_id = None
    raw_user_id = str(note_map.get("submission_actor_user_id") or "").strip()
    if raw_user_id:
        try:
            actor_user_id = UUID(raw_user_id)
        except ValueError:
            actor_user_id = None
    actor_name = str(note_map.get("submission_actor_email") or "").strip() or None
    if actor_user_id is not None:
        user = db.query(User).filter(User.id == actor_user_id).first()
        if user is not None:
            return user.id, _build_actor_name(user)
    return actor_user_id, actor_name


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
        if column.name != spec["name"] or column.color != spec["color"] or int(column.position or 0) != spec["position"]:
            column.name = spec["name"]
            column.color = spec["color"]
            column.position = spec["position"]
            column.updated_at = datetime.now(timezone.utc)
            changed = True
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
        or card.card_kind != "job"
    )
    card.card_kind = "job"
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
) -> list[WorkflowColumn]:
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
    cards_by_job_id = {item.job_id: item for item in existing_cards if item.job_id is not None}
    positions_by_column: Dict[UUID, int] = defaultdict(int)
    for card in db.query(WorkflowCard).order_by(WorkflowCard.position.asc(), WorkflowCard.created_at.asc()).all():
        positions_by_column[card.column_id] = max(positions_by_column[card.column_id], int(card.position or 0))

    content_titles = _get_content_titles_by_job_id(db, job_ids)
    changed = False

    for job, submission, client, site in rows:
        desired_column = columns_by_key[_workflow_column_key_for_status(job.job_status)]
        title_snapshot = _build_workflow_card_title(submission, content_titles.get(job.id, ""))
        card = cards_by_job_id.get(job.id)
        if card is None:
            actor_user_id_for_submission, actor_name_for_submission = _extract_submission_actor(db, submission)
            card = WorkflowCard(
                job_id=job.id,
                submission_id=submission.id,
                client_id=client.id,
                site_id=site.id,
                column_id=desired_column.id,
                card_kind="job",
                column_source="auto",
                position=_next_position(positions_by_column, desired_column.id),
                title_snapshot=title_snapshot,
                job_type="articles",
                request_kind_snapshot=submission.request_kind,
                job_status_snapshot=job.job_status,
                created_by_user_id=actor_user_id_for_submission,
                created_by_name_snapshot=actor_name_for_submission,
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
        actor_user_id_for_submission, actor_name_for_submission = _extract_submission_actor(db, submission)
        next_job_type = _infer_job_type(card, submission)
        next_created_by_user_id = actor_user_id_for_submission
        next_created_by_name = actor_name_for_submission
        if (
            card.submission_id != submission.id
            or card.client_id != client.id
            or card.site_id != site.id
            or (card.job_type or None) != next_job_type
            or card.created_by_user_id != next_created_by_user_id
            or (card.created_by_name_snapshot or None) != next_created_by_name
        ):
            changed = True
        card.submission_id = submission.id
        card.client_id = client.id
        card.site_id = site.id
        card.job_type = next_job_type
        card.created_by_user_id = next_created_by_user_id
        card.created_by_name_snapshot = next_created_by_name
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
        return _ensure_default_workflow_columns(db)
    return columns


def _select_workflow_card_rows(
    db: Session,
) -> list[tuple[WorkflowCard, Optional[Job], Optional[Submission], Optional[Client], Optional[Site]]]:
    return (
        db.query(WorkflowCard, Job, Submission, Client, Site)
        .outerjoin(Job, Job.id == WorkflowCard.job_id)
        .outerjoin(Submission, Submission.id == WorkflowCard.submission_id)
        .outerjoin(Client, Client.id == WorkflowCard.client_id)
        .outerjoin(Site, Site.id == WorkflowCard.site_id)
        .order_by(WorkflowCard.position.asc(), WorkflowCard.created_at.asc())
        .all()
    )


def _load_workflow_card_comments(
    db: Session,
    *,
    card_ids: Sequence[UUID],
    current_user_id: Optional[UUID],
) -> dict[UUID, list[WorkflowCommentOut]]:
    if not card_ids:
        return {}
    rows = (
        db.query(WorkflowCardComment)
        .filter(WorkflowCardComment.card_id.in_(card_ids))
        .order_by(WorkflowCardComment.created_at.asc(), WorkflowCardComment.updated_at.asc())
        .all()
    )
    comments_by_card: dict[UUID, list[WorkflowCommentOut]] = defaultdict(list)
    for row in rows:
        comments_by_card[row.card_id].append(
            WorkflowCommentOut(
                id=row.id,
                author_user_id=row.author_user_id,
                author_name=str(row.author_name_snapshot or "").strip() or "Unknown",
                body=str(row.body or ""),
                created_at=row.created_at,
                updated_at=row.updated_at,
                can_edit=bool(current_user_id and row.author_user_id == current_user_id),
            )
        )
    return comments_by_card


def _build_workflow_board_payload(
    columns: Sequence[WorkflowColumn],
    rows: Iterable[tuple[WorkflowCard, Optional[Job], Optional[Submission], Optional[Client], Optional[Site]]],
    *,
    comments_by_card: dict[UUID, list[WorkflowCommentOut]],
) -> WorkflowBoardOut:
    cards_by_column: Dict[UUID, List[WorkflowCardOut]] = defaultdict(list)
    open_card_count = 0
    completed_card_count = 0
    updated_at = max((item.updated_at for item in columns), default=datetime.now(timezone.utc))
    columns_by_id = {item.id: item for item in columns}

    for card, job, submission, client, site in rows:
        column = columns_by_id.get(card.column_id)
        column_key = column.column_key if column is not None else ""
        title = _build_workflow_card_title(submission, str(card.title_snapshot or ""), str(card.title_snapshot or ""))
        card_comments = comments_by_card.get(card.id, [])
        card_out = WorkflowCardOut(
            id=card.id,
            job_id=job.id if job is not None else None,
            submission_id=submission.id if submission is not None else card.submission_id,
            client_id=client.id if client is not None else card.client_id,
            client_name=(client.name or "").strip() if client is not None else "",
            site_id=site.id if site is not None else card.site_id,
            site_name=(site.name or "").strip() if site is not None else "",
            site_url=(site.site_url or "").strip() if site is not None else "",
            column_id=card.column_id,
            column_key=column_key,
            title=title,
            description=(card.description or "").strip() or None,
            card_kind=(card.card_kind or "job").strip() or "job",
            created_by_name=(card.created_by_name_snapshot or "").strip() or None,
            job_type=_infer_job_type(card, submission),
            flag_type=(card.flag_type or "").strip() or None,
            request_kind=(card.request_kind_snapshot or (submission.request_kind if submission is not None else "")).strip() or None,
            job_status=((job.job_status if job is not None else card.job_status_snapshot) or "manual").strip() or "manual",
            wp_post_url=((job.wp_post_url or "").strip() if job is not None else None) or None,
            last_error=((job.last_error or "").strip() if job is not None else None) or None,
            position=int(card.position or 0),
            created_at=card.created_at,
            updated_at=card.updated_at,
            comments=card_comments,
        )
        cards_by_column[card.column_id].append(card_out)
        if column_key == "done":
            completed_card_count += 1
        else:
            open_card_count += 1
        updated_at = max(updated_at, card.updated_at)
        if job is not None:
            updated_at = max(updated_at, job.updated_at)
        for comment in card_comments:
            updated_at = max(updated_at, comment.updated_at)

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
    columns = _sync_workflow_cards(db, actor_user_id=actor_user_id)
    rows = _select_workflow_card_rows(db)
    comments_by_card = _load_workflow_card_comments(
        db,
        card_ids=[card.id for card, *_ in rows],
        current_user_id=actor_user_id,
    )
    return _build_workflow_board_payload(columns, rows, comments_by_card=comments_by_card)


def _extract_anthropic_text(payload: dict) -> str:
    content = payload.get("content")
    if not isinstance(content, list):
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail="Workflow AI returned invalid content.")
    parts: list[str] = []
    for item in content:
        if not isinstance(item, dict):
            continue
        if item.get("type") != "text":
            continue
        text = item.get("text")
        if isinstance(text, str) and text.strip():
            parts.append(text.strip())
    if not parts:
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail="Workflow AI returned empty content.")
    return "\n".join(parts).strip()


def _rewrite_comment_body_with_haiku(body: str, language: str) -> str:
    api_key = (os.getenv("ANTHROPIC_API_KEY") or "").strip()
    if not api_key:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="ANTHROPIC_API_KEY is not configured.",
        )

    model = (os.getenv("WORKFLOW_COMMENT_ANTHROPIC_MODEL") or WORKFLOW_COMMENT_ANTHROPIC_MODEL).strip()
    base_url = (os.getenv("WORKFLOW_COMMENT_ANTHROPIC_BASE_URL") or WORKFLOW_COMMENT_ANTHROPIC_BASE_URL).strip()
    target_language = "German" if (language or "").strip().lower() == "de" else "English"
    system_prompt = (
        "You rewrite internal workflow comments for an operations board. "
        "Keep the original meaning, facts, names, and URLs. "
        "Make the comment concise, clear, professional, and actionable. "
        "Do not add new claims. Return plain text only."
    )
    user_prompt = (
        f"Rewrite this workflow comment in {target_language}.\n\n"
        "Original comment:\n"
        f"{body.strip()}"
    )
    try:
        response = requests.post(
            f"{base_url.rstrip('/')}/messages",
            headers={
                "x-api-key": api_key,
                "anthropic-version": "2023-06-01",
                "Content-Type": "application/json",
            },
            json={
                "model": model,
                "max_tokens": 300,
                "temperature": 0.2,
                "system": system_prompt,
                "messages": [{"role": "user", "content": user_prompt}],
            },
            timeout=30,
        )
    except requests.RequestException as exc:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"Workflow AI request failed: {exc}",
        ) from exc

    if response.status_code >= 400:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"Workflow AI HTTP {response.status_code}: {response.text[:300]}",
        )
    try:
        payload = response.json()
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Workflow AI returned non-JSON response.",
        ) from exc
    if not isinstance(payload, dict):
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Workflow AI returned unexpected payload type.",
        )
    rewritten = _extract_anthropic_text(payload)
    if not rewritten:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Workflow AI returned empty text.",
        )
    return rewritten


@router.get("/board", response_model=WorkflowBoardOut)
def get_workflow_board(
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin),
) -> WorkflowBoardOut:
    return _load_workflow_board(db, actor_user_id=current_user.id)


@router.post("/cards", response_model=WorkflowBoardOut)
def create_workflow_card(
    payload: WorkflowCardCreateIn,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin),
) -> WorkflowBoardOut:
    columns = _ensure_default_workflow_columns(db)
    todo_column = next((item for item in columns if item.column_key == "todo"), None)
    if todo_column is None:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="TO DO column is missing.")

    client = None
    site = None
    if payload.client_id is not None:
        client = db.query(Client).filter(Client.id == payload.client_id).first()
        if client is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Client not found.")
    if payload.site_id is not None:
        site = db.query(Site).filter(Site.id == payload.site_id).first()
        if site is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Publishing site not found.")

    positions_by_column: Dict[UUID, int] = defaultdict(int)
    for item in db.query(WorkflowCard).order_by(WorkflowCard.position.asc(), WorkflowCard.created_at.asc()).all():
        positions_by_column[item.column_id] = max(positions_by_column[item.column_id], int(item.position or 0))

    card = WorkflowCard(
        job_id=None,
        submission_id=None,
        client_id=client.id if client is not None else None,
        site_id=site.id if site is not None else None,
        column_id=todo_column.id,
        card_kind="manual",
        column_source="manual",
        position=_next_position(positions_by_column, todo_column.id),
        title_snapshot=payload.title,
        description=payload.description,
        job_type=payload.job_type,
        request_kind_snapshot=payload.request_kind,
        job_status_snapshot="manual",
        created_by_user_id=current_user.id,
        created_by_name_snapshot=_build_actor_name(current_user),
    )
    db.add(card)
    db.flush()
    _record_card_event(
        db,
        card=card,
        actor_user_id=current_user.id,
        event_type="manual_created",
        from_column_id=None,
        to_column_id=todo_column.id,
        payload={"request_kind": payload.request_kind},
    )
    db.commit()
    return _load_workflow_board(db, actor_user_id=current_user.id)


@router.patch("/cards/{card_id}/details", response_model=WorkflowBoardOut)
def update_workflow_card_details(
    card_id: UUID,
    payload: WorkflowCardUpdateIn,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin),
) -> WorkflowBoardOut:
    card = db.query(WorkflowCard).filter(WorkflowCard.id == card_id).first()
    if card is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Workflow card not found.")

    next_flag_type = payload.flag_type
    if (card.flag_type or None) != next_flag_type:
        card.flag_type = next_flag_type
        card.updated_at = datetime.now(timezone.utc)
        _record_card_event(
            db,
            card=card,
            actor_user_id=current_user.id,
            event_type="moved",
            from_column_id=card.column_id,
            to_column_id=card.column_id,
            payload={"flag_type": next_flag_type, "reason": "card_details_updated"},
        )
        db.commit()

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
        existing_cards = db.query(WorkflowCard).order_by(WorkflowCard.position.asc(), WorkflowCard.created_at.asc()).all()
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


@router.post("/cards/{card_id}/comments", response_model=WorkflowBoardOut)
def create_workflow_comment(
    card_id: UUID,
    payload: WorkflowCommentCreateIn,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin),
) -> WorkflowBoardOut:
    card = db.query(WorkflowCard).filter(WorkflowCard.id == card_id).first()
    if card is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Workflow card not found.")

    now = datetime.now(timezone.utc)
    comment = WorkflowCardComment(
        card_id=card.id,
        author_user_id=current_user.id,
        author_name_snapshot=_build_actor_name(current_user),
        body=payload.body,
        created_at=now,
        updated_at=now,
    )
    card.updated_at = now
    db.add(comment)
    db.flush()
    _record_card_event(
        db,
        card=card,
        actor_user_id=current_user.id,
        event_type="comment_added",
        from_column_id=card.column_id,
        to_column_id=card.column_id,
        payload={"comment_id": str(comment.id)},
    )
    db.commit()
    return _load_workflow_board(db, actor_user_id=current_user.id)


@router.patch("/comments/{comment_id}", response_model=WorkflowBoardOut)
def update_workflow_comment(
    comment_id: UUID,
    payload: WorkflowCommentUpdateIn,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin),
) -> WorkflowBoardOut:
    comment = db.query(WorkflowCardComment).filter(WorkflowCardComment.id == comment_id).first()
    if comment is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Workflow comment not found.")
    if comment.author_user_id != current_user.id:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="You can only edit your own comments.")

    card = db.query(WorkflowCard).filter(WorkflowCard.id == comment.card_id).first()
    if card is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Workflow card not found.")

    now = datetime.now(timezone.utc)
    comment.body = payload.body
    comment.updated_at = now
    card.updated_at = now
    _record_card_event(
        db,
        card=card,
        actor_user_id=current_user.id,
        event_type="comment_updated",
        from_column_id=card.column_id,
        to_column_id=card.column_id,
        payload={"comment_id": str(comment.id)},
    )
    db.commit()
    return _load_workflow_board(db, actor_user_id=current_user.id)


@router.post("/comments/rewrite", response_model=WorkflowCommentRewriteOut)
def rewrite_workflow_comment(
    payload: WorkflowCommentRewriteIn,
    current_user: User = Depends(require_admin),
) -> WorkflowCommentRewriteOut:
    _ = current_user
    return WorkflowCommentRewriteOut(body=_rewrite_comment_body_with_haiku(payload.body, payload.language))


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

    todo_column = next((item for item in columns if item.column_key == "todo"), None)
    if todo_column is None:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="TO DO column is missing.")

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
        card.column_id = todo_column.id
        card.column_source = "manual"
        card.position = _next_position(positions_by_column, todo_column.id)
        card.updated_at = datetime.now(timezone.utc)
        _record_card_event(
            db,
            card=card,
            actor_user_id=current_user.id,
            event_type="moved",
            from_column_id=previous_column_id,
            to_column_id=todo_column.id,
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
