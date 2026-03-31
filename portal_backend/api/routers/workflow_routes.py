from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
import os
import re
from typing import Dict, Iterable, List, Optional, Sequence
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
import requests
from sqlalchemy import or_
from sqlalchemy.orm import Session

from ..auth import is_super_admin, require_admin, require_super_admin
from ..db import get_db
from ..portal_models import User
from ..workflow_models import WorkflowCard, WorkflowCardComment, WorkflowCardEvent, WorkflowColumn
from ..workflow_schemas import (
    WORKFLOW_FLAG_ORDER,
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

DEFAULT_WORKFLOW_COLUMNS = (
    {"key": "todo", "name": "TO DO", "color": "#5e6c84", "position": 100},
    {"key": "in_progress", "name": "IN PROGRESS", "color": "#0c66e4", "position": 200},
    {"key": "done", "name": "DONE", "color": "#1f845a", "position": 300},
)
SYSTEM_WORKFLOW_COLUMN_KEYS = {item["key"] for item in DEFAULT_WORKFLOW_COLUMNS}
CUSTOM_WORKFLOW_COLUMN_COLOR = "#7c8aa5"
WORKFLOW_COMMENT_ANTHROPIC_BASE_URL = "https://api.anthropic.com/v1"
WORKFLOW_COMMENT_ANTHROPIC_MODEL = "claude-haiku-4-5-20251001"


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


def _build_workflow_card_title(title_snapshot: str) -> str:
    if title_snapshot.strip():
        return title_snapshot.strip()
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


def _normalize_flag_types(flag_types: Optional[Sequence[str]]) -> list[str]:
    seen = {
        str(flag_type or "").strip().lower()
        for flag_type in (flag_types or [])
        if str(flag_type or "").strip()
    }
    return [flag_type for flag_type in WORKFLOW_FLAG_ORDER if flag_type in seen]


def _get_assignable_workflow_user(db: Session, user_id: UUID) -> Optional[User]:
    return (
        db.query(User)
        .filter(
            User.id == user_id,
            User.is_active.is_(True),
            or_(User.role == "admin", User.role == "super_admin"),
        )
        .first()
    )


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


def _next_position(positions_by_column: Dict[UUID, int], column_id: UUID) -> int:
    next_value = positions_by_column.get(column_id, 0) + 100
    positions_by_column[column_id] = next_value
    return next_value


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


def _select_workflow_card_rows(
    db: Session,
) -> list[tuple[WorkflowCard, Optional[User]]]:
    return (
        db.query(WorkflowCard, User)
        .outerjoin(User, User.id == WorkflowCard.assignee_user_id)
        .filter(WorkflowCard.card_kind == "manual")
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
    rows: Iterable[tuple[WorkflowCard, Optional[User]]],
    *,
    comments_by_card: dict[UUID, list[WorkflowCommentOut]],
) -> WorkflowBoardOut:
    cards_by_column: Dict[UUID, List[WorkflowCardOut]] = defaultdict(list)
    open_card_count = 0
    completed_card_count = 0
    updated_at = max((item.updated_at for item in columns), default=datetime.now(timezone.utc))
    columns_by_id = {item.id: item for item in columns}

    for card, assignee in rows:
        column = columns_by_id.get(card.column_id)
        column_key = column.column_key if column is not None else ""
        title = _build_workflow_card_title(str(card.title_snapshot or ""))
        card_comments = comments_by_card.get(card.id, [])
        card_out = WorkflowCardOut(
            id=card.id,
            job_id=None,
            submission_id=card.submission_id,
            column_id=card.column_id,
            column_key=column_key,
            title=title,
            description=(card.description or "").strip() or None,
            card_kind=(card.card_kind or "manual").strip() or "manual",
            created_by_name=(card.created_by_name_snapshot or "").strip() or None,
            assignee_user_id=card.assignee_user_id,
            assignee_name=_build_actor_name(assignee) if assignee is not None else None,
            job_type=(card.job_type or "").strip() or None,
            priority=(card.priority or "medium").strip() or "medium",
            flag_types=_normalize_flag_types(card.flag_types),
            request_kind=(card.request_kind_snapshot or "manual").strip() or "manual",
            job_status=(card.job_status_snapshot or "manual").strip() or "manual",
            wp_post_url=None,
            last_error=None,
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
    columns = _ensure_default_workflow_columns(db)
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

    assignee = _get_assignable_workflow_user(db, payload.assignee_user_id)
    if assignee is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Assignee not found.")

    positions_by_column: Dict[UUID, int] = defaultdict(int)
    for item in db.query(WorkflowCard).order_by(WorkflowCard.position.asc(), WorkflowCard.created_at.asc()).all():
        positions_by_column[item.column_id] = max(positions_by_column[item.column_id], int(item.position or 0))

    card = WorkflowCard(
        job_id=None,
        submission_id=None,
        column_id=todo_column.id,
        card_kind="manual",
        column_source="manual",
        position=_next_position(positions_by_column, todo_column.id),
        title_snapshot=payload.title,
        description=payload.description,
        job_type=payload.job_type,
        priority=payload.priority,
        assignee_user_id=assignee.id,
        request_kind_snapshot="manual",
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
        payload={
            "request_kind": "manual",
            "priority": payload.priority,
            "assignee_user_id": str(assignee.id),
        },
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

    full_edit_fields = {"title", "description", "job_type", "priority", "assignee_user_id"}
    requested_fields = set(payload.__fields_set__)
    is_full_edit = bool(requested_fields & full_edit_fields)
    if is_full_edit and not is_super_admin(current_user):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Super admin access required.")

    changed = False
    if payload.title is not None and card.title_snapshot != payload.title:
        card.title_snapshot = payload.title
        changed = True

    if "description" in payload.__fields_set__:
        next_description = payload.description
        current_description = (card.description or "").strip() or None
        if current_description != next_description:
            card.description = next_description
            changed = True

    if payload.job_type is not None and (card.job_type or None) != payload.job_type:
        card.job_type = payload.job_type
        changed = True

    if payload.priority is not None and (card.priority or "medium") != payload.priority:
        card.priority = payload.priority
        changed = True

    if payload.assignee_user_id is not None and card.assignee_user_id != payload.assignee_user_id:
        assignee = _get_assignable_workflow_user(db, payload.assignee_user_id)
        if assignee is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Assignee not found.")
        card.assignee_user_id = assignee.id
        changed = True

    if "flag_types" in payload.__fields_set__:
        next_flag_types = _normalize_flag_types(payload.flag_types)
    else:
        next_flag_types = _normalize_flag_types(card.flag_types)
    if _normalize_flag_types(card.flag_types) != next_flag_types:
        card.flag_types = next_flag_types
        changed = True

    if changed:
        card.updated_at = datetime.now(timezone.utc)
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


@router.delete("/cards/{card_id}", response_model=WorkflowBoardOut)
def delete_workflow_card(
    card_id: UUID,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_super_admin),
) -> WorkflowBoardOut:
    card = db.query(WorkflowCard).filter(WorkflowCard.id == card_id).first()
    if card is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Workflow card not found.")
    db.delete(card)
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
