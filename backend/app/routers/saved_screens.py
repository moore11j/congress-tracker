from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel, Field
from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from app.auth import current_user
from app.db import get_db
from app.entitlements import current_entitlements, enforce_limit, require_feature
from app.models import SavedScreen, SavedScreenEvent, SavedScreenSnapshot
from app.services.saved_screen_monitoring import (
    event_to_dict,
    refresh_due_saved_screen_monitoring,
    refresh_saved_screen_monitoring,
    saved_screen_payload,
)

router = APIRouter(tags=["saved-screens"])


class SavedScreenCreatePayload(BaseModel):
    name: str = Field(min_length=1, max_length=160)
    params: dict[str, Any] = Field(default_factory=dict)
    last_viewed_at: datetime | None = None


class SavedScreenUpdatePayload(BaseModel):
    name: str | None = Field(default=None, min_length=1, max_length=160)
    params: dict[str, Any] | None = None
    last_viewed_at: datetime | None = None


def _require_screen_owner(db: Session, request: Request, saved_screen_id: int) -> SavedScreen:
    user = current_user(db, request, required=True)
    screen = db.get(SavedScreen, saved_screen_id)
    if screen is None or screen.user_id != user.id:
        raise HTTPException(status_code=404, detail="Saved screen not found.")
    return screen


@router.get("/saved-screens")
def list_saved_screens(
    request: Request,
    db: Session = Depends(get_db),
):
    user = current_user(db, request, required=True)
    rows = (
        db.execute(
            select(SavedScreen)
            .where(SavedScreen.user_id == user.id)
            .order_by(SavedScreen.updated_at.desc(), SavedScreen.id.desc())
        )
        .scalars()
        .all()
    )
    return {"items": [saved_screen_payload(row) for row in rows]}


@router.post("/saved-screens")
def create_saved_screen(
    payload: SavedScreenCreatePayload,
    request: Request,
    db: Session = Depends(get_db),
):
    user = current_user(db, request, required=True)
    entitlements = current_entitlements(request, db)
    require_feature(entitlements, "saved_views", message="Saved screens are included with your saved views entitlement.")
    existing_count = db.execute(select(SavedScreen).where(SavedScreen.user_id == user.id)).scalars().all()
    enforce_limit(
        entitlements,
        "saved_views",
        current_count=len(existing_count),
        message="Saved screen limit reached for this plan.",
    )
    now = datetime.now(timezone.utc)
    screen = SavedScreen(
        user_id=user.id,
        name=payload.name.strip(),
        params_json=json.dumps(payload.params, sort_keys=True),
    )
    screen.last_viewed_at = _coerce_utc(payload.last_viewed_at) or now
    db.add(screen)
    db.flush()
    result = refresh_saved_screen_monitoring(db, screen, now=now)
    db.commit()
    response = saved_screen_payload(screen)
    response["monitoring"] = result
    return response


@router.patch("/saved-screens/{saved_screen_id}")
def update_saved_screen(
    saved_screen_id: int,
    payload: SavedScreenUpdatePayload,
    request: Request,
    db: Session = Depends(get_db),
):
    screen = _require_screen_owner(db, request, saved_screen_id)
    fields = payload.model_fields_set
    reset_monitoring = False

    if "name" in fields and payload.name is not None:
        screen.name = payload.name.strip()
    if "params" in fields and payload.params is not None:
        screen.params_json = json.dumps(payload.params, sort_keys=True)
        screen.last_refreshed_at = None
        reset_monitoring = True
    if "last_viewed_at" in fields:
        screen.last_viewed_at = _coerce_utc(payload.last_viewed_at) or datetime.now(timezone.utc)

    if reset_monitoring:
        db.execute(delete(SavedScreenSnapshot).where(SavedScreenSnapshot.saved_screen_id == screen.id))
        db.execute(delete(SavedScreenEvent).where(SavedScreenEvent.saved_screen_id == screen.id))

    db.flush()
    if reset_monitoring:
        result = refresh_saved_screen_monitoring(db, screen)
    else:
        result = None
    db.commit()
    response = saved_screen_payload(screen)
    if result is not None:
        response["monitoring"] = result
    return response


@router.delete("/saved-screens/{saved_screen_id}", status_code=204)
def delete_saved_screen(
    saved_screen_id: int,
    request: Request,
    db: Session = Depends(get_db),
):
    screen = _require_screen_owner(db, request, saved_screen_id)
    db.execute(delete(SavedScreenSnapshot).where(SavedScreenSnapshot.saved_screen_id == screen.id))
    db.execute(delete(SavedScreenEvent).where(SavedScreenEvent.saved_screen_id == screen.id))
    db.delete(screen)
    db.commit()
    return None


@router.get("/saved-screens/events")
def list_saved_screen_events(
    request: Request,
    db: Session = Depends(get_db),
    limit: int = Query(20, ge=1, le=100),
):
    user = current_user(db, request, required=True)
    screens = (
        db.execute(select(SavedScreen).where(SavedScreen.user_id == user.id))
        .scalars()
        .all()
    )
    if not screens:
        return {"items": []}
    screen_names = {screen.id: screen.name for screen in screens}
    rows = (
        db.execute(
            select(SavedScreenEvent)
            .where(SavedScreenEvent.user_id == user.id)
            .order_by(SavedScreenEvent.created_at.desc(), SavedScreenEvent.id.desc())
            .limit(limit)
        )
        .scalars()
        .all()
    )
    return {"items": [event_to_dict(row, screen_name=screen_names.get(row.saved_screen_id)) for row in rows]}


@router.post("/saved-screens/monitoring/refresh")
def refresh_saved_screens_monitoring(
    request: Request,
    db: Session = Depends(get_db),
    saved_screen_id: int | None = None,
):
    user = current_user(db, request, required=True)
    if saved_screen_id is not None:
        screen = _require_screen_owner(db, request, saved_screen_id)
        result = refresh_saved_screen_monitoring(db, screen)
    else:
        result = refresh_due_saved_screen_monitoring(db, user_id=user.id)
    db.commit()
    return result


def _coerce_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
