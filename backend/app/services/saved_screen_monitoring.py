from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import and_, func, or_, select
from sqlalchemy.orm import Session

from app.models import SavedScreen, SavedScreenEvent, SavedScreenSnapshot
from app.services.screener import MAX_FETCH_ROWS, ScreenerParams, build_screener_rows, screener_params_from_mapping

SCREEN_EVENT_COOLDOWN = timedelta(hours=24)
SCREEN_REFRESH_INTERVAL = timedelta(minutes=60)

BAND_RANK = {
    "inactive": 0,
    "weak": 1,
    "moderate": 2,
    "strong": 3,
    "exceptional": 4,
}


@dataclass(frozen=True)
class SavedScreenState:
    ticker: str
    confirmation_score: int
    confirmation_band: str
    direction: str
    source_count: int
    why_now_state: str
    observed_at: datetime

    def as_dict(self) -> dict[str, Any]:
        return {
            "ticker": self.ticker,
            "confirmation_score": self.confirmation_score,
            "confirmation_band": self.confirmation_band,
            "direction": self.direction,
            "source_count": self.source_count,
            "why_now_state": self.why_now_state,
            "observed_at": self.observed_at.isoformat(),
        }


@dataclass(frozen=True)
class SavedScreenEventDecision:
    event_type: str
    title: str
    description: str
    before: dict[str, Any] | None
    after: dict[str, Any] | None


def saved_screen_payload(screen: SavedScreen) -> dict[str, Any]:
    return {
        "id": screen.id,
        "name": screen.name,
        "params": _loads_dict(screen.params_json),
        "last_viewed_at": screen.last_viewed_at,
        "last_refreshed_at": screen.last_refreshed_at,
        "created_at": screen.created_at,
        "updated_at": screen.updated_at,
    }


def event_to_dict(event: SavedScreenEvent, *, screen_name: str | None = None) -> dict[str, Any]:
    return {
        "id": event.id,
        "saved_screen_id": event.saved_screen_id,
        "screen_name": screen_name,
        "ticker": event.ticker,
        "event_type": event.event_type,
        "title": event.title,
        "description": event.description,
        "before_snapshot": _loads_dict_or_none(event.before_json),
        "after_snapshot": _loads_dict_or_none(event.after_json),
        "created_at": event.created_at,
    }


def refresh_saved_screen_monitoring(
    db: Session,
    screen: SavedScreen,
    *,
    now: datetime | None = None,
) -> dict[str, Any]:
    observed_at = now or datetime.now(timezone.utc)
    params = _params_from_saved_screen(screen)
    rows = build_screener_rows(db, params, requested_rows=MAX_FETCH_ROWS)
    membership_changes_allowed = len(rows) < MAX_FETCH_ROWS
    current_states = {
        state.ticker: state
        for state in (_state_from_row(row, observed_at=observed_at) for row in rows)
    }
    snapshots = _snapshot_map(db, screen)

    initialized = 0
    generated = 0
    deduped = 0
    items: list[dict[str, Any]] = []

    if not snapshots:
        for state in current_states.values():
            db.add(_snapshot_from_state(screen=screen, state=state))
            initialized += 1
        screen.last_refreshed_at = observed_at
        return {
            "screen_id": screen.id,
            "initialized": initialized,
            "generated": 0,
            "deduped": 0,
            "items": [],
            "membership_changes_allowed": membership_changes_allowed,
        }

    for ticker, after in current_states.items():
        snapshot = snapshots.get(ticker)
        if snapshot is None:
            if membership_changes_allowed:
                decision = _entry_decision(screen.name, after)
                if _recent_duplicate_exists(db, screen=screen, ticker=ticker, decision=decision, now=observed_at):
                    deduped += 1
                else:
                    event = _event_from_decision(screen=screen, ticker=ticker, decision=decision, observed_at=observed_at)
                    db.add(event)
                    db.flush()
                    generated += 1
                    items.append(event_to_dict(event, screen_name=screen.name))
                db.add(_snapshot_from_state(screen=screen, state=after))
            continue

        before = _state_from_snapshot(snapshot)
        decision = decide_saved_screen_event(before, after, screen_name=screen.name)
        if decision is not None:
            if _recent_duplicate_exists(db, screen=screen, ticker=ticker, decision=decision, now=observed_at):
                deduped += 1
            else:
                event = _event_from_decision(screen=screen, ticker=ticker, decision=decision, observed_at=observed_at)
                db.add(event)
                db.flush()
                generated += 1
                items.append(event_to_dict(event, screen_name=screen.name))
        _apply_state_to_snapshot(snapshot, after)

    if membership_changes_allowed:
        for ticker, snapshot in snapshots.items():
            if ticker in current_states:
                continue
            before = _state_from_snapshot(snapshot)
            decision = _exit_decision(screen.name, before)
            if _recent_duplicate_exists(db, screen=screen, ticker=ticker, decision=decision, now=observed_at):
                deduped += 1
            else:
                event = _event_from_decision(screen=screen, ticker=ticker, decision=decision, observed_at=observed_at)
                db.add(event)
                db.flush()
                generated += 1
                items.append(event_to_dict(event, screen_name=screen.name))
            db.delete(snapshot)

    screen.last_refreshed_at = observed_at
    return {
        "screen_id": screen.id,
        "initialized": initialized,
        "generated": generated,
        "deduped": deduped,
        "items": items,
        "membership_changes_allowed": membership_changes_allowed,
    }


def refresh_due_saved_screen_monitoring(
    db: Session,
    *,
    now: datetime | None = None,
    limit: int = 25,
    user_id: int | None = None,
) -> dict[str, Any]:
    observed_at = now or datetime.now(timezone.utc)
    cutoff = observed_at - SCREEN_REFRESH_INTERVAL

    q = select(SavedScreen).where(
        or_(
            SavedScreen.last_refreshed_at.is_(None),
            SavedScreen.last_refreshed_at <= cutoff,
        )
    )
    if user_id is not None:
        q = q.where(SavedScreen.user_id == user_id)
    screens = db.execute(q.order_by(SavedScreen.last_refreshed_at.asc(), SavedScreen.id.asc()).limit(limit)).scalars().all()
    refreshed = 0
    initialized = 0
    generated = 0
    deduped = 0
    items: list[dict[str, Any]] = []
    for screen in screens:
        result = refresh_saved_screen_monitoring(db, screen, now=observed_at)
        refreshed += 1
        initialized += int(result.get("initialized") or 0)
        generated += int(result.get("generated") or 0)
        deduped += int(result.get("deduped") or 0)
        items.extend(result.get("items") or [])

    return {
        "refreshed": refreshed,
        "initialized": initialized,
        "generated": generated,
        "deduped": deduped,
        "items": items,
    }


def decide_saved_screen_event(
    before: SavedScreenState,
    after: SavedScreenState,
    *,
    screen_name: str,
) -> SavedScreenEventDecision | None:
    if before.direction in {"bullish", "bearish"} and after.direction in {"bullish", "bearish"} and before.direction != after.direction:
        return SavedScreenEventDecision(
            event_type="direction_changed",
            title=f"{after.ticker} flipped from {before.direction} to {after.direction}",
            description=f"Direction changed while {after.ticker} stayed inside your '{screen_name}' screen.",
            before=before.as_dict(),
            after=after.as_dict(),
        )

    if before.source_count < 2 and after.source_count >= 2:
        return SavedScreenEventDecision(
            event_type="multi_source_gained",
            title=f"{after.ticker} gained multi-source confirmation",
            description=f"Active confirmation sources increased from {before.source_count} to {after.source_count} in '{screen_name}'.",
            before=before.as_dict(),
            after=after.as_dict(),
        )

    if before.source_count >= 2 and after.source_count < 2:
        return SavedScreenEventDecision(
            event_type="multi_source_lost",
            title=f"{after.ticker} lost multi-source confirmation",
            description=f"Active confirmation sources fell from {before.source_count} to {after.source_count} in '{screen_name}'.",
            before=before.as_dict(),
            after=after.as_dict(),
        )

    score_delta = after.confirmation_score - before.confirmation_score
    if score_delta >= 15 or BAND_RANK.get(after.confirmation_band, 0) > BAND_RANK.get(before.confirmation_band, 0):
        direction_label = f" {after.direction}" if after.direction in {"bullish", "bearish"} else ""
        return SavedScreenEventDecision(
            event_type="confirmation_upgraded",
            title=f"{after.ticker} upgraded to {after.confirmation_band}{direction_label} confirmation",
            description=(
                f"Confirmation improved from {before.confirmation_band} ({before.confirmation_score}) "
                f"to {after.confirmation_band} ({after.confirmation_score}) in '{screen_name}'."
            ),
            before=before.as_dict(),
            after=after.as_dict(),
        )

    if before.why_now_state != after.why_now_state:
        return SavedScreenEventDecision(
            event_type="why_now_changed",
            title=f"{after.ticker} moved from {before.why_now_state} to {after.why_now_state}",
            description=f"Why Now shifted while {after.ticker} remained in your '{screen_name}' screen.",
            before=before.as_dict(),
            after=after.as_dict(),
        )

    return None


def _entry_decision(screen_name: str, after: SavedScreenState) -> SavedScreenEventDecision:
    direction_label = f" {after.direction}" if after.direction in {"bullish", "bearish"} else ""
    return SavedScreenEventDecision(
        event_type="entered_screen",
        title=f"{after.ticker} entered your '{screen_name}' screen",
        description=f"Now matches this screen with {after.confirmation_band}{direction_label} confirmation.",
        before=None,
        after=after.as_dict(),
    )


def _exit_decision(screen_name: str, before: SavedScreenState) -> SavedScreenEventDecision:
    return SavedScreenEventDecision(
        event_type="exited_screen",
        title=f"{before.ticker} exited your '{screen_name}' screen",
        description="No longer matches this screen's filters.",
        before=before.as_dict(),
        after=None,
    )


def _snapshot_map(db: Session, screen: SavedScreen) -> dict[str, SavedScreenSnapshot]:
    rows = (
        db.execute(
            select(SavedScreenSnapshot)
            .where(SavedScreenSnapshot.user_id == screen.user_id)
            .where(SavedScreenSnapshot.saved_screen_id == screen.id)
        )
        .scalars()
        .all()
    )
    return {row.ticker.upper(): row for row in rows}


def _snapshot_from_state(*, screen: SavedScreen, state: SavedScreenState) -> SavedScreenSnapshot:
    return SavedScreenSnapshot(
        user_id=screen.user_id,
        saved_screen_id=screen.id,
        ticker=state.ticker,
        confirmation_score=state.confirmation_score,
        confirmation_band=state.confirmation_band,
        direction=state.direction,
        source_count=state.source_count,
        why_now_state=state.why_now_state,
        observed_at=state.observed_at,
    )


def _state_from_snapshot(snapshot: SavedScreenSnapshot) -> SavedScreenState:
    return SavedScreenState(
        ticker=snapshot.ticker.upper(),
        confirmation_score=int(snapshot.confirmation_score or 0),
        confirmation_band=snapshot.confirmation_band or "inactive",
        direction=snapshot.direction or "neutral",
        source_count=int(snapshot.source_count or 0),
        why_now_state=snapshot.why_now_state or "inactive",
        observed_at=snapshot.observed_at,
    )


def _apply_state_to_snapshot(snapshot: SavedScreenSnapshot, state: SavedScreenState) -> None:
    snapshot.confirmation_score = state.confirmation_score
    snapshot.confirmation_band = state.confirmation_band
    snapshot.direction = state.direction
    snapshot.source_count = state.source_count
    snapshot.why_now_state = state.why_now_state
    snapshot.observed_at = state.observed_at
    snapshot.updated_at = state.observed_at


def _state_from_row(row: dict[str, Any], *, observed_at: datetime) -> SavedScreenState:
    confirmation = row.get("confirmation") if isinstance(row.get("confirmation"), dict) else {}
    why_now = row.get("why_now") if isinstance(row.get("why_now"), dict) else {}
    ticker = str(row.get("symbol") or "").strip().upper()
    return SavedScreenState(
        ticker=ticker,
        confirmation_score=int(confirmation.get("score") or 0),
        confirmation_band=str(confirmation.get("band") or "inactive"),
        direction=str(confirmation.get("direction") or "neutral"),
        source_count=int(confirmation.get("source_count") or 0),
        why_now_state=str(why_now.get("state") or "inactive"),
        observed_at=observed_at,
    )


def _event_from_decision(
    *,
    screen: SavedScreen,
    ticker: str,
    decision: SavedScreenEventDecision,
    observed_at: datetime,
) -> SavedScreenEvent:
    return SavedScreenEvent(
        user_id=screen.user_id,
        saved_screen_id=screen.id,
        ticker=ticker,
        event_type=decision.event_type,
        title=decision.title,
        description=decision.description,
        before_json=json.dumps(decision.before, sort_keys=True) if decision.before is not None else None,
        after_json=json.dumps(decision.after, sort_keys=True) if decision.after is not None else None,
        created_at=observed_at,
    )


def _recent_duplicate_exists(
    db: Session,
    *,
    screen: SavedScreen,
    ticker: str,
    decision: SavedScreenEventDecision,
    now: datetime,
) -> bool:
    cutoff = now - SCREEN_EVENT_COOLDOWN
    recent = (
        db.execute(
            select(SavedScreenEvent)
            .where(
                and_(
                    SavedScreenEvent.user_id == screen.user_id,
                    SavedScreenEvent.saved_screen_id == screen.id,
                    func.upper(SavedScreenEvent.ticker) == ticker.upper(),
                    SavedScreenEvent.event_type == decision.event_type,
                    SavedScreenEvent.created_at >= cutoff,
                )
            )
            .order_by(SavedScreenEvent.created_at.desc(), SavedScreenEvent.id.desc())
            .limit(1)
        )
        .scalars()
        .first()
    )
    if recent is None:
        return False
    return (
        _loads_dict_or_none(recent.before_json) == decision.before
        and _loads_dict_or_none(recent.after_json) == decision.after
    )


def _params_from_saved_screen(screen: SavedScreen) -> ScreenerParams:
    return screener_params_from_mapping(_loads_dict(screen.params_json), page=1, page_size=100)


def _loads_dict(raw: str | None) -> dict[str, Any]:
    try:
        parsed = json.loads(raw or "{}")
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _loads_dict_or_none(raw: str | None) -> dict[str, Any] | None:
    if not raw:
        return None
    parsed = _loads_dict(raw)
    return parsed or {}
