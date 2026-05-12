from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import and_, func, or_, select
from sqlalchemy.orm import Session

from app.entitlements import entitlements_for_user, monitored_source_ids
from app.models import AppSetting, SavedScreen, SavedScreenEvent, SavedScreenSnapshot
from app.services.confirmation_score import normalize_confirmation_state
from app.services.screener import MAX_FETCH_ROWS, ScreenerParams, build_screener_rows, screener_params_from_mapping
from app.services.screener import confirmation_filter_diagnostics, matches_confirmation_filters

SCREEN_EVENT_COOLDOWN = timedelta(hours=24)
SCREEN_REFRESH_INTERVAL = timedelta(minutes=60)
SAVED_SCREEN_MONITORING_BASELINE_VERSION = "confirmation_filters_v2"
SCREEN_MEMBERSHIP_FLOOD_THRESHOLD = 25
SCREEN_MEMBERSHIP_FLOOD_RATIO = 0.30
logger = logging.getLogger(__name__)

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
    confirmation_status: str
    source_count: int
    why_now_state: str
    observed_at: datetime

    def as_dict(self) -> dict[str, Any]:
        return {
            "ticker": self.ticker,
            "confirmation_score": self.confirmation_score,
            "confirmation_band": self.confirmation_band,
            "direction": self.direction,
            "confirmation_status": self.confirmation_status,
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
    raw_states = {
        state.ticker: state
        for state in (_state_from_row(row, observed_at=observed_at) for row in rows)
    }
    current_states: dict[str, SavedScreenState] = {}
    for ticker, state in raw_states.items():
        if _state_allowed_for_screen(params, state, screen=screen):
            current_states[ticker] = state
            continue
        diagnostics = _confirmation_diagnostics_for_state(params, state)
        _log_monitoring_decision(
            screen=screen,
            ticker=ticker,
            before=None,
            after=state,
            previous_matched=False,
            current_matched=False,
            reason=str(diagnostics.get("reason") or "excluded_by_saved_screen_filters"),
            diagnostics=diagnostics,
        )
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
        _set_saved_screen_monitoring_baseline_version(db, screen, SAVED_SCREEN_MONITORING_BASELINE_VERSION)
        return {
            "screen_id": screen.id,
            "initialized": initialized,
            "generated": 0,
            "deduped": 0,
            "items": [],
            "membership_changes_allowed": membership_changes_allowed,
        }

    baseline_version = _saved_screen_monitoring_baseline_version(db, screen)
    if baseline_version != SAVED_SCREEN_MONITORING_BASELINE_VERSION:
        _reset_saved_screen_baseline(
            db,
            screen=screen,
            snapshots=snapshots,
            current_states=current_states,
            observed_at=observed_at,
        )
        _set_saved_screen_monitoring_baseline_version(db, screen, SAVED_SCREEN_MONITORING_BASELINE_VERSION)
        logger.info(
            "saved_screen_monitoring_baseline_reset screen_id=%s screen_name=%r previous_version=%r current_version=%r reason=version_mismatch snapshot_count=%s current_count=%s",
            screen.id,
            screen.name,
            baseline_version,
            SAVED_SCREEN_MONITORING_BASELINE_VERSION,
            len(snapshots),
            len(current_states),
        )
        return {
            "screen_id": screen.id,
            "initialized": 0,
            "generated": 0,
            "deduped": 0,
            "items": [],
            "membership_changes_allowed": membership_changes_allowed,
            "baseline_reset": True,
            "baseline_reset_reason": "version_mismatch",
        }

    if membership_changes_allowed:
        entry_count = sum(1 for ticker in current_states if ticker not in snapshots)
        exit_count = sum(1 for ticker in snapshots if ticker not in current_states)
        if _should_collapse_membership_changes(
            previous_count=len(snapshots),
            current_count=len(current_states),
            entry_count=entry_count,
            exit_count=exit_count,
        ):
            decision = _refresh_summary_decision(
                screen_name=screen.name,
                previous_count=len(snapshots),
                current_count=len(current_states),
                entry_count=entry_count,
                exit_count=exit_count,
            )
            if _recent_duplicate_exists(db, screen=screen, ticker="", decision=decision, now=observed_at):
                deduped += 1
            else:
                event = _event_from_decision(screen=screen, ticker="", decision=decision, observed_at=observed_at)
                db.add(event)
                db.flush()
                from app.services.monitoring_alerts import ensure_alert_for_saved_screen_event

                ensure_alert_for_saved_screen_event(db, event=event, screen=screen)
                generated += 1
                items.append(event_to_dict(event, screen_name=screen.name))
            _reset_saved_screen_baseline(
                db,
                screen=screen,
                snapshots=snapshots,
                current_states=current_states,
                observed_at=observed_at,
            )
            _set_saved_screen_monitoring_baseline_version(db, screen, SAVED_SCREEN_MONITORING_BASELINE_VERSION)
            logger.info(
                "saved_screen_monitoring_membership_wave_collapsed screen_id=%s screen_name=%r previous_count=%s current_count=%s entry_count=%s exit_count=%s",
                screen.id,
                screen.name,
                len(snapshots),
                len(current_states),
                entry_count,
                exit_count,
            )
            return {
                "screen_id": screen.id,
                "initialized": 0,
                "generated": generated,
                "deduped": deduped,
                "items": items,
                "membership_changes_allowed": membership_changes_allowed,
                "baseline_reset": True,
                "baseline_reset_reason": "membership_wave",
            }

    for ticker, after in current_states.items():
        snapshot = snapshots.get(ticker)
        if snapshot is None:
            if membership_changes_allowed:
                decision = _entry_decision(screen.name, after)
                _log_monitoring_decision(
                    screen=screen,
                    ticker=ticker,
                    before=None,
                    after=after,
                    previous_matched=False,
                    current_matched=True,
                    reason=decision.event_type,
                )
                if _recent_duplicate_exists(db, screen=screen, ticker=ticker, decision=decision, now=observed_at):
                    deduped += 1
                else:
                    event = _event_from_decision(screen=screen, ticker=ticker, decision=decision, observed_at=observed_at)
                    db.add(event)
                    db.flush()
                    from app.services.monitoring_alerts import ensure_alert_for_saved_screen_event

                    ensure_alert_for_saved_screen_event(db, event=event, screen=screen)
                    generated += 1
                    items.append(event_to_dict(event, screen_name=screen.name))
                db.add(_snapshot_from_state(screen=screen, state=after))
            continue

        before = _state_from_snapshot(snapshot)
        decision = decide_saved_screen_event(before, after, screen_name=screen.name)
        if decision is not None:
            _log_monitoring_decision(
                screen=screen,
                ticker=ticker,
                before=before,
                after=after,
                previous_matched=True,
                current_matched=True,
                reason=decision.event_type,
            )
            if _recent_duplicate_exists(db, screen=screen, ticker=ticker, decision=decision, now=observed_at):
                deduped += 1
            else:
                event = _event_from_decision(screen=screen, ticker=ticker, decision=decision, observed_at=observed_at)
                db.add(event)
                db.flush()
                from app.services.monitoring_alerts import ensure_alert_for_saved_screen_event

                ensure_alert_for_saved_screen_event(db, event=event, screen=screen)
                generated += 1
                items.append(event_to_dict(event, screen_name=screen.name))
        _apply_state_to_snapshot(snapshot, after)

    if membership_changes_allowed:
        for ticker, snapshot in snapshots.items():
            if ticker in current_states:
                continue
            before = _state_from_snapshot(snapshot)
            decision = _exit_decision(screen.name, before, after=raw_states.get(ticker))
            _log_monitoring_decision(
                screen=screen,
                ticker=ticker,
                before=before,
                after=raw_states.get(ticker),
                previous_matched=True,
                current_matched=False,
                reason=decision.event_type,
            )
            if _recent_duplicate_exists(db, screen=screen, ticker=ticker, decision=decision, now=observed_at):
                deduped += 1
            else:
                event = _event_from_decision(screen=screen, ticker=ticker, decision=decision, observed_at=observed_at)
                db.add(event)
                db.flush()
                from app.services.monitoring_alerts import ensure_alert_for_saved_screen_event

                ensure_alert_for_saved_screen_event(db, event=event, screen=screen)
                generated += 1
                items.append(event_to_dict(event, screen_name=screen.name))
            db.delete(snapshot)

    screen.last_refreshed_at = observed_at
    _set_saved_screen_monitoring_baseline_version(db, screen, SAVED_SCREEN_MONITORING_BASELINE_VERSION)
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

    from app.models import UserAccount

    q = select(SavedScreen).where(
        or_(
            SavedScreen.last_refreshed_at.is_(None),
            SavedScreen.last_refreshed_at <= cutoff,
        )
    )
    if user_id is not None:
        q = q.where(SavedScreen.user_id == user_id)
    screens = db.execute(q.order_by(SavedScreen.last_refreshed_at.asc(), SavedScreen.id.asc()).limit(limit)).scalars().all()
    if screens:
        user_ids = sorted({screen.user_id for screen in screens})
        users = {
            row.id: row
            for row in db.execute(select(UserAccount).where(UserAccount.id.in_(user_ids))).scalars().all()
        }
        eligible_screens: list[SavedScreen] = []
        for screen in screens:
            user = users.get(screen.user_id)
            if user is None:
                continue
            entitlements = entitlements_for_user(db, user)
            if not entitlements.has_feature("screener_monitoring"):
                continue
            if screen.id not in monitored_source_ids(db, user_id=user.id, entitlements=entitlements)["saved_screen_ids"]:
                continue
            eligible_screens.append(screen)
        screens = eligible_screens
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


def _exit_decision(screen_name: str, before: SavedScreenState, *, after: SavedScreenState | None = None) -> SavedScreenEventDecision:
    description = "No longer matches this screen's filters."
    if after is not None:
        if before.direction in {"bullish", "bearish"} and after.direction in {"bullish", "bearish"} and before.direction != after.direction:
            description = f"Direction changed from {before.direction} to {after.direction}."
        elif after.confirmation_status != "active":
            description = "No longer has active confirmation required by this screen."
    return SavedScreenEventDecision(
        event_type="exited_screen",
        title=f"{before.ticker} exited your '{screen_name}' screen",
        description=description,
        before=before.as_dict(),
        after=None,
    )


def _refresh_summary_decision(
    *,
    screen_name: str,
    previous_count: int,
    current_count: int,
    entry_count: int,
    exit_count: int,
) -> SavedScreenEventDecision:
    return SavedScreenEventDecision(
        event_type="screen_refreshed",
        title=f"{screen_name} screen refreshed",
        description=(
            "Results changed significantly in this refresh. "
            f"{exit_count} ticker{'s' if exit_count != 1 else ''} no longer match and "
            f"{entry_count} ticker{'s' if entry_count != 1 else ''} now match, "
            "so individual updates were collapsed into this summary."
        ),
        before={"matched_count": previous_count},
        after={
            "matched_count": current_count,
            "entered_count": entry_count,
            "exited_count": exit_count,
            "refresh_type": "collapsed_membership_wave",
        },
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
        confirmation_status=_normalized_status_from_parts(
            score=int(snapshot.confirmation_score or 0),
            band=snapshot.confirmation_band or "inactive",
            direction=snapshot.direction or "neutral",
            source_count=int(snapshot.source_count or 0),
            why_now_state=snapshot.why_now_state or "inactive",
        ),
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


def _reset_saved_screen_baseline(
    db: Session,
    *,
    screen: SavedScreen,
    snapshots: dict[str, SavedScreenSnapshot],
    current_states: dict[str, SavedScreenState],
    observed_at: datetime,
) -> None:
    for ticker, snapshot in snapshots.items():
        state = current_states.get(ticker)
        if state is None:
            db.delete(snapshot)
            continue
        _apply_state_to_snapshot(snapshot, state)

    for ticker, state in current_states.items():
        if ticker not in snapshots:
            db.add(_snapshot_from_state(screen=screen, state=state))

    screen.last_refreshed_at = observed_at


def _state_from_row(row: dict[str, Any], *, observed_at: datetime) -> SavedScreenState:
    confirmation = row.get("confirmation") if isinstance(row.get("confirmation"), dict) else {}
    why_now = row.get("why_now") if isinstance(row.get("why_now"), dict) else {}
    ticker = str(row.get("symbol") or "").strip().upper()
    normalized = normalize_confirmation_state(confirmation, why_now=why_now)
    return SavedScreenState(
        ticker=ticker,
        confirmation_score=int(normalized.score or 0),
        confirmation_band=str(normalized.band or "inactive"),
        direction=str(normalized.direction or "neutral"),
        confirmation_status=normalized.status,
        source_count=int(normalized.source_count or 0),
        why_now_state=str(why_now.get("state") or "inactive"),
        observed_at=observed_at,
    )


def _normalized_status_from_parts(
    *,
    score: int,
    band: str,
    direction: str,
    source_count: int,
    why_now_state: str,
) -> str:
    return normalize_confirmation_state(
        {
            "score": score,
            "band": band,
            "direction": direction,
            "source_count": source_count,
        },
        why_now={"state": why_now_state},
    ).status


def _state_allowed_for_screen(params: ScreenerParams, state: SavedScreenState, *, screen: SavedScreen) -> bool:
    return matches_confirmation_filters(_row_from_state(state), params)


def _confirmation_diagnostics_for_state(params: ScreenerParams, state: SavedScreenState) -> dict[str, Any]:
    return confirmation_filter_diagnostics(_row_from_state(state), params)


def _row_from_state(state: SavedScreenState) -> dict[str, Any]:
    return {
        "symbol": state.ticker,
        "confirmation": {
            "score": state.confirmation_score,
            "band": state.confirmation_band,
            "direction": state.direction,
            "status": state.confirmation_status,
            "normalized_status": state.confirmation_status,
            "source_count": state.source_count,
        },
        "why_now": {"state": state.why_now_state},
    }


def _log_monitoring_decision(
    *,
    screen: SavedScreen,
    ticker: str,
    before: SavedScreenState | None,
    after: SavedScreenState | None,
    previous_matched: bool,
    current_matched: bool,
    reason: str,
    diagnostics: dict[str, Any] | None = None,
) -> None:
    state = after or before
    diagnostics = diagnostics or {}
    logger.info(
        "saved_screen_monitoring_change ticker=%s screen_id=%s screen_name=%r previous_matched=%s current_matched=%s "
        "required_direction=%s actual_direction=%s required_status=%s actual_status=%s required_band=%s actual_band=%s "
        "source_count=%s score=%s reason=%s",
        ticker,
        screen.id,
        screen.name,
        previous_matched,
        current_matched,
        diagnostics.get("required_direction"),
        diagnostics.get("actual_direction") or (state.direction if state else None),
        diagnostics.get("required_status"),
        diagnostics.get("actual_status") or (state.confirmation_status if state else None),
        diagnostics.get("required_band"),
        diagnostics.get("actual_band") or (state.confirmation_band if state else None),
        state.source_count if state else None,
        state.confirmation_score if state else None,
        reason,
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


def _should_collapse_membership_changes(
    *,
    previous_count: int,
    current_count: int,
    entry_count: int,
    exit_count: int,
) -> bool:
    total_changes = entry_count + exit_count
    if total_changes <= 0:
        return False
    if total_changes > SCREEN_MEMBERSHIP_FLOOD_THRESHOLD:
        return True
    if total_changes < 10:
        return False
    baseline_count = max(previous_count, current_count, 1)
    return (total_changes / baseline_count) > SCREEN_MEMBERSHIP_FLOOD_RATIO


def _saved_screen_monitoring_baseline_version(db: Session, screen: SavedScreen) -> str | None:
    row = db.get(AppSetting, _saved_screen_monitoring_baseline_version_key(screen))
    value = (row.value or "").strip() if row and row.value else ""
    return value or None


def _set_saved_screen_monitoring_baseline_version(db: Session, screen: SavedScreen, version: str) -> None:
    key = _saved_screen_monitoring_baseline_version_key(screen)
    row = db.get(AppSetting, key)
    if row is None:
        db.add(AppSetting(key=key, value=version))
        return
    row.value = version


def _saved_screen_monitoring_baseline_version_key(screen: SavedScreen) -> str:
    return f"saved_screen_monitoring_baseline_version:{screen.user_id}:{screen.id}"


def _params_from_saved_screen(screen: SavedScreen) -> ScreenerParams:
    params = _loads_dict(screen.params_json)
    if not _string_param(params.get("confirmation_direction")):
        legacy_direction = _legacy_confirmation_direction_from_name(screen.name)
        if legacy_direction is not None:
            params = {**params, "confirmation_direction": legacy_direction}
    return screener_params_from_mapping(params, page=1, page_size=100)


def _legacy_confirmation_direction_from_name(name: str | None) -> str | None:
    normalized = (name or "").strip().lower()
    if normalized in {"bullish confirmation", "bullish confirmations"}:
        return "bullish"
    if normalized in {"bearish confirmation", "bearish confirmations"}:
        return "bearish"
    return None


def _string_param(value: Any) -> str | None:
    return value.strip() if isinstance(value, str) and value.strip() else None


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
