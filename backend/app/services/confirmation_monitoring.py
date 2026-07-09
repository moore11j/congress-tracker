from __future__ import annotations

import json
import logging
from time import perf_counter
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import and_, func, select
from sqlalchemy.orm import Session

from app.entitlements import entitlements_for_user, monitored_source_ids
from app.models import ConfirmationMonitoringEvent, ConfirmationMonitoringSnapshot, Security, UserAccount, WatchlistItem
from app.services.confirmation_score import (
    confirmation_active_source_count,
    confirmation_band_for_score,
    get_confirmation_score_bundles_for_tickers,
)

MATERIAL_SCORE_DELTA = 15
DEDUPE_WINDOW = timedelta(hours=24)

BAND_RANK = {
    "inactive": 0,
    "weak": 1,
    "moderate": 2,
    "strong": 3,
    "exceptional": 4,
}

EVENT_LABELS = {
    "new_multi_source_confirmation": "New multi-source",
    "confirmation_upgraded": "Strengthened",
    "confirmation_weakened": "Weakened",
    "direction_flipped": "Direction flipped",
    "multi_source_lost": "Lost multi-source",
    "confirmation_quality_upgraded": "Quality improved",
    "confirmation_quality_downgraded": "Quality downgraded",
    "price_volume_flip": "Price / Volume flipped",
    "fundamentals_flip": "Fundamentals flipped",
}
SOURCE_FLIP_EVENT_TYPES = {
    "price_volume": "price_volume_flip",
    "fundamentals": "fundamentals_flip",
}
SOURCE_FLIP_LABELS = {
    "price_volume": "price/volume",
    "fundamentals": "fundamentals",
}

logger = logging.getLogger(__name__)


def _isoformat(value: datetime | None) -> str | None:
    return value.isoformat() if isinstance(value, datetime) else None


@dataclass(frozen=True)
class ConfirmationMonitoringState:
    ticker: str
    score: int
    band: str
    direction: str
    source_count: int
    status: str
    observed_at: datetime
    source_states: dict[str, dict[str, Any]] = field(default_factory=dict)


@dataclass(frozen=True)
class ConfirmationMonitoringDecision:
    event_type: str
    title: str
    body: str
    payload: dict[str, Any]


def monitoring_state_from_bundle(
    ticker: str,
    bundle: dict,
    *,
    observed_at: datetime | None = None,
) -> ConfirmationMonitoringState:
    score = _coerce_score(bundle.get("score") if isinstance(bundle, dict) else None)
    band = bundle.get("band") if isinstance(bundle, dict) else None
    if band not in BAND_RANK:
        band = confirmation_band_for_score(score)

    direction = bundle.get("direction") if isinstance(bundle, dict) else None
    if direction not in {"bullish", "bearish", "neutral", "mixed"}:
        direction = "neutral"

    status = bundle.get("status") if isinstance(bundle, dict) else None
    if not isinstance(status, str) or not status.strip():
        status = "Inactive"

    return ConfirmationMonitoringState(
        ticker=(ticker or "").strip().upper(),
        score=score,
        band=band,
        direction=direction,
        source_count=confirmation_active_source_count(bundle),
        status=status.strip(),
        source_states=_source_states_from_bundle(bundle),
        observed_at=observed_at or datetime.now(timezone.utc),
    )


def decide_confirmation_monitoring_event(
    before: ConfirmationMonitoringState,
    after: ConfirmationMonitoringState,
) -> ConfirmationMonitoringDecision | None:
    score_delta = after.score - before.score
    before_multi = before.source_count >= 2
    after_multi = after.source_count >= 2
    before_rank = BAND_RANK.get(before.band, 0)
    after_rank = BAND_RANK.get(after.band, 0)

    base_payload = {
        "ticker": after.ticker,
        "score_before": before.score,
        "score_after": after.score,
        "score_delta": score_delta,
        "band_before": before.band,
        "band_after": after.band,
        "direction_before": before.direction,
        "direction_after": after.direction,
        "source_count_before": before.source_count,
        "source_count_after": after.source_count,
        "status_before": before.status,
        "status_after": after.status,
        "observed_at": after.observed_at.isoformat(),
    }

    if not before_multi and after_multi:
        return _decision(
            "new_multi_source_confirmation",
            f"{after.ticker} upgraded to {_lower_status(after.status)}",
            _strength_body("Confirmation strengthened", before, after),
            base_payload,
        )

    if before_multi and not after_multi:
        return _decision(
            "multi_source_lost",
            f"{after.ticker} lost multi-source confirmation",
            _strength_body("Confirmation narrowed", before, after),
            base_payload,
        )

    if before.direction in {"bullish", "bearish"} and after.direction in {"bullish", "bearish"} and before.direction != after.direction:
        return _decision(
            "direction_flipped",
            f"{after.ticker} flipped from {before.direction} to {after.direction} confirmation",
            _strength_body("Direction changed", before, after),
            base_payload,
        )

    if before.direction == "mixed" and after.direction in {"bullish", "bearish"} and (after_multi or after.score >= 40):
        return _decision(
            "direction_flipped",
            f"{after.ticker} flipped from mixed to {after.direction} confirmation",
            _strength_body("Direction clarified", before, after),
            base_payload,
        )

    if score_delta >= MATERIAL_SCORE_DELTA and after_rank > before_rank:
        return _decision(
            "confirmation_upgraded",
            f"{after.ticker} confirmation score rose from {before.score} to {after.score}",
            _strength_body("Confirmation strengthened", before, after),
            base_payload,
        )

    if score_delta <= -MATERIAL_SCORE_DELTA and after_rank < before_rank:
        return _decision(
            "confirmation_weakened",
            f"{after.ticker} confirmation score fell from {before.score} to {after.score}",
            _strength_body("Confirmation weakened", before, after),
            base_payload,
        )

    if before_rank == 0 and after_rank >= 1 and after.score >= 20:
        return _decision(
            "confirmation_quality_upgraded",
            f"{after.ticker} confirmation became {after.band}",
            _strength_body("Confirmation activated", before, after),
            base_payload,
        )

    if before_rank >= 3 and after_rank <= 1:
        return _decision(
            "confirmation_quality_downgraded",
            f"{after.ticker} confirmation weakened to {after.band}",
            _strength_body("Confirmation quality fell", before, after),
            base_payload,
        )

    return None


def decide_source_flip_events(
    before: ConfirmationMonitoringState,
    after: ConfirmationMonitoringState,
) -> list[ConfirmationMonitoringDecision]:
    decisions: list[ConfirmationMonitoringDecision] = []
    for source_key, event_type in SOURCE_FLIP_EVENT_TYPES.items():
        before_state = _interpreted_source_state(before.source_states.get(source_key))
        after_state = _interpreted_source_state(after.source_states.get(source_key))
        if not _is_meaningful_source_flip(before_state, after_state):
            continue
        label = SOURCE_FLIP_LABELS[source_key]
        before_label = before_state["direction"].title()
        after_label = after_state["direction"].title()
        title = f"{after.ticker} {label} flipped from {before_label} to {after_label}"
        body = _source_flip_body(after.ticker, source_key, before_state, after_state)
        payload = {
            "ticker": after.ticker,
            "source": source_key,
            "source_hash": _source_state_hash(source_key, after_state),
            "direction_before": before_state["direction"],
            "direction_after": after_state["direction"],
            "status_before": before_state["status"],
            "status_after": after_state["status"],
            "score_before": before.score,
            "score_after": after.score,
            "observed_at": after.observed_at.isoformat(),
        }
        decisions.append(_decision(event_type, title, body, payload))
    return decisions


def refresh_watchlist_confirmation_monitoring(
    db: Session,
    *,
    user_id: int,
    watchlist_id: int,
    tickers: list[str],
    lookback_days: int = 30,
    now: datetime | None = None,
) -> dict:
    observed_at = now or datetime.now(timezone.utc)
    symbols = sorted({(ticker or "").strip().upper() for ticker in tickers if (ticker or "").strip()})
    if not symbols:
        return {"updated": 0, "initialized": 0, "generated": 0, "deduped": 0, "items": []}

    bundles = get_confirmation_score_bundles_for_tickers(db, symbols, lookback_days=lookback_days)
    snapshots = _snapshot_map(db, user_id=user_id, watchlist_id=watchlist_id, tickers=symbols)

    initialized = 0
    generated = 0
    deduped = 0
    items: list[dict] = []

    for symbol in symbols:
        after = monitoring_state_from_bundle(symbol, bundles.get(symbol, {}), observed_at=observed_at)
        snapshot = snapshots.get(symbol)
        if snapshot is None:
            db.add(_snapshot_from_state(user_id=user_id, watchlist_id=watchlist_id, state=after))
            initialized += 1
            continue

        before = _state_from_snapshot(snapshot)
        decision = decide_confirmation_monitoring_event(before, after)
        if decision is not None:
            if _recent_duplicate_exists(db, user_id=user_id, watchlist_id=watchlist_id, ticker=symbol, decision=decision, now=observed_at):
                deduped += 1
            else:
                event = _event_from_decision(user_id=user_id, watchlist_id=watchlist_id, before=before, after=after, decision=decision)
                db.add(event)
                db.flush()
                generated += 1
                items.append(event_to_dict(event))

        source_decisions = decide_source_flip_events(before, after)
        for source_decision in source_decisions:
            if _recent_duplicate_exists(db, user_id=user_id, watchlist_id=watchlist_id, ticker=symbol, decision=source_decision, now=observed_at):
                deduped += 1
                continue
            event = _event_from_decision(user_id=user_id, watchlist_id=watchlist_id, before=before, after=after, decision=source_decision)
            db.add(event)
            db.flush()
            generated += 1
            items.append(event_to_dict(event))

        _apply_state_to_snapshot(snapshot, after)

    return {
        "updated": len(symbols),
        "initialized": initialized,
        "generated": generated,
        "deduped": deduped,
        "items": items,
    }


def refresh_all_monitored_watchlist_confirmation_monitoring(
    session_factory,
    *,
    lookback_days: int = 30,
    now: datetime | None = None,
) -> dict[str, int]:
    started = perf_counter()
    logger.info("scheduled_monitor_refresh_started")

    with session_factory() as db:
        users = (
            db.execute(
                select(UserAccount)
                .where(UserAccount.is_suspended.is_(False))
                .order_by(UserAccount.id.asc())
            )
            .scalars()
            .all()
        )
        work: list[tuple[int, int]] = []
        for user in users:
            entitlements = entitlements_for_user(db, user)
            allowed_ids = monitored_source_ids(db, user_id=user.id, entitlements=entitlements)["watchlist_ids"]
            work.extend((user.id, watchlist_id) for watchlist_id in sorted(allowed_ids))

    watchlists_checked = 0
    changes_created = 0
    initialized = 0
    deduped = 0
    observed_at = now or datetime.now(timezone.utc)

    for user_id, watchlist_id in work:
        with session_factory() as db:
            try:
                symbols = (
                    db.execute(
                        select(Security.symbol)
                        .join(WatchlistItem, WatchlistItem.security_id == Security.id)
                        .where(WatchlistItem.watchlist_id == watchlist_id)
                        .order_by(Security.symbol.asc())
                    )
                    .scalars()
                    .all()
                )
                result = refresh_watchlist_confirmation_monitoring(
                    db,
                    user_id=user_id,
                    watchlist_id=watchlist_id,
                    tickers=list(symbols),
                    lookback_days=lookback_days,
                    now=observed_at,
                )
                db.commit()
            except Exception:
                db.rollback()
                logger.exception(
                    "scheduled_monitor_refresh_watchlist_failed user_id=%s watchlist_id=%s",
                    user_id,
                    watchlist_id,
                )
                continue

        watchlists_checked += 1
        changes_created += int(result.get("generated") or 0)
        initialized += int(result.get("initialized") or 0)
        deduped += int(result.get("deduped") or 0)

    duration_ms = int(round((perf_counter() - started) * 1000))
    summary = {
        "watchlists_checked": watchlists_checked,
        "changes_created": changes_created,
        "initialized": initialized,
        "deduped": deduped,
        "duration_ms": duration_ms,
    }
    logger.info(
        "scheduled_monitor_refresh_finished watchlists_checked=%s changes_created=%s duration_ms=%s initialized=%s deduped=%s",
        watchlists_checked,
        changes_created,
        duration_ms,
        initialized,
        deduped,
    )
    return summary


def event_to_dict(event: ConfirmationMonitoringEvent) -> dict:
    payload: dict[str, Any] = {}
    try:
        parsed = json.loads(event.payload_json or "{}")
        if isinstance(parsed, dict):
            payload = parsed
    except Exception:
        payload = {}

    return {
        "id": event.id,
        "watchlist_id": event.watchlist_id,
        "ticker": event.ticker,
        "event_type": event.event_type,
        "event_label": EVENT_LABELS.get(event.event_type, event.event_type.replace("_", " ").title()),
        "title": event.title,
        "body": event.body,
        "score_before": event.score_before,
        "score_after": event.score_after,
        "band_before": event.band_before,
        "band_after": event.band_after,
        "direction_before": event.direction_before,
        "direction_after": event.direction_after,
        "source_count_before": event.source_count_before,
        "source_count_after": event.source_count_after,
        "payload": payload,
        "created_at": _isoformat(event.created_at),
    }


def _source_states_from_bundle(bundle: dict) -> dict[str, dict[str, Any]]:
    raw_sources = bundle.get("sources") if isinstance(bundle, dict) and isinstance(bundle.get("sources"), dict) else {}
    result: dict[str, dict[str, Any]] = {}
    for key in SOURCE_FLIP_EVENT_TYPES:
        raw = raw_sources.get(key)
        if not isinstance(raw, dict):
            result[key] = {"status": "unavailable", "direction": "unavailable", "present": False}
            continue
        direction = raw.get("direction") if raw.get("direction") in {"bullish", "bearish", "neutral", "mixed"} else "neutral"
        present = raw.get("present") is True
        status = "active" if present else str(raw.get("status") or "inactive").strip().lower()
        if not present and status in {"", "none"}:
            status = "inactive"
        result[key] = {
            "status": status,
            "direction": direction if present else "unavailable" if status in {"unavailable", "not_configured", "disabled", "provider_error", "error"} else "mixed",
            "present": present,
            "label": raw.get("label") if isinstance(raw.get("label"), str) else None,
        }
    return result


def _interpreted_source_state(state: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(state, dict):
        return {"status": "unavailable", "direction": "unavailable", "present": False}
    direction = state.get("direction")
    if direction not in {"bullish", "bearish", "mixed", "neutral", "unavailable"}:
        direction = "unavailable"
    if direction == "neutral":
        direction = "mixed"
    return {
        "status": str(state.get("status") or "unavailable").strip().lower(),
        "direction": direction,
        "present": state.get("present") is True,
    }


def _is_meaningful_source_flip(before: dict[str, Any], after: dict[str, Any]) -> bool:
    before_direction = before.get("direction")
    after_direction = after.get("direction")
    if after_direction not in {"bullish", "bearish"}:
        return False
    if before_direction == after_direction:
        return False
    if before_direction in {"bullish", "bearish", "mixed"}:
        return True
    if before_direction == "unavailable":
        return after.get("present") is True
    return False


def _source_flip_body(ticker: str, source_key: str, before: dict[str, Any], after: dict[str, Any]) -> str:
    before_label = str(before.get("direction") or "mixed").title()
    after_label = str(after.get("direction") or "mixed").title()
    if source_key == "fundamentals":
        return f"{ticker} fundamentals flipped from {before_label} to {after_label}."
    return f"{ticker} price/volume flipped from {before_label} to {after_label}."


def _source_state_hash(source_key: str, state: dict[str, Any]) -> str:
    return f"{source_key}:{state.get('status')}:{state.get('direction')}:{bool(state.get('present'))}"


def _snapshot_map(
    db: Session,
    *,
    user_id: int,
    watchlist_id: int,
    tickers: list[str],
) -> dict[str, ConfirmationMonitoringSnapshot]:
    rows = (
        db.execute(
            select(ConfirmationMonitoringSnapshot)
            .where(ConfirmationMonitoringSnapshot.user_id == user_id)
            .where(ConfirmationMonitoringSnapshot.watchlist_id == watchlist_id)
            .where(func.upper(ConfirmationMonitoringSnapshot.ticker).in_(tickers))
        )
        .scalars()
        .all()
    )
    return {row.ticker.upper(): row for row in rows}


def _snapshot_from_state(
    *,
    user_id: int,
    watchlist_id: int,
    state: ConfirmationMonitoringState,
) -> ConfirmationMonitoringSnapshot:
    return ConfirmationMonitoringSnapshot(
        user_id=user_id,
        watchlist_id=watchlist_id,
        ticker=state.ticker,
        score=state.score,
        band=state.band,
        direction=state.direction,
        source_count=state.source_count,
        status=state.status,
        source_states_json=json.dumps(state.source_states, sort_keys=True),
        observed_at=state.observed_at,
    )


def _state_from_snapshot(snapshot: ConfirmationMonitoringSnapshot) -> ConfirmationMonitoringState:
    source_states: dict[str, dict[str, Any]] = {}
    try:
        parsed = json.loads(snapshot.source_states_json or "{}")
        if isinstance(parsed, dict):
            source_states = {
                str(key): value
                for key, value in parsed.items()
                if isinstance(value, dict)
            }
    except Exception:
        source_states = {}
    return ConfirmationMonitoringState(
        ticker=snapshot.ticker.upper(),
        score=int(snapshot.score or 0),
        band=snapshot.band or "inactive",
        direction=snapshot.direction or "neutral",
        source_count=int(snapshot.source_count or 0),
        status=snapshot.status or "Inactive",
        source_states=source_states,
        observed_at=snapshot.observed_at,
    )


def _apply_state_to_snapshot(snapshot: ConfirmationMonitoringSnapshot, state: ConfirmationMonitoringState) -> None:
    snapshot.score = state.score
    snapshot.band = state.band
    snapshot.direction = state.direction
    snapshot.source_count = state.source_count
    snapshot.status = state.status
    snapshot.source_states_json = json.dumps(state.source_states, sort_keys=True)
    snapshot.observed_at = state.observed_at
    snapshot.updated_at = state.observed_at


def _event_from_decision(
    *,
    user_id: int,
    watchlist_id: int,
    before: ConfirmationMonitoringState,
    after: ConfirmationMonitoringState,
    decision: ConfirmationMonitoringDecision,
) -> ConfirmationMonitoringEvent:
    return ConfirmationMonitoringEvent(
        user_id=user_id,
        watchlist_id=watchlist_id,
        ticker=after.ticker,
        event_type=decision.event_type,
        title=decision.title,
        body=decision.body,
        score_before=before.score,
        score_after=after.score,
        band_before=before.band,
        band_after=after.band,
        direction_before=before.direction,
        direction_after=after.direction,
        source_count_before=before.source_count,
        source_count_after=after.source_count,
        payload_json=json.dumps(decision.payload, sort_keys=True),
        created_at=after.observed_at,
    )


def _recent_duplicate_exists(
    db: Session,
    *,
    user_id: int,
    watchlist_id: int,
    ticker: str,
    decision: ConfirmationMonitoringDecision,
    now: datetime,
) -> bool:
    cutoff = now - DEDUPE_WINDOW
    recent = (
        db.execute(
            select(ConfirmationMonitoringEvent)
            .where(
                and_(
                    ConfirmationMonitoringEvent.user_id == user_id,
                    ConfirmationMonitoringEvent.watchlist_id == watchlist_id,
                    func.upper(ConfirmationMonitoringEvent.ticker) == ticker.upper(),
                    ConfirmationMonitoringEvent.event_type == decision.event_type,
                    ConfirmationMonitoringEvent.created_at >= cutoff,
                )
            )
            .order_by(ConfirmationMonitoringEvent.created_at.desc(), ConfirmationMonitoringEvent.id.desc())
            .limit(1)
        )
        .scalars()
        .first()
    )
    if recent is None:
        return False

    try:
        payload = json.loads(recent.payload_json or "{}")
    except Exception:
        payload = {}
    if not isinstance(payload, dict):
        return False

    expected = decision.payload
    if expected.get("source_hash"):
        return payload.get("source_hash") == expected.get("source_hash")
    return (
        payload.get("score_after") == expected.get("score_after")
        and payload.get("band_after") == expected.get("band_after")
        and payload.get("direction_after") == expected.get("direction_after")
        and payload.get("source_count_after") == expected.get("source_count_after")
    )


def _decision(event_type: str, title: str, body: str, payload: dict[str, Any]) -> ConfirmationMonitoringDecision:
    return ConfirmationMonitoringDecision(
        event_type=event_type,
        title=title,
        body=body,
        payload={**payload, "event_type": event_type},
    )


def _strength_body(prefix: str, before: ConfirmationMonitoringState, after: ConfirmationMonitoringState) -> str:
    return (
        f"{prefix} from {before.band} {before.source_count}-source {before.direction} "
        f"to {after.band} {after.source_count}-source {after.direction}."
    )


def _lower_status(status: str) -> str:
    return status[:1].lower() + status[1:] if status else "confirmation"


def _coerce_score(value: Any) -> int:
    try:
        score = int(round(float(value)))
    except (TypeError, ValueError):
        return 0
    return max(0, min(100, score))
