from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any

from sqlalchemy import or_, select
from sqlalchemy.exc import IntegrityError, OperationalError
from sqlalchemy.orm import Session

from app.db import SessionLocal
from app.insider_market_trade import canonicalize_market_trade_type
from app.models import DataEnrichmentJob, Event, PriceCache, QuoteCache, TradeOutcome
from app.services.congress_outcome_eligibility import congress_equity_outcome_eligibility
from app.services.data_enrichment_queue import (
    ACTIVE_STATUSES,
    _job_completed_recently,
    build_dedupe_key,
    enqueue_data_enrichment_job,
)
from app.services.returns import signed_return_pct
from app.utils.symbols import normalize_symbol

logger = logging.getLogger(__name__)

FEED_PNL_EVENT_TYPES = {"congress_trade", "insider_trade"}
FEED_PNL_METHODOLOGY_VERSION = "feed_pnl_cache_v1"
FEED_PNL_PRIORITY_BASE = 5
FEED_PNL_QUOTE_PRIORITY = FEED_PNL_PRIORITY_BASE
FEED_PNL_PRICE_EOD_PRIORITY = FEED_PNL_PRIORITY_BASE + 1
FEED_PNL_REFRESH_PRIORITY = FEED_PNL_PRIORITY_BASE + 2


class FeedPnlInputMissing(RuntimeError):
    def __init__(self, event_id: int, missing: list[str]) -> None:
        self.event_id = event_id
        self.missing = missing
        self.reason_code = "feed_pnl_input_missing"
        self.retryable = True
        super().__init__(f"feed_pnl_input_missing event_id={event_id} missing={','.join(missing)}")


@dataclass(frozen=True)
class FeedPnlInputs:
    event_id: int | None
    event_type: str
    symbol: str | None
    trade_date: str | None
    trade_type: str | None
    member_id: str | None
    member_name: str | None
    structural_status: str | None = None
    structural_error: str | None = None


@dataclass(frozen=True)
class _QueuedJobSpec:
    job_type: str
    symbol: str | None
    date_key: str | None
    window_key: str | None
    dedupe_key: str
    source: str
    reason: str
    priority: int
    payload_json: str | None
    max_attempts: int


def _payload_dict(payload_json: str | dict | None) -> dict[str, Any]:
    if isinstance(payload_json, dict):
        return payload_json
    if not payload_json:
        return {}
    try:
        parsed = json.loads(payload_json)
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _first_text(*values: object) -> str | None:
    for value in values:
        if isinstance(value, str) and value.strip():
            return value.strip()
        if value is not None and not isinstance(value, (dict, list, tuple, set)):
            text = str(value).strip()
            if text:
                return text
    return None


def _parse_date_key(value: object) -> str | None:
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, str) and value.strip():
        text = value.strip()
        try:
            return date.fromisoformat(text[:10]).isoformat()
        except ValueError:
            return None
    return None


def _event_date_key(event: Event) -> str | None:
    return _parse_date_key(event.event_date) or _parse_date_key(event.ts)


def _payload_trade_date(event: Event, payload: dict[str, Any]) -> str | None:
    raw = payload.get("raw") if isinstance(payload.get("raw"), dict) else {}
    return (
        _parse_date_key(payload.get("trade_date"))
        or _parse_date_key(payload.get("tradeDate"))
        or _parse_date_key(payload.get("transaction_date"))
        or _parse_date_key(payload.get("transactionDate"))
        or _parse_date_key(raw.get("transactionDate"))
        or _parse_date_key(raw.get("tradeDate"))
        or _event_date_key(event)
    )


def _payload_symbol(event: Event, payload: dict[str, Any]) -> str | None:
    raw = payload.get("raw") if isinstance(payload.get("raw"), dict) else {}
    return normalize_symbol(
        _first_text(
            event.symbol,
            payload.get("symbol"),
            payload.get("ticker"),
            payload.get("issuer_symbol"),
            payload.get("issuerSymbol"),
            raw.get("symbol"),
            raw.get("ticker"),
        )
    )


def _event_trade_type(event: Event, payload: dict[str, Any]) -> str | None:
    raw = payload.get("raw") if isinstance(payload.get("raw"), dict) else {}
    return _first_text(
        event.trade_type,
        event.transaction_type,
        payload.get("trade_type"),
        payload.get("tradeType"),
        payload.get("transaction_type"),
        payload.get("transactionType"),
        raw.get("transactionType"),
    )


def _coerce_optional_bool(value: object) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"1", "true", "yes"}:
            return True
        if lowered in {"0", "false", "no"}:
            return False
    return None


def feed_pnl_inputs_for_event(event: Event) -> FeedPnlInputs:
    payload = _payload_dict(event.payload_json)
    event_type = (event.event_type or "").strip()
    symbol = _payload_symbol(event, payload)
    trade_date = _payload_trade_date(event, payload)
    trade_type = _event_trade_type(event, payload)
    raw = payload.get("raw") if isinstance(payload.get("raw"), dict) else {}
    member_id = _first_text(
        event.member_bioguide_id,
        payload.get("reporting_cik"),
        payload.get("reportingCik"),
        raw.get("reportingCik"),
        raw.get("rptOwnerCik"),
    )
    member_name = _first_text(event.member_name, payload.get("insider_name"), payload.get("insiderName"))

    status = None
    error = None
    if event_type not in FEED_PNL_EVENT_TYPES:
        status = "unsupported_event_type"
        error = f"Unsupported feed PnL event_type={event_type or 'unknown'}"
    elif not symbol:
        status = "no_symbol"
        error = "Missing event symbol"
    elif not trade_date:
        status = "missing_trade_date"
        error = "Missing trade date"
    elif event_type == "insider_trade":
        explicit_market = _coerce_optional_bool(payload.get("is_market_trade"))
        canonical_trade_type = canonicalize_market_trade_type(trade_type)
        if explicit_market is False or (explicit_market is None and canonical_trade_type is None):
            status = "insider_non_market"
            error = "Insider transaction is non-market activity and excluded from feed PnL"
        elif canonical_trade_type is None:
            status = "missing_trade_side"
            error = "Missing insider trade side"
    elif event_type == "congress_trade":
        eligibility = congress_equity_outcome_eligibility(
            event_type=event_type,
            symbol=symbol,
            payload=payload,
            trade_date=trade_date,
            side=trade_type,
            amount_min=event.amount_min,
            amount_max=event.amount_max,
        )
        if not eligibility.eligible:
            status = eligibility.skip_reason or "not_equity_outcome_eligible"
            error = eligibility.detail

    return FeedPnlInputs(
        event_id=event.id,
        event_type=event_type,
        symbol=symbol,
        trade_date=trade_date,
        trade_type=trade_type,
        member_id=member_id,
        member_name=member_name,
        structural_status=status,
        structural_error=error,
    )


def _enqueue_job_in_session(
    db: Session,
    *,
    job_type: str,
    symbol: str | None = None,
    date_key: str | None = None,
    window_key: str | None = None,
    source: str,
    reason: str,
    priority: int,
    payload: dict[str, Any] | None = None,
    max_attempts: int = 5,
) -> bool:
    normalized_symbol = normalize_symbol(symbol) if symbol else None
    if symbol is not None and not normalized_symbol:
        return False
    dedupe_key = build_dedupe_key(
        job_type=job_type,
        symbol=normalized_symbol,
        date_key=date_key,
        window_key=window_key,
    )
    if not job_type or not dedupe_key.strip("|"):
        return False
    for pending in db.new:
        if isinstance(pending, DataEnrichmentJob) and pending.dedupe_key == dedupe_key:
            return False
    now = datetime.now(timezone.utc)
    try:
        existing = db.execute(
            select(DataEnrichmentJob).where(DataEnrichmentJob.dedupe_key == dedupe_key)
        ).scalar_one_or_none()
    except OperationalError as exc:
        logger.info(
            "feed_pnl_jobs_skipped_duplicate reason=queue_table_unavailable job_type=%s symbol=%s error=%s",
            job_type,
            normalized_symbol,
            exc.__class__.__name__,
        )
        return False
    payload_json = json.dumps(payload, sort_keys=True) if payload else None
    if existing is not None:
        if existing.status in ACTIVE_STATUSES:
            return False
        if existing.status == "done" and _job_completed_recently(existing, now):
            return False
        existing.status = "queued"
        existing.reason = reason
        existing.source = source
        existing.priority = min(int(existing.priority or priority), int(priority))
        existing.error = None
        existing.next_run_at = now
        existing.updated_at = now
        if payload_json:
            existing.payload_json = payload_json
        return True

    db.add(
        DataEnrichmentJob(
            job_type=job_type,
            symbol=normalized_symbol,
            date_key=date_key,
            window_key=window_key,
            dedupe_key=dedupe_key,
            priority=int(priority),
            status="queued",
            attempts=0,
            max_attempts=int(max_attempts),
            source=source,
            reason=reason,
            payload_json=payload_json,
            next_run_at=now,
        )
    )
    return True


def _enqueue_job(
    db: Session | None,
    *,
    use_current_session: bool,
    job_type: str,
    symbol: str | None = None,
    date_key: str | None = None,
    window_key: str | None = None,
    source: str,
    reason: str,
    priority: int,
    payload: dict[str, Any] | None = None,
    max_attempts: int = 5,
) -> bool:
    if use_current_session and db is not None:
        return _enqueue_job_in_session(
            db,
            job_type=job_type,
            symbol=symbol,
            date_key=date_key,
            window_key=window_key,
            source=source,
            reason=reason,
            priority=priority,
            payload=payload,
            max_attempts=max_attempts,
        )
    return enqueue_data_enrichment_job(
        job_type=job_type,
        symbol=symbol,
        date_key=date_key,
        window_key=window_key,
        source=source,
        reason=reason,
        priority=priority,
        payload=payload,
        max_attempts=max_attempts,
    )


def enqueue_feed_pnl_enrichment_for_event(
    db: Session | None,
    event: Event,
    *,
    source: str = "event_ingest",
    reason: str = "feed_pnl_missing",
    priority: int = FEED_PNL_PRIORITY_BASE,
    use_current_session: bool = False,
) -> dict[str, Any]:
    inputs = feed_pnl_inputs_for_event(event)
    result: dict[str, Any] = {
        "event_id": inputs.event_id,
        "symbol": inputs.symbol,
        "trade_date": inputs.trade_date,
        "eligible": inputs.structural_status is None,
        "quote_enqueued": False,
        "price_eod_enqueued": False,
        "pnl_refresh_enqueued": False,
        "structural_outcome_written": False,
        "skipped_reason": inputs.structural_status,
    }
    if inputs.structural_status is not None or not inputs.symbol or not inputs.trade_date or inputs.event_id is None:
        result["structural_outcome_written"] = _write_structural_outcome_for_enqueue(
            db,
            event,
            inputs,
            use_current_session=use_current_session,
        )
        return result

    payload = {
        "event_id": inputs.event_id,
        "event_type": inputs.event_type,
        "symbol": inputs.symbol,
        "trade_date": inputs.trade_date,
    }
    result["quote_enqueued"] = _enqueue_job(
        db,
        use_current_session=use_current_session,
        job_type="quote",
        symbol=inputs.symbol,
        source=source,
        reason=reason,
        priority=priority,
        max_attempts=5,
    )
    result["price_eod_enqueued"] = _enqueue_job(
        db,
        use_current_session=use_current_session,
        job_type="price_eod",
        symbol=inputs.symbol,
        date_key=inputs.trade_date,
        source=source,
        reason=reason,
        priority=priority + 1,
        max_attempts=5,
    )
    result["pnl_refresh_enqueued"] = _enqueue_job(
        db,
        use_current_session=use_current_session,
        job_type="pnl_refresh",
        symbol=inputs.symbol,
        date_key=inputs.trade_date,
        window_key=f"event:{inputs.event_id}",
        source=source,
        reason=reason,
        priority=priority + 2,
        payload=payload,
        max_attempts=8,
    )

    if result["quote_enqueued"] or result["price_eod_enqueued"] or result["pnl_refresh_enqueued"]:
        logger.info(
            "feed_pnl_jobs_enqueued event_id=%s symbol=%s trade_date=%s quote=%s price_eod=%s pnl_refresh=%s",
            inputs.event_id,
            inputs.symbol,
            inputs.trade_date,
            result["quote_enqueued"],
            result["price_eod_enqueued"],
            result["pnl_refresh_enqueued"],
        )
    else:
        logger.info(
            "feed_pnl_jobs_skipped_duplicate event_id=%s symbol=%s trade_date=%s",
            inputs.event_id,
            inputs.symbol,
            inputs.trade_date,
        )
    return result


def _feed_pnl_job_specs(
    inputs: FeedPnlInputs,
    *,
    source: str,
    reason: str,
    priority: int,
) -> list[_QueuedJobSpec]:
    if inputs.structural_status is not None or not inputs.symbol or not inputs.trade_date or inputs.event_id is None:
        return []

    payload = {
        "event_id": inputs.event_id,
        "event_type": inputs.event_type,
        "symbol": inputs.symbol,
        "trade_date": inputs.trade_date,
    }
    payload_json = json.dumps(payload, sort_keys=True)
    raw_specs = [
        {
            "job_type": "quote",
            "symbol": inputs.symbol,
            "date_key": None,
            "window_key": None,
            "priority": priority,
            "payload_json": None,
            "max_attempts": 5,
        },
        {
            "job_type": "price_eod",
            "symbol": inputs.symbol,
            "date_key": inputs.trade_date,
            "window_key": None,
            "priority": priority + 1,
            "payload_json": None,
            "max_attempts": 5,
        },
        {
            "job_type": "pnl_refresh",
            "symbol": inputs.symbol,
            "date_key": inputs.trade_date,
            "window_key": f"event:{inputs.event_id}",
            "priority": priority + 2,
            "payload_json": payload_json,
            "max_attempts": 8,
        },
    ]
    return [
        _QueuedJobSpec(
            job_type=str(spec["job_type"]),
            symbol=str(spec["symbol"]) if spec["symbol"] else None,
            date_key=str(spec["date_key"]) if spec["date_key"] else None,
            window_key=str(spec["window_key"]) if spec["window_key"] else None,
            dedupe_key=build_dedupe_key(
                job_type=str(spec["job_type"]),
                symbol=str(spec["symbol"]) if spec["symbol"] else None,
                date_key=str(spec["date_key"]) if spec["date_key"] else None,
                window_key=str(spec["window_key"]) if spec["window_key"] else None,
            ),
            source=source,
            reason=reason,
            priority=int(spec["priority"]),
            payload_json=str(spec["payload_json"]) if spec["payload_json"] else None,
            max_attempts=int(spec["max_attempts"]),
        )
        for spec in raw_specs
    ]


def _enqueue_job_specs_in_session(db: Session, specs: list[_QueuedJobSpec]) -> dict[str, int]:
    if not specs:
        return {"attempted": 0, "enqueued": 0, "skipped": 0}

    unique_specs: dict[str, _QueuedJobSpec] = {}
    for spec in specs:
        if spec.dedupe_key.strip("|"):
            unique_specs.setdefault(spec.dedupe_key, spec)
    if not unique_specs:
        return {"attempted": 0, "enqueued": 0, "skipped": len(specs)}

    now = datetime.now(timezone.utc)
    try:
        existing_rows = db.execute(
            select(DataEnrichmentJob).where(DataEnrichmentJob.dedupe_key.in_(sorted(unique_specs)))
        ).scalars().all()
    except OperationalError as exc:
        logger.info(
            "feed_pnl_jobs_batch_skipped reason=queue_table_unavailable attempted=%s error=%s",
            len(unique_specs),
            exc.__class__.__name__,
        )
        return {"attempted": len(unique_specs), "enqueued": 0, "skipped": len(unique_specs)}

    existing_by_key = {row.dedupe_key: row for row in existing_rows}
    enqueued = 0
    skipped = 0
    for key, spec in unique_specs.items():
        existing = existing_by_key.get(key)
        if existing is not None:
            if existing.status in ACTIVE_STATUSES:
                skipped += 1
                continue
            if existing.status == "done" and _job_completed_recently(existing, now):
                skipped += 1
                continue
            existing.status = "queued"
            existing.reason = spec.reason
            existing.source = spec.source
            existing.priority = min(int(existing.priority or spec.priority), int(spec.priority))
            existing.error = None
            existing.next_run_at = now
            existing.updated_at = now
            if spec.payload_json:
                existing.payload_json = spec.payload_json
            enqueued += 1
            continue

        db.add(
            DataEnrichmentJob(
                job_type=spec.job_type,
                symbol=spec.symbol,
                date_key=spec.date_key,
                window_key=spec.window_key,
                dedupe_key=spec.dedupe_key,
                priority=int(spec.priority),
                status="queued",
                attempts=0,
                max_attempts=int(spec.max_attempts),
                source=spec.source,
                reason=spec.reason,
                payload_json=spec.payload_json,
                next_run_at=now,
            )
        )
        enqueued += 1

    return {"attempted": len(unique_specs), "enqueued": enqueued, "skipped": skipped}


def enqueue_feed_pnl_enrichment_for_events(
    db: Session | None,
    events: list[Event],
    *,
    source: str = "event_ingest",
    reason: str = "feed_pnl_missing",
    priority: int = FEED_PNL_PRIORITY_BASE,
    use_current_session: bool = False,
) -> dict[str, int]:
    if not events:
        return {
            "events": 0,
            "eligible_events": 0,
            "structural_outcomes": 0,
            "jobs_attempted": 0,
            "jobs_enqueued": 0,
            "jobs_skipped": 0,
        }

    own_session = not (use_current_session and db is not None)
    target_db = db if not own_session and db is not None else SessionLocal()
    try:
        specs: list[_QueuedJobSpec] = []
        structural_outcomes = 0
        eligible_events = 0
        for event in events:
            inputs = feed_pnl_inputs_for_event(event)
            if inputs.structural_status is not None or not inputs.symbol or not inputs.trade_date or inputs.event_id is None:
                if _write_structural_outcome_for_enqueue(
                    target_db,
                    event,
                    inputs,
                    use_current_session=True,
                ):
                    structural_outcomes += 1
                continue
            eligible_events += 1
            specs.extend(
                _feed_pnl_job_specs(
                    inputs,
                    source=source,
                    reason=reason,
                    priority=priority,
                )
            )

        queue_summary = _enqueue_job_specs_in_session(target_db, specs)
        if own_session:
            target_db.commit()
        summary = {
            "events": len(events),
            "eligible_events": eligible_events,
            "structural_outcomes": structural_outcomes,
            "jobs_attempted": queue_summary["attempted"],
            "jobs_enqueued": queue_summary["enqueued"],
            "jobs_skipped": queue_summary["skipped"],
        }
        logger.info(
            "feed_pnl_jobs_batch events=%s eligible_events=%s structural_outcomes=%s jobs_attempted=%s jobs_enqueued=%s jobs_skipped=%s",
            summary["events"],
            summary["eligible_events"],
            summary["structural_outcomes"],
            summary["jobs_attempted"],
            summary["jobs_enqueued"],
            summary["jobs_skipped"],
        )
        return summary
    except IntegrityError:
        target_db.rollback()
        logger.info("feed_pnl_jobs_batch_skipped reason=integrity_race events=%s", len(events))
    except OperationalError as exc:
        target_db.rollback()
        logger.info(
            "feed_pnl_jobs_batch_skipped reason=db_busy events=%s error=%s",
            len(events),
            exc.__class__.__name__,
        )
    except Exception:
        target_db.rollback()
        logger.exception("feed_pnl_jobs_batch_failed events=%s", len(events))
    finally:
        if own_session:
            target_db.close()

    return {
        "events": len(events),
        "eligible_events": 0,
        "structural_outcomes": 0,
        "jobs_attempted": 0,
        "jobs_enqueued": 0,
        "jobs_skipped": len(events),
    }


def _cached_entry_close(db: Session, symbol: str, trade_date: str) -> PriceCache | None:
    return db.execute(
        select(PriceCache)
        .where(PriceCache.symbol == symbol)
        .where(PriceCache.date <= trade_date)
        .order_by(PriceCache.date.desc())
        .limit(1)
    ).scalar_one_or_none()


def _parse_cache_date(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return date.fromisoformat(value[:10])
    except ValueError:
        return None


def _write_structural_outcome(db: Session, event: Event, inputs: FeedPnlInputs) -> None:
    existing = db.execute(select(TradeOutcome).where(TradeOutcome.event_id == event.id)).scalar_one_or_none()
    if existing is not None and existing.return_pct is not None and existing.scoring_status == "ok":
        return
    now = datetime.now(timezone.utc)
    target = existing or TradeOutcome(event_id=event.id)
    target.member_id = inputs.member_id
    target.member_name = inputs.member_name
    target.symbol = inputs.symbol
    target.trade_type = inputs.trade_type or event.trade_type
    target.source = event.source
    target.trade_date = _parse_cache_date(inputs.trade_date)
    target.entry_price = None
    target.entry_price_date = None
    target.current_price = None
    target.current_price_date = None
    target.benchmark_symbol = "^GSPC"
    target.benchmark_entry_price = None
    target.benchmark_current_price = None
    target.return_pct = None
    target.benchmark_return_pct = None
    target.alpha_pct = None
    target.holding_days = None
    target.amount_min = event.amount_min
    target.amount_max = event.amount_max
    target.scoring_status = inputs.structural_status or "not_priceable"
    target.scoring_error = inputs.structural_error
    target.methodology_version = FEED_PNL_METHODOLOGY_VERSION
    target.computed_at = now
    if existing is None:
        db.add(target)


def _write_structural_outcome_for_enqueue(
    db: Session | None,
    event: Event,
    inputs: FeedPnlInputs,
    *,
    use_current_session: bool,
) -> bool:
    if inputs.event_id is None:
        return False
    if inputs.structural_status is None and inputs.symbol and inputs.trade_date:
        return False

    if db is not None and use_current_session:
        _write_structural_outcome(db, event, inputs)
        return True

    local_db = SessionLocal()
    try:
        persisted_event = local_db.get(Event, inputs.event_id)
        if persisted_event is None:
            return False
        persisted_inputs = feed_pnl_inputs_for_event(persisted_event)
        if persisted_inputs.structural_status is None and persisted_inputs.symbol and persisted_inputs.trade_date:
            return False
        _write_structural_outcome(local_db, persisted_event, persisted_inputs)
        local_db.commit()
        return True
    except Exception:
        local_db.rollback()
        logger.exception("feed_pnl_structural_outcome_write_failed event_id=%s", inputs.event_id)
        return False
    finally:
        local_db.close()


def process_feed_pnl_refresh_job(db: Session, *, event_id: int) -> None:
    event = db.get(Event, event_id)
    if event is None:
        logger.info("feed_pnl_jobs_skipped_duplicate event_id=%s reason=missing_event", event_id)
        return

    inputs = feed_pnl_inputs_for_event(event)
    if inputs.structural_status is not None:
        _write_structural_outcome(db, event, inputs)
        return
    if not inputs.symbol or not inputs.trade_date:
        _write_structural_outcome(db, event, inputs)
        return

    quote = db.get(QuoteCache, inputs.symbol)
    entry = _cached_entry_close(db, inputs.symbol, inputs.trade_date)
    missing: list[str] = []
    if quote is None or quote.price is None or quote.price <= 0:
        missing.append("quote")
        _enqueue_job(
            None,
            use_current_session=False,
            job_type="quote",
            symbol=inputs.symbol,
            source="pnl_refresh",
            reason="missing_quote",
            priority=FEED_PNL_QUOTE_PRIORITY,
            max_attempts=5,
        )
    if entry is None or entry.close is None or entry.close <= 0:
        missing.append("entry_eod")
        _enqueue_job(
            None,
            use_current_session=False,
            job_type="price_eod",
            symbol=inputs.symbol,
            date_key=inputs.trade_date,
            source="pnl_refresh",
            reason="missing_entry_eod",
            priority=FEED_PNL_PRICE_EOD_PRIORITY,
            max_attempts=5,
        )
    if missing:
        logger.info(
            "feed_pnl_input_missing event_id=%s missing=%s symbol=%s trade_date=%s",
            event.id,
            ",".join(missing),
            inputs.symbol,
            inputs.trade_date,
        )
        raise FeedPnlInputMissing(event.id, missing)

    existing = db.execute(select(TradeOutcome).where(TradeOutcome.event_id == event.id)).scalar_one_or_none()
    if (
        existing is not None
        and existing.return_pct is not None
        and existing.scoring_status == "ok"
        and existing.methodology_version != FEED_PNL_METHODOLOGY_VERSION
    ):
        return

    trade_date = _parse_cache_date(inputs.trade_date)
    entry_date = _parse_cache_date(entry.date)
    current_price_date = quote.asof_ts.date() if isinstance(quote.asof_ts, datetime) else None
    return_pct = signed_return_pct(quote.price, entry.close, inputs.trade_type or event.trade_type)
    holding_days = (datetime.now(timezone.utc).date() - trade_date).days if trade_date else None
    target = existing or TradeOutcome(event_id=event.id)
    target.member_id = inputs.member_id
    target.member_name = inputs.member_name
    target.symbol = inputs.symbol
    target.trade_type = inputs.trade_type or event.trade_type
    target.source = event.source
    target.trade_date = trade_date
    target.entry_price = float(entry.close)
    target.entry_price_date = entry_date
    target.current_price = float(quote.price)
    target.current_price_date = current_price_date
    target.benchmark_symbol = existing.benchmark_symbol if existing is not None and existing.benchmark_symbol else "^GSPC"
    target.benchmark_entry_price = existing.benchmark_entry_price if existing is not None else None
    target.benchmark_current_price = existing.benchmark_current_price if existing is not None else None
    target.return_pct = return_pct
    target.benchmark_return_pct = existing.benchmark_return_pct if existing is not None else None
    target.alpha_pct = existing.alpha_pct if existing is not None else None
    target.holding_days = holding_days
    target.amount_min = event.amount_min
    target.amount_max = event.amount_max
    target.scoring_status = "ok"
    target.scoring_error = None
    target.methodology_version = FEED_PNL_METHODOLOGY_VERSION
    target.computed_at = datetime.now(timezone.utc)
    if existing is None:
        db.add(target)

    pnl_abs = None
    if return_pct is not None and event.amount_max is not None:
        pnl_abs = float(event.amount_max) * float(return_pct) / 100.0
    logger.info(
        "feed_pnl_calculated event_id=%s symbol=%s trade_date=%s pnl_abs=%s pnl_pct=%s",
        event.id,
        inputs.symbol,
        inputs.trade_date,
        pnl_abs,
        return_pct,
    )


def repair_recent_feed_pnl(
    db: Session,
    *,
    days: int = 3,
    limit: int = 200,
    symbols: list[str] | None = None,
) -> dict[str, Any]:
    cutoff = datetime.now(timezone.utc) - timedelta(days=max(0, int(days)))
    normalized_symbols = sorted({symbol for symbol in (normalize_symbol(s) for s in symbols or []) if symbol})
    q = (
        select(Event)
        .where(Event.event_type.in_(sorted(FEED_PNL_EVENT_TYPES)))
        .where(or_(Event.event_date >= cutoff, Event.ts >= cutoff))
        .order_by(Event.event_date.desc(), Event.ts.desc(), Event.id.desc())
        .limit(max(1, int(limit)))
    )
    if normalized_symbols:
        q = q.where(Event.symbol.in_(normalized_symbols))
    events = db.execute(q).scalars().all()
    event_ids = [event.id for event in events]
    outcomes = (
        {
            row.event_id: row
            for row in db.execute(select(TradeOutcome).where(TradeOutcome.event_id.in_(event_ids))).scalars().all()
        }
        if event_ids
        else {}
    )
    missing = [
        event
        for event in events
        if event.id not in outcomes or outcomes[event.id].return_pct is None
    ]
    counts = {
        "events_scanned": len(events),
        "events_missing_pnl": len(missing),
        "quote_enqueued": 0,
        "quote_skipped": 0,
        "price_eod_enqueued": 0,
        "price_eod_skipped": 0,
        "pnl_refresh_enqueued": 0,
        "pnl_refresh_skipped": 0,
        "symbols_affected": [],
    }
    affected: set[str] = set()
    for event in missing:
        result = enqueue_feed_pnl_enrichment_for_event(
            db,
            event,
            source="repair_recent_feed_pnl",
            reason="missing_feed_pnl",
            priority=FEED_PNL_PRIORITY_BASE,
            use_current_session=False,
        )
        if result.get("symbol"):
            affected.add(str(result["symbol"]))
        for key in ("quote", "price_eod", "pnl_refresh"):
            enqueued_key = f"{key}_enqueued"
            skipped_key = f"{key}_skipped"
            if result.get(enqueued_key):
                counts[enqueued_key] += 1
            else:
                counts[skipped_key] += 1
    counts["symbols_affected"] = sorted(affected)
    return counts
