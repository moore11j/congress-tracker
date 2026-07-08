from __future__ import annotations

import json
from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from typing import Any

from sqlalchemy import func, or_, select
from sqlalchemy.orm import Session

from app.models import Event, TradeOutcome
from app.services.congress_outcome_eligibility import congress_equity_outcome_eligibility
from app.services.member_performance import METHODOLOGY_VERSION, compute_congress_trade_outcomes


def _parse_payload(payload_json: object) -> dict:
    if isinstance(payload_json, dict):
        return dict(payload_json)
    if isinstance(payload_json, str) and payload_json:
        try:
            parsed = json.loads(payload_json)
        except Exception:
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def _payload_date(payload: dict, *keys: str) -> date | None:
    for key in keys:
        value = payload.get(key)
        if isinstance(value, date) and not isinstance(value, datetime):
            return value
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, str) and value.strip():
            try:
                return date.fromisoformat(value[:10])
            except ValueError:
                continue
    return None


@dataclass(frozen=True)
class CongressOutcomeEventSnapshot:
    id: int
    event_type: str
    ts: datetime | None
    event_date: datetime | None
    symbol: str | None
    source: str | None
    member_name: str | None
    member_bioguide_id: str | None
    chamber: str | None
    party: str | None
    trade_type: str | None
    transaction_type: str | None
    amount_min: int | None
    amount_max: int | None
    payload_json: object


def _snapshot_event(event: Event) -> CongressOutcomeEventSnapshot:
    return CongressOutcomeEventSnapshot(
        id=event.id,
        event_type=event.event_type,
        ts=event.ts,
        event_date=event.event_date,
        symbol=event.symbol,
        source=event.source,
        member_name=event.member_name,
        member_bioguide_id=event.member_bioguide_id,
        chamber=event.chamber,
        party=event.party,
        trade_type=event.trade_type,
        transaction_type=event.transaction_type,
        amount_min=event.amount_min,
        amount_max=event.amount_max,
        payload_json=event.payload_json,
    )


def _event_report_date(event: CongressOutcomeEventSnapshot, payload: dict) -> date | None:
    return (
        _payload_date(payload, "report_date", "reportDate", "filing_date", "filingDate")
        or (event.event_date.date() if event.event_date else None)
        or (event.ts.date() if event.ts else None)
    )


def _parse_date(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return date.fromisoformat(value[:10])
    except ValueError:
        return None


def _safe_outcome_status(status: str | None) -> str | None:
    if not status:
        return None
    if status.startswith("provider_"):
        return "price_unavailable"
    return status


@dataclass(frozen=True)
class CongressOutcomeRepairRow:
    event_id: int
    transaction_id: int | None
    member_name: str | None
    symbol: str | None
    company_name: str | None
    asset_class: str | None
    trade_date: str | None
    report_date: str | None
    side: str | None
    amount_min: int | None
    amount_max: int | None
    has_estimated_price: bool
    has_trade_outcome: bool
    proposed_estimated_price: float | None
    proposed_pnl_pct: float | None
    skip_reason: str | None
    safe_to_apply: bool

    def as_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "transaction_id": self.transaction_id,
            "member_name": self.member_name,
            "symbol": self.symbol,
            "company_name": self.company_name,
            "asset_class": self.asset_class,
            "trade_date": self.trade_date,
            "report_date": self.report_date,
            "side": self.side,
            "amount_min": self.amount_min,
            "amount_max": self.amount_max,
            "has_estimated_price": self.has_estimated_price,
            "has_trade_outcome": self.has_trade_outcome,
            "proposed_estimated_price": self.proposed_estimated_price,
            "proposed_pnl_pct": self.proposed_pnl_pct,
            "skip_reason": self.skip_reason,
            "safe_to_apply": self.safe_to_apply,
        }


def _event_query_since(since_report_date: date | None):
    sort_ts = func.coalesce(Event.event_date, Event.ts)
    q = select(Event).where(Event.event_type == "congress_trade").order_by(sort_ts.desc(), Event.id.desc())
    if since_report_date is not None:
        since_dt = datetime.combine(since_report_date, time.min, tzinfo=timezone.utc)
        q = q.where(or_(Event.event_date >= since_dt, Event.ts >= since_dt))
    return q


def _new_trade_outcome(
    event: CongressOutcomeEventSnapshot,
    outcome: dict,
    *,
    benchmark_symbol: str,
    computed_at: datetime,
) -> TradeOutcome:
    return TradeOutcome(
        event_id=event.id,
        member_id=outcome.get("member_id"),
        member_name=outcome.get("member_name"),
        symbol=outcome.get("symbol"),
        trade_type=outcome.get("trade_type"),
        source=outcome.get("source"),
        trade_date=_parse_date(outcome.get("trade_date")),
        entry_price=outcome.get("entry_price"),
        entry_price_date=_parse_date(outcome.get("entry_price_date")),
        current_price=outcome.get("current_price"),
        current_price_date=_parse_date(outcome.get("current_price_date")),
        benchmark_symbol=outcome.get("benchmark_symbol") or benchmark_symbol,
        benchmark_entry_price=outcome.get("benchmark_entry_price"),
        benchmark_current_price=outcome.get("benchmark_current_price"),
        return_pct=outcome.get("return_pct"),
        benchmark_return_pct=outcome.get("benchmark_return_pct"),
        alpha_pct=outcome.get("alpha_pct"),
        holding_days=outcome.get("holding_days"),
        amount_min=outcome.get("amount_min"),
        amount_max=outcome.get("amount_max"),
        scoring_status=outcome.get("scoring_status") or "unknown",
        scoring_error=outcome.get("scoring_error"),
        methodology_version=outcome.get("methodology_version") or METHODOLOGY_VERSION,
        computed_at=computed_at,
    )


def repair_recent_congress_outcomes(
    db: Session,
    *,
    since_report_date: date | None,
    dry_run: bool,
    limit: int | None = None,
    benchmark_symbol: str = "^GSPC",
) -> dict[str, Any]:
    q = _event_query_since(since_report_date)
    if limit is not None and limit > 0:
        q = q.limit(limit)
    events = [_snapshot_event(event) for event in db.execute(q).scalars().all()]
    event_ids = [event.id for event in events]
    outcomes = (
        db.execute(select(TradeOutcome).where(TradeOutcome.event_id.in_(event_ids))).scalars().all()
        if event_ids
        else []
    )
    outcome_by_event_id = {row.event_id: row for row in outcomes}

    missing_eligible_events: list[CongressOutcomeEventSnapshot] = []
    row_context: dict[int, dict[str, Any]] = {}
    rows: list[CongressOutcomeRepairRow] = []
    status_counts: Counter[str] = Counter()

    for event in events:
        payload = _parse_payload(event.payload_json)
        report_date = _event_report_date(event, payload)
        if since_report_date is not None and (report_date is None or report_date < since_report_date):
            continue

        eligibility = congress_equity_outcome_eligibility(
            event_type=event.event_type,
            symbol=event.symbol,
            payload=payload,
            amount_min=event.amount_min,
            amount_max=event.amount_max,
            side=event.trade_type or event.transaction_type,
        )
        existing = outcome_by_event_id.get(event.id)
        asset_class = payload.get("asset_class") or payload.get("assetClass")
        company_name = payload.get("company_name") or payload.get("companyName") or payload.get("security_name") or payload.get("securityName")
        transaction_id = payload.get("transaction_id") or payload.get("transactionId")
        transaction_id = transaction_id if isinstance(transaction_id, int) else None

        row_context[event.id] = {
            "payload": payload,
            "report_date": report_date.isoformat() if report_date else None,
            "eligibility": eligibility,
            "asset_class": asset_class,
            "company_name": company_name,
            "transaction_id": transaction_id,
        }

        if not eligibility.eligible:
            status_counts[eligibility.skip_reason or "not_eligible"] += 1
            rows.append(
                CongressOutcomeRepairRow(
                    event_id=event.id,
                    transaction_id=transaction_id,
                    member_name=event.member_name,
                    symbol=eligibility.symbol,
                    company_name=company_name,
                    asset_class=asset_class,
                    trade_date=eligibility.trade_date,
                    report_date=report_date.isoformat() if report_date else None,
                    side=eligibility.side,
                    amount_min=event.amount_min,
                    amount_max=event.amount_max,
                    has_estimated_price=bool(existing and existing.entry_price is not None),
                    has_trade_outcome=existing is not None,
                    proposed_estimated_price=None,
                    proposed_pnl_pct=None,
                    skip_reason=eligibility.skip_reason,
                    safe_to_apply=False,
                )
            )
            continue

        if existing is not None:
            skip_reason = None
            if existing.entry_price is None or existing.return_pct is None:
                skip_reason = _safe_outcome_status(existing.scoring_status) or "existing_outcome_incomplete"
            status_counts["existing_outcome"] += 1
            rows.append(
                CongressOutcomeRepairRow(
                    event_id=event.id,
                    transaction_id=transaction_id,
                    member_name=event.member_name,
                    symbol=eligibility.symbol,
                    company_name=company_name,
                    asset_class=asset_class,
                    trade_date=eligibility.trade_date,
                    report_date=report_date.isoformat() if report_date else None,
                    side=eligibility.side,
                    amount_min=event.amount_min,
                    amount_max=event.amount_max,
                    has_estimated_price=existing.entry_price is not None,
                    has_trade_outcome=True,
                    proposed_estimated_price=None,
                    proposed_pnl_pct=None,
                    skip_reason=skip_reason,
                    safe_to_apply=False,
                )
            )
            continue

        missing_eligible_events.append(event)

    computed = compute_congress_trade_outcomes(
        db=db,
        events=missing_eligible_events,
        benchmark_symbol=benchmark_symbol,
    ) if missing_eligible_events else []
    computed_by_event_id = {int(outcome["event_id"]): outcome for outcome in computed}

    inserted = 0
    now = datetime.now(timezone.utc)
    for event in missing_eligible_events:
        context = row_context[event.id]
        eligibility = context["eligibility"]
        outcome = computed_by_event_id.get(event.id)
        status = _safe_outcome_status(outcome.get("scoring_status") if outcome else None) or "missing_outcome"
        status_counts[status] += 1
        if outcome is not None and not dry_run:
            db.add(_new_trade_outcome(event, outcome, benchmark_symbol=benchmark_symbol, computed_at=now))
            inserted += 1

        rows.append(
            CongressOutcomeRepairRow(
                event_id=event.id,
                transaction_id=context["transaction_id"],
                member_name=event.member_name,
                symbol=eligibility.symbol,
                company_name=context["company_name"],
                asset_class=context["asset_class"],
                trade_date=eligibility.trade_date,
                report_date=context["report_date"],
                side=eligibility.side,
                amount_min=event.amount_min,
                amount_max=event.amount_max,
                has_estimated_price=False,
                has_trade_outcome=False,
                proposed_estimated_price=outcome.get("entry_price") if outcome else None,
                proposed_pnl_pct=outcome.get("return_pct") if outcome else None,
                skip_reason=None if outcome and outcome.get("scoring_status") == "ok" else status,
                safe_to_apply=outcome is not None,
            )
        )

    if dry_run:
        db.rollback()
    elif inserted:
        db.commit()

    rows.sort(key=lambda row: (row.report_date or "", row.event_id), reverse=True)
    missing_after_apply = len(missing_eligible_events) if dry_run else 0
    return {
        "dry_run": dry_run,
        "since_report_date": since_report_date.isoformat() if since_report_date else None,
        "scanned": len(events),
        "eligible_missing_outcomes": len(missing_eligible_events),
        "inserted": inserted,
        "missing_after_apply": missing_after_apply,
        "status_counts": dict(status_counts),
        "rows": [row.as_dict() for row in rows],
    }
