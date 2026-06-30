from __future__ import annotations

import json
import math
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any

from sqlalchemy import and_, func, inspect, or_, select
from sqlalchemy.orm import Session

from app.models import (
    Event,
    InstitutionalActivityEvent,
    InstitutionalFiling,
    InstitutionalHolder,
    InstitutionalHolderIndustryBreakdown,
    InstitutionalIndustrySummary,
    InstitutionalPosition,
    InstitutionalPositionChange,
    InstitutionalSymbolSummary,
)
from app.utils.symbols import normalize_symbol

INSTITUTIONAL_SOURCE_LABEL = "Institutional Activity"
INSTITUTIONAL_EVENT_SOURCE = "institutional_13f"
INSTITUTIONAL_EVENT_TYPES = (
    "institutional_accumulation",
    "institutional_distribution",
    "new_institutional_position",
    "major_holder_reduction",
    "major_holder_exit",
    "cluster_accumulation",
    "cluster_distribution",
    "smart_money_confirmation",
    "crowded_long",
    "contrarian_accumulation",
)
INSTITUTIONAL_ACTIVITY_TOOLTIP = (
    "Institutional activity is based on reported 13F holdings. These filings disclose quarter-end "
    "positions and may not reflect real-time trading. Walnut uses the filing date for freshness "
    "and the reported position change for direction."
)
INSTITUTIONAL_NET_REPORTED_30D_TOOLTIP = (
    "This measures net institutional position changes disclosed by filings received in the last 30 days. "
    "It does not mean the positions were traded during the last 30 days."
)
PASSIVE_HOLDER_PATTERNS = (
    "vanguard",
    "blackrock",
    "state street",
    "ishares",
    "spdr",
    "invesco",
    "fidelity index",
    "fidelity management trust",
    "dimensional fund advisors",
    "geode capital",
    "northern trust",
    "ssga",
)


@dataclass(frozen=True)
class InstitutionalFilingCandidate:
    cik: str
    holder_name: str | None
    accession_number: str | None
    filing_date: date
    report_year: int
    report_quarter: int
    report_period_end: date | None
    filing_url: str | None
    form_type: str | None
    is_amendment: bool
    raw: dict[str, Any]


@dataclass(frozen=True)
class InstitutionalPositionPayload:
    symbol: str | None
    normalized_symbol: str | None
    cusip: str | None
    issuer_name: str | None
    shares: float | None
    value_usd: float | None
    put_call: str | None
    investment_discretion: str | None
    voting_authority: dict[str, Any] | str | None
    portfolio_weight: float | None
    ownership_pct: float | None
    raw: dict[str, Any]


def normalize_cik(value: Any) -> str | None:
    if value is None:
        return None
    digits = re.sub(r"\D+", "", str(value))
    if not digits:
        return None
    return digits[-10:].zfill(10)


def normalize_holder_name(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    cleaned = re.sub(r"\s+", " ", value.strip())
    if not cleaned:
        return None
    return cleaned.casefold()


def is_passive_like_holder(holder_name: str | None) -> bool:
    normalized = normalize_holder_name(holder_name) or ""
    return any(pattern in normalized for pattern in PASSIVE_HOLDER_PATTERNS)


def recency_decay_30d(filing_date: date | datetime | None, *, now: date | datetime | None = None) -> float:
    if filing_date is None:
        return 0.0
    filing_day = filing_date.date() if isinstance(filing_date, datetime) else filing_date
    current_day = (now.date() if isinstance(now, datetime) else now) or datetime.now(timezone.utc).date()
    days = max((current_day - filing_day).days, 0)
    if days > 30:
        return 0.0
    return max(0.0, min(1.0, 1.0 - (days / 30.0)))


def institutional_confirmation_contribution(
    *,
    filing_date: date | datetime | None,
    materiality_score: float | int | None,
    direction: str | None,
    holder_quality_weight: float | int | None = 1.0,
    passive_adjustment: float | int | None = 1.0,
    now: date | datetime | None = None,
) -> float:
    recency = recency_decay_30d(filing_date, now=now)
    if recency <= 0:
        return 0.0
    signed = 1.0 if direction == "bullish" else -1.0 if direction == "bearish" else 0.0
    if signed == 0:
        return 0.0
    materiality = _clamp_float(materiality_score, 0.0, 100.0) / 100.0
    quality = _clamp_float(holder_quality_weight, 0.1, 2.0)
    passive = _clamp_float(passive_adjustment, 0.1, 1.5)
    contribution = recency * materiality * quality * passive * 15.0 * signed
    return round(_clamp_float(contribution, -15.0, 15.0), 2)


def parse_latest_filing(row: dict[str, Any]) -> InstitutionalFilingCandidate | None:
    cik = normalize_cik(_first_text(row, "cik", "holderCik", "institutionCik", "managerCik", "investorCik"))
    filing_date = _parse_date(_first_value(row, "filingDate", "filing_date", "date", "acceptedDate", "accepted_date"))
    report_period_end = _parse_date(
        _first_value(row, "reportPeriod", "report_period", "periodOfReport", "period_of_report", "reportDate", "report_date")
    )
    report_year = _first_int(row, "reportYear", "report_year", "year")
    report_quarter = _first_int(row, "reportQuarter", "report_quarter", "quarter")
    if report_period_end and (report_year is None or report_quarter is None):
        report_year = report_year or report_period_end.year
        report_quarter = report_quarter or _quarter_for_date(report_period_end)
    if not cik or filing_date is None or report_year is None or report_quarter is None:
        return None
    form_type = _first_text(row, "formType", "form_type", "form", "type")
    accession_number = _first_text(row, "accessionNumber", "accession_number", "accessionNo")
    holder_name = _first_text(row, "holderName", "holder", "institutionName", "managerName", "investorName", "name")
    is_amendment = bool(form_type and "/A" in form_type.upper()) or _boolish(_first_value(row, "isAmendment", "amendment"))
    return InstitutionalFilingCandidate(
        cik=cik,
        holder_name=holder_name,
        accession_number=accession_number,
        filing_date=filing_date,
        report_year=int(report_year),
        report_quarter=max(1, min(int(report_quarter), 4)),
        report_period_end=report_period_end,
        filing_url=_first_text(row, "filingUrl", "filing_url", "url", "link", "finalLink"),
        form_type=form_type,
        is_amendment=is_amendment,
        raw=row,
    )


def parse_position(row: dict[str, Any]) -> InstitutionalPositionPayload | None:
    raw_symbol = _first_text(row, "symbol", "ticker", "securityTicker", "tickerSymbol")
    normalized_symbol = normalize_symbol(raw_symbol)
    cusip = _first_text(row, "cusip", "cusipNumber", "securityCusip")
    if cusip:
        cusip = cusip.strip().upper()
    if not normalized_symbol and not cusip:
        return None
    voting_authority = _first_value(row, "votingAuthority", "voting_authority", "votingAuthorityJson")
    return InstitutionalPositionPayload(
        symbol=raw_symbol.strip().upper() if raw_symbol else normalized_symbol,
        normalized_symbol=normalized_symbol,
        cusip=cusip,
        issuer_name=_first_text(row, "issuerName", "issuer_name", "nameOfIssuer", "securityName", "companyName"),
        shares=_first_number(row, "shares", "sharesNumber", "sshPrnamt", "shrsOrPrnAmt", "balance"),
        value_usd=_first_number(row, "valueUsd", "value_usd", "marketValue", "market_value", "marketValueUsd", "value"),
        put_call=_clean_option_side(_first_text(row, "putCall", "put_call", "optionType")),
        investment_discretion=_first_text(row, "investmentDiscretion", "investment_discretion"),
        voting_authority=voting_authority,
        portfolio_weight=_first_number(row, "portfolioWeight", "portfolio_weight", "weight", "weightPct"),
        ownership_pct=_first_number(row, "ownershipPct", "ownership_pct", "ownershipPercentage"),
        raw=row,
    )


def upsert_institutional_holder(db: Session, candidate: InstitutionalFilingCandidate) -> InstitutionalHolder:
    holder = db.get(InstitutionalHolder, candidate.cik)
    if holder is None:
        holder = InstitutionalHolder(cik=candidate.cik)
        db.add(holder)
    if candidate.holder_name:
        holder.holder_name = candidate.holder_name
        holder.normalized_holder_name = normalize_holder_name(candidate.holder_name)
        holder.is_passive_like = is_passive_like_holder(candidate.holder_name)
    holder.latest_filing_date = max(
        [value for value in (holder.latest_filing_date, candidate.filing_date) if value is not None],
        default=candidate.filing_date,
    )
    if (
        holder.latest_report_year is None
        or (candidate.report_year, candidate.report_quarter) >= (holder.latest_report_year, holder.latest_report_quarter or 0)
    ):
        holder.latest_report_year = candidate.report_year
        holder.latest_report_quarter = candidate.report_quarter
    holder.updated_at = datetime.now(timezone.utc)
    return holder


def upsert_institutional_filing(db: Session, candidate: InstitutionalFilingCandidate) -> tuple[InstitutionalFiling, bool]:
    filing = None
    if candidate.accession_number:
        filing = db.execute(
            select(InstitutionalFiling).where(InstitutionalFiling.accession_number == candidate.accession_number)
        ).scalar_one_or_none()
    if filing is None:
        filing = db.execute(
            select(InstitutionalFiling).where(
                InstitutionalFiling.cik == candidate.cik,
                InstitutionalFiling.report_year == candidate.report_year,
                InstitutionalFiling.report_quarter == candidate.report_quarter,
                InstitutionalFiling.filing_date == candidate.filing_date,
                InstitutionalFiling.form_type == candidate.form_type,
            )
        ).scalar_one_or_none()
    created = filing is None
    if filing is None:
        filing = InstitutionalFiling(
            cik=candidate.cik,
            filing_date=candidate.filing_date,
            report_year=candidate.report_year,
            report_quarter=candidate.report_quarter,
        )
        db.add(filing)
    filing.accession_number = candidate.accession_number
    filing.report_period_end = candidate.report_period_end
    filing.filing_url = candidate.filing_url
    filing.form_type = candidate.form_type
    filing.is_amendment = candidate.is_amendment
    filing.raw_metadata_json = json.dumps(candidate.raw, sort_keys=True, default=str)
    filing.updated_at = datetime.now(timezone.utc)
    return filing, created


def upsert_positions_for_filing(
    db: Session,
    *,
    filing: InstitutionalFiling,
    rows: list[dict[str, Any]],
) -> dict[str, int]:
    inserted = updated = skipped = 0
    for row in rows:
        payload = parse_position(row)
        if payload is None:
            skipped += 1
            continue
        existing = _find_position(db, filing.id, payload.normalized_symbol, payload.cusip, payload.put_call)
        if existing is None:
            existing = InstitutionalPosition(
                filing_id=filing.id,
                cik=filing.cik,
                report_year=filing.report_year,
                report_quarter=filing.report_quarter,
                filing_date=filing.filing_date,
            )
            db.add(existing)
            inserted += 1
        else:
            updated += 1
        existing.symbol = payload.symbol
        existing.normalized_symbol = payload.normalized_symbol
        existing.cusip = payload.cusip
        existing.issuer_name = payload.issuer_name
        existing.shares = payload.shares
        existing.value_usd = payload.value_usd
        existing.put_call = payload.put_call
        existing.investment_discretion = payload.investment_discretion
        existing.voting_authority = json.dumps(payload.voting_authority, sort_keys=True, default=str) if isinstance(payload.voting_authority, dict) else payload.voting_authority
        existing.portfolio_weight = payload.portfolio_weight
        existing.ownership_pct = payload.ownership_pct
        existing.updated_at = datetime.now(timezone.utc)
    return {"inserted_positions": inserted, "updated_positions": updated, "skipped_positions": skipped}


def process_filing_changes_and_events(db: Session, filing: InstitutionalFiling) -> dict[str, int]:
    if filing.is_amendment:
        # TODO: implement 13F-HR/A supersession so amended filings safely replace
        # prior quarter events instead of duplicating user-facing activity.
        filing.processed_at = datetime.now(timezone.utc)
        return {
            "changes": 0,
            "summaries": 0,
            "activity_events": 0,
            "feed_events": 0,
            "amendment_suppressed": 1,
        }

    current_positions = db.execute(
        select(InstitutionalPosition).where(InstitutionalPosition.filing_id == filing.id)
    ).scalars().all()
    holder = db.get(InstitutionalHolder, filing.cik)
    holder_name = holder.holder_name if holder else None
    holder_quality_weight = _holder_quality_weight(holder)
    prior_positions = _prior_positions_for_filing(db, filing)
    prior_by_key = {_position_match_key(position): position for position in prior_positions}
    current_by_key = {_position_match_key(position): position for position in current_positions}

    changes = 0
    symbols: set[str] = set()
    for key, current in current_by_key.items():
        prior = prior_by_key.get(key)
        change = upsert_position_change(
            db,
            filing=filing,
            holder_name=holder_name,
            current=current,
            prior=prior,
            holder_quality_weight=holder_quality_weight,
            passive_like=bool(holder and holder.is_passive_like),
        )
        if change:
            changes += 1
            if change.normalized_symbol:
                symbols.add(change.normalized_symbol)

    for key, prior in prior_by_key.items():
        if key in current_by_key:
            continue
        change = upsert_position_change(
            db,
            filing=filing,
            holder_name=holder_name,
            current=None,
            prior=prior,
            holder_quality_weight=holder_quality_weight,
            passive_like=bool(holder and holder.is_passive_like),
        )
        if change:
            changes += 1
            if change.normalized_symbol:
                symbols.add(change.normalized_symbol)

    summaries = events = feed_events = 0
    for symbol in sorted(symbols):
        summary = refresh_symbol_summary(db, symbol, filing.report_year, filing.report_quarter)
        if summary:
            summaries += 1
            events += generate_activity_events_for_symbol(db, summary)
            feed_events += materialize_feed_events_for_symbol(db, summary)
    filing.processed_at = datetime.now(timezone.utc)
    return {"changes": changes, "summaries": summaries, "activity_events": events, "feed_events": feed_events}


def upsert_position_change(
    db: Session,
    *,
    filing: InstitutionalFiling,
    holder_name: str | None,
    current: InstitutionalPosition | None,
    prior: InstitutionalPosition | None,
    holder_quality_weight: float,
    passive_like: bool,
) -> InstitutionalPositionChange | None:
    symbol = normalize_symbol(current.normalized_symbol if current else prior.normalized_symbol if prior else None)
    if not symbol:
        return None
    cusip = (current.cusip if current else prior.cusip if prior else None) or None
    prev_shares = prior.shares if prior else None
    curr_shares = current.shares if current else None
    prev_value = prior.value_usd if prior else None
    curr_value = current.value_usd if current else None
    shares_delta = _delta(curr_shares, prev_shares)
    value_delta = _delta(curr_value, prev_value)
    shares_delta_pct = _pct_delta(curr_shares, prev_shares)
    value_delta_pct = _pct_delta(curr_value, prev_value)
    change_type = _change_type(prev_shares, curr_shares, prev_value, curr_value)
    direction = _direction_for_change(change_type)
    materiality_score = calculate_materiality_score(
        change_type=change_type,
        prev_value_usd=prev_value,
        curr_value_usd=curr_value,
        value_delta_usd=value_delta,
        value_delta_pct=value_delta_pct,
        ownership_pct_delta=_delta(current.ownership_pct if current else None, prior.ownership_pct if prior else None),
        holder_quality_weight=holder_quality_weight,
    )
    passive_adjustment = 0.45 if passive_like and materiality_score < 90 else 0.75 if passive_like else 1.0
    passive_adjusted_score = materiality_score * holder_quality_weight * passive_adjustment
    is_material = is_material_change(
        change_type=change_type,
        prev_value_usd=prev_value,
        curr_value_usd=curr_value,
        value_delta_usd=value_delta,
        value_delta_pct=value_delta_pct,
        holder_quality_weight=holder_quality_weight,
    )

    existing = db.execute(
        select(InstitutionalPositionChange).where(
            InstitutionalPositionChange.cik == filing.cik,
            InstitutionalPositionChange.normalized_symbol == symbol,
            InstitutionalPositionChange.report_year == filing.report_year,
            InstitutionalPositionChange.report_quarter == filing.report_quarter,
            InstitutionalPositionChange.change_type == change_type,
            _nullable_equals(InstitutionalPositionChange.cusip, cusip),
        )
    ).scalar_one_or_none()
    if existing is None:
        existing = InstitutionalPositionChange(
            cik=filing.cik,
            normalized_symbol=symbol,
            report_year=filing.report_year,
            report_quarter=filing.report_quarter,
            filing_date=filing.filing_date,
            change_type=change_type,
        )
        db.add(existing)
    existing.holder_name = holder_name
    existing.symbol = symbol
    existing.cusip = cusip
    existing.prev_shares = prev_shares
    existing.curr_shares = curr_shares
    existing.shares_delta = shares_delta
    existing.shares_delta_pct = shares_delta_pct
    existing.prev_value_usd = prev_value
    existing.curr_value_usd = curr_value
    existing.value_delta_usd = value_delta
    existing.value_delta_pct = value_delta_pct
    existing.prev_portfolio_weight = prior.portfolio_weight if prior else None
    existing.curr_portfolio_weight = current.portfolio_weight if current else None
    existing.portfolio_weight_delta = _delta(current.portfolio_weight if current else None, prior.portfolio_weight if prior else None)
    existing.prev_ownership_pct = prior.ownership_pct if prior else None
    existing.curr_ownership_pct = current.ownership_pct if current else None
    existing.ownership_pct_delta = _delta(current.ownership_pct if current else None, prior.ownership_pct if prior else None)
    existing.direction = direction
    existing.materiality_score = round(materiality_score, 2)
    existing.holder_quality_weight = round(holder_quality_weight, 4)
    existing.passive_adjusted_score = round(passive_adjusted_score, 2)
    existing.is_material = is_material
    existing.updated_at = datetime.now(timezone.utc)
    return existing


def calculate_materiality_score(
    *,
    change_type: str,
    prev_value_usd: float | None,
    curr_value_usd: float | None,
    value_delta_usd: float | None,
    value_delta_pct: float | None,
    ownership_pct_delta: float | None,
    holder_quality_weight: float = 1.0,
) -> float:
    magnitude = max(abs(value_delta_usd or 0.0), abs(curr_value_usd or 0.0), abs(prev_value_usd or 0.0))
    pct = abs(value_delta_pct or 0.0)
    ownership = abs(ownership_pct_delta or 0.0)
    base = 0.0
    if change_type in {"new_position", "exit"}:
        base += 25.0
    if magnitude >= 100_000_000:
        base += 45.0
    elif magnitude >= 50_000_000:
        base += 36.0
    elif magnitude >= 10_000_000:
        base += 28.0
    elif magnitude >= 5_000_000:
        base += 18.0
    elif magnitude > 0:
        base += min(15.0, magnitude / 500_000)
    base += min(20.0, pct / 2.0)
    base += min(15.0, ownership * 10.0)
    base *= _clamp_float(holder_quality_weight, 0.25, 1.75)
    return _clamp_float(base, 0.0, 100.0)


def is_material_change(
    *,
    change_type: str,
    prev_value_usd: float | None,
    curr_value_usd: float | None,
    value_delta_usd: float | None,
    value_delta_pct: float | None,
    holder_quality_weight: float = 1.0,
) -> bool:
    prev_value = abs(prev_value_usd or 0.0)
    curr_value = abs(curr_value_usd or 0.0)
    delta_value = abs(value_delta_usd or 0.0)
    pct = abs(value_delta_pct or 0.0)
    if change_type == "new_position" and curr_value >= 10_000_000:
        return True
    if change_type == "exit" and prev_value >= 10_000_000:
        return True
    if delta_value >= 10_000_000:
        return True
    if pct >= 25 and max(curr_value, prev_value) >= 5_000_000:
        return True
    if holder_quality_weight > 1.2 and delta_value >= 5_000_000:
        return True
    return False


def refresh_symbol_summary(
    db: Session,
    symbol: str,
    report_year: int,
    report_quarter: int,
) -> InstitutionalSymbolSummary | None:
    normalized = normalize_symbol(symbol)
    if not normalized:
        return None
    changes = db.execute(
        select(InstitutionalPositionChange).where(
            InstitutionalPositionChange.normalized_symbol == normalized,
            InstitutionalPositionChange.report_year == report_year,
            InstitutionalPositionChange.report_quarter == report_quarter,
        )
    ).scalars().all()
    current_positions = db.execute(
        select(InstitutionalPosition).where(
            InstitutionalPosition.normalized_symbol == normalized,
            InstitutionalPosition.report_year == report_year,
            InstitutionalPosition.report_quarter == report_quarter,
        )
    ).scalars().all()
    if not changes and not current_positions:
        return None
    latest_filing_date = max(
        [value for value in [*(change.filing_date for change in changes), *(position.filing_date for position in current_positions)] if value],
        default=None,
    )
    holders_increased = sum(1 for change in changes if change.change_type == "increase")
    holders_reduced = sum(1 for change in changes if change.change_type == "decrease")
    new_positions = sum(1 for change in changes if change.change_type == "new_position")
    exits = sum(1 for change in changes if change.change_type == "exit")
    unchanged = sum(1 for change in changes if change.change_type == "unchanged")
    total_value = sum(float(position.value_usd or 0.0) for position in current_positions)
    net_value_delta = sum(float(change.value_delta_usd or 0.0) for change in changes)
    net_shares_delta = sum(float(change.shares_delta or 0.0) for change in changes)
    accumulation_score = sum(float(change.passive_adjusted_score or 0.0) for change in changes if change.direction == "bullish")
    distribution_score = sum(float(change.passive_adjusted_score or 0.0) for change in changes if change.direction == "bearish")
    direction = "mixed"
    if accumulation_score > distribution_score * 1.2:
        direction = "bullish"
    elif distribution_score > accumulation_score * 1.2:
        direction = "bearish"
    elif accumulation_score <= 0 and distribution_score <= 0:
        direction = "neutral"
    materiality = min(100.0, max(accumulation_score, distribution_score, abs(net_value_delta) / 1_000_000))
    top_accumulators = _top_change_rows(changes, direction="bullish")
    top_reducers = _top_change_rows(changes, direction="bearish")

    summary = db.execute(
        select(InstitutionalSymbolSummary).where(
            InstitutionalSymbolSummary.normalized_symbol == normalized,
            InstitutionalSymbolSummary.report_year == report_year,
            InstitutionalSymbolSummary.report_quarter == report_quarter,
        )
    ).scalar_one_or_none()
    if summary is None:
        summary = InstitutionalSymbolSummary(
            symbol=normalized,
            normalized_symbol=normalized,
            report_year=report_year,
            report_quarter=report_quarter,
        )
        db.add(summary)
    summary.latest_filing_date = latest_filing_date
    summary.total_holders = len({position.cik for position in current_positions if position.cik})
    summary.holders_increased = holders_increased
    summary.holders_reduced = holders_reduced
    summary.new_positions = new_positions
    summary.exits = exits
    summary.unchanged_holders = unchanged
    summary.total_value_usd = round(total_value, 2)
    summary.net_value_delta_usd = round(net_value_delta, 2)
    summary.net_shares_delta = round(net_shares_delta, 2)
    ownership_values = [position.ownership_pct for position in current_positions if position.ownership_pct is not None]
    summary.institutional_ownership_pct = round(sum(ownership_values), 4) if ownership_values else None
    summary.put_call_ratio = _put_call_ratio(current_positions)
    summary.accumulation_score = round(accumulation_score, 2)
    summary.distribution_score = round(distribution_score, 2)
    summary.direction = direction
    summary.materiality_score = round(materiality, 2)
    summary.top_accumulators_json = json.dumps(top_accumulators, sort_keys=True, default=str)
    summary.top_reducers_json = json.dumps(top_reducers, sort_keys=True, default=str)
    summary.generated_at = datetime.now(timezone.utc)
    summary.updated_at = datetime.now(timezone.utc)
    return summary


def generate_activity_events_for_symbol(db: Session, summary: InstitutionalSymbolSummary) -> int:
    changes = db.execute(
        select(InstitutionalPositionChange).where(
            InstitutionalPositionChange.normalized_symbol == summary.normalized_symbol,
            InstitutionalPositionChange.report_year == summary.report_year,
            InstitutionalPositionChange.report_quarter == summary.report_quarter,
            InstitutionalPositionChange.is_material.is_(True),
        )
    ).scalars().all()
    created = 0
    for change in changes:
        event_type = _event_type_for_change(change)
        if _upsert_activity_event_from_change(db, change, event_type=event_type):
            created += 1
    if summary.materiality_score >= 80 or abs(summary.net_value_delta_usd or 0) >= 50_000_000 or (summary.holders_increased - summary.holders_reduced) >= 10:
        event_type = "cluster_accumulation" if summary.direction == "bullish" else "cluster_distribution" if summary.direction == "bearish" else "smart_money_confirmation"
        if _upsert_activity_event_from_summary(db, summary, event_type=event_type):
            created += 1
    return created


def materialize_feed_events_for_symbol(db: Session, summary: InstitutionalSymbolSummary) -> int:
    activities = db.execute(
        select(InstitutionalActivityEvent).where(
            InstitutionalActivityEvent.normalized_symbol == summary.normalized_symbol,
            InstitutionalActivityEvent.report_year == summary.report_year,
            InstitutionalActivityEvent.report_quarter == summary.report_quarter,
            InstitutionalActivityEvent.feed_visible.is_(True),
            InstitutionalActivityEvent.materiality_score >= 50,
        )
    ).scalars().all()
    created = 0
    for activity in activities:
        if _upsert_feed_event(db, activity):
            created += 1
    return created


def get_institutional_activity_summaries_for_symbols(
    db: Session,
    symbols: list[str],
    *,
    lookback_days: int = 30,
    feature_enabled: bool = True,
) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    normalized_symbols = sorted({symbol for raw in symbols if (symbol := normalize_symbol(raw))})
    if not normalized_symbols:
        return {}, _availability(status="unavailable", enabled=feature_enabled)
    if not feature_enabled:
        return {symbol: unavailable_institutional_summary(symbol, status="disabled") for symbol in normalized_symbols}, _availability(status="disabled", enabled=False)
    inspector = inspect(db.get_bind())
    if not inspector.has_table(InstitutionalSymbolSummary.__tablename__):
        return {symbol: unavailable_institutional_summary(symbol) for symbol in normalized_symbols}, _availability(status="unavailable", enabled=True)
    row_count = db.execute(select(func.count()).select_from(InstitutionalSymbolSummary)).scalar() or 0
    if int(row_count) <= 0:
        return {symbol: unavailable_institutional_summary(symbol, status="not_configured") for symbol in normalized_symbols}, _availability(status="not_configured", enabled=True)

    latest_rows = _latest_symbol_summaries(db, normalized_symbols)
    recent_events = _recent_activity_events_by_symbol(db, normalized_symbols, lookback_days=lookback_days)
    results: dict[str, dict[str, Any]] = {}
    for symbol in normalized_symbols:
        summary = latest_rows.get(symbol)
        events = recent_events.get(symbol, [])
        if summary is None:
            results[symbol] = unavailable_institutional_summary(symbol, status="no_data")
            continue
        results[symbol] = institutional_summary_payload(summary, events, lookback_days=lookback_days)
    return results, _availability(status="ok", enabled=True)


def get_ticker_institutional_activity(
    db: Session,
    symbol: str,
    *,
    lookback_days: int = 30,
    limit: int = 25,
) -> dict[str, Any]:
    normalized = normalize_symbol(symbol)
    if not normalized:
        return unavailable_institutional_summary(symbol, status="invalid_symbol")
    summaries, availability = get_institutional_activity_summaries_for_symbols(db, [normalized], lookback_days=lookback_days)
    events = db.execute(
        select(InstitutionalActivityEvent)
        .where(InstitutionalActivityEvent.normalized_symbol == normalized)
        .order_by(InstitutionalActivityEvent.filing_date.desc(), InstitutionalActivityEvent.materiality_score.desc())
        .limit(max(1, min(int(limit or 25), 100)))
    ).scalars().all()
    return {
        "symbol": normalized,
        "source_label": INSTITUTIONAL_SOURCE_LABEL,
        "availability": availability,
        "summary": summaries.get(normalized, unavailable_institutional_summary(normalized)),
        "items": [institutional_activity_event_payload(row) for row in events],
        "tooltip": INSTITUTIONAL_ACTIVITY_TOOLTIP,
    }


def institutional_summary_payload(
    summary: InstitutionalSymbolSummary,
    recent_events: list[InstitutionalActivityEvent],
    *,
    lookback_days: int,
) -> dict[str, Any]:
    latest_filing_date = summary.latest_filing_date
    freshness_days = (datetime.now(timezone.utc).date() - latest_filing_date).days if latest_filing_date else None
    active_events = [event for event in recent_events if event.materiality_score >= 50]
    active = bool(active_events)
    freshness_status = "active" if active else "stale" if freshness_days is not None and freshness_days > 90 else "quiet"
    net_30d = sum(float(event.value_delta_usd or 0.0) for event in active_events)
    new_30d = sum(1 for event in active_events if event.event_type == "new_institutional_position")
    exits_30d = sum(1 for event in active_events if event.event_type == "major_holder_exit")
    increased_30d = sum(1 for event in active_events if event.direction == "bullish")
    reduced_30d = sum(1 for event in active_events if event.direction == "bearish")
    contribution = _summary_confirmation_contribution(summary, active_events)
    return {
        "available": True,
        "locked": False,
        "active": active,
        "source_label": INSTITUTIONAL_SOURCE_LABEL,
        "latest_report_year": summary.report_year,
        "latest_report_quarter": summary.report_quarter,
        "latest_filing_date": latest_filing_date.isoformat() if latest_filing_date else None,
        "freshness_days": freshness_days,
        "freshness_status": freshness_status,
        "direction": summary.direction,
        "total_holders": summary.total_holders,
        "holders_increased": summary.holders_increased,
        "holders_reduced": summary.holders_reduced,
        "new_positions": summary.new_positions,
        "exits": summary.exits,
        "net_value_delta_usd": summary.net_value_delta_usd,
        "institutional_ownership_pct": summary.institutional_ownership_pct,
        "accumulation_score": summary.accumulation_score,
        "distribution_score": summary.distribution_score,
        "materiality_score": summary.materiality_score,
        "confirmation_contribution": contribution,
        "top_accumulators": _loads_list(summary.top_accumulators_json),
        "top_reducers": _loads_list(summary.top_reducers_json),
        "user_copy_note": INSTITUTIONAL_ACTIVITY_TOOLTIP,
        "status": "ok",
        "net_activity": net_30d if active else summary.net_value_delta_usd,
        "institution_count": summary.total_holders,
        "total_value": summary.total_value_usd,
        "latest_activity_date": latest_filing_date.isoformat() if latest_filing_date else None,
        "score_contribution": abs(contribution),
        "institutional_net_reported_30d": {
            "label": "Institutional net reported 30D",
            "net_reported_value_change_30d": round(net_30d, 2),
            "net_reported_shares_change_30d": None,
            "holders_increased_30d": increased_30d,
            "holders_reduced_30d": reduced_30d,
            "new_positions_30d": new_30d,
            "exits_30d": exits_30d,
            "top_accumulators_30d": [
                institutional_activity_event_payload(item)
                for item in sorted(
                    active_events,
                    key=lambda row: abs(row.value_delta_usd or row.reported_value_usd or 0),
                    reverse=True,
                )[:5]
                if item.direction == "bullish"
            ],
            "top_reducers_30d": [
                institutional_activity_event_payload(item)
                for item in sorted(
                    active_events,
                    key=lambda row: abs(row.value_delta_usd or row.reported_value_usd or 0),
                    reverse=True,
                )[:5]
                if item.direction == "bearish"
            ],
            "tooltip": INSTITUTIONAL_NET_REPORTED_30D_TOOLTIP,
        },
        "latest_quarter_snapshot": {
            "latest_report_year": summary.report_year,
            "latest_report_quarter": summary.report_quarter,
            "total_holders": summary.total_holders,
            "institutional_ownership_pct": summary.institutional_ownership_pct,
            "total_value_usd": summary.total_value_usd,
            "latest_top_holders": _loads_list(summary.top_accumulators_json)[:10],
        },
    }


def unavailable_institutional_summary(symbol: str | None = None, *, status: str = "unavailable") -> dict[str, Any]:
    return {
        "available": False,
        "locked": False,
        "active": False,
        "source_label": INSTITUTIONAL_SOURCE_LABEL,
        "latest_report_year": None,
        "latest_report_quarter": None,
        "latest_filing_date": None,
        "freshness_days": None,
        "freshness_status": status,
        "direction": "neutral",
        "total_holders": None,
        "holders_increased": None,
        "holders_reduced": None,
        "new_positions": None,
        "exits": None,
        "net_value_delta_usd": None,
        "institutional_ownership_pct": None,
        "accumulation_score": None,
        "distribution_score": None,
        "materiality_score": None,
        "confirmation_contribution": 0,
        "top_accumulators": [],
        "top_reducers": [],
        "user_copy_note": INSTITUTIONAL_ACTIVITY_TOOLTIP,
        "status": status,
        "symbol": normalize_symbol(symbol),
        "net_activity": None,
        "institution_count": None,
        "total_value": None,
        "latest_activity_date": None,
    }


def institutional_activity_event_payload(row: InstitutionalActivityEvent) -> dict[str, Any]:
    return {
        "id": row.id,
        "symbol": row.symbol,
        "cik": row.cik,
        "holder_name": row.holder_name,
        "event_type": row.event_type,
        "direction": row.direction,
        "title": row.title,
        "summary": row.summary,
        "filing_date": row.filing_date.isoformat() if row.filing_date else None,
        "report_year": row.report_year,
        "report_quarter": row.report_quarter,
        "reported_value_usd": row.reported_value_usd,
        "value_delta_usd": row.value_delta_usd,
        "ownership_pct": row.ownership_pct,
        "holder_breadth": row.holder_breadth,
        "materiality_score": row.materiality_score,
        "confirmation_score": row.confirmation_score,
        "freshness_status": row.freshness_status,
        "source_label": row.source_label,
        "metadata": _loads_dict(row.metadata_json),
    }


def list_institutional_holders(
    db: Session,
    *,
    q: str | None = None,
    sort: str = "latest_filing_date",
    direction: str = "desc",
    page: int = 0,
    limit: int = 50,
) -> dict[str, Any]:
    bounded_limit = max(1, min(int(limit or 50), 100))
    offset = max(0, int(page or 0)) * bounded_limit
    query = select(InstitutionalHolder)
    if q and q.strip():
        needle = f"%{q.strip().casefold()}%"
        query = query.where(
            or_(
                func.lower(InstitutionalHolder.holder_name).like(needle),
                func.lower(InstitutionalHolder.cik).like(needle),
            )
        )
    sort_column = InstitutionalHolder.latest_filing_date if sort == "latest_filing_date" else InstitutionalHolder.holder_name
    if direction == "asc":
        query = query.order_by(sort_column.asc())
    else:
        query = query.order_by(sort_column.desc().nullslast())
    rows = db.execute(query.offset(offset).limit(bounded_limit)).scalars().all()
    return {
        "items": [holder_payload(row) for row in rows],
        "page": max(0, int(page or 0)),
        "limit": bounded_limit,
        "has_next": len(rows) == bounded_limit,
    }


def holder_payload(holder: InstitutionalHolder) -> dict[str, Any]:
    return {
        "cik": holder.cik,
        "holder_name": holder.holder_name,
        "holder_type": holder.holder_type,
        "is_passive_like": holder.is_passive_like,
        "quality_score": holder.quality_score,
        "latest_filing_date": holder.latest_filing_date.isoformat() if holder.latest_filing_date else None,
        "latest_report_year": holder.latest_report_year,
        "latest_report_quarter": holder.latest_report_quarter,
    }


def holder_profile(db: Session, cik: str) -> dict[str, Any] | None:
    normalized = normalize_cik(cik)
    if not normalized:
        return None
    holder = db.get(InstitutionalHolder, normalized)
    if holder is None:
        return None
    latest_positions = []
    if holder.latest_report_year and holder.latest_report_quarter:
        latest_positions = db.execute(
            select(InstitutionalPosition)
            .where(
                InstitutionalPosition.cik == normalized,
                InstitutionalPosition.report_year == holder.latest_report_year,
                InstitutionalPosition.report_quarter == holder.latest_report_quarter,
            )
            .order_by(InstitutionalPosition.value_usd.desc().nullslast())
            .limit(25)
        ).scalars().all()
    return {
        **holder_payload(holder),
        "total_reported_value": round(sum(float(position.value_usd or 0.0) for position in latest_positions), 2),
        "holdings_count": len(latest_positions),
        "top_holdings": [position_payload(position) for position in latest_positions[:10]],
    }


def position_payload(position: InstitutionalPosition) -> dict[str, Any]:
    return {
        "id": position.id,
        "cik": position.cik,
        "symbol": position.normalized_symbol or position.symbol,
        "cusip": position.cusip,
        "issuer_name": position.issuer_name,
        "shares": position.shares,
        "value_usd": position.value_usd,
        "portfolio_weight": position.portfolio_weight,
        "ownership_pct": position.ownership_pct,
        "report_year": position.report_year,
        "report_quarter": position.report_quarter,
        "filing_date": position.filing_date.isoformat() if position.filing_date else None,
    }


def positions_for_holder(db: Session, cik: str, *, year: int | None = None, quarter: int | None = None, page: int = 0, limit: int = 50) -> dict[str, Any]:
    normalized = normalize_cik(cik)
    if not normalized:
        return {"items": [], "page": page, "limit": limit, "has_next": False}
    bounded_limit = max(1, min(int(limit or 50), 200))
    query = select(InstitutionalPosition).where(InstitutionalPosition.cik == normalized)
    if year is not None:
        query = query.where(InstitutionalPosition.report_year == int(year))
    if quarter is not None:
        query = query.where(InstitutionalPosition.report_quarter == int(quarter))
    rows = db.execute(
        query.order_by(InstitutionalPosition.report_year.desc(), InstitutionalPosition.report_quarter.desc(), InstitutionalPosition.value_usd.desc().nullslast())
        .offset(max(0, int(page or 0)) * bounded_limit)
        .limit(bounded_limit)
    ).scalars().all()
    return {"items": [position_payload(row) for row in rows], "page": page, "limit": bounded_limit, "has_next": len(rows) == bounded_limit}


def filings_for_holder(db: Session, cik: str, *, page: int = 0, limit: int = 50) -> dict[str, Any]:
    normalized = normalize_cik(cik)
    if not normalized:
        return {"items": [], "page": page, "limit": limit, "has_next": False}
    bounded_limit = max(1, min(int(limit or 50), 200))
    rows = db.execute(
        select(InstitutionalFiling)
        .where(InstitutionalFiling.cik == normalized)
        .order_by(InstitutionalFiling.filing_date.desc(), InstitutionalFiling.id.desc())
        .offset(max(0, int(page or 0)) * bounded_limit)
        .limit(bounded_limit)
    ).scalars().all()
    return {
        "items": [
            {
                "id": row.id,
                "cik": row.cik,
                "accession_number": row.accession_number,
                "filing_date": row.filing_date.isoformat() if row.filing_date else None,
                "report_year": row.report_year,
                "report_quarter": row.report_quarter,
                "report_period_end": row.report_period_end.isoformat() if row.report_period_end else None,
                "filing_url": row.filing_url,
                "form_type": row.form_type,
                "is_amendment": row.is_amendment,
                "processed_at": row.processed_at.isoformat() if row.processed_at else None,
            }
            for row in rows
        ],
        "page": page,
        "limit": bounded_limit,
        "has_next": len(rows) == bounded_limit,
    }


def industry_summary_payload(db: Session, *, year: int | None = None, quarter: int | None = None, limit: int = 50) -> dict[str, Any]:
    query = select(InstitutionalIndustrySummary)
    if year is not None:
        query = query.where(InstitutionalIndustrySummary.report_year == int(year))
    if quarter is not None:
        query = query.where(InstitutionalIndustrySummary.report_quarter == int(quarter))
    rows = db.execute(
        query.order_by(InstitutionalIndustrySummary.generated_at.desc(), InstitutionalIndustrySummary.net_value_delta_usd.desc().nullslast())
        .limit(max(1, min(int(limit or 50), 100)))
    ).scalars().all()
    return {
        "items": [
            {
                "industry": row.industry,
                "sector": row.sector,
                "report_year": row.report_year,
                "report_quarter": row.report_quarter,
                "total_value_usd": row.total_value_usd,
                "net_value_delta_usd": row.net_value_delta_usd,
                "accumulation_score": row.accumulation_score,
                "distribution_score": row.distribution_score,
                "direction": row.direction,
                "generated_at": row.generated_at.isoformat() if row.generated_at else None,
            }
            for row in rows
        ]
    }


def upsert_holder_performance_rows(db: Session, cik: str, rows: list[dict[str, Any]]) -> dict[str, int]:
    normalized = normalize_cik(cik)
    if not normalized or not rows:
        return {"updated": 0}
    holder = db.get(InstitutionalHolder, normalized)
    if holder is None:
        holder = InstitutionalHolder(cik=normalized)
        db.add(holder)
    score_candidates = [_first_number(row, "score", "performanceScore", "averageReturn", "alpha") for row in rows]
    score_values = [value for value in score_candidates if value is not None]
    if score_values:
        holder.quality_score = round(max(0.0, min(100.0, sum(score_values) / len(score_values))), 2)
        holder.updated_at = datetime.now(timezone.utc)
        return {"updated": 1}
    return {"updated": 0}


def upsert_holder_industry_breakdown_rows(db: Session, cik: str, year: int, quarter: int, rows: list[dict[str, Any]]) -> dict[str, int]:
    normalized = normalize_cik(cik)
    if not normalized:
        return {"updated": 0, "skipped": len(rows)}
    updated = skipped = 0
    for row in rows:
        industry = _first_text(row, "industry", "industryTitle", "name")
        if not industry:
            skipped += 1
            continue
        sector = _first_text(row, "sector")
        existing = db.execute(
            select(InstitutionalHolderIndustryBreakdown).where(
                InstitutionalHolderIndustryBreakdown.cik == normalized,
                InstitutionalHolderIndustryBreakdown.report_year == int(year),
                InstitutionalHolderIndustryBreakdown.report_quarter == int(quarter),
                InstitutionalHolderIndustryBreakdown.industry == industry,
                _nullable_equals(InstitutionalHolderIndustryBreakdown.sector, sector),
            )
        ).scalar_one_or_none()
        if existing is None:
            existing = InstitutionalHolderIndustryBreakdown(
                cik=normalized,
                report_year=int(year),
                report_quarter=int(quarter),
                industry=industry,
                sector=sector,
            )
            db.add(existing)
        existing.value_usd = _first_number(row, "valueUsd", "value", "marketValue")
        existing.weight_pct = _first_number(row, "weightPct", "weight", "portfolioWeight")
        existing.generated_at = datetime.now(timezone.utc)
        updated += 1
    return {"updated": updated, "skipped": skipped}


def upsert_industry_summary_rows(db: Session, year: int, quarter: int, rows: list[dict[str, Any]]) -> dict[str, int]:
    updated = skipped = 0
    for row in rows:
        industry = _first_text(row, "industry", "industryTitle", "name")
        if not industry:
            skipped += 1
            continue
        sector = _first_text(row, "sector")
        existing = db.execute(
            select(InstitutionalIndustrySummary).where(
                InstitutionalIndustrySummary.industry == industry,
                _nullable_equals(InstitutionalIndustrySummary.sector, sector),
                InstitutionalIndustrySummary.report_year == int(year),
                InstitutionalIndustrySummary.report_quarter == int(quarter),
            )
        ).scalar_one_or_none()
        if existing is None:
            existing = InstitutionalIndustrySummary(
                industry=industry,
                sector=sector,
                report_year=int(year),
                report_quarter=int(quarter),
            )
            db.add(existing)
        existing.total_value_usd = _first_number(row, "totalValueUsd", "totalValue", "value")
        existing.net_value_delta_usd = _first_number(row, "netValueDeltaUsd", "netValueDelta", "change")
        existing.accumulation_score = _first_number(row, "accumulationScore") or 0.0
        existing.distribution_score = _first_number(row, "distributionScore") or 0.0
        existing.direction = _direction_from_scores(existing.accumulation_score, existing.distribution_score)
        existing.generated_at = datetime.now(timezone.utc)
        updated += 1
    return {"updated": updated, "skipped": skipped}


def _availability(*, status: str, enabled: bool) -> dict[str, Any]:
    return {"enabled": enabled, "status": status, "filterable": enabled and status == "ok"}


def _summary_confirmation_contribution(summary: InstitutionalSymbolSummary, active_events: list[InstitutionalActivityEvent]) -> float:
    if active_events:
        contributions = [
            institutional_confirmation_contribution(
                filing_date=event.filing_date,
                materiality_score=event.materiality_score,
                direction=event.direction,
            )
            for event in active_events
        ]
        return round(_clamp_float(sum(contributions), -15.0, 15.0), 2)
    return institutional_confirmation_contribution(
        filing_date=summary.latest_filing_date,
        materiality_score=summary.materiality_score,
        direction=summary.direction,
    )


def _latest_symbol_summaries(db: Session, symbols: list[str]) -> dict[str, InstitutionalSymbolSummary]:
    rows = db.execute(
        select(InstitutionalSymbolSummary)
        .where(InstitutionalSymbolSummary.normalized_symbol.in_(symbols))
        .order_by(
            InstitutionalSymbolSummary.normalized_symbol.asc(),
            InstitutionalSymbolSummary.report_year.desc(),
            InstitutionalSymbolSummary.report_quarter.desc(),
            InstitutionalSymbolSummary.latest_filing_date.desc().nullslast(),
        )
    ).scalars().all()
    result: dict[str, InstitutionalSymbolSummary] = {}
    for row in rows:
        result.setdefault(row.normalized_symbol, row)
    return result


def _recent_activity_events_by_symbol(db: Session, symbols: list[str], *, lookback_days: int) -> dict[str, list[InstitutionalActivityEvent]]:
    since = datetime.now(timezone.utc).date() - timedelta(days=max(1, min(int(lookback_days or 30), 365)))
    rows = db.execute(
        select(InstitutionalActivityEvent)
        .where(InstitutionalActivityEvent.normalized_symbol.in_(symbols))
        .where(InstitutionalActivityEvent.filing_date >= since)
        .order_by(InstitutionalActivityEvent.filing_date.desc(), InstitutionalActivityEvent.materiality_score.desc())
    ).scalars().all()
    grouped: dict[str, list[InstitutionalActivityEvent]] = {symbol: [] for symbol in symbols}
    for row in rows:
        grouped.setdefault(row.normalized_symbol, []).append(row)
    return grouped


def _upsert_activity_event_from_change(db: Session, change: InstitutionalPositionChange, *, event_type: str) -> bool:
    title, summary = _copy_for_change(change)
    existing = db.execute(
        select(InstitutionalActivityEvent).where(
            InstitutionalActivityEvent.normalized_symbol == change.normalized_symbol,
            InstitutionalActivityEvent.cik == change.cik,
            InstitutionalActivityEvent.event_type == event_type,
            InstitutionalActivityEvent.report_year == change.report_year,
            InstitutionalActivityEvent.report_quarter == change.report_quarter,
        )
    ).scalar_one_or_none()
    created = existing is None
    if existing is None:
        existing = InstitutionalActivityEvent(
            symbol=change.normalized_symbol or change.symbol or "",
            normalized_symbol=change.normalized_symbol or change.symbol or "",
            cik=change.cik,
            event_type=event_type,
            filing_date=change.filing_date,
            report_year=change.report_year,
            report_quarter=change.report_quarter,
            title=title,
            summary=summary,
        )
        db.add(existing)
    existing.holder_name = change.holder_name
    existing.direction = change.direction
    existing.reported_value_usd = change.curr_value_usd if change.change_type != "exit" else change.prev_value_usd
    existing.value_delta_usd = change.value_delta_usd
    existing.ownership_pct = change.curr_ownership_pct
    existing.holder_breadth = 1
    existing.materiality_score = change.materiality_score
    existing.confirmation_score = abs(institutional_confirmation_contribution(
        filing_date=change.filing_date,
        materiality_score=change.materiality_score,
        direction=change.direction,
        holder_quality_weight=change.holder_quality_weight,
        passive_adjustment=(change.passive_adjusted_score / change.materiality_score / change.holder_quality_weight) if change.materiality_score and change.holder_quality_weight else 1.0,
    ))
    existing.freshness_status = "active" if recency_decay_30d(change.filing_date) > 0 else "stale"
    existing.title = title
    existing.summary = summary
    existing.metadata_json = json.dumps(
        {
            "change_type": change.change_type,
            "shares_delta_pct": change.shares_delta_pct,
            "value_delta_pct": change.value_delta_pct,
            "source_label": INSTITUTIONAL_SOURCE_LABEL,
        },
        sort_keys=True,
        default=str,
    )
    existing.updated_at = datetime.now(timezone.utc)
    return created


def _upsert_activity_event_from_summary(db: Session, summary: InstitutionalSymbolSummary, *, event_type: str) -> bool:
    existing = db.execute(
        select(InstitutionalActivityEvent).where(
            InstitutionalActivityEvent.normalized_symbol == summary.normalized_symbol,
            InstitutionalActivityEvent.cik.is_(None),
            InstitutionalActivityEvent.event_type == event_type,
            InstitutionalActivityEvent.report_year == summary.report_year,
            InstitutionalActivityEvent.report_quarter == summary.report_quarter,
        )
    ).scalar_one_or_none()
    created = existing is None
    title = f"Institutions report net {'accumulation' if summary.direction == 'bullish' else 'distribution' if summary.direction == 'bearish' else 'activity'} in {summary.normalized_symbol}"
    summary_text = (
        f"Recent 13F filings show {summary.holders_increased + summary.new_positions} holders increased or opened positions "
        f"and {summary.holders_reduced + summary.exits} reduced or exited {summary.normalized_symbol}, with net reported value change "
        f"of approximately {_money(summary.net_value_delta_usd)}. Filing date: {summary.latest_filing_date.isoformat() if summary.latest_filing_date else 'unavailable'}. "
        f"Report period: Q{summary.report_quarter} {summary.report_year}."
    )
    if existing is None:
        existing = InstitutionalActivityEvent(
            symbol=summary.normalized_symbol,
            normalized_symbol=summary.normalized_symbol,
            event_type=event_type,
            filing_date=summary.latest_filing_date or datetime.now(timezone.utc).date(),
            report_year=summary.report_year,
            report_quarter=summary.report_quarter,
            title=title,
            summary=summary_text,
        )
        db.add(existing)
    existing.direction = summary.direction
    existing.reported_value_usd = summary.total_value_usd
    existing.value_delta_usd = summary.net_value_delta_usd
    existing.ownership_pct = summary.institutional_ownership_pct
    existing.holder_breadth = (summary.holders_increased + summary.new_positions) - (summary.holders_reduced + summary.exits)
    existing.materiality_score = summary.materiality_score
    existing.confirmation_score = abs(_summary_confirmation_contribution(summary, []))
    existing.freshness_status = "active" if summary.latest_filing_date and recency_decay_30d(summary.latest_filing_date) > 0 else "stale"
    existing.title = title
    existing.summary = summary_text
    existing.metadata_json = json.dumps(
        {
            "total_holders": summary.total_holders,
            "holders_increased": summary.holders_increased,
            "holders_reduced": summary.holders_reduced,
            "new_positions": summary.new_positions,
            "exits": summary.exits,
        },
        sort_keys=True,
        default=str,
    )
    existing.updated_at = datetime.now(timezone.utc)
    return created


def _upsert_feed_event(db: Session, activity: InstitutionalActivityEvent) -> bool:
    source_filing_id = f"institutional:{activity.id}:{activity.event_type}:{activity.report_year}q{activity.report_quarter}"
    existing = db.execute(
        select(Event).where(
            Event.source_provider == INSTITUTIONAL_EVENT_SOURCE,
            Event.source_filing_id == source_filing_id,
        )
    ).scalar_one_or_none()
    created = existing is None
    event_ts = datetime.combine(activity.filing_date, datetime.min.time(), tzinfo=timezone.utc)
    payload = institutional_activity_event_payload(activity)
    payload["data_semantics"] = "institutional_13f_reported_holdings"
    payload["timing_note"] = "13F filings disclose quarter-end holdings and may not reflect real-time trading."
    if existing is None:
        existing = Event(
            event_type=activity.event_type,
            ts=event_ts,
            event_date=event_ts,
            symbol=activity.normalized_symbol,
            source="13F filing",
            impact_score=float(activity.materiality_score or 0.0),
            payload_json=json.dumps(payload, sort_keys=True, default=str),
        )
        db.add(existing)
    existing.member_name = activity.holder_name
    existing.member_bioguide_id = activity.cik
    existing.trade_type = _action_label_for_event(activity.event_type)
    existing.transaction_type = "13F filing"
    existing.amount_min = _event_amount(activity)
    existing.amount_max = _event_amount(activity)
    existing.source_provider = INSTITUTIONAL_EVENT_SOURCE
    existing.source_filing_id = source_filing_id
    existing.source_document_url = None
    existing.data_source = INSTITUTIONAL_SOURCE_LABEL
    existing.payload_json = json.dumps(payload, sort_keys=True, default=str)
    return created


def _find_position(db: Session, filing_id: int | None, symbol: str | None, cusip: str | None, put_call: str | None) -> InstitutionalPosition | None:
    query = select(InstitutionalPosition).where(InstitutionalPosition.filing_id == filing_id)
    if cusip:
        query = query.where(InstitutionalPosition.cusip == cusip)
    else:
        query = query.where(InstitutionalPosition.normalized_symbol == symbol)
    query = query.where(_nullable_equals(InstitutionalPosition.put_call, put_call))
    return db.execute(query).scalar_one_or_none()


def _prior_positions_for_filing(db: Session, filing: InstitutionalFiling) -> list[InstitutionalPosition]:
    prior_year, prior_quarter = _previous_quarter(filing.report_year, filing.report_quarter)
    return db.execute(
        select(InstitutionalPosition).where(
            InstitutionalPosition.cik == filing.cik,
            InstitutionalPosition.report_year == prior_year,
            InstitutionalPosition.report_quarter == prior_quarter,
        )
    ).scalars().all()


def _position_match_key(position: InstitutionalPosition) -> tuple[str, str | None]:
    if position.cusip:
        return ("cusip", position.cusip)
    return ("symbol", normalize_symbol(position.normalized_symbol or position.symbol) or "")


def _previous_quarter(year: int, quarter: int) -> tuple[int, int]:
    if quarter <= 1:
        return int(year) - 1, 4
    return int(year), int(quarter) - 1


def _holder_quality_weight(holder: InstitutionalHolder | None) -> float:
    if holder is None:
        return 1.0
    quality_score = holder.quality_score
    base = 1.0
    if quality_score is not None:
        base = 0.8 + _clamp_float(quality_score, 0.0, 100.0) / 100.0 * 0.6
    if holder.is_passive_like:
        base *= 0.65
    return _clamp_float(base, 0.25, 1.6)


def _change_type(prev_shares: float | None, curr_shares: float | None, prev_value: float | None, curr_value: float | None) -> str:
    prev_present = _positiveish(prev_shares) or _positiveish(prev_value)
    curr_present = _positiveish(curr_shares) or _positiveish(curr_value)
    if not prev_present and curr_present:
        return "new_position"
    if prev_present and not curr_present:
        return "exit"
    delta = _delta(curr_shares, prev_shares)
    if delta is None:
        delta = _delta(curr_value, prev_value)
    if delta is None or abs(delta) < 1e-9:
        return "unchanged"
    return "increase" if delta > 0 else "decrease"


def _direction_for_change(change_type: str) -> str:
    if change_type in {"new_position", "increase"}:
        return "bullish"
    if change_type in {"decrease", "exit"}:
        return "bearish"
    return "neutral"


def _event_type_for_change(change: InstitutionalPositionChange) -> str:
    if change.change_type == "new_position":
        return "new_institutional_position"
    if change.change_type == "exit":
        return "major_holder_exit"
    if change.change_type == "decrease":
        return "major_holder_reduction"
    if change.change_type == "increase":
        return "institutional_accumulation"
    return "smart_money_confirmation"


def _copy_for_change(change: InstitutionalPositionChange) -> tuple[str, str]:
    holder = change.holder_name or "Institution"
    symbol = change.normalized_symbol or change.symbol or "ticker"
    report = f"Q{change.report_quarter} {change.report_year}"
    filing = change.filing_date.isoformat() if change.filing_date else "unavailable"
    if change.change_type == "new_position":
        title = f"{holder} reports new {symbol} position"
        action = f"disclosed a new {symbol} position"
    elif change.change_type == "exit":
        title = f"{holder} reports {symbol} exit"
        action = f"reported exiting its {symbol} position"
    elif change.change_type == "decrease":
        title = f"{holder} reports reduced {symbol} position"
        action = f"reported a reduction in its {symbol} position"
    else:
        title = f"{holder} reports increased {symbol} position"
        action = f"reported an increase in its {symbol} position"
    value = change.curr_value_usd if change.change_type != "exit" else change.prev_value_usd
    summary = (
        f"{holder} {action} worth approximately {_money(value)} in its latest 13F filing. "
        f"Filing date: {filing}. Report period: {report}."
    )
    return title, summary


def _action_label_for_event(event_type: str) -> str:
    return {
        "new_institutional_position": "New Position",
        "major_holder_exit": "Exit",
        "major_holder_reduction": "Reduced",
        "institutional_distribution": "Reduced",
        "cluster_distribution": "Distribution",
        "institutional_accumulation": "Increased",
        "cluster_accumulation": "Accumulation",
    }.get(event_type, "Institutional Activity")


def _event_amount(activity: InstitutionalActivityEvent) -> int | None:
    value = activity.reported_value_usd if activity.reported_value_usd is not None else activity.value_delta_usd
    if value is None:
        return None
    return int(round(abs(float(value))))


def _top_change_rows(changes: list[InstitutionalPositionChange], *, direction: str) -> list[dict[str, Any]]:
    rows = [
        change
        for change in changes
        if change.direction == direction and change.change_type != "unchanged"
    ]
    rows.sort(key=lambda change: abs(change.value_delta_usd or change.curr_value_usd or change.prev_value_usd or 0.0), reverse=True)
    return [
        {
            "cik": change.cik,
            "holder_name": change.holder_name,
            "symbol": change.normalized_symbol,
            "change_type": change.change_type,
            "reported_value_usd": change.curr_value_usd if change.change_type != "exit" else change.prev_value_usd,
            "value_delta_usd": change.value_delta_usd,
            "value_delta_pct": change.value_delta_pct,
            "materiality_score": change.materiality_score,
            "filing_date": change.filing_date.isoformat() if change.filing_date else None,
        }
        for change in rows[:10]
    ]


def _put_call_ratio(positions: list[InstitutionalPosition]) -> float | None:
    put_value = sum(float(position.value_usd or 0.0) for position in positions if (position.put_call or "").lower() == "put")
    call_value = sum(float(position.value_usd or 0.0) for position in positions if (position.put_call or "").lower() == "call")
    if call_value <= 0:
        return None
    return round(put_value / call_value, 4)


def _direction_from_scores(accumulation: float | None, distribution: float | None) -> str:
    a = float(accumulation or 0.0)
    d = float(distribution or 0.0)
    if a > d * 1.2:
        return "bullish"
    if d > a * 1.2:
        return "bearish"
    if a > 0 and d > 0:
        return "mixed"
    return "neutral"


def _nullable_equals(column: Any, value: Any):
    return column.is_(None) if value is None else column == value


def _first_value(row: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in row and row.get(key) not in (None, ""):
            return row.get(key)
    lower_map = {str(key).lower(): value for key, value in row.items()}
    for key in keys:
        value = lower_map.get(key.lower())
        if value not in (None, ""):
            return value
    return None


def _first_text(row: dict[str, Any], *keys: str) -> str | None:
    value = _first_value(row, *keys)
    if value is None:
        return None
    cleaned = str(value).strip()
    return cleaned or None


def _first_number(row: dict[str, Any], *keys: str) -> float | None:
    return _parse_number(_first_value(row, *keys))


def _first_int(row: dict[str, Any], *keys: str) -> int | None:
    value = _first_number(row, *keys)
    return int(value) if value is not None else None


def _parse_number(value: Any) -> float | None:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, (int, float)):
        parsed = float(value)
        return parsed if math.isfinite(parsed) else None
    if isinstance(value, str):
        cleaned = value.replace("$", "").replace(",", "").replace("%", "").strip()
        if not cleaned:
            return None
        try:
            parsed = float(cleaned)
        except ValueError:
            return None
        return parsed if math.isfinite(parsed) else None
    return None


def _parse_date(value: Any) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if not isinstance(value, str):
        return None
    cleaned = value.strip()
    if not cleaned:
        return None
    try:
        return datetime.fromisoformat(cleaned.replace("Z", "+00:00")).date()
    except ValueError:
        pass
    try:
        return datetime.strptime(cleaned[:10], "%Y-%m-%d").date()
    except ValueError:
        return None


def _quarter_for_date(value: date) -> int:
    return ((value.month - 1) // 3) + 1


def _boolish(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "amendment"}
    return False


def _clean_option_side(value: str | None) -> str | None:
    normalized = (value or "").strip().lower()
    if normalized in {"put", "call"}:
        return normalized
    return None


def _delta(current: float | None, previous: float | None) -> float | None:
    if current is None and previous is None:
        return None
    return float(current or 0.0) - float(previous or 0.0)


def _pct_delta(current: float | None, previous: float | None) -> float | None:
    if previous is None or abs(float(previous)) < 1e-9:
        return None
    return ((float(current or 0.0) - float(previous)) / abs(float(previous))) * 100.0


def _positiveish(value: float | None) -> bool:
    return value is not None and abs(float(value)) > 1e-9


def _clamp_float(value: float | int | None, minimum: float, maximum: float) -> float:
    try:
        parsed = float(value if value is not None else 0.0)
    except (TypeError, ValueError):
        parsed = 0.0
    if not math.isfinite(parsed):
        parsed = 0.0
    return max(minimum, min(maximum, parsed))


def _loads_list(value: str | None) -> list[dict[str, Any]]:
    if not value:
        return []
    try:
        parsed = json.loads(value)
    except Exception:
        return []
    if isinstance(parsed, list):
        return [item for item in parsed if isinstance(item, dict)]
    return []


def _loads_dict(value: str | None) -> dict[str, Any]:
    if not value:
        return {}
    try:
        parsed = json.loads(value)
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _money(value: float | int | None) -> str:
    if value is None:
        return "unavailable"
    amount = abs(float(value))
    prefix = "-" if float(value) < 0 else ""
    if amount >= 1_000_000_000:
        return f"{prefix}${amount / 1_000_000_000:.1f}B"
    if amount >= 1_000_000:
        return f"{prefix}${amount / 1_000_000:.1f}M"
    if amount >= 1_000:
        return f"{prefix}${amount / 1_000:.1f}K"
    return f"{prefix}${amount:,.0f}"
