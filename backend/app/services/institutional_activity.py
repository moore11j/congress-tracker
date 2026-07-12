from __future__ import annotations

import json
import math
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any

from sqlalchemy import and_, func, inspect, or_, select
from sqlalchemy.orm import Session

from app.clients.fmp import fetch_symbol_positions_summary
from app.models import (
    Event,
    CikMeta,
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
INSTITUTIONAL_FEED_EVENT_MIN_MATERIALITY = 80.0
INSTITUTIONAL_FEED_EVENT_TYPES = (
    "smart_money_confirmation",
    "cluster_accumulation",
    "cluster_distribution",
    "major_holder_exit",
    "major_holder_reduction",
    "new_institutional_position",
)
INSTITUTIONAL_ALL_FEED_MIN_MATERIALITY = 90.0
INSTITUTIONAL_ALL_FEED_LARGE_VALUE_USD = 100_000_000.0
INSTITUTIONAL_ALL_FEED_CLUSTER_MIN_MATERIALITY = 95.0
INSTITUTIONAL_ALL_FEED_CLUSTER_VALUE_USD = 500_000_000.0
INSTITUTIONAL_ALL_FEED_CLUSTER_BREADTH = 3
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
        _first_value(row, "reportPeriod", "report_period", "periodOfReport", "period_of_report", "reportDate", "report_date", "date")
    )
    report_year = _first_int(row, "reportYear", "report_year", "year")
    report_quarter = _first_int(row, "reportQuarter", "report_quarter", "quarter")
    if report_period_end and (report_year is None or report_quarter is None):
        report_year = report_year or report_period_end.year
        report_quarter = report_quarter or _quarter_for_date(report_period_end)
    if not cik or filing_date is None or report_year is None or report_quarter is None:
        return None
    form_type = _first_text(row, "formType", "form_type", "form", "type")
    accession_number = _first_text(row, "accessionNumber", "accession_number", "accessionNo") or _accession_from_filing_links(row)
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


def _accession_from_filing_links(row: dict[str, Any]) -> str | None:
    for value in (
        _first_text(row, "filingUrl", "filing_url", "url", "link", "finalLink"),
        _first_text(row, "finalLink"),
    ):
        if not value:
            continue
        dashed = re.search(r"\b(\d{10}-\d{2}-\d{6})\b", value)
        if dashed:
            return dashed.group(1)
        compact = re.search(r"/(\d{18})(?:/|$)", value)
        if compact:
            digits = compact.group(1)
            return f"{digits[:10]}-{digits[10:12]}-{digits[12:]}"
    return None


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
        put_call=_clean_option_side(_first_text(row, "putCall", "put_call", "optionType", "putCallShare")),
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


def _fallback_period_filing(db: Session, candidate: InstitutionalFilingCandidate) -> InstitutionalFiling | None:
    period_rows = db.execute(
        select(InstitutionalFiling)
        .where(
            InstitutionalFiling.cik == candidate.cik,
            InstitutionalFiling.report_year == candidate.report_year,
            InstitutionalFiling.report_quarter == candidate.report_quarter,
        )
        .order_by(InstitutionalFiling.id)
    ).scalars().all()
    if len(period_rows) == 1:
        return period_rows[0]

    rich_rows = [row for row in period_rows if row.accession_number or row.form_type or row.filing_url]
    if len(rich_rows) == 1:
        return rich_rows[0]
    return _choose_canonical_filing(rich_rows or period_rows)


def _filing_is_amendment(filing: InstitutionalFiling) -> bool:
    form_type = (filing.form_type or "").upper()
    return bool(filing.is_amendment or "/A" in form_type)


def _canonical_filing_sort_key(filing: InstitutionalFiling) -> tuple[int, date, str, int]:
    return (
        1 if _filing_is_amendment(filing) else 0,
        filing.filing_date or date.min,
        filing.accession_number or "",
        int(filing.id or 0),
    )


def _choose_canonical_filing(rows: list[InstitutionalFiling]) -> InstitutionalFiling | None:
    if not rows:
        return None
    return max(rows, key=_canonical_filing_sort_key)


def get_canonical_filing_for_holder_period(
    db: Session,
    cik: str | None,
    report_year: int | None,
    report_quarter: int | None,
) -> InstitutionalFiling | None:
    normalized = normalize_cik(cik)
    if not normalized or report_year is None or report_quarter is None:
        return None
    rows = db.execute(
        select(InstitutionalFiling).where(
            InstitutionalFiling.cik == normalized,
            InstitutionalFiling.report_year == int(report_year),
            InstitutionalFiling.report_quarter == int(report_quarter),
        )
    ).scalars().all()
    return _choose_canonical_filing(rows)


def apply_institutional_filing_supersession(db: Session, filing: InstitutionalFiling) -> InstitutionalFiling:
    if filing.id is None:
        db.flush()
    rows = db.execute(
        select(InstitutionalFiling).where(
            InstitutionalFiling.cik == filing.cik,
            InstitutionalFiling.report_year == filing.report_year,
            InstitutionalFiling.report_quarter == filing.report_quarter,
        )
    ).scalars().all()
    canonical = _choose_canonical_filing(rows) or filing
    if canonical.id is None:
        db.flush()
    for row in rows:
        desired = None if row.id == canonical.id else canonical.id
        if row.superseded_by != desired:
            row.superseded_by = desired
            row.updated_at = datetime.now(timezone.utc)
    return canonical


def is_canonical_institutional_filing(db: Session, filing: InstitutionalFiling) -> bool:
    if filing.superseded_by is not None:
        return False
    canonical = get_canonical_filing_for_holder_period(db, filing.cik, filing.report_year, filing.report_quarter)
    return bool(canonical and filing.id == canonical.id)


def institutional_filing_duplicate_report(db: Session) -> dict[str, int]:
    total_period_duplicates = db.execute(
        select(func.count()).select_from(
            select(
                InstitutionalFiling.cik,
                InstitutionalFiling.report_year,
                InstitutionalFiling.report_quarter,
            )
            .group_by(
                InstitutionalFiling.cik,
                InstitutionalFiling.report_year,
                InstitutionalFiling.report_quarter,
            )
            .having(func.count(InstitutionalFiling.id) > 1)
            .subquery()
        )
    ).scalar_one()
    active_period_duplicates = db.execute(
        select(func.count()).select_from(
            select(
                InstitutionalFiling.cik,
                InstitutionalFiling.report_year,
                InstitutionalFiling.report_quarter,
            )
            .where(InstitutionalFiling.superseded_by.is_(None))
            .group_by(
                InstitutionalFiling.cik,
                InstitutionalFiling.report_year,
                InstitutionalFiling.report_quarter,
            )
            .having(func.count(InstitutionalFiling.id) > 1)
            .subquery()
        )
    ).scalar_one()
    accession_duplicates = db.execute(
        select(func.count()).select_from(
            select(InstitutionalFiling.accession_number)
            .where(InstitutionalFiling.accession_number.is_not(None))
            .group_by(InstitutionalFiling.accession_number)
            .having(func.count(InstitutionalFiling.id) > 1)
            .subquery()
        )
    ).scalar_one()
    return {
        "accession_duplicates": int(accession_duplicates or 0),
        "total_period_duplicates": int(total_period_duplicates or 0),
        "active_period_duplicates": int(active_period_duplicates or 0),
    }


def _active_filing_ids_for_period(
    db: Session,
    *,
    report_year: int,
    report_quarter: int,
    cik: str | None = None,
) -> list[int]:
    query = select(InstitutionalFiling.id).where(
        InstitutionalFiling.report_year == int(report_year),
        InstitutionalFiling.report_quarter == int(report_quarter),
        InstitutionalFiling.superseded_by.is_(None),
    )
    normalized = normalize_cik(cik)
    if normalized:
        query = query.where(InstitutionalFiling.cik == normalized)
    return [int(row[0]) for row in db.execute(query).all() if row[0] is not None]


def upsert_institutional_filing(db: Session, candidate: InstitutionalFilingCandidate) -> tuple[InstitutionalFiling, bool]:
    filing = None
    if candidate.accession_number:
        filing = db.execute(
            select(InstitutionalFiling).where(InstitutionalFiling.accession_number == candidate.accession_number)
        ).scalar_one_or_none()
    if filing is None and not candidate.accession_number and not candidate.form_type:
        filing = _fallback_period_filing(db, candidate)
    if filing is None:
        conditions = [
            InstitutionalFiling.cik == candidate.cik,
            InstitutionalFiling.report_year == candidate.report_year,
            InstitutionalFiling.report_quarter == candidate.report_quarter,
            InstitutionalFiling.filing_date == candidate.filing_date,
        ]
        if candidate.form_type:
            conditions.append(InstitutionalFiling.form_type == candidate.form_type)
        filing = db.execute(select(InstitutionalFiling).where(*conditions)).scalar_one_or_none()
    created = filing is None
    if filing is None:
        filing = InstitutionalFiling(
            cik=candidate.cik,
            filing_date=candidate.filing_date,
            report_year=candidate.report_year,
            report_quarter=candidate.report_quarter,
        )
        db.add(filing)
    if candidate.accession_number:
        filing.accession_number = candidate.accession_number
    if candidate.report_period_end:
        filing.report_period_end = candidate.report_period_end
    if candidate.filing_url:
        filing.filing_url = candidate.filing_url
    if candidate.form_type:
        filing.form_type = candidate.form_type
        filing.is_amendment = candidate.is_amendment
    elif created:
        filing.is_amendment = candidate.is_amendment
    if created or candidate.accession_number or candidate.form_type or candidate.filing_url or not filing.raw_metadata_json:
        filing.raw_metadata_json = json.dumps(candidate.raw, sort_keys=True, default=str)
    filing.updated_at = datetime.now(timezone.utc)
    db.flush()
    apply_institutional_filing_supersession(db, filing)
    return filing, created


def upsert_positions_for_filing(
    db: Session,
    *,
    filing: InstitutionalFiling,
    rows: list[dict[str, Any]],
) -> dict[str, int]:
    inserted = updated = skipped = 0
    payloads_by_key: dict[tuple[str, str, str], InstitutionalPositionPayload] = {}
    fingerprints_by_key: dict[tuple[str, str, str], set[str]] = {}
    for row in rows:
        payload = parse_position(row)
        if payload is None:
            skipped += 1
            continue
        key = _position_payload_identity_key(payload)
        fingerprint = _position_payload_fingerprint(payload)
        fingerprints = fingerprints_by_key.setdefault(key, set())
        if fingerprint in fingerprints:
            continue
        fingerprints.add(fingerprint)
        existing_payload = payloads_by_key.get(key)
        payloads_by_key[key] = _merge_position_payloads(existing_payload, payload) if existing_payload else payload

    for payload in payloads_by_key.values():
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


def _activity_feed_source_prefix(activity_id: int) -> str:
    return f"institutional:{activity_id}:"


def _delete_feed_events_for_activity_ids(db: Session, activity_ids: list[int]) -> int:
    if not activity_ids:
        return 0
    rows = db.execute(
        select(Event).where(
            Event.source_provider == INSTITUTIONAL_EVENT_SOURCE,
            or_(*[Event.source_filing_id.like(f"{_activity_feed_source_prefix(activity_id)}%") for activity_id in activity_ids]),
        )
    ).scalars().all()
    for row in rows:
        db.delete(row)
    return len(rows)


def _holder_period_activity_rows(db: Session, filing: InstitutionalFiling) -> list[InstitutionalActivityEvent]:
    return db.execute(
        select(InstitutionalActivityEvent).where(
            InstitutionalActivityEvent.cik == filing.cik,
            InstitutionalActivityEvent.report_year == filing.report_year,
            InstitutionalActivityEvent.report_quarter == filing.report_quarter,
        )
    ).scalars().all()


def _suppress_holder_period_activity(db: Session, filing: InstitutionalFiling) -> int:
    rows = _holder_period_activity_rows(db, filing)
    _delete_feed_events_for_activity_ids(db, [int(row.id) for row in rows if row.id is not None])
    for row in rows:
        row.feed_visible = False
        row.freshness_status = "superseded"
        row.updated_at = datetime.now(timezone.utc)
    return len(rows)


def _reset_holder_period_changes_and_activity(db: Session, filing: InstitutionalFiling) -> set[str]:
    existing_changes = db.execute(
        select(InstitutionalPositionChange).where(
            InstitutionalPositionChange.cik == filing.cik,
            InstitutionalPositionChange.report_year == filing.report_year,
            InstitutionalPositionChange.report_quarter == filing.report_quarter,
        )
    ).scalars().all()
    symbols = {row.normalized_symbol for row in existing_changes if row.normalized_symbol}
    activities = _holder_period_activity_rows(db, filing)
    _delete_feed_events_for_activity_ids(db, [int(row.id) for row in activities if row.id is not None])
    for row in activities:
        if row.normalized_symbol:
            symbols.add(row.normalized_symbol)
        db.delete(row)
    for row in existing_changes:
        db.delete(row)
    db.flush()
    return symbols


def process_filing_changes_and_events(db: Session, filing: InstitutionalFiling) -> dict[str, int]:
    apply_institutional_filing_supersession(db, filing)
    if not is_canonical_institutional_filing(db, filing):
        _suppress_holder_period_activity(db, filing)
        filing.processed_at = datetime.now(timezone.utc)
        return {
            "changes": 0,
            "summaries": 0,
            "activity_events": 0,
            "feed_events": 0,
            "superseded_suppressed": 1,
        }

    db.flush()
    reset_symbols = _reset_holder_period_changes_and_activity(db, filing) if _filing_is_amendment(filing) else set()
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
    symbols: set[str] = set(reset_symbols)
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
    if symbols:
        db.flush()
    for symbol in sorted(symbols):
        summary = refresh_symbol_summary(db, symbol, filing.report_year, filing.report_quarter)
        if summary:
            summaries += 1
            events += generate_activity_events_for_symbol(db, summary)
            db.flush()
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
    active_filing_ids = _active_filing_ids_for_period(db, report_year=report_year, report_quarter=report_quarter)
    current_positions = []
    if active_filing_ids:
        current_positions = db.execute(
            select(InstitutionalPosition).where(
                InstitutionalPosition.normalized_symbol == normalized,
                InstitutionalPosition.report_year == report_year,
                InstitutionalPosition.report_quarter == report_quarter,
                InstitutionalPosition.filing_id.in_(active_filing_ids),
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
            InstitutionalPositionChange.change_type != "unchanged",
        )
    ).scalars().all()
    created = 0
    changes_by_event_key: dict[tuple[str, str | None, str, int, int], InstitutionalPositionChange] = {}
    for change in changes:
        event_type = _event_type_for_change(change)
        key = _activity_event_key(
            change.normalized_symbol,
            change.cik,
            event_type,
            change.report_year,
            change.report_quarter,
        )
        existing = changes_by_event_key.get(key)
        if existing is None or _change_event_priority(change) > _change_event_priority(existing):
            changes_by_event_key[key] = change

    for change in changes_by_event_key.values():
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
            InstitutionalActivityEvent.event_type.in_(INSTITUTIONAL_FEED_EVENT_TYPES),
            InstitutionalActivityEvent.materiality_score >= INSTITUTIONAL_FEED_EVENT_MIN_MATERIALITY,
        )
    ).scalars().all()
    created = 0
    for activity in activities:
        if not is_institutional_activity_all_feed_eligible(activity):
            continue
        if _upsert_feed_event(db, activity):
            created += 1
    return created


def cleanup_overbroad_institutional_feed_events(db: Session, *, dry_run: bool = True) -> dict[str, Any]:
    institutional_event_filter = or_(
        Event.source_provider == INSTITUTIONAL_EVENT_SOURCE,
        Event.source_filing_id.like("institutional:%"),
        Event.data_source == INSTITUTIONAL_SOURCE_LABEL,
    )
    events = db.execute(select(Event).where(institutional_event_filter)).scalars().all()
    before = len(events)
    activity_ids = sorted(
        {
            activity_id
            for event in events
            for activity_id in [_activity_id_from_feed_source_filing_id(event.source_filing_id)]
            if activity_id is not None
        }
    )
    activities_by_id: dict[int, InstitutionalActivityEvent] = {}
    if activity_ids:
        activity_rows = db.execute(select(InstitutionalActivityEvent).where(InstitutionalActivityEvent.id.in_(activity_ids))).scalars().all()
        activities_by_id = {int(row.id): row for row in activity_rows if row.id is not None}

    removed_by_type: dict[str, int] = {}
    kept_by_type: dict[str, int] = {}
    remove_events: list[Event] = []
    for event in events:
        activity_id = _activity_id_from_feed_source_filing_id(event.source_filing_id)
        activity = activities_by_id.get(activity_id) if activity_id is not None else None
        keep = bool(activity and is_institutional_activity_all_feed_eligible(activity))
        bucket = kept_by_type if keep else removed_by_type
        bucket[event.event_type] = bucket.get(event.event_type, 0) + 1
        if not keep:
            remove_events.append(event)

    if not dry_run:
        for event in remove_events:
            db.delete(event)
        db.flush()

    remaining_by_type = kept_by_type if not dry_run else {
        event_type: int(count)
        for event_type, count in kept_by_type.items()
    }
    return {
        "status": "ok",
        "dry_run": dry_run,
        "before": before,
        "removed": len(remove_events),
        "remaining": before - len(remove_events),
        "removed_by_event_type": dict(sorted(removed_by_type.items())),
        "remaining_by_event_type": dict(sorted(remaining_by_type.items())),
    }


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
        .where(or_(InstitutionalActivityEvent.freshness_status.is_(None), InstitutionalActivityEvent.freshness_status != "superseded"))
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


def ticker_ownership_payload(
    db: Session,
    symbol: str,
    *,
    history_limit: int = 8,
    holder_limit: int = 15,
) -> dict[str, Any]:
    normalized = normalize_symbol(symbol)
    if not normalized:
        return unavailable_ticker_ownership_payload(symbol, status="invalid_symbol")
    inspector = inspect(db.get_bind())
    required_tables = {
        InstitutionalSymbolSummary.__tablename__,
        InstitutionalPosition.__tablename__,
        InstitutionalHolder.__tablename__,
    }
    if any(not inspector.has_table(table) for table in required_tables):
        return unavailable_ticker_ownership_payload(normalized)

    bounded_history_limit = max(2, min(int(history_limit or 8), 20))
    bounded_holder_limit = max(1, min(int(holder_limit or 15), 50))
    summaries = db.execute(
        select(InstitutionalSymbolSummary)
        .where(InstitutionalSymbolSummary.normalized_symbol == normalized)
        .order_by(
            InstitutionalSymbolSummary.report_year.desc(),
            InstitutionalSymbolSummary.report_quarter.desc(),
            InstitutionalSymbolSummary.latest_filing_date.desc().nullslast(),
        )
        .limit(bounded_history_limit)
    ).scalars().all()
    if not summaries:
        return unavailable_ticker_ownership_payload(normalized, status="no_data")

    latest = summaries[0]
    active_filing_ids = _active_filing_ids_for_period(
        db,
        report_year=latest.report_year,
        report_quarter=latest.report_quarter,
    )
    position_query = select(InstitutionalPosition).where(
        InstitutionalPosition.normalized_symbol == normalized,
        InstitutionalPosition.report_year == latest.report_year,
        InstitutionalPosition.report_quarter == latest.report_quarter,
    )
    if active_filing_ids:
        position_query = position_query.where(InstitutionalPosition.filing_id.in_(active_filing_ids))
    positions = db.execute(position_query).scalars().all()
    holder_ciks = sorted({position.cik for position in positions if position.cik})
    holder_names = {
        row.cik: row.holder_name
        for row in db.execute(select(InstitutionalHolder).where(InstitutionalHolder.cik.in_(holder_ciks))).scalars().all()
    } if holder_ciks else {}
    cik_names = {
        row.cik: row.company_name
        for row in db.execute(select(CikMeta).where(CikMeta.cik.in_(holder_ciks))).scalars().all()
    } if holder_ciks and inspector.has_table(CikMeta.__tablename__) else {}

    holders_by_cik: dict[str, dict[str, Any]] = {}
    for position in positions:
        cik = position.cik
        if not cik:
            continue
        holder = holders_by_cik.setdefault(
            cik,
            {
                "cik": cik,
                "holder_name": holder_names.get(cik) or cik_names.get(cik) or "Institution",
                "ownership_pct": 0.0,
                "value_usd": 0.0,
                "shares": 0.0,
                "portfolio_weight": 0.0,
                "filing_date": position.filing_date,
                "report_year": position.report_year,
                "report_quarter": position.report_quarter,
            },
        )
        if position.ownership_pct is not None:
            holder["ownership_pct"] = float(holder["ownership_pct"] or 0.0) + float(position.ownership_pct)
        if position.value_usd is not None:
            holder["value_usd"] = float(holder["value_usd"] or 0.0) + float(position.value_usd)
        if position.shares is not None:
            holder["shares"] = float(holder["shares"] or 0.0) + float(position.shares)
        if position.portfolio_weight is not None:
            holder["portfolio_weight"] = float(holder["portfolio_weight"] or 0.0) + float(position.portfolio_weight)
        if position.filing_date and (holder["filing_date"] is None or position.filing_date > holder["filing_date"]):
            holder["filing_date"] = position.filing_date

    holders = sorted(
        holders_by_cik.values(),
        key=lambda item: (float(item.get("ownership_pct") or 0.0), float(item.get("value_usd") or 0.0)),
        reverse=True,
    )[:bounded_holder_limit]
    for holder in holders:
        holder["ownership_pct"] = _round_optional(holder.get("ownership_pct"), 4)
        holder["value_usd"] = _round_optional(holder.get("value_usd"), 2)
        holder["shares"] = _round_optional(holder.get("shares"), 4)
        holder["portfolio_weight"] = _round_optional(holder.get("portfolio_weight"), 4)
        holder["filing_date"] = holder["filing_date"].isoformat() if holder.get("filing_date") else None

    latest_institutional_pct = _summary_ownership_pct(latest)
    if latest_institutional_pct is None:
        holder_pct_values = [float(holder["ownership_pct"]) for holder in holders if float(holder.get("ownership_pct") or 0.0) > 0]
        latest_institutional_pct = _ownership_pct(sum(holder_pct_values)) if holder_pct_values else None
    provider_snapshot = None
    if latest_institutional_pct is None:
        provider_snapshot = _provider_symbol_ownership_snapshot(
            normalized,
            report_year=latest.report_year,
            report_quarter=latest.report_quarter,
        )
        latest_institutional_pct = provider_snapshot.get("institutional_ownership_pct") if provider_snapshot else None

    latest_total_holders = provider_snapshot.get("total_holders") if provider_snapshot and provider_snapshot.get("total_holders") is not None else latest.total_holders
    latest_total_value = provider_snapshot.get("total_value_usd") if provider_snapshot and provider_snapshot.get("total_value_usd") is not None else latest.total_value_usd
    history = [_ownership_history_point(row) for row in reversed(summaries)]
    if provider_snapshot and history:
        history[-1] = {
            **history[-1],
            "institutional_ownership_pct": latest_institutional_pct,
            "retail_ownership_pct": _retail_pct(latest_institutional_pct),
            "total_holders": latest_total_holders,
            "total_value_usd": latest_total_value,
            "ownership_source": provider_snapshot.get("ownership_source"),
        }

    return {
        "status": "ok" if latest_institutional_pct is not None else "no_data",
        "symbol": normalized,
        "source_label": INSTITUTIONAL_SOURCE_LABEL,
        "locked": False,
        "required_plan": None,
        "message": None if latest_institutional_pct is not None else "Ownership percentage data is not available for this ticker yet.",
        "tooltip": INSTITUTIONAL_ACTIVITY_TOOLTIP,
        "latest": {
            "report_year": latest.report_year,
            "report_quarter": latest.report_quarter,
            "period": f"Q{latest.report_quarter} {latest.report_year}",
            "latest_filing_date": latest.latest_filing_date.isoformat() if latest.latest_filing_date else None,
            "institutional_ownership_pct": latest_institutional_pct,
            "retail_ownership_pct": _retail_pct(latest_institutional_pct),
            "total_holders": latest_total_holders,
            "total_value_usd": latest_total_value,
            "ownership_source": provider_snapshot.get("ownership_source") if provider_snapshot else "institutional_positions",
        },
        "holders": holders,
        "history": history,
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


def unavailable_ticker_ownership_payload(symbol: str | None = None, *, status: str = "unavailable") -> dict[str, Any]:
    return {
        "status": status,
        "symbol": normalize_symbol(symbol),
        "source_label": INSTITUTIONAL_SOURCE_LABEL,
        "locked": False,
        "required_plan": None,
        "message": "Ownership data is not available for this ticker yet.",
        "tooltip": INSTITUTIONAL_ACTIVITY_TOOLTIP,
        "latest": None,
        "holders": [],
        "history": [],
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
    fetched_rows = db.execute(query.offset(offset).limit(bounded_limit + 1)).scalars().all()
    rows = fetched_rows[:bounded_limit]
    return {
        "items": [holder_payload(row) for row in rows],
        "page": max(0, int(page or 0)),
        "limit": bounded_limit,
        "has_next": len(fetched_rows) > bounded_limit,
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


def _holder_display_name(db: Session, cik: str, holder_name: str | None = None) -> str | None:
    if holder_name and holder_name.strip():
        return holder_name.strip()
    normalized = normalize_cik(cik)
    if not normalized:
        return None
    meta = db.get(CikMeta, normalized)
    if meta and meta.company_name and meta.company_name.strip():
        return meta.company_name.strip()
    return None


def holder_profile(db: Session, cik: str) -> dict[str, Any] | None:
    normalized = normalize_cik(cik)
    if not normalized:
        return None
    holder = db.get(InstitutionalHolder, normalized)
    display_name = _holder_display_name(db, normalized, holder.holder_name if holder else None)
    if holder is None and not display_name:
        return None
    latest_report_year = holder.latest_report_year if holder else None
    latest_report_quarter = holder.latest_report_quarter if holder else None
    latest_filing_date = holder.latest_filing_date if holder else None
    if latest_report_year is None or latest_report_quarter is None:
        latest_position = db.execute(
            select(InstitutionalPosition)
            .join(InstitutionalFiling, InstitutionalFiling.id == InstitutionalPosition.filing_id)
            .where(InstitutionalPosition.cik == normalized)
            .where(InstitutionalFiling.superseded_by.is_(None))
            .order_by(
                InstitutionalPosition.report_year.desc(),
                InstitutionalPosition.report_quarter.desc(),
                InstitutionalPosition.filing_date.desc().nullslast(),
            )
            .limit(1)
        ).scalar_one_or_none()
        if latest_position is not None:
            latest_report_year = latest_position.report_year
            latest_report_quarter = latest_position.report_quarter
            latest_filing_date = latest_position.filing_date
    latest_positions = []
    total_reported_value = 0.0
    holdings_count = 0
    if latest_report_year and latest_report_quarter:
        active_filing_ids = _active_filing_ids_for_period(
            db,
            cik=normalized,
            report_year=latest_report_year,
            report_quarter=latest_report_quarter,
        )
        position_filters = [
            InstitutionalPosition.cik == normalized,
            InstitutionalPosition.report_year == latest_report_year,
            InstitutionalPosition.report_quarter == latest_report_quarter,
        ]
        if active_filing_ids:
            position_filters.append(InstitutionalPosition.filing_id.in_(active_filing_ids))
        total_reported_value, holdings_count = db.execute(
            select(
                func.coalesce(func.sum(InstitutionalPosition.value_usd), 0.0),
                func.count(InstitutionalPosition.id),
            ).where(*position_filters)
        ).one()
        latest_positions = db.execute(
            select(InstitutionalPosition)
            .where(*position_filters)
            .order_by(InstitutionalPosition.value_usd.desc().nullslast())
            .limit(25)
        ).scalars().all()
    payload = holder_payload(holder) if holder else {
        "cik": normalized,
        "holder_name": display_name,
        "holder_type": None,
        "is_passive_like": False,
        "quality_score": None,
        "latest_filing_date": latest_filing_date.isoformat() if latest_filing_date else None,
        "latest_report_year": latest_report_year,
        "latest_report_quarter": latest_report_quarter,
    }
    payload["holder_name"] = display_name or payload.get("holder_name")
    payload["latest_filing_date"] = latest_filing_date.isoformat() if latest_filing_date else payload.get("latest_filing_date")
    payload["latest_report_year"] = latest_report_year
    payload["latest_report_quarter"] = latest_report_quarter
    return {
        **payload,
        "total_reported_value": round(float(total_reported_value or 0.0), 2),
        "total_reported_value_usd": round(float(total_reported_value or 0.0), 2),
        "holdings_count": int(holdings_count or 0),
        "source_label": INSTITUTIONAL_SOURCE_LABEL,
        "availability_status": "ok" if holdings_count else "unavailable",
        "locked": False,
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
    if year is not None and quarter is not None:
        active_filing_ids = _active_filing_ids_for_period(
            db,
            cik=normalized,
            report_year=int(year),
            report_quarter=int(quarter),
        )
        if active_filing_ids:
            query = query.where(InstitutionalPosition.filing_id.in_(active_filing_ids))
    else:
        query = query.join(InstitutionalFiling, InstitutionalFiling.id == InstitutionalPosition.filing_id).where(
            InstitutionalFiling.superseded_by.is_(None)
        )
    fetched_rows = db.execute(
        query.order_by(InstitutionalPosition.report_year.desc(), InstitutionalPosition.report_quarter.desc(), InstitutionalPosition.value_usd.desc().nullslast())
        .offset(max(0, int(page or 0)) * bounded_limit)
        .limit(bounded_limit + 1)
    ).scalars().all()
    rows = fetched_rows[:bounded_limit]
    return {"items": [position_payload(row) for row in rows], "page": page, "limit": bounded_limit, "has_next": len(fetched_rows) > bounded_limit}


def _reported_action_label(change_type: str | None, value_delta_usd: float | None = None) -> str:
    normalized = (change_type or "").strip().lower()
    if normalized == "new_position":
        return "New Position"
    if normalized == "exit":
        return "Reported Exit"
    if normalized == "decrease":
        return "Reported Reduction"
    if normalized == "increase":
        return "Reported Increase"
    if value_delta_usd is not None:
        if value_delta_usd < 0:
            return "Reported Reduction"
        if value_delta_usd > 0:
            return "Reported Increase"
    return "Reported Activity"


def activity_for_holder(db: Session, cik: str, *, page: int = 0, limit: int = 50) -> dict[str, Any]:
    normalized = normalize_cik(cik)
    if not normalized:
        return {"items": [], "page": page, "limit": limit, "has_next": False}
    bounded_limit = max(1, min(int(limit or 50), 200))
    fetched_rows = db.execute(
        select(InstitutionalPositionChange)
        .where(InstitutionalPositionChange.cik == normalized)
        .where(InstitutionalPositionChange.change_type != "unchanged")
        .order_by(
            InstitutionalPositionChange.filing_date.desc(),
            InstitutionalPositionChange.materiality_score.desc(),
            InstitutionalPositionChange.id.desc(),
        )
        .offset(max(0, int(page or 0)) * bounded_limit)
        .limit(bounded_limit + 1)
    ).scalars().all()
    rows = fetched_rows[:bounded_limit]
    symbols = sorted({row.normalized_symbol for row in rows if row.normalized_symbol})
    issuer_names: dict[str, str | None] = {}
    if symbols:
        positions = db.execute(
            select(InstitutionalPosition.normalized_symbol, InstitutionalPosition.issuer_name)
            .where(InstitutionalPosition.cik == normalized)
            .where(InstitutionalPosition.normalized_symbol.in_(symbols))
            .where(InstitutionalPosition.issuer_name.is_not(None))
            .order_by(
                InstitutionalPosition.report_year.desc(),
                InstitutionalPosition.report_quarter.desc(),
                InstitutionalPosition.value_usd.desc().nullslast(),
            )
        ).all()
        for symbol, issuer_name in positions:
            issuer_names.setdefault(symbol, issuer_name)
    return {
        "items": [
            {
                "id": row.id,
                "symbol": row.normalized_symbol or row.symbol,
                "issuer_name": issuer_names.get(row.normalized_symbol or ""),
                "action": _reported_action_label(row.change_type, row.value_delta_usd),
                "change_type": row.change_type,
                "direction": row.direction,
                "current_value_usd": 0 if row.change_type == "exit" else row.curr_value_usd,
                "prior_value_usd": row.prev_value_usd,
                "value_delta_usd": row.value_delta_usd,
                "value_delta_pct": row.value_delta_pct,
                "portfolio_weight_delta": row.portfolio_weight_delta,
                "filing_date": row.filing_date.isoformat() if row.filing_date else None,
                "report_period": f"Q{row.report_quarter} {row.report_year}",
                "report_year": row.report_year,
                "report_quarter": row.report_quarter,
                "materiality_score": row.materiality_score,
            }
            for row in rows
        ],
        "page": page,
        "limit": bounded_limit,
        "has_next": len(fetched_rows) > bounded_limit,
    }


def filings_for_holder(db: Session, cik: str, *, page: int = 0, limit: int = 50) -> dict[str, Any]:
    normalized = normalize_cik(cik)
    if not normalized:
        return {"items": [], "page": page, "limit": limit, "has_next": False}
    bounded_limit = max(1, min(int(limit or 50), 200))
    fetched_rows = db.execute(
        select(InstitutionalFiling)
        .where(InstitutionalFiling.cik == normalized)
        .order_by(InstitutionalFiling.filing_date.desc(), InstitutionalFiling.id.desc())
        .offset(max(0, int(page or 0)) * bounded_limit)
        .limit(bounded_limit + 1)
    ).scalars().all()
    rows = fetched_rows[:bounded_limit]
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
                "superseded_by": row.superseded_by,
                "canonical": row.superseded_by is None,
                "processed_at": row.processed_at.isoformat() if row.processed_at else None,
                "holdings_count": db.execute(
                    select(func.count(InstitutionalPosition.id)).where(InstitutionalPosition.filing_id == row.id)
                ).scalar_one(),
                "status": (
                    "processed"
                    if row.processed_at
                    else "retryable"
                    if (row.form_type or "").upper() in {"13F-HR", "13F-HR/A"}
                    else "no holdings"
                ),
            }
            for row in rows
        ],
        "page": page,
        "limit": bounded_limit,
        "has_next": len(fetched_rows) > bounded_limit,
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
        .where(or_(InstitutionalActivityEvent.freshness_status.is_(None), InstitutionalActivityEvent.freshness_status != "superseded"))
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
    existing.feed_visible = is_institutional_activity_all_feed_eligible(existing)
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
    existing.feed_visible = is_institutional_activity_all_feed_eligible(existing)
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
    active_filing_ids = _active_filing_ids_for_period(
        db,
        cik=filing.cik,
        report_year=prior_year,
        report_quarter=prior_quarter,
    )
    query = select(InstitutionalPosition).where(
        InstitutionalPosition.cik == filing.cik,
        InstitutionalPosition.report_year == prior_year,
        InstitutionalPosition.report_quarter == prior_quarter,
    )
    if active_filing_ids:
        query = query.where(InstitutionalPosition.filing_id.in_(active_filing_ids))
    return db.execute(query).scalars().all()


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


def _activity_event_key(
    normalized_symbol: str | None,
    cik: str | None,
    event_type: str,
    report_year: int,
    report_quarter: int,
) -> tuple[str, str | None, str, int, int]:
    return (
        normalize_symbol(normalized_symbol) or "",
        normalize_cik(cik) if cik else None,
        event_type,
        int(report_year),
        int(report_quarter),
    )


def _change_event_priority(change: InstitutionalPositionChange) -> tuple[float, float, float, int]:
    materiality = float(change.materiality_score or 0.0)
    magnitude = max(
        abs(float(change.curr_value_usd or 0.0)),
        abs(float(change.prev_value_usd or 0.0)),
        abs(float(change.value_delta_usd or 0.0)),
    )
    ownership = abs(float(change.curr_ownership_pct or change.prev_ownership_pct or 0.0))
    return (materiality, magnitude, ownership, int(change.id or 0))


def _activity_event_value_magnitude(activity: InstitutionalActivityEvent) -> float:
    return max(
        abs(float(activity.reported_value_usd or 0.0)),
        abs(float(activity.value_delta_usd or 0.0)),
    )


def is_institutional_activity_all_feed_eligible(activity: InstitutionalActivityEvent) -> bool:
    event_type = (activity.event_type or "").strip()
    materiality = float(activity.materiality_score or 0.0)
    value = _activity_event_value_magnitude(activity)
    holder_breadth = abs(int(activity.holder_breadth or 0))

    if materiality < INSTITUTIONAL_ALL_FEED_MIN_MATERIALITY:
        return False
    if event_type == "smart_money_confirmation":
        return value >= INSTITUTIONAL_ALL_FEED_LARGE_VALUE_USD
    if event_type in {"major_holder_exit", "major_holder_reduction", "new_institutional_position"}:
        return value >= INSTITUTIONAL_ALL_FEED_LARGE_VALUE_USD
    if event_type in {"cluster_accumulation", "cluster_distribution"}:
        return (
            materiality >= INSTITUTIONAL_ALL_FEED_CLUSTER_MIN_MATERIALITY
            and value >= INSTITUTIONAL_ALL_FEED_CLUSTER_VALUE_USD
            and holder_breadth >= INSTITUTIONAL_ALL_FEED_CLUSTER_BREADTH
        )
    return False


def _activity_id_from_feed_source_filing_id(value: str | None) -> int | None:
    if not value:
        return None
    match = re.match(r"^institutional:(\d+):", str(value))
    if not match:
        return None
    try:
        return int(match.group(1))
    except ValueError:
        return None


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


def _position_payload_identity_key(payload: InstitutionalPositionPayload) -> tuple[str, str, str]:
    put_call = payload.put_call or ""
    if payload.cusip:
        return ("cusip", payload.cusip, put_call)
    return ("symbol", payload.normalized_symbol or "", put_call)


def _position_payload_fingerprint(payload: InstitutionalPositionPayload) -> str:
    return json.dumps(
        {
            "symbol": payload.symbol,
            "normalized_symbol": payload.normalized_symbol,
            "cusip": payload.cusip,
            "issuer_name": payload.issuer_name,
            "shares": payload.shares,
            "value_usd": payload.value_usd,
            "put_call": payload.put_call,
            "investment_discretion": payload.investment_discretion,
            "voting_authority": payload.voting_authority,
            "portfolio_weight": payload.portfolio_weight,
            "ownership_pct": payload.ownership_pct,
        },
        sort_keys=True,
        default=str,
    )


def _sum_optional_numbers(left: float | None, right: float | None) -> float | None:
    if left is None and right is None:
        return None
    return float(left or 0.0) + float(right or 0.0)


def _merge_optional_text(left: str | None, right: str | None) -> str | None:
    if not left:
        return right
    if not right or left == right:
        return left
    return "mixed"


def _merge_voting_authority(left: dict[str, Any] | str | None, right: dict[str, Any] | str | None) -> dict[str, Any] | str | None:
    if left is None:
        return right
    if right is None or left == right:
        return left
    components: list[Any] = []
    if isinstance(left, dict) and isinstance(left.get("components"), list):
        components.extend(left["components"])
    else:
        components.append(left)
    if isinstance(right, dict) and isinstance(right.get("components"), list):
        components.extend(right["components"])
    else:
        components.append(right)
    return {"components": components}


def _merge_position_payloads(
    left: InstitutionalPositionPayload,
    right: InstitutionalPositionPayload,
) -> InstitutionalPositionPayload:
    return InstitutionalPositionPayload(
        symbol=left.symbol or right.symbol,
        normalized_symbol=left.normalized_symbol or right.normalized_symbol,
        cusip=left.cusip or right.cusip,
        issuer_name=left.issuer_name or right.issuer_name,
        shares=_sum_optional_numbers(left.shares, right.shares),
        value_usd=_sum_optional_numbers(left.value_usd, right.value_usd),
        put_call=left.put_call or right.put_call,
        investment_discretion=_merge_optional_text(left.investment_discretion, right.investment_discretion),
        voting_authority=_merge_voting_authority(left.voting_authority, right.voting_authority),
        portfolio_weight=_sum_optional_numbers(left.portfolio_weight, right.portfolio_weight),
        ownership_pct=_sum_optional_numbers(left.ownership_pct, right.ownership_pct),
        raw={"components": [left.raw, right.raw]},
    )


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


def _round_optional(value: Any, digits: int) -> float | None:
    if value is None:
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(parsed):
        return None
    return round(parsed, digits)


def _ownership_pct(value: Any) -> float | None:
    parsed = _round_optional(value, 4)
    if parsed is None:
        return None
    return max(0.0, min(parsed, 100.0))


def _summary_ownership_pct(summary: InstitutionalSymbolSummary) -> float | None:
    parsed = _ownership_pct(summary.institutional_ownership_pct)
    if parsed is None:
        return None
    if parsed == 0 and (int(summary.total_holders or 0) > 0 or float(summary.total_value_usd or 0.0) > 0):
        return None
    return parsed


def _retail_pct(institutional_pct: float | None) -> float | None:
    if institutional_pct is None:
        return None
    return round(max(0.0, 100.0 - institutional_pct), 4)


def _ownership_history_point(summary: InstitutionalSymbolSummary) -> dict[str, Any]:
    institutional_pct = _summary_ownership_pct(summary)
    return {
        "report_year": summary.report_year,
        "report_quarter": summary.report_quarter,
        "period": f"Q{summary.report_quarter} {summary.report_year}",
        "latest_filing_date": summary.latest_filing_date.isoformat() if summary.latest_filing_date else None,
        "institutional_ownership_pct": institutional_pct,
        "retail_ownership_pct": _retail_pct(institutional_pct),
        "total_holders": summary.total_holders,
        "total_value_usd": summary.total_value_usd,
    }


def _provider_symbol_ownership_snapshot(
    symbol: str,
    *,
    report_year: int,
    report_quarter: int,
) -> dict[str, Any] | None:
    try:
        rows = fetch_symbol_positions_summary(symbol=symbol, year=report_year, quarter=report_quarter)
    except Exception:
        return None
    for row in rows:
        if not isinstance(row, dict):
            continue
        ownership_pct = _first_number(
            row,
            "institutionalOwnershipPct",
            "institutional_ownership_pct",
            "institutionalOwnershipPercent",
            "ownershipPercent",
            "ownershipPercentage",
            "percentOfSharesHeldByInstitutions",
            "percentageOfSharesHeldByInstitutions",
            "percentOfSharesOutstanding",
        )
        ownership_pct = _ownership_pct(ownership_pct)
        if ownership_pct is None:
            continue
        if ownership_pct == 0:
            continue
        return {
            "institutional_ownership_pct": ownership_pct,
            "retail_ownership_pct": _retail_pct(ownership_pct),
            "total_holders": _first_int(
                row,
                "investorsHolding",
                "investorHolding",
                "numberOfInstitutions",
                "numberOfInstitutionalHolders",
                "institutionsHolding",
                "holders",
                "totalHolders",
            ),
            "total_value_usd": _first_number(
                row,
                "totalInvested",
                "totalValue",
                "totalValueUsd",
                "marketValue",
                "marketValueUsd",
                "valueUsd",
            ),
            "ownership_source": "provider_symbol_positions_summary",
        }
    return None


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
