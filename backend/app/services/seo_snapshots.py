from __future__ import annotations

import hashlib
import json
import re
from datetime import date, datetime, timezone
from typing import Any, Literal

from sqlalchemy import desc, func, select
from sqlalchemy.orm import Session

from app.models import (
    Event,
    IndexMembership,
    InsiderTransaction,
    InsiderTransactionNormalized,
    Member,
    PriceCache,
    SeoEntitySnapshot,
    TickerMeta,
    Transaction,
)

SeoEntityType = Literal["ticker", "member", "insider"]
SEO_SNAPSHOT_SCHEMA_VERSION = 1
SEO_BATCH_MAX_LIMIT = 250


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _json_dumps(payload: dict[str, Any]) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)


def _content_hash(payload: dict[str, Any]) -> str:
    return hashlib.sha256(_json_dumps(payload).encode("utf-8")).hexdigest()


def _clean_text(value: object | None) -> str | None:
    text = str(value or "").strip()
    return text or None


def _iso(value: datetime | date | None) -> str | None:
    if value is None:
        return None
    return value.isoformat()


def _cap_data_as_of(value: datetime | None, generated_at: datetime) -> datetime | None:
    if value is None:
        return None
    comparable = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    return min(comparable, generated_at)


def _normalize_symbol(symbol: str) -> str:
    return re.sub(r"[^A-Z0-9.\-]", "", (symbol or "").strip().upper())


def _normalize_cik(value: str) -> str:
    digits = re.sub(r"\D+", "", value or "")
    return digits.zfill(10) if digits else ""


def _slugify(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", (value or "").strip().lower()).strip("-")
    return slug or "unknown"


def _member_name(member: Member) -> str:
    return " ".join(part for part in [member.first_name, member.last_name] if _clean_text(part)).strip() or member.bioguide_id


def _clamped_batch_limit(limit: int) -> int:
    return max(1, min(int(limit or 1), SEO_BATCH_MAX_LIMIT))


def get_seo_snapshot(db: Session, entity_type: SeoEntityType, entity_key: str) -> dict[str, Any] | None:
    normalized_key = normalize_snapshot_key(entity_type, entity_key)
    if not normalized_key:
        return None
    row = db.execute(
        select(SeoEntitySnapshot).where(
            SeoEntitySnapshot.entity_type == entity_type,
            SeoEntitySnapshot.entity_key == normalized_key,
        )
    ).scalar_one_or_none()
    if row is None:
        return None
    return seo_snapshot_row_payload(row)


def list_indexable_seo_snapshots(db: Session, entity_type: SeoEntityType, *, limit: int = 50000) -> list[dict[str, Any]]:
    rows = db.execute(
        select(SeoEntitySnapshot)
        .where(
            SeoEntitySnapshot.entity_type == entity_type,
            SeoEntitySnapshot.indexable.is_(True),
        )
        .order_by(desc(SeoEntitySnapshot.updated_at), SeoEntitySnapshot.entity_key)
        .limit(max(1, min(limit, 50000)))
    ).scalars().all()
    return [seo_snapshot_row_payload(row) for row in rows]


def list_seo_snapshot_batch_candidates(
    db: Session,
    entity_type: SeoEntityType,
    *,
    limit: int,
    include_existing: bool = False,
) -> list[str]:
    capped_limit = _clamped_batch_limit(limit)
    if entity_type == "ticker":
        return _ticker_batch_candidates(db, capped_limit, include_existing=include_existing)
    if entity_type == "member":
        return _member_batch_candidates(db, capped_limit, include_existing=include_existing)
    if entity_type == "insider":
        return _insider_batch_candidates(db, capped_limit, include_existing=include_existing)
    raise ValueError(f"Unsupported SEO snapshot entity type: {entity_type}")


def seo_snapshot_row_payload(row: SeoEntitySnapshot) -> dict[str, Any]:
    try:
        payload = json.loads(row.payload_json or "{}")
    except json.JSONDecodeError:
        payload = {}
    if not isinstance(payload, dict):
        payload = {}
    return {
        "entity_type": row.entity_type,
        "entity_key": row.entity_key,
        "canonical_path": row.canonical_path,
        "title": row.title,
        "meta_description": row.meta_description,
        "indexable": bool(row.indexable),
        "payload": payload,
        "content_hash": row.content_hash,
        "schema_version": row.schema_version,
        "data_as_of": _iso(row.data_as_of),
        "generated_at": _iso(row.generated_at),
        "updated_at": _iso(row.updated_at),
    }


def normalize_snapshot_key(entity_type: SeoEntityType, entity_key: str) -> str:
    if entity_type == "ticker":
        return _normalize_symbol(entity_key)
    if entity_type == "insider":
        return _normalize_cik(entity_key)
    return _slugify(entity_key)


def _existing_snapshot_keys(db: Session, entity_type: SeoEntityType) -> set[str]:
    rows = db.execute(
        select(SeoEntitySnapshot.entity_key).where(SeoEntitySnapshot.entity_type == entity_type)
    ).scalars().all()
    return {row for row in rows if row}


def _ticker_batch_candidates(db: Session, limit: int, *, include_existing: bool) -> list[str]:
    existing = set() if include_existing else _existing_snapshot_keys(db, "ticker")
    rows = db.execute(
        select(
            TickerMeta.symbol,
            func.max(PriceCache.updated_at).label("latest_price_at"),
            func.count(func.distinct(Event.id)).label("event_count"),
            func.count(func.distinct(IndexMembership.id)).label("index_count"),
        )
        .join(PriceCache, func.upper(PriceCache.symbol) == func.upper(TickerMeta.symbol))
        .outerjoin(Event, func.upper(func.coalesce(Event.symbol, "")) == func.upper(TickerMeta.symbol))
        .outerjoin(
            IndexMembership,
            (func.upper(IndexMembership.symbol) == func.upper(TickerMeta.symbol)) & (IndexMembership.is_active.is_(True)),
        )
        .where(TickerMeta.company_name.is_not(None))
        .group_by(TickerMeta.symbol)
        .having((func.count(func.distinct(Event.id)) > 0) | (func.count(func.distinct(IndexMembership.id)) > 0))
        .order_by(desc("event_count"), desc("latest_price_at"), TickerMeta.symbol)
        .limit(limit * 4)
    ).all()
    candidates: list[str] = []
    for row in rows:
        key = _normalize_symbol(row.symbol)
        if not key or key in existing or key in candidates:
            continue
        candidates.append(key)
        if len(candidates) >= limit:
            break
    return candidates


def _member_batch_candidates(db: Session, limit: int, *, include_existing: bool) -> list[str]:
    existing = set() if include_existing else _existing_snapshot_keys(db, "member")
    rows = db.execute(
        select(
            Member,
            func.count(Transaction.id).label("trade_count"),
            func.max(Transaction.report_date).label("latest_report_date"),
        )
        .join(Transaction, Transaction.member_id == Member.id)
        .group_by(Member.id)
        .having(func.count(Transaction.id) > 0)
        .order_by(desc("latest_report_date"), desc("trade_count"), Member.last_name, Member.first_name)
        .limit(limit * 4)
    ).all()
    candidates: list[str] = []
    for member, _trade_count, _latest_report_date in rows:
        key = _slugify(_member_name(member))
        if not key or key in existing or key in candidates:
            continue
        candidates.append(key)
        if len(candidates) >= limit:
            break
    return candidates


def _insider_batch_candidates(db: Session, limit: int, *, include_existing: bool) -> list[str]:
    existing = set() if include_existing else _existing_snapshot_keys(db, "insider")
    rows = db.execute(
        select(
            InsiderTransactionNormalized.reporting_owner_cik,
            InsiderTransactionNormalized.reporting_owner_name,
            func.count(InsiderTransactionNormalized.id).label("filing_count"),
            func.max(InsiderTransactionNormalized.filing_date).label("latest_filing_date"),
        )
        .where(
            InsiderTransactionNormalized.reporting_owner_cik.is_not(None),
            InsiderTransactionNormalized.reporting_owner_name.is_not(None),
            InsiderTransactionNormalized.is_duplicate.is_(False),
        )
        .group_by(
            InsiderTransactionNormalized.reporting_owner_cik,
            InsiderTransactionNormalized.reporting_owner_name,
        )
        .having(func.count(InsiderTransactionNormalized.id) > 0)
        .order_by(desc("latest_filing_date"), desc("filing_count"), InsiderTransactionNormalized.reporting_owner_name)
        .limit(limit * 4)
    ).all()
    candidates: list[str] = []
    for reporting_cik, reporting_owner_name, _filing_count, _latest_filing_date in rows:
        key = _normalize_cik(reporting_cik or "")
        if not key or not _clean_text(reporting_owner_name) or key in existing or key in candidates:
            continue
        candidates.append(key)
        if len(candidates) >= limit:
            break
    return candidates


def _upsert_snapshot(
    db: Session,
    *,
    entity_type: SeoEntityType,
    entity_key: str,
    canonical_path: str,
    title: str,
    meta_description: str,
    indexable: bool,
    payload: dict[str, Any],
    data_as_of: datetime | None,
) -> dict[str, Any]:
    now = _now()
    normalized_key = normalize_snapshot_key(entity_type, entity_key)
    row = db.execute(
        select(SeoEntitySnapshot).where(
            SeoEntitySnapshot.entity_type == entity_type,
            SeoEntitySnapshot.entity_key == normalized_key,
        )
    ).scalar_one_or_none()
    if row is None:
        row = SeoEntitySnapshot(
            entity_type=entity_type,
            entity_key=normalized_key,
            canonical_path=canonical_path,
            title=title,
            meta_description=meta_description,
            indexable=indexable,
            payload_json="{}",
            generated_at=now,
        )
        db.add(row)
    row.canonical_path = canonical_path
    row.title = title
    row.meta_description = meta_description
    row.indexable = indexable
    row.payload_json = _json_dumps(payload)
    row.content_hash = _content_hash(payload)
    row.schema_version = SEO_SNAPSHOT_SCHEMA_VERSION
    row.data_as_of = _cap_data_as_of(data_as_of, now)
    row.generated_at = now
    row.updated_at = now
    db.flush()
    return seo_snapshot_row_payload(row)


def refresh_ticker_seo_snapshot(db: Session, symbol: str) -> dict[str, Any]:
    normalized = _normalize_symbol(symbol)
    if not normalized:
        raise ValueError("symbol is required")

    meta = db.execute(select(TickerMeta).where(func.upper(TickerMeta.symbol) == normalized)).scalar_one_or_none()
    latest_price = db.execute(
        select(PriceCache)
        .where(func.upper(PriceCache.symbol) == normalized)
        .order_by(desc(PriceCache.date))
        .limit(1)
    ).scalar_one_or_none()
    recent_events = db.execute(
        select(Event)
        .where(func.upper(func.coalesce(Event.symbol, "")) == normalized)
        .order_by(desc(Event.ts))
        .limit(12)
    ).scalars().all()

    congress_count = sum(1 for event in recent_events if event.event_type == "congress_trade")
    insider_count = sum(1 for event in recent_events if event.event_type == "insider_trade")
    company_name = _clean_text(getattr(meta, "company_name", None)) or normalized
    data_dates = [
        value
        for value in [
            latest_price.updated_at if latest_price else None,
            max((event.ts for event in recent_events if event.ts), default=None),
        ]
        if value is not None
    ]
    data_as_of = max(data_dates) if data_dates else None
    sections = [
        {
            "heading": "Snapshot",
            "body": f"{normalized} is shown from Walnut's stored market and disclosure data. The page does not refresh providers during public rendering.",
        }
    ]
    if latest_price:
        sections.append(
            {
                "heading": "Stored Market Data",
                "body": f"Latest stored close is {latest_price.close:g} for {latest_price.date}.",
            }
        )
    if congress_count or insider_count:
        sections.append(
            {
                "heading": "Disclosure Activity",
                "body": f"Recent stored activity includes {congress_count} Congress item(s) and {insider_count} insider item(s) in the page snapshot.",
            }
        )
    payload = {
        "symbol": normalized,
        "company_name": company_name,
        "exchange": _clean_text(getattr(meta, "exchange", None)),
        "sector": _clean_text(getattr(meta, "sector", None)),
        "industry": _clean_text(getattr(meta, "industry", None)),
        "price": latest_price.close if latest_price else None,
        "price_date": latest_price.date if latest_price else None,
        "sections": sections,
        "links": [
            {"label": "Compare NVDA vs MU", "href": "/compare/NVDA/MU"} if normalized == "NVDA" else None,
        ],
    }
    payload["links"] = [item for item in payload["links"] if item]
    indexable = bool(company_name and (latest_price or recent_events))
    return _upsert_snapshot(
        db,
        entity_type="ticker",
        entity_key=normalized,
        canonical_path=f"/ticker/{normalized}",
        title=f"{normalized} Stock Analysis, Insider Activity & Research | Walnut Markets",
        meta_description=f"Analyze {normalized} with Walnut using stored market data, disclosures, insider activity, Congress trades, and cross-source research context.",
        indexable=indexable,
        payload=payload,
        data_as_of=data_as_of,
    )


def refresh_member_seo_snapshot(db: Session, slug_or_bioguide: str) -> dict[str, Any]:
    key = (slug_or_bioguide or "").strip()
    member = db.execute(select(Member).where(func.lower(Member.bioguide_id) == key.lower())).scalar_one_or_none()
    if member is None:
        slug = _slugify(key)
        members = db.execute(select(Member)).scalars().all()
        member = next((row for row in members if _slugify(_member_name(row)) == slug), None)
    if member is None:
        raise ValueError("member not found")

    member_name = _member_name(member)
    slug = _slugify(member_name)
    recent_trades = db.execute(
        select(Transaction)
        .where(Transaction.member_id == member.id)
        .order_by(desc(Transaction.report_date), desc(Transaction.trade_date), desc(Transaction.id))
        .limit(12)
    ).scalars().all()
    symbols = []
    for trade in recent_trades:
        if trade.description:
            symbols.append(trade.description[:80])
    data_as_of_date = max((trade.report_date for trade in recent_trades if trade.report_date), default=None)
    data_as_of = datetime.combine(data_as_of_date, datetime.min.time(), tzinfo=timezone.utc) if data_as_of_date else None
    payload = {
        "member_name": member_name,
        "bioguide_id": member.bioguide_id,
        "chamber": member.chamber,
        "party": member.party,
        "state": member.state,
        "sections": [
            {
                "heading": "Congress Trading Snapshot",
                "body": f"{member_name} has {len(recent_trades)} recent stored disclosure item(s) available in this SEO snapshot.",
            }
        ],
        "recent_activity": [
            {
                "transaction_type": trade.transaction_type,
                "description": trade.description,
                "trade_date": _iso(trade.trade_date),
                "report_date": _iso(trade.report_date),
            }
            for trade in recent_trades[:6]
        ],
        "links": [],
    }
    return _upsert_snapshot(
        db,
        entity_type="member",
        entity_key=slug,
        canonical_path=f"/member/{slug}",
        title=f"{member_name} Stock Trades & Congressional Activity | Walnut Markets",
        meta_description=f"Research {member_name}'s stored congressional disclosure activity, traded ticker context, and public profile in Walnut Markets.",
        indexable=bool(member.bioguide_id and (recent_trades or member_name)),
        payload=payload,
        data_as_of=data_as_of,
    )


def refresh_insider_seo_snapshot(db: Session, reporting_cik: str) -> dict[str, Any]:
    normalized_cik = _normalize_cik(reporting_cik)
    if not normalized_cik:
        raise ValueError("reporting_cik is required")
    rows = db.execute(
        select(InsiderTransactionNormalized)
        .where(func.replace(InsiderTransactionNormalized.reporting_owner_cik, "-", "") == normalized_cik)
        .order_by(desc(InsiderTransactionNormalized.filing_date), desc(InsiderTransactionNormalized.transaction_date))
        .limit(12)
    ).scalars().all()
    legacy_rows = []
    if not rows:
        legacy_rows = db.execute(
            select(InsiderTransaction)
            .where(func.replace(InsiderTransaction.reporting_cik, "-", "") == normalized_cik)
            .order_by(desc(InsiderTransaction.filing_date), desc(InsiderTransaction.transaction_date))
            .limit(12)
        ).scalars().all()

    working_rows: list[Any] = list(rows) or list(legacy_rows)
    insider_name = next(
        (
            _clean_text(getattr(row, "reporting_owner_name", None) or getattr(row, "insider_name", None))
            for row in working_rows
            if _clean_text(getattr(row, "reporting_owner_name", None) or getattr(row, "insider_name", None))
        ),
        None,
    ) or "Insider"
    symbol = next(
        (
            _clean_text(getattr(row, "ticker_normalized", None) or getattr(row, "symbol", None))
            for row in working_rows
            if _clean_text(getattr(row, "ticker_normalized", None) or getattr(row, "symbol", None))
        ),
        None,
    )
    role = next(
        (
            _clean_text(getattr(row, "officer_title", None) or getattr(row, "role", None) or getattr(row, "position", None))
            for row in working_rows
            if _clean_text(getattr(row, "officer_title", None) or getattr(row, "role", None) or getattr(row, "position", None))
        ),
        None,
    )
    latest_date = max((getattr(row, "filing_date", None) for row in working_rows if getattr(row, "filing_date", None)), default=None)
    data_as_of = datetime.combine(latest_date, datetime.min.time(), tzinfo=timezone.utc) if latest_date else None
    slug = f"{_slugify(insider_name)}-{normalized_cik}"
    payload = {
        "insider_name": insider_name,
        "reporting_cik": normalized_cik,
        "primary_symbol": symbol,
        "primary_role": role,
        "sections": [
            {
                "heading": "Form 4 Activity Snapshot",
                "body": f"{insider_name} has {len(working_rows)} recent stored Form 4 item(s) available in this SEO snapshot.",
            }
        ],
        "recent_activity": [
            {
                "symbol": _clean_text(getattr(row, "ticker_normalized", None) or getattr(row, "symbol", None)),
                "transaction_type": _clean_text(getattr(row, "transaction_type", None) or getattr(row, "transaction_code", None)),
                "transaction_date": _iso(getattr(row, "transaction_date", None)),
                "filing_date": _iso(getattr(row, "filing_date", None)),
            }
            for row in working_rows[:6]
        ],
        "links": [{"label": f"{symbol} stock research", "href": f"/ticker/{symbol}"}] if symbol else [],
    }
    return _upsert_snapshot(
        db,
        entity_type="insider",
        entity_key=normalized_cik,
        canonical_path=f"/insider/{slug}",
        title=f"{insider_name} Insider Trades & Form 4 Activity | Walnut Markets",
        meta_description=f"Research {insider_name}'s stored Form 4 insider trading activity, issuer context, and related ticker links in Walnut Markets.",
        indexable=bool(working_rows and insider_name != "Insider"),
        payload=payload,
        data_as_of=data_as_of,
    )


def refresh_seo_snapshot(db: Session, entity_type: SeoEntityType, entity_key: str) -> dict[str, Any]:
    if entity_type == "ticker":
        return refresh_ticker_seo_snapshot(db, entity_key)
    if entity_type == "member":
        return refresh_member_seo_snapshot(db, entity_key)
    if entity_type == "insider":
        return refresh_insider_seo_snapshot(db, entity_key)
    raise ValueError(f"Unsupported SEO snapshot entity type: {entity_type}")
