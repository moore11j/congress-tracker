from __future__ import annotations

import logging
import os
import re
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from html import unescape as html_unescape
from typing import Any, Iterable, Protocol
from urllib.parse import unquote

import requests
from lxml import html
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.clients.fmp import FMPClientError, FMPSubscriptionRestrictedError, fetch_index_constituents
from app.models import IndexMembership
from app.utils.symbols import normalize_symbol

logger = logging.getLogger(__name__)

INDEX_MEMBERSHIP_STALE_AFTER_DAYS = 7
WIKIPEDIA_API_URL = "https://en.wikipedia.org/w/api.php"
WIKIPEDIA_PARSER_VERSION = "wikipedia_component_table_v1"
WIKIPEDIA_SOURCE_KIND = "community_maintained_component_table"
WIKIPEDIA_USER_AGENT = os.getenv(
    "INDEX_MEMBERSHIP_WIKIPEDIA_USER_AGENT",
    "WalnutMarkets-IndexMembership/1.0 (contact: support@walnutmarkets.com)",
)


@dataclass(frozen=True)
class IndexUniverseDefinition:
    code: str
    label: str
    provider_code: str
    min_count: int
    max_count: int
    required_symbols: tuple[str, ...]


@dataclass(frozen=True)
class IndexMembershipSnapshot:
    code: str
    symbols: list[str]
    membership_count: int
    source: str | None
    source_kind: str | None
    source_page: str | None
    source_revision_id: str | None
    resolved_source_title: str | None
    parser_version: str | None
    source_as_of: str | None
    refreshed_at: str | None
    status: str
    reason: str | None = None

    @property
    def supported(self) -> bool:
        return self.status in {"available", "stale"}


@dataclass(frozen=True)
class IndexMembershipRefreshResult:
    code: str
    status: str
    incoming_count: int
    active_count: int
    inserted_count: int = 0
    retained_count: int = 0
    end_dated_count: int = 0
    parsed_count: int = 0
    accepted_count: int = 0
    rejected_count: int = 0
    source: str | None = None
    source_kind: str | None = None
    source_page: str | None = None
    resolved_source_title: str | None = None
    source_revision_id: str | None = None
    source_as_of: str | None = None
    parser_version: str | None = None
    reason: str | None = None
    warnings: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class MembershipRecord:
    symbol: str
    raw_symbol: str
    company_name: str | None = None
    sector: str | None = None
    sub_industry: str | None = None


@dataclass(frozen=True)
class MembershipDataset:
    index_code: str
    records: list[MembershipRecord]
    source: str
    source_kind: str
    source_page: str | None
    resolved_source_title: str | None
    source_revision_id: str | None
    source_as_of: date
    retrieved_at: datetime
    parser_version: str
    parsed_count: int
    rejected_count: int = 0
    warnings: list[str] = field(default_factory=list)

    @property
    def symbols(self) -> list[str]:
        return [record.symbol for record in self.records]


class IndexMembershipProvider(Protocol):
    source_name: str

    def fetch_memberships(self, index_code: str) -> MembershipDataset:
        ...


INDEX_UNIVERSES: dict[str, IndexUniverseDefinition] = {
    "sp500": IndexUniverseDefinition(
        code="sp500",
        label="S&P 500",
        provider_code="sp500",
        min_count=495,
        max_count=510,
        required_symbols=("NVDA", "AAPL", "MSFT", "JPM", "XOM"),
    ),
    "nasdaq100": IndexUniverseDefinition(
        code="nasdaq100",
        label="Nasdaq 100",
        provider_code="nasdaq100",
        min_count=98,
        max_count=105,
        required_symbols=("NVDA", "AAPL", "MSFT", "AMZN"),
    ),
}

WIKIPEDIA_PAGES = {
    "sp500": "List_of_S%26P_500_companies",
    "nasdaq100": "List_of_NASDAQ-100_companies",
}


class FmpIndexMembershipProvider:
    source_name = "fmp"

    def fetch_memberships(self, index_code: str) -> MembershipDataset:
        definition = _require_definition(index_code)
        rows = fetch_index_constituents(definition.provider_code)
        records = _records_from_rows(rows)
        retrieved_at = datetime.now(timezone.utc)
        return MembershipDataset(
            index_code=definition.code,
            records=records,
            source="fmp:index-constituents",
            source_kind="licensed_provider_endpoint",
            source_page=None,
            resolved_source_title=None,
            source_revision_id=None,
            source_as_of=retrieved_at.date(),
            retrieved_at=retrieved_at,
            parser_version="fmp_index_constituents_v1",
            parsed_count=len(rows),
            rejected_count=max(0, len(rows) - len(records)),
        )


class WikipediaIndexMembershipProvider:
    source_name = "wikipedia"

    def __init__(self, *, timeout_s: int = 20, max_retries: int = 2, session: requests.Session | None = None) -> None:
        self.timeout_s = timeout_s
        self.max_retries = max_retries
        self.session = session or requests.Session()

    def fetch_memberships(self, index_code: str) -> MembershipDataset:
        definition = _require_definition(index_code)
        page = WIKIPEDIA_PAGES[definition.code]
        payload, retrieved_at = self._fetch_parse_payload(page)
        return parse_wikipedia_membership_payload(definition.code, payload, source_page=page, retrieved_at=retrieved_at)

    def _fetch_parse_payload(self, page: str) -> tuple[dict[str, Any], datetime]:
        params = {
            "action": "parse",
            "page": unquote(page),
            "prop": "text|revid|displaytitle",
            "redirects": "1",
            "format": "json",
            "formatversion": "2",
        }
        headers = {"User-Agent": WIKIPEDIA_USER_AGENT}
        last_error: str | None = None
        for attempt in range(self.max_retries + 1):
            retrieved_at = datetime.now(timezone.utc)
            try:
                response = self.session.get(WIKIPEDIA_API_URL, params=params, headers=headers, timeout=self.timeout_s)
            except requests.RequestException as exc:
                last_error = str(exc)
                if attempt >= self.max_retries:
                    raise ValueError(f"wikipedia_request_failed:{last_error}") from exc
                time.sleep(0.5 * (2 ** attempt))
                continue

            if response.status_code in {429, 500, 502, 503, 504} and attempt < self.max_retries:
                time.sleep(0.5 * (2 ** attempt))
                continue
            if response.status_code >= 400:
                raise ValueError(f"wikipedia_http_error:{response.status_code}")

            try:
                payload = response.json()
            except ValueError as exc:
                raise ValueError("wikipedia_invalid_json") from exc
            if not isinstance(payload, dict):
                raise ValueError("wikipedia_invalid_payload")
            return payload, retrieved_at
        raise ValueError(f"wikipedia_request_failed:{last_error or 'unknown'}")


def parse_wikipedia_membership_payload(
    index_code: str,
    payload: dict[str, Any],
    *,
    source_page: str,
    retrieved_at: datetime | None = None,
) -> MembershipDataset:
    definition = _require_definition(index_code)
    if payload.get("error"):
        raise ValueError("wikipedia_api_error")
    parse = payload.get("parse")
    if not isinstance(parse, dict):
        raise ValueError("wikipedia_missing_parse")
    revision_id = parse.get("revid")
    if revision_id is None:
        raise ValueError("wikipedia_missing_revision_id")
    page_title = _clean_text(parse.get("displaytitle") or parse.get("title") or "")
    if not page_title:
        raise ValueError("wikipedia_missing_resolved_title")
    html_text = parse.get("text")
    if not isinstance(html_text, str) or not html_text.strip():
        raise ValueError("wikipedia_missing_html")
    if html_text.lstrip().lower().startswith("<!doctype html"):
        raise ValueError("wikipedia_unexpected_html_document")

    records, parsed_count, rejected_count = _parse_wikipedia_component_table(definition, html_text)
    source_time = retrieved_at or datetime.now(timezone.utc)
    return MembershipDataset(
        index_code=definition.code,
        records=records,
        source="wikipedia",
        source_kind=WIKIPEDIA_SOURCE_KIND,
        source_page=source_page,
        resolved_source_title=page_title,
        source_revision_id=str(revision_id),
        source_as_of=source_time.date(),
        retrieved_at=source_time,
        parser_version=WIKIPEDIA_PARSER_VERSION,
        parsed_count=parsed_count,
        rejected_count=rejected_count,
    )


def active_index_membership_snapshot(db: Session, index_code: str, *, now: datetime | None = None) -> IndexMembershipSnapshot:
    definition = INDEX_UNIVERSES.get(_normalize_index_code(index_code))
    if definition is None:
        return IndexMembershipSnapshot(
            code=_normalize_index_code(index_code),
            symbols=[],
            membership_count=0,
            source=None,
            source_kind=None,
            source_page=None,
            source_revision_id=None,
            resolved_source_title=None,
            parser_version=None,
            source_as_of=None,
            refreshed_at=None,
            status="unavailable",
            reason="unsupported_index",
        )

    rows = db.execute(
        select(IndexMembership)
        .where(IndexMembership.index_code == definition.code, IndexMembership.is_active.is_(True))
        .order_by(IndexMembership.symbol.asc())
    ).scalars().all()
    symbols = [row.symbol for row in rows]
    count = len(symbols)
    source = _single_metadata_value(row.source for row in rows)
    source_kind = _single_metadata_value(getattr(row, "source_kind", None) for row in rows)
    source_page = _single_metadata_value(getattr(row, "source_page", None) for row in rows)
    source_revision_id = _single_metadata_value(getattr(row, "source_revision_id", None) for row in rows)
    resolved_source_title = _single_metadata_value(getattr(row, "resolved_source_title", None) for row in rows)
    parser_version = _single_metadata_value(getattr(row, "parser_version", None) for row in rows)
    source_as_of = max((row.source_as_of for row in rows if row.source_as_of), default=None)
    refreshed_at = max((row.refreshed_at for row in rows if row.refreshed_at), default=None)

    reason = _validation_reason(definition, symbols, source=source, source_as_of=source_as_of, refreshed_at=refreshed_at)
    if reason:
        return IndexMembershipSnapshot(
            code=definition.code,
            symbols=symbols,
            membership_count=count,
            source=source,
            source_kind=source_kind,
            source_page=source_page,
            source_revision_id=source_revision_id,
            resolved_source_title=resolved_source_title,
            parser_version=parser_version,
            source_as_of=source_as_of.isoformat() if source_as_of else None,
            refreshed_at=_dt_iso(refreshed_at),
            status="unavailable",
            reason=reason,
        )

    reference_now = now or datetime.now(timezone.utc)
    status = "available"
    if refreshed_at and reference_now - _aware_utc(refreshed_at) > timedelta(days=INDEX_MEMBERSHIP_STALE_AFTER_DAYS):
        status = "stale"
    return IndexMembershipSnapshot(
        code=definition.code,
        symbols=symbols,
        membership_count=count,
        source=source,
        source_kind=source_kind,
        source_page=source_page,
        source_revision_id=source_revision_id,
        resolved_source_title=resolved_source_title,
        parser_version=parser_version,
        source_as_of=source_as_of.isoformat() if source_as_of else None,
        refreshed_at=_dt_iso(refreshed_at),
        status=status,
    )


def index_universe_capabilities(db: Session) -> dict[str, dict[str, Any]]:
    return {code: _snapshot_capability(active_index_membership_snapshot(db, code)) for code in INDEX_UNIVERSES}


def refresh_index_memberships_from_provider(
    db: Session,
    index_code: str,
    *,
    source: str | None = None,
    dry_run: bool = False,
) -> IndexMembershipRefreshResult:
    definition = _require_definition(index_code)
    provider = _provider_for_source(source or _configured_source(definition.code))
    try:
        dataset = provider.fetch_memberships(definition.code)
    except FMPSubscriptionRestrictedError as exc:
        logger.warning("index_membership_provider_restricted index_code=%s source=%s reason=%s", definition.code, provider.source_name, exc)
        snapshot = active_index_membership_snapshot(db, definition.code)
        return IndexMembershipRefreshResult(
            code=definition.code,
            status="restricted",
            incoming_count=0,
            active_count=snapshot.membership_count,
            source=provider.source_name,
            reason="provider_restricted_non_retryable",
        )
    except (FMPClientError, ValueError, requests.RequestException) as exc:
        logger.warning("index_membership_provider_failed index_code=%s source=%s reason=%s", definition.code, provider.source_name, exc)
        snapshot = active_index_membership_snapshot(db, definition.code)
        return IndexMembershipRefreshResult(
            code=definition.code,
            status="failed",
            incoming_count=0,
            active_count=snapshot.membership_count,
            source=provider.source_name,
            reason=str(exc),
        )

    return refresh_index_memberships_from_dataset(db, dataset, dry_run=dry_run)


def refresh_all_index_memberships_from_provider(
    db: Session,
    *,
    source: str | None = None,
    dry_run: bool = False,
) -> list[IndexMembershipRefreshResult]:
    return [
        refresh_index_memberships_from_provider(db, code, source=source, dry_run=dry_run)
        for code in INDEX_UNIVERSES
    ]


def refresh_index_memberships_from_dataset(
    db: Session,
    dataset: MembershipDataset,
    *,
    dry_run: bool = False,
) -> IndexMembershipRefreshResult:
    definition = _require_definition(dataset.index_code)
    active_rows = db.execute(
        select(IndexMembership)
        .where(IndexMembership.index_code == definition.code, IndexMembership.is_active.is_(True))
        .order_by(IndexMembership.symbol.asc())
    ).scalars().all()
    active_by_symbol = {row.symbol: row for row in active_rows}
    reason = validate_membership_dataset(definition, dataset)
    if reason:
        logger.warning(
            "index_membership_refresh_rejected index_code=%s incoming_count=%s active_count=%s reason=%s source=%s revision=%s",
            definition.code,
            len(dataset.records),
            len(active_by_symbol),
            reason,
            dataset.source,
            dataset.source_revision_id,
        )
        return _refresh_result(
            definition,
            dataset,
            status="rejected",
            active_count=len(active_by_symbol),
            reason=reason,
        )

    incoming = set(dataset.symbols)
    active_symbols = set(active_by_symbol)
    active_revision = _single_metadata_value(getattr(row, "source_revision_id", None) for row in active_rows)
    if active_symbols == incoming and active_revision and active_revision == dataset.source_revision_id:
        return _refresh_result(definition, dataset, status="unchanged", active_count=len(active_by_symbol))

    if dry_run:
        return _refresh_result(definition, dataset, status="dry_run", active_count=len(active_by_symbol))

    inserted = 0
    retained = 0
    end_dated = 0
    try:
        for symbol, active in active_by_symbol.items():
            if symbol in incoming:
                _apply_membership_metadata(active, dataset)
                retained += 1
            else:
                active.effective_to = dataset.source_as_of
                active.is_active = False
                active.refreshed_at = dataset.retrieved_at
                end_dated += 1

        for record in dataset.records:
            if record.symbol in active_by_symbol:
                continue
            db.add(
                IndexMembership(
                    index_code=definition.code,
                    symbol=record.symbol,
                    effective_from=dataset.source_as_of,
                    effective_to=None,
                    source=dataset.source,
                    source_kind=dataset.source_kind,
                    source_page=dataset.source_page,
                    source_revision_id=dataset.source_revision_id,
                    resolved_source_title=dataset.resolved_source_title,
                    parser_version=dataset.parser_version,
                    source_as_of=dataset.source_as_of,
                    refreshed_at=dataset.retrieved_at,
                    is_active=True,
                )
            )
            inserted += 1
        db.commit()
    except Exception:
        db.rollback()
        logger.exception("index_membership_activation_failed index_code=%s source=%s", definition.code, dataset.source)
        raise

    logger.info(
        "index_membership_refresh_complete index_code=%s incoming_count=%s active_count=%s inserted=%s retained=%s end_dated=%s source=%s source_kind=%s source_revision_id=%s source_as_of=%s",
        definition.code,
        len(dataset.records),
        len(incoming),
        inserted,
        retained,
        end_dated,
        dataset.source,
        dataset.source_kind,
        dataset.source_revision_id,
        dataset.source_as_of.isoformat(),
    )
    return _refresh_result(
        definition,
        dataset,
        status="ok",
        active_count=len(incoming),
        inserted_count=inserted,
        retained_count=retained,
        end_dated_count=end_dated,
    )


def refresh_index_memberships(
    db: Session,
    *,
    index_code: str,
    rows: Iterable[Any],
    source: str,
    source_as_of: date,
    refreshed_at: datetime | None = None,
    source_kind: str | None = None,
    source_page: str | None = None,
    source_revision_id: str | None = None,
    resolved_source_title: str | None = None,
    parser_version: str | None = None,
) -> IndexMembershipRefreshResult:
    definition = _require_definition(index_code)
    row_list = list(rows)
    records = _records_from_rows(row_list)
    refreshed_at = refreshed_at or datetime.now(timezone.utc)
    dataset = MembershipDataset(
        index_code=definition.code,
        records=records,
        source=source,
        source_kind=source_kind or "legacy_rows",
        source_page=source_page,
        resolved_source_title=resolved_source_title,
        source_revision_id=source_revision_id,
        source_as_of=source_as_of,
        retrieved_at=refreshed_at,
        parser_version=parser_version or "legacy_rows_v1",
        parsed_count=len(row_list),
        rejected_count=0,
    )
    return refresh_index_memberships_from_dataset(db, dataset)


def validate_membership_dataset(definition: IndexUniverseDefinition, dataset: MembershipDataset) -> str | None:
    symbols = dataset.symbols
    reason = _validation_reason(
        definition,
        symbols,
        source=dataset.source,
        source_as_of=dataset.source_as_of,
        refreshed_at=dataset.retrieved_at,
    )
    if reason:
        return reason
    if len(symbols) != len(set(symbols)):
        return "duplicate_normalized_symbols"
    if dataset.source_kind not in {"legacy_rows", "fixture"}:
        missing_known = [symbol for symbol in definition.required_symbols if symbol not in set(symbols)]
        if missing_known:
            return f"missing_required_symbols:{','.join(missing_known)}"
    sectors = {record.sector for record in dataset.records if record.sector}
    if dataset.source == "wikipedia" and len(sectors) < 3:
        return "insufficient_sector_diversity"
    if dataset.parsed_count <= 0:
        return "empty_parsed_dataset"
    if dataset.rejected_count > max(5, int(dataset.parsed_count * 0.05)):
        return "abnormal_symbol_rejection_rate"
    if dataset.source == "wikipedia":
        if not dataset.source_revision_id:
            return "missing_source_revision_id"
        if dataset.source_kind != WIKIPEDIA_SOURCE_KIND:
            return "incorrect_source_kind"
    return None


def _snapshot_capability(snapshot: IndexMembershipSnapshot) -> dict[str, Any]:
    return {
        "supported": snapshot.supported,
        "membershipCount": snapshot.membership_count,
        "source": snapshot.source,
        "sourceKind": snapshot.source_kind,
        "sourcePage": snapshot.source_page,
        "sourceRevisionId": snapshot.source_revision_id,
        "resolvedSourceTitle": snapshot.resolved_source_title,
        "sourceLabel": "Wikipedia contributors" if snapshot.source == "wikipedia" else snapshot.source,
        "sourceAsOf": snapshot.source_as_of,
        "refreshedAt": snapshot.refreshed_at,
        "parserVersion": snapshot.parser_version,
        "status": snapshot.status,
        "reason": snapshot.reason,
    }


def _records_from_rows(rows: Iterable[Any]) -> list[MembershipRecord]:
    records: list[MembershipRecord] = []
    for row in rows:
        raw_symbol: Any = None
        company_name: str | None = None
        sector: str | None = None
        if isinstance(row, str):
            raw_symbol = row
        elif isinstance(row, dict):
            raw_symbol = row.get("symbol") or row.get("ticker")
            company_name = row.get("name") or row.get("companyName") or row.get("security")
            sector = row.get("sector")
        else:
            raw_symbol = getattr(row, "symbol", None) or getattr(row, "ticker", None)
            company_name = getattr(row, "company_name", None)
            sector = getattr(row, "sector", None)
        symbol = normalize_symbol(raw_symbol)
        if symbol:
            records.append(MembershipRecord(symbol=symbol, raw_symbol=str(raw_symbol), company_name=company_name, sector=sector))
    seen: set[str] = set()
    deduped: list[MembershipRecord] = []
    for record in sorted(records, key=lambda item: item.symbol):
        if record.symbol in seen:
            continue
        seen.add(record.symbol)
        deduped.append(record)
    return deduped


def _parse_wikipedia_component_table(
    definition: IndexUniverseDefinition,
    html_text: str,
) -> tuple[list[MembershipRecord], int, int]:
    try:
        document = html.fromstring(html_text)
    except (TypeError, ValueError) as exc:
        raise ValueError("wikipedia_html_parse_failed") from exc

    for table in document.xpath(".//table"):
        header_cells = table.xpath(".//tr[.//th][1]/th")
        headers = [_normalize_header(cell.text_content()) for cell in header_cells]
        mapping = _component_header_mapping(definition.code, headers)
        if mapping is None:
            continue
        records: list[MembershipRecord] = []
        parsed_count = 0
        rejected_count = 0
        for row in table.xpath(".//tr[td]"):
            cells = row.xpath("./td")
            if len(cells) <= max(mapping.values()):
                continue
            parsed_count += 1
            raw_symbol = _clean_text(cells[mapping["symbol"]].text_content())
            company_name = _clean_text(cells[mapping["company"]].text_content()) if "company" in mapping else None
            sector = _clean_text(cells[mapping["sector"]].text_content()) if "sector" in mapping else None
            sub_industry = _clean_text(cells[mapping["sub_industry"]].text_content()) if "sub_industry" in mapping else None
            symbol = normalize_symbol(raw_symbol)
            if not symbol:
                rejected_count += 1
                continue
            records.append(
                MembershipRecord(
                    symbol=symbol,
                    raw_symbol=raw_symbol,
                    company_name=company_name,
                    sector=sector,
                    sub_industry=sub_industry,
                )
            )
        if not records:
            raise ValueError("wikipedia_component_table_empty")
        if len(records) != len({record.symbol for record in records}):
            raise ValueError("duplicate_normalized_symbols")
        return sorted(records, key=lambda item: item.symbol), parsed_count, rejected_count

    raise ValueError("wikipedia_component_table_not_found")


def _component_header_mapping(index_code: str, headers: list[str]) -> dict[str, int] | None:
    mapping: dict[str, int] = {}
    for idx, header in enumerate(headers):
        if index_code == "sp500":
            if header == "symbol":
                mapping["symbol"] = idx
            elif header == "security":
                mapping["company"] = idx
            elif header == "gics sector":
                mapping["sector"] = idx
            elif header in {"gics sub-industry", "gics sub industry"}:
                mapping["sub_industry"] = idx
        elif index_code == "nasdaq100":
            if header in {"ticker", "symbol"}:
                mapping["symbol"] = idx
            elif header in {"company", "company name", "security"}:
                mapping["company"] = idx
            elif header in {"sector", "gics sector", "industry", "icb industry"}:
                mapping["sector"] = idx
            elif header in {"sub industry", "sub-industry", "icb subsector"}:
                mapping["sub_industry"] = idx
    required = {"symbol", "company", "sector"}
    if index_code == "sp500":
        required.add("sub_industry")
    return mapping if required.issubset(mapping) else None


def _apply_membership_metadata(active: IndexMembership, dataset: MembershipDataset) -> None:
    active.source = dataset.source
    active.source_kind = dataset.source_kind
    active.source_page = dataset.source_page
    active.source_revision_id = dataset.source_revision_id
    active.resolved_source_title = dataset.resolved_source_title
    active.parser_version = dataset.parser_version
    active.source_as_of = dataset.source_as_of
    active.refreshed_at = dataset.retrieved_at


def _refresh_result(
    definition: IndexUniverseDefinition,
    dataset: MembershipDataset,
    *,
    status: str,
    active_count: int,
    inserted_count: int = 0,
    retained_count: int = 0,
    end_dated_count: int = 0,
    reason: str | None = None,
) -> IndexMembershipRefreshResult:
    return IndexMembershipRefreshResult(
        code=definition.code,
        status=status,
        incoming_count=len(dataset.records),
        active_count=active_count,
        inserted_count=inserted_count,
        retained_count=retained_count,
        end_dated_count=end_dated_count,
        parsed_count=dataset.parsed_count,
        accepted_count=len(dataset.records),
        rejected_count=dataset.rejected_count,
        source=dataset.source,
        source_kind=dataset.source_kind,
        source_page=dataset.source_page,
        resolved_source_title=dataset.resolved_source_title,
        source_revision_id=dataset.source_revision_id,
        source_as_of=dataset.source_as_of.isoformat(),
        parser_version=dataset.parser_version,
        reason=reason,
        warnings=dataset.warnings,
    )


def _provider_for_source(source: str) -> IndexMembershipProvider:
    normalized = (source or "").strip().lower().replace("-", "_")
    if normalized in {"wikipedia", "wiki"}:
        return WikipediaIndexMembershipProvider()
    if normalized == "fmp":
        return FmpIndexMembershipProvider()
    raise ValueError(f"unsupported index membership source: {source}")


def _configured_source(index_code: str) -> str:
    key = f"INDEX_MEMBERSHIP_{index_code.upper()}_SOURCE"
    return os.getenv(key, os.getenv("INDEX_MEMBERSHIP_SOURCE", "wikipedia")).strip() or "wikipedia"


def _validation_reason(
    definition: IndexUniverseDefinition,
    symbols: list[str],
    *,
    source: str | None,
    source_as_of: date | None,
    refreshed_at: datetime | None,
) -> str | None:
    if not symbols:
        return "empty_membership"
    if len(symbols) < definition.min_count:
        return "membership_count_too_low"
    if len(symbols) > definition.max_count:
        return "membership_count_too_high"
    if not source:
        return "missing_source"
    if source_as_of is None:
        return "missing_source_as_of"
    if refreshed_at is None:
        return "missing_refreshed_at"
    return None


def _single_metadata_value(values: Iterable[str | None]) -> str | None:
    normalized = sorted({value for value in values if value})
    if not normalized:
        return None
    return normalized[0] if len(normalized) == 1 else "mixed"


def _normalize_index_code(index_code: str | None) -> str:
    return (index_code or "").strip().lower().replace("-", "").replace("_", "")


def _require_definition(index_code: str) -> IndexUniverseDefinition:
    normalized = _normalize_index_code(index_code)
    if normalized == "nasdaq100":
        key = "nasdaq100"
    elif normalized == "sp500":
        key = "sp500"
    else:
        key = normalized
    try:
        return INDEX_UNIVERSES[key]
    except KeyError as exc:
        raise ValueError(f"unsupported index universe: {index_code}") from exc


def _aware_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _dt_iso(value: datetime | None) -> str | None:
    if value is None:
        return None
    return _aware_utc(value).isoformat().replace("+00:00", "Z")


def _normalize_header(value: str) -> str:
    return _clean_text(value).lower().replace("_", " ").replace("-", " ")


def _clean_text(value: Any) -> str:
    text = "" if value is None else str(value)
    text = re.sub(r"<[^>]+>", "", text)
    text = html_unescape(text)
    text = re.sub(r"\[[^\]]+\]", "", text)
    text = text.replace("\xa0", " ")
    return re.sub(r"\s+", " ", text).strip()
