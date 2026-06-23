from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any

import requests
from sqlalchemy import func
from sqlalchemy.dialects.postgresql import insert as postgres_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.models import CikMeta, TickerMeta
from app.request_priority import get_request_context
from app.services.data_enrichment_queue import enqueue_data_enrichment_job
from app.services.provider_usage import (
    ProviderUnavailable,
    ensure_fmp_live_allowed,
    reason_from_exception,
    record_cache_hit,
    record_cache_miss,
    record_fallback,
    record_provider_response,
)
from app.utils.symbols import normalize_symbol

TICKER_META_TTL_DAYS = int(os.getenv("TICKER_META_TTL_DAYS", "7"))
TICKER_META_MISS_TTL_DAYS = int(os.getenv("TICKER_META_MISS_TTL_DAYS", "1"))
TICKER_META_PLACEHOLDER_VALUES = {"n/a", "na", "none", "null", "unknown", "-", "--"}

CIK_META_TTL_DAYS = int(os.getenv("CIK_META_TTL_DAYS", "30"))
CIK_META_MISS_TTL_DAYS = int(os.getenv("CIK_META_MISS_TTL_DAYS", "7"))

logger = logging.getLogger(__name__)


def _fmp_api_key() -> str | None:
    key = os.getenv("FMP_API_KEY", "").strip()
    return key or None


def _fmp_profile(symbol: str, api_key: str) -> tuple[str | None, str | None, str | None, str | None, str | None]:
    try:
        response = requests.get(
            f"https://financialmodelingprep.com/api/v3/profile/{symbol}",
            params={"apikey": api_key},
            timeout=10,
        )
        record_provider_response(category="ticker_meta:profile", symbol=symbol, status_code=response.status_code)
        if response.status_code != 200:
            return None, None, None, None, None
        payload = response.json()
    except Exception:
        return None, None, None, None, None

    if isinstance(payload, list) and payload and isinstance(payload[0], dict):
        first = payload[0]
        return (
            first.get("companyName") or first.get("name"),
            first.get("exchangeShortName") or first.get("exchange") or first.get("stockExchange"),
            first.get("sector"),
            first.get("industry"),
            first.get("country"),
        )
    return None, None, None, None, None


def _fmp_stable_profile(symbol: str, api_key: str) -> tuple[str | None, str | None, str | None, str | None, str | None]:
    try:
        response = requests.get(
            "https://financialmodelingprep.com/stable/profile",
            params={"symbol": symbol, "apikey": api_key},
            timeout=10,
        )
        record_provider_response(category="ticker_meta:stable-profile", symbol=symbol, status_code=response.status_code)
        if response.status_code != 200:
            return None, None, None, None, None
        payload = response.json()
    except Exception:
        return None, None, None, None, None

    if isinstance(payload, list) and payload and isinstance(payload[0], dict):
        first = payload[0]
    elif isinstance(payload, dict):
        first = payload
    else:
        return None, None, None, None, None

    return (
        first.get("companyName") or first.get("name"),
        first.get("exchangeShortName") or first.get("exchange") or first.get("stockExchange"),
        first.get("sector"),
        first.get("industry"),
        first.get("country"),
    )


def _fmp_search(symbol: str, api_key: str) -> tuple[str | None, str | None, str | None, str | None, str | None]:
    try:
        response = requests.get(
            "https://financialmodelingprep.com/api/v3/search",
            params={"query": symbol, "limit": 10, "apikey": api_key},
            timeout=10,
        )
        record_provider_response(category="ticker_meta:search", symbol=symbol, status_code=response.status_code)
        if response.status_code != 200:
            return None, None, None, None, None
        payload = response.json()
    except Exception:
        return None, None, None, None, None

    if not isinstance(payload, list):
        return None, None, None, None, None

    symbol_upper = symbol.upper()
    exact_match: dict[str, Any] | None = None
    for row in payload:
        if not isinstance(row, dict):
            continue
        row_symbol = normalize_symbol(row.get("symbol"))
        if row_symbol == symbol_upper:
            exact_match = row
            break

    if exact_match is None and payload:
        first = payload[0]
        if isinstance(first, dict):
            exact_match = first

    if not exact_match:
        return None, None, None, None, None

    return (
        exact_match.get("name"),
        exact_match.get("exchangeShortName") or exact_match.get("exchange"),
        None,
        None,
        None,
    )


def _fmp_stable_search_symbol(symbol: str, api_key: str) -> tuple[str | None, str | None, str | None, str | None, str | None]:
    try:
        response = requests.get(
            "https://financialmodelingprep.com/stable/search-symbol",
            params={"query": symbol, "apikey": api_key},
            timeout=10,
        )
        record_provider_response(category="ticker_meta:search-symbol", symbol=symbol, status_code=response.status_code)
        if response.status_code != 200:
            return None, None, None, None, None
        payload = response.json()
    except Exception:
        return None, None, None, None, None

    if not isinstance(payload, list) or not payload:
        return None, None, None, None, None

    wanted = symbol.upper()

    best = None
    for row in payload:
        if not isinstance(row, dict):
            continue
        row_symbol = normalize_symbol(row.get("symbol"))
        if row_symbol and row_symbol.upper() == wanted:
            best = row
            break

    if best is None:
        best = next((row for row in payload if isinstance(row, dict)), None)

    if not best:
        return None, None, None, None, None

    name = best.get("name") or best.get("companyName")
    exchange = best.get("exchange") or best.get("exchangeShortName") or best.get("stockExchange")
    return name, exchange, None, None, None


def normalize_cik(raw: str | None) -> str | None:
    if raw is None:
        return None
    cleaned = "".join(ch for ch in str(raw).strip() if ch.isdigit())
    if not cleaned:
        return None
    return cleaned.zfill(10)


def _fmp_search_cik(cik: str, api_key: str) -> str | None:
    try:
        response = requests.get(
            "https://financialmodelingprep.com/stable/search-cik",
            params={"cik": cik, "apikey": api_key},
            timeout=10,
        )
        record_provider_response(category="ticker_meta:search-cik", symbol=cik, status_code=response.status_code)
        if response.status_code != 200:
            return None
        payload = response.json()
    except Exception:
        return None

    if not isinstance(payload, list) or not payload:
        return None

    first = payload[0]
    if not isinstance(first, dict):
        return None
    return first.get("name") or first.get("companyName")


def debug_stable_search_row(symbol: str) -> dict[str, str | None] | None:
    api_key = _fmp_api_key()
    if not api_key:
        return {"error": "missing_api_key"}

    try:
        response = requests.get(
            "https://financialmodelingprep.com/stable/search-symbol",
            params={"query": symbol, "apikey": api_key},
            timeout=10,
        )
        if response.status_code != 200:
            return {"error": f"status_{response.status_code}"}
        data = response.json()
    except Exception:
        return {"error": "exception"}

    if not isinstance(data, list) or not data:
        return None

    wanted = symbol.upper()
    best = None
    for row in data:
        if isinstance(row, dict) and normalize_symbol(row.get("symbol")) == wanted:
            best = row
            break

    if best is None:
        best = next((r for r in data if isinstance(r, dict)), None)
    if not best:
        return None

    return {
        "symbol": best.get("symbol"),
        "name": best.get("name") or best.get("companyName"),
        "exchange": best.get("exchange") or best.get("exchangeShortName") or best.get("stockExchange"),
    }


def _fetch_symbol_meta(symbol: str) -> tuple[str | None, str | None, str | None, str | None, str | None]:
    symbol = normalize_symbol(symbol)
    if not symbol:
        return None, None, None, None, None

    ensure_fmp_live_allowed(category="ticker_meta", symbol=symbol)
    api_key = _fmp_api_key()
    if not api_key:
        logger.warning("ticker_meta: missing FMP_API_KEY")
        route = str((get_request_context() or {}).get("path") or "")
        user_api_request = route.startswith("/api/") and not route.startswith("/api/admin/")
        raise ProviderUnavailable("provider_disabled" if user_api_request else "background_provider_disabled")

    company_name, exchange, sector, industry, country = _fmp_stable_search_symbol(symbol, api_key)

    profile_name, profile_exchange, profile_sector, profile_industry, profile_country = _fmp_stable_profile(symbol, api_key)
    company_name = company_name or profile_name
    exchange = exchange or profile_exchange
    sector = sector or profile_sector
    industry = industry or profile_industry
    country = country or profile_country

    if not (company_name and exchange and (sector or industry or country)):
        profile_name, profile_exchange, profile_sector, profile_industry, profile_country = _fmp_profile(symbol, api_key)
        company_name = company_name or profile_name
        exchange = exchange or profile_exchange
        sector = sector or profile_sector
        industry = industry or profile_industry
        country = country or profile_country

    if company_name or sector or industry:
        return company_name, exchange, sector, industry, country

    return _fmp_search(symbol, api_key)


def _fetch_cik_meta(cik: str) -> str | None:
    ensure_fmp_live_allowed(category="cik_meta", symbol=cik)
    api_key = _fmp_api_key()
    if not api_key:
        logger.warning("cik_meta: missing FMP_API_KEY")
        raise ProviderUnavailable("provider_disabled")
    return _fmp_search_cik(cik, api_key)


def _is_public_request_context() -> bool:
    context = get_request_context() or {}
    route = str(context.get("path") or "")
    return route.startswith("/api/") and not route.startswith("/api/admin/")


def _ticker_meta_text(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    cleaned = value.strip()
    if not cleaned:
        return None
    if cleaned.lower() in TICKER_META_PLACEHOLDER_VALUES:
        return None
    return cleaned


def _has_ticker_profile_identity(row: TickerMeta) -> bool:
    return bool(
        _ticker_meta_text(row.company_name)
        and _ticker_meta_text(row.exchange)
        and (
            _ticker_meta_text(row.sector)
            or _ticker_meta_text(row.industry)
            or _ticker_meta_text(row.country)
        )
    )


def _enqueue_ticker_meta_refresh(symbol: str, *, reason: str) -> None:
    if not _is_public_request_context():
        return
    enqueue_data_enrichment_job(
        job_type="ticker_meta",
        symbol=symbol,
        source="page_load",
        reason=reason,
        priority=60,
    )


def _enqueue_cik_meta_refresh(cik: str, *, reason: str) -> None:
    if not _is_public_request_context():
        return
    enqueue_data_enrichment_job(
        job_type="cik_meta",
        symbol=None,
        window_key=cik,
        source="page_load",
        reason=reason,
        priority=60,
    )


def _ttl_days_for_row(row: TickerMeta) -> int:
    if row.company_name:
        return max(TICKER_META_TTL_DAYS, 1)
    return max(TICKER_META_MISS_TTL_DAYS, 1)


def _freshness_datetime(value: datetime) -> datetime:
    if value.tzinfo is not None:
        return value.astimezone(timezone.utc).replace(tzinfo=None)
    return value


def _is_fresh(row: TickerMeta, now: datetime) -> bool:
    return _freshness_datetime(row.updated_at) >= _freshness_datetime(now) - timedelta(days=_ttl_days_for_row(row))


def get_ticker_meta(
    db: Session,
    symbols: list[str],
    *,
    allow_refresh: bool = True,
    enqueue_refresh: bool = True,
) -> dict[str, dict[str, str | None]]:
    try:
        normalized = sorted({sym for raw in symbols for sym in [normalize_symbol(raw)] if sym})
        if not normalized:
            return {}

        existing_rows = db.query(TickerMeta).filter(TickerMeta.symbol.in_(normalized)).all()
        by_symbol = {row.symbol: row for row in existing_rows}

        now = datetime.utcnow()
        stale_or_missing: list[str] = []
        for symbol in normalized:
            row = by_symbol.get(symbol)
            if row is None:
                record_cache_miss(category="ticker_meta", symbol=symbol)
                stale_or_missing.append(symbol)
                continue
            if not _ticker_meta_text(row.company_name):
                record_cache_miss(category="ticker_meta", symbol=symbol)
                stale_or_missing.append(symbol)
                continue
            if not _is_fresh(row, now):
                record_cache_miss(category="ticker_meta", symbol=symbol)
                stale_or_missing.append(symbol)
                continue
            if not _has_ticker_profile_identity(row):
                record_cache_miss(category="ticker_meta", symbol=symbol)
                stale_or_missing.append(symbol)
                continue
            record_cache_hit(category="ticker_meta", symbol=symbol)

        if stale_or_missing and enqueue_refresh and _is_public_request_context():
            for symbol in stale_or_missing:
                reason = "missing_profile_identity" if by_symbol.get(symbol) is not None else "cache_miss"
                _enqueue_ticker_meta_refresh(symbol, reason=reason)

        if stale_or_missing and allow_refresh and not _is_public_request_context():
            resolved: dict[str, tuple[str | None, str | None, str | None, str | None, str | None]] = {}
            for symbol in stale_or_missing:
                normalized_symbol = normalize_symbol(symbol)
                if not normalized_symbol:
                    continue
                try:
                    company_name, exchange, sector, industry, country = _fetch_symbol_meta(normalized_symbol)
                    logger.info("ticker_meta resolved symbol=%s has_name=%s", normalized_symbol, bool(company_name))
                    resolved[normalized_symbol] = (company_name, exchange, sector, industry, country)
                except ProviderUnavailable as exc:
                    reason = reason_from_exception(exc)
                    record_fallback(category="ticker_meta", symbol=normalized_symbol, reason=reason)
                    _enqueue_ticker_meta_refresh(normalized_symbol, reason=reason)
                except Exception:
                    record_fallback(category="ticker_meta", symbol=normalized_symbol, reason="provider_unavailable")
                    _enqueue_ticker_meta_refresh(normalized_symbol, reason="provider_unavailable")

            rows = [
                {
                    "symbol": symbol,
                    "company_name": company_name,
                    "exchange": exchange,
                    "sector": sector,
                    "industry": industry,
                    "country": country,
                    "updated_at": now,
                }
                for symbol, (company_name, exchange, sector, industry, country) in resolved.items()
            ]

            if rows:
                for row in rows:
                    logger.info(
                        "ticker_meta_identity_upsert symbol=%s has_name=%s has_sector=%s has_industry=%s has_exchange=%s",
                        row["symbol"],
                        bool(row.get("company_name")),
                        bool(row.get("sector")),
                        bool(row.get("industry")),
                        bool(row.get("exchange")),
                    )
                insert_fn = postgres_insert if db.get_bind().dialect.name == "postgresql" else sqlite_insert
                stmt = insert_fn(TickerMeta.__table__).values(rows)
                stmt = stmt.on_conflict_do_update(
                    index_elements=["symbol"],
                    set_={
                        "company_name": func.coalesce(stmt.excluded.company_name, TickerMeta.__table__.c.company_name),
                        "exchange": func.coalesce(stmt.excluded.exchange, TickerMeta.__table__.c.exchange),
                        "sector": func.coalesce(stmt.excluded.sector, TickerMeta.__table__.c.sector),
                        "industry": func.coalesce(stmt.excluded.industry, TickerMeta.__table__.c.industry),
                        "country": func.coalesce(stmt.excluded.country, TickerMeta.__table__.c.country),
                        "updated_at": stmt.excluded.updated_at,
                    },
                )

                try:
                    logger.info("ticker_meta upsert rows=%d", len(rows))
                    db.execute(stmt)
                    db.commit()
                except IntegrityError:
                    db.rollback()
                except Exception:
                    db.rollback()

                existing_rows = db.query(TickerMeta).filter(TickerMeta.symbol.in_(normalized)).all()
                by_symbol = {row.symbol: row for row in existing_rows}

        return {
            symbol: {
                "company_name": by_symbol[symbol].company_name,
                "exchange": by_symbol[symbol].exchange,
                "sector": by_symbol[symbol].sector,
                "industry": by_symbol[symbol].industry,
                "country": by_symbol[symbol].country,
            }
            for symbol in normalized
            if symbol in by_symbol
        }
    except Exception:
        db.rollback()
        logger.exception("ticker_meta enrichment failed")
        return {}


def _ttl_days_for_cik_row(row: CikMeta) -> int:
    if row.company_name:
        return max(CIK_META_TTL_DAYS, 1)
    return max(CIK_META_MISS_TTL_DAYS, 1)


def _is_cik_row_fresh(row: CikMeta, now: datetime) -> bool:
    return _freshness_datetime(row.updated_at) >= _freshness_datetime(now) - timedelta(days=_ttl_days_for_cik_row(row))


def get_cik_meta(
    db: Session,
    ciks: list[str],
    *,
    allow_refresh: bool = True,
    enqueue_refresh: bool = True,
) -> dict[str, str | None]:
    try:
        normalized = sorted({cik for raw in ciks for cik in [normalize_cik(raw)] if cik})
        if not normalized:
            return {}

        existing_rows = db.query(CikMeta).filter(CikMeta.cik.in_(normalized)).all()
        by_cik = {row.cik: row for row in existing_rows}

        now = datetime.utcnow()
        stale_or_missing: list[str] = []
        for cik in normalized:
            row = by_cik.get(cik)
            if row is None or not _is_cik_row_fresh(row, now):
                record_cache_miss(category="cik_meta", symbol=cik)
                stale_or_missing.append(cik)
            else:
                record_cache_hit(category="cik_meta", symbol=cik)

        if stale_or_missing and enqueue_refresh and _is_public_request_context():
            for cik in stale_or_missing:
                _enqueue_cik_meta_refresh(cik, reason="cache_miss")

        if stale_or_missing and allow_refresh:
            resolved: dict[str, str | None] = {}
            for cik in stale_or_missing:
                try:
                    company_name = _fetch_cik_meta(cik)
                    logger.info("cik_meta resolved cik=%s has_name=%s", cik, bool(company_name))
                    resolved[cik] = company_name
                except ProviderUnavailable as exc:
                    reason = reason_from_exception(exc)
                    record_fallback(category="cik_meta", symbol=cik, reason=reason)
                    _enqueue_cik_meta_refresh(cik, reason=reason)
                except Exception:
                    record_fallback(category="cik_meta", symbol=cik, reason="provider_unavailable")
                    _enqueue_cik_meta_refresh(cik, reason="provider_unavailable")

            rows = [
                {
                    "cik": cik,
                    "company_name": company_name,
                    "updated_at": now,
                }
                for cik, company_name in resolved.items()
            ]

            if rows:
                insert_fn = postgres_insert if db.get_bind().dialect.name == "postgresql" else sqlite_insert
                stmt = insert_fn(CikMeta.__table__).values(rows)
                stmt = stmt.on_conflict_do_update(
                    index_elements=["cik"],
                    set_={
                        "company_name": stmt.excluded.company_name,
                        "updated_at": stmt.excluded.updated_at,
                    },
                )

                try:
                    logger.info("cik_meta upsert rows=%d", len(rows))
                    db.execute(stmt)
                    db.commit()
                except IntegrityError:
                    db.rollback()
                except Exception:
                    db.rollback()

                existing_rows = db.query(CikMeta).filter(CikMeta.cik.in_(normalized)).all()
                by_cik = {row.cik: row for row in existing_rows}

        return {cik: by_cik[cik].company_name for cik in normalized if cik in by_cik}
    except Exception:
        db.rollback()
        logger.exception("cik_meta enrichment failed")
        return {}
