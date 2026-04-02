from __future__ import annotations

import os
import logging
from datetime import datetime, timedelta
from typing import Any

import requests
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.models import CikMeta, TickerMeta
from app.utils.symbols import normalize_symbol

TICKER_META_TTL_DAYS = int(os.getenv("TICKER_META_TTL_DAYS", "7"))
TICKER_META_MISS_TTL_DAYS = int(os.getenv("TICKER_META_MISS_TTL_DAYS", "1"))

CIK_META_TTL_DAYS = int(os.getenv("CIK_META_TTL_DAYS", "30"))
CIK_META_MISS_TTL_DAYS = int(os.getenv("CIK_META_MISS_TTL_DAYS", "7"))

logger = logging.getLogger(__name__)


def _fmp_api_key() -> str | None:
    key = os.getenv("FMP_API_KEY", "").strip()
    return key or None


def _fmp_profile(symbol: str, api_key: str) -> tuple[str | None, str | None]:
    try:
        response = requests.get(
            f"https://financialmodelingprep.com/api/v3/profile/{symbol}",
            params={"apikey": api_key},
            timeout=10,
        )
        if response.status_code != 200:
            return None, None
        payload = response.json()
    except Exception:
        return None, None

    if isinstance(payload, list) and payload and isinstance(payload[0], dict):
        first = payload[0]
        return first.get("companyName"), first.get("exchangeShortName")
    return None, None


def _fmp_search(symbol: str, api_key: str) -> tuple[str | None, str | None]:
    try:
        response = requests.get(
            "https://financialmodelingprep.com/api/v3/search",
            params={"query": symbol, "limit": 10, "apikey": api_key},
            timeout=10,
        )
        if response.status_code != 200:
            return None, None
        payload = response.json()
    except Exception:
        return None, None

    if not isinstance(payload, list):
        return None, None

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
        return None, None

    return exact_match.get("name"), exact_match.get("exchangeShortName") or exact_match.get("exchange")


def _fmp_stable_search_symbol(symbol: str, api_key: str) -> tuple[str | None, str | None]:
    try:
        response = requests.get(
            "https://financialmodelingprep.com/stable/search-symbol",
            params={"query": symbol, "apikey": api_key},
            timeout=10,
        )
        if response.status_code != 200:
            return None, None
        payload = response.json()
    except Exception:
        return None, None

    if not isinstance(payload, list) or not payload:
        return None, None

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
        return None, None

    name = best.get("name") or best.get("companyName")
    exchange = best.get("exchange") or best.get("exchangeShortName") or best.get("stockExchange")
    return name, exchange


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


def _fetch_symbol_meta(symbol: str) -> tuple[str | None, str | None]:
    symbol = normalize_symbol(symbol)
    if not symbol:
        return None, None

    api_key = _fmp_api_key()
    if not api_key:
        logger.warning("ticker_meta: missing FMP_API_KEY")
        return None, None

    company_name, exchange = _fmp_stable_search_symbol(symbol, api_key)
    if company_name:
        return company_name, exchange

    company_name, exchange = _fmp_profile(symbol, api_key)
    if company_name:
        return company_name, exchange

    return _fmp_search(symbol, api_key)


def _ttl_days_for_row(row: TickerMeta) -> int:
    if row.company_name:
        return max(TICKER_META_TTL_DAYS, 1)
    return max(TICKER_META_MISS_TTL_DAYS, 1)


def _is_fresh(row: TickerMeta, now: datetime) -> bool:
    return row.updated_at >= now - timedelta(days=_ttl_days_for_row(row))


def get_ticker_meta(
    db: Session,
    symbols: list[str],
    *,
    allow_refresh: bool = True,
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
                stale_or_missing.append(symbol)
                continue
            if not row.company_name:
                stale_or_missing.append(symbol)
                continue
            if not _is_fresh(row, now):
                stale_or_missing.append(symbol)

        if stale_or_missing and allow_refresh:
            resolved: dict[str, tuple[str | None, str | None]] = {}
            for symbol in stale_or_missing:
                normalized_symbol = normalize_symbol(symbol)
                if not normalized_symbol:
                    continue
                company_name, exchange = _fetch_symbol_meta(normalized_symbol)
                logger.info("ticker_meta resolved symbol=%s has_name=%s", normalized_symbol, bool(company_name))
                resolved[normalized_symbol] = (company_name, exchange)

            rows = [
                {
                    "symbol": symbol,
                    "company_name": company_name,
                    "exchange": exchange,
                    "updated_at": now,
                }
                for symbol, (company_name, exchange) in resolved.items()
            ]

            if rows:
                stmt = sqlite_insert(TickerMeta.__table__).values(rows)
                stmt = stmt.on_conflict_do_update(
                    index_elements=["symbol"],
                    set_={
                        "company_name": stmt.excluded.company_name,
                        "exchange": stmt.excluded.exchange,
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
    return row.updated_at >= now - timedelta(days=_ttl_days_for_cik_row(row))


def get_cik_meta(
    db: Session,
    ciks: list[str],
    *,
    allow_refresh: bool = True,
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
                stale_or_missing.append(cik)

        if stale_or_missing and allow_refresh:
            api_key = _fmp_api_key()
            resolved: dict[str, str | None] = {}
            if api_key:
                for cik in stale_or_missing:
                    company_name = _fmp_search_cik(cik, api_key)
                    logger.info("cik_meta resolved cik=%s has_name=%s", cik, bool(company_name))
                    resolved[cik] = company_name
            else:
                logger.warning("cik_meta: missing FMP_API_KEY")

            rows = [
                {
                    "cik": cik,
                    "company_name": resolved.get(cik),
                    "updated_at": now,
                }
                for cik in stale_or_missing
            ]

            if rows:
                stmt = sqlite_insert(CikMeta.__table__).values(rows)
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
