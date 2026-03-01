from __future__ import annotations

import os
import logging
from datetime import datetime, timedelta
from typing import Any

import requests
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.models import TickerMeta
from app.utils.symbols import normalize_symbol

TICKER_META_TTL_DAYS = int(os.getenv("TICKER_META_TTL_DAYS", "7"))
TICKER_META_MISS_TTL_DAYS = int(os.getenv("TICKER_META_MISS_TTL_DAYS", "1"))

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
        row_sym = normalize_symbol(row.get("symbol"))
        if row_sym and row_sym.upper() == wanted:
            best = row
            break

    if best is None:
        for row in payload:
            if isinstance(row, dict):
                best = row
                break

    if not best:
        return None, None

    name = best.get("name") or best.get("companyName")
    exchange = best.get("exchange") or best.get("exchangeShortName") or best.get("stockExchange")
    return name, exchange


def _fetch_symbol_meta(symbol: str) -> tuple[str | None, str | None]:
    symbol = normalize_symbol(symbol)
    if not symbol:
        return None, None

    api_key = _fmp_api_key()
    if not api_key:
        return None, None

    company_name, exchange = _fmp_stable_search_symbol(symbol, api_key)
    if company_name:
        logger.info("ticker_meta fetched: symbol=%s name=%s", symbol, bool(company_name))
        return company_name, exchange

    company_name, exchange = _fmp_profile(symbol, api_key)
    if company_name:
        logger.info("ticker_meta fetched: symbol=%s name=%s", symbol, bool(company_name))
        return company_name, exchange

    company_name, exchange = _fmp_search(symbol, api_key)
    logger.info("ticker_meta fetched: symbol=%s name=%s", symbol, bool(company_name))
    return company_name, exchange


def _ttl_days_for_row(row: TickerMeta) -> int:
    if row.company_name:
        return max(TICKER_META_TTL_DAYS, 1)
    return max(TICKER_META_MISS_TTL_DAYS, 1)


def _is_fresh(row: TickerMeta, now: datetime) -> bool:
    return row.updated_at >= now - timedelta(days=_ttl_days_for_row(row))


def get_ticker_meta(db: Session, symbols: list[str]) -> dict[str, dict[str, str | None]]:
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
            if row is None or not _is_fresh(row, now):
                stale_or_missing.append(symbol)

        if stale_or_missing:
            resolved: dict[str, tuple[str | None, str | None]] = {}
            for symbol in stale_or_missing:
                normalized_symbol = normalize_symbol(symbol)
                if not normalized_symbol:
                    continue
                company_name, exchange = _fetch_symbol_meta(normalized_symbol)
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
