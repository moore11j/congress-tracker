from __future__ import annotations

import os
from datetime import datetime, timedelta
from typing import Any

import requests
from sqlalchemy.orm import Session

from app.models import TickerMeta
from app.utils.symbols import normalize_symbol

TICKER_META_TTL_DAYS = int(os.getenv("TICKER_META_TTL_DAYS", "7"))
TICKER_META_MISS_TTL_DAYS = int(os.getenv("TICKER_META_MISS_TTL_DAYS", "1"))


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


def _fetch_symbol_meta(symbol: str) -> tuple[str | None, str | None]:
    api_key = _fmp_api_key()
    if not api_key:
        return None, None

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


def get_ticker_meta(db: Session, symbols: list[str]) -> dict[str, dict[str, str | None]]:
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
        for symbol in stale_or_missing:
            company_name, exchange = _fetch_symbol_meta(symbol)
            row = by_symbol.get(symbol)
            if row is None:
                row = TickerMeta(symbol=symbol)
                db.add(row)
                by_symbol[symbol] = row

            row.company_name = company_name
            row.exchange = exchange
            row.updated_at = now

        db.commit()

    return {
        symbol: {
            "company_name": by_symbol[symbol].company_name,
            "exchange": by_symbol[symbol].exchange,
        }
        for symbol in normalized
        if symbol in by_symbol
    }
