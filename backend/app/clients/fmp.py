from __future__ import annotations

import os
from typing import Any

import requests

from app.services.provider_usage import ensure_fmp_live_allowed, record_provider_response

FMP_BASE_URL = "https://financialmodelingprep.com/stable"


class FMPClientError(RuntimeError):
    pass


class FMPSubscriptionRestrictedError(FMPClientError):
    pass


def _api_key() -> str:
    key = os.getenv("FMP_API_KEY", "").strip()
    if not key:
        raise FMPClientError("Missing FMP_API_KEY")
    return key


def _rows_from_payload(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]
    if isinstance(payload, dict):
        rows = payload.get("data")
        if isinstance(rows, list):
            return [row for row in rows if isinstance(row, dict)]
        if isinstance(rows, dict):
            return [rows]
        return [payload] if payload else []
    return []


def _request_stable_rows(
    endpoint: str,
    *,
    params: dict[str, Any] | None = None,
    category: str,
    symbol: str | None = None,
    timeout_s: int = 30,
) -> list[dict[str, Any]]:
    request_params: dict[str, Any] = {"apikey": _api_key()}
    for key, value in (params or {}).items():
        if value is None:
            continue
        if isinstance(value, str) and not value.strip():
            continue
        request_params[key] = value

    ensure_fmp_live_allowed(category=category, symbol=symbol)
    try:
        response = requests.get(
            f"{FMP_BASE_URL}/{endpoint}",
            params=request_params,
            timeout=timeout_s,
        )
        record_provider_response(category=category, symbol=symbol, status_code=response.status_code)
    except requests.RequestException as exc:
        raise FMPClientError(f"FMP API request failed for {endpoint}: {exc}") from exc

    if response.status_code in {400, 404}:
        return []
    if response.status_code in {401, 403}:
        raise FMPClientError(f"FMP API auth failed ({response.status_code}) for {endpoint}: {response.text[:200]}")
    if response.status_code == 402:
        raise FMPSubscriptionRestrictedError(
            f"FMP API subscription restricted (402) for {endpoint}: {response.text[:200]}"
        )
    if response.status_code == 429:
        raise FMPClientError(f"FMP API rate-limited (429) for {endpoint}")

    try:
        response.raise_for_status()
    except requests.HTTPError as exc:
        raise FMPClientError(f"FMP API error ({response.status_code}) for {endpoint}: {response.text[:200]}") from exc

    try:
        payload = response.json()
    except ValueError as exc:
        raise FMPClientError(f"FMP API returned invalid JSON for {endpoint}") from exc
    return _rows_from_payload(payload)


def fetch_insider_trades(
    *,
    symbol: str | None = None,
    page: int = 0,
    limit: int = 200,
    timeout_s: int = 30,
) -> list[dict[str, Any]]:
    """Fetch insider trades from FMP stable API.

    - If symbol is provided: uses stable insider-trading/search
    - Otherwise: uses stable insider-trading/latest
    """
    params: dict[str, Any] = {
        "apikey": _api_key(),
        "page": page,
        "limit": limit,
    }

    if symbol and symbol.strip():
        endpoint = "insider-trading/search"
        params["symbol"] = symbol.upper().strip()
    else:
        endpoint = "insider-trading/latest"

    category = "ticker:insider-trades" if symbol else "ingest:insider-trades"
    ensure_fmp_live_allowed(category=category, symbol=symbol)
    try:
        response = requests.get(
            f"{FMP_BASE_URL}/{endpoint}",
            params=params,
            timeout=timeout_s,
        )
        record_provider_response(category=category, symbol=symbol, status_code=response.status_code)
    except requests.RequestException as exc:
        raise FMPClientError(f"FMP insider API request failed: {exc}") from exc

    if response.status_code in {401, 403}:
        raise FMPClientError(f"FMP insider API auth failed ({response.status_code}): {response.text[:200]}")
    if response.status_code == 429:
        raise FMPClientError("FMP insider API rate-limited (429)")
    if response.status_code in {400, 404}:
        return []

    try:
        response.raise_for_status()
    except requests.HTTPError as exc:
        raise FMPClientError(f"FMP insider API error ({response.status_code}): {response.text[:200]}") from exc

    data = response.json()
    if isinstance(data, list):
        return [row for row in data if isinstance(row, dict)]
    if isinstance(data, dict):
        rows = data.get("data")
        if isinstance(rows, list):
            return [row for row in rows if isinstance(row, dict)]
    return []


def fetch_institutional_buys(
    *,
    page: int = 0,
    limit: int = 200,
    timeout_s: int = 30,
) -> list[dict[str, Any]]:
    """Fetch institutional holder updates from FMP stable API.

    FMP has changed endpoint naming over time, so we attempt a short list of
    candidate endpoints and return the first successful payload.
    """
    params: dict[str, Any] = {
        "apikey": _api_key(),
        "page": page,
        "limit": limit,
    }

    candidate_endpoints = [
        "institutional-ownership/latest",
        "institutional-holder/latest",
        "institutional-holdings/latest",
    ]

    ensure_fmp_live_allowed(category="ingest:institutional-buys")
    last_error: str | None = None
    for endpoint in candidate_endpoints:
        try:
            response = requests.get(
                f"{FMP_BASE_URL}/{endpoint}",
                params=params,
                timeout=timeout_s,
            )
            record_provider_response(category=f"institutional:{endpoint}", status_code=response.status_code)
        except requests.RequestException as exc:
            last_error = str(exc)
            continue

        if response.status_code in {400, 404}:
            continue
        if response.status_code in {401, 403}:
            raise FMPClientError(
                f"FMP institutional API auth failed ({response.status_code}): {response.text[:200]}"
            )
        if response.status_code == 402:
            raise FMPSubscriptionRestrictedError(
                f"FMP institutional API subscription restricted (402): {response.text[:200]}"
            )
        if response.status_code == 429:
            raise FMPClientError("FMP institutional API rate-limited (429)")

        try:
            response.raise_for_status()
        except requests.HTTPError as exc:
            last_error = f"{response.status_code}: {response.text[:200]}"
            continue

        data = response.json()
        if isinstance(data, list):
            return [row for row in data if isinstance(row, dict)]
        if isinstance(data, dict):
            rows = data.get("data")
            if isinstance(rows, list):
                return [row for row in rows if isinstance(row, dict)]
            return []

    if last_error:
        raise FMPClientError(f"FMP institutional API request failed: {last_error}")
    return []


def fetch_latest_institutional_filings(
    *,
    page: int = 0,
    limit: int = 100,
    timeout_s: int = 30,
) -> list[dict[str, Any]]:
    return _request_stable_rows(
        "institutional-ownership/latest",
        params={"page": page, "limit": limit},
        category="ingest:institutional-ownership:latest",
        timeout_s=timeout_s,
    )


def fetch_institutional_filing_extract(
    *,
    cik: str,
    year: int,
    quarter: int,
    timeout_s: int = 30,
) -> list[dict[str, Any]]:
    return _request_stable_rows(
        "institutional-ownership/extract",
        params={"cik": str(cik).strip(), "year": int(year), "quarter": int(quarter)},
        category="ingest:institutional-ownership:extract",
        timeout_s=timeout_s,
    )


def fetch_institutional_filing_dates(
    *,
    cik: str,
    timeout_s: int = 30,
) -> list[dict[str, Any]]:
    return _request_stable_rows(
        "institutional-ownership/dates",
        params={"cik": str(cik).strip()},
        category="ingest:institutional-ownership:dates",
        timeout_s=timeout_s,
    )


def fetch_symbol_positions_summary(
    *,
    symbol: str,
    year: int,
    quarter: int,
    timeout_s: int = 30,
) -> list[dict[str, Any]]:
    provider_symbol = str(symbol or "").strip().upper()
    return _request_stable_rows(
        "institutional-ownership/symbol-positions-summary",
        params={"symbol": provider_symbol, "year": int(year), "quarter": int(quarter)},
        category="ticker:institutional-ownership:symbol-summary",
        symbol=provider_symbol,
        timeout_s=timeout_s,
    )


def fetch_extract_analytics_by_holder(
    *,
    symbol: str,
    year: int,
    quarter: int,
    page: int = 0,
    limit: int = 10,
    timeout_s: int = 30,
) -> list[dict[str, Any]]:
    provider_symbol = str(symbol or "").strip().upper()
    return _request_stable_rows(
        "institutional-ownership/extract-analytics/holder",
        params={
            "symbol": provider_symbol,
            "year": int(year),
            "quarter": int(quarter),
            "page": int(page),
            "limit": int(limit),
        },
        category="ticker:institutional-ownership:holder-analytics",
        symbol=provider_symbol,
        timeout_s=timeout_s,
    )


def fetch_holder_performance_summary(
    *,
    cik: str,
    page: int = 0,
    timeout_s: int = 30,
) -> list[dict[str, Any]]:
    return _request_stable_rows(
        "institutional-ownership/holder-performance-summary",
        params={"cik": str(cik).strip(), "page": int(page)},
        category="ticker:institutional-ownership:holder-performance",
        timeout_s=timeout_s,
    )


def fetch_holder_industry_breakdown(
    *,
    cik: str,
    year: int,
    quarter: int,
    timeout_s: int = 30,
) -> list[dict[str, Any]]:
    return _request_stable_rows(
        "institutional-ownership/holder-industry-breakdown",
        params={"cik": str(cik).strip(), "year": int(year), "quarter": int(quarter)},
        category="ticker:institutional-ownership:holder-industry",
        timeout_s=timeout_s,
    )


def fetch_industry_summary(
    *,
    year: int,
    quarter: int,
    timeout_s: int = 30,
) -> list[dict[str, Any]]:
    return _request_stable_rows(
        "institutional-ownership/industry-summary",
        params={"year": int(year), "quarter": int(quarter)},
        category="ticker:institutional-ownership:industry-summary",
        timeout_s=timeout_s,
    )


def fetch_company_screener(
    *,
    filters: dict[str, Any] | None = None,
    limit: int = 100,
    timeout_s: int = 30,
) -> list[dict[str, Any]]:
    """Fetch rows from FMP's stable company screener endpoint."""
    bounded_limit = max(1, min(int(limit or 100), 250))
    params: dict[str, Any] = {
        "apikey": _api_key(),
        "limit": bounded_limit,
        "isActivelyTrading": "true",
    }
    for key, value in (filters or {}).items():
        if value is None:
            continue
        if isinstance(value, str) and not value.strip():
            continue
        params[key] = value

    provider_symbol = str((filters or {}).get("symbol") or "").strip().upper() or None
    ensure_fmp_live_allowed(category="screener:company-screener", symbol=provider_symbol)
    try:
        response = requests.get(
            f"{FMP_BASE_URL}/company-screener",
            params=params,
            timeout=timeout_s,
        )
        record_provider_response(category="screener:company-screener", symbol=provider_symbol, status_code=response.status_code)
    except requests.RequestException as exc:
        raise FMPClientError(f"FMP company screener request failed: {exc}") from exc

    if response.status_code in {401, 403}:
        raise FMPClientError(f"FMP company screener auth failed ({response.status_code}): {response.text[:200]}")
    if response.status_code == 429:
        raise FMPClientError("FMP company screener rate-limited (429)")
    if response.status_code in {400, 404}:
        return []

    try:
        response.raise_for_status()
    except requests.HTTPError as exc:
        raise FMPClientError(f"FMP company screener error ({response.status_code}): {response.text[:200]}") from exc

    data = response.json()
    if isinstance(data, list):
        return [row for row in data if isinstance(row, dict)]
    if isinstance(data, dict):
        rows = data.get("data")
        if isinstance(rows, list):
            return [row for row in rows if isinstance(row, dict)]
    return []
