from __future__ import annotations

import json
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from urllib.parse import parse_qsl, quote, urlparse, urlunparse
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import requests
from sqlalchemy.orm import Session

from app.clients.fmp import FMP_BASE_URL
from app.models import ProviderSetting, ProviderUsageEvent
from app.services.provider_registry import ProviderDomainDefault, provider_domain_catalog, provider_uses_endpoint_url
from app.services.provider_settings import get_provider_settings_by_domain
from app.services.provider_usage import ProviderUnavailable, ensure_fmp_live_allowed, reason_for_status

FMP_ORIGIN = "https://financialmodelingprep.com"
ADMIN_TEST_CATEGORY_PREFIX = "admin-data-source-test"
SYMBOL_PLACEHOLDER_PATTERN = re.compile(r"(\{symbol\}|\[symbol\])", re.IGNORECASE)
DATE_PLACEHOLDER_PATTERN = re.compile(r"(\{date\}|\[date\])", re.IGNORECASE)
DATETIME_PLACEHOLDER_PATTERN = re.compile(r"(\{datetime\}|\[datetime\])", re.IGNORECASE)
DATE_FORMAT_ALIASES = {
    "YYYY-MM-DD": "%Y-%m-%d",
    "YYYY-MM-DD HH:MM:SS": "%Y-%m-%d %H:%M:%S",
}
SYMBOL_ENDPOINT_SUFFIXES = {
    "profile",
    "historical-price-eod/full",
    "historical-price-eod/light",
    "historical-chart/1min",
    "earnings",
    "earnings-calendar",
    "analyst-estimates",
    "income-statement",
    "balance-sheet-statement",
    "cash-flow-statement",
    "ratios",
    "ratios-ttm",
    "key-metrics-ttm",
}


@dataclass(frozen=True)
class FmpEndpointRequest:
    role: str
    provider: str
    endpoint_url: str
    request_url: str
    request_params: dict[str, Any]
    endpoint_name: str
    endpoint_contract: dict[str, Any]


def endpoint_test_category(domain_key: str, role: str) -> str:
    return f"{ADMIN_TEST_CATEGORY_PREFIX}:{domain_key}:{role}"


def _default_endpoint_url(default: ProviderDomainDefault, role: str, provider: str | None) -> str | None:
    if not provider_uses_endpoint_url(provider):
        return None
    return default.primary_endpoint_url if role == "primary" else default.fallback_endpoint_url


def configured_endpoint_url(setting: ProviderSetting, default: ProviderDomainDefault, role: str) -> str | None:
    if role == "primary":
        provider = setting.active_provider
        value = setting.primary_endpoint_url
    else:
        provider = setting.fallback_provider
        value = setting.fallback_endpoint_url
    if not provider_uses_endpoint_url(provider):
        return None
    return value or _default_endpoint_url(default, role, provider)


def endpoint_urls_for_setting(setting: ProviderSetting, default: ProviderDomainDefault) -> dict[str, str | None]:
    return {
        "primary": configured_endpoint_url(setting, default, "primary"),
        "fallback": configured_endpoint_url(setting, default, "fallback"),
    }


def configured_endpoint_contract(setting: ProviderSetting, default: ProviderDomainDefault, role: str) -> str | None:
    if role == "primary":
        provider = setting.active_provider
        value = setting.primary_endpoint_contract_json
        default_value = default.primary_endpoint_contract_json
    else:
        provider = setting.fallback_provider
        value = setting.fallback_endpoint_contract_json
        default_value = default.fallback_endpoint_contract_json
    if not provider_uses_endpoint_url(provider):
        return None
    return value or default_value


def endpoint_contracts_for_setting(setting: ProviderSetting, default: ProviderDomainDefault) -> dict[str, str | None]:
    return {
        "primary": configured_endpoint_contract(setting, default, "primary"),
        "fallback": configured_endpoint_contract(setting, default, "fallback"),
    }


def _coerce_fmp_url(raw_endpoint_url: str) -> str:
    raw = raw_endpoint_url.strip()
    if raw.startswith("http://") or raw.startswith("https://"):
        return raw
    if raw.startswith("/"):
        return f"{FMP_ORIGIN}{raw}"
    return f"{FMP_BASE_URL}/{raw.lstrip('/')}"


def _replace_symbol_placeholders(endpoint_url: str, symbol: str) -> str:
    return SYMBOL_PLACEHOLDER_PATTERN.sub(quote(symbol, safe=""), endpoint_url)


def _python_date_format(value: str | None, fallback: str) -> str:
    if not value:
        return fallback
    return DATE_FORMAT_ALIASES.get(value, value)


def _format_as_of(as_of: datetime, fmt: str) -> str:
    return as_of.strftime(_python_date_format(fmt, fmt))


def _request_as_of(as_of: datetime, request_config: dict[str, Any]) -> datetime:
    timezone_name = request_config.get("timezone")
    if not timezone_name:
        return as_of
    base = as_of if as_of.tzinfo else as_of.replace(tzinfo=timezone.utc)
    try:
        return base.astimezone(ZoneInfo(str(timezone_name)))
    except ZoneInfoNotFoundError:
        return as_of


def _resolve_request_template(value: object, *, symbol: str, as_of: datetime, request_config: dict[str, Any]) -> str:
    text_value = str(value)
    date_format = _python_date_format(str(request_config.get("date_format") or "YYYY-MM-DD"), "%Y-%m-%d")
    datetime_format = _python_date_format(str(request_config.get("datetime_format") or "YYYY-MM-DD HH:MM:SS"), "%Y-%m-%d %H:%M:%S")
    text_value = SYMBOL_PLACEHOLDER_PATTERN.sub(quote(symbol, safe=""), text_value)
    text_value = DATE_PLACEHOLDER_PATTERN.sub(_format_as_of(as_of, date_format), text_value)
    return DATETIME_PLACEHOLDER_PATTERN.sub(_format_as_of(as_of, datetime_format), text_value)


def _endpoint_contract(endpoint_contract_json: str | None) -> dict[str, Any]:
    if not endpoint_contract_json:
        return {}
    try:
        parsed = json.loads(endpoint_contract_json)
    except (TypeError, ValueError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def endpoint_display_name(endpoint_url: str | None) -> str | None:
    if not endpoint_url:
        return None
    rendered = _replace_symbol_placeholders(endpoint_url, "AAPL")
    rendered = DATE_PLACEHOLDER_PATTERN.sub("2026-07-02", rendered)
    rendered = DATETIME_PLACEHOLDER_PATTERN.sub("2026-07-02 15:59:00", rendered)
    parsed = urlparse(_coerce_fmp_url(rendered))
    path = parsed.path.strip("/")
    if path.startswith("stable/"):
        path = path[len("stable/") :]
    query = [(key, value) for key, value in parse_qsl(parsed.query, keep_blank_values=True) if key.lower() not in {"apikey", "api_key"}]
    if query:
        rendered = "&".join(f"{key}={value}" for key, value in query[:4])
        return f"{path}?{rendered}"
    return path or endpoint_url


def _endpoint_accepts_symbol(path: str) -> bool:
    normalized = path.strip("/")
    if normalized.startswith("stable/"):
        normalized = normalized[len("stable/") :]
    return normalized in SYMBOL_ENDPOINT_SUFFIXES


def build_fmp_endpoint_request(
    *,
    role: str,
    provider: str,
    endpoint_url: str,
    api_key: str,
    symbol: str | None = None,
    endpoint_contract_json: str | None = None,
    as_of: datetime | None = None,
) -> FmpEndpointRequest:
    original = endpoint_url.strip()
    had_symbol_template = bool(SYMBOL_PLACEHOLDER_PATTERN.search(original))
    resolved_symbol = (symbol or "").strip().upper()
    contract = _endpoint_contract(endpoint_contract_json)
    request_config = contract.get("request") if isinstance(contract.get("request"), dict) else {}
    request_as_of = _request_as_of(as_of or datetime.now(timezone.utc), request_config)
    replaced = original
    if resolved_symbol:
        replaced = _replace_symbol_placeholders(replaced, resolved_symbol)
    replaced = DATE_PLACEHOLDER_PATTERN.sub(_format_as_of(request_as_of, _python_date_format(str(request_config.get("date_format") or "YYYY-MM-DD"), "%Y-%m-%d")), replaced)
    replaced = DATETIME_PLACEHOLDER_PATTERN.sub(_format_as_of(request_as_of, _python_date_format(str(request_config.get("datetime_format") or "YYYY-MM-DD HH:MM:SS"), "%Y-%m-%d %H:%M:%S")), replaced)
    parsed = urlparse(_coerce_fmp_url(replaced))
    params = {
        key: value
        for key, value in parse_qsl(parsed.query, keep_blank_values=True)
        if key.lower() not in {"apikey", "api_key"}
    }
    contract_params = request_config.get("params") if isinstance(request_config, dict) else None
    if isinstance(contract_params, dict):
        for key, value in contract_params.items():
            if not key:
                continue
            params[str(key)] = _resolve_request_template(value, symbol=resolved_symbol, as_of=request_as_of, request_config=request_config)
    symbol_param = str(request_config.get("symbol_param") or "symbol") if isinstance(request_config, dict) else "symbol"
    if resolved_symbol and (had_symbol_template or symbol_param in params or "symbol" in params or _endpoint_accepts_symbol(parsed.path)):
        params[symbol_param] = resolved_symbol
    params["apikey"] = api_key
    request_url = urlunparse(parsed._replace(query=""))
    return FmpEndpointRequest(
        role=role,
        provider=provider,
        endpoint_url=endpoint_url,
        request_url=request_url,
        request_params=params,
        endpoint_name=endpoint_display_name(endpoint_url) or parsed.path.strip("/") or endpoint_url,
        endpoint_contract=contract,
    )


def fmp_endpoint_requests_for_domain(
    db: Session,
    domain_key: str,
    *,
    symbol: str | None,
    api_key: str,
    include_fallback: bool = True,
) -> list[FmpEndpointRequest]:
    catalog = provider_domain_catalog()
    if domain_key not in catalog:
        raise KeyError(domain_key)
    default = catalog[domain_key]
    setting = get_provider_settings_by_domain(db)[domain_key]
    requests_to_make: list[FmpEndpointRequest] = []
    roles = ("primary", "fallback") if include_fallback else ("primary",)
    seen: set[tuple[str, str]] = set()
    for role in roles:
        provider = setting.active_provider if role == "primary" else setting.fallback_provider
        if provider != "fmp":
            continue
        endpoint_url = configured_endpoint_url(setting, default, role)
        if not endpoint_url:
            continue
        endpoint_contract_json = configured_endpoint_contract(setting, default, role)
        request = build_fmp_endpoint_request(
            role=role,
            provider=provider,
            endpoint_url=endpoint_url,
            api_key=api_key,
            symbol=symbol,
            endpoint_contract_json=endpoint_contract_json,
        )
        dedupe_key = (request.request_url, "&".join(f"{key}={value}" for key, value in sorted(request.request_params.items()) if key != "apikey"))
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        requests_to_make.append(request)
    return requests_to_make


def _record_endpoint_test(
    db: Session,
    *,
    domain_key: str,
    role: str,
    provider: str,
    endpoint_name: str | None,
    symbol: str | None,
    requested_by: str | None,
    status_code: int | str | None,
    duration_ms: float | None,
    success: bool,
    error: str | None,
) -> ProviderUsageEvent:
    row = ProviderUsageEvent(
        provider=provider,
        category=endpoint_test_category(domain_key, role),
        endpoint=endpoint_name,
        symbol=symbol,
        source="admin_endpoint_test",
        route=f"/api/admin/data-sources/test/{domain_key}",
        cache_status=None,
        status_code=str(status_code) if status_code is not None else None,
        duration_ms=duration_ms,
        success=success,
        throttled=str(status_code) == "429",
        error=error,
        created_at=datetime.now(timezone.utc),
    )
    db.add(row)
    return row


def _endpoint_result_from_row(row: ProviderUsageEvent | None) -> dict[str, Any] | None:
    if row is None:
        return None
    status = "healthy" if row.success else "error"
    return {
        "status": status,
        "status_code": row.status_code,
        "error": row.error,
        "endpoint": row.endpoint,
        "tested_at": row.created_at.isoformat() if row.created_at else None,
    }


def test_fmp_endpoint(
    db: Session,
    *,
    domain_key: str,
    role: str,
    endpoint_url: str,
    endpoint_contract_json: str | None = None,
    symbol: str,
    requested_by: str | None,
) -> dict[str, Any]:
    api_key = os.getenv("FMP_API_KEY", "").strip()
    endpoint_name = endpoint_display_name(endpoint_url)
    if not api_key:
        row = _record_endpoint_test(
            db,
            domain_key=domain_key,
            role=role,
            provider="fmp",
            endpoint_name=endpoint_name,
            symbol=symbol,
            requested_by=requested_by,
            status_code=None,
            duration_ms=None,
            success=False,
            error="missing_api_key",
        )
        return _endpoint_result_from_row(row) or {}

    request = build_fmp_endpoint_request(
        role=role,
        provider="fmp",
        endpoint_url=endpoint_url,
        api_key=api_key,
        symbol=symbol,
        endpoint_contract_json=endpoint_contract_json,
    )
    try:
        ensure_fmp_live_allowed(category=endpoint_test_category(domain_key, role), symbol=symbol)
    except ProviderUnavailable as exc:
        row = _record_endpoint_test(
            db,
            domain_key=domain_key,
            role=role,
            provider="fmp",
            endpoint_name=request.endpoint_name,
            symbol=symbol,
            requested_by=requested_by,
            status_code=None,
            duration_ms=None,
            success=False,
            error=getattr(exc, "reason", "provider_unavailable"),
        )
        return _endpoint_result_from_row(row) or {}

    started = time.perf_counter()
    try:
        response = requests.get(request.request_url, params=request.request_params, timeout=10)
        duration_ms = round((time.perf_counter() - started) * 1000, 1)
        success = 200 <= response.status_code < 400
        error = None if success else reason_for_status(response.status_code)
        row = _record_endpoint_test(
            db,
            domain_key=domain_key,
            role=role,
            provider="fmp",
            endpoint_name=request.endpoint_name,
            symbol=symbol,
            requested_by=requested_by,
            status_code=response.status_code,
            duration_ms=duration_ms,
            success=success,
            error=error,
        )
    except requests.RequestException as exc:
        duration_ms = round((time.perf_counter() - started) * 1000, 1)
        row = _record_endpoint_test(
            db,
            domain_key=domain_key,
            role=role,
            provider="fmp",
            endpoint_name=request.endpoint_name,
            symbol=symbol,
            requested_by=requested_by,
            status_code=exc.__class__.__name__,
            duration_ms=duration_ms,
            success=False,
            error="provider_error",
        )
    return _endpoint_result_from_row(row) or {}
