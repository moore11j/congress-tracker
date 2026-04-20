from __future__ import annotations

import os
from datetime import datetime, timezone
from math import isfinite
from typing import Any

import requests

from app.services.options_flow import OptionsFlowObservation, OptionsFlowUnavailable


class MassiveOptionsFlowProvider:
    name = "massive"

    def __init__(self, *, api_key: str | None = None, base_url: str | None = None, timeout: float = 4.0) -> None:
        self.api_key = (api_key or os.getenv("MASSIVE_API_KEY") or "").strip()
        self.base_url = (base_url or os.getenv("MASSIVE_BASE_URL") or "https://api.massive.com").rstrip("/")
        self.timeout = timeout

    def fetch_observations(self, symbol: str, *, lookback_days: int) -> list[OptionsFlowObservation]:
        if not self.api_key:
            raise OptionsFlowUnavailable("missing_api_key")

        params: dict[str, Any] = {
            "limit": 250,
            "sort": "expiration_date",
            "order": "asc",
        }
        rows = self._get_chain_rows(symbol, params=params)
        observations = [_observation_from_snapshot_row(row) for row in rows]
        return [obs for obs in observations if obs is not None]

    def _get_chain_rows(self, symbol: str, *, params: dict[str, Any]) -> list[dict[str, Any]]:
        url = f"{self.base_url}/v3/snapshot/options/{symbol.upper()}"
        rows: list[dict[str, Any]] = []
        next_url: str | None = url
        page_count = 0

        while next_url and page_count < 2 and len(rows) < 500:
            page_params = dict(params) if page_count == 0 else None
            response = requests.get(
                next_url,
                params=page_params,
                headers={"Authorization": f"Bearer {self.api_key}"},
                timeout=self.timeout,
            )
            if response.status_code in {401, 403}:
                raise OptionsFlowUnavailable("provider_unauthorized")
            if response.status_code == 404:
                raise OptionsFlowUnavailable("provider_unsupported")
            if response.status_code == 429:
                raise OptionsFlowUnavailable("provider_rate_limited")
            if response.status_code >= 400:
                raise OptionsFlowUnavailable("provider_error")

            payload = response.json()
            results = payload.get("results")
            if isinstance(results, list):
                rows.extend(row for row in results if isinstance(row, dict))

            raw_next = payload.get("next_url")
            next_url = raw_next if isinstance(raw_next, str) and raw_next.strip() else None
            if next_url and "apiKey=" not in next_url and "apikey=" not in next_url:
                separator = "&" if "?" in next_url else "?"
                next_url = f"{next_url}{separator}apiKey={self.api_key}"
            page_count += 1

        return rows


def _observation_from_snapshot_row(row: dict[str, Any]) -> OptionsFlowObservation | None:
    details = _mapping(row.get("details"))
    day = _mapping(row.get("day"))
    last_trade = _mapping(row.get("last_trade"))
    contract_type = _contract_type(details.get("contract_type") or row.get("contract_type"))
    if contract_type is None:
        return None

    shares_per_contract = _positive_float(details.get("shares_per_contract")) or 100.0
    day_volume = _positive_int(_first_present(day, "volume", "v"))
    day_price = _positive_float(_first_present(day, "vwap", "vw", "close", "c"))
    last_trade_size = _positive_int(_first_present(last_trade, "size", "s"))
    last_trade_price = _positive_float(_first_present(last_trade, "price", "p"))

    if day_volume is not None and day_price is not None:
        premium = day_volume * day_price * shares_per_contract
        contract_volume = day_volume
    elif last_trade_size is not None and last_trade_price is not None:
        premium = last_trade_size * last_trade_price * shares_per_contract
        contract_volume = last_trade_size
    else:
        return None

    observed_at = _timestamp_from_trade(last_trade)
    if observed_at is None:
        observed_at = _timestamp_from_snapshot(row)

    return OptionsFlowObservation(
        contract_type=contract_type,
        premium=premium,
        contract_volume=contract_volume,
        observed_at=observed_at,
    )


def _mapping(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _first_present(mapping: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        value = mapping.get(key)
        if value is not None:
            return value
    return None


def _contract_type(value: Any) -> str | None:
    normalized = str(value or "").strip().lower()
    if normalized in {"call", "put"}:
        return normalized
    return None


def _positive_float(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if not isfinite(parsed) or parsed <= 0:
        return None
    return parsed


def _positive_int(value: Any) -> int | None:
    try:
        parsed = int(float(value))
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def _timestamp_from_trade(trade: dict[str, Any]) -> datetime | None:
    for key in ("sip_timestamp", "participant_timestamp", "timestamp", "t"):
        parsed = _timestamp_to_datetime(trade.get(key))
        if parsed is not None:
            return parsed
    return None


def _timestamp_from_snapshot(row: dict[str, Any]) -> datetime | None:
    for key in ("last_updated", "updated", "fmv_last_updated"):
        parsed = _timestamp_to_datetime(row.get(key))
        if parsed is not None:
            return parsed
    return None


def _timestamp_to_datetime(value: Any) -> datetime | None:
    try:
        raw = float(value)
    except (TypeError, ValueError):
        return None
    if not isfinite(raw) or raw <= 0:
        return None

    if raw > 10_000_000_000_000:
        seconds = raw / 1_000_000_000
    elif raw > 10_000_000_000:
        seconds = raw / 1_000
    else:
        seconds = raw

    try:
        return datetime.fromtimestamp(seconds, tz=timezone.utc)
    except (OverflowError, OSError, ValueError):
        return None
