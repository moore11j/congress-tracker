from __future__ import annotations

import os
from typing import Any

import requests

FMP_BASE_URL = "https://financialmodelingprep.com/stable"


class FMPClientError(RuntimeError):
    pass


def _api_key() -> str:
    key = os.getenv("FMP_API_KEY", "").strip()
    if not key:
        raise FMPClientError("Missing FMP_API_KEY")
    return key


def fetch_insider_trades(
    *,
    symbol: str | None = None,
    page: int = 0,
    limit: int = 200,
    timeout_s: int = 30,
) -> list[dict[str, Any]]:
    endpoint = "insider-trading-latest"
    params: dict[str, Any] = {
        "apikey": _api_key(),
        "page": page,
        "limit": limit,
    }
    if symbol:
        params["symbol"] = symbol.upper().strip()

    try:
        response = requests.get(
            f"{FMP_BASE_URL}/{endpoint}",
            params=params,
            timeout=timeout_s,
        )
    except requests.RequestException as exc:
        raise FMPClientError(f"FMP insider API request failed: {exc}") from exc

    if response.status_code in {401, 403}:
        raise FMPClientError(f"FMP insider API auth failed ({response.status_code})")
    if response.status_code == 429:
        raise FMPClientError("FMP insider API rate-limited (429)")
    if response.status_code in {400, 404}:
        return []

    try:
        response.raise_for_status()
    except requests.HTTPError as exc:
        raise FMPClientError(f"FMP insider API error ({response.status_code})") from exc

    data = response.json()
    if isinstance(data, list):
        return [row for row in data if isinstance(row, dict)]
    if isinstance(data, dict):
        rows = data.get("data")
        if isinstance(rows, list):
            return [row for row in rows if isinstance(row, dict)]
    return []
