"""Bounded Finnhub news/recommendation adapter. No prices or premium endpoints.

Recommendation periods are source dates, not ingestion dates. This adapter does
not write canonical analyst history, confirmation scores, or notification events.
"""
from __future__ import annotations

import hashlib
import ipaddress
import os
import re
import time
from datetime import date, datetime, timedelta, timezone
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import requests

from app.utils.symbols import normalize_symbol

BASE_URL = "https://finnhub.io/api/v1"
MAX_BYTES = 2_000_000
MAX_ROWS = 1000


class FinnhubUnavailable(RuntimeError):
    pass


def selected_news_provider() -> str:
    value = (os.getenv("NEWS_PROVIDER") or "fmp").strip().lower()
    if value not in {"fmp", "finnhub"}:
        raise ValueError("Unsupported NEWS_PROVIDER")
    return value


def request_json(path: str, params: dict):
    if path not in {"news", "company-news", "stock/recommendation", "calendar/earnings",
                    "stock/earnings", "stock/metric", "stock/profile2", "stock/peers"}:
        raise ValueError("Unsupported research endpoint")
    key = (os.getenv("FINNHUB_API_KEY") or "").strip()
    if not key:
        raise FinnhubUnavailable("missing_api_key")
    from app.services.finnhub_budget import reserve, cooldown
    reserve()
    try:
        started = time.monotonic()
        # Header auth and no redirects keep the key out of URLs and other hosts.
        with requests.get(f"{BASE_URL}/{path}", params=params,
                          headers={"X-Finnhub-Token": key}, timeout=(3, 12),
                          allow_redirects=False, stream=True) as response:
            if response.status_code != 200:
                if response.status_code == 429:
                    cooldown(response.headers.get('Retry-After'))
                reason = {401: "authentication_failed", 403: "access_denied", 429: "rate_limited"}.get(
                    response.status_code, "provider_unavailable")
                raise FinnhubUnavailable(reason)
            data = bytearray()
            for chunk in response.iter_content(65536):
                data.extend(chunk)
                if len(data) > MAX_BYTES:
                    raise FinnhubUnavailable("response_too_large")
                if time.monotonic() - started > 25:
                    raise FinnhubUnavailable("provider_timeout")
        import json
        rows = json.loads(data)
    except requests.Timeout:
        raise FinnhubUnavailable("provider_timeout") from None
    except (requests.RequestException, ValueError):
        # Never propagate provider bodies, auth headers or exception URLs.
        raise FinnhubUnavailable("invalid_response") from None
    if not isinstance(rows, (dict, list)):
        raise FinnhubUnavailable("invalid_response")
    if len(rows) > MAX_ROWS:
        raise FinnhubUnavailable("response_too_large")
    return rows


def request_rows(path: str, params: dict) -> list[dict]:
    rows = request_json(path, params)
    if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
        raise FinnhubUnavailable("invalid_response")
    return rows


def canonical_news_url(value: object) -> str | None:
    if not isinstance(value, str) or len(value) > 4096:
        return None
    try:
        parts = urlsplit(value.strip())
        host = (parts.hostname or "").lower()
        if parts.scheme not in {"http", "https"} or not host or parts.username or parts.password:
            return None
        if host == "localhost" or "." not in host:
            return None
        try:
            if not ipaddress.ip_address(host).is_global:
                return None
        except ValueError:
            pass
        query = [(k, v) for k, v in parse_qsl(parts.query, keep_blank_values=True)
                 if not k.lower().startswith("utm_") and k.lower() not in {"fbclid", "gclid"}]
        port = parts.port
        netloc = host + (f":{port}" if port and port not in {80, 443} else "")
        return urlunsplit((parts.scheme, netloc, parts.path or "/", urlencode(query), ""))
    except ValueError:
        return None


def _news_symbol_key(symbol: str | None) -> str | None:
    normalized = normalize_symbol(symbol)
    match = re.fullmatch(r'([A-Z]{1,6})[./-]([A-Z])', normalized or '')
    return f'{match[1]}.{match[2]}' if match else normalized


def normalize_news(rows: list[dict], *, observed_at: datetime, symbol: str | None = None) -> dict:
    observed_at = observed_at.astimezone(timezone.utc)
    normalized_symbol = normalize_symbol(symbol) if symbol else None
    from app.services.news_thumbnails import image_url
    by_url: dict[str, dict] = {}
    ids: dict[str, str] = {}
    rejected = 0
    outdated = 0
    for row in rows:
        url = canonical_news_url(row.get("url"))
        title = row.get("headline")
        publisher = row.get("source")
        try:
            raw_stamp = row.get("datetime")
            if isinstance(raw_stamp, bool) or not isinstance(raw_stamp, (int, float)):
                raise ValueError()
            published = datetime.fromtimestamp(raw_stamp, timezone.utc)
            if published > observed_at + timedelta(minutes=5):
                raise ValueError()
        except (ValueError, TypeError, OverflowError, OSError):
            rejected += 1
            continue
        related = {_news_symbol_key(s) for s in str(row.get("related") or "").split(",") if s.strip()}
        if not url or not isinstance(title, str) or not title.strip() or not isinstance(publisher, str) or not publisher.strip():
            rejected += 1
            continue
        if normalized_symbol and related and _news_symbol_key(normalized_symbol) not in related:
            rejected += 1
            continue
        if published < observed_at - timedelta(days=7):
            outdated += 1
            continue
        provider_id = str(row.get("id") or "")
        if provider_id and provider_id in ids and ids[provider_id] != url:
            raise FinnhubUnavailable("conflicting_news_identity")
        ids[provider_id] = url
        item = {
            "title": title.strip(), "url": url, "site": publisher.strip(),
            "source": "finnhub", "provider_id": provider_id or None,
            "identity": hashlib.sha256(url.encode()).hexdigest(),
            "published_at": published.isoformat(), "observed_at": observed_at.isoformat(),
            "symbol": normalized_symbol, "image_url": image_url(row.get("image")), "summary": None,
            "coverage": "publisher_headline_link", "market_read": "neutral",
        }
        # One publisher URL across provider IDs and tracking variants. No full
        # article republication or inferred ticker matching from ambiguous words.
        by_url.setdefault(url, item)
    if rows and not by_url and rejected:
        raise FinnhubUnavailable("no_valid_recent_rows")
    return {"items": sorted(by_url.values(), key=lambda x: (x["published_at"], x["url"]), reverse=True),
            "source": "finnhub", "observed_at": observed_at.isoformat(), "rejected_count": rejected,
            "outdated_count": outdated,
            "coverage": "Recent publisher headlines and source links; not full articles.",
            "status": "ok" if by_url else "empty"}


def fetch_news(*, symbol: str | None = None, category: str = "general", observed_at: datetime | None = None) -> dict:
    now = observed_at or datetime.now(timezone.utc)
    if symbol is not None:
        normalized = normalize_symbol(symbol)
        if not normalized:
            raise ValueError("Invalid symbol")
        rows = request_rows("company-news", {"symbol": _news_symbol_key(normalized),
            "from": (now - timedelta(days=7)).date().isoformat(), "to": now.date().isoformat()})
    else:
        if category not in {"general", "forex", "crypto"}:
            raise ValueError("Invalid news category")
        rows = request_rows("news", {"category": category})
    return normalize_news(rows, observed_at=now, symbol=symbol)


def normalize_recommendations(rows: list[dict], symbol: str, *, observed_at: datetime) -> dict:
    symbol = normalize_symbol(symbol)
    if not symbol:
        raise ValueError("Invalid symbol")
    periods: dict[str, dict] = {}
    for row in rows:
        if normalize_symbol(str(row.get("symbol") or "")) != symbol:
            raise FinnhubUnavailable("recommendation_symbol_mismatch")
        try:
            period = date.fromisoformat(row["period"])
            if period > observed_at.date():
                raise ValueError()
            counts = {key: row[key] for key in ("strongBuy", "buy", "hold", "sell", "strongSell")}
            if any(type(value) is not int or value < 0 for value in counts.values()):
                raise ValueError()
        except (KeyError, ValueError, TypeError):
            raise FinnhubUnavailable("invalid_recommendation") from None
        record = {"period": period.isoformat(), **counts, "total": sum(counts.values())}
        if record["period"] in periods and periods[record["period"]] != record:
            raise FinnhubUnavailable("conflicting_recommendation_period")
        periods[record["period"]] = record
    items = sorted(periods.values(), key=lambda x: x["period"], reverse=True)
    current = items[0] if items else None
    days_old = (observed_at.date() - date.fromisoformat(current["period"])).days if current else None
    return {"symbol": symbol, "source": "finnhub", "observed_at": observed_at.isoformat(),
            "items": items, "current": current, "period_age_days": days_old,
            "status": "empty" if not current else "stale" if days_old > 45 else "ok",
            "coverage": "Recommendation distribution only. Period is provider-reported, not a publication timestamp.",
            "price_targets_available": False, "earnings_estimates_available": False,
            "publication_eligible": False}


def fetch_recommendations(symbol: str, *, observed_at: datetime | None = None) -> dict:
    normalized = normalize_symbol(symbol)
    if not normalized:
        raise ValueError("Invalid symbol")
    return normalize_recommendations(request_rows("stock/recommendation", {"symbol": normalized}),
                                     normalized, observed_at=observed_at or datetime.now(timezone.utc))
