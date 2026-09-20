"""Massive Basic-compatible, on-demand reference/EOD data. No snapshot subscription required."""
from __future__ import annotations

import hashlib
import json
import math
import os
import threading
import time
from datetime import datetime, timezone
from urllib.parse import parse_qs, urlsplit

import requests
from sqlalchemy import select, text

from app.db import SessionLocal
from app.models import TickerContentCache


class OptionsDataError(Exception):
    def __init__(self, message: str, status: int = 503):
        super().__init__(message)
        self.status = status


_lock = threading.Lock()


def _cache_key(path: str, params: dict) -> str:
    return hashlib.sha256(json.dumps([path, params], sort_keys=True).encode()).hexdigest()


def _row(db, key: str):
    return db.scalar(select(TickerContentCache).where(
        TickerContentCache.content_type == "options_calculator",
        TickerContentCache.symbol == "_", TickerContentCache.window_key == key))


def _save(db, key: str, payload: dict, source: str = "massive"):
    row = _row(db, key)
    if row is None:
        row = TickerContentCache(content_type="options_calculator", symbol="_", window_key=key,
                                 cache_key=f"options_calculator:{key}", status="ok", item_count=0, source="massive")
        db.add(row)
    row.payload_json = json.dumps(payload)
    row.source = source
    row.fetched_at = datetime.now(timezone.utc)


def _request(path: str, params: dict) -> dict:
    key = (os.getenv("MASSIVE_API_KEY") or os.getenv("POLYGON_API_KEY") or "").strip()
    if not key:
        raise OptionsDataError("Market data is not configured. You can still enter prices manually.")
    cache_key = _cache_key(path, params)
    # Shared Postgres transaction lock + persisted sliding window protect the free
    # allowance across workers. SQLite development uses the process lock.
    with _lock, SessionLocal() as db:
        if db.bind.dialect.name == "postgresql":
            if not db.scalar(text("SELECT pg_try_advisory_xact_lock(84193627)")):
                raise OptionsDataError("Market data is busy. Try again shortly.", 429)
        now = time.time()
        row = _row(db, cache_key)
        if row is not None:
            stamp = row.fetched_at.replace(tzinfo=timezone.utc) if row.fetched_at.tzinfo is None else row.fetched_at
            if now - stamp.timestamp() < 3600:
                return json.loads(row.payload_json)
        budget_key = "budget:" + hashlib.sha256(key.encode()).hexdigest()[:16]
        budget = _row(db, budget_key)
        hits = [t for t in json.loads(budget.payload_json).get("hits", []) if t > now - 61] if budget else []
        if len(hits) >= 5:
            raise OptionsDataError("The free data allowance is busy (5 requests per minute). Wait a minute, or enter a price manually.", 429)
        _save(db, budget_key, {"hits": [*hits, now]})
        try:
            response = requests.get("https://api.massive.com" + path, params=params,
                                    headers={"Authorization": f"Bearer {key}"}, timeout=8)
            if response.status_code == 429:
                raise OptionsDataError("Massive's data allowance is busy. Please retry in a minute.", 429)
            if response.status_code in (401, 403):
                raise OptionsDataError("This data is unavailable with the configured Massive access. Enter a price manually.")
            if response.status_code != 200:
                raise OptionsDataError("Massive is temporarily unavailable. Enter prices manually or retry later.")
            payload = response.json()
            if not isinstance(payload, dict) or payload.get("status") in ("ERROR", "NOT_AUTHORIZED"):
                raise OptionsDataError("Massive could not return this data. Please try another contract.")
            # Never persist or return next_url: provider URLs may contain credentials.
            cursor = parse_qs(urlsplit(payload.get("next_url") or "").query).get("cursor", [None])[0]
            safe = {"results": payload.get("results", []), "truncated": bool(payload.get("next_url")), "next_cursor": cursor}
            _save(db, cache_key, safe)
            db.commit()
            return safe
        except (requests.RequestException, ValueError):
            db.commit()  # Failed upstream requests also consume the allowance.
            raise OptionsDataError("Market data could not be loaded. You can still enter prices manually.") from None
        except OptionsDataError:
            db.commit()
            raise


def contracts(symbol: str, expiration: str, cursor: str | None = None) -> dict:
    params = {"cursor": cursor, "limit": 1000} if cursor else {"underlying_ticker": symbol,
        "expiration_date": expiration, "expired": "false", "limit": 1000, "sort": "strike_price", "order": "asc"}
    payload = _request("/v3/reference/options/contracts", params)
    items, excluded = [], 0
    for row in payload.get("results") or []:
        if row.get("underlying_ticker", symbol) != symbol or row.get("expiration_date") != expiration:
            continue
        if (row.get("shares_per_contract") != 100 or row.get("additional_underlyings")
                or row.get("contract_type") not in ("call", "put")):
            excluded += 1
            continue
        items.append({"ticker": row["ticker"], "kind": row["contract_type"], "strike": row["strike_price"],
                      "expiration": row["expiration_date"], "exercise_style": row.get("exercise_style", "unknown")})
    # Read all cached closes in one database query, without spending provider requests.
    keys = {_cache_key(f"/v2/aggs/ticker/{item['ticker']}/prev", {"adjusted": "true"}): item for item in items}
    if keys:
        with SessionLocal() as db:
            cached = db.scalars(select(TickerContentCache).where(
                TickerContentCache.content_type == "options_calculator", TickerContentCache.symbol == "_",
                TickerContentCache.window_key.in_(keys)))
            for row in cached:
                stamp = row.fetched_at.replace(tzinfo=timezone.utc) if row.fetched_at.tzinfo is None else row.fetched_at
                if time.time() - stamp.timestamp() >= 3600:
                    continue
                item = keys[row.window_key]
                try:
                    item["close"] = _close(item["ticker"], json.loads(row.payload_json))
                except OptionsDataError:
                    item["no_trade"] = True
    from app.services.options_alpaca import enabled
    if enabled():
        # Let the batch source check contracts with no recent Massive trade, too.
        for item in items:
            item.pop("no_trade", None)
    return {"symbol": symbol, "expiration": expiration, "contracts": items, "excluded": excluded,
            "truncated": payload["truncated"], "next_cursor": payload.get("next_cursor"),
            "price_provider": "alpaca" if enabled() else "massive",
            "source": "Massive contract reference", "price_basis": "reference_with_cached_closes"}


def expirations(symbol: str, spot: float, cursor: str | None = None) -> dict:
    # A narrow strike window discovers dates without downloading every strike for every date.
    params = {"cursor": cursor, "limit": 1000} if cursor else {"underlying_ticker": symbol,
        "expired": "false", "strike_price.gte": round(spot * .975, 2), "strike_price.lte": round(spot * 1.025, 2),
        "limit": 1000, "sort": "expiration_date", "order": "asc"}
    payload = _request("/v3/reference/options/contracts", params)
    dates = sorted({row["expiration_date"] for row in payload.get("results") or []
                    if row.get("shares_per_contract") == 100 and not row.get("additional_underlyings")
                    and row.get("contract_type") in ("call", "put") and row.get("expiration_date")
                    and row.get("underlying_ticker", symbol) == symbol})
    return {"symbol": symbol, "expirations": dates, "truncated": payload["truncated"],
            "next_cursor": payload.get("next_cursor"), "source": "Massive listed expirations near the stock price"}


def previous_close(ticker: str) -> dict:
    from app.services.options_alpaca import enabled, prices
    if ticker.startswith("O:") and enabled():
        result = prices([ticker])
        if result["closes"]:
            return result["closes"][0]
        raise OptionsDataError("No historical trade found in the past seven days. Enter the premium manually.", 404)
    payload = _request(f"/v2/aggs/ticker/{ticker}/prev", {"adjusted": "true"})
    return _close(ticker, payload)


def _close(ticker: str, payload: dict) -> dict:
    rows = payload.get("results") or []
    if not rows:
        raise OptionsDataError("No previous-session trade was available. Enter the premium manually.", 404)
    row = rows[0]
    price, stamp = row.get("c"), row.get("t")
    if not isinstance(price, (int, float)) or not math.isfinite(price) or price < 0 or not stamp:
        raise OptionsDataError("No valid closing price was available. Enter the premium manually.", 404)
    return {"ticker": ticker, "price": price, "as_of": datetime.fromtimestamp(stamp / 1000, timezone.utc).isoformat(),
            "source": "Massive", "price_basis": "previous_session_close"}
