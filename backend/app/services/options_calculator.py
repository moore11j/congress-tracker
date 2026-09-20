"""Massive Basic-compatible, on-demand reference/EOD data. No snapshot subscription required."""
from __future__ import annotations

import hashlib
import json
import math
import os
import threading
import time
from datetime import datetime, timezone

import requests
from sqlalchemy import select, text

from app.db import SessionLocal
from app.models import TickerContentCache


class OptionsDataError(Exception):
    def __init__(self, message: str, status: int = 503):
        super().__init__(message)
        self.status = status


_lock = threading.Lock()


def _row(db, key: str):
    return db.scalar(select(TickerContentCache).where(
        TickerContentCache.content_type == "options_calculator",
        TickerContentCache.symbol == "_", TickerContentCache.window_key == key))


def _save(db, key: str, payload: dict):
    row = _row(db, key)
    if row is None:
        row = TickerContentCache(content_type="options_calculator", symbol="_", window_key=key,
                                 cache_key=f"options_calculator:{key}", status="ok", item_count=0, source="massive")
        db.add(row)
    row.payload_json = json.dumps(payload)
    row.fetched_at = datetime.now(timezone.utc)


def _request(path: str, params: dict) -> dict:
    key = (os.getenv("MASSIVE_API_KEY") or os.getenv("POLYGON_API_KEY") or "").strip()
    if not key:
        raise OptionsDataError("Market data is not configured. You can still enter prices manually.")
    cache_key = hashlib.sha256(json.dumps([path, params], sort_keys=True).encode()).hexdigest()
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
            safe = {"results": payload.get("results", []), "truncated": bool(payload.get("next_url"))}
            _save(db, cache_key, safe)
            db.commit()
            return safe
        except (requests.RequestException, ValueError):
            db.commit()  # Failed upstream requests also consume the allowance.
            raise OptionsDataError("Market data could not be loaded. You can still enter prices manually.") from None
        except OptionsDataError:
            db.commit()
            raise


def contracts(symbol: str, expiration: str) -> dict:
    payload = _request("/v3/reference/options/contracts", {"underlying_ticker": symbol,
        "expiration_date": expiration, "expired": "false", "limit": 1000, "sort": "strike_price", "order": "asc"})
    items, excluded = [], 0
    for row in payload.get("results") or []:
        if (row.get("shares_per_contract") != 100 or row.get("additional_underlyings")
                or row.get("contract_type") not in ("call", "put")):
            excluded += 1
            continue
        items.append({"ticker": row["ticker"], "kind": row["contract_type"], "strike": row["strike_price"],
                      "expiration": row["expiration_date"], "exercise_style": row.get("exercise_style", "unknown")})
    return {"symbol": symbol, "expiration": expiration, "contracts": items, "excluded": excluded,
            "truncated": payload["truncated"], "source": "Massive contract reference", "price_basis": "reference_only"}


def previous_close(ticker: str) -> dict:
    payload = _request(f"/v2/aggs/ticker/{ticker}/prev", {"adjusted": "true"})
    rows = payload.get("results") or []
    if not rows:
        raise OptionsDataError("No previous-session trade was available. Enter the premium manually.", 404)
    row = rows[0]
    price, stamp = row.get("c"), row.get("t")
    if not isinstance(price, (int, float)) or not math.isfinite(price) or price < 0 or not stamp:
        raise OptionsDataError("No valid closing price was available. Enter the premium manually.", 404)
    return {"ticker": ticker, "price": price, "as_of": datetime.fromtimestamp(stamp / 1000, timezone.utc).isoformat(),
            "source": "Massive", "price_basis": "previous_session_close"}
