"""Actual historical daily option bars; never indicative snapshots or synthetic quotes."""
from __future__ import annotations

import hashlib
import json
import math
import os
import re
import threading
import time
from datetime import datetime, timedelta, timezone

import requests
from sqlalchemy import text

from app.services.options_calculator import OptionsDataError, SessionLocal, _cache_key, _row, _save

_lock = threading.Lock()
_CONTRACT = re.compile(r"O:[A-Z0-9.\-]{1,10}\d{6}[CP]\d{8}\Z")


def enabled() -> bool:
    # Explicit opt-in after verifying the account's historical-data entitlement.
    return (os.getenv("OPTIONS_PRICE_PROVIDER", "").lower() == "alpaca"
            and bool(os.getenv("APCA_API_KEY_ID")) and bool(os.getenv("APCA_API_SECRET_KEY")))


def _request(params: dict) -> dict:
    key = os.getenv("APCA_API_KEY_ID", "").strip()
    secret = os.getenv("APCA_API_SECRET_KEY", "").strip()
    if not key or not secret:
        raise OptionsDataError("Alpaca historical prices are not configured. Enter premiums manually.")
    account = hashlib.sha256((key + ":" + secret).encode()).hexdigest()[:16]
    cache_key = _cache_key("alpaca:options:bars:v2:" + account, params)
    with _lock, SessionLocal() as db:
        if db.bind.dialect.name == "postgresql" and not db.scalar(text("SELECT pg_try_advisory_xact_lock(84193628)")):
            raise OptionsDataError("Historical prices are busy. Retry shortly.", 429)
        now = time.time()
        row = _row(db, cache_key)
        if row is not None:
            stamp = row.fetched_at.replace(tzinfo=timezone.utc) if row.fetched_at.tzinfo is None else row.fetched_at
            if now - stamp.timestamp() < 3600:
                return json.loads(row.payload_json)
        budget_key = "alpaca-budget:" + hashlib.sha256(key.encode()).hexdigest()[:16]
        budget = _row(db, budget_key)
        hits = [t for t in json.loads(budget.payload_json).get("hits", []) if t > now - 61] if budget else []
        if len(hits) >= 180:
            raise OptionsDataError("Alpaca's shared data allowance is busy. Retry in one minute.", 429)
        _save(db, budget_key, {"hits": [*hits, now]}, source="alpaca")
        try:
            response = requests.get("https://data.alpaca.markets/v1beta1/options/bars", params=params,
                                    headers={"APCA-API-KEY-ID": key, "APCA-API-SECRET-KEY": secret}, timeout=12)
            if response.status_code == 429:
                raise OptionsDataError("Alpaca's data allowance is busy. Retry in one minute.", 429)
            if response.status_code in (401, 403):
                raise OptionsDataError("Alpaca historical access was denied. Check the server's keys and data access, or enter premiums manually.")
            if response.status_code != 200:
                raise OptionsDataError("Alpaca historical prices are unavailable. Retry or enter premiums manually.")
            payload = response.json()
            if not isinstance(payload, dict) or not isinstance(payload.get("bars"), dict):
                raise OptionsDataError("Alpaca returned an incomplete price response. Retry or enter premiums manually.")
            token = payload.get("next_page_token")
            if token is not None and (not isinstance(token, str) or len(token) > 4096):
                raise OptionsDataError("Alpaca returned invalid pagination. Retry later.")
            # Persist only bar fields used by the calculator, never provider messages or URLs.
            allowed = set(params["symbols"].split(","))
            bars = {symbol: [{k: bar.get(k) for k in ("c", "t", "v", "vw", "n")} for bar in values if isinstance(bar, dict)]
                    for symbol, values in payload["bars"].items() if symbol in allowed and isinstance(values, list)}
            safe = {"bars": bars, "next_page_token": token}
            _save(db, cache_key, safe, source="alpaca")
            db.commit()
            return safe
        except (requests.RequestException, ValueError):
            db.commit()
            raise OptionsDataError("Historical prices could not be loaded. Retry or enter premiums manually.") from None
        except OptionsDataError:
            db.commit()
            raise


def prices(tickers: list[str]) -> dict:
    if not 1 <= len(tickers) <= 100 or any(not _CONTRACT.fullmatch(t) for t in tickers):
        raise OptionsDataError("Request between 1 and 100 valid option contracts.", 422)
    # Previous completed US trading sessions only. Exclude today's partial daily
    # bar, and keep the cutoff outside Basic's most recent 15-minute restriction.
    now = datetime.now(timezone.utc)
    end = min(now.replace(hour=0, minute=0, second=0, microsecond=0), now - timedelta(minutes=16))
    start = end - timedelta(days=7)
    tickers = sorted(set(tickers))
    params = {"symbols": ",".join(t[2:] for t in tickers), "timeframe": "1Day",
              "start": start.isoformat(), "end": end.isoformat(), "limit": 10000, "sort": "desc"}
    latest: dict[str, dict] = {}
    seen_tokens: set[str] = set()
    for _ in range(10):
        payload = _request(params)
        for symbol, bars in payload["bars"].items():
            ticker = "O:" + symbol
            if ticker not in tickers:
                continue
            for bar in bars:
                price = bar.get("c")
                try:
                    stamp = datetime.fromisoformat(bar.get("t", "").replace("Z", "+00:00"))
                except (ValueError, TypeError, AttributeError):
                    continue
                if (stamp.tzinfo is None or not start <= stamp < end or isinstance(price, bool)
                        or not isinstance(price, (int, float)) or not math.isfinite(price) or price < 0):
                    continue
                if ticker not in latest or stamp > datetime.fromisoformat(latest[ticker]["as_of"]):
                    latest[ticker] = {"ticker": ticker, "price": price, "as_of": stamp.isoformat(),
                                      "source": "Alpaca", "price_basis": "daily_bar_close"}
        token = payload.get("next_page_token")
        if not token:
            return {"closes": list(latest.values()), "no_trade": [t for t in tickers if t not in latest],
                    "source": "Alpaca", "price_basis": "daily_bar_close", "window_start": start.isoformat(), "window_end": end.isoformat()}
        if token in seen_tokens:
            break
        seen_tokens.add(token)
        params = {**params, "page_token": token}
    # Do not mark unvisited contracts as having no trades on partial/error responses.
    raise OptionsDataError("Historical price pagination did not complete. Retry with fewer contracts.")
