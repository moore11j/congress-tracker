"""Exact-session reference prices for daily percentage alerts; never prior-row fallback."""
from __future__ import annotations

import json
import logging
import math
from datetime import datetime, timedelta, timezone
from time import monotonic
from zoneinfo import ZoneInfo

from sqlalchemy import select

from app.models import PriceCache, QuoteCache, Security, UserAccount, Watchlist, WatchlistAlertRule, WatchlistItem
from app.services.price_lookup import is_market_trading_day, previous_market_trading_day, refresh_recent_price_history

logger = logging.getLogger(__name__)
MARKET_TZ = ZoneInfo("America/New_York")


def _positive(value):
    try:
        number = float(value)
        return number if math.isfinite(number) and number > 0 else None
    except (TypeError, ValueError):
        return None


def reference_close(row):
    if row is None:
        return None
    return _positive(row.adjusted_close if row.adjusted_close is not None else row.close)


def daily_price_observation(db, ticker: str, now: datetime) -> dict:
    current = now if now.tzinfo else now.replace(tzinfo=timezone.utc)
    session = current.astimezone(MARKET_TZ).date()
    reference_date = previous_market_trading_day(session).isoformat()
    evidence = {"session_date": session.isoformat(), "reference_date": reference_date,
                "reference_price": None, "current_price": None, "observed_at": None,
                "change_pct": None, "status": "missing_reference_close"}
    if not is_market_trading_day(session):
        return {**evidence, "status": "not_trading_day"}
    prior = reference_close(db.get(PriceCache, (ticker, reference_date)))
    if prior is None:
        return evidence
    evidence["reference_price"] = prior
    quote = db.get(QuoteCache, ticker)
    asof = quote.asof_ts if quote is not None else None
    if asof is not None and asof.tzinfo is None:
        asof = asof.replace(tzinfo=timezone.utc)
    price = _positive(quote.price) if quote is not None else None
    if price is not None and asof is not None and current - timedelta(minutes=30) <= asof <= current and asof.astimezone(MARKET_TZ).date() == session:
        evidence.update(current_price=price, observed_at=asof.isoformat(), current_source="quote")
    else:
        price = reference_close(db.get(PriceCache, (ticker, session.isoformat())))
        if price is None:
            return {**evidence, "status": "missing_current_price"}
        evidence.update(current_price=price, observed_at=current.isoformat(), current_source="daily_bar")
    return {**evidence, "status": "ok", "change_pct": (price / prior - 1) * 100}


def valid_daily_price_evidence(evidence, occurred_at) -> bool:
    if not isinstance(evidence, dict) or evidence.get("status") != "ok":
        return False
    session = occurred_at.astimezone(MARKET_TZ).date()
    prior = _positive(evidence.get("reference_price"))
    current = _positive(evidence.get("current_price"))
    try:
        change = float(evidence.get("change_pct"))
    except (TypeError, ValueError):
        return False
    return bool(prior and current and math.isfinite(change)
                and evidence.get("session_date") == session.isoformat()
                and evidence.get("reference_date") == previous_market_trading_day(session).isoformat()
                and math.isclose(change, (current / prior - 1) * 100, abs_tol=1e-8))


def refresh_daily_price_references(session_factory, *, now=None, symbols=None, max_fetches=20, max_seconds=60) -> dict:
    current = now or datetime.now(timezone.utc)
    session = current.astimezone(MARKET_TZ).date()
    expected = previous_market_trading_day(session).isoformat()
    if symbols is None:
        with session_factory() as db:
            rows = db.execute(select(WatchlistAlertRule, Security.symbol)
                .join(Watchlist, Watchlist.id == WatchlistAlertRule.watchlist_id)
                .join(UserAccount, UserAccount.id == WatchlistAlertRule.user_id)
                .join(WatchlistItem, WatchlistItem.watchlist_id == Watchlist.id)
                .join(Security, Security.id == WatchlistItem.security_id)
                .where(WatchlistAlertRule.enabled.is_(True), UserAccount.is_suspended.is_(False),
                       Watchlist.owner_user_id == WatchlistAlertRule.user_id)).all()
            symbols = set()
            for rule, symbol in rows:
                try:
                    conditions = json.loads(rule.conditions_json or "[]")
                except (TypeError, ValueError):
                    logger.warning("price_alert_reference_invalid_rule rule_id=%s", rule.id)
                    continue
                if not isinstance(conditions, list) or not conditions or not all(isinstance(c, dict) and c.get("metric") == "price_change_pct" and c.get("time_window") == {"value": 1, "unit": "day"} for c in conditions):
                    continue
                if rule.scope_type == "specific_ticker" and (rule.scope_ticker or "").upper() != symbol.upper():
                    continue
                symbols.add(symbol.upper())
    started = monotonic()
    result = {"reference_date": expected, "checked": 0, "fetched": 0, "available": 0, "unavailable": 0}
    for ticker in sorted(set(symbols)):
        with session_factory() as db:
            result["checked"] += 1
            available = reference_close(db.get(PriceCache, (ticker, expected))) is not None
            if not available and result["fetched"] < max_fetches and monotonic() - started < max_seconds:
                result["fetched"] += 1
                try:
                    # This API caches bars under their actual provider dates.
                    # Do not use get_eod_close: its generic prior-date fallback
                    # can relabel an older close with the requested date.
                    db.rollback()
                    refresh_recent_price_history(db, ticker, lookback_days=1, end_date=expected)
                    db.expire_all()
                    available = reference_close(db.get(PriceCache, (ticker, expected))) is not None
                except Exception:
                    db.rollback()
                    logger.exception("price_alert_reference_refresh_failed ticker=%s date=%s", ticker, expected)
            result["available" if available else "unavailable"] += 1
            if not available:
                logger.warning("price_alert_reference_unavailable ticker=%s required_date=%s", ticker, expected)
    return result
