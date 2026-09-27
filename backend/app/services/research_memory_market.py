"""Cached ticker context and immutable, explicitly dated thesis baselines."""
from __future__ import annotations

import json
import math
from datetime import datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import ConfirmationScoreSnapshot, PriceCache, QuoteCache, ResearchThesis, ResearchThesisMarketBaseline

NY = ZoneInfo("America/New_York")


def aware(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def number(value):
    if isinstance(value, bool):
        return None
    try:
        result = float(value)
        return result if math.isfinite(result) else None
    except (ValueError, TypeError):
        return None


def close_price(row):
    return number(row.raw_close if row.raw_close is not None else row.close)


def close_time(row):
    return datetime.combine(datetime.fromisoformat(row.date).date(), time(16), NY).astimezone(timezone.utc)


def latest_score(db, security_id, cutoff):
    return db.execute(select(ConfirmationScoreSnapshot).where(
        ConfirmationScoreSnapshot.security_id == security_id,
        ConfirmationScoreSnapshot.calculation_type == "live",
        ConfirmationScoreSnapshot.calculated_at <= cutoff,
        ConfirmationScoreSnapshot.created_at <= cutoff,
    ).order_by(ConfirmationScoreSnapshot.calculated_at.desc(), ConfirmationScoreSnapshot.id.desc()).limit(1)).scalar_one_or_none()


def score_values(row):
    return {"score": row.score if row else None, "direction": row.direction if row else None,
            "score_as_of": aware(row.calculated_at).isoformat() if row else None,
            "methodology_version_id": row.methodology_version_id if row else None,
            "score_snapshot_id": row.id if row else None}


def market_context(db: Session, thesis: ResearchThesis, cutoff: datetime, *, historical=False):
    cutoff = aware(cutoff)
    symbol = thesis.ticker_at_creation
    score = latest_score(db, thesis.security_id, cutoff)
    rows = db.execute(select(PriceCache).where(
        PriceCache.symbol == symbol, PriceCache.date <= cutoff.astimezone(NY).date().isoformat(),
        PriceCache.date >= (cutoff - timedelta(days=10)).date().isoformat(),
    ).order_by(PriceCache.date.desc()).limit(8)).scalars().all()
    closes = [row for row in rows if close_time(row) <= cutoff and (close_price(row) or 0) > 0]
    price, price_at, source = None, None, "unavailable"
    # Only a quote actually cached by the cutoff may enter the baseline.
    quote = db.get(QuoteCache, symbol)
    if quote and cutoff - timedelta(days=7) <= aware(quote.asof_ts) <= cutoff and (number(quote.price) or 0) > 0:
        price, price_at, source = number(quote.price), aware(quote.asof_ts), "cached_quote"
    if historical and score and score.reference_price_at and cutoff - timedelta(days=7) <= aware(score.reference_price_at) <= cutoff and (number(score.reference_price) or 0) > 0:
        if price_at is None or aware(score.reference_price_at) > price_at:
            price, price_at, source = number(score.reference_price), aware(score.reference_price_at), "historical_snapshot"
    if closes and (price_at is None or close_time(closes[0]) > price_at):
        price, price_at, source = close_price(closes[0]), close_time(closes[0]), "historical_close" if historical else "cached_close"
    previous = None
    if price_at and closes:
        market_day = price_at.astimezone(NY).date()
        # On weekends a refreshed quote still describes the last trading session.
        if market_day.weekday() >= 5:
            market_day = market_day - timedelta(days=market_day.weekday() - 4)
        previous = next((close_price(row) for row in closes if row.date < market_day.isoformat()), None)
        split = next((number(row.split_coefficient) for row in closes if row.date == market_day.isoformat()), None)
        if previous and split and split > 0:
            previous /= split
    change = price - previous if price is not None and previous else None
    return {"price": price, "price_as_of": price_at.isoformat() if price_at else None, "price_source": source,
            "day_change": change, "day_change_percent": change / previous * 100 if change is not None else None,
            "is_stale": bool(price_at and cutoff - price_at > timedelta(days=4)), **score_values(score)}


def baseline_context(db, thesis, *, historical):
    return {**market_context(db, thesis, thesis.created_at, historical=historical),
            "kind": "historical" if historical else "captured", "thesis_created_at": aware(thesis.created_at).isoformat()}


def capture_baseline(db: Session, thesis: ResearchThesis, *, historical=False):
    row = db.get(ResearchThesisMarketBaseline, thesis.id)
    if row is None:
        row = ResearchThesisMarketBaseline(thesis_id=thesis.id, payload_json=json.dumps(baseline_context(db, thesis, historical=historical)))
        db.add(row)
    return row


def thesis_market(db: Session, thesis: ResearchThesis, current=None):
    stored = db.get(ResearchThesisMarketBaseline, thesis.id)
    baseline = json.loads(stored.payload_json) if stored else baseline_context(db, thesis, historical=True)
    current = current if current is not None else market_context(db, thesis, datetime.now(timezone.utc))
    base_price, price = baseline.get("price"), current.get("price")
    price_change = percent = None
    reason = "baseline_unavailable" if not base_price else "current_price_unavailable"
    if base_price and price and current.get("price_as_of") and baseline.get("price_as_of"):
        start, end = datetime.fromisoformat(baseline["price_as_of"]), datetime.fromisoformat(current["price_as_of"])
        splits = db.execute(select(PriceCache.date).where(
            PriceCache.symbol == thesis.ticker_at_creation,
            PriceCache.date > start.astimezone(NY).date().isoformat(),
            PriceCache.date <= end.astimezone(NY).date().isoformat(),
            PriceCache.split_coefficient.is_not(None), PriceCache.split_coefficient != 1,
        ).limit(1)).first()
        reason = "split_review_required" if splits else "current_price_predates_baseline" if end < start else None
        if reason is None:
            price_change, percent = price - base_price, (price / base_price - 1) * 100
    same_method = baseline.get("methodology_version_id") is not None and baseline.get("methodology_version_id") == current.get("methodology_version_id")
    score_change = current["score"] - baseline["score"] if same_method and baseline.get("score") is not None and current.get("score") is not None else None
    return {"baseline": baseline, "current": current, "price_change": price_change, "price_change_percent": percent,
            "price_change_unavailable_reason": reason, "score_change": score_change,
            "score_methodology_changed": baseline.get("score") is not None and current.get("score") is not None and not same_method}
