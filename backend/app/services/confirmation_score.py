from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from math import isfinite
from typing import Literal

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.models import Event, PriceCache
from app.services.event_activity_filters import insider_visibility_clause
from app.services.price_lookup import get_eod_close_series
from app.services.signal_freshness import slim_signal_freshness_bundle
from app.services.signal_score import calculate_smart_score
from app.services.why_now import slim_why_now_bundle

ConfirmationDirection = Literal["bullish", "bearish", "neutral", "mixed"]
ConfirmationBand = Literal["inactive", "weak", "moderate", "strong", "exceptional"]
ConfirmationSourceKey = Literal["congress", "insiders", "signals", "price_volume"]

BUY_TRADE_TYPES = {"purchase", "buy", "p-purchase"}
SELL_TRADE_TYPES = {"sale", "sell", "s-sale"}
SIGNAL_DEFAULTS = {
    "congress_trade": {
        "baseline_days": 365,
        "multiple": 1.75,
        "min_amount": 10_000,
        "min_baseline_count": 3,
    },
    "insider_trade": {
        "baseline_days": 365,
        "multiple": 1.5,
        "min_amount": 10_000,
        "min_baseline_count": 3,
    },
}


@dataclass(frozen=True)
class ConfirmationSourceSummary:
    present: bool
    direction: ConfirmationDirection
    strength: int
    quality: int
    freshness_days: int | None
    label: str

    def as_dict(self) -> dict:
        return {
            "present": self.present,
            "direction": self.direction,
            "strength": self.strength,
            "quality": self.quality,
            "freshness_days": self.freshness_days,
            "label": self.label,
        }


@dataclass(frozen=True)
class ConfirmationScoreBundle:
    ticker: str
    lookback_days: int
    score: int
    band: ConfirmationBand
    direction: ConfirmationDirection
    status: str
    explanation: str
    sources: dict[ConfirmationSourceKey, ConfirmationSourceSummary]
    drivers: list[str]

    def as_dict(self) -> dict:
        return {
            "ticker": self.ticker,
            "lookback_days": self.lookback_days,
            "score": self.score,
            "band": self.band,
            "direction": self.direction,
            "status": self.status,
            "explanation": self.explanation,
            "sources": {key: value.as_dict() for key, value in self.sources.items()},
            "drivers": list(self.drivers),
        }


def confirmation_band_for_score(score: int) -> ConfirmationBand:
    if score <= 19:
        return "inactive"
    if score <= 39:
        return "weak"
    if score <= 59:
        return "moderate"
    if score <= 79:
        return "strong"
    return "exceptional"


def inactive_confirmation_score_bundle(ticker: str, *, lookback_days: int = 30) -> dict:
    return _empty_bundle(ticker.strip().upper(), lookback_days).as_dict()


def confirmation_active_source_count(bundle: dict) -> int:
    """Count active directional sources from a canonical confirmation bundle."""
    sources = bundle.get("sources") if isinstance(bundle, dict) else None
    if not isinstance(sources, dict):
        return 0

    count = 0
    for source in sources.values():
        if not isinstance(source, dict):
            continue
        if source.get("present") is True and source.get("direction") != "neutral":
            count += 1
    return count


def slim_confirmation_score_bundle(bundle: dict) -> dict:
    score = bundle.get("score") if isinstance(bundle, dict) else 0
    try:
        score_int = _clamp_int(float(score))
    except (TypeError, ValueError):
        score_int = 0

    band = bundle.get("band") if isinstance(bundle, dict) else None
    if band not in {"inactive", "weak", "moderate", "strong", "exceptional"}:
        band = confirmation_band_for_score(score_int)

    direction = bundle.get("direction") if isinstance(bundle, dict) else None
    if direction not in {"bullish", "bearish", "neutral", "mixed"}:
        direction = "neutral"

    source_count = confirmation_active_source_count(bundle)
    drivers = bundle.get("drivers") if isinstance(bundle, dict) else None
    first_driver = next((driver for driver in drivers if isinstance(driver, str) and driver.strip()), None) if isinstance(drivers, list) else None
    explanation = bundle.get("explanation") if isinstance(bundle, dict) else None

    return {
        "confirmation_score": score_int,
        "confirmation_band": band,
        "confirmation_direction": direction,
        "confirmation_status": bundle.get("status") if isinstance(bundle, dict) and isinstance(bundle.get("status"), str) else "Inactive",
        "confirmation_source_count": source_count,
        "confirmation_explanation": first_driver or (explanation if isinstance(explanation, str) else None),
        "is_multi_source": source_count >= 2,
        "why_now": slim_why_now_bundle(bundle),
        "signal_freshness": slim_signal_freshness_bundle(bundle),
    }


def get_slim_confirmation_score_bundles_for_tickers(
    db: Session,
    tickers: list[str],
    *,
    lookback_days: int = 30,
) -> dict[str, dict]:
    bundles = get_confirmation_score_bundles_for_tickers(db, tickers, lookback_days=lookback_days)
    return {
        symbol: slim_confirmation_score_bundle(bundle)
        for symbol, bundle in bundles.items()
    }


def get_confirmation_score_bundles_for_tickers(
    db: Session,
    tickers: list[str],
    *,
    lookback_days: int = 30,
    benchmark_symbol: str = "^GSPC",
) -> dict[str, dict]:
    symbols = sorted({(ticker or "").strip().upper() for ticker in tickers if (ticker or "").strip()})
    if not symbols:
        return {}

    bounded_lookback = max(1, min(int(lookback_days or 30), 365))
    now = datetime.now(timezone.utc)

    try:
        congress_sources = _trade_activity_sources(db, symbols, "congress_trade", bounded_lookback, now)
    except Exception:
        congress_sources = {}
    try:
        insider_sources = _trade_activity_sources(db, symbols, "insider_trade", bounded_lookback, now)
    except Exception:
        insider_sources = {}
    try:
        signal_sources = _signals_sources(db, symbols, bounded_lookback, now)
    except Exception:
        signal_sources = {}
    try:
        price_sources = _price_volume_sources(db, symbols, benchmark_symbol, bounded_lookback, now)
    except Exception:
        price_sources = {}

    results: dict[str, dict] = {}
    for symbol in symbols:
        sources: dict[ConfirmationSourceKey, ConfirmationSourceSummary] = {
            "congress": congress_sources.get(symbol, _empty_source("Inactive")),
            "insiders": insider_sources.get(symbol, _empty_source("Inactive")),
            "signals": signal_sources.get(symbol, _empty_source("No current smart signal")),
            "price_volume": price_sources.get(symbol, _empty_source("No price confirmation")),
        }
        results[symbol] = _score_bundle(symbol, bounded_lookback, sources).as_dict()
    return results


def get_confirmation_score_bundle_for_ticker(
    db: Session,
    ticker: str,
    *,
    lookback_days: int = 30,
    benchmark_symbol: str = "^GSPC",
) -> dict:
    symbol = (ticker or "").strip().upper()
    if not symbol:
        return inactive_confirmation_score_bundle(ticker, lookback_days=lookback_days)

    bounded_lookback = max(1, min(int(lookback_days or 30), 365))
    now = datetime.now(timezone.utc)

    sources: dict[ConfirmationSourceKey, ConfirmationSourceSummary] = {
        "congress": _safe_source(lambda: _trade_activity_source(db, symbol, "congress_trade", bounded_lookback, now)),
        "insiders": _safe_source(lambda: _trade_activity_source(db, symbol, "insider_trade", bounded_lookback, now)),
        "signals": _safe_source(lambda: _signals_source(db, symbol, bounded_lookback, now)),
        "price_volume": _safe_source(lambda: _price_volume_source(db, symbol, benchmark_symbol, bounded_lookback, now)),
    }

    return _score_bundle(symbol, bounded_lookback, sources).as_dict()


def _empty_source(label: str = "Inactive") -> ConfirmationSourceSummary:
    return ConfirmationSourceSummary(
        present=False,
        direction="neutral",
        strength=0,
        quality=0,
        freshness_days=None,
        label=label,
    )


def _empty_bundle(ticker: str, lookback_days: int) -> ConfirmationScoreBundle:
    sources: dict[ConfirmationSourceKey, ConfirmationSourceSummary] = {
        "congress": _empty_source("Inactive"),
        "insiders": _empty_source("Inactive"),
        "signals": _empty_source("No current smart signal"),
        "price_volume": _empty_source("No price confirmation"),
    }
    return ConfirmationScoreBundle(
        ticker=ticker,
        lookback_days=lookback_days,
        score=0,
        band="inactive",
        direction="neutral",
        status="Inactive",
        explanation="Congress, insider, smart signal, and price confirmation sources are inactive for this lookback.",
        sources=sources,
        drivers=["Congress inactive", "Insiders inactive", "No current smart signal"],
    )


def _safe_source(build):
    try:
        return build()
    except Exception:
        return _empty_source()


def _coerce_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _freshness_days(value: datetime | None, now: datetime) -> int | None:
    ts = _coerce_utc(value)
    if ts is None:
        return None
    return max((now - ts).days, 0)


def _freshness_score(days: int | None) -> int:
    if days is None:
        return 0
    if days <= 3:
        return 100
    if days <= 7:
        return 85
    if days <= 14:
        return 65
    if days <= 30:
        return 40
    return 15


def _normalized_side(value: str | None) -> Literal["buy", "sell"] | None:
    normalized = (value or "").strip().lower()
    if normalized in BUY_TRADE_TYPES or "purchase" in normalized or normalized.startswith("p-"):
        return "buy"
    if normalized in SELL_TRADE_TYPES or "sale" in normalized or normalized.startswith("s-"):
        return "sell"
    return None


def _source_direction(buys: int, sells: int) -> ConfirmationDirection:
    sided_total = buys + sells
    if sided_total <= 0:
        return "neutral"
    if buys > 0 and sells > 0 and abs(buys - sells) / sided_total < 0.34:
        return "mixed"
    if buys > sells:
        return "bullish"
    if sells > buys:
        return "bearish"
    return "mixed"


def _skew_ratio(buys: int, sells: int) -> float:
    total = buys + sells
    if total <= 0:
        return 0.0
    return abs(buys - sells) / total


def _insider_participant_key(payload_json: str | None, member_name: str | None) -> str:
    if payload_json:
        try:
            payload = json.loads(payload_json)
        except Exception:
            payload = {}
        if isinstance(payload, dict):
            for key in ("reporting_cik", "reportingCik"):
                value = payload.get(key)
                if isinstance(value, str) and value.strip():
                    return f"cik:{value.strip()}"
            raw = payload.get("raw")
            if isinstance(raw, dict):
                for key in ("reportingCik", "reportingCIK"):
                    value = raw.get(key)
                    if isinstance(value, str) and value.strip():
                        return f"cik:{value.strip()}"
    return f"name:{(member_name or '').strip().lower()}"


def _trade_activity_source(
    db: Session,
    symbol: str,
    event_type: Literal["congress_trade", "insider_trade"],
    lookback_days: int,
    now: datetime,
) -> ConfirmationSourceSummary:
    since = now - timedelta(days=lookback_days)
    trade_ts = func.coalesce(Event.event_date, Event.ts)
    rows = db.execute(
        select(
            Event.event_date,
            Event.ts,
            Event.trade_type,
            Event.amount_max,
            Event.member_bioguide_id,
            Event.member_name,
            Event.payload_json,
        )
        .where(Event.event_type == event_type)
        .where(Event.symbol.is_not(None))
        .where(func.upper(Event.symbol) == symbol)
        .where(trade_ts >= since)
        .where(insider_visibility_clause())
        .order_by(trade_ts.desc())
        .limit(200)
    ).all()

    return _trade_activity_summary_from_rows(rows, event_type, now)


def _trade_activity_sources(
    db: Session,
    symbols: list[str],
    event_type: Literal["congress_trade", "insider_trade"],
    lookback_days: int,
    now: datetime,
) -> dict[str, ConfirmationSourceSummary]:
    if not symbols:
        return {}

    since = now - timedelta(days=lookback_days)
    trade_ts = func.coalesce(Event.event_date, Event.ts)
    normalized_symbol = func.upper(Event.symbol)
    rows = db.execute(
        select(
            normalized_symbol.label("symbol"),
            Event.event_date,
            Event.ts,
            Event.trade_type,
            Event.amount_max,
            Event.member_bioguide_id,
            Event.member_name,
            Event.payload_json,
        )
        .where(Event.event_type == event_type)
        .where(Event.symbol.is_not(None))
        .where(normalized_symbol.in_(symbols))
        .where(trade_ts >= since)
        .where(insider_visibility_clause())
        .order_by(normalized_symbol, trade_ts.desc())
    ).all()

    rows_by_symbol: dict[str, list] = {symbol: [] for symbol in symbols}
    for row in rows:
        symbol = (row.symbol or "").strip().upper()
        if symbol in rows_by_symbol and len(rows_by_symbol[symbol]) < 200:
            rows_by_symbol[symbol].append(row)

    return {
        symbol: _trade_activity_summary_from_rows(symbol_rows, event_type, now)
        for symbol, symbol_rows in rows_by_symbol.items()
        if symbol_rows
    }


def _trade_activity_summary_from_rows(
    rows,
    event_type: Literal["congress_trade", "insider_trade"],
    now: datetime,
) -> ConfirmationSourceSummary:
    if not rows:
        return _empty_source("Inactive")

    buys = 0
    sells = 0
    total_amount = 0.0
    participants: set[str] = set()
    latest_ts: datetime | None = None
    for row in rows:
        side = _normalized_side(row.trade_type)
        if side == "buy":
            buys += 1
        elif side == "sell":
            sells += 1
        amount = _positive_float(row.amount_max)
        if amount is not None:
            total_amount += amount
        if event_type == "congress_trade":
            participant = (row.member_bioguide_id or row.member_name or "").strip().lower()
        else:
            participant = _insider_participant_key(row.payload_json, row.member_name)
        if participant:
            participants.add(participant)
        row_ts = _coerce_utc(row.event_date or row.ts)
        if row_ts is not None and (latest_ts is None or row_ts > latest_ts):
            latest_ts = row_ts

    count = len(rows)
    breadth = len(participants)
    direction = _source_direction(buys, sells)
    skew = _skew_ratio(buys, sells)
    amount_score = min(total_amount / 1_000_000, 1.0) * 10
    neutral_penalty = 0 if direction in ("bullish", "bearish") else -12
    strength = _clamp_int(22 + min(count, 6) * 7 + skew * 40 + amount_score + neutral_penalty)
    quality_base = 30 if event_type == "insider_trade" else 24
    quality = _clamp_int(quality_base + min(count, 6) * 8 + min(breadth, 5) * 7 + skew * 20)
    freshness = _freshness_days(latest_ts, now)

    if direction == "bullish":
        label = "Active / buy-skewed"
    elif direction == "bearish":
        label = "Active / sell-skewed"
    elif direction == "mixed":
        label = "Active / mixed"
    else:
        label = "Active / neutral"

    return ConfirmationSourceSummary(
        present=True,
        direction=direction,
        strength=strength,
        quality=quality,
        freshness_days=freshness,
        label=label,
    )


def _positive_float(value) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if not isfinite(parsed) or parsed <= 0:
        return None
    return parsed


def _clamp_int(value: float, minimum: int = 0, maximum: int = 100) -> int:
    return max(minimum, min(maximum, int(round(value))))


def _signals_source(db: Session, symbol: str, lookback_days: int, now: datetime) -> ConfirmationSourceSummary:
    since = now - timedelta(days=lookback_days)
    baseline_since = now - timedelta(days=365)
    baseline_rows = db.execute(
        select(
            Event.event_type,
            func.avg(Event.amount_max).label("baseline_amount"),
            func.count().label("baseline_count"),
        )
        .where(Event.event_type.in_(["congress_trade", "insider_trade"]))
        .where(Event.symbol.is_not(None))
        .where(func.upper(Event.symbol) == symbol)
        .where(Event.amount_max.is_not(None))
        .where(Event.ts >= baseline_since)
        .where(insider_visibility_clause())
        .group_by(Event.event_type)
    ).all()
    baselines = {
        row.event_type: (float(row.baseline_amount or 0), int(row.baseline_count or 0))
        for row in baseline_rows
    }

    rows = db.execute(
        select(Event.event_type, Event.ts, Event.event_date, Event.trade_type, Event.amount_max)
        .where(Event.event_type.in_(["congress_trade", "insider_trade"]))
        .where(Event.symbol.is_not(None))
        .where(func.upper(Event.symbol) == symbol)
        .where(Event.amount_max.is_not(None))
        .where(func.coalesce(Event.event_date, Event.ts) >= since)
        .where(insider_visibility_clause())
        .order_by(func.coalesce(Event.event_date, Event.ts).desc())
        .limit(200)
    ).all()

    return _signals_summary_from_rows(rows, baselines, now)


def _signals_sources(
    db: Session,
    symbols: list[str],
    lookback_days: int,
    now: datetime,
) -> dict[str, ConfirmationSourceSummary]:
    if not symbols:
        return {}

    since = now - timedelta(days=lookback_days)
    baseline_since = now - timedelta(days=365)
    normalized_symbol = func.upper(Event.symbol)
    baseline_rows = db.execute(
        select(
            normalized_symbol.label("symbol"),
            Event.event_type,
            func.avg(Event.amount_max).label("baseline_amount"),
            func.count().label("baseline_count"),
        )
        .where(Event.event_type.in_(["congress_trade", "insider_trade"]))
        .where(Event.symbol.is_not(None))
        .where(normalized_symbol.in_(symbols))
        .where(Event.amount_max.is_not(None))
        .where(Event.ts >= baseline_since)
        .where(insider_visibility_clause())
        .group_by(normalized_symbol, Event.event_type)
    ).all()

    baselines_by_symbol: dict[str, dict[str, tuple[float, int]]] = {symbol: {} for symbol in symbols}
    for row in baseline_rows:
        symbol = (row.symbol or "").strip().upper()
        if symbol in baselines_by_symbol:
            baselines_by_symbol[symbol][row.event_type] = (
                float(row.baseline_amount or 0),
                int(row.baseline_count or 0),
            )

    rows = db.execute(
        select(
            normalized_symbol.label("symbol"),
            Event.event_type,
            Event.ts,
            Event.event_date,
            Event.trade_type,
            Event.amount_max,
        )
        .where(Event.event_type.in_(["congress_trade", "insider_trade"]))
        .where(Event.symbol.is_not(None))
        .where(normalized_symbol.in_(symbols))
        .where(Event.amount_max.is_not(None))
        .where(func.coalesce(Event.event_date, Event.ts) >= since)
        .where(insider_visibility_clause())
        .order_by(normalized_symbol, func.coalesce(Event.event_date, Event.ts).desc())
    ).all()

    rows_by_symbol: dict[str, list] = {symbol: [] for symbol in symbols}
    for row in rows:
        symbol = (row.symbol or "").strip().upper()
        if symbol in rows_by_symbol and len(rows_by_symbol[symbol]) < 200:
            rows_by_symbol[symbol].append(row)

    results: dict[str, ConfirmationSourceSummary] = {}
    for symbol in symbols:
        summary = _signals_summary_from_rows(rows_by_symbol.get(symbol, []), baselines_by_symbol.get(symbol, {}), now)
        if summary.present:
            results[symbol] = summary
    return results


def _signals_summary_from_rows(
    rows,
    baselines: dict[str, tuple[float, int]],
    now: datetime,
) -> ConfirmationSourceSummary:
    if not baselines:
        return _empty_source("No current smart signal")

    candidates: list[dict] = []
    for row in rows:
        baseline_amount, baseline_count = baselines.get(row.event_type, (0.0, 0))
        defaults = SIGNAL_DEFAULTS.get(row.event_type)
        amount = _positive_float(row.amount_max)
        if defaults is None or amount is None or baseline_amount <= 0:
            continue
        if baseline_count < defaults["min_baseline_count"]:
            continue
        if amount < defaults["min_amount"]:
            continue
        unusual_multiple = amount / baseline_amount
        if unusual_multiple < defaults["multiple"]:
            continue
        ts = _coerce_utc(row.event_date or row.ts) or now
        smart_score, smart_band = calculate_smart_score(
            unusual_multiple=unusual_multiple,
            amount_max=amount,
            ts=ts,
            confirmation_30d=None,
        )
        if smart_score < 35:
            continue
        candidates.append(
            {
                "score": smart_score,
                "band": smart_band,
                "direction": _source_direction(1 if _normalized_side(row.trade_type) == "buy" else 0, 1 if _normalized_side(row.trade_type) == "sell" else 0),
                "freshness_days": _freshness_days(ts, now),
            }
        )

    if not candidates:
        return _empty_source("No current smart signal")

    directions = [item["direction"] for item in candidates if item["direction"] != "neutral"]
    direction = _combined_direction(directions)
    top = max(candidates, key=lambda item: item["score"])
    strength = int(top["score"])
    quality = _clamp_int(max(item["score"] for item in candidates) + min(len(candidates), 4) * 4)
    freshness = min((item["freshness_days"] for item in candidates if item["freshness_days"] is not None), default=None)
    label = f"{top['band'].title()} smart signal"

    return ConfirmationSourceSummary(
        present=True,
        direction=direction,
        strength=strength,
        quality=quality,
        freshness_days=freshness,
        label=label,
    )


def _price_volume_source(
    db: Session,
    symbol: str,
    benchmark_symbol: str,
    lookback_days: int,
    now: datetime,
) -> ConfirmationSourceSummary:
    end_date = now.date()
    start_date = end_date - timedelta(days=max(lookback_days - 1, 1))
    price_map = get_eod_close_series(db, symbol, start_date.isoformat(), end_date.isoformat())
    if len(price_map) < 2:
        return _empty_source("No price confirmation")

    benchmark_map = get_eod_close_series(db, benchmark_symbol, start_date.isoformat(), end_date.isoformat())
    return _price_volume_summary_from_maps(price_map, benchmark_map, lookback_days, now)


def _price_volume_sources(
    db: Session,
    symbols: list[str],
    benchmark_symbol: str,
    lookback_days: int,
    now: datetime,
) -> dict[str, ConfirmationSourceSummary]:
    if not symbols:
        return {}

    end_date = now.date()
    start_date = end_date - timedelta(days=max(lookback_days - 1, 1))
    start_key = start_date.isoformat()
    end_key = end_date.isoformat()
    lookup_symbols = sorted(set(symbols + [benchmark_symbol]))
    rows = db.execute(
        select(PriceCache.symbol, PriceCache.date, PriceCache.close)
        .where(PriceCache.symbol.in_(lookup_symbols))
        .where(PriceCache.date >= start_key)
        .where(PriceCache.date <= end_key)
    ).all()

    price_maps: dict[str, dict[str, float]] = {symbol: {} for symbol in lookup_symbols}
    for row in rows:
        symbol = (row.symbol or "").strip().upper()
        if symbol not in price_maps:
            continue
        price_maps[symbol][str(row.date)] = float(row.close)

    benchmark_map = dict(sorted(price_maps.get(benchmark_symbol, {}).items()))
    results: dict[str, ConfirmationSourceSummary] = {}
    for symbol in symbols:
        summary = _price_volume_summary_from_maps(
            dict(sorted(price_maps.get(symbol, {}).items())),
            benchmark_map,
            lookback_days,
            now,
        )
        if summary.present:
            results[symbol] = summary
    return results


def _price_volume_summary_from_maps(
    price_map: dict[str, float],
    benchmark_map: dict[str, float],
    lookback_days: int,
    now: datetime,
) -> ConfirmationSourceSummary:
    if len(price_map) < 2:
        return _empty_source("No price confirmation")

    sorted_days = sorted(price_map)
    first_close = _positive_float(price_map.get(sorted_days[0]))
    last_close = _positive_float(price_map.get(sorted_days[-1]))
    if first_close is None or last_close is None:
        return _empty_source("No price confirmation")

    ticker_return = ((last_close - first_close) / first_close) * 100
    benchmark_return = None
    if len(benchmark_map) >= 2:
        benchmark_days = sorted(benchmark_map)
        benchmark_first = _positive_float(benchmark_map.get(benchmark_days[0]))
        benchmark_last = _positive_float(benchmark_map.get(benchmark_days[-1]))
        if benchmark_first is not None and benchmark_last is not None:
            benchmark_return = ((benchmark_last - benchmark_first) / benchmark_first) * 100

    relative_return = ticker_return - benchmark_return if benchmark_return is not None else ticker_return
    if abs(relative_return) < 2.0:
        return _empty_source("No price confirmation")

    direction: ConfirmationDirection = "bullish" if relative_return > 0 else "bearish"
    strength = _clamp_int(18 + abs(relative_return) * 4)
    expected_points = max(2, int(lookback_days * 5 / 7))
    density = min(len(price_map) / expected_points, 1.0)
    quality = _clamp_int(25 + density * 50 + (20 if benchmark_return is not None else 0))
    latest_day = datetime.strptime(sorted_days[-1], "%Y-%m-%d").replace(tzinfo=timezone.utc)
    freshness = _freshness_days(latest_day, now)
    strength_word = "Weak" if strength < 45 else "Moderate" if strength < 70 else "Strong"

    return ConfirmationSourceSummary(
        present=True,
        direction=direction,
        strength=strength,
        quality=quality,
        freshness_days=freshness,
        label=f"{strength_word} {direction} price confirmation",
    )


def _combined_direction(directions: list[str]) -> ConfirmationDirection:
    values = {direction for direction in directions if direction != "neutral"}
    if not values:
        return "neutral"
    if "mixed" in values or ("bullish" in values and "bearish" in values):
        return "mixed"
    if "bullish" in values:
        return "bullish"
    if "bearish" in values:
        return "bearish"
    return "neutral"


def _score_bundle(
    ticker: str,
    lookback_days: int,
    sources: dict[ConfirmationSourceKey, ConfirmationSourceSummary],
) -> ConfirmationScoreBundle:
    present_sources = [source for source in sources.values() if source.present]
    active_count = len(present_sources)
    direction = _combined_direction([source.direction for source in present_sources])

    if active_count == 0:
        empty = _empty_bundle(ticker, lookback_days)
        return ConfirmationScoreBundle(
            ticker=empty.ticker,
            lookback_days=empty.lookback_days,
            score=empty.score,
            band=empty.band,
            direction=empty.direction,
            status=empty.status,
            explanation=empty.explanation,
            sources=sources,
            drivers=empty.drivers,
        )

    breadth_component = (active_count / 4) * 100
    agreement_component = _agreement_component(present_sources)
    quality_component = sum(source.quality for source in present_sources) / active_count
    freshness_component = sum(_freshness_score(source.freshness_days) for source in present_sources) / active_count
    price_component = sources["price_volume"].strength if sources["price_volume"].present else 0

    score = _clamp_int(
        breadth_component * 0.25
        + agreement_component * 0.25
        + quality_component * 0.20
        + freshness_component * 0.15
        + price_component * 0.15
    )
    if active_count == 1:
        score = min(score, 39)
    if direction == "mixed":
        score = min(score, 59)

    band = confirmation_band_for_score(score)
    drivers = _driver_bullets(sources)
    status = _status_text(active_count, direction)
    explanation = _explanation(sources, drivers, direction)

    return ConfirmationScoreBundle(
        ticker=ticker,
        lookback_days=lookback_days,
        score=score,
        band=band,
        direction=direction,
        status=status,
        explanation=explanation,
        sources=sources,
        drivers=drivers,
    )


def _agreement_component(sources: list[ConfirmationSourceSummary]) -> float:
    directions = [source.direction for source in sources if source.direction != "neutral"]
    if not directions:
        return 20.0
    if len(directions) == 1:
        return 45.0 if directions[0] != "mixed" else 25.0
    direction = _combined_direction(directions)
    if direction == "mixed":
        return 30.0
    return 100.0


def _status_text(active_count: int, direction: ConfirmationDirection) -> str:
    if active_count <= 0:
        return "Inactive"
    if active_count == 1:
        return f"Single-source {direction}"
    if direction == "mixed":
        return "Mixed multi-source setup"
    if direction == "neutral":
        return "Neutral multi-source setup"
    return f"{active_count}-source {direction} confirmation"


def _source_driver(key: ConfirmationSourceKey, source: ConfirmationSourceSummary) -> str | None:
    if not source.present:
        return None
    if key == "congress":
        if source.direction == "bullish":
            return "Congress buy-skewed"
        if source.direction == "bearish":
            return "Congress sell-skewed"
        if source.direction == "mixed":
            return "Congress mixed"
        return "Congress active"
    if key == "insiders":
        if source.direction == "bullish":
            return "Recent insider buying"
        if source.direction == "bearish":
            return "Recent insider selling"
        if source.direction == "mixed":
            return "Insider activity mixed"
        return "Insiders active"
    if key == "signals":
        if source.direction == "bullish":
            return "Bullish smart signal"
        if source.direction == "bearish":
            return "Bearish smart signal"
        if source.direction == "mixed":
            return "Mixed smart signals"
        return "Smart signal active"
    if key == "price_volume":
        strength = "Weak" if source.strength < 45 else "Moderate" if source.strength < 70 else "Strong"
        if source.direction in ("bullish", "bearish"):
            return f"{strength} {source.direction} price confirmation"
        return "Price confirmation active"
    return None


def _driver_bullets(sources: dict[ConfirmationSourceKey, ConfirmationSourceSummary]) -> list[str]:
    active = [
        (key, source)
        for key, source in sources.items()
        if source.present
    ]
    active.sort(key=lambda item: item[1].strength, reverse=True)
    drivers = [
        driver
        for key, source in active
        if (driver := _source_driver(key, source)) is not None
    ][:3]

    inactive_candidates = [
        ("congress", "Congress inactive"),
        ("insiders", "Insiders inactive"),
        ("signals", "No current smart signal"),
        ("price_volume", "No price confirmation"),
    ]
    for key, label in inactive_candidates:
        if len(drivers) >= 4:
            break
        if not sources[key].present:
            drivers.append(label)
        if len(drivers) >= 2 and active:
            break

    return drivers[:4] or ["No active confirmation sources"]


def _inactive_source_names(sources: dict[ConfirmationSourceKey, ConfirmationSourceSummary]) -> list[str]:
    labels = {
        "congress": "Congress",
        "insiders": "insider activity",
        "signals": "smart signals",
        "price_volume": "price confirmation",
    }
    return [labels[key] for key, source in sources.items() if not source.present]


def _lower_first(value: str) -> str:
    return value[:1].lower() + value[1:] if value else value


def _join_compact(parts: list[str]) -> str:
    if not parts:
        return ""
    if len(parts) == 1:
        return parts[0]
    if len(parts) == 2:
        return f"{parts[0]} and {parts[1]}"
    return f"{', '.join(parts[:-1])}, and {parts[-1]}"


def _explanation(
    sources: dict[ConfirmationSourceKey, ConfirmationSourceSummary],
    drivers: list[str],
    direction: ConfirmationDirection,
) -> str:
    active_drivers = [
        driver for driver in drivers
        if not driver.endswith("inactive") and not driver.startswith("No ")
    ]
    inactive_names = _inactive_source_names(sources)
    inactive_clause = _join_compact(inactive_names[:2])

    if direction == "mixed" and active_drivers:
        suffix = f", while {inactive_clause} remain inactive" if inactive_clause else ""
        return f"Active sources are mixed: {_join_compact([_lower_first(item) for item in active_drivers[:3]])}{suffix}."
    if len(active_drivers) >= 2:
        suffix = f", while {inactive_clause} remain inactive" if inactive_clause else ""
        return f"{active_drivers[0]} aligns with {_lower_first(active_drivers[1])}{suffix}."
    if len(active_drivers) == 1:
        suffix = f", while {inactive_clause} remain inactive" if inactive_clause else ""
        return f"{active_drivers[0]} is the only active confirmation source{suffix}."
    return "Source activity is present but direction is neutral, with no aligned confirmation yet."
