"""Structured watchlist alert rules evaluated from Walnut's persisted data.

This module deliberately has no provider calls.  The page only reads definitions;
the monitoring job evaluates active rules against cached prices, confirmation
snapshots, and durable activity events.
"""
from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.models import (
    ConfirmationMonitoringSnapshot,
    Event,
    FundamentalsCache,
    MonitoringAlert,
    PriceCache,
    QuoteCache,
    Security,
    UserAccount,
    Watchlist,
    WatchlistAlertRule,
    WatchlistAlertRuleState,
    WatchlistAlertRuleTrigger,
    WatchlistItem,
)
from app.services.technical_indicators import _ema, _rsi

logger = logging.getLogger(__name__)

MAX_CONDITIONS = 10
SCOPE_TYPES = {"any_watchlist_ticker", "specific_ticker", "watchlist_aggregate"}
MATCH_TYPES = {"all", "any"}
DELIVERIES = {"immediate", "daily", "both"}
WINDOW_UNITS = {"hour", "day", "month"}


# This registry is the server-side source of truth.  It describes only metrics
# that are backed by persisted Walnut data today, so the UI never promises data
# that cannot be evaluated without an expensive synchronous provider request.
METRIC_REGISTRY: dict[str, dict[str, Any]] = {
    "price": {"label": "Price", "category": "Price & Volume", "kind": "numeric", "operators": ["gt", "gte", "lt", "lte", "crosses_above", "crosses_below"], "metric_comparison": True},
    "price_change_pct": {"label": "Price % change", "category": "Price & Volume", "kind": "numeric", "operators": ["gt", "gte", "lt", "lte", "increases_by", "decreases_by"], "requires_window": True},
    "volume": {"label": "Volume", "category": "Price & Volume", "kind": "numeric", "operators": ["gt", "gte", "lt", "lte"]},
    "relative_volume": {"label": "Relative volume", "category": "Price & Volume", "kind": "numeric", "operators": ["gt", "gte", "lt", "lte"]},
    "market_cap": {"label": "Market cap", "category": "Price & Volume", "kind": "numeric", "operators": ["gt", "gte", "lt", "lte"]},
    "rsi": {"label": "RSI", "category": "Technical", "kind": "numeric", "operators": ["gt", "gte", "lt", "lte", "crosses_above", "crosses_below"], "params": {"period": {"default": 14, "min": 2, "max": 100}}},
    "sma": {"label": "SMA", "category": "Technical", "kind": "numeric", "operators": ["gt", "gte", "lt", "lte", "crosses_above", "crosses_below"], "metric_comparison": True, "params": {"period": {"default": 20, "min": 2, "max": 400}}},
    "ema": {"label": "EMA", "category": "Technical", "kind": "numeric", "operators": ["gt", "gte", "lt", "lte", "crosses_above", "crosses_below"], "metric_comparison": True, "params": {"period": {"default": 20, "min": 2, "max": 400}}},
    "macd": {"label": "MACD", "category": "Technical", "kind": "numeric", "operators": ["gt", "gte", "lt", "lte", "crosses_above", "crosses_below"], "metric_comparison": True},
    "macd_signal": {"label": "MACD signal", "category": "Technical", "kind": "numeric", "operators": ["gt", "gte", "lt", "lte", "crosses_above", "crosses_below"], "metric_comparison": True},
    "vwap": {"label": "VWAP", "category": "Technical", "kind": "numeric", "operators": ["gt", "gte", "lt", "lte", "crosses_above", "crosses_below"], "metric_comparison": True, "params": {"period": {"default": 20, "min": 2, "max": 252}}},
    "bollinger_upper": {"label": "Bollinger upper band", "category": "Technical", "kind": "numeric", "operators": ["gt", "gte", "lt", "lte", "crosses_above", "crosses_below"], "params": {"period": {"default": 20, "min": 2, "max": 252}}},
    "bollinger_lower": {"label": "Bollinger lower band", "category": "Technical", "kind": "numeric", "operators": ["gt", "gte", "lt", "lte", "crosses_above", "crosses_below"], "params": {"period": {"default": 20, "min": 2, "max": 252}}},
    "week_52_high": {"label": "52-week high", "category": "Technical", "kind": "numeric", "operators": ["gt", "gte", "lt", "lte", "crosses_above", "crosses_below"]},
    "week_52_low": {"label": "52-week low", "category": "Technical", "kind": "numeric", "operators": ["gt", "gte", "lt", "lte", "crosses_above", "crosses_below"]},
    "revenue_growth": {"label": "Revenue growth", "category": "Fundamentals", "kind": "numeric", "operators": ["gt", "gte", "lt", "lte"]},
    "eps_growth": {"label": "EPS growth", "category": "Fundamentals", "kind": "numeric", "operators": ["gt", "gte", "lt", "lte"]},
    "trailing_pe": {"label": "P/E", "category": "Fundamentals", "kind": "numeric", "operators": ["gt", "gte", "lt", "lte"]},
    "forward_pe": {"label": "Forward P/E", "category": "Fundamentals", "kind": "numeric", "operators": ["gt", "gte", "lt", "lte"]},
    "ev_to_ebitda": {"label": "EV / EBITDA", "category": "Fundamentals", "kind": "numeric", "operators": ["gt", "gte", "lt", "lte"]},
    "roe": {"label": "ROE", "category": "Fundamentals", "kind": "numeric", "operators": ["gt", "gte", "lt", "lte"]},
    "operating_margin": {"label": "Operating margin", "category": "Fundamentals", "kind": "numeric", "operators": ["gt", "gte", "lt", "lte"]},
    "net_debt_to_ebitda": {"label": "Net debt / EBITDA", "category": "Fundamentals", "kind": "numeric", "operators": ["gt", "gte", "lt", "lte"]},
    "debt_to_equity": {"label": "Debt-to-equity", "category": "Fundamentals", "kind": "numeric", "operators": ["gt", "gte", "lt", "lte"]},
    "current_ratio": {"label": "Current ratio", "category": "Fundamentals", "kind": "numeric", "operators": ["gt", "gte", "lt", "lte"]},
    "confirmation_score": {"label": "Confirmation Score", "category": "Walnut", "kind": "numeric", "operators": ["gt", "gte", "lt", "lte", "crosses_above", "crosses_below"]},
    "bullish_state": {"label": "Bullish state", "category": "Walnut", "kind": "boolean", "operators": ["is_true", "is_false"]},
    "bearish_state": {"label": "Bearish state", "category": "Walnut", "kind": "boolean", "operators": ["is_true", "is_false"]},
    "cross_source_count": {"label": "Cross-source confirmation count", "category": "Walnut", "kind": "numeric", "operators": ["gt", "gte", "lt", "lte", "crosses_above", "crosses_below"]},
    "congress_purchase_count": {"label": "Congress purchase count", "category": "Congress", "kind": "event", "operators": ["gte", "gt"], "requires_window": True},
    "congress_sale_count": {"label": "Congress sale count", "category": "Congress", "kind": "event", "operators": ["gte", "gt"], "requires_window": True},
    "congress_unique_buyers": {"label": "Unique Congress purchasers", "category": "Congress", "kind": "event", "operators": ["gte", "gt"], "requires_window": True},
    "congress_unique_sellers": {"label": "Unique Congress sellers", "category": "Congress", "kind": "event", "operators": ["gte", "gt"], "requires_window": True},
    "congress_purchase_value": {"label": "Congress purchase value", "category": "Congress", "kind": "event", "operators": ["gte", "gt"], "requires_window": True},
    "congress_sale_value": {"label": "Congress sale value", "category": "Congress", "kind": "event", "operators": ["gte", "gt"], "requires_window": True},
    "insider_purchase_count": {"label": "Insider purchase count", "category": "Insiders", "kind": "event", "operators": ["gte", "gt"], "requires_window": True},
    "insider_sale_count": {"label": "Insider sale count", "category": "Insiders", "kind": "event", "operators": ["gte", "gt"], "requires_window": True},
    "insider_unique_buyers": {"label": "Unique insider purchasers", "category": "Insiders", "kind": "event", "operators": ["gte", "gt"], "requires_window": True},
    "insider_unique_sellers": {"label": "Unique insider sellers", "category": "Insiders", "kind": "event", "operators": ["gte", "gt"], "requires_window": True},
    "insider_purchase_value": {"label": "Insider purchase value", "category": "Insiders", "kind": "event", "operators": ["gte", "gt"], "requires_window": True},
    "insider_sale_value": {"label": "Insider sale value", "category": "Insiders", "kind": "event", "operators": ["gte", "gt"], "requires_window": True},
    "government_contract_count": {"label": "Government contract count", "category": "Government Contracts", "kind": "event", "operators": ["gte", "gt"], "requires_window": True},
    "government_contract_value": {"label": "Government contract value", "category": "Government Contracts", "kind": "event", "operators": ["gte", "gt"], "requires_window": True},
}


def metric_registry_payload() -> list[dict[str, Any]]:
    return [{"key": key, **value} for key, value in METRIC_REGISTRY.items()]


def _window_delta(value: Any) -> timedelta:
    if not isinstance(value, dict):
        raise ValueError("This metric requires a time window.")
    amount = value.get("value")
    unit = str(value.get("unit") or "").lower()
    if not isinstance(amount, int) or amount < 1 or amount > 365 or unit not in WINDOW_UNITS:
        raise ValueError("Time windows must use 1–365 hours, days, or months.")
    return timedelta(hours=amount if unit == "hour" else amount * (24 if unit == "day" else 24 * 30))


def _condition_metric_key(condition: dict[str, Any], key: str = "metric") -> str:
    value = str(condition.get(key) or "").strip().lower()
    if value not in METRIC_REGISTRY:
        raise ValueError("Choose a supported metric.")
    return value


def validate_conditions(conditions: Any) -> list[dict[str, Any]]:
    if not isinstance(conditions, list) or not conditions or len(conditions) > MAX_CONDITIONS:
        raise ValueError(f"A custom alert needs between 1 and {MAX_CONDITIONS} conditions.")
    normalized: list[dict[str, Any]] = []
    for raw in conditions:
        if not isinstance(raw, dict):
            raise ValueError("Each condition must be a structured object.")
        metric = _condition_metric_key(raw)
        registry = METRIC_REGISTRY[metric]
        operator = str(raw.get("operator") or "").strip().lower()
        if operator not in registry["operators"]:
            raise ValueError(f"{registry['label']} does not support that operator.")
        comparison_type = str(raw.get("comparison_type") or "value").strip().lower()
        if comparison_type not in {"value", "metric", "none"}:
            raise ValueError("Comparison type must be value, metric, or none.")
        if registry["kind"] == "boolean":
            if comparison_type != "none":
                raise ValueError(f"{registry['label']} does not take a comparison value.")
            comparison_metric = None
            value = None
        elif comparison_type == "none":
            raise ValueError("Only state metrics may omit a comparison value.")
        elif comparison_type == "metric":
            if not registry.get("metric_comparison"):
                raise ValueError(f"{registry['label']} cannot be compared with another metric.")
            comparison_metric = _condition_metric_key(raw, "comparison_metric")
            if METRIC_REGISTRY[comparison_metric]["kind"] != "numeric":
                raise ValueError("A metric comparison must use a numeric metric.")
        else:
            comparison_metric = None
            value = raw.get("comparison_value")
            if not isinstance(value, (int, float)) or isinstance(value, bool):
                raise ValueError("Enter a numeric comparison value.")
            if operator in {"increases_by", "decreases_by"} and not 0 < float(value) <= 1000:
                raise ValueError("Percentage changes must be greater than 0 and at most 1000%.")
        params = raw.get("metric_params") if isinstance(raw.get("metric_params"), dict) else {}
        param_spec = registry.get("params") or {}
        clean_params: dict[str, int] = {}
        for key, spec in param_spec.items():
            raw_param = params.get(key, spec["default"])
            if not isinstance(raw_param, int) or not spec["min"] <= raw_param <= spec["max"]:
                raise ValueError(f"{registry['label']} {key} must be between {spec['min']} and {spec['max']}.")
            clean_params[key] = raw_param
        window = raw.get("time_window")
        if registry.get("requires_window"):
            _window_delta(window)
        elif window is not None:
            _window_delta(window)
        normalized.append({
            "metric": metric,
            "metric_params": clean_params,
            "operator": operator,
            "comparison_type": comparison_type,
            "comparison_value": None if comparison_type in {"metric", "none"} else float(raw["comparison_value"]),
            "comparison_metric": comparison_metric,
            "comparison_metric_params": raw.get("comparison_metric_params") if isinstance(raw.get("comparison_metric_params"), dict) else {},
            "time_window": window if window is not None else None,
        })
    return normalized


def format_condition(condition: dict[str, Any]) -> str:
    metric = str(condition.get("metric") or "").lower()
    label = METRIC_REGISTRY.get(metric, {}).get("label", metric.replace("_", " ").title())
    params = condition.get("metric_params") if isinstance(condition.get("metric_params"), dict) else {}
    if "period" in params:
        label = f"{label} ({params['period']})"
    operators = {"gt": ">", "gte": "≥", "lt": "<", "lte": "≤", "crosses_above": "crosses above", "crosses_below": "crosses below", "increases_by": "increases by", "decreases_by": "decreases by"}
    operator = operators.get(str(condition.get("operator") or ""), str(condition.get("operator") or "").replace("_", " "))
    if condition.get("comparison_type") == "none":
        target = "true" if condition.get("operator") == "is_true" else "false"
    elif condition.get("comparison_type") == "metric":
        other = str(condition.get("comparison_metric") or "").lower()
        target = METRIC_REGISTRY.get(other, {}).get("label", other.replace("_", " ").title())
        other_params = condition.get("comparison_metric_params") if isinstance(condition.get("comparison_metric_params"), dict) else {}
        if "period" in other_params:
            target = f"{target} ({other_params['period']})"
    else:
        value = condition.get("comparison_value")
        target = f"{value:g}" if isinstance(value, (int, float)) else "—"
        if str(condition.get("operator")) in {"increases_by", "decreases_by"}:
            target += "%"
    text = f"{label} {operator} {target}"
    window = condition.get("time_window")
    if isinstance(window, dict):
        amount = window.get("value")
        unit = str(window.get("unit") or "day").lower()
        text += f" {'over' if metric == 'price_change_pct' else 'within'} {amount} {unit}{'' if amount == 1 else 's'}"
    return text


def format_rule_summary(rule: WatchlistAlertRule | dict[str, Any]) -> str:
    conditions = _loads(rule.conditions_json if isinstance(rule, WatchlistAlertRule) else rule.get("conditions", []), [])
    match_type = rule.match_type if isinstance(rule, WatchlistAlertRule) else rule.get("match_type", "all")
    separator = " AND " if str(match_type).lower() == "all" else " OR "
    return separator.join(format_condition(item) for item in conditions if isinstance(item, dict)) or "Custom condition"


def rule_payload(rule: WatchlistAlertRule) -> dict[str, Any]:
    conditions = _loads(rule.conditions_json, [])
    return {
        "id": rule.id, "name": rule.name, "enabled": rule.enabled, "scope": {"type": rule.scope_type, "ticker": rule.scope_ticker},
        "match_type": rule.match_type, "conditions": conditions, "delivery": rule.delivery,
        "summary": format_rule_summary(rule), "last_triggered_at": rule.last_triggered_at, "last_triggered_ticker": rule.last_triggered_ticker,
        "created_at": rule.created_at, "updated_at": rule.updated_at,
    }


def _loads(value: Any, default: Any) -> Any:
    try:
        parsed = json.loads(value) if isinstance(value, str) else value
        return parsed if isinstance(parsed, type(default)) else default
    except (TypeError, ValueError):
        return default


def _price_rows(db: Session, ticker: str) -> list[PriceCache]:
    return db.execute(select(PriceCache).where(func.upper(PriceCache.symbol) == ticker).order_by(PriceCache.date.asc())).scalars().all()


def _metric_value(db: Session, ticker: str, condition: dict[str, Any], now: datetime) -> tuple[float | None, list[int]]:
    metric = condition["metric"]
    price_metrics = {"price", "price_change_pct", "volume", "relative_volume", "rsi", "sma", "ema", "macd", "macd_signal", "vwap", "bollinger_upper", "bollinger_lower", "week_52_high", "week_52_low"}
    rows = _price_rows(db, ticker) if metric in price_metrics else []
    closes = [float(row.adjusted_close or row.close) for row in rows if (row.adjusted_close or row.close) is not None]
    if metric == "price":
        quote = db.get(QuoteCache, ticker)
        return (float(quote.price) if quote and quote.price is not None else (closes[-1] if closes else None), [])
    if metric == "price_change_pct":
        delta = _window_delta(condition["time_window"])
        cutoff = (now - delta).date().isoformat()
        prior = next((float(row.adjusted_close or row.close) for row in reversed(rows) if row.date <= cutoff and (row.adjusted_close or row.close) is not None), None)
        current = closes[-1] if closes else None
        return (((current - prior) / prior * 100) if prior and current is not None else None, [])
    if metric == "volume": return (float(rows[-1].volume) if rows and rows[-1].volume is not None else None, [])
    if metric == "relative_volume":
        values = [float(row.volume) for row in rows if row.volume is not None]
        return ((values[-1] / (sum(values[-21:-1]) / len(values[-21:-1]))) if len(values) > 2 and sum(values[-21:-1]) else None, [])
    if metric == "market_cap":
        quote = db.get(QuoteCache, ticker)
        return (float(quote.market_cap) if quote and quote.market_cap is not None else None, [])
    if metric == "rsi": return (_rsi(closes, int(condition.get("metric_params", {}).get("period", 14))), [])
    if metric in {"sma", "ema"}:
        period = int(condition.get("metric_params", {}).get("period", 20))
        if len(closes) < period: return (None, [])
        return ((sum(closes[-period:]) / period) if metric == "sma" else _ema(closes, period)[-1], [])
    if metric in {"macd", "macd_signal"}:
        if len(closes) < 35: return (None, [])
        line = [short - long for short, long in zip(_ema(closes, 12), _ema(closes, 26))]
        return ((line[-1] if metric == "macd" else _ema(line, 9)[-1]), [])
    if metric == "vwap":
        period = int(condition.get("metric_params", {}).get("period", 20))
        sample = [(float(row.adjusted_close or row.close), float(row.volume)) for row in rows[-period:] if (row.adjusted_close or row.close) is not None and row.volume]
        total_volume = sum(volume for _, volume in sample)
        return (sum(close * volume for close, volume in sample) / total_volume if total_volume else None, [])
    if metric in {"bollinger_upper", "bollinger_lower"}:
        period = int(condition.get("metric_params", {}).get("period", 20))
        sample = closes[-period:]
        if len(sample) < period:
            return None, []
        average = sum(sample) / period
        deviation = (sum((value - average) ** 2 for value in sample) / period) ** 0.5
        return average + (2 * deviation if metric == "bollinger_upper" else -2 * deviation), []
    if metric == "week_52_high":
        return (max(closes[-252:]) if closes else None, [])
    if metric == "week_52_low":
        return (min(closes[-252:]) if closes else None, [])
    if metric in {"revenue_growth", "eps_growth", "trailing_pe", "forward_pe", "ev_to_ebitda", "roe", "operating_margin", "net_debt_to_ebitda", "debt_to_equity", "current_ratio"}:
        fundamentals = db.execute(select(FundamentalsCache).where(func.upper(FundamentalsCache.symbol) == ticker, FundamentalsCache.status == "ok").order_by(FundamentalsCache.fetched_at.desc()).limit(1)).scalar_one_or_none()
        raw = getattr(fundamentals, metric, None) if fundamentals is not None else None
        return (float(raw) if raw is not None else None, [])
    if metric in {"confirmation_score", "cross_source_count", "bullish_state", "bearish_state"}:
        snapshot = db.execute(select(ConfirmationMonitoringSnapshot).where(ConfirmationMonitoringSnapshot.ticker == ticker).order_by(ConfirmationMonitoringSnapshot.observed_at.desc()).limit(1)).scalar_one_or_none()
        if snapshot is None:
            return None, []
        if metric == "confirmation_score":
            return float(snapshot.score), []
        if metric == "cross_source_count":
            return float(snapshot.source_count), []
        return float(str(snapshot.direction).lower() == ("bullish" if metric == "bullish_state" else "bearish")), []
    return _event_metric_value(db, ticker, condition, now)


def _event_metric_value(db: Session, ticker: str, condition: dict[str, Any], now: datetime) -> tuple[float | None, list[int]]:
    metric = condition["metric"]
    since = now - _window_delta(condition["time_window"])
    event_type = "congress" if metric.startswith("congress_") else "insider" if metric.startswith("insider_") else "government_contract"
    rows = db.execute(select(Event).where(func.upper(Event.symbol) == ticker).where(Event.event_type.like(f"{event_type}%")).where(func.coalesce(Event.event_date, Event.ts) >= since)).scalars().all()
    if event_type != "government_contract":
        is_purchase = "purchase" in metric or "buyers" in metric
        rows = [
            row
            for row in rows
            if (
                "purchase" in str(getattr(row, "trade_type", None) or getattr(row, "transaction_type", None) or "").lower()
                or "buy" in str(getattr(row, "trade_type", None) or getattr(row, "transaction_type", None) or "").lower()
            )
            == is_purchase
        ]
    ids = [int(row.id) for row in rows if row.id is not None]
    if "unique" in metric:
        people = {str(row.member_bioguide_id or row.member_name or row.id) for row in rows}
        return float(len(people)), ids
    if metric.endswith("_value"):
        return float(sum(float(row.amount_max or row.amount_min or 0) for row in rows)), ids
    return float(len(rows)), ids


def _compare(value: float | None, target: float | None, operator: str, previous_value: float | None) -> bool:
    if value is None: return False
    if operator == "is_true": return bool(value)
    if operator == "is_false": return not bool(value)
    if target is None: return False
    if operator == "gt": return value > target
    if operator == "gte": return value >= target
    if operator == "lt": return value < target
    if operator == "lte": return value <= target
    if operator == "increases_by": return value >= target
    if operator == "decreases_by": return value <= -abs(target)
    if operator == "crosses_above": return previous_value is not None and previous_value < target <= value
    if operator == "crosses_below": return previous_value is not None and previous_value > target >= value
    return False


@dataclass(frozen=True)
class Evaluation:
    matched: bool
    condition_results: list[dict[str, Any]]
    values: dict[str, Any]
    dedupe_key: str


def evaluate_rule(db: Session, rule: WatchlistAlertRule, ticker: str, state: WatchlistAlertRuleState | None, now: datetime) -> Evaluation:
    conditions = _loads(rule.conditions_json, [])
    old_values = _loads(state.values_json, {}) if state else {}
    results: list[dict[str, Any]] = []
    fingerprints: list[str] = []
    values: dict[str, Any] = {}
    for index, condition in enumerate(conditions):
        value, event_ids = _metric_value(db, ticker, condition, now)
        target: float | None
        if condition["comparison_type"] == "metric":
            target, _ = _metric_value(db, ticker, {"metric": condition["comparison_metric"], "metric_params": condition.get("comparison_metric_params") or {}, "time_window": condition.get("time_window")}, now)
        elif condition["comparison_type"] == "none":
            target = None
        else:
            target = condition.get("comparison_value")
        previous = old_values.get(str(index)) if isinstance(old_values.get(str(index)), (int, float)) else None
        matched = _compare(value, target, condition["operator"], previous)
        # For ordinary thresholds the durable condition state, rather than the
        # raw prior metric, controls re-arming.  Crossing operators use both.
        if condition["operator"] in {"crosses_above", "crosses_below"}:
            matched = _compare(value, target, condition["operator"], previous)
        results.append({"condition": format_condition(condition), "matched": matched, "value": value, "target": target, "event_ids": event_ids})
        values[str(index)] = value
        fingerprints.extend(str(event_id) for event_id in event_ids)
    matched = all(item["matched"] for item in results) if rule.match_type == "all" else any(item["matched"] for item in results)
    # Activity rules dedupe on their durable event identities. Threshold and
    # crossing rules are already protected by their false -> true state, so a
    # fresh transition must receive a fresh identity after it has re-armed.
    fingerprint = "|".join(sorted(fingerprints)) if fingerprints else f"transition:{now.isoformat()}:{'|'.join(f'{item['value']}:{item['target']}' for item in results)}"
    return Evaluation(matched=matched, condition_results=results, values=values, dedupe_key=hashlib.sha256(f"{rule.id}:{ticker}:{fingerprint}".encode()).hexdigest()[:40])


def _watchlist_tickers(db: Session, watchlist_id: int) -> list[str]:
    return sorted({str(symbol).upper() for symbol in db.execute(select(Security.symbol).join(WatchlistItem, WatchlistItem.security_id == Security.id).where(WatchlistItem.watchlist_id == watchlist_id, WatchlistItem.target_type == "ticker")).scalars().all() if symbol})


def evaluate_watchlist_custom_alerts(db: Session, *, user_id: int, watchlist_id: int, now: datetime | None = None) -> dict[str, int]:
    current = now or datetime.now(timezone.utc)
    rules = db.execute(select(WatchlistAlertRule).where(WatchlistAlertRule.user_id == user_id, WatchlistAlertRule.watchlist_id == watchlist_id, WatchlistAlertRule.enabled.is_(True))).scalars().all()
    tickers = _watchlist_tickers(db, watchlist_id)
    states = {(state.rule_id, state.ticker): state for state in db.execute(select(WatchlistAlertRuleState).where(WatchlistAlertRuleState.rule_id.in_([rule.id for rule in rules] or [-1]))).scalars().all()}
    evaluated = triggered = initialized = 0
    for rule in rules:
        scoped = [rule.scope_ticker.upper()] if rule.scope_type == "specific_ticker" and rule.scope_ticker else tickers
        for ticker in scoped:
            if ticker not in tickers: continue
            state = states.get((rule.id, ticker))
            result = evaluate_rule(db, rule, ticker, state, current)
            evaluated += 1
            # A missing or stale state is a fresh baseline, not a historical
            # transition. This also prevents a Pro re-upgrade from replaying
            # alerts that could have occurred while evaluation was suspended.
            last_evaluated_at = state.last_evaluated_at if state is not None else None
            if last_evaluated_at is not None and last_evaluated_at.tzinfo is None:
                last_evaluated_at = last_evaluated_at.replace(tzinfo=timezone.utc)
            if state is None or (last_evaluated_at is not None and last_evaluated_at < current - timedelta(hours=2)):
                if state is None:
                    db.add(WatchlistAlertRuleState(rule_id=rule.id, ticker=ticker, previous_result=result.matched, current_result=result.matched, last_evaluated_at=current, values_json=json.dumps(result.values)))
                else:
                    state.previous_result = result.matched
                    state.current_result = result.matched
                    state.last_evaluated_at = current
                    state.values_json = json.dumps(result.values)
                initialized += 1
                continue
            should_trigger = result.matched and not state.current_result
            state.previous_result, state.current_result, state.last_evaluated_at, state.values_json = state.current_result, result.matched, current, json.dumps(result.values)
            if not should_trigger:
                continue
            trigger = WatchlistAlertRuleTrigger(user_id=user_id, rule_id=rule.id, watchlist_id=watchlist_id, ticker=ticker, dedupe_key=result.dedupe_key, title=f"Custom Alert - {rule.name}", body=f"{ticker}: " + "; ".join(item["condition"] for item in result.condition_results if item["matched"]), conditions_json=json.dumps(result.condition_results))
            db.add(trigger)
            db.flush()
            state.last_triggered_at, state.dedupe_key = current, result.dedupe_key
            rule.last_triggered_at, rule.last_triggered_ticker = current, ticker
            # Negative IDs live in a distinct namespace from canonical Event IDs.
            db.add(MonitoringAlert(user_id=user_id, source_type="watchlist", source_id=str(watchlist_id), source_name=db.get(Watchlist, watchlist_id).name, event_id=-int(trigger.id), alert_type="custom_alert", symbol=ticker, title=trigger.title, body=trigger.body, payload_json=json.dumps({"custom_alert": True, "rule_id": rule.id, "rule_name": rule.name, "delivery": rule.delivery, "conditions": result.condition_results, "href": f"/watchlists/{watchlist_id}"}), event_created_at=current))
            triggered += 1
            logger.info("custom_alert_rule_triggered rule_id=%s watchlist_id=%s ticker=%s", rule.id, watchlist_id, ticker)
    return {"evaluated": evaluated, "triggered": triggered, "initialized": initialized}
