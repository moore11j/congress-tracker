from __future__ import annotations

import os
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from math import isfinite
from typing import Literal, Protocol

logger = logging.getLogger(__name__)

OptionsFlowState = Literal["bullish", "bearish", "mixed", "inactive", "unavailable"]
OptionsFlowConfidence = Literal["low", "moderate", "high"]
OptionsContractType = Literal["call", "put"]


@dataclass(frozen=True)
class OptionsFlowObservation:
    contract_type: OptionsContractType
    premium: float
    contract_volume: int
    observed_at: datetime | None = None


class OptionsFlowProvider(Protocol):
    name: str

    def fetch_observations(self, symbol: str, *, lookback_days: int) -> list[OptionsFlowObservation]:
        ...


def get_options_flow_summary(
    symbol: str,
    lookback_days: int = 30,
    *,
    provider: str | None = None,
) -> dict:
    ticker = (symbol or "").strip().upper()
    bounded_lookback = max(1, min(int(lookback_days or 30), 365))
    provider_name = (provider or os.getenv("OPTIONS_FLOW_PROVIDER") or "massive").strip().lower()

    if not ticker:
        summary = unavailable_options_flow_summary("", bounded_lookback, provider=provider_name, reason="missing_symbol")
        _log_final(summary)
        return summary

    try:
        provider_impl = _provider(provider_name)
    except ValueError:
        summary = unavailable_options_flow_summary(ticker, bounded_lookback, provider=provider_name, reason="unsupported_provider")
        _log_final(summary)
        return summary

    _log_event(
        "provider_selected",
        ticker=ticker,
        provider=provider_name,
        api_key_present=bool(str(getattr(provider_impl, "api_key", "") or "").strip()),
    )

    try:
        observations = provider_impl.fetch_observations(ticker, lookback_days=bounded_lookback)
    except OptionsFlowUnavailable as exc:
        summary = unavailable_options_flow_summary(ticker, bounded_lookback, provider=provider_name, reason=exc.reason)
        _log_final(summary)
        return summary
    except Exception as exc:
        _log_event("provider_exception", ticker=ticker, provider=provider_name, error=exc.__class__.__name__)
        summary = unavailable_options_flow_summary(ticker, bounded_lookback, provider=provider_name, reason="provider_error")
        _log_final(summary)
        return summary

    summary = summarize_options_flow(ticker, observations, lookback_days=bounded_lookback, provider=provider_name)
    _log_final(summary)
    return summary


def unavailable_options_flow_summary(
    ticker: str,
    lookback_days: int,
    *,
    provider: str,
    reason: str = "unavailable",
) -> dict:
    return _summary(
        ticker=ticker,
        lookback_days=lookback_days,
        state="unavailable",
        label="Options flow unavailable",
        is_active=False,
        confidence="low",
        freshness_days=None,
        latest_flow_date=None,
        summary="Options flow unavailable.",
        signals=["Options flow unavailable"],
        metrics={
            "put_call_premium_ratio": None,
            "call_put_premium_ratio": None,
            "net_premium_skew": 0,
            "total_premium": None,
            "recent_contract_volume": 0,
            "observed_contracts": 0,
            "freshness_days": None,
        },
        can_confirm=False,
        provider=provider,
        reason=reason,
    )


def summarize_options_flow(
    ticker: str,
    observations: list[OptionsFlowObservation],
    *,
    lookback_days: int,
    provider: str,
    now: datetime | None = None,
) -> dict:
    symbol = (ticker or "").strip().upper()
    current_time = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    bounded_lookback = max(1, min(int(lookback_days or 30), 365))
    recent = [
        obs
        for obs in observations
        if _positive_float(obs.premium) is not None
        and obs.contract_volume > 0
        and _freshness_days(obs.observed_at, current_time) is not None
        and (_freshness_days(obs.observed_at, current_time) or 0) <= bounded_lookback
    ]

    if not recent:
        return _inactive_summary(symbol, bounded_lookback, provider)

    call_premium = sum(obs.premium for obs in recent if obs.contract_type == "call")
    put_premium = sum(obs.premium for obs in recent if obs.contract_type == "put")
    call_volume = sum(obs.contract_volume for obs in recent if obs.contract_type == "call")
    put_volume = sum(obs.contract_volume for obs in recent if obs.contract_type == "put")
    total_premium = call_premium + put_premium
    total_volume = call_volume + put_volume
    freshness_values = [
        days
        for obs in recent
        if (days := _freshness_days(obs.observed_at, current_time)) is not None
    ]
    freshness = min(freshness_values) if freshness_values else None
    latest_observed_at = max(
        (
            obs.observed_at.replace(tzinfo=timezone.utc)
            if obs.observed_at is not None and obs.observed_at.tzinfo is None
            else obs.observed_at.astimezone(timezone.utc)
            for obs in recent
            if obs.observed_at is not None
        ),
        default=None,
    )
    observed_contracts = len(recent)
    ratio = (put_premium / call_premium) if call_premium > 0 else (None if put_premium <= 0 else float("inf"))
    call_put_ratio = (call_premium / put_premium) if put_premium > 0 else (None if call_premium <= 0 else float("inf"))
    net_premium_skew = call_premium - put_premium

    active = _is_meaningful_activity(total_premium, total_volume, observed_contracts)
    if not active:
        return _summary(
            ticker=symbol,
            lookback_days=bounded_lookback,
            state="inactive",
            label="Inactive options flow",
            is_active=False,
            confidence="low",
            freshness_days=freshness,
            latest_flow_date=latest_observed_at.date().isoformat() if latest_observed_at is not None else None,
            summary="No notable recent options flow.",
            signals=["No notable recent options flow"],
            metrics=_metrics(ratio, call_put_ratio, net_premium_skew, total_premium, total_volume, observed_contracts, freshness),
            can_confirm=False,
            provider=provider,
        )

    premium_direction = _premium_direction(call_premium, put_premium)
    volume_direction = _volume_direction(call_volume, put_volume)
    conflicted = (
        premium_direction in {"bullish", "bearish"}
        and volume_direction in {"bullish", "bearish"}
        and premium_direction != volume_direction
    )
    confidence = _confidence(
        call_premium=call_premium,
        put_premium=put_premium,
        total_premium=total_premium,
        total_volume=total_volume,
        observed_contracts=observed_contracts,
        freshness_days=freshness,
        conflicted=conflicted,
    )

    if premium_direction == "bullish" and confidence != "low" and not conflicted:
        state: OptionsFlowState = "bullish"
    elif premium_direction == "bearish" and confidence != "low" and not conflicted:
        state = "bearish"
    else:
        state = "mixed"

    signals = _signals(
        state=state,
        call_premium=call_premium,
        put_premium=put_premium,
        freshness_days=freshness,
    )
    can_confirm = (
        state in {"bullish", "bearish"}
        and confidence in {"moderate", "high"}
        and freshness is not None
        and freshness <= 5
        and (total_premium >= 500_000 or total_volume >= 250)
    )

    return _summary(
        ticker=symbol,
        lookback_days=bounded_lookback,
        state=state,
        label=_label(state),
        is_active=True,
        confidence=confidence,
        freshness_days=freshness,
        latest_flow_date=latest_observed_at.date().isoformat() if latest_observed_at is not None else None,
        summary=_plain_summary(state),
        signals=signals,
        metrics=_metrics(ratio, call_put_ratio, net_premium_skew, total_premium, total_volume, observed_contracts, freshness),
        can_confirm=can_confirm,
        provider=provider,
    )


class OptionsFlowUnavailable(Exception):
    def __init__(self, reason: str = "unavailable") -> None:
        super().__init__(reason)
        self.reason = reason


def _provider(provider: str) -> OptionsFlowProvider:
    if provider == "massive":
        from app.services.options_flow_providers.massive import MassiveOptionsFlowProvider

        return MassiveOptionsFlowProvider()
    raise ValueError(provider)


def _log_event(event: str, **payload) -> None:
    logger.info("options_flow %s", json.dumps({"event": event, **payload}, sort_keys=True))


def _log_final(summary: dict) -> None:
    _log_event(
        "final_classification",
        ticker=summary.get("ticker"),
        provider=summary.get("provider"),
        state=summary.get("state"),
        is_active=summary.get("is_active"),
        confidence=summary.get("confidence"),
        can_confirm=summary.get("can_confirm"),
        reason=summary.get("reason"),
    )


def _inactive_summary(ticker: str, lookback_days: int, provider: str) -> dict:
    return _summary(
        ticker=ticker,
        lookback_days=lookback_days,
        state="inactive",
        label="Inactive options flow",
        is_active=False,
        confidence="low",
        freshness_days=None,
        latest_flow_date=None,
        summary="No notable recent options flow.",
        signals=["No notable recent options flow"],
        metrics=_metrics(None, None, 0, None, 0, 0, None),
        can_confirm=False,
        provider=provider,
    )


def _summary(
    *,
    ticker: str,
    lookback_days: int,
    state: OptionsFlowState,
    label: str,
    is_active: bool,
    confidence: OptionsFlowConfidence,
    freshness_days: int | None,
    latest_flow_date: str | None,
    summary: str,
    signals: list[str],
    metrics: dict,
    can_confirm: bool,
    provider: str,
    reason: str | None = None,
) -> dict:
    payload = {
        "ticker": ticker,
        "lookback_days": lookback_days,
        "state": state,
        "direction": state if state in {"bullish", "bearish", "mixed"} else "neutral",
        "label": label,
        "is_active": is_active,
        "active": is_active,
        "confidence": confidence,
        "intensity": _intensity(state=state, confidence=confidence, metrics=metrics, is_active=is_active),
        "freshness_days": freshness_days,
        "latest_flow_date": latest_flow_date,
        "summary": summary,
        "signals": signals[:4],
        "metrics": metrics,
        "can_confirm": can_confirm,
        "provider": provider,
        "source": provider,
        "score": _score(
            state=state,
            confidence=confidence,
            freshness_days=freshness_days,
            metrics=metrics,
            is_active=is_active,
        ),
        "call_put_premium_ratio": metrics.get("call_put_premium_ratio"),
        "total_premium": metrics.get("total_premium"),
        "status": "unavailable" if state == "unavailable" else "ok",
    }
    if reason:
        payload["reason"] = reason
    return payload


def _metrics(
    put_call_premium_ratio: float | None,
    call_put_premium_ratio: float | None,
    net_premium_skew: float,
    total_premium: float | None,
    recent_contract_volume: int,
    observed_contracts: int,
    freshness_days: int | None,
) -> dict:
    ratio = _finite_ratio(put_call_premium_ratio)
    call_ratio = _finite_ratio(call_put_premium_ratio)
    return {
        "put_call_premium_ratio": ratio,
        "call_put_premium_ratio": call_ratio,
        "net_premium_skew": round(net_premium_skew, 2),
        "total_premium": round(float(total_premium), 2) if total_premium is not None and isfinite(total_premium) else None,
        "recent_contract_volume": recent_contract_volume,
        "observed_contracts": observed_contracts,
        "freshness_days": freshness_days,
    }


def _is_meaningful_activity(total_premium: float, total_volume: int, observed_contracts: int) -> bool:
    return total_premium >= 250_000 or total_volume >= 100 or observed_contracts >= 10


def _premium_direction(call_premium: float, put_premium: float) -> OptionsFlowState:
    if call_premium <= 0 and put_premium <= 0:
        return "inactive"
    larger = max(call_premium, put_premium)
    smaller = max(min(call_premium, put_premium), 1.0)
    if larger / smaller < 1.6 or larger - smaller < 100_000:
        return "mixed"
    return "bullish" if call_premium > put_premium else "bearish"


def _volume_direction(call_volume: int, put_volume: int) -> OptionsFlowState:
    if call_volume <= 0 and put_volume <= 0:
        return "inactive"
    larger = max(call_volume, put_volume)
    smaller = max(min(call_volume, put_volume), 1)
    if larger / smaller < 1.3:
        return "mixed"
    return "bullish" if call_volume > put_volume else "bearish"


def _confidence(
    *,
    call_premium: float,
    put_premium: float,
    total_premium: float,
    total_volume: int,
    observed_contracts: int,
    freshness_days: int | None,
    conflicted: bool,
) -> OptionsFlowConfidence:
    if conflicted or freshness_days is None or freshness_days > 10:
        return "low"
    larger = max(call_premium, put_premium)
    smaller = max(min(call_premium, put_premium), 1.0)
    ratio = larger / smaller
    net = larger - smaller
    if ratio >= 2.5 and net >= 1_000_000 and freshness_days <= 2 and (total_volume >= 500 or observed_contracts >= 20):
        return "high"
    if ratio >= 1.6 and net >= 250_000 and freshness_days <= 5 and (total_premium >= 500_000 or total_volume >= 250):
        return "moderate"
    return "low"


def _signals(
    *,
    state: OptionsFlowState,
    call_premium: float,
    put_premium: float,
    freshness_days: int | None,
) -> list[str]:
    if state == "bullish":
        signals = ["Call premium outweighs puts in recent flow", "Recent flow skews bullish"]
    elif state == "bearish":
        signals = ["Put premium outweighs calls in recent flow", "Recent flow skews bearish"]
    elif state == "mixed":
        signals = ["Flow is active, but directional conviction is mixed"]
        if call_premium > put_premium:
            signals.append("Call premium leads, but confirmation is not clean")
        elif put_premium > call_premium:
            signals.append("Put premium leads, but confirmation is not clean")
    else:
        signals = ["No notable recent options flow"]

    if freshness_days is not None:
        signals.append("Fresh today" if freshness_days == 0 else f"Fresh in last {freshness_days}D")
    return signals[:4]


def _label(state: OptionsFlowState) -> str:
    if state == "bullish":
        return "Bullish flow skew"
    if state == "bearish":
        return "Bearish flow skew"
    if state == "mixed":
        return "Mixed options flow"
    if state == "inactive":
        return "Inactive options flow"
    return "Options flow unavailable"


def _plain_summary(state: OptionsFlowState) -> str:
    if state == "bullish":
        return "Call premium outweighs puts in recent flow."
    if state == "bearish":
        return "Put premium outweighs calls in recent flow."
    if state == "mixed":
        return "Flow is active, but directional conviction is mixed."
    if state == "inactive":
        return "No notable recent options flow."
    return "Options flow unavailable."


def _finite_ratio(value: float | None) -> float | None:
    if value is not None and isfinite(value):
        return round(value, 2)
    return None


def _score(
    *,
    state: OptionsFlowState,
    confidence: OptionsFlowConfidence,
    freshness_days: int | None,
    metrics: dict,
    is_active: bool,
) -> int | None:
    if not is_active or state == "unavailable":
        return None
    total_premium = float(metrics.get("total_premium") or 0)
    total_volume = int(metrics.get("recent_contract_volume") or 0)
    base = 50 if state in {"bullish", "bearish"} else 42
    confidence_bonus = 24 if confidence == "high" else 14 if confidence == "moderate" else 6
    premium_bonus = 12 if total_premium >= 5_000_000 else 8 if total_premium >= 1_000_000 else 4 if total_premium >= 250_000 else 0
    volume_bonus = 10 if total_volume >= 500 else 6 if total_volume >= 250 else 2 if total_volume >= 100 else 0
    freshness_bonus = 10 if freshness_days is not None and freshness_days <= 1 else 6 if freshness_days is not None and freshness_days <= 5 else 0
    return min(100, int(round(base + confidence_bonus + premium_bonus + volume_bonus + freshness_bonus)))


def _intensity(
    *,
    state: OptionsFlowState,
    confidence: OptionsFlowConfidence,
    metrics: dict,
    is_active: bool,
) -> Literal["low", "medium", "high"] | None:
    if not is_active or state == "unavailable":
        return None
    total_premium = float(metrics.get("total_premium") or 0)
    volume = int(metrics.get("recent_contract_volume") or 0)
    if confidence == "high" or total_premium >= 5_000_000 or volume >= 500:
        return "high"
    if confidence == "moderate" or total_premium >= 1_000_000 or volume >= 250:
        return "medium"
    return "low"


def _freshness_days(value: datetime | None, now: datetime) -> int | None:
    if value is None:
        return None
    ts = value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)
    return max((now - ts).days, 0)


def _positive_float(value) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if not isfinite(parsed) or parsed <= 0:
        return None
    return parsed
