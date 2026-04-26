from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Literal

from sqlalchemy.orm import Session

from app.services.price_lookup import get_daily_close_series_with_fallback

logger = logging.getLogger(__name__)

IndicatorSignal = Literal["bullish", "bearish", "neutral", "unavailable"]
IndicatorStatus = Literal["ok", "unavailable"]


def _ema(values: list[float], period: int) -> list[float]:
    if not values:
        return []
    multiplier = 2 / (period + 1)
    ema_values = [values[0]]
    for value in values[1:]:
        ema_values.append((value - ema_values[-1]) * multiplier + ema_values[-1])
    return ema_values


def _rsi(values: list[float], period: int = 14) -> float | None:
    if len(values) <= period:
        return None

    gains: list[float] = []
    losses: list[float] = []
    for idx in range(1, len(values)):
        delta = values[idx] - values[idx - 1]
        gains.append(max(delta, 0.0))
        losses.append(max(-delta, 0.0))

    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period

    for idx in range(period, len(gains)):
        avg_gain = ((avg_gain * (period - 1)) + gains[idx]) / period
        avg_loss = ((avg_loss * (period - 1)) + losses[idx]) / period

    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def _indicator_payload(
    *,
    status: IndicatorStatus,
    signal: IndicatorSignal,
    message: str,
    reason: str | None = None,
    value: float | None = None,
    extra: dict | None = None,
) -> dict:
    payload = {
        "status": status,
        "signal": signal,
        "message": message,
        "reason": reason,
        "value": value,
    }
    if isinstance(extra, dict):
        payload.update(extra)
    return payload


def _rsi_indicator(symbol: str, closes: list[float]) -> dict:
    value = _rsi(closes, 14)
    if value is None:
        reason = "provider_error" if len(closes) == 0 else "insufficient_price_history"
        logger.info("technical_indicator_missing symbol=%s indicator=RSI reason=%s", symbol, reason)
        message = "RSI temporarily unavailable" if reason == "provider_error" else "RSI unavailable - insufficient price history"
        return _indicator_payload(
            status="unavailable",
            signal="unavailable",
            message=message,
            reason=reason,
            extra={"period": 14},
        )
    if value > 55:
        signal: IndicatorSignal = "bullish"
        message = "RSI above neutral"
    elif value < 45:
        signal = "bearish"
        message = "RSI below neutral"
    else:
        signal = "neutral"
        message = "RSI near neutral"
    return _indicator_payload(
        status="ok",
        signal=signal,
        message=message,
        value=round(value, 2),
        extra={"period": 14},
    )


def _macd_indicator(closes: list[float]) -> dict:
    if len(closes) < 35:
        message = "MACD temporarily unavailable" if len(closes) == 0 else "MACD unavailable - insufficient price history"
        reason = "provider_error" if len(closes) == 0 else "insufficient_price_history"
        return _indicator_payload(status="unavailable", signal="unavailable", message=message, reason=reason)

    ema12 = _ema(closes, 12)
    ema26 = _ema(closes, 26)
    macd_line = [short - long for short, long in zip(ema12, ema26)]
    signal_series = _ema(macd_line, 9)
    macd_value = macd_line[-1]
    signal_value = signal_series[-1]
    histogram = macd_value - signal_value
    if macd_value > signal_value:
        signal: IndicatorSignal = "bullish"
        message = "MACD bullish crossover"
    elif macd_value < signal_value:
        signal = "bearish"
        message = "MACD bearish crossover"
    else:
        signal = "neutral"
        message = "MACD mixed"
    return _indicator_payload(
        status="ok",
        signal=signal,
        message=message,
        extra={
            "macd": round(macd_value, 4),
            "signal_line": round(signal_value, 4),
            "histogram": round(histogram, 4),
        },
    )


def _ema_trend_indicator(closes: list[float]) -> dict:
    if len(closes) < 26:
        message = "EMA trend temporarily unavailable" if len(closes) == 0 else "EMA trend unavailable - insufficient price history"
        reason = "provider_error" if len(closes) == 0 else "insufficient_price_history"
        return _indicator_payload(status="unavailable", signal="unavailable", message=message, reason=reason)

    short_ema = _ema(closes, 12)[-1]
    medium_ema = _ema(closes, 26)[-1]
    if short_ema > medium_ema:
        signal: IndicatorSignal = "bullish"
        message = "Short EMA above medium EMA"
    elif short_ema < medium_ema:
        signal = "bearish"
        message = "Short EMA below medium EMA"
    else:
        signal = "neutral"
        message = "EMA trend mixed"
    return _indicator_payload(
        status="ok",
        signal=signal,
        message=message,
        extra={
            "short_period": 12,
            "medium_period": 26,
            "short_ema": round(short_ema, 4),
            "medium_ema": round(medium_ema, 4),
        },
    )


def build_ticker_technical_indicators(
    db: Session,
    symbol: str,
    *,
    lookback_days: int = 90,
) -> dict:
    normalized_symbol = (symbol or "").strip().upper()
    now = datetime.now(timezone.utc)
    end_date = now.date()
    start_date = end_date - timedelta(days=max(lookback_days - 1, 0))
    price_map = get_daily_close_series_with_fallback(
        db,
        normalized_symbol,
        start_date.isoformat(),
        end_date.isoformat(),
    )
    ordered_days = sorted(price_map)
    closes = [float(price_map[day]) for day in ordered_days]

    return {
        "source": "daily_close_history",
        "asof": ordered_days[-1] if ordered_days else None,
        "price_points": len(closes),
        "rsi": _rsi_indicator(normalized_symbol, closes),
        "macd": _macd_indicator(closes),
        "ema_trend": _ema_trend_indicator(closes),
    }
