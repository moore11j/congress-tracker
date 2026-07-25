from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Any

from app.clients.fmp import FMPClientError, FMPSubscriptionRestrictedError, _request_stable_rows
from app.utils.symbols import normalize_symbol

UNAVAILABLE_MESSAGE = "Valuation inputs are not available for this ticker yet."
TEMPORARILY_UNAVAILABLE_MESSAGE = "Valuation data is temporarily unavailable."

DCF_VALUE_KEYS = (
    "fairValue",
    "fair_value",
    "dcf",
    "DCF",
    "intrinsicValue",
    "intrinsic_value",
    "equityValuePerShare",
    "equity_value_per_share",
    "valuePerShare",
    "stockValue",
)
CURRENT_PRICE_KEYS = (
    "stockPrice",
    "Stock Price",
    "currentPrice",
    "current_price",
    "marketPrice",
    "market_price",
    "price",
)
ACTUAL_CASH_FLOW_KEYS = (
    "freeCashFlow",
    "free_cash_flow",
    "fcf",
    "cashFlow",
    "cash_flow",
    "operatingCashFlow",
    "operating_cash_flow",
    "unleveredFreeCashFlow",
    "unlevered_free_cash_flow",
)
DISCOUNTED_CASH_FLOW_KEYS = (
    "discountedCashFlow",
    "discounted_cash_flow",
    "discountedFreeCashFlow",
    "discounted_free_cash_flow",
    "presentValueOfFreeCashFlow",
    "present_value_of_free_cash_flow",
    "presentValueFCF",
    "pvFreeCashFlow",
    "pv_fcf",
)
ASSUMPTION_KEYS: tuple[tuple[str, str], ...] = (
    ("Revenue growth", "revenueGrowthPct"),
    ("EBITDA margin", "ebitdaPct"),
    ("D&A", "depreciationAndAmortizationPct"),
    ("Cash & investments", "cashAndShortTermInvestmentsPct"),
    ("Receivables", "receivablesPct"),
    ("Inventory", "inventoriesPct"),
    ("Payables", "payablePct"),
    ("EBIT margin", "ebitPct"),
    ("Capex", "capitalExpenditurePct"),
    ("Operating cash flow", "operatingCashFlowPct"),
    ("SG&A", "sellingGeneralAndAdministrativeExpensesPct"),
    ("Tax rate", "taxRate"),
    ("Terminal growth", "longTermGrowthRate"),
    ("Cost of debt", "costOfDebt"),
    ("Cost of equity", "costOfEquity"),
    ("Market risk premium", "marketRiskPremium"),
    ("Beta", "beta"),
    ("Risk-free rate", "riskFreeRate"),
)


def _number(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        parsed = float(value)
        return parsed if math.isfinite(parsed) else None
    if isinstance(value, str):
        cleaned = value.replace("$", "").replace(",", "").replace("%", "").strip()
        if not cleaned:
            return None
        try:
            parsed = float(cleaned)
        except ValueError:
            return None
        return parsed if math.isfinite(parsed) else None
    return None


def _text(value: Any) -> str | None:
    if value is None:
        return None
    trimmed = str(value).strip()
    return trimmed or None


def _first_number(row: dict[str, Any], keys: tuple[str, ...]) -> float | None:
    for key in keys:
        value = _number(row.get(key))
        if value is not None:
            return value
    return None


def _first_row_number(rows: list[dict[str, Any]], keys: tuple[str, ...]) -> float | None:
    for row in reversed(rows):
        value = _first_number(row, keys)
        if value is not None:
            return value
    for row in rows:
        value = _first_number(row, keys)
        if value is not None:
            return value
    return None


def _year(row: dict[str, Any]) -> str | None:
    raw = _text(row.get("year") or row.get("fiscalYear") or row.get("calendarYear") or row.get("date"))
    if not raw:
        return None
    if raw[:4].isdigit():
        return raw[:4]
    return raw


def _target_consensus_row(rows: list[dict[str, Any]]) -> dict[str, Any]:
    row = rows[0] if rows else {}
    return {
        "targetConsensus": _number(row.get("targetConsensus")),
        "targetHigh": _number(row.get("targetHigh")),
        "targetLow": _number(row.get("targetLow")),
        "targetMedian": _number(row.get("targetMedian")),
        "status": "ok" if _number(row.get("targetConsensus")) is not None else "unavailable",
    }


def _cash_flow_points(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    points: list[dict[str, Any]] = []
    for row in rows:
        year = _year(row)
        actual = _first_number(row, ACTUAL_CASH_FLOW_KEYS)
        discounted = _first_number(row, DISCOUNTED_CASH_FLOW_KEYS)
        if year is None or (actual is None and discounted is None):
            continue
        points.append(
            {
                "year": year,
                "actualCashFlow": actual,
                "discountedCashFlow": discounted,
            }
        )
    return points[:8]


def _assumptions(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not rows:
        return []
    merged: dict[str, Any] = {}
    for row in rows:
        merged.update({key: value for key, value in row.items() if value is not None})
    items: list[dict[str, Any]] = []
    for label, key in ASSUMPTION_KEYS:
        value = _number(merged.get(key))
        if value is None:
            continue
        items.append({"label": label, "value": value, "key": key})
    return items


def _judgment(upside_downside_pct: float | None) -> str:
    if upside_downside_pct is None:
        return "Unavailable"
    if upside_downside_pct >= 15:
        return "Undervalued"
    if upside_downside_pct <= -15:
        return "Overvalued"
    return "Fairly valued"


def _method_signals(judgment: str) -> list[dict[str, str]]:
    final_signal = "Neutral"
    if judgment == "Undervalued":
        final_signal = "Bullish"
    elif judgment == "Overvalued":
        final_signal = "Bearish"
    return [
        {"method": "DCF", "signal": final_signal},
        {"method": "Asset / Income", "signal": "Neutral"},
        {"method": "Street comparison", "signal": "Reference only"},
        {"method": "Final valuation", "signal": final_signal},
    ]


def _valuation_from_dcf_rows(symbol: str, rows: list[dict[str, Any]]) -> dict[str, Any]:
    fair_value = _first_row_number(rows, DCF_VALUE_KEYS)
    current_price = _first_row_number(rows, CURRENT_PRICE_KEYS)
    upside_downside_pct = None
    if fair_value is not None and current_price not in (None, 0):
        upside_downside_pct = ((fair_value - float(current_price)) / abs(float(current_price))) * 100

    bear_value = fair_value * 0.85 if fair_value is not None else None
    bull_value = fair_value * 1.15 if fair_value is not None else None
    judgment = _judgment(upside_downside_pct)
    return {
        "symbol": symbol,
        "fairValue": fair_value,
        "bearValue": bear_value,
        "bullValue": bull_value,
        "currentPrice": current_price,
        "upsideDownsidePct": upside_downside_pct,
        "judgment": judgment,
        "method": "Custom DCF Advanced",
        "rangeSource": "dcf_sensitivity" if fair_value is not None else "unavailable",
        "cashFlows": _cash_flow_points(rows),
        "assumptions": _assumptions(rows),
        "methodSignals": _method_signals(judgment),
    }


def _unavailable(symbol: str, *, consensus: dict[str, Any] | None = None, message: str = UNAVAILABLE_MESSAGE) -> dict[str, Any]:
    return {
        "symbol": symbol,
        "status": "unavailable",
        "message": message,
        "dcf": _valuation_from_dcf_rows(symbol, []),
        "consensus": consensus or {"targetConsensus": None, "targetHigh": None, "targetLow": None, "targetMedian": None, "status": "unavailable"},
        "updatedAt": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
    }


def get_ticker_valuation(symbol: str) -> dict[str, Any]:
    normalized_symbol = normalize_symbol(symbol)
    if not normalized_symbol:
        return _unavailable("")

    consensus: dict[str, Any] | None = None
    try:
        consensus_rows = _request_stable_rows(
            "price-target-consensus",
            params={"symbol": normalized_symbol},
            category="valuation:price-target-consensus",
            symbol=normalized_symbol,
            timeout_s=5,
            allow_user_request=True,
        )
        consensus = _target_consensus_row(consensus_rows)
    except (FMPClientError, FMPSubscriptionRestrictedError):
        consensus = {"targetConsensus": None, "targetHigh": None, "targetLow": None, "targetMedian": None, "status": "unavailable"}

    try:
        dcf_rows = _request_stable_rows(
            "custom-discounted-cash-flow",
            params={"symbol": normalized_symbol},
            category="valuation:custom-discounted-cash-flow",
            symbol=normalized_symbol,
            timeout_s=8,
            allow_user_request=True,
        )
    except FMPSubscriptionRestrictedError:
        return _unavailable(normalized_symbol, consensus=consensus, message=TEMPORARILY_UNAVAILABLE_MESSAGE)
    except FMPClientError:
        return _unavailable(normalized_symbol, consensus=consensus, message=TEMPORARILY_UNAVAILABLE_MESSAGE)

    dcf = _valuation_from_dcf_rows(normalized_symbol, dcf_rows)
    if dcf["fairValue"] is None and not dcf["cashFlows"]:
        return _unavailable(normalized_symbol, consensus=consensus)

    return {
        "symbol": normalized_symbol,
        "status": "ok" if dcf["fairValue"] is not None else "partial",
        "message": None,
        "dcf": dcf,
        "consensus": consensus or {"targetConsensus": None, "targetHigh": None, "targetLow": None, "targetMedian": None, "status": "unavailable"},
        "updatedAt": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
    }
