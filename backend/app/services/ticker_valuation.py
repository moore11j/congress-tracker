from __future__ import annotations

import math
import os
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
DCF_INPUT_PARAM_KEYS = tuple(key for _, key in ASSUMPTION_KEYS)
RATIO_ASSUMPTION_KEYS = {
    "revenueGrowthPct",
    "ebitdaPct",
    "depreciationAndAmortizationPct",
    "cashAndShortTermInvestmentsPct",
    "receivablesPct",
    "inventoriesPct",
    "payablePct",
    "ebitPct",
    "capitalExpenditurePct",
    "operatingCashFlowPct",
    "sellingGeneralAndAdministrativeExpensesPct",
    "taxRate",
}
DEFAULT_DCF_ASSUMPTIONS = {
    "revenueGrowthPct": 0.03,
    "ebitdaPct": 0.20,
    "depreciationAndAmortizationPct": 0.04,
    "cashAndShortTermInvestmentsPct": 0.05,
    "receivablesPct": 0.15,
    "inventoriesPct": 0.10,
    "payablePct": 0.10,
    "ebitPct": 0.15,
    "capitalExpenditurePct": 0.04,
    "operatingCashFlowPct": 0.18,
    "sellingGeneralAndAdministrativeExpensesPct": 0.12,
    "taxRate": 0.21,
    "longTermGrowthRate": 3.0,
    "costOfDebt": 4.5,
    "costOfEquity": 9.5,
    "marketRiskPremium": 4.72,
    "beta": 1.0,
    "riskFreeRate": 4.43,
}


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


def _env_float(name: str, fallback: float) -> float:
    parsed = _number(os.getenv(name))
    return parsed if parsed is not None else fallback


def _clamp(value: float | None, low: float, high: float) -> float | None:
    if value is None:
        return None
    return min(max(value, low), high)


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


def _latest_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(rows, key=lambda row: _text(row.get("date") or row.get("calendarYear") or row.get("fiscalYear") or row.get("year")) or "")


def _latest_row(rows: list[dict[str, Any]]) -> dict[str, Any]:
    ordered = _latest_rows(rows)
    return ordered[-1] if ordered else {}


def _ratio(numerator: float | None, denominator: float | None, *, absolute: bool = False) -> float | None:
    if numerator is None or denominator is None or denominator == 0:
        return None
    value = abs(numerator) if absolute else numerator
    ratio = value / abs(denominator)
    return ratio if math.isfinite(ratio) else None


def _positive_price(value: float | None) -> float | None:
    if value is None:
        return None
    return value if value > 0 else None


def _non_negative_price(value: float | None) -> float | None:
    if value is None:
        return None
    return max(value, 0.0)


def _year(row: dict[str, Any]) -> str | None:
    raw = _text(row.get("year") or row.get("fiscalYear") or row.get("calendarYear") or row.get("date"))
    if not raw:
        return None
    if raw[:4].isdigit():
        return raw[:4]
    return raw


def _revenue_estimate(row: dict[str, Any]) -> float | None:
    return _first_number(row, (
        "revenueAvg",
        "revenueAverage",
        "estimatedRevenueAvg",
        "estimatedRevenueAverage",
        "revenueEstimate",
        "estimatedRevenue",
    ))


def _fiscal_year(row: dict[str, Any]) -> int | None:
    raw = _text(row.get("fiscalYear") or row.get("calendarYear") or row.get("year") or row.get("date"))
    if raw and raw[:4].isdigit():
        return int(raw[:4])
    return None


def _forward_revenue_growth_pct(income_rows: list[dict[str, Any]], estimate_rows: list[dict[str, Any]]) -> float | None:
    latest_income = _latest_row(income_rows)
    base_revenue = _first_number(latest_income, ("revenue", "totalRevenue"))
    if base_revenue is not None and base_revenue > 0:
        base_year = _fiscal_year(latest_income)
        candidates = []
        for row in estimate_rows:
            estimate = _revenue_estimate(row)
            year = _fiscal_year(row)
            if estimate is None or estimate <= 0:
                continue
            if base_year is not None and year is not None and year < base_year:
                continue
            candidates.append((year if year is not None else 9999, estimate))
        if candidates:
            _, next_revenue = sorted(candidates, key=lambda item: item[0])[0]
            growth = (next_revenue - base_revenue) / base_revenue
            if math.isfinite(growth):
                return _clamp(growth, -0.50, 5.00)

    ordered = _latest_rows(income_rows)
    if len(ordered) >= 2:
        previous = _first_number(ordered[-2], ("revenue", "totalRevenue"))
        latest = _first_number(ordered[-1], ("revenue", "totalRevenue"))
        if previous is not None and previous > 0 and latest is not None:
            growth = (latest - previous) / previous
            if math.isfinite(growth):
                return _clamp(growth, -0.50, 5.00)
    return None


def _fetch_dcf_input_rows(symbol: str) -> dict[str, list[dict[str, Any]]]:
    specs: dict[str, tuple[str, dict[str, Any]]] = {
        "annual_income": ("income-statement", {"symbol": symbol, "period": "annual", "page": 0, "limit": 6}),
        "annual_cash": ("cash-flow-statement", {"symbol": symbol, "period": "annual", "page": 0, "limit": 6}),
        "annual_balance": ("balance-sheet-statement", {"symbol": symbol, "period": "annual", "page": 0, "limit": 6}),
        "annual_estimates": ("analyst-estimates", {"symbol": symbol, "period": "annual", "page": 0, "limit": 10}),
        "profile": ("profile", {"symbol": symbol}),
    }
    rows_by_key: dict[str, list[dict[str, Any]]] = {key: [] for key in specs}
    for key, (endpoint, params) in specs.items():
        try:
            rows_by_key[key] = _request_stable_rows(
                endpoint,
                params=params,
                category=f"valuation:dcf-inputs:{endpoint}",
                symbol=symbol,
                timeout_s=5,
                allow_user_request=True,
            )
        except (FMPClientError, FMPSubscriptionRestrictedError):
            rows_by_key[key] = []
    return rows_by_key


def _walnut_dcf_assumptions(symbol: str) -> dict[str, float]:
    rows = _fetch_dcf_input_rows(symbol)
    income = _latest_row(rows["annual_income"])
    cash = _latest_row(rows["annual_cash"])
    balance = _latest_row(rows["annual_balance"])
    profile = rows["profile"][0] if rows["profile"] else {}

    revenue = _first_number(income, ("revenue", "totalRevenue"))
    pretax_income = _first_number(income, ("incomeBeforeTax", "incomeBeforeTaxRatio"))
    tax_expense = _first_number(income, ("incomeTaxExpense", "taxProvision"))
    interest_expense = _first_number(income, ("interestExpense", "interestAndDebtExpense"))
    total_debt = _first_number(balance, ("totalDebt", "shortTermDebtAndCapitalLeaseObligations", "longTermDebt"))

    risk_free_rate = _clamp(_env_float("WALNUT_DCF_RISK_FREE_RATE", DEFAULT_DCF_ASSUMPTIONS["riskFreeRate"]), 0.0, 12.0) or DEFAULT_DCF_ASSUMPTIONS["riskFreeRate"]
    market_risk_premium = _clamp(_env_float("WALNUT_DCF_MARKET_RISK_PREMIUM", DEFAULT_DCF_ASSUMPTIONS["marketRiskPremium"]), 0.0, 12.0) or DEFAULT_DCF_ASSUMPTIONS["marketRiskPremium"]
    beta = _clamp(_number(profile.get("beta")), 0.5, 2.5) or DEFAULT_DCF_ASSUMPTIONS["beta"]
    cost_of_equity = _clamp(risk_free_rate + beta * market_risk_premium, 0.0, 30.0) or DEFAULT_DCF_ASSUMPTIONS["costOfEquity"]

    computed: dict[str, float | None] = {
        "revenueGrowthPct": _forward_revenue_growth_pct(rows["annual_income"], rows["annual_estimates"]),
        "ebitdaPct": _ratio(_first_number(income, ("ebitda", "EBITDA")), revenue),
        "depreciationAndAmortizationPct": _ratio(_first_number(cash, ("depreciationAndAmortization", "depreciationAndAmortizationExpense")), revenue, absolute=True),
        "cashAndShortTermInvestmentsPct": _ratio(_first_number(balance, ("cashAndShortTermInvestments", "cashAndCashEquivalents")), revenue),
        "receivablesPct": _ratio(_first_number(balance, ("netReceivables", "accountsReceivables", "accountReceivables")), revenue),
        "inventoriesPct": _ratio(_first_number(balance, ("inventory", "inventories")), revenue),
        "payablePct": _ratio(_first_number(balance, ("accountPayables", "accountsPayable", "payables")), revenue),
        "ebitPct": _ratio(_first_number(income, ("ebit", "operatingIncome")), revenue),
        "capitalExpenditurePct": _ratio(_first_number(cash, ("capitalExpenditure", "capitalExpenditures")), revenue, absolute=True),
        "operatingCashFlowPct": _ratio(_first_number(cash, ("operatingCashFlow", "netCashProvidedByOperatingActivities")), revenue),
        "sellingGeneralAndAdministrativeExpensesPct": _ratio(_first_number(income, ("sellingGeneralAndAdministrativeExpenses", "sellingAndMarketingExpenses")), revenue, absolute=True),
        "taxRate": _clamp(_ratio(tax_expense, pretax_income, absolute=False), 0.0, 1.0),
        "longTermGrowthRate": _clamp(_env_float("WALNUT_DCF_LONG_TERM_GROWTH_RATE", DEFAULT_DCF_ASSUMPTIONS["longTermGrowthRate"]), 0.0, 6.0),
        "costOfDebt": _clamp(_ratio(interest_expense, total_debt, absolute=True) * 100 if _ratio(interest_expense, total_debt, absolute=True) is not None else None, 0.0, 25.0),
        "costOfEquity": cost_of_equity,
        "marketRiskPremium": market_risk_premium,
        "beta": beta,
        "riskFreeRate": risk_free_rate,
    }

    assumptions: dict[str, float] = {}
    for key in DCF_INPUT_PARAM_KEYS:
        value = computed.get(key)
        if value is None or not math.isfinite(value):
            value = DEFAULT_DCF_ASSUMPTIONS[key]
        if key in RATIO_ASSUMPTION_KEYS and key != "revenueGrowthPct":
            value = _clamp(value, 0.0, 2.0) or 0.0
        assumptions[key] = round(float(value), 6)
    return assumptions


def _target_consensus_row(rows: list[dict[str, Any]]) -> dict[str, Any]:
    row = rows[0] if rows else {}
    target_consensus = _positive_price(_number(row.get("targetConsensus")))
    return {
        "targetConsensus": target_consensus,
        "targetHigh": _positive_price(_number(row.get("targetHigh"))),
        "targetLow": _positive_price(_number(row.get("targetLow"))),
        "targetMedian": _positive_price(_number(row.get("targetMedian"))),
        "status": "ok" if target_consensus is not None else "unavailable",
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


def _assumptions(rows: list[dict[str, Any]], inputs: dict[str, float] | None = None) -> list[dict[str, Any]]:
    merged: dict[str, Any] = {}
    for row in rows:
        merged.update({key: value for key, value in row.items() if value is not None})
    if inputs:
        merged.update(inputs)
    items: list[dict[str, Any]] = []
    for label, key in ASSUMPTION_KEYS:
        value = _number(merged.get(key))
        if value is None:
            continue
        if key == "taxRate":
            value = max(0.0, min(value, 100.0))
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


def _valuation_from_dcf_rows(symbol: str, rows: list[dict[str, Any]], *, inputs: dict[str, float] | None = None) -> dict[str, Any]:
    fair_value = _non_negative_price(_first_row_number(rows, DCF_VALUE_KEYS))
    current_price = _positive_price(_first_row_number(rows, CURRENT_PRICE_KEYS))
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
        "assumptions": _assumptions(rows, inputs),
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
        dcf_inputs = _walnut_dcf_assumptions(normalized_symbol)
        dcf_rows = _request_stable_rows(
            "custom-discounted-cash-flow",
            params={"symbol": normalized_symbol, **dcf_inputs},
            category="valuation:custom-discounted-cash-flow",
            symbol=normalized_symbol,
            timeout_s=8,
            allow_user_request=True,
        )
    except FMPSubscriptionRestrictedError:
        return _unavailable(normalized_symbol, consensus=consensus, message=TEMPORARILY_UNAVAILABLE_MESSAGE)
    except FMPClientError:
        return _unavailable(normalized_symbol, consensus=consensus, message=TEMPORARILY_UNAVAILABLE_MESSAGE)

    dcf = _valuation_from_dcf_rows(normalized_symbol, dcf_rows, inputs=dcf_inputs)
    if dcf["fairValue"] is None and not dcf["cashFlows"] and not dcf["assumptions"]:
        return _unavailable(normalized_symbol, consensus=consensus)

    return {
        "symbol": normalized_symbol,
        "status": "ok" if dcf["fairValue"] is not None else "partial",
        "message": None,
        "dcf": dcf,
        "consensus": consensus or {"targetConsensus": None, "targetHigh": None, "targetLow": None, "targetMedian": None, "status": "unavailable"},
        "updatedAt": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
    }
