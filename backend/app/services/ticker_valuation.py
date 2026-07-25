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
RATIO_ASSUMPTION_LIMITS = {
    "revenueGrowthPct": (-0.30, 0.35),
    "ebitdaPct": (0.0, 0.75),
    "depreciationAndAmortizationPct": (0.0, 0.35),
    "cashAndShortTermInvestmentsPct": (0.0, 1.0),
    "receivablesPct": (0.0, 1.0),
    "inventoriesPct": (0.0, 1.0),
    "payablePct": (0.0, 1.0),
    "ebitPct": (0.0, 0.65),
    "capitalExpenditurePct": (0.0, 0.75),
    "operatingCashFlowPct": (0.0, 0.75),
    "sellingGeneralAndAdministrativeExpensesPct": (0.0, 1.0),
    "taxRate": (0.0, 0.50),
}
DCF_BETA_MIN = 1.0
DCF_BETA_MAX = 1.8
PRE_PROFIT_DCF_BETA = 1.8
QUALITY_COMPOUNDER_DCF_BETA_MAX = 1.35
CASH_RETURN_GROWTH_ADJUSTMENT_MAX = 0.06
FAIR_VALUE_MODEL_WEIGHT = 0.5
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


def _first_available_row(*row_groups: list[dict[str, Any]]) -> dict[str, Any]:
    for rows in row_groups:
        row = _latest_row(rows)
        if row:
            return row
    return {}


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


def _sum_numbers(row: dict[str, Any], keys: tuple[str, ...]) -> float | None:
    values = [_number(row.get(key)) for key in keys]
    present = [value for value in values if value is not None]
    if not present:
        return None
    total = sum(present)
    return total if math.isfinite(total) else None


def _shares_outstanding(balance: dict[str, Any], profile: dict[str, Any]) -> float | None:
    shares = _first_number(
        profile,
        (
            "sharesOutstanding",
            "weightedAverageShsOutDil",
            "weightedAverageShsOut",
            "weightedAverageSharesDiluted",
            "commonStockSharesOutstanding",
        ),
    ) or _first_number(balance, ("commonStockSharesOutstanding", "weightedAverageShsOutDil", "weightedAverageShsOut"))
    if shares is None:
        market_cap = _first_number(profile, ("marketCap", "mktCap"))
        price = _first_number(profile, ("price", "currentPrice"))
        shares = _ratio(market_cap, price)
    return shares if shares is not None and shares > 0 else None


def _asset_nav_per_share(balance: dict[str, Any], profile: dict[str, Any]) -> float | None:
    equity = _first_number(balance, ("totalStockholdersEquity", "totalEquity", "totalShareholderEquity"))
    if equity is None:
        assets = _first_number(balance, ("totalAssets",))
        liabilities = _first_number(balance, ("totalLiabilities",))
        if assets is not None and liabilities is not None:
            equity = assets - liabilities
    shares = _shares_outstanding(balance, profile)
    if equity is None or equity <= 0 or shares is None or shares <= 0:
        return None
    nav = equity / shares
    return nav if math.isfinite(nav) and nav > 0 else None


def _retained_cash_flow_per_share(cash: dict[str, Any], balance: dict[str, Any], profile: dict[str, Any]) -> float | None:
    shares = _shares_outstanding(balance, profile)
    if shares is None:
        return None
    free_cash_flow = _first_number(cash, ("freeCashFlow", "free_cash_flow", "fcf"))
    if free_cash_flow is None:
        operating_cash_flow = _first_number(cash, ("operatingCashFlow", "netCashProvidedByOperatingActivities"))
        capital_expenditure = _first_number(cash, ("capitalExpenditure", "capitalExpenditures"))
        if operating_cash_flow is not None:
            free_cash_flow = operating_cash_flow + (capital_expenditure or 0.0)
    if free_cash_flow is None:
        return None
    dividends = abs(_first_number(cash, ("dividendsPaid", "commonDividendsPaid")) or 0.0)
    buybacks = abs(_first_number(cash, ("commonStockRepurchased", "repurchasesOfCommonStock", "stockRepurchased")) or 0.0)
    retained_cash_flow = free_cash_flow - dividends - buybacks
    value = retained_cash_flow / shares
    return value if math.isfinite(value) else None


def _market_cap(balance: dict[str, Any], profile: dict[str, Any]) -> float | None:
    market_cap = _first_number(profile, ("marketCap", "mktCap"))
    if market_cap is not None and market_cap > 0:
        return market_cap
    shares = _shares_outstanding(balance, profile)
    price = _first_number(profile, ("price", "currentPrice"))
    if shares is None or price is None or price <= 0:
        return None
    value = shares * price
    return value if math.isfinite(value) and value > 0 else None


def _shareholder_return_yield(cash: dict[str, Any], balance: dict[str, Any], profile: dict[str, Any]) -> float | None:
    market_cap = _market_cap(balance, profile)
    if market_cap is None:
        return None
    dividends = abs(_first_number(cash, ("dividendsPaid", "commonDividendsPaid")) or 0.0)
    buybacks = abs(_first_number(cash, ("commonStockRepurchased", "repurchasesOfCommonStock", "stockRepurchased")) or 0.0)
    total_return = dividends + buybacks
    if total_return <= 0:
        return None
    value = total_return / market_cap
    return value if math.isfinite(value) and value > 0 else None


def _positive_price(value: float | None) -> float | None:
    if value is None:
        return None
    return value if value > 0 else None


def _anchored_fair_value(model_value: float | None, current_price: float | None) -> float | None:
    if model_value is None:
        return None
    if current_price is None:
        return model_value
    return (model_value * FAIR_VALUE_MODEL_WEIGHT) + (float(current_price) * (1 - FAIR_VALUE_MODEL_WEIGHT))


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


def _revenue_estimate_points(estimate_rows: list[dict[str, Any]]) -> list[tuple[int, float]]:
    by_year: dict[int, float] = {}
    for row in estimate_rows:
        estimate = _revenue_estimate(row)
        year = _fiscal_year(row)
        if estimate is None or estimate <= 0 or year is None:
            continue
        by_year.setdefault(year, estimate)
    return sorted(by_year.items(), key=lambda item: item[0])


def _annualized_revenue_growth(start_revenue: float | None, end_revenue: float | None, years: int) -> float | None:
    if start_revenue is None or end_revenue is None or start_revenue <= 0 or end_revenue <= 0 or years <= 0:
        return None
    if years == 1:
        growth = (end_revenue - start_revenue) / start_revenue
    else:
        growth = (end_revenue / start_revenue) ** (1 / years) - 1
    if not math.isfinite(growth):
        return None
    low, high = RATIO_ASSUMPTION_LIMITS["revenueGrowthPct"]
    return _clamp(growth, low, high)


def _forward_revenue_growth_pct(income_rows: list[dict[str, Any]], estimate_rows: list[dict[str, Any]]) -> float | None:
    latest_income = _latest_row(income_rows)
    base_revenue = _first_number(latest_income, ("revenue", "totalRevenue"))
    estimate_points = _revenue_estimate_points(estimate_rows)
    if base_revenue is not None and base_revenue > 0:
        base_year = _fiscal_year(latest_income)
        candidates = [(year, estimate) for year, estimate in estimate_points if base_year is None or year > base_year]
        if candidates:
            target_year, target_revenue = candidates[-1]
            years = target_year - base_year if base_year is not None else len(candidates)
            growth = _annualized_revenue_growth(base_revenue, target_revenue, max(years, 1))
            if growth is not None:
                return growth

    if len(estimate_points) >= 2:
        base_year, base_estimate = estimate_points[0]
        target_year, target_estimate = estimate_points[-1]
        growth = _annualized_revenue_growth(base_estimate, target_estimate, max(target_year - base_year, 1))
        if growth is not None:
            return growth

    ordered = _latest_rows(income_rows)
    if len(ordered) >= 2:
        previous = _first_number(ordered[-2], ("revenue", "totalRevenue"))
        latest = _first_number(ordered[-1], ("revenue", "totalRevenue"))
        if previous is not None and previous > 0 and latest is not None:
            growth = (latest - previous) / previous
            if math.isfinite(growth):
                low, high = RATIO_ASSUMPTION_LIMITS["revenueGrowthPct"]
                return _clamp(growth, low, high)
    return None


def _statement_growth(row: dict[str, Any], keys: tuple[str, ...]) -> float | None:
    value = _first_number(row, keys)
    if value is None:
        return None
    low, high = RATIO_ASSUMPTION_LIMITS["revenueGrowthPct"]
    return _clamp(value, low, high)


def _first_present(*values: float | None) -> float | None:
    for value in values:
        if value is not None:
            return value
    return None


def _fetch_dcf_input_rows(symbol: str) -> dict[str, list[dict[str, Any]]]:
    specs: dict[str, tuple[str, dict[str, Any]]] = {
        "ttm_income": ("income-statement-ttm", {"symbol": symbol}),
        "ttm_cash": ("cash-flow-statement-ttm", {"symbol": symbol}),
        "ttm_balance": ("balance-sheet-statement-ttm", {"symbol": symbol}),
        "annual_income": ("income-statement", {"symbol": symbol, "period": "annual", "page": 0, "limit": 6}),
        "annual_cash": ("cash-flow-statement", {"symbol": symbol, "period": "annual", "page": 0, "limit": 6}),
        "annual_balance": ("balance-sheet-statement", {"symbol": symbol, "period": "annual", "page": 0, "limit": 6}),
        "annual_estimates": ("analyst-estimates", {"symbol": symbol, "period": "annual", "page": 0, "limit": 10}),
        "income_growth": ("income-statement-growth", {"symbol": symbol, "period": "annual", "limit": 2}),
        "cash_growth": ("cash-flow-statement-growth", {"symbol": symbol, "period": "annual", "limit": 2}),
        "balance_growth": ("balance-sheet-statement-growth", {"symbol": symbol, "period": "annual", "limit": 2}),
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


def _walnut_valuation_model(symbol: str) -> dict[str, Any]:
    rows = _fetch_dcf_input_rows(symbol)
    income = _first_available_row(rows["ttm_income"], rows["annual_income"])
    cash = _first_available_row(rows["ttm_cash"], rows["annual_cash"])
    balance = _first_available_row(rows["ttm_balance"], rows["annual_balance"])
    income_growth = _latest_row(rows["income_growth"])
    profile = rows["profile"][0] if rows["profile"] else {}

    revenue = _first_number(income, ("revenue", "totalRevenue"))
    ebit = _first_number(income, ("ebit", "operatingIncome"))
    ebitda = _first_number(income, ("ebitda", "EBITDA"))
    operating_cash_flow = _first_number(cash, ("operatingCashFlow", "netCashProvidedByOperatingActivities"))
    capital_expenditure_pct = _ratio(_first_number(cash, ("capitalExpenditure", "capitalExpenditures")), revenue, absolute=True)
    ebit_pct = _ratio(ebit, revenue)
    ebitda_pct = _ratio(ebitda, revenue)
    operating_cash_flow_pct = _ratio(operating_cash_flow, revenue)
    pretax_income = _first_number(income, ("incomeBeforeTax", "incomeBeforeTaxTtm"))
    tax_expense = _first_number(income, ("incomeTaxExpense", "taxProvision"))
    interest_expense = _first_number(income, ("interestExpense", "interestAndDebtExpense"))
    total_debt = _first_number(balance, ("totalDebt", "totalDebtTtm"))
    if total_debt is None:
        total_debt = _sum_numbers(balance, ("shortTermDebtAndCapitalLeaseObligations", "shortTermDebt", "longTermDebt"))
    is_pre_profit = (ebit is not None and ebit <= 0) or (pretax_income is not None and pretax_income <= 0)
    total_assets = _first_number(balance, ("totalAssets",))
    cash_and_investments = _first_number(balance, ("cashAndShortTermInvestments", "cashAndCashEquivalents", "shortTermInvestments"))
    nav_per_share = _asset_nav_per_share(balance, profile)
    retained_cash_flow_per_share = _retained_cash_flow_per_share(cash, balance, profile)
    shareholder_return_yield = _shareholder_return_yield(cash, balance, profile)
    asset_to_revenue = _ratio(total_assets, revenue)
    cash_asset_ratio = _ratio(cash_and_investments, total_assets)
    is_asset_heavy = bool(
        nav_per_share is not None
        and is_pre_profit
        and (operating_cash_flow_pct or 0.0) <= 0.10
        and ((asset_to_revenue or 0.0) >= 2.0 or (cash_asset_ratio or 0.0) >= 0.50)
    )
    is_quality_compounder = (
        not is_pre_profit
        and (ebitda_pct or 0) >= 0.50
        and (ebit_pct or 0) >= 0.40
        and (operating_cash_flow_pct or 0) >= 0.30
        and (capital_expenditure_pct or 1) <= 0.10
    )
    is_cash_return_compounder = (
        not is_pre_profit
        and (ebit_pct or 0) >= 0.20
        and (operating_cash_flow_pct or 0) >= 0.20
        and (capital_expenditure_pct or 1) <= 0.08
        and (shareholder_return_yield or 0) >= 0.02
    )

    risk_free_rate = _clamp(_env_float("WALNUT_DCF_RISK_FREE_RATE", DEFAULT_DCF_ASSUMPTIONS["riskFreeRate"]), 0.0, 12.0) or DEFAULT_DCF_ASSUMPTIONS["riskFreeRate"]
    market_risk_premium = _clamp(_env_float("WALNUT_DCF_MARKET_RISK_PREMIUM", DEFAULT_DCF_ASSUMPTIONS["marketRiskPremium"]), 0.0, 12.0) or DEFAULT_DCF_ASSUMPTIONS["marketRiskPremium"]
    beta = _clamp(_number(profile.get("beta")), DCF_BETA_MIN, DCF_BETA_MAX) or DEFAULT_DCF_ASSUMPTIONS["beta"]
    if is_pre_profit:
        beta = max(beta, PRE_PROFIT_DCF_BETA)
    elif is_quality_compounder:
        beta = min(beta, QUALITY_COMPOUNDER_DCF_BETA_MAX)
    cost_of_equity = _clamp(risk_free_rate + beta * market_risk_premium, 0.0, 30.0) or DEFAULT_DCF_ASSUMPTIONS["costOfEquity"]
    discounted_retained_cash_flow_per_share = (
        retained_cash_flow_per_share / (1 + cost_of_equity / 100) if retained_cash_flow_per_share is not None else None
    )
    asset_value = (
        nav_per_share + (discounted_retained_cash_flow_per_share or 0.0)
        if nav_per_share is not None
        else None
    )

    debt_cost_ratio = _ratio(interest_expense, total_debt, absolute=True)
    revenue_base_rows = rows["annual_income"] or rows["ttm_income"]
    computed: dict[str, float | None] = {
        "revenueGrowthPct": _first_present(
            _forward_revenue_growth_pct(revenue_base_rows, rows["annual_estimates"]),
            _statement_growth(income_growth, ("growthRevenue", "revenueGrowth", "growthRevenueTtm")),
        ),
        "ebitdaPct": ebitda_pct,
        "depreciationAndAmortizationPct": _ratio(_first_number(cash, ("depreciationAndAmortization", "depreciationAndAmortizationExpense")), revenue, absolute=True),
        "cashAndShortTermInvestmentsPct": _ratio(_first_number(balance, ("cashAndShortTermInvestments", "cashAndCashEquivalents")), revenue),
        "receivablesPct": _ratio(_first_number(balance, ("netReceivables", "accountsReceivables", "accountReceivables")), revenue),
        "inventoriesPct": _ratio(_first_number(balance, ("inventory", "inventories")), revenue),
        "payablePct": _ratio(_first_number(balance, ("accountPayables", "accountsPayable", "payables")), revenue),
        "ebitPct": ebit_pct,
        "capitalExpenditurePct": capital_expenditure_pct,
        "operatingCashFlowPct": operating_cash_flow_pct,
        "sellingGeneralAndAdministrativeExpensesPct": _ratio(_first_number(income, ("sellingGeneralAndAdministrativeExpenses", "sellingAndMarketingExpenses")), revenue, absolute=True),
        "taxRate": _clamp(_ratio(tax_expense, pretax_income, absolute=False), 0.0, 1.0),
        "longTermGrowthRate": _clamp(_env_float("WALNUT_DCF_LONG_TERM_GROWTH_RATE", DEFAULT_DCF_ASSUMPTIONS["longTermGrowthRate"]), 0.0, 6.0),
        "costOfDebt": _clamp(debt_cost_ratio * 100 if debt_cost_ratio is not None else None, 0.0, 25.0),
        "costOfEquity": cost_of_equity,
        "marketRiskPremium": market_risk_premium,
        "beta": beta,
        "riskFreeRate": risk_free_rate,
    }
    if is_pre_profit:
        computed["revenueGrowthPct"] = min(computed["revenueGrowthPct"] or DEFAULT_DCF_ASSUMPTIONS["revenueGrowthPct"], 0.20)
        computed["ebitdaPct"] = min(computed["ebitdaPct"] or 0.0, 0.15)
        computed["ebitPct"] = 0.0
        computed["operatingCashFlowPct"] = min(computed["operatingCashFlowPct"] or 0.0, 0.05)
        computed["taxRate"] = max(computed["taxRate"] or 0.0, DEFAULT_DCF_ASSUMPTIONS["taxRate"])
    elif is_cash_return_compounder:
        buyback_adjusted_growth = (computed["revenueGrowthPct"] or DEFAULT_DCF_ASSUMPTIONS["revenueGrowthPct"]) + min(
            shareholder_return_yield or 0.0,
            CASH_RETURN_GROWTH_ADJUSTMENT_MAX,
        )
        computed["revenueGrowthPct"] = buyback_adjusted_growth

    assumptions: dict[str, float] = {}
    for key in DCF_INPUT_PARAM_KEYS:
        value = computed.get(key)
        if value is None or not math.isfinite(value):
            value = DEFAULT_DCF_ASSUMPTIONS[key]
        limits = RATIO_ASSUMPTION_LIMITS.get(key)
        if limits is not None:
            value = _clamp(value, limits[0], limits[1]) or 0.0
        assumptions[key] = round(float(value), 6)
    return {
        "assumptions": assumptions,
        "assetValue": round(float(asset_value), 6) if is_asset_heavy and asset_value is not None else None,
    }


def _walnut_dcf_assumptions(symbol: str) -> dict[str, float]:
    return _walnut_valuation_model(symbol)["assumptions"]


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


def _method_signals(judgment: str, *, method: str = "Custom DCF Advanced") -> list[dict[str, str]]:
    final_signal = "Neutral"
    if judgment == "Undervalued":
        final_signal = "Bullish"
    elif judgment == "Overvalued":
        final_signal = "Bearish"
    if method == "Asset / NAV":
        return [
            {"method": "DCF", "signal": "Cash-flow limited"},
            {"method": "Asset / NAV", "signal": final_signal},
            {"method": "Street comparison", "signal": "Reference only"},
            {"method": "Final valuation", "signal": final_signal},
        ]
    return [
        {"method": "DCF", "signal": final_signal},
        {"method": "Asset / Income", "signal": "Neutral"},
        {"method": "Street comparison", "signal": "Reference only"},
        {"method": "Final valuation", "signal": final_signal},
    ]


def _valuation_from_dcf_rows(symbol: str, rows: list[dict[str, Any]], *, inputs: dict[str, float] | None = None, asset_value: float | None = None) -> dict[str, Any]:
    dcf_value = _non_negative_price(_first_row_number(rows, DCF_VALUE_KEYS))
    nav_value = _non_negative_price(asset_value)
    model_value = nav_value if nav_value is not None and (dcf_value is None or nav_value > dcf_value) else dcf_value
    current_price = _positive_price(_first_row_number(rows, CURRENT_PRICE_KEYS))
    fair_value = _anchored_fair_value(model_value, current_price)
    upside_downside_pct = None
    if fair_value is not None and current_price not in (None, 0):
        upside_downside_pct = ((fair_value - float(current_price)) / abs(float(current_price))) * 100

    bear_value = fair_value * 0.85 if fair_value is not None else None
    bull_value = fair_value * 1.15 if fair_value is not None else None
    judgment = _judgment(upside_downside_pct)
    method = "Asset / NAV" if nav_value is not None and model_value == nav_value else "Custom DCF Advanced"
    is_anchored = fair_value is not None and model_value is not None and current_price is not None
    return {
        "symbol": symbol,
        "fairValue": fair_value,
        "modelValue": model_value,
        "valuationAnchor": current_price if is_anchored else None,
        "anchorWeight": FAIR_VALUE_MODEL_WEIGHT if is_anchored else None,
        "bearValue": bear_value,
        "bullValue": bull_value,
        "currentPrice": current_price,
        "upsideDownsidePct": upside_downside_pct,
        "judgment": judgment,
        "method": method,
        "rangeSource": "fair_value_anchor" if is_anchored else "asset_nav" if method == "Asset / NAV" else "dcf_sensitivity" if fair_value is not None else "unavailable",
        "cashFlows": _cash_flow_points(rows),
        "assumptions": _assumptions(rows, inputs),
        "methodSignals": _method_signals(judgment, method=method),
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
        valuation_model = _walnut_valuation_model(normalized_symbol)
        dcf_inputs = valuation_model["assumptions"]
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

    dcf = _valuation_from_dcf_rows(normalized_symbol, dcf_rows, inputs=dcf_inputs, asset_value=valuation_model.get("assetValue"))
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
