"""Reported TTM revenue / ending total assets; never assets / liabilities."""
from datetime import date
import math

VERSION = "sec_asset_turnover_v1"
BASIS = "ttm_revenue_to_ending_total_assets"


def _finite(value):
    try:
        return type(value) in (int, float) and math.isfinite(value)
    except OverflowError:
        return False


def derive(evidence, assets):
    result = {"methodology": VERSION, "basis": BASIS, "unit": "multiple",
        "formula": "TTM revenue / total assets at the same period end",
        "status": "unavailable", "value": None}
    revenue = evidence.get("current", {}).get("revenue")
    if not isinstance(revenue, dict):
        return {**result, "reason": "missing_ttm_revenue"}
    try:
        end = date.fromisoformat(evidence["period_end"])
        start = date.fromisoformat(evidence["period_start"])
        available = date.fromisoformat(evidence["available_by"])
    except (KeyError, TypeError, ValueError):
        return {**result, "reason": "invalid_period"}
    if (revenue.get("start") != str(start) or revenue.get("end") != str(end)
            or not 350 <= (end-start).days+1 <= 380 or end > available):
        return {**result, "reason": "unaligned_ttm_period"}
    asset = assets.get((None, end))
    if not isinstance(asset, dict):
        return {**result, "reason": "missing_or_conflicting_total_assets"}
    result.update(period_start=str(start), period_end=str(end), revenue=revenue, assets=asset)
    closing = [row for row in revenue.get("inputs", []) if row.get("end") == str(end)]
    if len(closing) != 1:
        return {**result, "reason": "ambiguous_closing_revenue_filing"}
    rows = [*revenue.get("inputs", []), asset]
    cik = str(evidence.get("cik", "")).zfill(10)
    if (not cik.isdigit() or int(cik) <= 0 or any(
            row.get("unit") != "USD" or not row.get("accession")
            or not row.get("filed") or row["filed"] > str(available)
            or row.get("source_cik") is not None and str(row["source_cik"]).zfill(10) != cik
            for row in rows)):
        return {**result, "reason": "invalid_source_provenance"}
    if (asset.get("tag") != "Assets" or asset.get("start") is not None
            or any(asset.get(key) != closing[0].get(key) for key in ("end", "filed", "accession", "unit"))):
        return {**result, "reason": "income_balance_filing_mismatch"}
    numerator, denominator = revenue.get("value"), asset.get("value")
    if not _finite(numerator) or numerator < 0 or not _finite(denominator) or denominator <= 0:
        return {**result, "reason": "invalid_financial_amount"}
    value = numerator/denominator
    if not _finite(value):
        return {**result, "reason": "nonfinite_ratio"}
    return {**result, "status": "available", "value": value}
