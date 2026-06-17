from __future__ import annotations

from dataclasses import dataclass

from app.utils.symbols import normalize_symbol


GENERIC_SYMBOL_LABELS = {
    "CONGRESS_TRADE",
    "CONGRESS_TREASURY_TRADE",
    "CONGRESS_CRYPTO_TRADE",
    "INSIDER_TRADE",
    "SECURITY",
    "STOCK",
    "EQUITY",
    "OTHER",
    "UNRESOLVED",
    "N/A",
    "NA",
    "NONE",
    "NULL",
}

PUBLIC_EQUITY_ASSET_CLASSES = {
    "common stock",
    "common stocks",
    "closed end fund",
    "closed-end fund",
    "equity",
    "equities",
    "etf",
    "etf fund",
    "etf_fund",
    "exchange traded fund",
    "exchange traded product",
    "exchange-traded fund",
    "exchange-traded product",
    "fund",
    "index fund",
    "mutual fund",
    "public fund",
    "public equity",
    "stock",
    "stocks",
}

NON_EQUITY_ASSET_TERMS = (
    "bond",
    "corporate bond",
    "crypto",
    "cryptocurrency",
    "debt",
    "fixed income",
    "government security",
    "municipal",
    "municipal bond",
    "other",
    "private fund",
    "treasury",
    "unresolved",
)

NON_EQUITY_DESCRIPTION_TERMS = (
    " bond",
    " bonds",
    " corporate bond",
    " municipal bond",
    " treasury",
    " t-bill",
    " tbill",
    " debenture",
    " cryptocurrency",
)


@dataclass(frozen=True)
class CongressOutcomeEligibility:
    eligible: bool
    symbol: str | None
    trade_date: str | None
    side: str | None
    skip_reason: str | None = None
    detail: str | None = None


def _clean_text(value: object | None) -> str | None:
    if value is None:
        return None
    cleaned = str(value).strip()
    return cleaned or None


def _normalized_text(value: object | None) -> str:
    return (_clean_text(value) or "").lower().replace("-", " ").replace("_", " ")


def _first_text(*values: object | None) -> str | None:
    for value in values:
        cleaned = _clean_text(value)
        if cleaned:
            return cleaned
    return None


def _is_generic_symbol(symbol: str | None, event_type: str | None) -> bool:
    if not symbol:
        return True
    normalized = symbol.strip().upper()
    if normalized in GENERIC_SYMBOL_LABELS:
        return True
    return bool(event_type and normalized == event_type.strip().upper())


def _non_equity_reason(asset_class: object | None, security_description: object | None) -> str | None:
    class_text = _normalized_text(asset_class)
    description = f" {_normalized_text(security_description)} "
    if class_text:
        for term in NON_EQUITY_ASSET_TERMS:
            if term in class_text:
                return f"asset_class={asset_class}"
    for term in NON_EQUITY_DESCRIPTION_TERMS:
        if term in description:
            return f"security_description contains {term.strip()}"
    return None


def congress_equity_outcome_eligibility(
    *,
    event_type: str | None,
    symbol: object | None,
    payload: dict | None = None,
    asset_class: object | None = None,
    security_description: object | None = None,
    trade_date: object | None = None,
    side: object | None = None,
    amount_min: object | None = None,
    amount_max: object | None = None,
) -> CongressOutcomeEligibility:
    payload = payload if isinstance(payload, dict) else {}
    event_type_value = _clean_text(event_type)
    raw_symbol = _first_text(symbol, payload.get("symbol"), payload.get("ticker"))
    normalized_symbol = normalize_symbol(raw_symbol)

    if event_type_value not in {"congress_trade", "insider_trade"}:
        return CongressOutcomeEligibility(False, normalized_symbol, None, None, "unsupported_event_type", event_type_value)
    if _is_generic_symbol(normalized_symbol, event_type_value):
        return CongressOutcomeEligibility(False, normalized_symbol, None, None, "invalid_symbol", "Missing or generic ticker symbol")

    resolved_asset_class = _first_text(asset_class, payload.get("asset_class"), payload.get("assetClass"))
    resolved_description = _first_text(
        security_description,
        payload.get("security_description"),
        payload.get("securityDescription"),
        payload.get("security_name"),
        payload.get("securityName"),
        payload.get("description"),
    )
    non_equity_detail = _non_equity_reason(resolved_asset_class, resolved_description)
    if non_equity_detail:
        return CongressOutcomeEligibility(False, normalized_symbol, None, None, "not_equity_outcome_eligible", non_equity_detail)

    class_text = _normalized_text(resolved_asset_class)
    if class_text and class_text not in PUBLIC_EQUITY_ASSET_CLASSES:
        return CongressOutcomeEligibility(False, normalized_symbol, None, None, "not_equity_outcome_eligible", f"asset_class={resolved_asset_class}")

    resolved_trade_date = _first_text(
        trade_date,
        payload.get("trade_date"),
        payload.get("tradeDate"),
        payload.get("transaction_date"),
        payload.get("transactionDate"),
    )
    if not resolved_trade_date:
        return CongressOutcomeEligibility(False, normalized_symbol, None, None, "missing_trade_date", "Missing trade date")

    resolved_side = _first_text(
        side,
        payload.get("trade_type"),
        payload.get("tradeType"),
        payload.get("transaction_type"),
        payload.get("transactionType"),
    )
    if not resolved_side:
        return CongressOutcomeEligibility(False, normalized_symbol, resolved_trade_date[:10], None, "missing_trade_side", "Missing trade side")

    if amount_min is None and amount_max is None:
        amount_min = payload.get("amount_range_min") or payload.get("amountMin")
        amount_max = payload.get("amount_range_max") or payload.get("amountMax")
    if amount_min is None and amount_max is None:
        return CongressOutcomeEligibility(False, normalized_symbol, resolved_trade_date[:10], resolved_side, "missing_amount", "Missing disclosed amount range")

    return CongressOutcomeEligibility(
        True,
        normalized_symbol,
        resolved_trade_date[:10],
        resolved_side,
    )
