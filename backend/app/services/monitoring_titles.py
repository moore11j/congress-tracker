from __future__ import annotations

from typing import Any

from app.insider_market_trade import canonicalize_market_trade_type
from app.models import Event

_PROVIDER_LABELS = {
    "api",
    "data source",
    "data_source",
    "event source",
    "fmp",
    "provider",
    "sec",
    "source",
    "vendor",
}


def _clean_text(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    cleaned = value.strip()
    return cleaned or None


def _clean_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _looks_like_provider_label(value: str | None) -> bool:
    if not value:
        return False
    normalized = value.strip().lower().replace("-", " ").replace("_", " ")
    return normalized in _PROVIDER_LABELS


def _first_valid_name(*values: Any) -> str | None:
    for value in values:
        cleaned = _clean_text(value)
        if cleaned and not _looks_like_provider_label(cleaned):
            return cleaned
    return None


def resolve_insider_name(payload: dict[str, Any], *, event_member_name: str | None = None) -> str | None:
    raw = _clean_dict(payload.get("raw"))
    insider = _clean_dict(payload.get("insider"))
    reporting_owner = _clean_dict(payload.get("reporting_owner"))
    reporting_owner_camel = _clean_dict(payload.get("reportingOwner"))
    owner = _clean_dict(payload.get("owner"))

    return _first_valid_name(
        payload.get("reporting_owner_name"),
        payload.get("reportingOwnerName"),
        payload.get("owner_name"),
        payload.get("ownerName"),
        payload.get("insider_name"),
        payload.get("insiderName"),
        payload.get("person_name"),
        payload.get("personName"),
        reporting_owner.get("name"),
        reporting_owner.get("owner_name"),
        reporting_owner_camel.get("name"),
        reporting_owner_camel.get("ownerName"),
        owner.get("name"),
        owner.get("owner_name"),
        insider.get("name"),
        raw.get("reporting_owner_name"),
        raw.get("reportingOwnerName"),
        raw.get("owner_name"),
        raw.get("ownerName"),
        raw.get("insider_name"),
        raw.get("insiderName"),
        raw.get("person_name"),
        raw.get("personName"),
        event_member_name,
    )


def normalize_trade_side(*values: Any) -> str | None:
    for value in values:
        cleaned = _clean_text(value)
        if not cleaned:
            continue
        canonical = canonicalize_market_trade_type(cleaned)
        if canonical:
            return canonical
        normalized = cleaned.lower()
        if normalized in {"a", "acquired", "acquisition", "buy", "bought"} or "acquir" in normalized:
            return "purchase"
        if normalized in {"d", "disposition", "dispose", "disposed", "sell", "sold"} or "disposition" in normalized:
            return "sale"
    return None


def build_monitoring_event_title(event: Event, payload: dict[str, Any]) -> str:
    symbol = _clean_text(event.symbol) or _clean_text(payload.get("symbol")) or _clean_text(payload.get("ticker"))
    if symbol:
        symbol = symbol.upper()

    if event.event_type == "insider_trade":
        raw = _clean_dict(payload.get("raw"))
        insider_name = resolve_insider_name(payload, event_member_name=event.member_name) or "Insider"
        side = (
            normalize_trade_side(
                event.trade_type,
                event.transaction_type,
                payload.get("trade_type"),
                payload.get("tradeType"),
                payload.get("transaction_type"),
                payload.get("transactionType"),
                payload.get("side"),
                raw.get("transaction_type"),
                raw.get("transactionType"),
                raw.get("side"),
            )
            or "trade"
        )
        return " - ".join(part for part in (symbol, insider_name, side) if part)

    actor = (
        _clean_text(event.member_name)
        or _clean_text(payload.get("member_name"))
        or _clean_text(_clean_dict(payload.get("member")).get("name"))
        or _clean_text(payload.get("insider_name"))
        or _clean_text(payload.get("insiderName"))
        or _clean_text(payload.get("reporting_owner_name"))
        or _clean_text(payload.get("reportingOwnerName"))
        or _clean_text(event.source)
    )
    action = (
        _clean_text(event.trade_type)
        or _clean_text(event.transaction_type)
        or _clean_text(payload.get("transaction_type"))
        or _clean_text(payload.get("transactionType"))
    )
    return " - ".join(part for part in (symbol, actor, action) if part) or event.event_type.replace("_", " ").title()
