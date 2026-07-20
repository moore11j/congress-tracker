from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.utils.symbols import normalize_symbol

DecisionItem = dict[str, Any]

LOCKED_STATES = {"premium_locked", "pro_locked", "locked", "requires_login"}
UNAVAILABLE_STATES = {"unavailable", "not_configured", "disabled", "provider_error", "error"}
SOURCE_LABELS = {
    "congress": "Congress activity",
    "insiders": "Insider activity",
    "signals": "Signals",
    "price_volume": "Price / volume",
    "fundamentals": "Fundamentals",
    "options_flow": "Options flow",
    "government_contracts": "Government contracts",
    "institutional_activity": "Institutional activity",
    "macro_positioning": "Macro positioning",
}
SOURCE_ORDER = (
    "price_volume",
    "fundamentals",
    "insiders",
    "congress",
    "signals",
    "government_contracts",
    "options_flow",
    "institutional_activity",
    "macro_positioning",
)


def build_ticker_decision_layer(
    symbol: str,
    *,
    confirmation_bundle: dict[str, Any] | None,
    source_contexts: dict[str, Any] | None = None,
    generated_at: str | None = None,
    freshness_window: str = "30d",
) -> dict[str, Any]:
    normalized_symbol = normalize_symbol(symbol) or str(symbol or "").strip().upper()
    bundle = confirmation_bundle if isinstance(confirmation_bundle, dict) else {}
    contexts = source_contexts if isinstance(source_contexts, dict) else {}
    sources = bundle.get("sources") if isinstance(bundle.get("sources"), dict) else {}
    score = _int_or_none(bundle.get("score"))
    direction = _direction(bundle.get("direction"))
    band = _band(bundle.get("band"), score)
    history = _score_history(bundle.get("history"))
    confirmation = {
        "score": score,
        "label": _confirmation_label(band, direction, score),
        "direction": direction,
        "band": band,
        "updated_at": _latest_source_timestamp(sources, contexts) or generated_at,
        "history": history,
    }
    summary = _summary(sources, contexts, direction)
    what_changed = _what_changed(confirmation, sources, contexts)
    catalysts = _catalysts(sources, contexts)
    risks = _risks(sources, contexts)
    watch_items = _watch_items(catalysts, risks, sources, contexts)
    return {
        "symbol": normalized_symbol,
        "generated_at": generated_at or datetime.now(timezone.utc).isoformat(),
        "freshness_window": freshness_window,
        "confirmation": confirmation,
        "summary": summary,
        "what_changed": what_changed,
        "catalysts": catalysts,
        "risks": risks,
        "watch_items": watch_items,
        "missing_data_notes": _missing_data_notes(sources, contexts),
    }


def _decision_item(
    *,
    category: str,
    title: str,
    description: str,
    date: str | None = None,
    freshness: str | None = None,
    confidence: str | None = None,
    value: Any = None,
) -> DecisionItem:
    return {
        "category": category,
        "title": title,
        "description": description,
        "value": value,
        "date": date,
        "freshness": freshness,
        "confidence": confidence,
    }


def _source(sources: dict[str, Any], key: str) -> dict[str, Any]:
    value = sources.get(key)
    return value if isinstance(value, dict) else {}


def _context(contexts: dict[str, Any], key: str) -> dict[str, Any]:
    value = contexts.get(key)
    return value if isinstance(value, dict) else {}


def _status(value: dict[str, Any]) -> str:
    return str(value.get("status") or value.get("lock_state") or "").strip().lower()


def _is_locked(value: dict[str, Any]) -> bool:
    return value.get("locked") is True or _status(value) in LOCKED_STATES


def _is_unavailable(value: dict[str, Any]) -> bool:
    return not _is_locked(value) and _status(value) in UNAVAILABLE_STATES


def _present(sources: dict[str, Any], key: str) -> bool:
    source = _source(sources, key)
    return source.get("present") is True and not _is_locked(source)


def _direction(value: Any) -> str:
    text = str(value or "").strip().lower()
    return text if text in {"bullish", "bearish", "mixed", "neutral"} else "neutral"


def _source_direction(sources: dict[str, Any], key: str) -> str:
    return _direction(_source(sources, key).get("direction"))


def _band(value: Any, score: int | None) -> str:
    text = str(value or "").strip().lower()
    if text in {"inactive", "weak", "moderate", "strong", "exceptional"}:
        return text
    if score is None:
        return "inactive"
    if score <= 19:
        return "inactive"
    if score <= 39:
        return "weak"
    if score <= 59:
        return "moderate"
    if score <= 79:
        return "strong"
    return "exceptional"


def _int_or_none(value: Any) -> int | None:
    try:
        if value is None:
            return None
        return max(0, min(100, int(round(float(value)))))
    except (TypeError, ValueError):
        return None


def _number(value: Any) -> float | None:
    try:
        if value is None:
            return None
        parsed = float(value)
        return parsed if parsed == parsed else None
    except (TypeError, ValueError):
        return None


def _text(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    cleaned = value.strip()
    if not cleaned:
        return None
    lowered = cleaned.lower()
    if any(term in lowered for term in ("provider", "cache", "api", "secret", "endpoint")):
        return None
    return cleaned


def _summary_text(source: dict[str, Any], context: dict[str, Any]) -> str | None:
    return (
        _text(source.get("detail"))
        or _text(source.get("summary"))
        or _text(context.get("subtitle"))
        or _text(context.get("summary"))
        or _text(context.get("title"))
        or _text(source.get("label"))
    )


def _freshness(source: dict[str, Any], context: dict[str, Any]) -> str | None:
    days = source.get("freshness_days")
    if not isinstance(days, int):
        days = context.get("freshness_days")
    if isinstance(days, int) and days >= 0:
        if days == 0:
            return "today"
        return f"{days}d ago"
    return None


def _latest_source_timestamp(sources: dict[str, Any], contexts: dict[str, Any]) -> str | None:
    candidates: list[str] = []
    for key in SOURCE_ORDER:
        context = _context(contexts, key)
        for field in ("latest_date", "updated_at", "updated", "as_of"):
            value = _text(context.get(field))
            if value:
                candidates.append(value)
        source = _source(sources, key)
        value = _text(source.get("updated_at"))
        if value:
            candidates.append(value)
    return sorted(candidates)[-1] if candidates else None


def _confirmation_label(band: str, direction: str, score: int | None) -> str:
    if score is None:
        return "Unavailable"
    if band == "inactive" and direction == "neutral":
        return "Inactive"
    direction_label = "Conflicted" if direction == "mixed" else direction.title()
    return f"{band.title()} {direction_label}"


def _score_history(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    points: list[dict[str, Any]] = []
    for item in value:
        if not isinstance(item, dict):
            continue
        score = _int_or_none(item.get("score"))
        date = _text(item.get("date") or item.get("as_of") or item.get("updated_at"))
        if score is None or not date:
            continue
        points.append({"date": date, "score": score})
    return points[-60:]


def _summary(sources: dict[str, Any], contexts: dict[str, Any], direction: str) -> str:
    fundamentals = _source_direction(sources, "fundamentals") if _present(sources, "fundamentals") else "neutral"
    tape = _source_direction(sources, "price_volume") if _present(sources, "price_volume") else "neutral"
    disclosure = [
        _source_direction(sources, key)
        for key in ("insiders", "congress", "signals")
        if _present(sources, key)
    ]
    if fundamentals == "bullish" and tape == "bearish":
        return "Fundamentals look supportive, but price and volume still lean negative."
    if fundamentals == "bearish" and tape == "bullish":
        return "Price action is supportive, but fundamentals remain a risk."
    if direction == "bullish":
        return "The available evidence leans bullish across the current confirmation window."
    if direction == "bearish":
        return "The available evidence leans bearish across the current confirmation window."
    if direction == "mixed" or len(set(d for d in disclosure if d != "neutral")) > 1:
        return "Material evidence is mixed, with no clean directional edge yet."
    if _present(sources, "government_contracts"):
        return "Government-contract support is present, but broader confirmation remains limited."
    if _context(contexts, "price_volume").get("latest_close") is not None:
        return "Market data is available, but directional confirmation is limited."
    return "Decision context is limited because key confirmation inputs are unavailable or inactive."


def _what_changed(confirmation: dict[str, Any], sources: dict[str, Any], contexts: dict[str, Any]) -> list[DecisionItem]:
    items: list[DecisionItem] = []
    history = confirmation.get("history") if isinstance(confirmation.get("history"), list) else []
    if len(history) >= 2:
        before = _int_or_none(history[-2].get("score")) if isinstance(history[-2], dict) else None
        after = _int_or_none(history[-1].get("score")) if isinstance(history[-1], dict) else None
        if before is not None and after is not None and abs(after - before) >= 5:
            direction = "rose" if after > before else "dropped"
            items.append(_decision_item(
                category="confirmation",
                title=f"Confirmation score {direction}",
                description=f"From {before} to {after}",
                date=_text(history[-1].get("date")) if isinstance(history[-1], dict) else None,
                value=after,
            ))

    price = _context(contexts, "price_volume")
    macd = price.get("macd") if isinstance(price.get("macd"), dict) else {}
    macd_message = _text(macd.get("message"))
    macd_signal = _direction(macd.get("signal"))
    if macd_message and "crossover" in macd_message.lower() and macd_signal in {"bullish", "bearish"}:
        items.append(_decision_item(
            category="technical",
            title=f"MACD crossed {macd_signal}",
            description=macd_message,
            date=_text(price.get("latest_date")),
        ))

    for key, title in (
        ("insiders", "New insider activity"),
        ("congress", "New Congress disclosure"),
        ("government_contracts", "Government-contract activity updated"),
        ("institutional_activity", "Reported institutional activity"),
    ):
        source = _source(sources, key)
        if not _present(sources, key):
            continue
        context = _context(contexts, key)
        items.append(_decision_item(
            category=key,
            title=title,
            description=_summary_text(source, context) or f"{SOURCE_LABELS[key]} is active in the current window.",
            date=_text(context.get("latest_date")),
            freshness=_freshness(source, context),
        ))

    return _dedupe_items(items)[:5]


def _catalysts(sources: dict[str, Any], contexts: dict[str, Any]) -> list[DecisionItem]:
    items: list[DecisionItem] = []
    for key, title in (
        ("fundamentals", "Fundamental strength"),
        ("price_volume", "Bullish tape confirmation"),
        ("signals", "Signal activity confirming"),
        ("congress", "Congress buy-skewed activity"),
        ("insiders", "Insider buy-skewed activity"),
        ("government_contracts", "Government contracts activity"),
        ("options_flow", "Bullish options flow"),
        ("institutional_activity", "Reported institutional accumulation"),
        ("macro_positioning", "Supportive macro positioning"),
    ):
        if not _present(sources, key):
            continue
        direction = _source_direction(sources, key)
        if key != "government_contracts" and direction != "bullish":
            continue
        source = _source(sources, key)
        context = _context(contexts, key)
        items.append(_decision_item(
            category=key,
            title=title,
            description=_positive_description(key, source, context),
            freshness=_freshness(source, context),
        ))
    return _dedupe_items(items)[:4]


def _risks(sources: dict[str, Any], contexts: dict[str, Any]) -> list[DecisionItem]:
    items: list[DecisionItem] = []
    for key, title in (
        ("price_volume", "Weak price and volume confirmation"),
        ("fundamentals", "Fundamental pressure"),
        ("signals", "Bearish signal activity"),
        ("insiders", "Sell-skewed insider activity"),
        ("congress", "Sell-skewed Congress activity"),
        ("options_flow", "Bearish options flow"),
        ("institutional_activity", "Reported institutional distribution"),
        ("macro_positioning", "Macro positioning headwind"),
    ):
        if not _present(sources, key) or _source_direction(sources, key) != "bearish":
            continue
        source = _source(sources, key)
        context = _context(contexts, key)
        items.append(_decision_item(
            category=key,
            title=title,
            description=_risk_description(key, source, context),
            freshness=_freshness(source, context),
        ))
    if not items and _present(sources, "price_volume") and _source_direction(sources, "price_volume") == "mixed":
        source = _source(sources, "price_volume")
        context = _context(contexts, "price_volume")
        items.append(_decision_item(
            category="price_volume",
            title="Tape confirmation is mixed",
            description=_summary_text(source, context) or "Price and volume have not produced a clean directional read.",
            freshness=_freshness(source, context),
        ))
    return _dedupe_items(items)[:4]


def _positive_description(key: str, source: dict[str, Any], context: dict[str, Any]) -> str:
    if key == "fundamentals":
        metrics = context.get("metrics") if isinstance(context.get("metrics"), dict) else {}
        revenue = _metric_display(metrics, "revenue_growth")
        roe = _metric_display(metrics, "roe")
        if revenue and roe:
            return f"Revenue growth and returns are supportive ({revenue}, {roe})."
        return "Fundamental metrics are supportive in the current data."
    return _summary_text(source, context) or f"{SOURCE_LABELS.get(key, key)} is supportive in the current window."


def _risk_description(key: str, source: dict[str, Any], context: dict[str, Any]) -> str:
    if key == "price_volume":
        lines = context.get("lines") if isinstance(context.get("lines"), list) else []
        clean_lines = [_text(line) for line in lines]
        clean_lines = [line for line in clean_lines if line]
        if clean_lines:
            return clean_lines[0]
    return _summary_text(source, context) or f"{SOURCE_LABELS.get(key, key)} is a negative input in the current window."


def _metric_display(metrics: dict[str, Any], key: str) -> str | None:
    metric = metrics.get(key)
    if not isinstance(metric, dict):
        return None
    display = _text(metric.get("display"))
    if display:
        return display
    value = _number(metric.get("value"))
    if value is None:
        return None
    return f"{value:.1f}%"


def _watch_items(
    catalysts: list[DecisionItem],
    risks: list[DecisionItem],
    sources: dict[str, Any],
    contexts: dict[str, Any],
) -> list[DecisionItem]:
    items: list[DecisionItem] = []
    for risk in risks[:3]:
        items.append(_decision_item(
            category=risk["category"],
            title=_watch_label(str(risk["category"])),
            description=_watch_description(str(risk["category"]), negative=True),
            freshness=risk.get("freshness"),
        ))
    if not any(item["category"] == "price_volume" for item in items):
        items.append(_decision_item(
            category="price_volume",
            title="Tape confirmation",
            description="Watch whether price and volume confirm or fade.",
            freshness=_freshness(_source(sources, "price_volume"), _context(contexts, "price_volume")),
        ))
    for catalyst in catalysts:
        if len(items) >= 5:
            break
        category = str(catalyst["category"])
        if any(item["category"] == category for item in items):
            continue
        items.append(_decision_item(
            category=category,
            title=_watch_label(category),
            description=_watch_description(category, negative=False),
            freshness=catalyst.get("freshness"),
        ))
    if not any(item["category"] == "fundamentals" for item in items) and not _is_unavailable(_source(sources, "fundamentals")):
        items.append(_decision_item(
            category="fundamentals",
            title="Fundamental update",
            description="Watch the next reported fundamental refresh.",
        ))
    return _dedupe_items(items)[:5]


def _watch_label(category: str) -> str:
    return {
        "price_volume": "Tape confirmation",
        "fundamentals": "Fundamental durability",
        "signals": "Signal follow-through",
        "insiders": "Insider disclosure skew",
        "congress": "Congress disclosure skew",
        "government_contracts": "Contract activity",
        "options_flow": "Options-flow skew",
        "institutional_activity": "Institutional filings",
        "macro_positioning": "Macro positioning",
    }.get(category, "Confirmation input")


def _watch_description(category: str, *, negative: bool) -> str:
    if category == "price_volume":
        return "Watch for accumulation or further breakdown."
    if category == "fundamentals":
        return "Watch whether upcoming reported metrics confirm durability."
    if category == "signals":
        return "Watch whether signal activity broadens or fades."
    if category in {"insiders", "congress"}:
        return "Watch whether new disclosures reinforce the current skew."
    if category == "government_contracts":
        return "Watch for additional qualifying awards or a pause in activity."
    if category == "options_flow":
        return "Watch whether premium flow keeps leaning the same way."
    if category == "institutional_activity":
        return "Watch the next reported holder changes."
    if category == "macro_positioning":
        return "Watch the next weekly positioning refresh."
    return "Watch whether this input keeps confirming."


def _missing_data_notes(sources: dict[str, Any], contexts: dict[str, Any]) -> list[str]:
    notes: list[str] = []
    for key in SOURCE_ORDER:
        source = _source(sources, key)
        context = _context(contexts, key)
        if _is_locked(source) or _is_locked(context):
            continue
        if _is_unavailable(source) or _is_unavailable(context):
            notes.append(f"{SOURCE_LABELS.get(key, key)} unavailable.")
    return notes[:5]


def _dedupe_items(items: list[DecisionItem]) -> list[DecisionItem]:
    seen: set[tuple[str, str]] = set()
    result: list[DecisionItem] = []
    for item in items:
        key = (str(item.get("category") or ""), str(item.get("title") or ""))
        if key in seen:
            continue
        seen.add(key)
        result.append(item)
    return result
