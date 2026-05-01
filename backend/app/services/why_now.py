from __future__ import annotations

from typing import Any, Literal

WhyNowState = Literal["early", "strengthening", "strong", "mixed", "fading", "inactive"]

_DIRECTION_VALUES = {"bullish", "bearish", "neutral", "mixed"}
_BAND_VALUES = {"inactive", "weak", "moderate", "strong", "exceptional"}
_SUPPORT_ONLY_SOURCES = {"government_contracts"}
_SOURCE_ORDER = (
    "congress",
    "insiders",
    "signals",
    "price_volume",
    "government_contracts",
    "options_flow",
    "institutional_activity",
)


def build_why_now_bundle(
    ticker: str,
    confirmation_bundle: dict[str, Any] | None,
    *,
    lookback_days: int | None = None,
) -> dict[str, Any]:
    """Build a deterministic explanation from the canonical confirmation bundle."""
    symbol = (ticker or _read_str(confirmation_bundle, "ticker") or "").strip().upper()
    bundle = confirmation_bundle if isinstance(confirmation_bundle, dict) else {}
    lookback = _bounded_lookback(lookback_days if lookback_days is not None else bundle.get("lookback_days"))
    score = _read_int(bundle, "score", default=0)
    band = _read_str(bundle, "band") if _read_str(bundle, "band") in _BAND_VALUES else _band_for_score(score)
    direction = _read_str(bundle, "direction") if _read_str(bundle, "direction") in _DIRECTION_VALUES else "neutral"
    sources = _sources(bundle)
    active_sources = [
        (key, source)
        for key, source in sources.items()
        if source["present"] and source["direction"] != "neutral"
    ]
    active_count = len(active_sources)
    conflicting = _has_conflict(active_sources, direction)
    state = _classify_state(
        score=score,
        band=band,
        direction=direction,
        active_sources=active_sources,
        conflicting=conflicting,
    )
    active_drivers = _active_driver_phrases(active_sources)
    evidence = _evidence(bundle, sources, active_drivers, active_count)
    caveat = _caveat(
        state=state,
        active_count=active_count,
        conflicting=conflicting,
        has_signal=sources["signals"]["present"],
        has_price=sources["price_volume"]["present"],
    )

    return {
        "ticker": symbol,
        "lookback_days": lookback,
        "state": state,
        "headline": _headline(symbol, state, direction, active_count, active_drivers),
        "evidence": evidence,
        "caveat": caveat,
    }


def slim_why_now_bundle(bundle: dict[str, Any] | None) -> dict[str, Any]:
    """Return the lean shape used by rows and monitoring-like surfaces."""
    why_now = build_why_now_bundle(_read_str(bundle, "ticker") or "", bundle)
    return {
        "ticker": why_now["ticker"],
        "lookback_days": why_now["lookback_days"],
        "state": why_now["state"],
        "headline": why_now["headline"],
        "evidence": why_now["evidence"][:2],
        "caveat": why_now["caveat"],
    }


def inactive_why_now_bundle(ticker: str, *, lookback_days: int = 30) -> dict[str, Any]:
    return build_why_now_bundle(
        ticker,
        {
            "ticker": ticker,
            "lookback_days": lookback_days,
            "score": 0,
            "band": "inactive",
            "direction": "neutral",
            "status": "Inactive",
            "sources": {},
            "drivers": [],
        },
        lookback_days=lookback_days,
    )


def _bounded_lookback(value: Any) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = 30
    return max(1, min(parsed, 365))


def _read_str(mapping: dict[str, Any] | None, key: str) -> str | None:
    if not isinstance(mapping, dict):
        return None
    value = mapping.get(key)
    return value.strip() if isinstance(value, str) and value.strip() else None


def _read_int(mapping: dict[str, Any], key: str, *, default: int) -> int:
    try:
        parsed = int(round(float(mapping.get(key, default))))
    except (TypeError, ValueError):
        parsed = default
    return max(0, min(parsed, 100))


def _band_for_score(score: int) -> str:
    if score <= 19:
        return "inactive"
    if score <= 39:
        return "weak"
    if score <= 59:
        return "moderate"
    if score <= 79:
        return "strong"
    return "exceptional"


def _empty_source(label: str) -> dict[str, Any]:
    return {
        "present": False,
        "direction": "neutral",
        "strength": 0,
        "quality": 0,
        "freshness_days": None,
        "label": label,
    }


def _source(raw: Any, fallback_label: str) -> dict[str, Any]:
    if not isinstance(raw, dict):
        return _empty_source(fallback_label)
    direction = raw.get("direction") if raw.get("direction") in _DIRECTION_VALUES else "neutral"
    return {
        "present": raw.get("present") is True,
        "direction": direction,
        "strength": _read_int(raw, "strength", default=0),
        "quality": _read_int(raw, "quality", default=0),
        "freshness_days": raw.get("freshness_days") if isinstance(raw.get("freshness_days"), int) else None,
        "label": _read_str(raw, "label") or fallback_label,
    }


def _sources(bundle: dict[str, Any]) -> dict[str, dict[str, Any]]:
    raw_sources = bundle.get("sources") if isinstance(bundle.get("sources"), dict) else {}
    return {
        "congress": _source(raw_sources.get("congress"), "Inactive"),
        "insiders": _source(raw_sources.get("insiders"), "Inactive"),
        "signals": _source(raw_sources.get("signals"), "No current smart signal"),
        "price_volume": _source(raw_sources.get("price_volume"), "No price confirmation"),
        "government_contracts": _source(raw_sources.get("government_contracts"), "No recent government contracts"),
        "options_flow": _source(raw_sources.get("options_flow"), "Options flow not confirming"),
        "institutional_activity": _source(raw_sources.get("institutional_activity"), "Institutional activity not configured"),
    }


def _has_conflict(active_sources: list[tuple[str, dict[str, Any]]], direction: str) -> bool:
    directions = {
        source["direction"]
        for key, source in active_sources
        if source["direction"] != "neutral" and key not in _SUPPORT_ONLY_SOURCES
    }
    return "mixed" in directions or ("bullish" in directions and "bearish" in directions) or direction == "mixed"


def _classify_state(
    *,
    score: int,
    band: str,
    direction: str,
    active_sources: list[tuple[str, dict[str, Any]]],
    conflicting: bool,
) -> WhyNowState:
    active_count = len(active_sources)
    if active_count == 0 or band == "inactive" or score <= 19:
        return "inactive"
    if conflicting or direction == "mixed":
        return "mixed"
    if score >= 80 and active_count >= 3:
        return "strong"
    if _looks_fading(score, band, active_sources):
        return "fading"
    if active_count >= 2 and score >= 40:
        return "strengthening"
    return "early"


def _looks_fading(score: int, band: str, active_sources: list[tuple[str, dict[str, Any]]]) -> bool:
    if not active_sources:
        return False
    has_market_confirmation = any(key in {"signals", "price_volume", "options_flow"} for key, _ in active_sources)
    freshness_values = [
        source["freshness_days"]
        for _, source in active_sources
        if isinstance(source.get("freshness_days"), int)
    ]
    stale = bool(freshness_values) and min(freshness_values) >= 14
    if stale and band in {"weak", "moderate"}:
        return True
    return len(active_sources) >= 2 and not has_market_confirmation and score < 60


def _source_driver(key: str, source: dict[str, Any]) -> str | None:
    direction = source["direction"]
    if key == "congress":
        if direction == "bullish":
            return "Congress buy-skewed"
        if direction == "bearish":
            return "Congress sell-skewed"
        if direction == "mixed":
            return "Congress mixed"
        return "Congress active"
    if key == "insiders":
        if direction == "bullish":
            return "Recent insider buying"
        if direction == "bearish":
            return "Recent insider selling"
        if direction == "mixed":
            return "Insider activity mixed"
        return "Insiders active"
    if key == "signals":
        if direction == "bullish":
            return "Bullish smart signal"
        if direction == "bearish":
            return "Bearish smart signal"
        if direction == "mixed":
            return "Mixed smart signals"
        return "Smart signal active"
    if key == "price_volume":
        strength = "Weak" if source["strength"] < 45 else "Moderate" if source["strength"] < 70 else "Strong"
        if direction in {"bullish", "bearish"}:
            return f"{strength} {direction} price confirmation"
        return "Price confirmation active"
    if key == "government_contracts":
        return "Government contracts bullish support"
    if key == "options_flow":
        if direction == "bullish":
            return "Bullish options flow"
        if direction == "bearish":
            return "Bearish options flow"
        if direction == "mixed":
            return "Mixed options flow"
        return "Options flow active"
    if key == "institutional_activity":
        if direction == "bullish":
            return "Bullish institutional activity"
        if direction == "bearish":
            return "Bearish institutional activity"
        if direction == "mixed":
            return "Mixed institutional activity"
        return "Institutional activity active"
    return None


def _active_driver_phrases(active_sources: list[tuple[str, dict[str, Any]]]) -> list[str]:
    direction = _combined_direction(
        [
            source["direction"]
            for key, source in active_sources
            if source["direction"] != "neutral" and key not in _SUPPORT_ONLY_SOURCES
        ]
    )
    sorted_sources = sorted(
        active_sources,
        key=lambda item: (
            0 if _source_aligns_with_direction(item[0], item[1], direction) else 1,
            -item[1]["strength"],
        ),
    )
    drivers = []
    for key, source in sorted_sources:
        driver = _source_driver(key, source)
        if driver and driver not in drivers:
            drivers.append(driver)
    return drivers[:4]


def _evidence(
    bundle: dict[str, Any],
    sources: dict[str, dict[str, Any]],
    active_drivers: list[str],
    active_count: int,
) -> list[str]:
    evidence: list[str] = []
    status = _read_str(bundle, "status")
    if status and status != "Inactive" and active_count >= 2:
        evidence.append(status)
    evidence.extend(active_drivers)
    if active_count == 0:
        evidence.append("No active confirmation sources")
    elif active_count == 1:
        evidence.append("Source breadth remains limited")

    inactive_labels = {
        "congress": "Congress activity remains inactive",
        "insiders": "Insider activity remains inactive",
        "signals": "No current smart signal",
        "price_volume": "No price confirmation",
        "government_contracts": "No recent government contracts",
        "options_flow": "Options flow not confirming",
        "institutional_activity": "Institutional activity not configured",
    }
    for key in _SOURCE_ORDER:
        if len(evidence) >= 4:
            break
        if not sources[key]["present"]:
            evidence.append(inactive_labels[key])

    return _dedupe(evidence)[:4]


def _caveat(
    *,
    state: WhyNowState,
    active_count: int,
    conflicting: bool,
    has_signal: bool,
    has_price: bool,
) -> str | None:
    if state == "inactive":
        return None
    if conflicting:
        return "Evidence is conflicting across active sources."
    if active_count <= 1:
        return "Broader confirmation is still limited."
    if not has_signal:
        return "No current smart signal is reinforcing the move."
    if not has_price:
        return "Price has not confirmed yet."
    return None


def _headline(
    ticker: str,
    state: WhyNowState,
    direction: str,
    active_count: int,
    active_drivers: list[str],
) -> str:
    symbol = ticker or "This ticker"
    direction_phrase = f"{direction} " if direction in {"bullish", "bearish"} else ""
    if state == "inactive":
        return f"No active confirmation sources are currently putting {symbol} on the radar."
    if state == "mixed":
        if active_drivers:
            return f"{active_drivers[0]} is active, but mixed confirmation is limiting conviction on {symbol}."
        return f"Active sources are mixed, leaving {symbol} without a clear directional read."
    if state == "fading":
        driver = active_drivers[0] if active_drivers else "Activity"
        return f"{driver} remains active, but confirmation is fading for {symbol}."
    if state == "early":
        driver = active_drivers[0] if active_drivers else "A single source"
        return f"{driver} is putting {symbol} on the radar, but broader confirmation is still limited."
    if state == "strong":
        return f"{active_count}-source {direction_phrase}confirmation is keeping {symbol} high on the radar."

    if len(active_drivers) >= 2:
        return f"{active_drivers[0]} and {_lower_first(active_drivers[1])} are strengthening the {direction_phrase}setup for {symbol}."
    driver = active_drivers[0] if active_drivers else "Confirmation"
    return f"{driver} is strengthening the current setup for {symbol}."


def _dedupe(values: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        cleaned = value.strip()
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        result.append(cleaned)
    return result


def _source_aligns_with_direction(key: str, source: dict[str, Any], direction: str) -> bool:
    if direction == "mixed":
        return True
    if direction == "neutral":
        return key in _SUPPORT_ONLY_SOURCES or source["direction"] == "neutral"
    if key in _SUPPORT_ONLY_SOURCES:
        return direction == "bullish" and source["direction"] == "bullish"
    return source["direction"] == direction


def _combined_direction(directions: list[str]) -> str:
    values = {direction for direction in directions if direction != "neutral"}
    if not values:
        return "neutral"
    if "mixed" in values or ("bullish" in values and "bearish" in values):
        return "mixed"
    if "bullish" in values:
        return "bullish"
    if "bearish" in values:
        return "bearish"
    return "neutral"


def _lower_first(value: str) -> str:
    return value[:1].lower() + value[1:] if value else value
