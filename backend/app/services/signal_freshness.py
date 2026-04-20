from __future__ import annotations

from typing import Any, Literal

SignalFreshnessState = Literal["fresh", "early", "active", "maturing", "stale", "inactive"]

_DIRECTION_VALUES = {"bullish", "bearish", "neutral", "mixed"}
_SOURCE_LABELS = {
    "congress": "Congress activity",
    "insiders": "insider activity",
    "signals": "smart signal",
    "price_volume": "price confirmation",
    "options_flow": "options flow",
}


def build_signal_freshness_bundle(
    ticker: str,
    confirmation_bundle: dict[str, Any] | None,
    *,
    lookback_days: int | None = None,
) -> dict[str, Any]:
    """Build the canonical timing/decay read from confirmation source freshness."""
    symbol = (ticker or _read_str(confirmation_bundle, "ticker") or "").strip().upper()
    bundle = confirmation_bundle if isinstance(confirmation_bundle, dict) else {}
    lookback = _bounded_lookback(lookback_days if lookback_days is not None else bundle.get("lookback_days"))
    sources = _sources(bundle)
    active_sources = [
        (key, source)
        for key, source in sources.items()
        if source["present"] and source["direction"] != "neutral"
    ]
    active_count = len(active_sources)
    timing_values = [
        source["freshness_days"]
        for _, source in active_sources
        if isinstance(source.get("freshness_days"), int)
    ]
    freshest = min(timing_values) if timing_values else None
    stalest = max(timing_values) if timing_values else None
    overlap_window = stalest - freshest if freshest is not None and stalest is not None else None
    direction = _read_str(bundle, "direction") if _read_str(bundle, "direction") in _DIRECTION_VALUES else _combined_direction(active_sources)
    mixed = _is_mixed(active_sources, direction)
    has_smart_signal = sources["signals"]["present"] and sources["signals"]["direction"] != "neutral"
    has_price_confirmation = sources["price_volume"]["present"] and sources["price_volume"]["direction"] != "neutral"
    state = _classify_state(
        active_count=active_count,
        freshest=freshest,
        overlap_window=overlap_window,
        mixed=mixed,
    )
    score = _score(
        state=state,
        active_count=active_count,
        freshest=freshest,
        overlap_window=overlap_window,
        mixed=mixed,
        has_smart_signal=has_smart_signal,
        has_price_confirmation=has_price_confirmation,
    )
    active_labels = [_SOURCE_LABELS[key] for key, _ in active_sources]

    return {
        "ticker": symbol,
        "lookback_days": lookback,
        "freshness_score": score,
        "freshness_state": state,
        "freshness_label": _label(state, active_count),
        "explanation": _explanation(
            state=state,
            active_count=active_count,
            active_labels=active_labels,
            freshest=freshest,
            mixed=mixed,
        ),
        "timing": {
            "freshest_source_days": freshest,
            "stalest_active_source_days": stalest,
            "active_source_count": active_count,
            "overlap_window_days": overlap_window,
        },
    }


def slim_signal_freshness_bundle(confirmation_bundle: dict[str, Any] | None) -> dict[str, Any]:
    """Return the compact freshness shape used by row/card surfaces."""
    bundle = build_signal_freshness_bundle(_read_str(confirmation_bundle, "ticker") or "", confirmation_bundle)
    return {
        "ticker": bundle["ticker"],
        "lookback_days": bundle["lookback_days"],
        "freshness_score": bundle["freshness_score"],
        "freshness_state": bundle["freshness_state"],
        "freshness_label": bundle["freshness_label"],
        "explanation": bundle["explanation"],
        "timing": dict(bundle["timing"]),
    }


def inactive_signal_freshness_bundle(ticker: str, *, lookback_days: int = 30) -> dict[str, Any]:
    return build_signal_freshness_bundle(
        ticker,
        {
            "ticker": ticker,
            "lookback_days": lookback_days,
            "direction": "neutral",
            "sources": {},
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
    freshness_days = raw.get("freshness_days")
    return {
        "present": raw.get("present") is True,
        "direction": direction,
        "strength": _read_int(raw, "strength", default=0),
        "quality": _read_int(raw, "quality", default=0),
        "freshness_days": freshness_days if isinstance(freshness_days, int) and freshness_days >= 0 else None,
        "label": _read_str(raw, "label") or fallback_label,
    }


def _sources(bundle: dict[str, Any]) -> dict[str, dict[str, Any]]:
    raw_sources = bundle.get("sources") if isinstance(bundle.get("sources"), dict) else {}
    return {
        "congress": _source(raw_sources.get("congress"), "Inactive"),
        "insiders": _source(raw_sources.get("insiders"), "Inactive"),
        "signals": _source(raw_sources.get("signals"), "No current smart signal"),
        "price_volume": _source(raw_sources.get("price_volume"), "No price confirmation"),
        "options_flow": _source(raw_sources.get("options_flow"), "Options flow not confirming"),
    }


def _combined_direction(active_sources: list[tuple[str, dict[str, Any]]]) -> str:
    values = {source["direction"] for _, source in active_sources if source["direction"] != "neutral"}
    if not values:
        return "neutral"
    if "mixed" in values or ("bullish" in values and "bearish" in values):
        return "mixed"
    if "bullish" in values:
        return "bullish"
    if "bearish" in values:
        return "bearish"
    return "neutral"


def _is_mixed(active_sources: list[tuple[str, dict[str, Any]]], direction: str | None) -> bool:
    directions = {source["direction"] for _, source in active_sources if source["direction"] != "neutral"}
    return direction == "mixed" or "mixed" in directions or ("bullish" in directions and "bearish" in directions)


def _classify_state(
    *,
    active_count: int,
    freshest: int | None,
    overlap_window: int | None,
    mixed: bool,
) -> SignalFreshnessState:
    if active_count <= 0:
        return "inactive"
    if freshest is None:
        return "maturing" if active_count >= 2 else "stale"
    if freshest > 45:
        return "stale"
    if active_count >= 2 and freshest <= 7 and (overlap_window is None or overlap_window <= 10) and not mixed:
        return "fresh"
    if active_count == 1 and freshest <= 10:
        return "early"
    if active_count >= 2 and freshest <= 21 and (overlap_window is None or overlap_window <= 21):
        return "active"
    if active_count == 1 and freshest > 30:
        return "stale"
    return "maturing"


def _recency_score(days: int | None) -> int:
    if days is None:
        return 25
    if days <= 3:
        return 100
    if days <= 7:
        return 90
    if days <= 14:
        return 72
    if days <= 21:
        return 58
    if days <= 30:
        return 42
    if days <= 45:
        return 25
    return 8


def _breadth_score(active_count: int) -> int:
    if active_count <= 0:
        return 0
    if active_count == 1:
        return 45
    if active_count == 2:
        return 75
    if active_count == 3:
        return 90
    return 100


def _overlap_score(active_count: int, overlap_window: int | None) -> int:
    if active_count <= 1:
        return 50
    if overlap_window is None:
        return 35
    if overlap_window <= 5:
        return 100
    if overlap_window <= 10:
        return 88
    if overlap_window <= 21:
        return 65
    if overlap_window <= 30:
        return 42
    return 20


def _score(
    *,
    state: SignalFreshnessState,
    active_count: int,
    freshest: int | None,
    overlap_window: int | None,
    mixed: bool,
    has_smart_signal: bool,
    has_price_confirmation: bool,
) -> int:
    if state == "inactive":
        return 0

    support_score = min(100, 45 + (20 if has_smart_signal else 0) + (20 if has_price_confirmation else 0))
    alignment_score = 45 if mixed else (100 if active_count >= 2 else 70)
    raw = round(
        _recency_score(freshest) * 0.35
        + _breadth_score(active_count) * 0.25
        + _overlap_score(active_count, overlap_window) * 0.20
        + support_score * 0.15
        + alignment_score * 0.05
    )
    if mixed:
        raw = min(raw, 68)
    return _clamp_to_state(raw, state)


def _clamp_to_state(score: int, state: SignalFreshnessState) -> int:
    bounds = {
        "inactive": (0, 9),
        "stale": (10, 29),
        "maturing": (30, 49),
        "active": (50, 74),
        "early": (60, 79),
        "fresh": (75, 100),
    }
    minimum, maximum = bounds[state]
    return max(minimum, min(maximum, int(score)))


def _label(state: SignalFreshnessState, active_count: int) -> str:
    if state == "fresh":
        return "Fresh multi-source setup"
    if state == "early":
        return "Early setup"
    if state == "active":
        return "Active setup" if active_count < 2 else "Active multi-source setup"
    if state == "maturing":
        return "Maturing setup"
    if state == "stale":
        return "Stale setup"
    return "No active setup"


def _join_compact(parts: list[str]) -> str:
    if not parts:
        return ""
    if len(parts) == 1:
        return parts[0]
    if len(parts) == 2:
        return f"{parts[0]} and {parts[1]}"
    return f"{', '.join(parts[:-1])}, and {parts[-1]}"


def _explanation(
    *,
    state: SignalFreshnessState,
    active_count: int,
    active_labels: list[str],
    freshest: int | None,
    mixed: bool,
) -> str:
    if state == "inactive":
        return "No active directional confirmation sources are present in this lookback."
    if freshest is None:
        return "Active sources are present, but source timing is incomplete."
    if mixed:
        return "Recent sources are active, but mixed direction reduces timing conviction."
    if state == "fresh":
        names = _join_compact(active_labels[:2]) or "active sources"
        return f"Recent {names} remain tightly clustered."
    if state == "early":
        return "A single recent source is active, but broader confirmation is still limited."
    if state == "active":
        return "Recent evidence remains close enough to keep the setup timely."
    if state == "maturing":
        return "The setup is still active, but the confirming evidence is starting to age."
    if active_count <= 1:
        return "Current evidence looks stale and lacks recent reinforcement."
    return "Current evidence is aging and no longer tightly clustered."
