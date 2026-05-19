from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any

BULLISH_CONFIRMATION_SCREEN_NAMES = {"bullish confirmation", "bullish confirmations"}
BEARISH_CONFIRMATION_SCREEN_NAMES = {"bearish confirmation", "bearish confirmations"}


def load_saved_screen_params(params_json: str | None, *, screen_name: str | None = None) -> dict[str, Any]:
    return normalize_saved_screen_params(_loads_dict(params_json), screen_name=screen_name)


def normalize_saved_screen_params(params: Mapping[str, Any] | None, *, screen_name: str | None = None) -> dict[str, Any]:
    normalized = dict(params or {})
    screen_key = (screen_name or "").strip().lower()
    if screen_key in BULLISH_CONFIRMATION_SCREEN_NAMES:
        normalized.setdefault("confirmation_direction", "bullish")
        normalized.setdefault("confirmation_score_min", "60")
        normalized.setdefault("confirmation_band", "strong_plus")
    elif screen_key in BEARISH_CONFIRMATION_SCREEN_NAMES:
        normalized.setdefault("confirmation_direction", "bearish")
        normalized.setdefault("confirmation_score_min", "60")
        normalized.setdefault("confirmation_band", "strong_plus")
    return normalized


def _loads_dict(payload: str | None) -> dict[str, Any]:
    if not payload:
        return {}
    try:
        parsed = json.loads(payload)
    except (TypeError, ValueError):
        return {}
    return parsed if isinstance(parsed, dict) else {}
