from __future__ import annotations

from datetime import datetime, timezone
from typing import Mapping, Any


CROSS_SOURCE_CONFIRMATION_BONUS = 6
REPEAT_SOURCE_CONFIRMATION_BONUS = 2
MAX_GOVERNMENT_EXPOSURE_BONUS = 2


def _confirmation_bonus(confirmation_30d: Mapping[str, Any] | None) -> int:
    if not confirmation_30d:
        return 0

    bonus = 0
    if confirmation_30d.get("cross_source_confirmed_30d") is True:
        bonus += CROSS_SOURCE_CONFIRMATION_BONUS
    if confirmation_30d.get("repeat_insider_30d") is True:
        bonus += REPEAT_SOURCE_CONFIRMATION_BONUS
    if confirmation_30d.get("repeat_congress_30d") is True:
        bonus += REPEAT_SOURCE_CONFIRMATION_BONUS
    return bonus


def _government_exposure_bonus(government_exposure_signal_boost: float | None) -> int:
    if not government_exposure_signal_boost or government_exposure_signal_boost <= 0:
        return 0
    scaled_bonus = round(government_exposure_signal_boost * 0.6)
    return max(0, min(int(scaled_bonus), MAX_GOVERNMENT_EXPOSURE_BONUS))


def calculate_smart_score(
    *,
    unusual_multiple: float,
    amount_max: float | None,
    ts: datetime,
    confirmation_30d: Mapping[str, Any] | None = None,
    government_exposure_signal_boost: float | None = None,
) -> tuple[int, str]:
    conviction_score = 5
    if unusual_multiple >= 30:
        conviction_score = 50
    elif unusual_multiple >= 20:
        conviction_score = 45
    elif unusual_multiple >= 10:
        conviction_score = 38
    elif unusual_multiple >= 5:
        conviction_score = 30
    elif unusual_multiple >= 3:
        conviction_score = 20
    elif unusual_multiple >= 2:
        conviction_score = 12

    now = datetime.now(timezone.utc)
    ts_utc = ts if ts.tzinfo is not None else ts.replace(tzinfo=timezone.utc)
    days_old = max((now - ts_utc).days, 0)

    if days_old <= 7:
        recency_score = 20
    elif days_old <= 14:
        recency_score = 16
    elif days_old <= 30:
        recency_score = 10
    elif days_old <= 60:
        recency_score = 5
    else:
        recency_score = 2

    size_score = 0
    if amount_max is not None:
        if amount_max >= 1_000_000:
            size_score = 20
        elif amount_max >= 500_000:
            size_score = 14
        elif amount_max >= 100_000:
            size_score = 8
        else:
            size_score = 3

    confirmation_bonus = _confirmation_bonus(confirmation_30d)
    government_bonus = _government_exposure_bonus(government_exposure_signal_boost)
    score = min(conviction_score + recency_score + size_score + confirmation_bonus + government_bonus, 100)
    if score >= 75:
        band = "strong"
    elif score >= 55:
        band = "notable"
    elif score >= 35:
        band = "mild"
    else:
        band = "noise"

    return score, band
