from __future__ import annotations

from datetime import datetime, timezone


def calculate_smart_score(
    *, unusual_multiple: float, amount_max: float | None, ts: datetime
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

    score = min(conviction_score + recency_score + size_score, 100)
    if score >= 75:
        band = "strong"
    elif score >= 55:
        band = "notable"
    elif score >= 35:
        band = "mild"
    else:
        band = "noise"

    return score, band
