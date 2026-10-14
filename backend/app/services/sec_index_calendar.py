"""Only SEC-confirmed closure dates may suppress a weekday index request.

Unknown years retain weekday requests and visible errors. This calendar never
changes source transport, filing identity, backlog processing or publication.
"""
from datetime import date, timedelta

SOURCE_URL = "https://www.sec.gov/submit-filings/filer-support-resources/edgar-calendar"
REVIEWED_AT = "2026-10-14"
VERSION = "sec_edgar_calendar_2026_v1"
# Published by SEC, last updated April 28, 2026. This is EDGAR's calendar,
# not the stock-market calendar (Columbus/Veterans Day differ).
REVIEWED_YEARS = frozenset({2026})
CLOSED_DATES = frozenset(date.fromisoformat(value) for value in (
    "2026-01-01", "2026-01-19", "2026-02-16", "2026-05-25", "2026-06-19",
    "2026-07-03", "2026-09-07", "2026-10-12", "2026-11-11", "2026-11-26", "2026-12-25",
))


def confirmed_closure(day: date) -> bool:
    return day in CLOSED_DATES


def window_receipt(start: date, end: date) -> dict:
    days = [start + timedelta(days=offset) for offset in range((end-start).days+1)]
    return {
        "version": VERSION,
        "reviewed_at": REVIEWED_AT,
        "source_url": SOURCE_URL,
        "closed_days": [{"date": str(day), "reason": "published_edgar_closure", "source_url": SOURCE_URL}
                        for day in days if confirmed_closure(day)],
        "weekend_days": [str(day) for day in days if day.weekday() >= 5],
        "requested_weekdays": [str(day) for day in days if day.weekday() < 5 and not confirmed_closure(day)],
        "unreviewed_years": sorted({day.year for day in days} - REVIEWED_YEARS),
    }
