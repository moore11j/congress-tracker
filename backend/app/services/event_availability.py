"""Separate official filing dates from an explicitly observed availability day.

Legacy events retain their existing date semantics until a source-bound review
adds this evidence. Invalid explicit evidence must not fall back to an earlier
filing date. Day precision matches the existing daily execution models.
"""
from datetime import date


def available_event_date(payload, filing_date):
    if 'source_availability' not in payload:
        return filing_date
    evidence = payload['source_availability']
    if not isinstance(evidence, dict) or evidence.get('basis') not in {
            'direct_publication', 'retained_legacy_report_date'}:
        return None
    try:
        day = date.fromisoformat(evidence['date'])
    except (KeyError, TypeError, ValueError):
        return None
    if filing_date is None or day < filing_date:
        return None
    return day


def availability_timestamp_expr(db, fallback):
    """Use the preserved arrival index for explicitly annotated events only."""
    from sqlalchemy import case, cast, func
    from sqlalchemy.dialects.postgresql import JSONB
    from app.models import Event
    if db.get_bind().dialect.name == 'postgresql':
        basis = cast(Event.payload_json, JSONB)['source_availability']['basis'].astext
    else:
        # Existing legacy malformed payloads retain their prior behavior.
        basis = case((func.json_valid(Event.payload_json),
            func.json_extract(Event.payload_json, '$.source_availability.basis')), else_=None)
    return case((basis.in_(['direct_publication','retained_legacy_report_date']), Event.ts), else_=fallback)
