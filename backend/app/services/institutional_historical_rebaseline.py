"""Explicit historical activity refresh, with no new feed events or alerts.

The caller must first reconcile source positions and changes, refresh complete
peer summaries, and own the canonical feed transaction. This module never
selects a source, sends notifications, or applies source-value corrections.
"""
from datetime import date, datetime
from sqlalchemy import select
from app.models import InstitutionalActivityEvent, InstitutionalPositionChange, InstitutionalSymbolSummary
from app.services import institutional_activity as activity
from app.services.feed_source_control import canonical_writer


@canonical_writer('sec_13f')
def rebaseline_historical_activity(db, summary, *, publish_since: date):
    if not isinstance(publish_since, date) or isinstance(publish_since, datetime):
        raise ValueError('An explicit historical publication boundary is required')
    if summary.id is None or not summary.latest_filing_date or summary.latest_filing_date >= publish_since:
        raise ValueError('Only an existing complete historical summary may be rebaselined')
    period = (summary.report_year, summary.report_quarter)
    if period[1] not in (1, 2, 3, 4):
        raise ValueError('Invalid historical report quarter')
    for model in (InstitutionalPositionChange, InstitutionalActivityEvent):
        dates = db.scalars(select(model.filing_date).where(
            model.normalized_symbol == summary.normalized_symbol,
            model.report_year == period[0], model.report_quarter == period[1]))
        if any(filed is None or filed >= publish_since for filed in dates):
            raise ValueError('Historical scope contains current or undated activity')
    # A refresh can discover an old cluster or cross an amount threshold. Its
    # current activity is refreshed, but it cannot become a fresh feed event.
    created = activity.generate_activity_events_for_symbol(db, summary, preserve_published_events=True)
    db.flush()
    rows = list(db.scalars(select(InstitutionalActivityEvent).where(
        InstitutionalActivityEvent.normalized_symbol == summary.normalized_symbol,
        InstitutionalActivityEvent.report_year == period[0],
        InstitutionalActivityEvent.report_quarter == period[1])))
    refreshed = 0
    for row in rows:
        if row.freshness_status == 'superseded':
            continue
        inserted = activity._upsert_feed_event(db, row, existing_only=True)
        if inserted:
            raise RuntimeError('Historical rebaseline attempted a new feed event')
        refreshed += 1
    db.flush()
    return dict(historical_activity_created=created, activity_candidates=refreshed,
                inserted_feed_events=0, deleted_feed_events=0, emails=0)


@canonical_writer('sec_13f')
def rebaseline_historical_symbol(db, symbol, report_year, report_quarter, *, publish_since: date):
    summary = db.scalar(select(InstitutionalSymbolSummary).where(
        InstitutionalSymbolSummary.normalized_symbol == symbol,
        InstitutionalSymbolSummary.report_year == report_year,
        InstitutionalSymbolSummary.report_quarter == report_quarter))
    if summary is None:
        raise ValueError('Historical rebaseline requires an existing summary identity')
    if (not isinstance(publish_since, date) or isinstance(publish_since, datetime)
            or not summary.latest_filing_date or summary.latest_filing_date >= publish_since):
        raise ValueError('An existing historical summary and explicit boundary are required')
    def stable(row):
        return {col.name: getattr(row, col.name) for col in row.__table__.columns
                if col.name not in {'generated_at', 'updated_at'}}
    before = stable(summary)
    generated_at, updated_at = summary.generated_at, summary.updated_at
    with db.begin_nested():
        summary = activity.refresh_symbol_summary(db, symbol, report_year, report_quarter)
        if stable(summary) == before:
            summary.generated_at, summary.updated_at = generated_at, updated_at
        return rebaseline_historical_activity(db, summary, publish_since=publish_since)
