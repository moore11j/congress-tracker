"""Free recommendation distribution, isolated from FMP history and score inputs."""
from datetime import date, datetime, timedelta, timezone
import json
import os

from app.models import InsightsSnapshot, AnalystConsensusSnapshot
from app.services.finnhub_research import fetch_recommendations, FinnhubUnavailable

TTL = timedelta(hours=24)


def selected():
    return os.getenv('ANALYST_PROVIDER', 'fmp').strip().lower() == 'finnhub'


def _load(db, symbol, now):
    row = db.get(InsightsSnapshot, f'finnhub:recommendations:{symbol}')
    if row is None or row.source != 'finnhub':
        return None
    stamp = row.fetched_at.replace(tzinfo=timezone.utc) if row.fetched_at.tzinfo is None else row.fetched_at
    if not timedelta(0) <= now - stamp < TTL:
        return None
    payload = json.loads(row.payload_json)
    if payload.get('source') != 'finnhub' or payload.get('symbol') != symbol:
        return None
    return payload


def refresh(db, symbol):
    from app.services.analyst_consensus import analyst_symbol_rejection_reason
    symbol, reason = analyst_symbol_rejection_reason(symbol)
    if not (selected() or os.getenv('FREE_RESEARCH_WARMING_ENABLED', '0') == '1') or reason or not symbol:
        raise ValueError('Unsupported recommendation refresh')
    now = datetime.now(timezone.utc)
    if db.get_bind().dialect.name == 'postgresql':
        from sqlalchemy import text
        if not db.execute(text('SELECT pg_try_advisory_xact_lock(hashtext(:key))'), {'key': f'finnhub:recommendations:{symbol}'}).scalar():
            raise FinnhubUnavailable('refresh_in_progress')
    if _load(db, symbol, now) is not None:
        return {'status': 'cached'}
    payload = fetch_recommendations(symbol, observed_at=now)
    key = f'finnhub:recommendations:{symbol}'
    row = db.get(InsightsSnapshot, key)
    if row is None:
        row = InsightsSnapshot(kind=key, source='finnhub', fetched_at=now, payload_json='{}')
        db.add(row)
    row.source, row.fetched_at, row.payload_json = 'finnhub', now, json.dumps(payload, sort_keys=True)
    db.flush()
    return {'status': payload['status']}


def current_payload(db, symbol, *, include_details):
    from app.services import analyst_consensus as legacy
    from app.services.data_enrichment_queue import enqueue_data_enrichment_job
    now = datetime.now(timezone.utc)
    try:
        payload = _load(db, symbol, now)
    except (ValueError, TypeError):
        payload = None
    result = {'symbol': symbol, 'access': {'detailLevel': 'full_detail' if include_details else 'current_summary',
        'detailsLocked': not include_details, 'requiredPlanForDetails': 'premium'},
        'currentSnapshot': None, 'availability': {'status': 'unavailable', 'reason': 'replacement_cache_miss'},
        'providerStatus': {'status': 'unavailable', 'source': 'finnhub'},
        'message': 'Finnhub recommendation counts only. Price targets, estimate forecasts and recommendation changes are unavailable. Recorded FMP events remain historical.',
        'methodologyVersion': 'finnhub_recommendations_display_v1'}
    if payload is None:
        if os.getenv('FINNHUB_API_KEY', '').strip():
            enqueue_data_enrichment_job(job_type='analyst_recommendations', symbol=symbol, source='page_load',
                reason='replacement_analyst_refresh', priority=50)
        else:
            result['availability']['reason'] = 'missing_api_key'
        return result
    current = payload.get('current')
    if not current or payload.get('status') != 'ok' or (now.date()-date.fromisoformat(current['period'])).days > 45:
        result['availability']['reason'] = 'recommendation_period_unavailable'
        return result
    counts = legacy.rating_counts_from_row(current)
    weight = legacy.weighted_sentiment(counts)
    label = legacy.recommendation_label(weight, current['total'])
    snapshot = AnalystConsensusSnapshot(symbol=symbol, snapshot_date=date.fromisoformat(current['period']),
        source='finnhub', ingested_at=datetime.fromisoformat(payload['observed_at']),
        total_rating_count=current['total'], weighted_rating_value=weight, recommendation_label=label,
        availability_status='partial', provider_status='ok', **counts)
    view = legacy.snapshot_payload(snapshot) if include_details else legacy.snapshot_summary_payload(snapshot)
    view['availabilityStatus'] = 'partial'
    # The period isn't a publication timestamp and must not look like today's
    # upgrade/downgrade or a price-based return opportunity.
    view['sourcePeriod'] = current['period']
    view['source'] = 'finnhub'
    result.update(currentSnapshot=view, availability={'status': 'partial'}, providerStatus={'status': 'ok', 'source': 'finnhub'},
        currentSummary={'recommendationLabel': label, 'combinedLabel': label, 'trendDirection': 'Unavailable',
            'coverageLevel': 'limited', 'consensusImpliedUpsidePct': None, 'medianImpliedUpsidePct': None},
        freshness={'status': 'partial', 'daysOld': (now.date()-date.fromisoformat(current['period'])).days,
                   'basis': 'provider_recommendation_period'})
    return result
