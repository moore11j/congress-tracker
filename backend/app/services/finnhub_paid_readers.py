"""Cache-only Finnhub projections into existing product contracts.

No network, refresh queue, canonical writes or automatic publication review.
The empty reviewed-basis registry is intentional until actual provider basis
evidence is available. Enabling readers alone cannot publish unreviewed data.
"""
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json
import os
import re
from urllib.parse import urlsplit

from app.services import finnhub_paid_preparation as preparation
from app.services.finnhub_research import FinnhubUnavailable, MAX_BYTES

VERSION = 'finnhub_paid_readers_v1'
# Key: (provider symbol, kind, frequency). A reviewed entry must contain
# currency, unit, accountingBasis, shareBasis, reviewedAt, validUntil,
# evidenceUrl and evidenceSha256. No listing-currency inference is permitted.
REVIEWED_BASIS = {}


def enabled():
    return os.getenv('FINNHUB_PAID_READERS_ENABLED', '0') == '1'


def _review(provider_symbol, kind, frequency, now):
    policy = REVIEWED_BASIS.get((provider_symbol, kind, frequency))
    if not isinstance(policy, dict):
        return None
    expected = {'currency': 'USD', 'unit': 'currency', 'accountingBasis': 'us_gaap', 'shareBasis': 'not_applicable'}
    if kind == 'eps':
        expected.update(unit='currency_per_share', shareBasis='listed_diluted_share')
    elif kind == 'targets':
        expected.update(unit='currency_per_share', accountingBasis='not_applicable', shareBasis='listed_share')
    if kind != 'targets':
        expected.update(statementMethodology='sec_statements_v1', comparisonCompatible=True)
    if any(type(policy.get(key)) is not type(value) or policy.get(key) != value for key, value in expected.items()):
        return None
    try:
        reviewed = datetime.fromisoformat(policy['reviewedAt'])
        expires = datetime.fromisoformat(policy['validUntil'])
        url = urlsplit(policy['evidenceUrl'])
        valid = (reviewed.tzinfo is not None and expires.tzinfo is not None
                 and reviewed <= now < expires and expires - reviewed <= timedelta(days=180)
                 and url.scheme == 'https' and url.hostname in {'finnhub.io', 'www.finnhub.io'}
                 and not url.username and not url.password and not url.query and not url.fragment
                 and re.fullmatch(r'[a-f0-9]{64}', policy['evidenceSha256']))
    except (KeyError, ValueError, TypeError):
        return None
    return deepcopy(policy) if valid else None


def read(db, symbol, kind, *, frequency=None, now=None):
    """Read and verify a staged scope without changing any evidence clock."""
    unavailable = lambda reason: {'status': 'unavailable', 'reason': reason, 'data': None}
    if not enabled():
        return unavailable('disabled')
    now = preparation._now(now)
    try:
        symbol, provider_symbol, key = preparation.scope(symbol, kind, frequency)
    except FinnhubUnavailable:
        return unavailable('invalid_scope')
    review = _review(provider_symbol, kind, frequency, now)
    if review is None:
        return unavailable('source_basis_unverified')
    from app.models import InsightsSnapshot
    row = db.get(InsightsSnapshot, key)
    if row is None or row.source != preparation.VERSION:
        return unavailable('cache_miss')
    try:
        fetched = row.fetched_at
        if fetched.tzinfo is None:
            fetched = fetched.replace(tzinfo=timezone.utc)
        if not timedelta(0) <= now - fetched < preparation.TTL:
            return unavailable('cache_stale')
        if not isinstance(row.payload_json, str) or len(row.payload_json.encode()) > MAX_BYTES * 3:
            return unavailable('cache_invalid')
        saved = json.loads(row.payload_json)
        # Aliases share a provider key, but never discard a share class. Validate
        # at the original caller identity/time before projecting the requested alias.
        original_symbol = saved['normalized']['symbol']
        if preparation.scope(original_symbol, kind, frequency)[1] != provider_symbol:
            return unavailable('cache_identity_mismatch')
        original = preparation.normalize(saved['raw'], original_symbol, kind, frequency=frequency, observed_at=fetched)
        if original != saved['normalized']:
            return unavailable('cache_invalid')
        current = preparation.normalize(saved['raw'], original_symbol, kind, frequency=frequency, observed_at=now)
    except (KeyError, ValueError, TypeError, AttributeError, FinnhubUnavailable):
        return unavailable('cache_invalid')
    if current['status'] not in {'prepared', 'partial'}:
        return unavailable('source_' + current['status'])
    current.update(symbol=symbol, observedAt=original['observedAt'], evaluatedAt=now.isoformat(),
                   publicationReview=review, publicationEligible=True)
    # Staged evidence retains its original unverified flags. This separate read
    # projection is backed by the reviewed contract, never an in-place promotion.
    current.update({key: review[key] for key in ('currency', 'accountingBasis', 'shareBasis', 'unit')})
    return {'status': 'partial', 'reason': None, 'data': current}


def _evidence(data):
    return {key: deepcopy(data[key]) for key in ('source', 'endpoint', 'sourceSha256', 'observedAt',
        'sourceUpdatedAt', 'currency', 'unit', 'accountingBasis', 'shareBasis', 'estimateBasis', 'publicationReview')}


def forecast(db, symbol, frequency, *, now=None):
    now = preparation._now(now)
    reads = {kind: read(db, symbol, kind, frequency=frequency, now=now) for kind in ('eps', 'revenue')}
    usable = {kind: value['data'] for kind, value in reads.items()
              if value['data'] is not None and value['data']['nextPeriod'] is not None}
    if not usable:
        return None
    next_date = min(value['nextPeriod']['period'] for value in usable.values())
    selected = {kind: data for kind, data in usable.items() if data['nextPeriod']['period'] == next_date}
    identities = {(data['nextPeriod']['fiscalYear'], None if frequency == 'annual' else data['nextPeriod']['fiscalQuarter'])
                  for data in selected.values()}
    if len(identities) != 1:
        return None
    year, quarter = next(iter(identities))
    result = dict.fromkeys(('revenueEstimate', 'revenueLow', 'revenueHigh', 'epsEstimate', 'epsLow', 'epsHigh',
                           'earningsEstimate', 'earningsLow', 'earningsHigh'))
    result.update(period=f'FY {year}' if frequency == 'annual' else f'Q{quarter} FY {year}', date=next_date,
                  fiscalYear=year, fiscalQuarter=quarter, source='finnhub', currency='USD',
                  estimateBasis='provider_average_includes_proprietary_estimates',
                  sourceEvidence={}, status='partial', forwardValuationEligible=False)
    for kind, data in selected.items():
        row = data['nextPeriod']
        if row['average'] is None or (row['numberAnalysts'] or 0) <= 0:
            continue
        for suffix, field in (('Estimate', 'average'), ('Low', 'low'), ('High', 'high')):
            result[kind + suffix] = row[field]
        result['sourceEvidence'][kind] = _evidence(data)
        result['sourceEvidence'][kind]['numberAnalysts'] = row['numberAnalysts']
    return result if result['sourceEvidence'] else None


def attach_financials(db, symbol, payload, *, now=None):
    """Overlay compatible forecasts without modifying the SEC stored panel."""
    if not enabled():
        return payload
    evidence = payload.get('sourceEvidence') or {}
    if (payload.get('symbol') != symbol or payload.get('source') != 'sec_edgar'
            or evidence.get('methodology') != 'sec_statements_v1'
            or evidence.get('reviewedReportingPolicy') or evidence.get('nativeReporting')
            or evidence.get('standardForeignReporting') or payload.get('reportingCurrency', 'USD') != 'USD'):
        return payload
    now = preparation._now(now)
    forecasts = {'nextQuarter': forecast(db, symbol, 'quarterly', now=now),
                 'nextFiscalYear': forecast(db, symbol, 'annual', now=now)}
    if not any(forecasts.values()):
        return payload
    result = deepcopy(payload)
    result['forecasts'] = forecasts
    result['forecastBasis'] = ('Finnhub averages include analyst and proprietary estimates. '
                               'Fiscal periods and USD reporting basis are preserved. Coverage is partial.')
    result.setdefault('sections', {})['forecasts'] = 'partial'
    result.setdefault('subsections', {})['analyst_estimates'] = {
        'status': 'limited', 'reason_code': 'partial_finnhub_forecasts', 'data': deepcopy(forecasts)}
    result['status'], result['unavailable'] = 'partial', False
    # Keep the SEC source clock/evidence and valuation fields unchanged.
    result['message'] = 'SEC reported financials with separately sourced Finnhub forecasts; coverage is partial.'
    return result


def attach_targets(db, symbol, payload, *, include_details, now=None):
    """Premium target range, independently timed from monthly recommendations."""
    if not enabled() or not include_details:
        return payload
    data = read(db, symbol, 'targets', now=now)['data']
    if data is None or data['targetMean'] is None or (data['numberAnalysts'] or 0) <= 0:
        return payload
    result = deepcopy(payload)
    snapshot = result.get('currentSnapshot')
    if snapshot is None:
        snapshot = {'symbol': symbol, 'source': 'finnhub', 'snapshotDate': None,
                    'recommendationLabel': None, 'availabilityStatus': 'partial', 'providerStatus': 'ok'}
        result['currentSnapshot'] = snapshot
    snapshot['priceTargetRange'] = {'high': data['targetHigh'], 'low': data['targetLow'],
        'median': data['targetMedian'], 'consensus': data['targetMean'], 'average': data['targetMean'],
        'analystCount': data['numberAnalysts']}
    snapshot['targetSourceEvidence'] = _evidence(data)
    snapshot['targetSourceUpdatedAt'] = data['sourceUpdatedAt']
    snapshot['targetObservedAt'] = data['observedAt']
    snapshot['targetHorizon'] = data['targetHorizon']
    # No implicit current quote, split adjustment, upside, score or target event.
    snapshot['impliedUpside'] = {'medianPct': None, 'consensusPct': None}
    result['availability'] = {'status': 'partial'}
    result['providerStatus'] = {'status': 'ok', 'source': 'finnhub'}
    result['message'] = ('Finnhub recommendation periods and price-target updates are separate. '
                         'Target horizon and price-based upside are unavailable. Recorded FMP events remain historical.')
    return result
