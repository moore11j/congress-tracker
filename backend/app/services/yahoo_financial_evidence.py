"""Source-bound Yahoo diagnostics; agreement is never publication approval.

The candidate quarter window uses provider end labels only. Its sum is a
diagnostic, not a reconstructed fiscal year or canonical trailing value.
"""
from collections import defaultdict
from datetime import date
from decimal import Decimal, localcontext

from app.clients.yahoo_financials import parse_timeseries, YahooStatementError

VERSION = 'yahoo_financial_bundle_v1'


def prepare_bundle(captures, *, symbol):
    """Prepare up to six captured JSON responses without network or DB access.

    Every capture supplies raw, statement, source_url and observed_at. The raw
    response is reparsed instead of accepting caller-projected financial values.
    Exact repeats merge provenance; revisions with different values are held.
    """
    if not isinstance(captures, (tuple, list)) or not 1 <= len(captures) <= 6:
        raise YahooStatementError('bundle_capture_count_invalid')
    if any(not isinstance(c, dict) or not isinstance(c.get('raw'), bytes) for c in captures):
        raise YahooStatementError('bundle_capture_invalid')
    if sum(len(c['raw']) for c in captures) > 4_000_000:
        raise YahooStatementError('bundle_size_invalid')
    sources, observations = {}, defaultdict(dict)
    for capture in captures:
        result = parse_timeseries(capture['raw'], symbol=symbol,
            statement=capture['statement'], source_url=capture['source_url'],
            observed_at=capture['observed_at'])
        # Keep source capture time with its hash. A later observation never
        # rewrites an earlier capture or establishes historical availability.
        source_key = (result['source_sha256'], result['source_url'], result['observed_at'], result['statement'])
        sources[source_key] = {k: v for k, v in result.items() if k != 'values'}
        for point in result['values']:
            key = (result['statement'], point['metric'], point['frequency'], point['provider_as_of_date'])
            variant = (point['currency'], Decimal(str(point['value'])))
            entry = observations[key].setdefault(variant, {'point': point, 'sources': set()})
            entry['sources'].add(source_key)
    ordered_sources = sorted(sources)
    source_ids = {key: i for i, key in enumerate(ordered_sources)}
    points, conflicts = {}, []
    for key, variants in sorted(observations.items()):
        entries = [{'point': item['point'], 'source_indices': sorted(source_ids[s] for s in item['sources'])}
            for _, item in sorted(variants.items())]
        if len(entries) != 1:
            conflicts.append({'statement': key[0], 'metric': key[1], 'frequency': key[2],
                'provider_as_of_date': key[3], 'reason': 'conflicting_source_observations', 'alternatives': entries})
        else:
            points[key] = entries[0]
    comparisons = []
    for key, entry in sorted(points.items()):
        statement, metric, frequency, end = key
        if statement == 'balance' or frequency == 'quarterly':
            continue  # Instant stocks are never summed across quarters.
        comparison = {'statement': statement, 'metric': metric, 'frequency': frequency,
            'provider_as_of_date': end, 'reported': entry, 'canonical_eligible': False,
            'status': 'incomplete_quarter_window', 'candidate_quarters': []}
        candidates = sorted(k for k in observations if k[:3] == (statement, metric, 'quarterly') and k[3] <= end)[-4:]
        if len(candidates) == 4 and candidates[-1][3] == end:
            dates = [date.fromisoformat(k[3]) for k in candidates]
            if all(70 <= (b-a).days <= 110 for a, b in zip(dates, dates[1:])):
                if any(k not in points for k in candidates):
                    comparison['status'] = 'conflicting_quarter_observations'
                else:
                    quarters = [points[k] for k in candidates]
                    comparison['candidate_quarters'] = quarters
                    if any(q['point']['currency'] != entry['point']['currency'] for q in quarters):
                        comparison['status'] = 'currency_mismatch'
                    else:
                        # Decimal avoids false binary-float discrepancies. Source
                        # values have already been bounded to finite float range.
                        with localcontext() as context:
                            context.prec = 1024
                            total = sum((Decimal(str(q['point']['value'])) for q in quarters), Decimal(0))
                            delta = total - Decimal(str(entry['point']['value']))
                        comparison.update(status='agrees' if delta == 0 else 'differs',
                            candidate_sum=str(total), difference=str(delta))
        comparisons.append(comparison)
    return {'version': VERSION, 'provider': 'yahoo_finance', 'symbol': symbol,
        'status': 'evidence_only', 'canonical_eligible': False,
        'sources': [sources[k] for k in ordered_sources],
        'observations': [points[k] for k in sorted(points)], 'conflicts': conflicts,
        'comparisons': comparisons,
        'limitations': ['Quarter windows use provider end labels; fiscal starts and issuer periods remain unverified.',
            'Agreement does not resolve provider definitions, revisions, freshness or issuer/security identity.',
            'No candidate sum replaces a reported value or enters a public ratio, score or alert.']}
