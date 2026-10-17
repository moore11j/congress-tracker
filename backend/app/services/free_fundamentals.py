"""SEC-first fundamentals supplemented by explicitly attributed free metrics."""
from copy import deepcopy
from datetime import date
import json

from app.services import finnhub_metrics, sec_fundamentals
from app.services.finnhub_research import FinnhubUnavailable

VERSION = 'free_fundamentals_v1'


def combine(values, metrics):
    if values.get('provider') != 'sec_edgar' or metrics.get('symbol') != values.get('symbol'):
        raise ValueError('Incompatible fundamentals identity')
    if metrics.get('version') != finnhub_metrics.VERSION or metrics.get('source') != 'finnhub':
        raise ValueError('Incompatible fundamentals source')
    result = deepcopy(values)
    result['provider'] = 'free_direct'
    evidence = json.loads(result['source_evidence_json'])
    from app.services.sec_tagged_foreign import cached_currency_valid
    if not cached_currency_valid(evidence):
        raise ValueError('Unreviewed foreign statement currency')
    from app.services.finnhub_metric_identity import verify_combination
    verify_combination(metrics, evidence)
    evidence['methodology'] = VERSION
    evidence['supplement'] = deepcopy(metrics)
    evidence['supplement']['used_fields'] = []
    for field, value in metrics['values'].items():
        if field not in {*finnhub_metrics.SCALARS, *finnhub_metrics.SERIES, *finnhub_metrics.DERIVED}:
            raise ValueError('Unreviewed supplemental field')
        if result.get(field) is not None:
            continue
        field_period = metrics['fields'][field]['period_date']
        # A lagging dated ratio cannot fill a newer SEC period's missing field.
        if field_period and result.get('period_date') and field_period < str(result['period_date']):
            if field != 'ebitda_growth' or metrics['fields'][field].get('basis') != 'annual_yoy':
                continue
            # Annual growth has its own fiscal period, not the SEC TTM end.
            # Reject it if SEC evidence already contains a newer full year.
            revenue = evidence.get('current', {}).get('revenue') or {}
            annual_ends = []
            for fact in revenue.get('inputs', []):
                if fact.get('start') and fact.get('end'):
                    start, end = date.fromisoformat(fact['start']), date.fromisoformat(fact['end'])
                    if 350 <= (end-start).days+1 <= 380:
                        annual_ends.append(str(end))
            if (annual_ends and field_period < max(annual_ends)) or (
                    date.fromisoformat(str(result['period_date'])) - date.fromisoformat(field_period)).days > 380:
                continue
        result[field] = value
        evidence['supplement']['used_fields'].append(field)
    result['source_evidence_json'] = json.dumps(evidence, sort_keys=True)
    return result


def fetch_fundamentals(symbol):
    # SEC verifies issuer identity and preserves the independently selected
    # Massive market-data path. Neither fallback calls FMP.
    values = sec_fundamentals.fetch_fundamentals(symbol)
    try:
        metrics = finnhub_metrics.prepare(symbol)
        return combine(values, metrics)
    except FinnhubUnavailable as exc:
        values['provider'] = 'free_direct'
        evidence = json.loads(values['source_evidence_json'])
        evidence.update(methodology=VERSION, supplement_error=str(exc))
        values['source_evidence_json'] = json.dumps(evidence, sort_keys=True)
        return values
