"""SEC-first fundamentals supplemented by explicitly attributed free metrics."""
from copy import deepcopy
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
    from app.services.finnhub_metric_identity import verify_combination
    verify_combination(metrics, evidence)
    evidence['methodology'] = VERSION
    evidence['supplement'] = deepcopy(metrics)
    evidence['supplement']['used_fields'] = []
    for field, value in metrics['values'].items():
        if field not in {*finnhub_metrics.SCALARS, *finnhub_metrics.SERIES}:
            raise ValueError('Unreviewed supplemental field')
        if result.get(field) is not None:
            continue
        field_period = metrics['fields'][field]['period_date']
        # A lagging dated ratio cannot fill a newer SEC period's missing field.
        if field_period and result.get('period_date') and field_period < str(result['period_date']):
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
