"""Select one complete corroborated statement; never splice document values."""
import hashlib
import json
import math

VERSION = 'sec_same_date_complete_statement_v1'


def select(items):
    from app.services.sec_tabular_reporting import TabularReportingError

    def hold():
        raise TabularReportingError('table_current_selection_ambiguous')

    if not 2 <= len(items) <= 6:
        hold()
    identities = set()
    projections = []
    sources = []
    for result, raw, args in items:
        evidence = result['evidence']
        source = evidence['source']
        identities.add((source['cik'], source['symbol'], evidence['accounting_basis'],
                        evidence['currency'], evidence['current_period'], source['filed']))
        if evidence['conflicts']:
            hold()
        if hashlib.sha256(raw).hexdigest() != source['statement_sha256']:
            hold()
        values = {}
        for metric, records in result['records'].items():
            for (start, end), point in records.items():
                if (point is None or isinstance(point.get('value'), bool)
                        or not isinstance(point.get('value'), (int, float))
                        or not math.isfinite(point['value'])
                        or point.get('unit') != evidence['currency']):
                    hold()
                key = (metric, str(start) if start else '', str(end), point['unit'])
                values[key] = point['value']
        if not values:
            hold()
        projections.append(values)
        digest = hashlib.sha256(json.dumps(sorted(values.items()), separators=(',', ':')).encode()).hexdigest()
        sources.append({'source': source, 'normalized_observations': len(values),
                        'normalized_sha256': digest})
    if len(identities) != 1:
        hold()
    urls = [s['source']['statement_url'] for s in sources]
    if len(set(urls)) != len(urls):
        hold()
    # One existing document must contain every supported observation from all
    # peers, with exact agreement. Complementary fragments cannot form a result.
    eligible = [i for i, values in enumerate(projections)
                if all(all(key in values and values[key] == value for key, value in peer.items())
                       for peer in projections)]
    if not eligible:
        hold()
    chosen = min(eligible, key=lambda i: urls[i])
    return chosen, {'methodology': VERSION, 'values_spliced': False,
                    'selected_url': urls[chosen],
                    'sources': sorted(sources, key=lambda s: s['source']['statement_url'])}


def cached_valid(proof, selected_source):
    """Check persisted proof structure; original values are verified by select."""
    import re
    from app.services.sec_statement_currency import source_identity
    try:
        if (not isinstance(proof, dict) or proof.get('methodology') != VERSION
                or proof.get('values_spliced') is not False
                or proof.get('selected_url') != selected_source['statement_url']):
            return False
        peers = proof['sources']
        if not isinstance(peers, list) or not 2 <= len(peers) <= 6:
            return False
        urls = []
        selected_count = None
        counts = []
        for peer in peers:
            source = peer['source']
            urls.append(source['statement_url'])
            count = peer['normalized_observations']
            if (type(count) is not int or count <= 0
                    or not re.fullmatch(r'[0-9a-f]{64}', peer['normalized_sha256'])
                    or any(source[k] != selected_source[k] for k in ('cik', 'symbol', 'filed'))
                    or source_identity(source['statement_url']) != (source['cik'], source['accession'])
                    or source_identity(source['cover_url']) != (source['cik'], source['accession'])
                    or not all(re.fullmatch(r'[0-9a-f]{64}', source[k])
                               for k in ('statement_sha256', 'cover_sha256', 'directory_sha256'))):
                return False
            counts.append(count)
            if source['statement_url'] == proof['selected_url']:
                if source != selected_source:
                    return False
                selected_count = count
        return (len(set(urls)) == len(urls) and urls == sorted(urls)
                and selected_count == max(counts))
    except (KeyError, TypeError, ValueError, AttributeError):
        return False
