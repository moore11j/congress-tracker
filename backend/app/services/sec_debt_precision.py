"""Original-file precision evidence for conflicting complete debt totals.

Evidence only: does not relax overlap guards or select a canonical metric.
"""
from decimal import Decimal, localcontext
import math
import re

VERSION = 'sec_debt_precision_v1'
NAMES = ('leverage_debt_with_finance_leases', 'leverage_current_debt',
         'leverage_noncurrent_debt_with_finance_leases')


def decimals(raw):
    if raw == 'INF':
        return raw
    if not isinstance(raw, str) or not re.fullmatch(r'-?[0-9]{1,2}', raw):
        return None
    return int(raw) if -20 <= int(raw) <= 20 else None


def request(payload, *, company, available_by, end, forms):
    from app.services import sec_fundamentals as sec, sec_leverage as leverage
    if not 0 <= (available_by-end).days <= 180:
        return None
    points = [sec._records(payload, 'equity', available_by, concepts=leverage.INSTANT[name],
                          forms=forms).get((None, end)) for name in NAMES]
    if (not all(points) or not leverage._compatible(points)
            or any(p['value'] < 0 or not math.isfinite(p['value']) for p in points)):
        return None
    maturity_sum = points[1]['value']+points[2]['value']
    if not math.isfinite(maturity_sum) or math.isclose(points[0]['value'], maturity_sum, rel_tol=1e-12, abs_tol=.01):
        return None
    recent = company.get('filings', {}).get('recent', {})
    keys = ('accessionNumber', 'form', 'filingDate', 'reportDate', 'primaryDocument')
    arrays = [recent.get(k, []) for k in keys]
    if len({len(a) for a in arrays}) != 1:
        return None
    matches = {(report, filed, acc, form, filename)
        for acc, form, filed, report, filename in zip(*arrays)
        if acc == points[0]['accession'] and filed == points[0]['filed']
        and report == str(end) and form in forms}
    return next(iter(matches)) if len(matches) == 1 else None


def acquire(payload, *, company, available_by, end, forms, cik, client):
    selected = request(payload, company=company, available_by=available_by, end=end, forms=forms)
    if selected is None:
        return None
    report, filed, accession, form, filename = selected
    evidence = {'methodology': VERSION, 'status': 'unavailable', 'cik': cik,
                'accession': accession, 'filed': filed, 'report_date': report,
                'form': form, 'scope': 'optional debt precision; no replacement core facts'}
    result = {'debt_precision_only': True, 'evidence': evidence, 'facts': {}, 'provenance': {}}
    if (not re.fullmatch(r'[0-9]{10}-[0-9]{2}-[0-9]{6}', accession)
            or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,180}\.html?', filename)):
        evidence['reason'] = 'filing_directory_identity'
        return result
    url = f'https://www.sec.gov/Archives/edgar/data/{int(cik)}/{accession.replace("-", "")}/{filename}'
    from app.services.sec_inline_financials import parse, InlineFactsError
    from app.clients.direct_sources import DirectSourceError
    evidence['url'] = url
    try:
        parsed = parse(client.get(url), cik=cik, accession=accession, filed=filed,
                       report_date=report, form=form, url=url)
    except (InlineFactsError, DirectSourceError) as exc:
        evidence['reason'] = type(exc).__name__
        return result
    evidence.update(status='original_parsed', sha256=parsed['evidence']['sha256'])
    result.update(facts=parsed['facts'], provenance=parsed['provenance'])
    return result


def annotate(records, supplement):
    """Attach only exact original observations; never overwrite a selected value."""
    evidence = supplement['evidence']
    for name in NAMES:
        for fact in records.get(name, {}).values():
            if not fact or fact['accession'] != evidence['accession'] or fact['filed'] != evidence['filed']:
                continue
            proof = supplement['provenance'].get((fact['tag'], fact['unit'], fact['start'], fact['end']), {})
            precision = proof.get('reported_precision')
            if (precision and precision['value'] == fact['value']
                    and fact.get('source_cik', evidence['cik']) == evidence['cik']
                    and proof.get('source_cik') == evidence['cik']
                    and proof.get('source_sha256') == evidence.get('sha256')):
                fact['reported_precision'] = {**precision, 'source_url': proof['source_url'],
                    'source_sha256': proof['source_sha256'], 'source_cik': proof['source_cik'],
                    'accession': fact['accession'], 'filed': fact['filed'], 'tag': fact['tag'],
                    'start': fact['start'], 'end': fact['end'], 'unit': fact['unit'],
                    'context_id': proof['context_id']}
    return records


def reconcile(instant):
    from app.services.sec_leverage import _compatible, _finite
    points = [instant.get(name) for name in NAMES]
    if not all(points) or not _compatible(points):
        return {'status': 'incomplete_or_incompatible', 'canonical_selection': False}
    if any(not _finite(p['value']) or p['value'] < 0 for p in points):
        return {'status': 'invalid_input', 'canonical_selection': False}
    proof = {'methodology': VERSION, 'status': 'original_precision_unavailable',
             'canonical_selection': False, 'components': list(NAMES), 'inputs': points}
    bounds = []
    identities = set()
    for point in points:
        p = point.get('reported_precision', {})
        if (p.get('value') != point['value'] or p.get('derived') or
                any(p.get(k) != point.get(k) for k in ('tag','accession','filed','start','end','unit'))):
            return proof
        d = decimals(p.get('decimals'))
        sha = p.get('source_sha256', '')
        if (d is None or not isinstance(sha,str) or not re.fullmatch(r'[0-9a-f]{64}', sha)
                or not p.get('source_cik') or not p.get('context_id')
                or point.get('source_cik',p['source_cik']) != p['source_cik']):
            return proof
        cik = p['source_cik']
        if not isinstance(cik,str) or not re.fullmatch(r'[0-9]{10}',cik):
            return proof
        origin = f'https://www.sec.gov/Archives/edgar/data/{int(cik)}/{point["accession"].replace("-","")}/'
        url = p.get('source_url')
        if not isinstance(url,str) or not url.startswith(origin) or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,180}\.html?',url[len(origin):]):
            return proof
        identities.add((sha,cik,url))
        with localcontext() as ctx:
            ctx.prec = 64
            value = Decimal(str(point['value']))
            if abs(value) > Decimal('1e16'):
                return proof
            quantum = Decimal(0) if d == 'INF' else Decimal(10)**(-d)
            if quantum and value % quantum != 0:
                return {**proof, 'status': 'digits_exceed_reported_precision'}
            error = quantum/2
            bounds.append((value-error,value+error))
    if len(identities) != 1:
        return {**proof, 'status': 'incompatible_originals'}
    total, current, noncurrent = bounds
    with localcontext() as ctx:
        ctx.prec = 64
        summed = (current[0]+noncurrent[0],current[1]+noncurrent[1])
    # Require containment, not mere overlap. This does not establish rounding
    # mode or disjoint lease components, and cannot authorize an added lease.
    contains = total[0] <= summed[0] and summed[1] <= total[1]
    return {**proof, 'status': 'contained_under_round_to_nearest' if contains else 'not_contained',
        'reported_total_interval': [str(x) for x in total],
        'maturity_sum_interval': [str(x) for x in summed],
        'rounding_assumption': 'round_to_nearest',
        'limitation': 'Precision evidence only; rounding/truncation mode and accounting meaning require review. No overlap guard is relaxed.'}
