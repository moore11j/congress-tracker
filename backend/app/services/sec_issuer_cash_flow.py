"""Reviewed issuer cash-flow components absent from SEC companyfacts.

NVIDIA's extension is read from its own filed, entity-wide inline XBRL.
No inferred zero payments, broad extension matching or provider fallback.
"""
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
import hashlib
import json
import re
from lxml import html

from app.clients.direct_sources import DirectSourceError

CIK = '0001045810'
PRINCIPAL = 'nvda:PaymentsForFinancedPropertyPlantAndEquipmentAndIntangibleAssetsFinancingActivities'
PRODUCTIVE = 'PaymentsToAcquireProductiveAssets'
BASIS = 'Operating cash flow less purchases and principal payments for property, equipment and intangible assets'
DEFINITION_URL = 'https://investor.nvidia.com/news/press-release-details/2026/NVIDIA-Announces-Financial-Results-for-Second-Quarter-Fiscal-2027/'
VERSION = 'reviewed_issuer_cash_flow_v1'


class CashFlowEvidenceError(ValueError):
    pass


def _nodes(root, local):
    return [n for n in root.iter() if isinstance(n.tag, str) and n.tag.lower().split(':')[-1] == local]


def parse_principal(raw, *, accession, filed, report_date):
    """Only unsegmented USD facts with verified issuer and filing-period identity."""
    if not raw or len(raw) > 4_000_000:
        raise CashFlowEvidenceError('filing_size')
    tree = html.fromstring(raw)
    if not re.fullmatch(r'http://www\.nvidia\.com/[0-9]{8}', tree.get('xmlns:nvda', '')):
        raise CashFlowEvidenceError('issuer_namespace')
    identities = {n.text_content().strip().zfill(10) for n in tree.iter()
                  if n.get('name') == 'dei:EntityCentralIndexKey'}
    ends = set()
    for n in tree.iter():
        if n.get('name') == 'dei:DocumentPeriodEndDate':
            if n.get('format') != 'ixt:date-monthname-day-year-en':
                raise CashFlowEvidenceError('unsupported_filing_date')
            ends.add(str(datetime.strptime(' '.join(n.text_content().split()), '%B %d, %Y').date()))
    if identities != {CIK} or ends != {report_date}:
        raise CashFlowEvidenceError('filing_identity')
    contexts, units = {}, set()
    for n in _nodes(tree, 'context'):
        key = n.get('id')
        if not key or key in contexts:
            raise CashFlowEvidenceError('duplicate_context')
        contexts[key] = None
        if _nodes(n, 'segment') or _nodes(n, 'scenario'):
            continue
        ids, starts, ends = _nodes(n, 'identifier'), _nodes(n, 'startdate'), _nodes(n, 'enddate')
        if len(ids) != 1 or ids[0].get('scheme') != 'http://www.sec.gov/CIK' or ids[0].text_content().strip().zfill(10) != CIK:
            continue
        if len(starts) == len(ends) == 1:
            start, end = date.fromisoformat(starts[0].text_content()), date.fromisoformat(ends[0].text_content())
            if 1 <= (end-start).days+1 <= 380 and str(end) <= report_date:
                contexts[key] = (start, end)
    unit_ids = set()
    for n in _nodes(tree, 'unit'):
        if not n.get('id') or n.get('id') in unit_ids:
            raise CashFlowEvidenceError('duplicate_unit')
        unit_ids.add(n.get('id'))
        measures = _nodes(n, 'measure')
        if len(measures) == 1 and measures[0].text_content().strip() == 'iso4217:USD' and not _nodes(n, 'divide'):
            units.add(n.get('id'))
    rows = {}
    for n in tree.iter():
        if n.get('name') != PRINCIPAL:
            continue
        period = contexts.get(n.get('contextref'))
        if not period or n.get('unitref') not in units:
            continue
        text = n.text_content().strip()
        valid_format = n.get('format') == 'ixt:num-dot-decimal' or (n.get('format') is None and ',' not in text)
        if (not valid_format or n.get('sign') or n.get('continuedat') or n.get('xsi:nil')
                or not re.fullmatch(r'(?:[0-9]+|[0-9]{1,3}(?:,[0-9]{3})+)(?:\.[0-9]+)?', text)
                or not re.fullmatch(r'[0-9]', n.get('scale', '0'))):
            raise CashFlowEvidenceError('unsupported_principal_number')
        try:
            value = float(Decimal(text.replace(',', '')) * Decimal(10)**int(n.get('scale', '0')))
        except (InvalidOperation, ValueError, OverflowError) as exc:
            raise CashFlowEvidenceError('invalid_principal_number') from exc
        if value < 0 or value > 1e15:
            raise CashFlowEvidenceError('invalid_principal_value')
        fact = {'value': value, 'tag': PRINCIPAL, 'unit': 'USD', 'start': str(period[0]),
                'end': str(period[1]), 'filed': filed, 'accession': accession, 'fact_id': n.get('id'),
                'context_id': n.get('contextref'), 'filing_sha256': hashlib.sha256(raw).hexdigest()}
        if period in rows and rows[period]['value'] != value:
            raise CashFlowEvidenceError('conflicting_principal_facts')
        rows[period] = fact
    if not rows:
        raise CashFlowEvidenceError('principal_not_reported')
    return rows


def supplement(values, facts_raw, company_raw, *, client):
    """At most three filed originals for the reviewed issuer, before any DB write."""
    from app.services import sec_fundamentals as sec
    evidence = json.loads(values['source_evidence_json'])
    if values.get('symbol') != 'NVDA' or evidence.get('cik') != CIK or not evidence.get('period_end'):
        return values
    result = dict(values)
    # The reviewed company definition includes financing payments; no partial
    # companyfacts result may survive a failed supplemental check.
    for key in ('free_cash_flow', 'fcf_margin', 'fcf_growth'):
        result.pop(key, None)
        evidence['calculations'].pop(key, None)
    review = {'methodology': VERSION, 'definition_url': DEFINITION_URL, 'definition': BASIS,
              'status': 'unavailable', 'filings': []}
    evidence['issuer_cash_flow'] = review
    try:
        payload, company = json.loads(facts_raw), json.loads(company_raw)
        if str(company.get('cik', '')).zfill(10) != CIK or str(payload.get('cik', '')).zfill(10) != CIK:
            raise CashFlowEvidenceError('company_identity')
        available = date.fromisoformat(evidence['available_by'])
        records = sec._records(payload, 'capex', available, concepts=(PRODUCTIVE,))
        current, prior = evidence['current'], evidence['prior']
        cash = current.get('operating_cash_flow')
        capex = sec._ttm(records, date.fromisoformat(evidence['period_end']))
        if not cash or not capex or cash['start'] != capex['start'] or cash['start'] != evidence['period_start']:
            raise CashFlowEvidenceError('current_period_incomplete')
        old_cash = prior.get('operating_cash_flow')
        old_capex = sec._ttm(records, date.fromisoformat(old_cash['end'])) if old_cash else None
        if old_cash and (not old_capex or old_capex['start'] != old_cash['start']):
            old_cash = old_capex = None
        periods = [('current', cash, capex)] + ([('prior', old_cash, old_capex)] if old_cash else [])
        inputs = [i for _, flow, investment in periods for i in flow['inputs'] + investment['inputs']]
        accessions = sorted({i['accession'] for i in inputs})
        if len(accessions) > 3:
            raise CashFlowEvidenceError('filing_budget_exceeded')
        recent = company.get('filings', {}).get('recent', {})
        originals = {}
        for accession in accessions:
            indices = [i for i, a in enumerate(recent.get('accessionNumber', [])) if a == accession]
            if len(indices) != 1 or not re.fullmatch(r'0001045810-[0-9]{2}-[0-9]{6}', accession):
                raise CashFlowEvidenceError('filing_not_in_directory')
            i = indices[0]
            form, filed, end, filename = [recent[k][i] for k in ('form', 'filingDate', 'reportDate', 'primaryDocument')]
            if (form not in {'10-Q','10-K','10-Q/A','10-K/A'} or filed > str(available)
                    or not re.fullmatch(r'nvda-[0-9]{8}\.htm', filename)):
                raise CashFlowEvidenceError('filing_directory_identity')
            if any(f['filed'] != filed for f in inputs if f['accession'] == accession):
                raise CashFlowEvidenceError('filing_date_mismatch')
            url = f'https://www.sec.gov/Archives/edgar/data/1045810/{accession.replace("-", "")}/{filename}'
            raw = client.get(url)
            originals[accession] = parse_principal(raw, accession=accession, filed=filed, report_date=end)
            review['filings'].append({'url': url, 'sha256': hashlib.sha256(raw).hexdigest(), 'accession': accession, 'filed': filed})
        calculated = {}
        for kind, flow, investment in periods:
            principals = []
            for f in flow['inputs']:
                key = (date.fromisoformat(f['start']), date.fromisoformat(f['end']))
                row = originals[f['accession']].get(key)
                if row is None:
                    raise CashFlowEvidenceError('principal_period_missing')
                principals.append(row)
            if len(principals) not in (1, 3) or investment['value'] < 0:
                raise CashFlowEvidenceError('unsupported_trailing_inputs')
            principal = principals[0]['value'] if len(principals) == 1 else principals[0]['value']+principals[1]['value']-principals[2]['value']
            if principal < 0:
                raise CashFlowEvidenceError('negative_trailing_principal')
            calculated[kind] = flow['value']-investment['value']-principal
            review[kind] = {'operating_cash_flow': flow, 'productive_asset_purchases': investment,
                            'principal_payments': {'value': principal, 'inputs': principals}, 'free_cash_flow': calculated[kind]}
        result['free_cash_flow'] = calculated['current']
        revenue = current['revenue']['value']
        if revenue > 0:
            result['fcf_margin'] = calculated['current']/revenue*100
        if calculated.get('prior', 0) > 0:
            result['fcf_growth'] = (calculated['current']/calculated['prior']-1)*100
        evidence['calculations']['free_cash_flow'] = 'TTM '+BASIS
        if 'fcf_margin' in result: evidence['calculations']['fcf_margin'] = 'reviewed issuer TTM free cash flow / TTM revenue * 100'
        if 'fcf_growth' in result: evidence['calculations']['fcf_growth'] = '(reviewed issuer TTM free cash flow / prior comparable TTM free cash flow - 1) * 100'
        review['status'] = 'available'
    except (CashFlowEvidenceError, KeyError, IndexError, ValueError) as exc:
        review['reason'] = str(exc)
    result['source_evidence_json'] = json.dumps(evidence, sort_keys=True)
    return result
