from copy import deepcopy
from datetime import date, datetime, timezone
from functools import lru_cache
from pathlib import Path
import gzip, hashlib, json, socket
from unittest.mock import patch
from lxml import html
import pytest

from app.services import sec_rental_revenue as rental, sec_fundamentals as sec, sec_inline_financials as inline
from app.services import sec_financial_statements as statements, sec_tagged_foreign

ROOT = Path(__file__).parent/'fixtures/sec_rental_revenue'
NOW = datetime(2026, 11, 7, 1, tzinfo=timezone.utc)
AVAILABLE = date(2026, 11, 6)
CIK = '0000906107'


@lru_cache(maxsize=5)
def raw(name):
    proof = json.loads((ROOT/'manifest.json').read_bytes())[name]
    value = gzip.decompress((ROOT/(name+'.gz')).read_bytes())
    assert len(value) == proof['bytes'] and hashlib.sha256(value).hexdigest() == proof['sha256']
    return value


def inputs():
    return json.loads(raw('VMRK-facts.json')), json.loads(raw('VMRK-company.json'))


class Client:
    issuer_document_urls = frozenset()
    def __init__(self):
        self.calls = []
        self.names = {p['url']: name for name, p in json.loads((ROOT/'manifest.json').read_bytes()).items()}
    def get(self, url):
        self.calls.append(url)
        assert url in self.names, 'Uncaptured original'
        return raw(self.names[url])


@lru_cache(maxsize=1)
def acquired():
    client = Client()
    with patch.object(socket.socket, 'connect', side_effect=AssertionError('Offline evidence only')):
        result = inline.acquire(raw('VMRK-facts.json'), raw('VMRK-company.json'), symbol='VMRK', cik=CIK,
                                observed_at=NOW, client=client, include_debt_precision=True)
    assert len(client.calls) == len(set(client.calls)) == 3
    return result


def project(supplement):
    return sec.project_fundamentals(raw('VMRK-facts.json'), raw('VMRK-company.json'), symbol='VMRK',
                                   cik=CIK, observed_at=NOW, filing_supplement=supplement)


def test_complete_originals_recover_revenue_without_splicing_or_overclaiming_classification():
    source = acquired(); after = project(source); without = deepcopy(source)
    without['evidence'].pop('rental_revenue_selection')
    before = project(without)
    assert before.get('revenue_growth') is None
    assert after['roe'] == before['roe']
    evidence = json.loads(after['source_evidence_json'])
    assert evidence['current']['revenue']['value'] == 3_129_217_000
    assert after['revenue_growth'] == pytest.approx((3_129_217_000/3_044_764_000-1)*100)
    assert after.get('net_debt_to_ebitda') is None and after.get('operating_margin') is None
    assert all(p['tag'] == rental.CONCEPT for p in evidence['current']['revenue']['inputs'])
    assert rental.cached_valid(source['evidence']['rental_revenue_selection'], cik=CIK)
    assert sec_tagged_foreign.cached_currency_valid(evidence)
    assert project(source) == after


def test_original_verified_quarters_and_old_reported_history_are_preserved():
    source = acquired(); without = deepcopy(source); without['evidence'].pop('rental_revenue_selection')
    kwargs = dict(symbol='VMRK', cik=CIK, observed_at=NOW)
    before = statements.project(raw('VMRK-facts.json'), raw('VMRK-company.json'), filing_supplement=without, **kwargs)
    after = statements.project(raw('VMRK-facts.json'), raw('VMRK-company.json'), filing_supplement=source, **kwargs)
    for kind in ('annual', 'quarterly'):
        rows = {p['date']: p for p in after[kind]}
        for point in before[kind]:
            if point['date'] in rows:
                for key, value in point.items():
                    if value is not None: assert rows[point['date']][key] == value
    assert any(p.get('revenue') == 785_049_000 for p in after['quarterly'])


@pytest.mark.parametrize('defect', ['value', 'owner', 'unit', 'hash', 'url', 'component', 'heading', 'splice', 'missing_period'])
def test_persisted_proof_corruption_is_held(defect):
    proof = deepcopy(acquired()['evidence']['rental_revenue_selection']); point = proof['rows'][0]
    original = point['original_statement']
    if defect == 'value': point['value'] += 1
    elif defect == 'owner': original['source_cik'] = '0000000001'
    elif defect == 'unit': point['unit'] = 'EUR'
    elif defect == 'hash': original['source_sha256'] = 'bad'
    elif defect == 'url': original['source_url'] = 'https://example.com/statement'
    elif defect == 'component': original['revenue_section'].append('management fees')
    elif defect == 'heading': original['statement_heading'] = ['Adjusted consolidated statements of income']
    elif defect == 'splice': proof['basis_spliced'] = True
    else:
        wanted = rental.plan(*inputs(), cik=CIK, available_by=AVAILABLE)
        key = next(iter(wanted['needed']))
        proof['rows'] = [p for p in proof['rows'] if (p['start'], p['end']) != key]
    with pytest.raises(ValueError):
        rental.apply({'revenue': {}}, *inputs(), cik=CIK, available_by=AVAILABLE,
                     supplement={'evidence': {'rental_revenue_selection': proof}})


@pytest.mark.parametrize('defect', ['component_label', 'extra_revenue', 'adjusted_title', 'wrong_entity'])
def test_complete_current_statement_rejects_components_adjustments_and_other_entities(defect):
    facts, company = inputs(); wanted = rental.plan(facts, company, cik=CIK, available_by=AVAILABLE)
    filing = next(f for f in wanted['filings'].values() if f['report_date'] == '2026-06-30')
    tree = html.fromstring(raw('VMRK-current.htm')); changes = 0
    for node in list(tree.iter()):
        if not isinstance(node.tag, str): continue
        if defect == 'wrong_entity' and node.tag.lower() == 'xbrli:identifier' and node.text_content().strip().zfill(10) == CIK:
            node.text = '0000000001'; changes += 1
        if node.text and defect == 'adjusted_title' and 'CONSOLIDATED STATEMENTS OF' in node.text:
            node.text = node.text.replace('CONSOLIDATED STATEMENTS', 'ADJUSTED CONSOLIDATED STATEMENTS'); changes += 1
        if node.text and defect == 'component_label' and node.text.strip() == 'Rental income':
            node.text = 'Rental income component'; changes += 1
        if defect == 'extra_revenue' and node.tag.lower() == 'tr' and ' '.join(node.text_content().split()).strip().lower() == 'expenses':
            node.addprevious(html.fromstring('<tr><td>Management fees</td><td>100</td></tr>')); changes += 1
    assert changes
    changed = html.tostring(tree)
    try:
        result = rental.statement_rows(changed, filing)
    except ValueError:
        result = {}
    assert result == {}


def test_current_supported_total_keeps_priority_and_conflicting_partial_total_is_held():
    facts, company = inputs()
    rental_rows = facts['facts']['us-gaap'][rental.CONCEPT]['units']['USD']
    facts['facts']['us-gaap']['Revenues'] = {'units': {'USD': deepcopy(rental_rows)}}
    assert rental.plan(facts, company, cik=CIK, available_by=AVAILABLE) is None
    wanted = rental.plan(*inputs(), cik=CIK, available_by=AVAILABLE)
    point = next(iter(wanted['needed'].values()))
    conflicting = dict(start=point['start'], end=point['end'], val=point['value']+1,
                       accn=point['accession'], filed=point['filed'], form='10-K')
    facts['facts']['us-gaap']['Revenues']['units']['USD'] = [conflicting]
    with pytest.raises(ValueError, match='total_conflict'):
        rental.plan(facts, company, cik=CIK, available_by=AVAILABLE)


def test_future_proof_cannot_be_admitted_before_its_filing_day():
    with pytest.raises(ValueError):
        rental.apply({'revenue': {}}, *inputs(), cik=CIK, available_by=date(2026, 7, 29), supplement=acquired())
