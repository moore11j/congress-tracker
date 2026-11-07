"""Complete abbreviated-date originals, parent ownership and strict basis guards."""
from copy import deepcopy
from datetime import date, datetime
from pathlib import Path
from types import SimpleNamespace
import calendar
import gzip
import hashlib
import json
import socket

import pytest
from lxml import html
from app.services import sec_tabular_financials as tables, sec_tabular_reporting as reporting
from app.services import sec_comparative_basis as basis, sec_financial_statements as panels
from app.services import sec_fundamentals as core
from test_sec_financial_discovery_store import database

ROOT = Path(__file__).parent / 'fixtures/sec_statement_dates'
META = json.loads((ROOT/'sources.json').read_text())
NOW = datetime.fromisoformat(META['observed_at'])


@pytest.fixture(autouse=True)
def offline(monkeypatch):
    monkeypatch.setattr(socket.socket, 'connect', lambda *a, **k: pytest.fail('Offline replay'))


def source(url):
    item = META['sources'][url]
    raw = gzip.decompress((ROOT/item['file']).read_bytes())
    assert len(raw) == item['bytes'] and hashlib.sha256(raw).hexdigest() == item['sha256']
    return raw


def originals(index=0):
    candidate = META['candidates'][index]
    url = next(u for u in META['sources'] if candidate['accession'].replace('-', '') in u and 'ex99-1' in u)
    args = dict(source_url=url, cover_url=candidate['cover_url'],
                company_raw=source(META['company_url']), cover_raw=source(candidate['cover_url']),
                symbol='COE', cik=META['cik'], available_by=date(2026, 11, 6))
    return source(url), args


def inputs():
    raw, args = originals()
    tree = html.fromstring(raw)
    for node in tree.xpath('//script|//style|//template|//blockquote'):
        node.drop_tree()
    return tables.visible_text(tree), tables.extract(raw, **args, _parent_deficit=True), args


class Client:
    def __init__(self): self.calls = []
    def get(self, url): self.calls.append(url); return source(url)


def supplement():
    _, args = originals()
    return reporting.acquire(args['company_raw'], symbol='COE', cik=args['cik'],
        available_by=args['available_by'], client=Client(), basis_facts_raw=source(META['facts_url']),
        candidate_accessions=tuple(c['accession'] for c in META['candidates']))


def test_complete_original_parent_deficit_is_not_consolidated_equity():
    raw, args = originals()
    ordinary = tables.extract(raw, **args)
    assert len(ordinary['facts']) == 28
    assert not any(f['metric']=='parent_equity' for f in ordinary['facts'])
    observation = tables.extract(raw, **args, _parent_deficit=True)
    assert not observation['holds'] and not observation['conflicts']
    assert len(observation['tables']) == 2 and len(observation['facts']) == 30
    parent = [f for f in observation['facts'] if f['metric'] == 'parent_equity']
    assert [(f['end'], f['value']) for f in parent] == [('2025-12-31', -31357000), ('2026-06-30', -36942000)]
    assert not any(f['metric'] == 'total_equity' for f in observation['facts'])
    normalized = reporting.normalize(raw, **args, basis_facts_raw=source(META['facts_url']))
    assert normalized == reporting.normalize(raw, **args, basis_facts_raw=source(META['facts_url']))
    proof = normalized['evidence']['comparative_basis']
    assert proof['anchors'][-1] == dict(metric='parent_equity', tag='StockholdersEquity', value='-31357000')
    assert normalized['evidence']['current_period'] == '2026-06-30'
    assert not normalized['records']['equity'] and not normalized['records']['consolidated_equity']
    assert not normalized['records']['net_income']
    assert reporting.cached_valid(normalized['evidence'], symbol='COE', cik=args['cik'])


def test_comparative_extension_cannot_change_existing_observations(monkeypatch):
    raw, args = originals()
    original = tables.extract
    def changed(*a, **kw):
        result = original(*a, **kw)
        if kw.get('_parent_deficit'):
            next(f for f in result['facts'] if f['metric']=='current_assets')['value'] += 1
        return result
    monkeypatch.setattr(tables, 'extract', changed)
    with pytest.raises(reporting.TabularReportingError, match='accounting_basis_unproved'):
        reporting.normalize(raw, **args, basis_facts_raw=source(META['facts_url']))


def test_complete_consumer_output_repeat_and_persisted_cache():
    _, args = originals()
    facts = source(META['facts_url'])
    result = supplement()
    assert result == supplement()
    kwargs = dict(symbol='COE', cik=args['cik'], observed_at=NOW, filing_supplement=result)
    values = core.project_fundamentals(facts, args['company_raw'], **kwargs)
    assert values == core.project_fundamentals(facts, args['company_raw'], **kwargs)
    assert values['current_ratio'] == 64523000/104686000
    assert not {'roe', 'debt_to_equity', 'net_debt_to_ebitda', 'ev_to_ebitda'} & values.keys()
    panel = panels.project(facts, args['company_raw'], **kwargs)
    assert panel == panels.project(facts, args['company_raw'], **kwargs)
    assert panel['status'] == 'partial'
    assert panel['summary']['currentRatio'] == values['current_ratio']
    assert all(r['netIncome'] is None and r['freeCashFlow'] is None for r in panel['quarterly'])
    cached = SimpleNamespace(source='sec_edgar', fetched_at=NOW, payload_json=json.dumps(panel))
    assert panels.cached_row_payload(cached, 'COE', now=NOW) == panel
    changed = deepcopy(result)
    changed['table_original'] = changed['table_original'].replace(b'64,523', b'64,524')
    assert changed['table_original'] != result['table_original']
    with pytest.raises(ValueError):
        panels.project(facts, args['company_raw'], **{**kwargs, 'filing_supplement': changed})


@pytest.mark.parametrize('defect', ['parent_value', 'consolidated_substitution', 'missing_parent',
                                  'currency', 'annual_value', 'annual_identity', 'expired'])
def test_comparative_guards_reject_changed_inputs(defect):
    text, observation, args = inputs()
    facts = json.loads(source(META['facts_url']))
    if defect in {'parent_value', 'consolidated_substitution'}:
        point = next(f for f in observation['facts'] if f['metric']=='parent_equity' and f['end']=='2025-12-31')
        point['value'] = -31253000 if defect=='consolidated_substitution' else -31357001
    if defect=='missing_parent':
        observation['facts'] = [f for f in observation['facts'] if f['metric']!='parent_equity']
    if defect=='currency': observation['facts'][0]['currency']='CAD'
    if defect=='annual_value':
        for point in facts['facts']['us-gaap']['StockholdersEquity']['units']['USD']:
            if point.get('accn')=='0001104659-26-047273': point['val'] += 1
    if defect=='annual_identity': facts['cik']=1
    if defect=='expired': args['available_by']=date(2027, 1, 1)
    with pytest.raises(basis.ComparativeBasisError):
        basis.prove(text, observation, company_raw=args['company_raw'],
            facts_raw=json.dumps(facts).encode(), available_by=args['available_by'])


def test_old_quarter_and_absent_basis_remain_held():
    raw, args = originals(1)
    assert len(tables.extract(raw, **args, _parent_deficit=True)['facts']) == 22
    with pytest.raises(reporting.TabularReportingError, match='accounting_basis_unproved'):
        reporting.normalize(raw, **args, basis_facts_raw=source(META['facts_url']))
    raw, args = originals()
    with pytest.raises(reporting.TabularReportingError, match='accounting_basis_unproved'):
        reporting.normalize(raw, **args)


def test_complete_retained_directory_progress_restore_and_repeat(database, monkeypatch):
    from app.services import sec_financial_discovery_store as store, sec_financial_discovery_reader as reader
    _, args = originals()
    facts = source(META['facts_url'])
    kw = dict(symbol='COE', cik=args['cik'], available_by=args['available_by'], observed_at=NOW)
    client = Client()
    for _ in range(8):
        before = len(client.calls)
        result = store.progress(args['company_raw'], facts, client=client, **kw)
        assert len(client.calls)-before <= 6
        if result['phase']=='complete': break
    assert result['status']=='ready_for_original_revalidation'
    checkpoint = store.read('COE', args['cik'])
    restored = reader.restore(args['company_raw'], facts, checkpoint=checkpoint, client=Client(), **kw)
    assert restored == reader.restore(args['company_raw'], facts, checkpoint=checkpoint, client=Client(), **kw)
    actual = core.project_fundamentals(facts, args['company_raw'], symbol='COE', cik=args['cik'],
                                    observed_at=NOW, filing_supplement=restored)
    assert actual['current_ratio'] == 64523000/104686000
    before = len(client.calls)
    repeated = store.progress(args['company_raw'], facts, client=client, **kw)
    assert repeated['phase']=='complete' and len(client.calls)==before
    monkeypatch.delenv('SEC_FINANCIAL_DISCOVERY_READ_ENABLED', raising=False)
    assert reader.prepared_choice(args['company_raw'], facts, client=Client(), **kw) is None


@pytest.mark.parametrize('month', range(1, 13))
def test_explicit_abbreviated_dates_keep_full_month_meaning(month):
    end = date(2026, month, calendar.monthrange(2026, month)[1])
    for name in {calendar.month_name[month], calendar.month_abbr[month], calendar.month_abbr[month]+'.'}:
        assert tables.period_for(f'As of {name} {end.day}, 2026', 'balance', date(2026,12,31)) == (None, end, None)


@pytest.mark.parametrize('header,reason', [
    ('As of JunX 30, 2026', 'column_date_ambiguous'),
    ('As of 06/30/2026', 'column_date_ambiguous'),
    ('As of Jun. 30, 2026 and Dec. 31, 2025', 'column_date_ambiguous'),
    ('As of Feb. 30, 2026', 'column_date_invalid'),
    ('As of Dec. 31, 2027', 'column_period_after_filing'),
])
def test_abbreviations_do_not_guess_dates(header, reason):
    with pytest.raises(tables.TabularFinancialError, match=reason):
        tables.period_for(header, 'balance', date(2026,12,31))


@pytest.mark.parametrize('text', [
    'Financial information is not prepared in accordance with GAAP.',
    'Projected financial information prepared in accordance with GAAP.',
    'Non-GAAP financial information prepared in accordance with GAAP.',
    'Financial statements prepared in accordance with GAAP in Canada.',
    'Financial information will be prepared in accordance with GAAP.',
    'Hypothetical financial statements prepared in accordance with GAAP.',
])
def test_new_subjects_do_not_admit_non_declarations(text):
    assert basis.definition(text) is None
