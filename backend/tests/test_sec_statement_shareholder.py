from datetime import date
from pathlib import Path
import copy,gzip,hashlib,json
from lxml import html
import pytest
from app.services import sec_statement_shareholder as notes,sec_tabular_financials as tab,sec_tabular_reporting as reporting

ROOT=Path(__file__).parent/'fixtures/sec_statement_layout'
MANIFEST=json.loads((ROOT/'manifest.json').read_bytes());ENTRY=MANIFEST['entry']
def original(url):
    m=MANIFEST['originals'][url];raw=gzip.decompress((ROOT/m['file']).read_bytes())
    assert len(raw)==m['bytes'] and hashlib.sha256(raw).hexdigest()==m['sha256'];return raw
RAW=original(ENTRY['url']);COMPANY=original(ENTRY['company_url'])
ARGS=dict(source_url=ENTRY['url'],cover_url=ENTRY['cover_url'],cover_raw=RAW,company_raw=COMPANY,symbol=ENTRY['symbol'],cik=ENTRY['cik'],available_by=date(2026,11,1))

@pytest.fixture(scope='module')
def observations():return tab.extract(RAW,**ARGS)

def enrich(raw,observations):
    return notes.enrich(raw,observations,issuer=json.loads(COMPANY)['name'],filed=date(2026,8,11),currency='BRL')

def test_complete_original_parent_and_diluted_eps_are_distinct_source_values(observations):
    result=enrich(RAW,observations)
    expected={}
    for start,end,income,eps in [('2026-04-01','2026-06-30',549076000,1.9648),('2025-04-01','2025-06-30',536759000,1.7851),('2026-01-01','2026-06-30',1094601000,3.8932),('2025-01-01','2025-06-30',1061851000,3.5032)]:
        expected['parent_income',start,end]=income;expected['eps',start,end]=eps
    assert {(f['metric'],f['start'],f['end']):f['value'] for f in result['facts']}==expected
    assert len(result['facts'])==8 and result['context']['number']=='24'
    assert {f['occurrences'][0]['scale'] for f in result['facts'] if f['metric']=='eps'}=={1}
    assert {f['occurrences'][0]['scale'] for f in result['facts'] if f['metric']=='parent_income'}=={1000}
    assert result==enrich(RAW,observations)

@pytest.mark.parametrize('mutation,reason',[
    ('owner','parent_holder_identity_unproved'),('conditional','parent_holder_identity_unproved'),
    ('reference','primary_note_reference_mismatch'),('basic','note_primary_eps_mismatch'),
    ('period','note_primary_period_mismatch'),('scale','note_scale_conflict'),
    ('duplicate','competing_parent_notes')])
def test_note_cannot_reassign_owner_period_units_or_primary_reference(observations,mutation,reason):
    tree=html.fromstring(RAW);tables=tree.xpath('//table')
    if mutation in {'owner','conditional'}:
        p=next(n for n in tables[102].itersiblings(preceding=True) if tab.visible_text(n).startswith('Basic earnings per share is calculated'))
        text=tab.visible_text(p)
        for c in list(p):p.remove(c)
        p.text=text.replace('equity holders of PagSeguro Digital','equity holders of Other Company') if mutation=='owner' else 'If '+text
    elif mutation=='reference':
        row=tables[10].xpath('.//tr')[23]
        cell=row.xpath('./td|./th')[1]
        for c in list(cell):cell.remove(c)
        cell.text='25'
    elif mutation in {'basic','period'}:
        row=tables[102].xpath('.//tr')[-1 if mutation=='basic' else 1]
        cell=row.xpath('./td|./th')[1]
        for c in list(cell):cell.remove(c)
        cell.text='9.9999' if mutation=='basic' else '2024'
    elif mutation=='scale':
        raw=html.tostring(tables[100]).replace(b'thousands',b'millions')
        tables[100].getparent().replace(tables[100],html.fromstring(raw))
    elif mutation=='duplicate':tables[102].addnext(copy.deepcopy(tables[102]))
    with pytest.raises(ValueError,match=reason):enrich(html.tostring(tree),observations)

def test_parent_income_is_not_required_to_equal_consolidated_income(observations):
    tree=html.fromstring(RAW);cell=tree.xpath('//table')[102].xpath('.//tr')[2].xpath('./td|./th')[1]
    for c in list(cell):cell.remove(c)
    cell.text='500,000'
    result=enrich(html.tostring(tree),observations)
    point=next(f for f in result['facts'] if f['metric']=='parent_income' and f['start']=='2026-04-01')
    assert point['value']==500000000
    assert next(f for f in observations['facts'] if f['metric']=='consolidated_income' and f['start']=='2026-04-01')['value']==549076000

def test_non_layout_statements_keep_existing_observations(observations):
    prior=copy.deepcopy(observations);prior['source'].pop('statement_layout_policy')
    assert enrich(RAW,prior) is None

def test_normalization_keeps_reported_eps_unit_and_retains_no_share_count_inference():
    norm=reporting.normalize(RAW,**ARGS)
    assert len(norm['records']['net_income'])==len(norm['records']['eps'])==4
    assert {r['unit'] for r in norm['records']['eps'].values()}=={'BRL/shares'}
    assert not norm['records']['revenue']
    assert not {'shares','weighted_shares','market_cap'} & norm['records'].keys()
    changed=copy.deepcopy(norm['evidence']);changed['shareholder_note']['facts'][0]['value']+=1
    supplement=dict(table_original=RAW,table_args=ARGS,evidence={'table_reporting':changed})
    with pytest.raises(reporting.TabularReportingError,match='table_original_evidence_changed'):
        reporting.validate(supplement,company_raw=COMPANY,symbol=ENTRY['symbol'],cik=ENTRY['cik'],available_by=ARGS['available_by'])


def test_partial_panel_preserves_cash_only_periods_and_does_not_derive_eps(monkeypatch):
    from datetime import datetime,timezone
    norm=reporting.normalize(RAW,**ARGS)
    supplement=dict(table_original=RAW,table_args=ARGS,evidence={'table_reporting':norm['evidence']})
    facts=json.dumps({'cik':int(ENTRY['cik'])}).encode()
    kw=dict(symbol=ENTRY['symbol'],cik=ENTRY['cik'],observed_at=datetime(2026,11,2,2,tzinfo=timezone.utc),supplement=supplement)
    # Reported YTD EPS cannot reconstruct a quarter without share-weight data.
    norm['records']['eps'].pop((date(2026,4,1),date(2026,6,30)))
    monkeypatch.setattr(reporting,'validate',lambda *a,**k:norm)
    panel=reporting.panel(facts,COMPANY,**kw)
    assert panel['quarterly'][-1]['eps'] is None
    assert panel['semiannual'][-1]['eps']==3.8932
    assert panel['quarterly'][-1]['netIncome']==549076000
    norm['records']['net_income']={};norm['records']['eps']={}
    panel=reporting.panel(facts,COMPANY,**kw)
    assert not panel['quarterly'] and len(panel['semiannual'])==2
    assert panel['semiannual'][-1]['operatingCashFlow']==1938645000
    assert panel['status']=='partial' and panel['sections']['cashFlow']=='partial'
    assert panel['sections']['income']!='partial'
    assert all(r['netIncome'] is None and r['revenue'] is None and r['eps'] is None for r in panel['semiannual'])
