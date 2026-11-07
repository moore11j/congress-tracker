"""Whole-source qualification, consumer replay and adversarial caption boundaries."""
from copy import deepcopy
from datetime import date,datetime,timezone
from pathlib import Path
import gzip,hashlib,json,socket
from types import SimpleNamespace
from lxml import html
import pytest
from app.services import sec_tabular_financials as tab,sec_tabular_reporting as reporting
from app.services import sec_statement_font_context as font,sec_statement_declarations as declarations

ROOT=Path(__file__).parent/'fixtures/sec_font_statements'
MANIFEST=json.loads((ROOT/'manifest.json').read_bytes())
ORIGINALS={}
for item in MANIFEST['sources']:
    raw=gzip.decompress((ROOT/item['file']).read_bytes())
    assert len(raw)==item['bytes'] and hashlib.sha256(raw).hexdigest()==item['sha256']
    ORIGINALS[item['url']]=raw
COMPANY=next(v for k,v in ORIGINALS.items() if '/submissions/' in k)
FACTS=next(v for k,v in ORIGINALS.items() if '/companyfacts/' in k)
COVER=MANIFEST['candidate']['cover_url']
SOURCE=next(k for k in ORIGINALS if k.endswith('.htm') and k!=COVER)
RAW=ORIGINALS[SOURCE]
ARGS=dict(source_url=SOURCE,cover_url=COVER,cover_raw=ORIGINALS[COVER],company_raw=COMPANY,
          symbol=MANIFEST['symbol'],cik=MANIFEST['cik'],available_by=date(2026,11,6))


@pytest.fixture(autouse=True)
def offline(monkeypatch):
    monkeypatch.setattr(socket.socket,'connect',lambda *a,**k:pytest.fail('Unexpected network'))


def test_whole_original_native_periods_units_and_exact_repeat():
    result=tab.extract(RAW,**ARGS)
    assert result==tab.extract(RAW,**ARGS)
    assert len(result['facts'])==26 and not result['holds'] and not result['conflicts']
    assert {t['table'] for t in result['tables']}=={1,4}
    assert result['source']['statement_font_policy']==font.POLICY
    assert result['source']['excluded_numbered_note_tables']==[25]
    values={(f['metric'],f['start'],f['end']):f['value'] for f in result['facts']}
    assert values['current_assets',None,'2026-06-30']==569753000
    assert values['current_liabilities',None,'2026-06-30']==50919000
    assert values['total_assets',None,'2026-06-30']==643895000
    assert values['total_liabilities',None,'2026-06-30']==84159000
    assert values['total_equity',None,'2026-06-30']==559736000
    assert values['operating_income','2026-04-01','2026-06-30']==-16842000
    assert values['operating_income','2026-01-01','2026-06-30']==-30491000
    assert values['consolidated_income','2026-04-01','2026-06-30']==-20276000
    assert values['consolidated_income','2026-01-01','2026-06-30']==-31668000
    assert values['revenue','2026-04-01','2026-06-30']==20598000
    assert values['revenue','2025-04-01','2025-06-30']==17842000
    assert values['revenue','2026-01-01','2026-06-30']==40019000
    assert values['gross_profit','2026-04-01','2026-06-30']==4080000
    assert len(result['source']['statement_revenue_reconciliation'][0]['columns'])==4
    assert {f['currency'] for f in result['facts']}=={'USD'}
    assert {f['metric'] for f in result['facts']}.isdisjoint({'operating_cash_flow','parent_income','capex','eps'})
    normalized=reporting.normalize(RAW,**ARGS)
    assert normalized==reporting.normalize(RAW,**ARGS)
    assert normalized['evidence']['accounting_basis']=='IFRS'
    supplement=dict(evidence={'table_reporting':normalized['evidence']},table_original=RAW,table_args=ARGS)
    assert reporting.validate(supplement,company_raw=COMPANY,symbol=ARGS['symbol'],cik=ARGS['cik'],available_by=ARGS['available_by'])==normalized


@pytest.mark.parametrize('old,new',[
    (b'BALLARD POWER SYSTEMS INC.',b'OTHER HOLDINGS INC.'),
    (b'Expressed in thousands of U.S. dollars',b'Expressed in thousands of dollars'),
    (b'Expressed in thousands of U.S. dollars',b'Expressed in millions and thousands of U.S. dollars'),
    (b'Loss and Comprehensive Loss',b'Adjusted Loss and Comprehensive Loss'),
])
def test_whole_source_ambiguities_do_not_admit_a_panel(old,new):
    assert old in RAW
    with pytest.raises(reporting.TabularReportingError):
        reporting.normalize(RAW.replace(old,new),**ARGS)


def test_missing_numbered_note_context_keeps_legacy_hold():
    tree=html.fromstring(RAW)
    nodes=[n for n in tree.xpath('//div') if tab.visible_text(n)=='12. Trade and other payables:']
    assert len(nodes)==1
    nodes[0].clear();nodes[0].text='Trade and other payables:'
    with pytest.raises(reporting.TabularReportingError):
        reporting.normalize(html.tostring(tree),**ARGS)


@pytest.mark.parametrize('old,new',[(b'20,598',b'20,599'),(b'4,080',b'4.080'),
                                  (b'Product and service revenues',b'Other product and service revenues')])
def test_revenue_requires_exact_total_and_gross_profit_reconciliation(old,new):
    assert old in RAW
    changed=RAW.replace(old,new)
    result=tab.extract(changed,**ARGS)
    assert not any(f['metric']=='revenue' for f in result['facts'])


def test_adjusted_loss_is_not_admitted_as_reported_income():
    changed=RAW.replace(b'Net loss for the period',b'Adjusted net loss for the period')
    assert changed!=RAW
    result=tab.extract(changed,**ARGS)
    assert any(f['metric']=='revenue' for f in result['facts'])
    assert not any(f['metric']=='consolidated_income' for f in result['facts'])


def caption(owner='Example Inc.',heading='Condensed Consolidated Interim Statements of Financial Position',
            units='(Expressed in thousands of U.S. dollars)',between='',extra=''):
    return html.fromstring(f'<div><div>{owner}</div><div>{heading}</div>{between}<div>'
                           f'<font>Unaudited</font><font></font><font>{units}</font>{extra}'
                           '<table id="values"><tr><td>Total assets</td><td>10</td></tr></table></div></div>')


@pytest.mark.parametrize('fault',['owner','intervening','extra_currency','quote','unknown_heading','inferred_units'])
def test_adjacent_context_never_crosses_ownership_or_currency_boundaries(fault):
    kw={}
    if fault=='owner':kw['owner']='Other Inc.'
    if fault=='intervening':kw['between']='<div>Different entity</div>'
    if fault=='extra_currency':kw['extra']='<font>Amounts in EUR</font>'
    if fault=='unknown_heading':kw['heading']='Adjusted Consolidated Interim Statements of Financial Position'
    if fault=='inferred_units':kw['units']='(Expressed in dollars)'
    tree=caption(**kw)
    if fault=='quote':tree=html.fromstring('<blockquote>'+html.tostring(tree,encoding='unicode')+'</blockquote>')
    if fault=='owner':
        with pytest.raises(tab.TabularFinancialError,match='owner_unproved'):font.context(tree.get_element_by_id('values'),'Example Inc.')
    else:assert font.context(tree.get_element_by_id('values'),'Example Inc.') is None


@pytest.mark.parametrize('alias',['IAS','"IAS"',"'IAS'",'“IAS”','‘IAS’'])
def test_explicit_parenthetical_standard_declaration(alias):
    text='These financial statements have been prepared in accordance with International Accounting Standard ('+alias+') 34.'
    assert declarations.declarations(text)
    assert not declarations.declarations(text.replace(') 34',') 1'))
    assert not declarations.declarations(text.replace('been prepared','not been prepared'))


def test_existing_observations_and_conflicts_are_not_reinterpreted(monkeypatch):
    monkeypatch.setattr(tab,'extract',lambda *a,**k:pytest.fail('Existing evidence must not be reparsed'))
    for baseline in ({'facts':[{'value':1}],'conflicts':[]},{'facts':[],'conflicts':[{'value':1}]}):
        assert font.recover(baseline,None,b'') is None


@pytest.fixture(scope='module')
def qualified():
    from app.services import sec_financial_discovery as discovery,sec_financial_discovery_reader as reader
    now=datetime(2026,11,7,12,tzinfo=timezone.utc)
    kw=dict(symbol=ARGS['symbol'],cik=ARGS['cik'],available_by=ARGS['available_by'],observed_at=now)
    state=discovery.inspect(COMPANY,**kw)['state']
    candidate=next(c for c in discovery._inventory(COMPANY,symbol=kw['symbol'],cik=kw['cik'],available_by=kw['available_by']) if c['cover_url']==COVER)
    cover=discovery._observe(ORIGINALS[COVER],candidate,now)
    state['receipts'][discovery._fingerprint(candidate)]=cover
    class Client:
        def __init__(self):self.calls=[]
        def get(self,url):self.calls.append(url);return ORIGINALS[url]
    client=Client()
    result=reader.inspect_candidate(COMPANY,FACTS,checkpoint=state,accession=candidate['accession'],client=client,**kw)
    assert client.calls==[COVER,SOURCE]
    assert result==reader.inspect_candidate(COMPANY,FACTS,checkpoint=state,accession=candidate['accession'],client=Client(),**kw)
    assert result['receipt']['status']=='qualified' and not result['receipt']['financial_publication_eligible']
    return result,cover,kw


def test_prepared_consumers_remain_partial_and_repeat(qualified):
    from app.services import sec_financial_statements as statements,sec_fundamentals as core
    result,cover,kw=qualified
    args=dict(symbol=kw['symbol'],cik=kw['cik'],observed_at=kw['observed_at'],filing_supplement=result['supplement'])
    panel=statements.project(FACTS,COMPANY,**args)
    assert panel==statements.project(FACTS,COMPANY,**args)
    values=core.project_fundamentals(FACTS,COMPANY,**args)
    assert values==core.project_fundamentals(FACTS,COMPANY,**args)
    assert values['current_ratio']==pytest.approx(569753/50919)
    assert panel['status']=='partial' and panel['sections']['health']=='partial'
    assert not panel['annual'] and len(panel['quarterly'])==2
    assert panel['quarterly'][-1]['revenue']==20598000
    assert all(row['netIncome'] is None and row['freeCashFlow'] is None for row in panel['quarterly'])
    assert panel['summary']['currentRatio']==values['current_ratio']
    assert not {'revenue_growth','roe','net_margin','ebitda','free_cash_flow'} & values.keys()
    cached=SimpleNamespace(source='sec_edgar',fetched_at=kw['observed_at'],payload_json=json.dumps(panel))
    assert statements.cached_row_payload(cached,kw['symbol'],now=kw['observed_at'])==panel
