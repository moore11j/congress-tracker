from pathlib import Path
from datetime import datetime,timedelta,date
from zoneinfo import ZoneInfo
import gzip,hashlib,json
from lxml import html
import pytest
from app.services import sec_tabular_financials as tab,sec_tabular_reporting as reporting
from app.services import sec_statement_layout as layout,sec_statement_declarations as declarations

ROOT=Path(__file__).parent/'fixtures/sec_statement_layout'
PROOF=json.loads((ROOT/'manifest.json').read_bytes());ENTRY=PROOF['entry']
NOW=datetime.fromisoformat(PROOF['observed_at'])
def original(url):
    meta=PROOF['originals'][url];raw=gzip.decompress((ROOT/meta['file']).read_bytes())
    assert len(raw)==meta['bytes'] and hashlib.sha256(raw).hexdigest()==meta['sha256'];return raw
RAW=original(ENTRY['url']);COMPANY=original(ENTRY['company_url'])
ARGS=dict(source_url=ENTRY['url'],cover_url=ENTRY['cover_url'],cover_raw=RAW,
    company_raw=COMPANY,symbol=ENTRY['symbol'],cik=ENTRY['cik'],
    available_by=NOW.astimezone(ZoneInfo('America/New_York')).date()-timedelta(days=1))

@pytest.fixture(scope='module')
def observations():return tab.extract(RAW,**ARGS)

def test_complete_original_all_values_periods_and_source_identity(observations):
    expected={}
    for metric,values in {'current_assets':[65887356,64933053],'current_liabilities':[42496851,47783052],
        'total_assets':[75697476,74409523],'total_liabilities':[60681611,59769953],'total_equity':[15015865,14639570]}.items():
        for end,value in zip(('2026-06-30','2025-12-31'),values):expected[metric,None,end]=value*1000
    for start,end,value in [('2026-04-01','2026-06-30',549076),('2025-04-01','2025-06-30',536759),
            ('2026-01-01','2026-06-30',1094601),('2025-01-01','2025-06-30',1061851)]:
        expected['consolidated_income',start,end]=value*1000
    for start,end,value in [('2026-01-01','2026-06-30',1938645),('2025-01-01','2025-06-30',3451822)]:
        expected['operating_cash_flow',start,end]=value*1000
    assert len(observations['facts'])==16 and not observations['holds'] and not observations['conflicts']
    assert {(f['metric'],f['start'],f['end']):f['value'] for f in observations['facts']}==expected
    assert {f['currency'] for f in observations['facts']}=={'BRL'}
    assert observations['source']['statement_sha256']==hashlib.sha256(RAW).hexdigest()
    assert observations['source']['statement_layout_policy']==layout.POLICY
    assert observations['publication_eligible'] is False
    assert not {'revenue','parent_income','eps','capex','ebitda'} & {f['metric'] for f in observations['facts']}

def test_original_normalization_and_consumers_repeat_without_semantic_substitution():
    norm=reporting.normalize(RAW,**ARGS)
    assert norm==reporting.normalize(RAW,**ARGS)
    assert norm['evidence']['currency']=='BRL' and norm['evidence']['accounting_basis']=='IFRS'
    assert any('with the International Financial Reporting Standards' in v for v in norm['evidence']['accounting_declarations'])
    supplement=dict(table_original=RAW,table_args=ARGS,evidence={'table_reporting':norm['evidence']})
    facts=json.dumps({'cik':int(ENTRY['cik'])}).encode();kw=dict(symbol=ENTRY['symbol'],cik=ENTRY['cik'],observed_at=NOW,supplement=supplement)
    core=reporting.core(facts,COMPANY,**kw);panel=reporting.panel(facts,COMPANY,**kw)
    assert core==reporting.core(facts,COMPANY,**kw) and panel==reporting.panel(facts,COMPANY,**kw)
    assert core['current_ratio']==65887356/42496851
    assert not panel['annual'] and not panel['quarterly'] and not panel['semiannual']
    assert not norm['records']['revenue'] and not norm['records']['net_income'] and not norm['records']['eps']

def layout_tree(owner='Example Ltd.',between='<p></p>',caption='Unaudited condensed consolidated interim balance sheet',scale='thousands'):
    return html.fromstring(f'<div><table><tr><td><p>{owner}</p><p>{caption}</p><p>(All amounts in {scale} of reais)</p></td><td></td></tr></table>{between}<table id="values"><tr><td></td><td>June 30, 2026</td><td>December 31, 2025</td></tr><tr><td>Total current assets</td><td>10</td><td>8</td></tr></table></div>')

@pytest.mark.parametrize('between',['<p>Different Company Ltd.</p>','<p>Unreviewed context</p>','<table><tr><td>42</td></tr></table>'])
def test_caption_cannot_cross_substantive_context_or_another_table(between):
    tree=layout_tree(between=between)
    assert layout.context(tree.get_element_by_id('values'),'Example Ltd.') is None

@pytest.mark.parametrize('owner',['Example subsidiary Ltd.','Other Company Ltd.'])
def test_nearest_explicit_company_cannot_be_replaced_by_filer(owner):
    tree=layout_tree(owner=owner)
    with pytest.raises(tab.TabularFinancialError,match='layout_statement_owner_unproved'):
        layout.context(tree.get_element_by_id('values'),'Example Ltd.')

def test_quoted_layout_and_adjusted_headings_are_not_financial_admission():
    tree=html.fromstring('<blockquote>'+html.tostring(layout_tree()).decode()+'</blockquote>')
    assert not layout.has_caption(tree,'Example Ltd.')
    tree=layout_tree(caption='Adjusted consolidated interim balance sheet')
    assert layout.context(tree.get_element_by_id('values'),'Example Ltd.') is None

def declaration_tree(currency='Brazilian reais',scale='thousands',prefix=''):
    return html.fromstring(f'<div><p>{prefix}The consolidated financial statements are presented in {scale} of {currency}, unless otherwise indicated, which is the functional currency of Example Group.</p></div>')

def test_explicit_currency_and_scale_are_independent_of_bare_reais_marker():
    assert layout.currency_scope(layout_tree(),'Example Ltd.') is None
    scope=layout.currency_scope(declaration_tree(),'Example Ltd.')
    assert scope['currency']=='BRL' and scope['scale']==1000
    tree=layout_tree(scale='millions')
    with pytest.raises(tab.TabularFinancialError,match='layout_scale_conflict'):
        tab.parse_table(tree.get_element_by_id('values'),issuer='Example Ltd.',filed=date(2026,8,20),currency_scope=scope,_layout=True)

@pytest.mark.parametrize('currency,scale',[('U.S. dollars','thousands'),('Brazilian reais','millions')])
def test_conflicting_statement_declarations_cannot_authorize_currency(currency,scale):
    tree=declaration_tree();tree.append(declaration_tree(currency,scale))
    assert layout.currency_scope(tree,'Example Ltd.') is None

@pytest.mark.parametrize('prefix',['Not ','Hypothetical ','If '])
def test_conditional_currency_statement_stays_unproved(prefix):
    assert layout.currency_scope(declaration_tree(prefix=prefix),'Example Ltd.') is None

def test_period_wording_preserves_separate_quarter_and_ytd_columns():
    shared=['For the three- and six-month periods ended June 30, 2026 and 2025']
    for title,months,start in [('Three-month periods ended June 30,',3,date(2026,4,1)),('Six-month periods ended June 30,',6,date(2026,1,1))]:
        texts=[title,'2026'];header=' '.join(shared+texts)
        assert tab.column_period(layout.period_text(header),[layout.period_text(t) for t in texts],
            [layout.period_text(t) for t in shared],'income',date(2026,8,20))==(start,date(2026,6,30),months)

def test_rejected_heading_does_not_consume_later_valid_preparation_subject():
    text='Financial statements and policies. These financial statements have been prepared in accordance with the International Financial Reporting Standards as issued by the IASB.'
    assert declarations.declarations(text)==['financial statements have been prepared in accordance with the International Financial Reporting Standards']

@pytest.mark.parametrize('text',[
    'These financial statements have not been prepared in accordance with the International Financial Reporting Standards.',
    'These financial statements will be prepared in accordance with the International Financial Reporting Standards.',
    'Hypothetical financial statements have been prepared in accordance with the International Financial Reporting Standards.',
    'Financial statements are missing. Other materials were prepared in accordance with the International Financial Reporting Standards.',
])
def test_new_clause_scan_preserves_negation_future_and_sentence_guards(text):
    assert declarations.declarations(text)==[]

def test_existing_supported_declaration_does_not_change_when_new_wording_present():
    old='Financial statements have been prepared in accordance with IFRS.'
    new='These financial statements have been prepared in accordance with the International Financial Reporting Standards.'
    assert declarations.declarations(old+' '+new)==declarations.declarations(old)

def test_primary_source_bytes_cannot_be_mismatched():
    with pytest.raises(tab.TabularFinancialError,match='primary_bytes_mismatch'):
        tab.extract(RAW+b' ',**ARGS)


@pytest.fixture(scope='module')
def complete_inventory():
    from app.services import sec_financial_discovery as discovery,sec_financial_discovery_reader as reader
    kw=dict(symbol=ENTRY['symbol'],cik=ENTRY['cik'],available_by=ARGS['available_by'],observed_at=NOW)
    state=discovery.inspect(COMPANY,**kw)['state'];used=set()
    class Originals:
        def get(self,url):used.add(url);return original(url)
    client=Originals();facts=json.dumps({'cik':int(ENTRY['cik'])}).encode()
    inventory=discovery._inventory(COMPANY,symbol=kw['symbol'],cik=kw['cik'],available_by=kw['available_by'])
    assert len(inventory)==8
    for candidate in inventory:
        key=discovery._fingerprint(candidate)
        state['receipts'][key]=discovery._observe(client.get(candidate['cover_url']),candidate,NOW)
        result=reader.inspect_candidate(COMPANY,facts,checkpoint=state,accession=candidate['accession'],client=client,annual_rollover=True,**kw)
        state['receipts'][key]['qualification']=result['receipt']
    plan=reader.selection_plan(COMPANY,checkpoint=state,**kw)
    assert plan['status']=='ready_for_original_revalidation' and len(used)==8
    assert plan['pending_covers']==plan['pending_qualifications']==0
    assert plan['selected_accessions']==['0001554855-26-001791']
    assert plan['financial_publication_eligible'] is False
    return kw,state,client,facts


def test_full_current_inventory_source_restoration_and_repeat(complete_inventory):
    from app.services import sec_financial_discovery_reader as reader
    kw,state,client,facts=complete_inventory
    supplement=reader.restore(COMPANY,facts,checkpoint=state,client=client,annual_rollover=True,**kw)
    assert supplement==reader.restore(COMPANY,facts,checkpoint=state,client=client,annual_rollover=True,**kw)
    proof=supplement['evidence']['table_reporting']
    assert proof['currency']=='BRL' and proof['native_observation_count']==16
    assert proof['current_period']=='2026-06-30'


def test_revalidation_rejects_changed_primary_original(complete_inventory):
    from app.services import sec_financial_discovery as discovery,sec_financial_discovery_reader as reader
    kw,state,client,facts=complete_inventory
    class Changed:
        def get(self,url):
            raw=client.get(url)
            if url==ENTRY['url']:
                changed=raw.replace(b'65,887,356',b'65,887,357',1)
                assert changed!=raw
                return changed
            return raw
    with pytest.raises(discovery.DiscoveryError,match='discovery_qualified_original_changed'):
        reader.restore(COMPANY,facts,checkpoint=state,client=Changed(),annual_rollover=True,**kw)
