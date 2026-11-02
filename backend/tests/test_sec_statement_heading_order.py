from pathlib import Path
from datetime import datetime,timedelta
from zoneinfo import ZoneInfo
import copy,gzip,hashlib,json
from lxml import html
import pytest
from app.services import sec_tabular_financials as tab,sec_tabular_reporting as reporting

ROOT=Path(__file__).parent/'fixtures/sec_statement_heading_order'
PROOF=json.loads((ROOT/'manifest.json').read_bytes())
ENTRY=PROOF['entry']
NOW=datetime.fromisoformat(PROOF['observed_at'])

def original(url):
    meta=PROOF['originals'][url];raw=gzip.decompress((ROOT/meta['file']).read_bytes())
    assert len(raw)==meta['bytes'] and hashlib.sha256(raw).hexdigest()==meta['sha256']
    return raw

RAW=original(ENTRY['url'])
COMPANY=original(ENTRY['company_url'])
ARGS=dict(source_url=ENTRY['url'],cover_url=ENTRY['cover_url'],cover_raw=original(ENTRY['cover_url']),
    company_raw=COMPANY,symbol=ENTRY['symbol'],cik=ENTRY['cik'],
    available_by=NOW.astimezone(ZoneInfo('America/New_York')).date()-timedelta(days=1))

def changed_text(old,new):
    tree=html.fromstring(RAW);count=0
    for n in tree.xpath('//*[self::p or self::div or self::td or self::th][not(.//p)][not(.//div)][not(.//table)]'):
        text=tab.visible_text(n)
        if old in text:
            for child in list(n):n.remove(child)
            n.text=text.replace(old,new);count+=1
    assert count
    return html.tostring(tree)

def test_complete_original_40_source_values_periods_and_repeat():
    result=tab.extract(RAW,**ARGS)
    assert result==tab.extract(RAW,**ARGS)
    periods=[('2026-01-01','2026-06-30'),('2025-01-01','2025-06-30'),
        ('2026-04-01','2026-06-30'),('2025-04-01','2025-06-30'),('2025-01-01','2025-12-31')]
    expected={}
    for metric,values in {
        'revenue':[3177.2,3642.3,1780.7,1635.7,6904.2],
        'operating_income':[126,613.5,144.3,149.2,1016],
        'operating_cash_flow':[657.3,1296,394.6,441.3,2299.5],
        'consolidated_income':[-22.2,319.8,64.1,23.7,481.5],
        'gross_profit':[312.2,754,255,220.8,1320.9],
    }.items():
        for (start,end),value in zip(periods,values):expected[metric,start,end]=round(value*1_000_000)
    for metric,values in {
        'current_assets':[2853.8,2766.7,2630.6],
        'current_liabilities':[2295.4,2304.5,2134.1],
        'total_assets':[10665.2,11097.7,11008.1],
        'total_equity':[3884.6,3901.7,4025.6],
        'total_liabilities':[6780.6,7196,6982.5],
    }.items():
        for end,value in zip(['2026-06-30','2025-06-30','2025-12-31'],values):expected[metric,None,end]=round(value*1_000_000)
    assert {(f['metric'],f['start'],f['end']):f['value'] for f in result['facts']}==expected
    assert len(result['facts'])==40 and not result['holds'] and not result['conflicts']
    assert {f['currency'] for f in result['facts']}=={'USD'}
    assert result['publication_eligible'] is False
    assert result['source']['statement_heading_policy']=='explicit_statement_heading_order_v1'
    assert not {'parent_income','eps','capex','ebitda','net_debt'} & {f['metric'] for f in result['facts']}

def test_original_normalization_core_panel_complete_repeat():
    normalized=reporting.normalize(RAW,**ARGS)
    assert normalized==reporting.normalize(RAW,**ARGS)
    supplement=dict(table_original=RAW,table_args=ARGS,evidence={'table_reporting':normalized['evidence']})
    facts=json.dumps({'cik':int(ENTRY['cik'])}).encode()
    kw=dict(symbol=ENTRY['symbol'],cik=ENTRY['cik'],observed_at=NOW,supplement=supplement)
    core=reporting.core(facts,COMPANY,**kw);panel=reporting.panel(facts,COMPANY,**kw)
    assert core==reporting.core(facts,COMPANY,**kw) and panel==reporting.panel(facts,COMPANY,**kw)
    assert core['current_ratio']==2853.8/2295.4
    assert panel['status']=='partial' and panel['reportingCurrency']=='USD'
    assert len(panel['annual'])==1 and len(panel['quarterly'])==2 and not panel['semiannual']
    assert [p['revenue'] for p in panel['quarterly']]==[1635.7e6,1780.7e6]
    assert panel['annual'][0]['revenue']==6904.2e6
    assert all(p['netIncome'] is None and p['eps'] is None for p in panel['quarterly']+panel['annual'])

@pytest.mark.parametrize('prefix',['Adjusted ','Non-GAAP ','Pro forma ','Notes to ','Non-IFRS '])
def test_non_primary_heading_does_not_admit_new_values(prefix):
    altered=changed_text('CONSOLIDATED',prefix+'CONSOLIDATED')
    result=tab.extract(altered,**ARGS)
    assert not result['facts']

def test_quote_cannot_own_recovered_statement():
    tree=html.Element('blockquote');tree.append(html.fromstring(RAW))
    assert not tab.extract(html.tostring(tree),**ARGS)['facts']

@pytest.mark.parametrize('old,new',[
    ('US $','CA $'),
    ('2026','2027'),
    ('ZIM INTEGRATED SHIPPING SERVICES LTD.','OTHER SHIPPING SERVICES LTD.'),
    ('ZIM INTEGRATED SHIPPING SERVICES LTD.',''),
])
def test_currency_future_period_or_company_change_cannot_qualify(old,new):
    result=tab.extract(changed_text(old,new),**ARGS)
    assert not result['facts']

def test_whole_document_source_identity_and_link_required():
    with pytest.raises(tab.TabularFinancialError,match='source_accession_identity'):
        tab.extract(RAW,**{**ARGS,'source_url':ENTRY['url'].replace('004191','004192')})
    with pytest.raises(tab.TabularFinancialError,match='source_not_linked'):
        tab.extract(RAW,**{**ARGS,'cover_raw':b'<html>No exhibit link</html>'})

@pytest.mark.parametrize('label,kind',[
    ('Adjusted operating profit','income'),('Profit for the period attributable to owners','income'),
    ('Profit for the period per share','income'),('Profit for the period','cashflow'),
    ('Acquisition and capitalized expenditures of tangible assets, intangible assets and interest in investees','cashflow'),
    ('Net cash generated from financing activities','cashflow'),
])
def test_ambiguous_or_different_metric_never_substitutes(label,kind):
    assert tab.metric_for(label,kind,'Example Ltd',expanded=True) is None

def test_new_grammar_does_not_guess_arbitrary_words_or_qualifiers():
    assert tab.table_kind('Consolidated unaudited interim income statements',expanded=True)=='income'
    assert tab.table_kind('Consolidated unaudited interim income statements') is None
    for title in ['Consolidated selected income statements','Consolidated hypothetical statements of income',
                  'Consolidated adjusted income statements','Consolidated non-IFRS income statements']:
        assert tab.table_kind(title,expanded=True) is None

def test_single_filing_qualifies_but_does_not_bypass_incomplete_discovery():
    from app.services import sec_financial_discovery as discovery,sec_financial_discovery_reader as reader
    kw=dict(symbol=ENTRY['symbol'],cik=ENTRY['cik'],available_by=ARGS['available_by'],observed_at=NOW)
    state=discovery.inspect(COMPANY,**kw)['state']
    candidate=next(c for c in discovery._inventory(COMPANY,symbol=kw['symbol'],cik=kw['cik'],available_by=kw['available_by']) if c['cover_url']==ENTRY['cover_url'])
    key=discovery._fingerprint(candidate)
    state['receipts'][key]=discovery._observe(ARGS['cover_raw'],candidate,NOW)
    class Originals:
        def get(self,url):return original(url)
    facts=json.dumps({'cik':int(ENTRY['cik'])}).encode()
    result=reader.inspect_candidate(COMPANY,facts,checkpoint=state,accession=candidate['accession'],client=Originals(),annual_rollover=True,**kw)
    assert result['receipt']['status']=='qualified'
    assert result['receipt']['current_period']=='2026-06-30'
    assert result['receipt']['financial_publication_eligible'] is False
    state['receipts'][key]['qualification']=result['receipt']
    assert reader.selection_plan(COMPANY,checkpoint=state,**kw)['status']=='pending'
    # An old empty-format hold retries once; already qualified receipts retain
    # their exact shape and do not force a universe-wide cache reset.
    current=result['receipt'];cover=state['receipts'][key]
    assert reader.validate_receipt(current,cover,now=NOW)==current
    assert 'statement_heading_policy' not in current
    old=copy.deepcopy(current)
    old.update(status='held',kind=None,current_period=None,
        holds=[{'accession':candidate['accession'],'filed':candidate['filed'],'reason':'financial_hint_not_qualified'}])
    assert reader.validate_receipt(old,cover,now=NOW) is None
    old['statement_heading_policy']='explicit_statement_heading_order_v1'
    assert reader.validate_receipt(old,cover,now=NOW)==old


@pytest.fixture(scope='module')
def complete_inventory():
    from app.services import sec_financial_discovery as discovery,sec_financial_discovery_reader as reader
    kw=dict(symbol=ENTRY['symbol'],cik=ENTRY['cik'],available_by=ARGS['available_by'],observed_at=NOW)
    state=discovery.inspect(COMPANY,**kw)['state'];used=set()
    class Originals:
        def get(self,url):used.add(url);return original(url)
    client=Originals();facts=json.dumps({'cik':int(ENTRY['cik'])}).encode()
    candidates=discovery._inventory(COMPANY,symbol=kw['symbol'],cik=kw['cik'],available_by=kw['available_by'])
    assert len(candidates)==10
    for candidate in candidates:
        key=discovery._fingerprint(candidate)
        state['receipts'][key]=discovery._observe(client.get(candidate['cover_url']),candidate,NOW)
        result=reader.inspect_candidate(COMPANY,facts,checkpoint=state,accession=candidate['accession'],client=client,annual_rollover=True,**kw)
        state['receipts'][key]['qualification']=result['receipt']
    plan=reader.selection_plan(COMPANY,checkpoint=state,**kw)
    assert plan['status']=='ready_for_original_revalidation'
    assert plan['pending_covers']==plan['pending_qualifications']==0
    assert plan['selected_accessions']==['0001178913-26-004191']
    assert plan['financial_publication_eligible'] is False
    assert len(used)==15
    return kw,state,client,facts


def test_complete_current_inventory_revalidation_and_consumer_repeat(complete_inventory):
    from app.services import sec_financial_discovery_reader as reader
    kw,state,client,facts=complete_inventory
    supplement=reader.restore(COMPANY,facts,checkpoint=state,client=client,annual_rollover=True,**kw)
    assert supplement==reader.restore(COMPANY,facts,checkpoint=state,client=client,annual_rollover=True,**kw)
    args={k:v for k,v in kw.items() if k!='available_by'}
    core=reporting.core(facts,COMPANY,supplement=supplement,**args)
    panel=reporting.panel(facts,COMPANY,supplement=supplement,**args)
    assert core==reporting.core(facts,COMPANY,supplement=supplement,**args)
    assert panel==reporting.panel(facts,COMPANY,supplement=supplement,**args)
    assert core['current_ratio']==2853.8/2295.4 and len(panel['quarterly'])==2
    assert [r['revenue'] for r in panel['quarterly']]==[1635.7e6,1780.7e6]


def test_complete_revalidation_rejects_modified_statement(complete_inventory):
    from app.services import sec_financial_discovery as discovery,sec_financial_discovery_reader as reader
    kw,state,client,facts=complete_inventory
    class Changed:
        def get(self,url):
            raw=client.get(url)
            if url==ENTRY['url']:
                changed=raw.replace(b'3,177.2',b'3,177.3',1)
                assert changed!=raw
                return changed
            return raw
    with pytest.raises(discovery.DiscoveryError,match='discovery_qualified_original_changed'):
        reader.restore(COMPANY,facts,checkpoint=state,client=Changed(),annual_rollover=True,**kw)
