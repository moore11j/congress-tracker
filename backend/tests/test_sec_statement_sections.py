from pathlib import Path
import copy,gzip,hashlib,json
import pytest
from lxml import html
from app.services.sec_statement_sections import section_observations
from app.services import sec_tabular_financials as tab

ROOT=Path(__file__).parent/'fixtures/sec_statement_sections'
PROOF=json.loads((ROOT/'manifest.json').read_bytes())
ITEM=next(r for u,r in PROOF['originals'].items() if 'ex99' in u)
RAW=gzip.decompress((ROOT/ITEM['file']).read_bytes())
COMPANY=json.loads(gzip.decompress((ROOT/'company.json.gz').read_bytes()))


def changed_text(old,new):
    tree=html.fromstring(RAW);count=0
    for n in tree.xpath('//*[self::p or self::div or self::td or self::th][not(.//p)][not(.//div)][not(.//table)]'):
        text=tab.visible_text(n)
        if old in text:
            for child in list(n):n.remove(child)
            n.text=text.replace(old,new);count+=1
    assert count>0
    return html.tostring(tree)


def test_full_original_all_28_values_periods_currency_and_repeat():
    assert len(RAW)==ITEM['bytes'] and hashlib.sha256(RAW).hexdigest()==ITEM['sha256']
    result=section_observations(RAW,COMPANY)
    assert result==section_observations(RAW,COMPANY)
    expected={}
    for start,end,values in [
        ('2026-07-01','2026-09-30',[10270,7510,2930,2269]),
        ('2025-07-01','2025-09-30',[9076,6671,2487,2051]),
        ('2026-01-01','2026-09-30',[29703,21710,8313,6424]),
        ('2025-01-01','2025-09-30',[27116,19898,7276,5596])]:
        for metric,value in zip(('revenue','gross_profit','operating_income','consolidated_income'),values):
            expected[metric,start,end]=value*1_000_000
    for end,values in [('2026-09-30',[22115,19110,76336,29875,46461]),('2025-12-31',[20247,17409,70358,25284,45073])]:
        for metric,value in zip(('current_assets','current_liabilities','total_assets','total_liabilities','total_equity'),values):
            expected[metric,None,end]=value*1_000_000
    expected['operating_cash_flow','2026-01-01','2026-09-30']=8586_000_000
    expected['operating_cash_flow','2025-01-01','2025-09-30']=7859_000_000
    assert len(result['facts'])==28
    assert {(f['metric'],f['start'],f['end']):f['value'] for f in result['facts']}==expected
    assert {f['currency'] for f in result['facts']}=={'EUR'}
    assert result['publication_eligible'] is False
    assert not {'parent_income','eps','net_debt','ebitda'} & {f['metric'] for f in result['facts']}


@pytest.mark.parametrize('field,value,reason',[
    ('name','Different Company SE','section_issuer_mismatch'),
    ('name','SAP France SE','section_issuer_mismatch'),
    ('fiscalYearEnd','0630','calendar_quarter_unproved'),
    ('fiscalYearEnd',None,'calendar_quarter_unproved'),
])
def test_company_and_fiscal_context_cannot_be_inferred(field,value,reason):
    company={**COMPANY,field:value}
    with pytest.raises(ValueError,match=reason):section_observations(RAW,company)


@pytest.mark.parametrize('old,new,reason',[
    ('Non-IFRS Numbers','Other information','section_end_unproved'),
    ('Primary Financial Statements of SAP Group (IFRS)','Primary Financial Statements of Another Group (IFRS)','section_issuer_mismatch'),
    ('For the three months ended September 30, 2026 and 2025','For the three months ended June 30, 2026 and 2025','quarter_date_corroboration_missing'),
    ('Q3 2026','Q2 2026','quarter_header_conflict'),
    ('9/30/2026','9/10/2026','numeric_date_ambiguous'),
    ('12/31/2025','12/31/2026','year_only_balance_columns_ambiguous'),
    ('Consolidated Statements of Financial Position','Subsidiary Consolidated Statements of Financial Position','unrecognized_section_context'),
    ('€ millions','USD millions','explicit_eur_scale_missing'),
])
def test_complete_document_mutations_keep_ambiguities_held(old,new,reason):
    with pytest.raises(ValueError,match=reason):section_observations(changed_text(old,new),COMPANY)


def test_adjusted_number_inserted_inside_section_is_rejected():
    tree=html.fromstring(RAW)
    heading=next(n for n in tree.xpath('//p') if ''.join(n.itertext()).strip()=='Primary Financial Statements of SAP Group (IFRS)')
    n=html.Element('p');n.text='Adjusted financial results';heading.addnext(n)
    with pytest.raises(ValueError,match='mixed_accounting_section'):section_observations(html.tostring(tree),COMPANY)


def test_unrelated_entity_between_owner_and_table_is_rejected():
    tree=html.fromstring(RAW)
    heading=next(n for n in tree.xpath('//p') if ''.join(n.itertext()).strip()=='Primary Financial Statements of SAP Group (IFRS)')
    n=html.Element('p');n.text='Acquired Subsidiary Limited';heading.addnext(n)
    with pytest.raises(ValueError,match='unrecognized_section_context'):section_observations(html.tostring(tree),COMPANY)


def test_layout_is_not_keyed_to_sap_symbol_or_issuer():
    raw=changed_text('SAP','Example Technologies')
    company={**COMPANY,'name':'Example Technologies SE','tickers':['TEST']}
    result=section_observations(raw,company)
    original=section_observations(RAW,COMPANY)
    assert result['facts']==original['facts'] and result['tables']==original['tables']
    assert result['publication_eligible'] is False



@pytest.fixture(scope='module')
def source_pipeline():
    from datetime import datetime,timedelta
    from zoneinfo import ZoneInfo
    from app.services import sec_financial_discovery as discovery,sec_financial_discovery_reader as reader
    company=gzip.decompress((ROOT/PROOF['company']['file']).read_bytes())
    assert len(company)==PROOF['company']['bytes'] and hashlib.sha256(company).hexdigest()==PROOF['company']['sha256']
    facts=json.dumps({'cik':1000184}).encode()
    now=datetime.fromisoformat(PROOF['observed_at'])
    kw=dict(symbol='SAP',cik='0001000184',available_by=now.astimezone(ZoneInfo('America/New_York')).date()-timedelta(days=1),observed_at=now)
    class Originals:
        def __init__(self):self.used=set()
        def get(self,url):
            assert url in PROOF['originals'],'Uncaptured source forbidden'
            meta=PROOF['originals'][url];raw=gzip.decompress((ROOT/meta['file']).read_bytes())
            assert len(raw)==meta['bytes'] and hashlib.sha256(raw).hexdigest()==meta['sha256']
            self.used.add(url);return raw
    client=Originals();state=discovery.inspect(company,**kw)['state']
    for candidate in discovery._inventory(company,symbol='SAP',cik=kw['cik'],available_by=kw['available_by']):
        key=discovery._fingerprint(candidate)
        state['receipts'][key]=discovery._observe(client.get(candidate['cover_url']),candidate,now)
        result=reader.inspect_candidate(company,facts,checkpoint=state,accession=candidate['accession'],client=client,annual_rollover=True,**kw)
        state['receipts'][key]['qualification']=result['receipt']
    selection=reader.selection_plan(company,checkpoint=state,**kw)
    assert selection['status']=='ready_for_original_revalidation'
    supplement=reader.restore(company,facts,checkpoint=state,client=client,annual_rollover=True,**kw)
    return company,facts,kw,client,state,supplement


def test_full_discovery_original_revalidation_panel_core_and_repeat(source_pipeline):
    from app.services import sec_tabular_reporting as reporting,sec_financial_discovery_reader as reader
    company,facts,kw,client,state,supplement=source_pipeline
    assert len(client.used)==4
    assert supplement==reader.restore(company,facts,checkpoint=state,client=client,annual_rollover=True,**kw)
    args={k:v for k,v in kw.items() if k!='available_by'}
    core=reporting.core(facts,company,supplement=supplement,**args)
    panel=reporting.panel(facts,company,supplement=supplement,**args)
    assert core==reporting.core(facts,company,supplement=supplement,**args)
    assert panel==reporting.panel(facts,company,supplement=supplement,**args)
    assert core['current_ratio']==22115/19110
    assert len(panel['quarterly'])==2 and not panel['annual'] and not panel['semiannual']
    assert [r['revenue'] for r in panel['quarterly']]==[9076_000_000,10270_000_000]
    assert all(r['netIncome'] is None and r['eps'] is None for r in panel['quarterly'])
    assert panel['reportingCurrency']=='EUR' and panel['status']=='partial'
    assert panel['sourceEvidence']['tableReporting']['source']['statement_section']['policy']=='explicit_named_statement_sections_v1'


def test_revalidation_rejects_changed_original_values(source_pipeline):
    from app.services import sec_financial_discovery as discovery,sec_financial_discovery_reader as reader
    company,facts,kw,client,state,_=source_pipeline
    class Changed:
        def get(self,url):
            raw=client.get(url)
            if 'ex99' in url:
                changed=raw.replace(b'10,270',b'10,271',1)
                assert changed!=raw
                return changed
            return raw
    with pytest.raises(discovery.DiscoveryError,match='discovery_qualified_original_changed'):
        reader.restore(company,facts,checkpoint=state,client=Changed(),annual_rollover=True,**kw)


def test_legacy_currency_hold_retries_once_and_qualified_receipt_is_unchanged(source_pipeline):
    from app.services import sec_financial_discovery as discovery,sec_financial_discovery_reader as reader
    from app.services.sec_statement_sections import POLICY
    _,_,kw,_,state,_=source_pipeline
    old=copy.deepcopy(PROOF['receipts'][0]);key=discovery._fingerprint(old['candidate']);cover=state['receipts'][key]
    assert reader.validate_receipt(old,cover,now=kw['observed_at']) is None
    old['statement_section_policy']=POLICY
    assert reader.validate_receipt(old,cover,now=kw['observed_at'])==old
    current=cover['qualification']
    assert current['status']=='qualified' and 'statement_section_policy' not in current
    assert reader.validate_receipt(current,cover,now=kw['observed_at'])==current


def test_existing_numeric_observations_and_other_holds_cannot_be_overwritten():
    from app.services.sec_statement_sections import extend
    prior={'facts':[{'metric':'revenue','value':42}]}
    assert extend(prior,RAW,json.dumps(COMPANY).encode()) is prior
    prior={'facts':[],'holds':[{'table':36,'reason':'column_currency_conflict'}],
        'conflicts':[],'source':{'filed':'2026-10-26'}}
    assert extend(prior,RAW,json.dumps(COMPANY).encode()) is prior
    prior={'facts':[],'holds':[],'conflicts':[],'source':{'filed':'2026-09-01'}}
    assert extend(prior,RAW,json.dumps(COMPANY).encode()) is prior


def test_quoted_section_never_authorizes_financial_values():
    original=html.fromstring(RAW);tree=html.Element('div')
    quote=html.SubElement(tree,'blockquote');quote.append(original)
    with pytest.raises(ValueError,match='section_identity_ambiguous'):
        section_observations(html.tostring(tree),COMPANY)
