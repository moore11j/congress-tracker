"""Complete-original admission, accounting and acquisition boundary checks."""
from pathlib import Path
from datetime import date,datetime,timezone
from functools import lru_cache
from copy import deepcopy
from unittest.mock import patch
import gzip,hashlib,json,socket
import pytest
from app.services import sec_primary_income as income,sec_inline_financials as inline,sec_fundamentals as sec

ROOT=Path(__file__).parent/'fixtures/sec_primary_income'
MANIFEST=json.loads((ROOT/'sources.json').read_text())
NOW=datetime(2026,10,27,4,50,tzinfo=timezone.utc)
DAY=date(2026,10,26)
CIK="0000711404"
from app.services import sec_financial_statements as statements

@lru_cache(maxsize=8)
def raw(name):
    meta=MANIFEST[name];value=gzip.decompress((ROOT/meta['file']).read_bytes())
    assert hashlib.sha256(value).hexdigest()==meta['sha256'] and len(value)==meta['bytes']
    return value

def inputs(symbol):return json.loads(raw(symbol+'-facts.json')),json.loads(raw(symbol+'-company.json'))

class Client:
    def __init__(self):self.calls=[]
    def get(self,url):
        self.calls.append(url)
        names=[n for n,m in MANIFEST.items() if m['url']==url]
        assert len(names)==1,'Unexpected source '+url
        return raw(names[0])

@lru_cache(maxsize=2)
def acquired(symbol='COO'):
    facts,company=inputs(symbol);client=Client()
    with patch.object(socket.socket,'connect',side_effect=AssertionError('Offline fixture attempted network')):
        item=inline.acquire(raw(symbol+'-facts.json'),raw(symbol+'-company.json'),symbol=symbol,
            cik=str(company['cik']).zfill(10),observed_at=NOW,client=client)
    assert len(client.calls)==len(set(client.calls))==2
    return item

def project(symbol,item):
    _,company=inputs(symbol)
    return sec.project_fundamentals(raw(symbol+'-facts.json'),raw(symbol+'-company.json'),symbol=symbol,
        cik=str(company['cik']).zfill(10),observed_at=NOW,filing_supplement=item)

def test_complete_sources_restore_ratio_without_changing_other_supported_values():
    before=project('COO',None);item=acquired();after=project('COO',item)
    assert before.get('net_debt_to_ebitda') is None
    assert after['net_debt_to_ebitda']==2389500000/935600000
    old=json.loads(before['source_evidence_json'])
    assert all(after[k]==before[k] for k in old['calculations'])
    proof=item['evidence']['primary_consolidated_income']
    assert len(proof['rows'])==3 and income.cached_valid(proof,cik='0000711404')
    assert {p['value'] for p in proof['rows']}=={374900000,485700000,290300000}
    assert after==project('COO',item)
    ev=json.loads(after['source_evidence_json'])
    assert ev['leverage_preparation']['ebitda']['value']==935600000
    assert ev['inline_filing_supplement']['primary_consolidated_income']==proof

def test_second_issuer_keeps_unproved_debt_unavailable():
    before=project('AMC',None);after=project('AMC',acquired('AMC'))
    assert after.get('net_debt_to_ebitda') is None
    old=json.loads(before['source_evidence_json']);ev=json.loads(after['source_evidence_json'])
    assert all(before[k]==after[k] for k in old['calculations'])
    assert ev['leverage_preparation']['ebitda']['value']==321900000
    assert ev['leverage_preparation']['ebitda']['status']=='components_complete'

@pytest.mark.parametrize('change',['value','hash','raw','displayed','missing_document'])
def test_full_original_and_displayed_proof_cannot_diverge(change):
    item=deepcopy(acquired());part=item['primary_income'];url=next(iter(part['documents']))
    if change=='value':part['evidence']['rows'][0]['value']+=1
    elif change=='hash':part['evidence']['rows'][0]['source_sha256']='f'*64
    elif change=='raw':
        original=part['documents'][url];changed=original.replace(b'374.9',b'375.9').replace(b'485.7',b'486.7');assert changed!=original;part['documents'][url]=changed
    elif change=='displayed':item['evidence']['primary_consolidated_income']=None
    else:del part['documents'][url]
    with pytest.raises((ValueError,KeyError)):project('COO',item)

@pytest.mark.parametrize('change',['hash','unit','issuer','accession','arithmetic','nonfinite','bool','duplicate','heading','context','methodology','label','future'])
def test_cached_proof_rejects_corruption(change):
    proof=deepcopy(acquired()['evidence']['primary_consolidated_income']);r=proof['rows'][0]
    if change=='hash':r['source_sha256']='invalid'
    elif change=='unit':r['unit']='EUR'
    elif change=='issuer':r['cik']='0000000001'
    elif change=='accession':r['accession']='0000000001-26-000001'
    elif change=='arithmetic':r['tax']+=1
    elif change=='nonfinite':r['value']=float('nan')
    elif change=='bool':r['value']=True
    elif change=='duplicate':proof['rows'].append(deepcopy(r))
    elif change=='heading':r['heading']=[]
    elif change=='context':r['context']=''
    elif change=='methodology':r['methodology']='unreviewed'
    elif change=='label':r['row_label']='adjusted earnings'
    else:r['end']='2030-01-01'
    assert not income.cached_valid(proof,cik='0000711404')

@pytest.mark.parametrize('change',['wrong_issuer','wrong_total','tax','nil','dimensions','column','hidden','allocation_label','discontinued_label','shared_date','duration'])
def test_complete_original_negative_controls(change):
    from lxml import html
    facts,company=inputs('COO');planned=income.plan(facts,company,symbol='COO',cik='0000711404',available_by=DAY)
    filing=next(r['filing'] for r in planned['rows'] if r['filing']['form']=='10-K')
    name=next(n for n,m in MANIFEST.items() if m['url']==filing['url']);original=raw(name);tree=html.fromstring(original)
    baseline=income.statement_rows(original,filing,company['name']);assert len(baseline)==3
    target=next(p for p in baseline.values() if p['end']=='2025-10-31');ctx=target['context']
    node=next(n for n in inline.nodes(tree,'nonfraction') if n.get('name')=='us-gaap:NetIncomeLoss' and n.get('contextref')==ctx and inline._number(n,tree)==374900000)
    table=node.xpath('ancestor::table')[-1];issuer=company['name']
    if change=='wrong_issuer':issuer='Unrelated Issuer Inc.'
    elif change=='wrong_total':node.text='375.9'
    elif change=='tax':
        tax=next(n for n in inline.nodes(table,'nonfraction') if n.get('name')=='us-gaap:IncomeTaxExpenseBenefit' and n.get('contextref')==ctx);tax.text='192.6'
    elif change=='nil':node.set('xsi:nil','true')
    elif change=='dimensions':
        context=next(n for n in inline.nodes(tree,'context') if n.get('id')==ctx);segment=html.Element('xbrli:segment');context.append(segment)
    elif change=='column':
        other=next(n for n in inline.nodes(table,'nonfraction') if n.get('name')=='us-gaap:NetIncomeLoss' and n.get('contextref')!=ctx);node.set('contextref',other.get('contextref'))
    elif change=='hidden':
        parent=node.getparent();wrapper=html.Element('ix:hidden');parent.replace(node,wrapper);wrapper.append(node)
    elif change in {'allocation_label','discontinued_label'}:
        row=node.xpath('ancestor::tr')[-1];cell=row.xpath('./td')[0];cell.clear();cell.text='Net income attributable to parent' if change=='allocation_label' else 'Net income from discontinued operations'
    else:
        for cell in table.xpath('.//td|.//th'):
            if 'Years Ended October 31' in cell.text_content():
                for textnode in cell.iter():
                    if textnode.text and 'Years Ended October 31' in textnode.text:
                        textnode.text=textnode.text.replace('Years Ended October 31','Years Ended September 30' if change=='shared_date' else 'Two Years Ended October 31')
    modified=html.tostring(tree);assert change=='wrong_issuer' or modified!=original
    try:proof=income.statement_rows(modified,filing,issuer)
    except ValueError:proof={}
    assert not any(p['end']=='2025-10-31' for p in proof.values()),change

def test_serialized_original_remains_valid():
    from lxml import html
    facts,company=inputs('COO');p=income.plan(facts,company,symbol='COO',cik='0000711404',available_by=DAY)
    filing=p['rows'][0]['filing'];name=next(n for n,m in MANIFEST.items() if m['url']==filing['url'])
    assert len(income.statement_rows(html.tostring(html.fromstring(raw(name))),filing,company['name']))>=3

def test_foreign_later_basis_and_financial_industry_do_not_acquire_domestic_income():
    facts,company=inputs('COO');changed=deepcopy(company);recent=changed['filings']['recent']
    for k,value in {'accessionNumber':'0000711404-26-999999','form':'20-F','filingDate':'2026-10-20','reportDate':'2026-10-01','primaryDocument':'new.htm'}.items():recent[k].insert(0,value)
    assert income.plan(facts,changed,symbol='COO',cik='0000711404',available_by=DAY) is None
    changed=deepcopy(company);changed['sic']='6021'
    assert income.plan(facts,changed,symbol='COO',cik='0000711404',available_by=DAY) is None

def test_transport_error_is_not_swallowed():
    from app.clients.direct_sources import DirectSourceError
    _,company=inputs('COO');client=Client()
    with patch.object(client,'get',side_effect=DirectSourceError('bounded transport hold')):
        with pytest.raises(DirectSourceError):inline.acquire(raw('COO-facts.json'),raw('COO-company.json'),symbol='COO',cik='0000711404',observed_at=NOW,client=client)


def test_primary_cache_readers_preserve_proof_and_avoid_refetch(monkeypatch):
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    from app.models import InsightsSnapshot
    from app.services import sec_fundamentals_preparation as prep,sec_directory
    monkeypatch.setattr(socket.socket,'connect',lambda *a,**k:pytest.fail('Unexpected network'))
    monkeypatch.setenv('SEC_FUNDAMENTALS_WARMING_ENABLED','1')
    monkeypatch.setattr(sec_directory,'directory',lambda:{'COO':{'cik':CIK}})
    class Clock(datetime):
        @classmethod
        def now(cls,tz=None):return NOW
    engine=create_engine('sqlite://');InsightsSnapshot.__table__.create(engine);factory=sessionmaker(bind=engine);client=Client()
    try:
        with patch.object(prep,'SessionLocal',factory),patch.object(prep,'datetime',Clock),patch.object(prep,'DirectSourceClient',lambda:client):
            payload=prep.prepare('COO');assert payload['values']['net_debt_to_ebitda']==project('COO',acquired())['net_debt_to_ebitda']
            count=len(client.calls);assert prep.prepare('COO')==payload and len(client.calls)==count
        with patch.object(statements,'SessionLocal',factory),patch.object(statements,'datetime',Clock),patch.object(statements,'DirectSourceClient',lambda:client):
            panel=statements.prepared('COO');count=len(client.calls);assert statements.prepared('COO')==panel and len(client.calls)==count
        with factory() as db:
            for key,field in [('sec-current-fundamentals:COO:v1','calculation_evidence'),('sec-financials:COO:v1','sourceEvidence')]:
                row=db.get(InsightsSnapshot,key);saved=json.loads(row.payload_json)
                nested='inline_filing_supplement' if field=='calculation_evidence' else 'inlineFilingSupplement'
                saved[field][nested]['primary_consolidated_income']['rows'][0]['value']+=1
                row.payload_json=json.dumps(saved)
            db.commit();assert statements.cached_payload(db,'COO',now=NOW) is None
        with patch.object(prep,'SessionLocal',factory):assert prep._cached('COO',NOW) is None
    finally:engine.dispose()


def test_primary_source_score_and_watchlist_recovery_repeat_crossing_rollback(monkeypatch):
    import test_sec_tagged_foreign_consumers as consumer
    urls={m['url']:raw(n) for n,m in MANIFEST.items()}
    monkeypatch.setattr(consumer,'originals',lambda symbol:(raw('COO-facts.json'),raw('COO-company.json'),CIK,urls))
    values=project('COO',acquired());metrics={k:values[k] for k in json.loads(values['source_evidence_json'])['calculations']}
    monkeypatch.setattr(consumer,'EXPECTED',{'COO':{'metrics':metrics}});monkeypatch.setattr(consumer,'NOW',NOW)
    consumer.test_original_source_score_and_alert_recovery_repeat_crossing_rollback('COO','net_debt_to_ebitda',monkeypatch)




@pytest.mark.parametrize('change',['reported_total','reported_conflict','allocation','parent_conflict','identity','future','stale'])
def test_planning_preserves_conflicting_or_inapplicable_observations(change):
    facts,company=inputs('COO');day=DAY
    expected=income.plan(facts,company,symbol='COO',cik=CIK,available_by=day)
    target=expected['rows'][0]['fact']
    base=dict(start=target['start'],end=target['end'],filed=target['filed'],accn=target['accession'],form=expected['rows'][0]['filing']['form'])
    tags=facts['facts']['us-gaap']
    if change in ('reported_total','reported_conflict'):
        rows=tags.setdefault('ProfitLoss',{'units':{'USD':[]}})['units']['USD']
        rows.append({**base,'val':target['value']})
        if change=='reported_conflict':rows.append({**base,'val':target['value']+1})
    elif change=='allocation':tags.setdefault('NetIncomeLossAttributableToNoncontrollingInterest',{'units':{'USD':[]}})['units']['USD'].append({**base,'val':1})
    elif change=='parent_conflict':tags['NetIncomeLoss']['units']['USD'].append({**base,'val':target['value']+1})
    elif change=='identity':
        company['cik']=1
        with pytest.raises(ValueError):income.plan(facts,company,symbol='COO',cik=CIK,available_by=day)
        return
    elif change=='future':day=date(2023,1,1)
    else:day=date(2030,1,1)
    result=income.plan(facts,company,symbol='COO',cik=CIK,available_by=day)
    assert not result or all(r['fact']['start']!=target['start'] or r['fact']['end']!=target['end'] for r in result['rows'])


def test_income_acquisition_shares_six_document_limit_without_refetch(monkeypatch):
    from app.services import sec_operating_history as op
    baseline={'evidence':{'cik':CIK,'methodology':'existing_source'}}
    class BoundClient:
        def __init__(self):self.calls=[]
        def get(self,url):self.calls.append(url);return url.encode()
    def primary(*a,client,**kw):
        for i in range(5):client.get(str(i))
        return deepcopy(baseline)
    def supplement(*a,client,**kw):
        assert client.get('0')==b'0'
        client.get('5');client.get('6')
        pytest.fail('Seventh source admitted')
    monkeypatch.setattr(inline,'_acquire',primary);monkeypatch.setattr(op,'acquire',lambda *a,**kw:None);monkeypatch.setattr(income,'acquire',supplement)
    client=BoundClient()
    result=inline.acquire(raw('COO-facts.json'),raw('COO-company.json'),symbol='COO',cik=CIK,observed_at=NOW,client=client)
    assert result==baseline and client.calls==[str(i) for i in range(6)]
