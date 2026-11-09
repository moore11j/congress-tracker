"""Complete-original arithmetic, identity, cache and consumer acceptance."""
from copy import deepcopy
from datetime import datetime,timezone
from functools import lru_cache
from pathlib import Path
from unittest.mock import patch
import gzip,hashlib,json,socket
import pytest
from lxml import html,etree
from app.services import sec_operating_history as op,sec_operating_bridge as bridge,sec_inline_financials as inline,sec_fundamentals as sec,sec_financial_statements as statements

ROOT=Path(__file__).parent/'fixtures/sec_operating_bridge'
MANIFEST=json.loads((ROOT/'sources.json').read_text())
NOW=datetime(2026,11,9,5,20,tzinfo=timezone.utc)
CIK='0000320187'
@lru_cache(maxsize=5)
def raw(name):
    m=MANIFEST[name];data=gzip.decompress((ROOT/m['file']).read_bytes())
    assert len(data)==m['bytes'] and hashlib.sha256(data).hexdigest()==m['sha256']
    return data
class Client:
    def __init__(self):self.calls=[]
    def get(self,url):
        self.calls.append(url);matches=[n for n,m in MANIFEST.items() if m['url']==url]
        assert len(matches)==1,'Unretained source '+url
        return raw(matches[0])
def inputs():return json.loads(raw('NKE-facts.gz')),json.loads(raw('NKE-company.gz'))
@lru_cache(maxsize=1)
def acquired():
    with patch.object(socket.socket,'connect',side_effect=AssertionError('Unexpected network')):
        c=Client();item=inline.acquire(raw('NKE-facts.gz'),raw('NKE-company.gz'),symbol='NKE',cik=CIK,observed_at=NOW,client=c)
        assert len(c.calls)==len(set(c.calls))==3
        return item
def project(item):return sec.project_fundamentals(raw('NKE-facts.gz'),raw('NKE-company.gz'),symbol='NKE',cik=CIK,observed_at=NOW,filing_supplement=item)

def test_whole_originals_restore_two_margins_and_preserve_every_existing_value():
    before=project(None);after=project(acquired());e=json.loads(before['source_evidence_json'])
    assert all(after[k]==before[k] for k in e['calculations'])
    assert after['operating_margin']==3758000000/45891000000*100
    assert after['operating_margin_expansion']==after['operating_margin']-3420000000/46440000000*100
    assert after.get('net_debt_to_ebitda') is None and after.get('ev_to_ebitda') is None
    proof=acquired()['evidence']['operating_statement_history']
    assert len(proof['rows'])==5 and op.cached_valid(proof,cik=CIK)
    assert all(p['expense_bridge']['methodology']==bridge.METHOD for p in proof['rows'])
    assert project(acquired())==after
    panel=statements.project(raw('NKE-facts.gz'),raw('NKE-company.gz'),symbol='NKE',cik=CIK,observed_at=NOW,filing_supplement=acquired())
    assert panel['sourceEvidence']['inlineFilingSupplement']['operating_statement_history']==proof
    from types import SimpleNamespace
    from app.services.fundamentals_cache import fundamentals_summary_from_cache_row
    summary=fundamentals_summary_from_cache_row(SimpleNamespace(**after),now=NOW)
    assert summary['status']!='unavailable'
    assert summary['metrics']['operating_margin_expansion']['provenance']['version']==bridge.METHOD
    assert 'calculated subtotal' in summary['context']['source_note']

@pytest.mark.parametrize('change',['missing','method','value','tag','label','context','order','tax','basis','old_label'])
def test_cached_proof_rejects_changed_bridge(change):
    p=deepcopy(acquired()['evidence']['operating_statement_history']);r=p['rows'][0];b=r['expense_bridge']
    if change=='missing':del r['expense_bridge']
    elif change=='method':b['methodology']='unknown'
    elif change=='value':b['components']['interest']['value']+=1
    elif change=='tag':b['components']['interest']['tag']='InterestExpense'
    elif change=='label':b['components']['sga']['label']='research expense'
    elif change=='context':b['components']['gross']['context']=''
    elif change=='order':b['components']['pretax']['row']=0
    elif change=='tax':b['tax']+=1
    elif change=='basis':r['basis']='Reported EBITDA'
    else:r['row_labels']=['Gross profit','Total operating expenses']
    assert not op.cached_valid(p,cik=CIK)

@pytest.mark.parametrize('change',['raw','proof','displayed_proof','missing_original'])
def test_consumers_recheck_original_and_proof(change):
    item=deepcopy(acquired());h=item['operating_history'];url=next(iter(h['documents']))
    if change=='raw':h['documents'][url]=h['documents'][url].replace(b'1,209',b'1,210').replace(b'5,257',b'5,258')
    elif change=='proof':h['evidence']['rows'][0]['expense_bridge']['pretax']+=1
    elif change=='displayed_proof':item['evidence']['operating_statement_history']=None
    else:del h['documents'][url]
    with pytest.raises((ValueError,KeyError)):project(item)

@pytest.mark.parametrize('change',['unknown_expense','negative_expense','wrong_tag','net_interest_sign','wrong_context','hidden','nil','owner','subtotal','component_sum'])
def test_complete_current_statement_rejects_bad_bridge(change):
    facts,company=inputs();filing=op.plan(facts,company,cik=CIK,available_by=NOW.date())['rows'][-1]['filing']
    tree=html.fromstring(raw('NKE-0000320187-26-000193.gz'))
    nodes=[n for n in inline.nodes(tree,'nonfraction') if n.get('name')=='us-gaap:SellingGeneralAndAdministrativeExpense']
    table=nodes[0].xpath('ancestor::table')[-1]
    if change in {'unknown_expense','subtotal'}:
        row=deepcopy(nodes[0].xpath('ancestor::tr')[-1]);cells=row.xpath('./td|./th')
        first=next(c for c in cells if c.text_content().strip());first.clear();first.text='Additional restructuring expense' if change=='unknown_expense' else 'Operating income'
        table.insert(list(table).index(nodes[0].xpath('ancestor::tr')[-1])+1,row)
    elif change=='owner':company['name']='Unrelated Issuer'
    else:
        target={'negative_expense':'SellingGeneralAndAdministrativeExpense','wrong_tag':'SellingGeneralAndAdministrativeExpense','net_interest_sign':'InterestIncomeExpenseNonoperatingNet','wrong_context':'SellingGeneralAndAdministrativeExpense','hidden':'SellingGeneralAndAdministrativeExpense','nil':'SellingGeneralAndAdministrativeExpense','component_sum':'MarketingAndAdvertisingExpense'}[change]
        chosen=[n for n in inline.nodes(table,'nonfraction') if n.get('name')=='us-gaap:'+target]
        for node in chosen:
            if change=='negative_expense':node.set('sign','-')
            elif change=='wrong_tag':node.set('name','us-gaap:OperatingExpenses')
            elif change=='net_interest_sign':node.set('sign','-')
            elif change=='wrong_context':node.set('contextref','unknown')
            elif change=='hidden':
                parent=node.getparent();wrapper=html.fragment_fromstring('<ix:hidden></ix:hidden>');parent.replace(node,wrapper);wrapper.append(node)
            elif change=='nil':node.set('xmlns:xsi','http://www.w3.org/2001/XMLSchema-instance');node.set('xsi:nil','true')
            else:node.text='1'
    mutated=etree.tostring(tree,method='html')
    try:rows=bridge.statement_rows(mutated,filing,issuer=company['name'])
    except ValueError:rows={}
    assert not rows

def test_not_a_ticker_rule_and_conflicting_reported_operating_value_is_not_filled():
    facts,company=inputs();c=Client()
    company['tickers']=['OTHER']
    item=inline.acquire(raw('NKE-facts.gz'),json.dumps(company).encode(),symbol='OTHER',cik=CIK,observed_at=NOW,client=c)
    assert item==acquired()
    target=op.plan(facts,company,cik=CIK,available_by=NOW.date())['rows'][0]['gross']
    point=dict(start=target['start'],end=target['end'],filed=target['filed'],accn=target['accession'],form='10-Q')
    facts['facts']['us-gaap'].setdefault('OperatingIncomeLoss',{'units':{'USD':[]}})['units']['USD'].extend([{**point,'val':1},{**point,'val':2}])
    plan=op.plan(facts,company,cik=CIK,available_by=NOW.date())
    assert not plan or all((r['gross']['start'],r['gross']['end'])!=(target['start'],target['end']) for r in plan['rows'])

def test_score_and_watchlist_recovery_repeat_crossing_rollback(monkeypatch):
    import test_sec_tagged_foreign_consumers as consumer
    urls={m['url']:raw(n) for n,m in MANIFEST.items()}
    monkeypatch.setattr(consumer,'originals',lambda symbol:(raw('NKE-facts.gz'),raw('NKE-company.gz'),CIK,urls))
    values=project(acquired());metrics={k:values[k] for k in json.loads(values['source_evidence_json'])['calculations']}
    monkeypatch.setattr(consumer,'EXPECTED',{'NKE':{'metrics':metrics}});monkeypatch.setattr(consumer,'NOW',NOW)
    consumer.test_original_source_score_and_alert_recovery_repeat_crossing_rollback('NKE','operating_margin',monkeypatch)

def test_alert_basis_rebaselines_the_calculation_without_tracking_numbers():
    from app.models import FundamentalsCache
    from app.services.custom_alert_rules import _fundamental_basis
    p=project(acquired());row=FundamentalsCache(symbol='NKE',provider='sec_edgar',source_evidence_json=p['source_evidence_json'])
    original=_fundamental_basis(row,{'operating_margin','revenue_growth'})
    assert original['operating_margin']['operating_measurement']==[bridge.METHOD]
    evidence=json.loads(row.source_evidence_json)
    evidence['inline_filing_supplement']['operating_statement_history']['rows'][0]['expense_bridge']['pretax']+=1
    row.source_evidence_json=json.dumps(evidence)
    assert _fundamental_basis(row,{'operating_margin','revenue_growth'})==original
    del evidence['inline_filing_supplement']['operating_statement_history']
    row.source_evidence_json=json.dumps(evidence)
    changed=_fundamental_basis(row,{'operating_margin','revenue_growth'})
    assert changed['operating_margin']!=original['operating_margin']
    assert changed['revenue_growth']==original['revenue_growth']

def test_cache_readers_keep_qualified_bridge_and_reject_modified_proof(monkeypatch):
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    from app.models import InsightsSnapshot
    from app.services import sec_fundamentals_preparation as prep,sec_directory
    monkeypatch.setattr(socket.socket,'connect',lambda *a,**k:pytest.fail('Unexpected network'))
    monkeypatch.setenv('SEC_FUNDAMENTALS_WARMING_ENABLED','1')
    monkeypatch.setattr(sec_directory,'directory',lambda:{'NKE':{'cik':CIK}})
    class Clock(datetime):
        @classmethod
        def now(cls,tz=None):return NOW
    engine=create_engine('sqlite://');InsightsSnapshot.__table__.create(engine);factory=sessionmaker(bind=engine);client=Client()
    try:
        with patch.object(prep,'SessionLocal',factory),patch.object(prep,'datetime',Clock),patch.object(prep,'DirectSourceClient',lambda:client):
            p=prep.prepare('NKE');assert p['values']['operating_margin_expansion']==project(acquired())['operating_margin_expansion']
            count=len(client.calls);assert prep.prepare('NKE')==p and len(client.calls)==count
        with patch.object(statements,'SessionLocal',factory),patch.object(statements,'datetime',Clock),patch.object(statements,'DirectSourceClient',lambda:client):
            p=statements.prepared('NKE');count=len(client.calls);assert statements.prepared('NKE')==p and len(client.calls)==count
        with factory() as db:
            for key,field,nested in [('sec-current-fundamentals:NKE:v1','calculation_evidence','inline_filing_supplement'),('sec-financials:NKE:v1','sourceEvidence','inlineFilingSupplement')]:
                row=db.get(InsightsSnapshot,key);p=json.loads(row.payload_json)
                p[field][nested]['operating_statement_history']['rows'][0]['expense_bridge']['tax']+=1
                row.payload_json=json.dumps(p)
            db.commit();assert statements.cached_payload(db,'NKE',now=NOW) is None
        with patch.object(prep,'SessionLocal',factory):assert prep._cached('NKE',NOW) is None
    finally:engine.dispose()
