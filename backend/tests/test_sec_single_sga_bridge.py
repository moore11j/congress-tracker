import copy,gzip,hashlib,json
from pathlib import Path
import pytest
from lxml import html
from app.services.sec_single_sga_bridge import _qualify_original as qualify,inline,TAGS
B=Path(__file__).resolve().parent/'fixtures/sec_single_sga_bridge'
MANIFEST=json.loads((B/'manifest.json').read_bytes())
COMPANY=json.loads(gzip.decompress((B/'BAER-company.gz').read_bytes()))
def source(role):
    s=next(x for x in MANIFEST['rows'] if x['symbol']=='BAER' and x['kind']==role)
    return gzip.decompress((B/s['file']).read_bytes()),s

def test_all_complete_statements_reconcile_and_repeat():
    proofs={}
    for role,count in [('current',4),('annual',2),('prior',4)]:
        raw,s=source(role);rows=qualify(raw,s,COMPANY)
        assert len(rows)==count and rows==qualify(raw,s,COMPANY)
        proofs[role]={(r['start'],r['end']):r['operating_income'] for r in rows}
    assert proofs['annual'][('2025-01-01','2025-12-31')]+proofs['current'][('2026-01-01','2026-09-30')]-proofs['prior'][('2025-01-01','2025-09-30')]==4623000
    assert proofs['annual'][('2024-01-01','2024-12-31')]+proofs['prior'][('2025-01-01','2025-09-30')]-proofs['prior'][('2024-01-01','2024-09-30')]==26923000

@pytest.mark.parametrize('change',['amount','missing_sga','hidden','wrong_period','wrong_unit','unknown_tag','wrong_label','extra_amount','wrong_issuer','hash'])
def test_invalid_complete_originals_cannot_qualify(change):
    raw,s=source('current');s=copy.deepcopy(s);company=copy.deepcopy(COMPANY)
    tree=html.fromstring(raw)
    n=next(n for n in inline.nodes(tree,'nonfraction') if n.get('name')=='us-gaap:SellingGeneralAndAdministrativeExpense' and n.xpath('ancestor::table'))
    if change=='amount':n.text='9,999'
    elif change=='missing_sga':n.attrib.pop('name')
    elif change=='hidden':n.set('style','display:none')
    elif change=='wrong_period':
        refs=[x.get('contextref') for x in inline.nodes(tree,'nonfraction') if x.get('name')=='us-gaap:SellingGeneralAndAdministrativeExpense' and x.get('contextref')!=n.get('contextref')];n.set('contextref',refs[0])
    elif change=='wrong_unit':n.set('unitref','unreviewed')
    elif change=='unknown_tag':n.set('name','us-gaap:ResearchAndDevelopmentExpense')
    elif change=='wrong_label':
        row=n.xpath('ancestor::tr')[-1];cell=row.xpath('./td|./th')[0];cell.clear();cell.text='Unreviewed operating adjustment'
    elif change=='extra_amount':
        row=n.xpath('ancestor::tr')[-1];extra=copy.deepcopy(row)
        for tag in inline.nodes(extra,'nonfraction'):tag.tag='span';tag.attrib.clear()
        row.addnext(extra)
    elif change=='wrong_issuer':company['name']='Another Company'
    modified=html.tostring(tree,encoding='utf-8');s['sha256']=hashlib.sha256(modified).hexdigest()
    if change=='hash':s['sha256']='0'*64
    with pytest.raises(ValueError):qualify(modified,s,company)


from datetime import datetime,date,timezone
from functools import lru_cache
from unittest.mock import patch
import socket
from app.services import sec_operating_history as op,sec_fundamentals as sec,sec_inline_financials as supplement,sec_financial_statements as statements
NOW=datetime(2026,11,25,20,0,tzinfo=timezone.utc)
CIK='0001941536'
def checked_original(kind):
    item=next(x for x in MANIFEST['rows'] if x['kind']==kind)
    value=gzip.decompress((B/item['file']).read_bytes())
    assert len(value)==item['bytes'] and hashlib.sha256(value).hexdigest()==item['sha256']
    return value
FACTS=checked_original('facts')
DIRECTORY=checked_original('company')
class Client:
    def __init__(self):self.calls=[]
    def get(self,url):
        self.calls.append(url)
        s=next(x for x in MANIFEST['rows'] if x['url']==url)
        raw=gzip.decompress((B/s['file']).read_bytes())
        assert hashlib.sha256(raw).hexdigest()==s['sha256']
        return raw
@lru_cache(maxsize=1)
def acquired():
    with patch.object(socket.socket,'connect',side_effect=AssertionError('Unexpected network')):
        client=Client();item=supplement.acquire(FACTS,DIRECTORY,symbol='BAER',cik=CIK,observed_at=NOW,client=client)
        assert len(client.calls)==len(set(client.calls))==2
        return item

def project(item):return sec.project_fundamentals(FACTS,DIRECTORY,symbol='BAER',cik=CIK,observed_at=NOW,filing_supplement=item)

def test_full_acquisition_restores_only_missing_margin_inputs():
    before=project(None);item=acquired();after=project(item)
    assert before.get('operating_margin') is None
    assert after['operating_margin']==4623000/123966000*100
    assert after['operating_margin_expansion']==after['operating_margin']-26923000/129868000*100
    old=json.loads(before['source_evidence_json'])
    assert all(after[k]==before[k] for k in old['calculations'])
    assert project(item)==after
    proof=item['evidence']['operating_statement_history'];assert op.cached_valid(proof,cik=CIK)
    assert len(proof['rows'])==2
    records={k:sec._records(json.loads(FACTS),k,date(2026,11,24)) for k in ['revenue','operating_income']}
    restored=op.apply(records,json.loads(FACTS),json.loads(DIRECTORY),cik=CIK,available_by=date(2026,11,24),supplement=item)
    assert all(restored['operating_income'][k]==v for k,v in records['operating_income'].items())
    panel=statements.project(FACTS,DIRECTORY,symbol='BAER',cik=CIK,observed_at=NOW,filing_supplement=item)
    assert panel['sourceEvidence']['inlineFilingSupplement']['operating_statement_history']==proof

@pytest.mark.parametrize('change',['amount','display','order','method','label','context','source','period','bool','duplicate','missing'])
def test_cached_bridge_tampering_is_rejected(change):
    proof=copy.deepcopy(acquired()['evidence']['operating_statement_history']);row=proof['rows'][0];part=row['expense_bridge']['components'][1]
    if change=='amount':part['value']+=1
    elif change=='display':part['display']+=1
    elif change=='order':part['row']=0
    elif change=='method':row['expense_bridge']['methodology']='other'
    elif change=='label':part['label']='unreviewed'
    elif change=='context':part['context']=''
    elif change=='source':row['source_sha256']='bad'
    elif change=='period':row['end']='2030-01-01'
    elif change=='bool':part['value']=True
    elif change=='duplicate':proof['rows'].append(copy.deepcopy(row))
    else:row['expense_bridge']['components'].pop()
    assert not op.cached_valid(proof,cik=CIK)


def test_source_score_and_operating_alert_lifecycle(monkeypatch):
    import test_sec_tagged_foreign_consumers as consumer
    urls={m['url']:gzip.decompress((B/m['file']).read_bytes()) for m in MANIFEST['rows']}
    monkeypatch.setattr(consumer,'originals',lambda symbol:(FACTS,DIRECTORY,CIK,urls))
    values=project(acquired());metrics={k:values[k] for k in json.loads(values['source_evidence_json'])['calculations']}
    monkeypatch.setattr(consumer,'EXPECTED',{'BAER':{'metrics':metrics}});monkeypatch.setattr(consumer,'NOW',NOW)
    consumer.test_original_source_score_and_alert_recovery_repeat_crossing_rollback('BAER','operating_margin',monkeypatch)


@pytest.mark.parametrize('change',['missing_other','expense_conflict','different_accession','negative_expense','pretax_conflict'])
def test_full_upstream_component_conflicts_do_not_create_a_candidate(change):
    facts=json.loads(FACTS);company=json.loads(DIRECTORY)
    target=op.plan(facts,company,cik=CIK,available_by=date(2026,11,24))['rows'][-1]['gross']
    if change=='missing_other':del facts['facts']['us-gaap'][TAGS[4]]
    else:
        tag=TAGS[5] if change=='pretax_conflict' else TAGS[1]
        rows=facts['facts']['us-gaap'][tag]['units']['USD']
        selected=[r for r in rows if r.get('start')==target['start'] and r['end']==target['end'] and r['filed']==target['filed']]
        assert selected
        if change in ('expense_conflict','pretax_conflict'):rows.append({**selected[0],'val':selected[0]['val']+1})
        elif change=='different_accession':
            for r in selected:r['accn']='0000000001-26-000001'
        else:
            for r in selected:r['val']=-1
    result=op.plan(facts,company,cik=CIK,available_by=date(2026,11,24))
    assert not result or all(r['gross']['start']!=target['start'] or r['gross']['end']!=target['end'] for r in result['rows'])


def test_cache_readers_preserve_proof_and_avoid_refetch(monkeypatch):
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    from app.models import InsightsSnapshot
    from app.services import sec_fundamentals_preparation as prep,sec_directory
    monkeypatch.setattr(socket.socket,'connect',lambda *a,**k:pytest.fail('Unexpected network'))
    monkeypatch.setenv('SEC_FUNDAMENTALS_WARMING_ENABLED','1')
    monkeypatch.setattr(sec_directory,'directory',lambda:{'BAER':{'cik':CIK}})
    class Clock(datetime):
        @classmethod
        def now(cls,tz=None):return NOW
    engine=create_engine('sqlite://');InsightsSnapshot.__table__.create(engine);factory=sessionmaker(bind=engine);client=Client()
    try:
        with patch.object(prep,'SessionLocal',factory),patch.object(prep,'datetime',Clock),patch.object(prep,'DirectSourceClient',lambda:client):
            payload=prep.prepare('BAER');assert payload['values']['operating_margin_expansion']==project(acquired())['operating_margin_expansion']
            count=len(client.calls);assert prep.prepare('BAER')==payload and len(client.calls)==count
        with patch.object(statements,'SessionLocal',factory),patch.object(statements,'datetime',Clock),patch.object(statements,'DirectSourceClient',lambda:client):
            panel=statements.prepared('BAER');count=len(client.calls);assert statements.prepared('BAER')==panel and len(client.calls)==count
        with factory() as db:
            for key,field in [('sec-current-fundamentals:BAER:v1','calculation_evidence'),('sec-financials:BAER:v1','sourceEvidence')]:
                row=db.get(InsightsSnapshot,key);saved=json.loads(row.payload_json)
                nested='inline_filing_supplement' if field=='calculation_evidence' else 'inlineFilingSupplement'
                saved[field][nested]['operating_statement_history']['rows'][0]['value']+=1
                row.payload_json=json.dumps(saved)
            db.commit();assert statements.cached_payload(db,'BAER',now=NOW) is None
        with patch.object(prep,'SessionLocal',factory):assert prep._cached('BAER',NOW) is None
    finally:engine.dispose()

