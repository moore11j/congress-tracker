"""An unused missing quarter cannot contaminate or block a verified TTM series."""
from datetime import datetime,timezone
from pathlib import Path
from functools import lru_cache
from copy import deepcopy
from unittest.mock import patch
import gzip,hashlib,json,socket
import pytest
from app.services import sec_revenue_series as series,sec_inline_financials as inline,sec_fundamentals as sec,sec_financial_statements as statements

ROOT=Path(__file__).parent/'fixtures/sec_revenue_unused_periods'
MANIFEST=json.loads((ROOT/'sources.json').read_text())
NOW=datetime(2026,10,27,5,34,tzinfo=timezone.utc)
CIK='0001857816'

@lru_cache(maxsize=4)
def raw(name):
 m=MANIFEST[name];value=gzip.decompress((ROOT/m['file']).read_bytes())
 assert hashlib.sha256(value).hexdigest()==m['sha256'] and len(value)==m['bytes']
 return value

def inputs():return json.loads(raw('GCT-facts.json')),json.loads(raw('GCT-company.json'))

class Client:
 issuer_document_urls=frozenset()
 def __init__(self):self.calls=[]
 def get(self,url):
  self.calls.append(url)
  matches=[name for name,m in MANIFEST.items() if m['url']==url]
  assert len(matches)==1,'Unexpected original '+url
  return raw(matches[0])

@lru_cache(maxsize=1)
def acquired():
 client=Client()
 with patch.object(socket.socket,'connect',side_effect=AssertionError('Offline fixture')):
  s=inline.acquire(raw('GCT-facts.json'),raw('GCT-company.json'),symbol='GCT',cik=CIK,observed_at=NOW,client=client)
 assert len(client.calls)==len(set(client.calls))==2
 return s

def project(s):return sec.project_fundamentals(raw('GCT-facts.json'),raw('GCT-company.json'),symbol='GCT',cik=CIK,observed_at=NOW,filing_supplement=s)

def test_complete_originals_restore_five_metrics_without_splicing_or_false_growth():
 before=project(None);s=acquired();after=project(s)
 ev=json.loads(after['source_evidence_json']);old=json.loads(before['source_evidence_json'])
 assert all(before[k]==after[k] for k in old['calculations'])
 assert set(ev['calculations'])-set(old['calculations'])=={'fcf_growth','fcf_margin','gross_margin','net_margin','operating_margin'}
 assert ev['current']['revenue']['value']==1289897000+771131000-594512000
 assert after['operating_margin']==pytest.approx(170800000/1466516000*100)
 assert after.get('revenue_growth') is None and after.get('net_debt_to_ebitda') is None
 legs=ev['current']['revenue']['inputs']
 assert len(legs)==3 and {p['tag'] for p in legs}=={series.CONTRACT}
 assert all(p['original_statement']['row_label']=='total revenues' and p['original_statement']['source_sha256'] for p in legs)
 assert after==project(s)
 proof=s['evidence']['revenue_series_selection']
 assert len(proof['overlap'])>=2 and proof['basis_spliced'] is False
 assert all(p['end']!='2025-09-30' for p in proof['rows'])

def test_unselected_history_is_retained_without_grafting_into_core_ttm():
 s=acquired();without=deepcopy(s);without['evidence'].pop('revenue_series_selection')
 args=dict(symbol='GCT',cik=CIK,observed_at=NOW)
 before=statements.project(raw('GCT-facts.json'),raw('GCT-company.json'),filing_supplement=without,**args)
 after=statements.project(raw('GCT-facts.json'),raw('GCT-company.json'),filing_supplement=s,**args)
 for kind in ('annual','quarterly'):
  rows={p['date']:p for p in after[kind]}
  for point in before[kind]:
   if point['date'] in rows:
    for key,value in point.items():
     if value is not None:assert rows[point['date']][key]==value

@pytest.mark.parametrize('defect',['required_value','required_identity','unused_present_conflict','unused_ambiguous','missing_required','source_total','source_hash'])
def test_real_conflicts_and_missing_required_proof_still_hold(defect):
 facts,company=inputs();client=Client()
 total=facts['facts']['us-gaap'][series.TOTAL]['units']['USD']
 contract=facts['facts']['us-gaap'][series.CONTRACT]['units']['USD']
 if defect in {'required_value','required_identity'}:
  for row in total:
   if row.get('start')=='2026-01-01' and row['end']=='2026-06-30':
    if defect=='required_value':row['val']+=1
    else:row['accn']='0001857816-26-999999'
 elif defect in {'unused_present_conflict','unused_ambiguous'}:
  row=next(p for p in total if p.get('start')=='2025-01-01' and p['end']=='2025-09-30')
  contract.append({**row,'val':row['val']+1})
  if defect=='unused_ambiguous':contract.append(deepcopy(row))
 elif defect=='missing_required':
  contract[:]=[p for p in contract if not(p.get('start')=='2025-01-01' and p['end']=='2025-12-31')]
 else:
  s=deepcopy(acquired());proof=s['evidence']['revenue_series_selection'];p=proof['rows'][0]
  if defect=='source_total':p['original_statement']['row_label']='product revenues'
  else:p['original_statement']['source_sha256']='bad'
  with pytest.raises(ValueError):series.apply({'revenue':{}},facts,company,cik=CIK,available_by=NOW.date(),supplement=s)
  return
 if defect=='missing_required':assert series.acquire(facts,company,cik=CIK,available_by=NOW.date(),client=client) is None
 else:
  with pytest.raises(ValueError,match='total_contract_conflict'):series.acquire(facts,company,cik=CIK,available_by=NOW.date(),client=client)
 assert client.calls==[]

def test_actual_cache_reuse_and_corrupt_source_proof_rejection(monkeypatch):
 from sqlalchemy import create_engine
 from sqlalchemy.orm import sessionmaker
 from app.models import InsightsSnapshot
 from app.services import sec_fundamentals_preparation as prep,sec_directory
 monkeypatch.setattr(socket.socket,'connect',lambda *a,**k:pytest.fail('Unexpected network'))
 monkeypatch.setenv('SEC_FUNDAMENTALS_WARMING_ENABLED','1')
 monkeypatch.setattr(sec_directory,'directory',lambda:{'GCT':{'cik':CIK}})
 class Clock(datetime):
  @classmethod
  def now(cls,tz=None):return NOW
 engine=create_engine('sqlite://');InsightsSnapshot.__table__.create(engine);factory=sessionmaker(bind=engine);client=Client()
 try:
  with patch.object(prep,'SessionLocal',factory),patch.object(prep,'datetime',Clock),patch.object(prep,'DirectSourceClient',lambda:client):
   payload=prep.prepare('GCT');assert payload['values']['operating_margin']==project(acquired())['operating_margin']
   count=len(client.calls);assert prep.prepare('GCT')==payload and len(client.calls)==count
  with patch.object(statements,'SessionLocal',factory),patch.object(statements,'datetime',Clock),patch.object(statements,'DirectSourceClient',lambda:client):
   panel=statements.prepared('GCT');count=len(client.calls);assert statements.prepared('GCT')==panel and len(client.calls)==count
  with factory() as db:
   for key,field in [('sec-current-fundamentals:GCT:v1','calculation_evidence'),('sec-financials:GCT:v1','sourceEvidence')]:
    row=db.get(InsightsSnapshot,key);saved=json.loads(row.payload_json)
    nested='inline_filing_supplement' if field=='calculation_evidence' else 'inlineFilingSupplement'
    saved[field][nested]['revenue_series_selection']['rows'][0]['original_statement']['source_sha256']='invalid'
    row.payload_json=json.dumps(saved)
   db.commit();assert statements.cached_payload(db,'GCT',now=NOW) is None
  with patch.object(prep,'SessionLocal',factory):assert prep._cached('GCT',NOW) is None
 finally:engine.dispose()

def test_actual_source_score_and_watchlist_lifecycle(monkeypatch):
 import test_sec_tagged_foreign_consumers as consumer
 urls={m['url']:raw(n) for n,m in MANIFEST.items()}
 monkeypatch.setattr(consumer,'originals',lambda symbol:(raw('GCT-facts.json'),raw('GCT-company.json'),CIK,urls))
 value=project(acquired());metrics={k:value[k] for k in json.loads(value['source_evidence_json'])['calculations']}
 monkeypatch.setattr(consumer,'EXPECTED',{'GCT':{'metrics':metrics}});monkeypatch.setattr(consumer,'NOW',NOW)
 consumer.test_original_source_score_and_alert_recovery_repeat_crossing_rollback('GCT','operating_margin',monkeypatch)

@pytest.mark.parametrize('change',['hash','value','unit','issuer','accession','context','heading','label','duplicate','missing_leg','splice','overlap','nonfinite','service'])
def test_saved_revenue_evidence_rejects_corruption(change):
 proof=deepcopy(acquired()['evidence']['revenue_series_selection']);p=proof['rows'][0];original=p['original_statement']
 assert series.cached_valid(proof,cik=CIK)
 if change=='hash':original['source_sha256']='invalid'
 elif change=='value':p['value']+=1
 elif change=='unit':p['unit']='EUR'
 elif change=='issuer':original['source_cik']='0000000001'
 elif change=='accession':p['accession']='0000000001-26-000001'
 elif change=='context':original['context_id']=''
 elif change=='heading':original['statement_heading']=['Notes to financial statements']
 elif change=='label':original['row_label']='product revenue'
 elif change=='duplicate':proof['rows'].append(deepcopy(p))
 elif change=='missing_leg':proof['rows']=[r for r in proof['rows'] if r['end']!='2025-12-31']
 elif change=='splice':proof['basis_spliced']=True
 elif change=='overlap':proof['overlap']=[]
 elif change=='nonfinite':p['value']=original['value']=float('nan')
 else:original['row_label']='revenue from services'
 assert not series.cached_valid(proof,cik=CIK)
