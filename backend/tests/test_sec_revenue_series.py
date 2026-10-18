from copy import deepcopy
from datetime import date,datetime,timezone
from functools import lru_cache
import gzip,hashlib,json,socket,re
from pathlib import Path
from unittest.mock import patch
import pytest
from app.services import sec_revenue_series as series,sec_fundamentals as sec,sec_inline_financials as inline

ROOT=Path(__file__).parent/'fixtures/sec_revenue_series'
NOW=datetime(2026,10,18,3,45,tzinfo=timezone.utc)
AVAILABLE=date(2026,10,16)
REVENUES={'KELYA':4063100000,'KGS':1392494000,'WAT':4644286000}
MANIFEST=json.loads((ROOT/'sources.json').read_text())

@lru_cache(maxsize=18)
def raw(name):
 value=gzip.decompress((ROOT/(name+'.gz')).read_bytes())
 assert hashlib.sha256(value).hexdigest()==MANIFEST[name]['sha256']
 return value

def sources(symbol):
 return json.loads(raw(symbol+'-facts.json')),json.loads(raw(symbol+'-company.json'))

class Client:
 issuer_document_urls=frozenset()
 def __init__(self):self.calls=[]
 def get(self,url):
  self.calls.append(url)
  matches=[name for name,m in MANIFEST.items() if m['url']==url]
  assert len(matches)==1,'Unexpected original: '+url
  return raw(matches[0])

@lru_cache(maxsize=3)
def proof_for(symbol):
 facts,company=sources(symbol)
 with patch.object(socket.socket,'connect',side_effect=AssertionError('Unexpected network')):
  return series.acquire(facts,company,cik=str(company['cik']).zfill(10),available_by=AVAILABLE,client=Client())

@pytest.mark.parametrize('symbol',REVENUES)
def test_complete_original_totals_and_actual_reader_projection(symbol):
 facts,company=sources(symbol);cik=str(company['cik']).zfill(10);client=Client()
 with patch.object(socket.socket,'connect',side_effect=AssertionError('Unexpected network')):
  before=sec.project_fundamentals(raw(symbol+'-facts.json'),raw(symbol+'-company.json'),symbol=symbol,cik=cik,observed_at=NOW)
  supplement=inline.acquire(raw(symbol+'-facts.json'),raw(symbol+'-company.json'),symbol=symbol,cik=cik,observed_at=NOW,client=client)
  assert 'revenue_series_hold' not in supplement['evidence']
  assert len(client.calls)==len(set(client.calls))<=6
  result=sec.project_fundamentals(raw(symbol+'-facts.json'),raw(symbol+'-company.json'),symbol=symbol,cik=cik,observed_at=NOW,filing_supplement=supplement)
  evidence=json.loads(result['source_evidence_json'])
  assert evidence['current']['revenue']['value']==REVENUES[symbol]
  assert {p['tag'] for p in evidence['current']['revenue']['inputs']}=={series.CONTRACT}
  assert all(p['original_statement']['source_sha256'] and not p['revenue_selection']['basis_spliced'] for p in evidence['current']['revenue']['inputs'])
  for field in ('current_ratio','roe','free_cash_flow'):
   if field in before:assert result[field]==before[field]
  assert result['operating_margin']==pytest.approx(evidence['current']['operating_income']['value']/REVENUES[symbol]*100)
  assert sec.project_fundamentals(raw(symbol+'-facts.json'),raw(symbol+'-company.json'),symbol=symbol,cik=cik,observed_at=NOW,filing_supplement=supplement)==result
  from app.services import sec_financial_statements as statements
  panel=statements.project(raw(symbol+'-facts.json'),raw(symbol+'-company.json'),symbol=symbol,cik=cik,observed_at=NOW,filing_supplement=supplement)
  assert panel['status']=='partial' and panel['annual'] and panel['quarterly']
  original_supplement=deepcopy(supplement)
  original_supplement['evidence'].pop('revenue_series_selection')
  old_panel=statements.project(raw(symbol+'-facts.json'),raw(symbol+'-company.json'),symbol=symbol,cik=cik,observed_at=NOW,filing_supplement=original_supplement)
  for kind,limit in [('annual',6),('quarterly',12)]:
   current_rows={r['date']:r for r in panel[kind]}
   expected_dates=sorted({r['date'] for r in old_panel[kind]}|set(current_rows))[-limit:]
   assert sorted(current_rows)==expected_dates
   for row in old_panel[kind]:
    if row['date'] in current_rows:
     assert current_rows[row['date']]==row
  if symbol=='KELYA':
   assert all(p['original_statement']['service_total_reconciliation']['cost_of_services']>=0 for p in proof_for(symbol)['rows'])

def test_conflicting_actual_total_is_held_without_original_requests():
 facts,company=sources('GCT');client=Client()
 with pytest.raises(ValueError,match='total_contract_conflict'):
  series.acquire(facts,company,cik=str(company['cik']).zfill(10),available_by=AVAILABLE,client=client)
 assert client.calls==[]

@pytest.mark.parametrize('defect',['wrong_cik','current_conflict','annual_missing','different_accession','newer_filing','complete_total'])
def test_no_selection_from_incompatible_or_unnecessary_concept(defect):
 facts,company=sources('KGS');cik=str(company['cik']).zfill(10)
 rows=facts['facts']['us-gaap'][series.CONTRACT]['units']['USD']
 if defect=='wrong_cik':company['cik']=1
 elif defect=='current_conflict':
  point=next(r for r in rows if r['end']=='2026-06-30' and r.get('start')=='2026-01-01');rows.append({**point,'val':point['val']+1})
 elif defect=='annual_missing':
  rows[:]=[r for r in rows if not(r.get('start')=='2025-01-01' and r['end']=='2025-12-31')]
 elif defect=='different_accession':
  for p in rows:
   if p['end']=='2026-06-30':p['accn']='0001767042-26-999999'
 elif defect=='newer_filing':
  recent=company['filings']['recent'];i=recent['form'].index('10-Q')
  recent['reportDate'][i]='2026-09-30';recent['filingDate'][i]='2026-10-15'
 else:facts['facts']['us-gaap'][series.TOTAL]['units']['USD']=deepcopy(rows)
 client=Client()
 if defect=='wrong_cik':
  with pytest.raises(ValueError,match='identity'):series.acquire(facts,company,cik=cik,available_by=AVAILABLE,client=client)
 else:assert series.acquire(facts,company,cik=cik,available_by=AVAILABLE,client=client) is None
 assert client.calls==[]

@pytest.mark.parametrize('defect',['cik','period','version','splice','concept','overlap','value','source_url','source_hash','source_period','source_cik','row_label','statement_heading','missing_period','duplicate'])
def test_saved_proof_cannot_be_reassigned_or_weakened(defect):
 facts,company=sources('KGS');cik=str(company['cik']).zfill(10);proof=deepcopy(proof_for('KGS'))
 p=proof['rows'][0]
 if defect=='cik':proof['cik']='0000000001'
 elif defect=='period':proof['period_end']='2026-03-31'
 elif defect=='version':proof['methodology']='unknown'
 elif defect=='splice':proof['basis_spliced']=True
 elif defect=='concept':proof['selected_concept']=series.TOTAL
 elif defect=='overlap':proof['overlap']=[]
 elif defect=='value':p['value']+=1
 elif defect=='source_url':p['original_statement']['source_url']='https://example.com/unrelated'
 elif defect=='source_hash':p['original_statement']['source_sha256']=''
 elif defect=='source_period':p['original_statement']['start']='2026-01-01'
 elif defect=='source_cik':p['original_statement']['source_cik']='0000000001'
 elif defect=='row_label':p['original_statement']['row_label']='product revenue'
 elif defect=='statement_heading':p['original_statement']['statement_heading']=['See our consolidated statements of operations for further details.']
 elif defect=='missing_period':proof['rows']=[]
 else:proof['rows'].append(deepcopy(p))
 with pytest.raises(ValueError,match='revenue_series_'):
  series.apply({'revenue':{}},facts,company,cik=cik,available_by=AVAILABLE,supplement={'evidence':{'revenue_series_selection':proof}})

def test_service_total_requires_the_independent_gross_profit_equation():
 facts,company=sources('KELYA');proof=deepcopy(proof_for('KELYA'))
 proof['rows'][0]['original_statement']['service_total_reconciliation']['cost_of_services']+=1
 with pytest.raises(ValueError,match='service_total_mismatch'):
  series.apply({'revenue':{}},facts,company,cik=str(company['cik']).zfill(10),available_by=AVAILABLE,supplement={'evidence':{'revenue_series_selection':proof}})

@pytest.mark.parametrize('defect',['heading','label','dimensions','hidden'])
def test_original_statement_scope_is_required(defect):
 name='KGS-0001767042-26-000012.htm';meta=MANIFEST[name]
 filing={k:meta[k] for k in ('cik','accession','form','filed','report_date','url')}
 text=raw(name).decode()
 if defect=='heading':
  from lxml import html
  tree=html.fromstring(text.encode());changes=0
  for node in tree.iter():
   if node.text:
    updated,count=re.subn(r'CONSOLIDATED\s+STATEMENTS\s+OF\s+OPERATIONS','NOTES TO CONSOLIDATED FINANCIAL STATEMENTS',node.text)
    node.text=updated;changes+=count
  assert changes>0
  text=html.tostring(tree).decode()
 elif defect=='label':text=text.replace('Total revenues','Product revenues')
 elif defect=='dimensions':text=text.replace('contextRef="c-1"','contextRef="nonexistent"').replace('contextRef="c-2"','contextRef="nonexistent"').replace('contextRef="c-3"','contextRef="nonexistent"')
 else:
  from lxml import html
  tree=html.fromstring(text.encode())
  for table in tree.xpath('//table'):
   if 'Total revenues' in table.text_content():
    wrapper=html.Element('div');wrapper.tag='ix:hidden';table.addprevious(wrapper);wrapper.append(table)
  text=html.tostring(tree).decode()
 if defect=='dimensions':
  # Inline identity validation may itself reject the intentionally broken document.
  try:rows=series.statement_rows(text.encode(),filing)
  except ValueError:return
  assert ('2025-01-01','2025-12-31') not in rows
 else:assert series.statement_rows(text.encode(),filing)=={}


@pytest.mark.parametrize('symbol',REVENUES)
def test_ordinary_preparation_contract_and_cache_repeat(symbol,monkeypatch):
 from sqlalchemy import create_engine,select,func
 from sqlalchemy.orm import sessionmaker
 from app.db import Base
 from app.models import FundamentalsCache,Event,EmailDelivery
 from app.services import sec_fundamentals_preparation as prep
 facts,company=sources(symbol);cik=str(company['cik']).zfill(10)
 engine=create_engine('sqlite://');Base.metadata.create_all(engine);factory=sessionmaker(bind=engine)
 client=Client()
 class Clock(datetime):
  @classmethod
  def now(cls,tz=None):return NOW
 monkeypatch.setenv('SEC_FUNDAMENTALS_WARMING_ENABLED','1')
 with patch.object(socket.socket,'connect',side_effect=AssertionError('Unexpected network')),patch.object(prep,'SessionLocal',factory),patch.object(prep,'datetime',Clock),patch.object(prep,'DirectSourceClient',return_value=client),patch.object(prep.sec_directory,'directory',return_value={symbol:{'cik':cik}}):
  result=prep.prepare(symbol)
  assert result['status']=='partial' and 'operating_margin' in result['values']
  assert result['calculation_evidence']['current']['revenue']['value']==REVENUES[symbol]
  calls=len(client.calls)
  assert prep.prepare(symbol)==result and len(client.calls)==calls
  with factory() as db:
   for model in (FundamentalsCache,Event,EmailDelivery):assert db.scalar(select(func.count()).select_from(model))==0
 engine.dispose()


def test_preparsed_original_must_match_bytes_and_identity():
 name='KGS-0001767042-26-000012.htm';meta=MANIFEST[name]
 filing={k:meta[k] for k in ('cik','accession','form','filed','report_date','url')}
 parsed=inline.parse(raw(name),**filing)
 with pytest.raises(ValueError,match='original_identity'):
  series.statement_rows(raw(name)+b' ',filing,parsed)
 altered={**filing,'accession':'0001767042-26-999999'}
 with pytest.raises(ValueError,match='original_identity'):
  series.statement_rows(raw(name),altered,parsed)


@pytest.mark.parametrize('tag',[series.TOTAL,series.CONTRACT])
def test_explicit_nil_cannot_be_filled_by_an_alternate_revenue_concept(tag):
 facts,company=sources('KGS');proof=proof_for('KGS')
 with pytest.raises(ValueError,match='nil_revenue'):
  series.apply({'revenue':{}},facts,company,cik=str(company['cik']).zfill(10),available_by=AVAILABLE,
   supplement={'evidence':{'revenue_series_selection':proof,'nil_facts':[{'tag':tag,'end':'2026-06-30'}]}})


def test_missing_original_preserves_independent_metrics_without_selecting_revenue():
 from app.clients.direct_sources import DirectSourceError
 symbol='KGS';facts,company=sources(symbol);cik=str(company['cik']).zfill(10)
 class MissingOriginal(Client):
  def get(self,url):
   if '000176704226000012' in url:
    self.calls.append(url);raise DirectSourceError('Original unavailable')
   return super().get(url)
 client=MissingOriginal()
 supplement=inline.acquire(raw(symbol+'-facts.json'),raw(symbol+'-company.json'),symbol=symbol,cik=cik,observed_at=NOW,client=client)
 assert 'revenue_series_selection' not in supplement['evidence']
 assert 'Original unavailable' in supplement['evidence']['revenue_series_hold']
 assert len(client.calls)==len(set(client.calls))<=6
 result=sec.project_fundamentals(raw(symbol+'-facts.json'),raw(symbol+'-company.json'),symbol=symbol,cik=cik,observed_at=NOW,filing_supplement=supplement)
 assert result['current_ratio']>0 and 'revenue_growth' not in result and 'operating_margin' not in result
