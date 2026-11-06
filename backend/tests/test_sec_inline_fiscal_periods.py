"""Complete filing recovery and independent identity/period negative controls."""
from copy import deepcopy
from datetime import date,datetime,timezone
from functools import lru_cache
from pathlib import Path
from unittest.mock import patch
import gzip,hashlib,json,socket
import pytest
from lxml import html
from app.services import sec_primary_income as income,sec_fundamentals as sec
from app.services.sec_inline_fiscal_periods import resolve

ROOT=Path(__file__).parent/'fixtures/sec_income_rollover_nov6'
MANIFEST=json.loads((ROOT/'manifest.json').read_bytes())
NOW=datetime(2026,11,6,23,tzinfo=timezone.utc)
CIK='0000047111'

@lru_cache(maxsize=4)
def raw(name):
 p=MANIFEST[name];value=gzip.decompress((ROOT/p['file']).read_bytes())
 assert len(value)==p['bytes'] and hashlib.sha256(value).hexdigest()==p['sha256']
 return value

def inputs():return json.loads(raw('HSY-facts.json')),json.loads(raw('HSY-company.json'))

class Client:
 def __init__(self):self.calls=[]
 def get(self,url):
  self.calls.append(url);names=[n for n,p in MANIFEST.items() if p['url']==url]
  assert len(names)==1
  return raw(names[0])

@lru_cache(maxsize=1)
def acquired():
 facts,company=inputs();client=Client()
 with patch.object(socket.socket,'connect',side_effect=AssertionError('Offline fixture attempted network')):
  item=income.inline.acquire(raw('HSY-facts.json'),raw('HSY-company.json'),symbol='HSY',cik=CIK,observed_at=NOW,client=client)
 assert len(client.calls)==len(set(client.calls))==2
 return item

def project(item):
 return sec.project_fundamentals(raw('HSY-facts.json'),raw('HSY-company.json'),symbol='HSY',cik=CIK,
  observed_at=NOW,filing_supplement=item)

def test_complete_originals_recover_three_legs_and_preserve_existing_metrics():
 before=project(None);item=acquired();after=project(item)
 proof=item['primary_income']['evidence'];assert len(proof['rows'])==3
 assert {(p['start'],p['end'],p['value']) for p in proof['rows']}=={
  ('2025-01-01','2025-12-31',883259000),('2026-01-01','2026-09-27',1473584000),('2025-01-01','2025-09-28',563242000)}
 assert income.cached_valid(proof,cik=CIK)
 old=json.loads(before['source_evidence_json']);new=json.loads(after['source_evidence_json'])
 assert all(before[k]==after[k] for k in old['calculations'])
 assert {k:v for k,v in before.items() if k!='source_evidence_json'}=={k:v for k,v in after.items() if k!='source_evidence_json'}
 assert before.get('net_debt_to_ebitda') is None and after.get('net_debt_to_ebitda') is None
 assert new['leverage_preparation']['ebitda']['value']==3077817000
 assert new['leverage_preparation']['debt_addition_reconciliations']==old['leverage_preparation']['debt_addition_reconciliations']
 assert new['leverage_preparation']['ebitda']['status']=='components_complete'
 assert after==project(item)

@pytest.mark.parametrize('change',['wrong_owner','subsidiary','printed_period','tax','hidden_total','dimension','wrong_cik'])
def test_current_complete_statement_cannot_be_authorized_by_other_period_or_entity(change):
 facts,company=inputs();expected=income.plan(facts,company,symbol='HSY',cik=CIK,available_by=NOW.date())
 filing=next(r['filing'] for r in expected['rows'] if r['filing']['form']=='10-Q')
 value=raw('HSY-interim.html');tree=html.fromstring(value)
 if change in {'wrong_owner','subsidiary'}:
  value=value.replace(b'THE HERSHEY COMPANY',b'OTHER COMPANY' if change=='wrong_owner' else b'THE HERSHEY COMPANY SUBSIDIARY')
 elif change=='printed_period':
  for node in tree.iter():
   if node.text and 'September 27, 2026' in node.text:node.text=node.text.replace('September 27, 2026','September 26, 2026')
  value=html.tostring(tree)
 elif change=='tax':value=value.replace(b'475,394',b'475,395')
 elif change=='wrong_cik':filing={**filing,'cik':'0000000001'}
 else:
  if change=='hidden_total':
   nodes=[n for n in income.inline.nodes(tree,'nonfraction') if n.get('name')=='us-gaap:NetIncomeLoss']
   for n in nodes:
    parent=n.getparent();pos=parent.index(n);parent.remove(n);hidden=html.Element('ix:hidden');hidden.append(n);parent.insert(pos,hidden)
  elif change=='dimension':
   for context in income.inline.nodes(tree,'context'):
    context.append(html.Element('xbrli:scenario'))
  value=html.tostring(tree)
 assert value!=raw('HSY-interim.html') or change=='wrong_cik'
 try:rows=income.statement_rows(value,filing,company['name'])
 except ValueError:rows={}
 assert not any((r['start'],r['end'])==('2026-01-01','2026-09-27') for r in rows.values())

@pytest.mark.parametrize('header,periods',[
 ('Nine months ended September 27, 2026',[('2026-01-01','2026-09-27'),('2026-01-02','2026-09-27')]),
 ('Six months ended September 27, 2026',[('2026-01-01','2026-09-27')]),
 ('Nine months ended September 27, 2030',[('2030-01-01','2030-09-27')]),
 ('Nine months ended September 27, 2026 and September 28, 2025',[('2026-01-01','2026-09-27')]),
 ('Nine months ended September 27, 2026',[]),
 ('Nine months ended September 27, 2026',[('2026-01-01','2026-09-26')]),
 ('Three years ended September 27, 2026',[('2026-01-01','2026-09-27')]),
 ])
def test_conflicting_or_unproved_fiscal_columns_hold(header,periods):
 with pytest.raises(ValueError):resolve(header,'income',NOW.date(),periods)

def test_untagged_reader_keeps_noncalendar_period_held():
 with pytest.raises(ValueError):income.tables.period_for('Nine months ended September 27, 2026','income',NOW.date())
 assert resolve('Nine months ended September 27, 2026','income',NOW.date(),[('2026-01-01','2026-09-27')])==(date(2026,1,1),date(2026,9,27),9)

def test_terminal_company_abbreviation_does_not_strip_other_identity():
 key=income.tables.company_key
 assert key('THE HERSHEY COMPANY')==key('HERSHEY CO')
 assert key('THE HERSHEY COMPANY and its subsidiaries')==key('HERSHEY CO')
 assert key('HERSHEY COMPANY SUBSIDIARY')!=key('HERSHEY CO')
 assert key('Company Holdings')!=key('Co Holdings')
