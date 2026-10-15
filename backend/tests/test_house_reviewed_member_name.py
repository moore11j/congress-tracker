"""Reviewed official member aliases must retain term, district and source guards."""
from copy import deepcopy
from datetime import date
import hashlib,json
from pathlib import Path
import pytest
from sqlalchemy import create_engine,select,func
from sqlalchemy.orm import Session
from app.db import Base
from app.models import Event,Transaction,EmailDelivery
from app.services.direct_congress_reconciliation import resolve_direct_member
from app.services.direct_congress_worker import publish_document
from app.services.direct_feed_collection import parse_document
from app.services.direct_feed_store import discover,record_document
from app.services.feed_source_control import select_feed_source

FIXTURES=Path(__file__).with_name('fixtures')
META=json.loads((FIXTURES/'house_allen_2026_10_14.json').read_text(encoding='utf-8'))
DIRECTORY=json.loads((Path(__file__).parents[1]/'config/congress_members_2026_10_08.json').read_text(encoding='utf-8'))

def test_reviewed_name_resolves_only_actual_member():
 result=resolve_direct_member(META,'house',DIRECTORY)
 assert result['status']=='resolved'
 assert result['member']['bioguide_id']=='A000372'
 assert result['member']['state']=='GA'

@pytest.mark.parametrize('change',[{'district':'GA07'},{'district':'FL12'}, {'filing_date':'2014-10-14'}, {'member_name':'Richard Q. Allen'}, {'member_name':'Richard Allen'}, {'member_name':'Richard W. Another'}])
def test_alias_keeps_identity_and_term_guards(change):
 assert resolve_direct_member({**META,**change},'house',DIRECTORY)['status']=='held'

def test_alias_does_not_resolve_ambiguous_directory_or_wrong_chamber():
 assert resolve_direct_member(META,'house',DIRECTORY*2)['status']=='held'
 assert resolve_direct_member({**META,'filer_type':'Senator (Senator)'},'senate',DIRECTORY)['status']=='held'
 modified=deepcopy(DIRECTORY)
 for person in modified:
  if person['id'].get('bioguide')=='A000372':person['id']['bioguide']='A999999'
 assert resolve_direct_member(META,'house',modified)['status']=='held'

def test_complete_official_pdf_publishes_both_lots_and_repeats_without_new_events(monkeypatch):
 def forbidden(*a,**k):raise AssertionError('Source replay attempted network/email')
 monkeypatch.setattr('requests.sessions.Session.request',forbidden)
 monkeypatch.setattr('httpx.Client.send',forbidden)
 raw=(FIXTURES/'house_allen_2026_10_14.pdf').read_bytes()
 assert hashlib.sha256(raw).hexdigest()=='69ae5a2b6ab70b7fa3d68adb3c852343f8963599c43738a3cee6e01b5c5b918e'
 source,parsed,reasons=parse_document('house_ptr',raw,META)
 assert reasons==[] and len(parsed['transactions'])==2
 assert [(r['ticker_normalized'],r['owner_normalized'],str(r['transaction_date']),r['amount_low'],r['amount_high']) for r in parsed['transactions']]==[('AZO','spouse','2026-09-22',15001,50000),('COST','spouse','2026-09-18',1001,15000)]
 engine=create_engine('sqlite:///:memory:');Base.metadata.create_all(engine)
 with Session(engine,autoflush=False) as db:
  select_feed_source(db,feed='house_ptr',provider='official_house',publish_since=date(2026,10,1),expected_generation=0,reason='isolated source replay');db.commit()
  doc=discover(db,'house_ptr',META);record_document(db,doc,raw,source,parsed);db.commit()
  first=publish_document(db,doc.id,directory=DIRECTORY)
  assert first['status']=='published',first
  assert first['inserted_transactions']==first['inserted_events']==2
  before=[(e.id,e.ts,e.event_date,e.payload_json,e.source_filing_id) for e in db.scalars(select(Event).order_by(Event.id))]
  assert publish_document(db,doc.id,directory=DIRECTORY)['inserted_events']==0
  assert before==[(e.id,e.ts,e.event_date,e.payload_json,e.source_filing_id) for e in db.scalars(select(Event).order_by(Event.id))]
  assert db.scalar(select(func.count()).select_from(Transaction))==2
  assert db.scalar(select(func.count()).select_from(EmailDelivery))==0
 engine.dispose()
