import hashlib
import json
from pathlib import Path

import pytest
from sqlalchemy import select, func

from app.models import Event, Filing, Member, Transaction, EmailDelivery
from app.services.direct_congress_worker import publish_document, CongressRowBinding
from app.services.direct_feed_store import discover, record_document
from app.services.direct_feed_collection import parse_document
from app.services.feed_source_control import select_feed_source
from app.services.direct_congress_repair import _record, _digest
from test_direct_congress_worker import db, counts
from test_direct_congress_repair import load_rows
from datetime import date

FIXTURES=Path(__file__).with_name('fixtures')
DIRECTORY=json.loads((FIXTURES.parent.parent/'config/congress_members_2026_10_08.json').read_bytes())


def stage_treasury(db):
    case=json.loads((FIXTURES/'house_fong_treasury.json').read_bytes())
    raw=(FIXTURES/'house_20035539.pdf').read_bytes()
    assert hashlib.sha256(raw).hexdigest()==case['sha256']
    for key,model in [('members',Member),('filings',Filing),('transactions',Transaction),('events',Event)]:
        load_rows(db,model,case[key])
    document=discover(db,'house_ptr',case['metadata'])
    text,parsed,reasons=parse_document('house_ptr',raw,case['metadata'])
    assert not reasons and parsed['transactions'][0]['asset_type_normalized']=='treasury_bill'
    record_document(db,document,raw,text,parsed);db.commit()
    select_feed_source(db,feed='house_ptr',provider='official_house',publish_since=date(2026,10,1),expected_generation=0,reason='Isolated Treasury adoption test');db.commit()
    return document.id


def test_real_existing_treasury_is_adopted_with_no_equity_or_history_rewrite(db):
    identifier=stage_treasury(db)
    before=_digest([_record(r) for r in db.scalars(select(Event))])
    result=publish_document(db,identifier,directory=DIRECTORY)
    assert result['status']=='existing' and result['non_stock_rows']==1
    assert result['inserted_events']==result['inserted_transactions']==0
    assert result['event_ids']==[472403]
    assert db.scalar(select(CongressRowBinding)).event_id==472403
    assert before==_digest([_record(r) for r in db.scalars(select(Event))])
    baseline=counts(db)
    assert publish_document(db,identifier,directory=DIRECTORY)['status']=='existing'
    assert baseline==counts(db)
    assert db.scalar(select(func.count()).select_from(Event).where(Event.event_type=='congress_trade'))==0
    assert db.scalar(select(func.count()).select_from(EmailDelivery))==0


@pytest.mark.parametrize('fault',['equity_type','symbol','member','amount','description','instrument','source_url','security','date','owner'])
def test_existing_non_stock_event_conflicts_remain_held(db,fault):
    identifier=stage_treasury(db);event=db.scalar(select(Event));payload=json.loads(event.payload_json)
    if fault=='equity_type':event.event_type='congress_trade'
    elif fault=='symbol':event.symbol='BIL'
    elif fault=='member':event.member_bioguide_id='WRONG'
    elif fault=='amount':event.amount_max=100000
    elif fault=='description':payload['security_description']='Different Treasury instrument'
    elif fault=='instrument':payload['instrument_type']='corporate_bond'
    elif fault=='source_url':payload['document_url']='https://example.com/not-the-source'
    elif fault=='security':payload['security_id']=99
    elif fault=='date':payload['report_date']='2026-10-06'
    else:payload['owner_type']='spouse'
    event.payload_json=json.dumps(payload);db.commit()
    result=publish_document(db,identifier,directory=DIRECTORY)
    assert result['status']=='held'
    assert db.scalar(select(func.count()).select_from(CongressRowBinding))==0
    assert db.scalar(select(func.count()).select_from(Event))==1
