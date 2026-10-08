import copy
from datetime import date, datetime, timezone
import hashlib
import json
from pathlib import Path

import pytest
from sqlalchemy import create_engine, select, func
from sqlalchemy.orm import Session

from app.db import Base
from app.models import (Event, Filing, Transaction, Member, Security, TradeOutcome,
    MonitoringAlert, ReplicatedPortfolioPosition, EmailDelivery, DataEnrichmentJob)
from app.services import direct_congress_dates as dates
from app.services.direct_congress_repair import _record, _digest, filing_state, CongressRepairArchive
from app.services.backtesting.queries import event_entry_date
from app.services.replicated_portfolios import _event_public_date, _event_context_by_id
from app.services.direct_feed_collection import parse_document
from app.services.direct_congress_reconciliation import resolve_direct_member
from test_direct_congress_repair import load_rows, add_pnl_job

FIXTURES = Path(__file__).with_name('fixtures')
CASES = json.loads((FIXTURES/'congress_date_corrections.json').read_bytes())['cases']
DIRECTORY = json.loads((Path(__file__).resolve().parents[1]/'config/congress_members_2026_10_08.json').read_bytes())


@pytest.fixture(params=CASES, ids=lambda c:c['metadata']['filing_id'])
def setup(request, monkeypatch):
    case = copy.deepcopy(request.param)
    def forbidden(*args, **kwargs):
        pytest.fail('Date correction attempted network or email')
    monkeypatch.setattr('requests.sessions.Session.request', forbidden)
    monkeypatch.setattr('httpx.Client.send', forbidden)
    engine = create_engine('sqlite:///:memory:')
    Base.metadata.create_all(engine)
    with Session(engine, autoflush=False) as db:
        for key, model in [('members',Member),('securities',Security),('filings',Filing),('transactions',Transaction),('events',Event)]:
            load_rows(db,model,case['state'][key])
        db.commit()
        raw = (FIXTURES/case['source_file']).read_bytes()
        assert hashlib.sha256(raw).hexdigest() == case['sha256']
        document = dict(raw=raw, content_hash=case['sha256'], feed='house_ptr',metadata=case['metadata'])
        yield db, document, case
    engine.dispose()


def test_real_date_repair_preserves_ids_availability_history_alerts_jobs_and_repeat(setup):
    db, document, case = setup
    first = db.get(Event,case['state']['events'][0]['id'])
    db.add(TradeOutcome(event_id=first.id,return_pct=12.5,symbol=first.symbol))
    position = ReplicatedPortfolioPosition(run_id=1,source_event_id=first.id,symbol=first.symbol,
        status='closed',entry_price=100,exit_price=120,return_pct=20,source_reason='saved history')
    db.add(position)
    db.add(MonitoringAlert(user_id=1,source_type='member',source_id='isolated',source_name='Test',
        event_id=first.id,alert_type='trade',title='Previously delivered snapshot',
        event_created_at=first.created_at,read_at=datetime(2026,10,8,tzinfo=timezone.utc)))
    db.commit()
    add_pnl_job(db,first.id)
    protected_models=(TradeOutcome,MonitoringAlert,ReplicatedPortfolioPosition,DataEnrichmentJob)
    protected={m.__tablename__:_digest([_record(r) for r in db.scalars(select(m))]) for m in protected_models}
    before=filing_state(db,document['metadata']['url'])
    context_before=_event_context_by_id(db,[position])
    prior={r.id:(r.ts,r.created_at,event_entry_date(r,json.loads(r.payload_json))) for r in db.scalars(select(Event))}
    plan=dates.inspect_date_correction(db,document,DIRECTORY)
    assert plan['status']=='planned'
    assert filing_state(db,document['metadata']['url'])==before
    result=dates.rehearse_date_correction(db,document,DIRECTORY,expected_before_hash=plan['before_hash'])
    assert result['status']=='rehearsed' and result['updated_events']==len(prior)
    db.commit()
    assert _event_context_by_id(db,[position])==context_before
    for row in db.scalars(select(Event)):
        payload=json.loads(row.payload_json)
        ts,created,entry=prior[row.id]
        assert (row.ts,row.created_at)==(ts,created)
        assert event_entry_date(row,payload)==_event_public_date(row,payload)==entry
        assert row.event_date.date().isoformat()==payload['filing_date']==payload['report_date']==document['metadata']['filing_date']
        assert payload['source_date_correction']['source_sha256']==document['content_hash']
    assert db.scalar(select(func.count()).select_from(CongressRepairArchive))==1+2*len(prior)
    for m in protected_models:
        assert protected[m.__tablename__]==_digest([_record(r) for r in db.scalars(select(m))])
    assert db.scalar(select(func.count()).select_from(EmailDelivery))==0
    after=filing_state(db,document['metadata']['url'])
    assert dates.rehearse_date_correction(db,document,DIRECTORY,expected_before_hash=plan['before_hash'])['status']=='existing'
    assert filing_state(db,document['metadata']['url'])==after


@pytest.mark.parametrize('fault',['amount','extra_event','mixed_date','created_date','existing_availability','wrong_member'])
def test_non_date_conflicts_stay_held(setup,fault):
    db, document, case=setup
    state=filing_state(db,document['metadata']['url'])
    _,parsed,holds=parse_document('house_ptr',document['raw'],document['metadata'])
    assert not holds
    member=resolve_direct_member(document['metadata'],'house',DIRECTORY)['member']
    if fault=='amount':state['events'][0]['amount_min']=3
    elif fault=='extra_event':state['events'].append({**state['events'][0],'id':999999})
    elif fault=='mixed_date':state['transactions'][0]['report_date']='2026-01-01'
    elif fault=='created_date':state['events'][0]['created_at']='2026-01-01'
    elif fault=='existing_availability':
        payload=json.loads(state['events'][0]['payload_json']);payload['source_availability']={};state['events'][0]['payload_json']=json.dumps(payload)
    else:member={**member,'bioguide_id':'WRONG'}
    assert dates.plan_date_correction(parsed,member,state)['status']=='held'


def test_stale_hash_rejected_and_production_path_requires_postgres(setup):
    db,document,_=setup
    with pytest.raises(ValueError,match='population changed'):
        dates.rehearse_date_correction(db,document,DIRECTORY,expected_before_hash='stale')
    with pytest.raises(ValueError,match='fresh PostgreSQL'):
        dates.apply_reviewed_date_correction(db,document,DIRECTORY,expected_before_hash='reviewed',expected_generation=1)
    assert db.scalar(select(func.count()).select_from(CongressRepairArchive))==0


def test_real_watchlist_and_monitoring_windows_keep_corrected_rows_without_new_alerts(setup,monkeypatch):
    from app.models import WatchlistItem
    from app.services import email_digests as digests
    from app.services.monitoring_alerts import _ensure_alert_for_event
    from test_email_digests import _user,_watchlist
    db,document,case=setup
    user=_user(db,'date-review@example.test')
    watch=_watchlist(db,user,alert_triggers=['congress_activity'])
    existing=set(db.scalars(select(WatchlistItem.security_id).where(WatchlistItem.watchlist_id==watch.id)))
    for security in db.scalars(select(Security)):
        if security.id not in existing:
            db.add(WatchlistItem(watchlist_id=watch.id,security_id=security.id))
    db.commit()
    rows=list(db.scalars(select(Event)))
    for row in rows:
        assert _ensure_alert_for_event(db,user_id=user.id,watchlist=watch,event=row)
    db.commit()
    since=rows[0].ts.replace(tzinfo=timezone.utc)
    from datetime import timedelta
    end=since+timedelta(days=1)
    monkeypatch.setattr(digests,'_upcoming_calendar_events_for_digest',lambda *a,**k:([], 'Calendar outside date-only correction'))
    def previews():
        return [digests.build_watchlist_activity_digest(db,user,watch,since),
            digests.build_monitoring_digest(db,user,watch,since,window_end=end),
            digests.build_signal_alert_digest(db,user,since,window_end=end)]
    before=previews()
    plan=dates.inspect_date_correction(db,document,DIRECTORY)
    assert dates.rehearse_date_correction(db,document,DIRECTORY,expected_before_hash=plan['before_hash'])['status']=='rehearsed'
    db.commit()
    after=previews()
    assert [r.items_count for r in before]==[r.items_count for r in after]
    assert before[0].items_count==len(rows)
    for row in rows:
        assert not _ensure_alert_for_event(db,user_id=user.id,watchlist=watch,event=row)
    assert db.scalar(select(func.count()).select_from(MonitoringAlert))==len(rows)
    assert db.scalar(select(func.count()).select_from(EmailDelivery))==0
