from copy import deepcopy
from datetime import datetime,timedelta,timezone
import json
import pytest
from sqlalchemy import create_engine,select,inspect
from sqlalchemy.orm import Session
from app.models import InsightsSnapshot
from app.services import finnhub_rating_preparation as prep,finnhub_research as transport

NOW=datetime(2026,10,16,12,tzinfo=timezone.utc)
WINDOW={'start':'2026-10-01','end':'2026-10-16','observed_at':NOW}

def event(**changes):
    return {'symbol':'AAPL','gradeTime':int((NOW-timedelta(days=1)).timestamp()),'company':'Example Research','fromGrade':'Neutral','toGrade':'Overweight','action':'up',**changes}

def normalize(rows=None,**changes):
    args={**WINDOW,**changes};return prep.normalize([event()] if rows is None else rows,'AAPL',**args)

@pytest.fixture
def db(monkeypatch):
    monkeypatch.setenv('FINNHUB_PAID_PREPARATION_ENABLED','1')
    monkeypatch.setenv('FINNHUB_SHARED_LIMITER_ENABLED','1')
    engine=create_engine('sqlite:///:memory:');InsightsSnapshot.__table__.create(engine)
    with Session(engine) as session:yield session
    engine.dispose()

@pytest.mark.parametrize('action,label',list(prep.ACTIONS.items()))
def test_exact_actions_keep_source_clock_actor_and_no_inferred_publication(action,label):
    raw=[event(action=action,fromGrade='' if action=='init' else 'Neutral')];before=deepcopy(raw)
    result=normalize(raw);item=result['items'][0]
    assert item['action']==label and item['providerAction']==action
    assert item['sourceActor']=='Example Research' and item['analystName'] is None
    assert item['sourceTimestamp']=='2026-10-15T12:00:00+00:00'
    assert item['gradeTime']==raw[0]['gradeTime']
    assert not result['publicationEligible'] and result['crossProviderReconciliationRequired']
    assert not result['priceTargetEventsAvailable']
    assert 'unverified' in result['sourceTimeBasis'] and 'unverified' in result['coverage']
    assert raw==before and normalize(raw)==result

def test_duplicate_identity_survives_overlapping_windows_and_observation_times():
    first=normalize([event(),event()]);assert len(first['items'])==1
    other=normalize(start='2026-10-10',observed_at=NOW+timedelta(hours=1))
    assert other['items']==first['items'] and other['observedAt']!=first['observedAt']
    with pytest.raises(transport.FinnhubUnavailable,match='conflicting_rating_event'):
        normalize([event(),event(toGrade='Buy')])
    assert len(normalize([event(),event(company='Different Research')])['items'])==2

@pytest.mark.parametrize('changes',[
 {'symbol':'MSFT'},{'symbol':'aapl'},{'gradeTime':True},{'gradeTime':1.1},{'gradeTime':'1'},
 {'gradeTime':-1},{'gradeTime':10**30},{'gradeTime':int(NOW.timestamp())+1},
 {'gradeTime':int((NOW-timedelta(days=30)).timestamp())},{'company':''},{'company':None},
 {'company':'x'*201},{'company':'bad\nactor'},{'toGrade':None},{'toGrade':''},
 {'fromGrade':[]},{'action':[]},{'action':'unknown'},{'action':None}])
def test_bad_identity_time_text_or_action_rejects_scope(changes):
    with pytest.raises(transport.FinnhubUnavailable):normalize([event(**changes)])

@pytest.mark.parametrize('start,end', [('2026-10-17','2026-10-17'),('2026-10-02','2026-10-01'),
 ('2026-01-01','2026-10-16'),('2026-02-30','2026-03-01'),('2026-1-01','2026-10-16'),('1969-01-01','1969-01-02')])
def test_bad_windows_rejected(start,end):
    with pytest.raises(transport.FinnhubUnavailable):normalize(start=start,end=end)

def test_empty_bound_and_share_class_are_explicit():
    assert normalize([])['status']=='empty' and not normalize([])['publicationEligible']
    with pytest.raises(transport.FinnhubUnavailable,match='bound'):normalize([event()]*1000)
    with pytest.raises(transport.FinnhubUnavailable):normalize({'data':[]})
    value=prep.normalize([event(symbol='BRK.B')],'BRK-B',**WINDOW)
    assert value['providerSymbol']=='BRK.B' and value['items'][0]['symbol']=='BRK-B'
    with pytest.raises(transport.FinnhubUnavailable):prep.normalize([event(symbol='BRK.A')],'BRK-B',**WINDOW)
    with pytest.raises(transport.FinnhubUnavailable):normalize(observed_at=NOW.replace(tzinfo=None))

def test_preparation_reader_repeat_preserve_entire_cache_and_no_canonical_tables(db,monkeypatch):
    calls=[];monkeypatch.setattr(transport,'request_json',lambda *a:calls.append(a) or [event(),event()])
    first=prep.prepare(db,'AAPL',**WINDOW);db.commit()
    row=db.scalar(select(InsightsSnapshot));before=(row.payload_json,row.fetched_at)
    assert prep.read_prepared(db,'AAPL',**WINDOW)['data']==normalize([event(),event()])
    repeat=prep.prepare(db,'AAPL',**{**WINDOW,'observed_at':NOW+timedelta(hours=1)})
    assert repeat['status']=='cached' and first['status']=='prepared'
    assert calls==[('stock/upgrade-downgrade',{'symbol':'AAPL','from':'2026-10-01','to':'2026-10-16'})]
    assert before==(row.payload_json,row.fetched_at)
    assert inspect(db.bind).get_table_names()==['insights_snapshots']
    view=prep.read_prepared(db,'AAPL',**WINDOW);view['data']['items'].clear()
    assert len(prep.read_prepared(db,'AAPL',**WINDOW)['data']['items'])==1

def test_invalid_refresh_preserves_previous_response_and_clocks(db,monkeypatch):
    monkeypatch.setattr(transport,'request_json',lambda *a:[event()]);prep.prepare(db,'AAPL',**WINDOW);db.commit()
    row=db.scalar(select(InsightsSnapshot));before=(row.payload_json,row.fetched_at)
    later={**WINDOW,'observed_at':NOW+timedelta(days=1)}
    assert prep.read_prepared(db,'AAPL',**later)['reason']=='rating_cache_stale_or_future'
    monkeypatch.setattr(transport,'request_json',lambda *a:[event(symbol='MSFT')])
    with pytest.raises(transport.FinnhubUnavailable):prep.prepare(db,'AAPL',**later)
    assert (row.payload_json,row.fetched_at)==before

@pytest.mark.parametrize('change',['raw','projection','source','clock'])
def test_reader_rejects_tampered_or_future_evidence(db,monkeypatch,change):
    monkeypatch.setattr(transport,'request_json',lambda *a:[event()]);prep.prepare(db,'AAPL',**WINDOW);db.commit()
    row=db.scalar(select(InsightsSnapshot));payload=json.loads(row.payload_json)
    if change=='raw':payload['raw'][0]['company']='Tampered'
    elif change=='projection':payload['normalized']['items'][0]['action']='Downgrade'
    elif change=='source':row.source='fmp'
    else:row.fetched_at=NOW+timedelta(seconds=1)
    row.payload_json=json.dumps(payload);db.commit()
    assert prep.read_prepared(db,'AAPL',**WINDOW)['status']=='unavailable'

def test_disabled_transport_prepare_and_cli_do_not_use_database_or_network(monkeypatch,capsys):
    from app.jobs.prepare_finnhub_ratings import main
    def forbidden(*a,**k):pytest.fail('Unexpected DB/provider request')
    monkeypatch.delenv('FINNHUB_PAID_PREPARATION_ENABLED',raising=False)
    monkeypatch.setattr(transport.requests,'get',forbidden)
    with pytest.raises(transport.FinnhubUnavailable,match='paid_preparation_disabled'):prep.prepare(None,'AAPL',**WINDOW)
    with pytest.raises(transport.FinnhubUnavailable,match='paid_preparation_disabled'):transport.request_json(prep.ENDPOINT,{})
    assert main(['--symbol','AAPL','--from','2026-10-01','--to','2026-10-16'])==1
    assert json.loads(capsys.readouterr().out)['reason']=='paid_preparation_disabled'
    monkeypatch.setenv('FINNHUB_PAID_PREPARATION_ENABLED','1');monkeypatch.delenv('FINNHUB_SHARED_LIMITER_ENABLED',raising=False)
    with pytest.raises(transport.FinnhubUnavailable,match='shared_request_budget_required'):prep.prepare(None,'AAPL',**WINDOW)


def test_complete_http_response_and_share_class_repeat(db, monkeypatch):
    from app.services import finnhub_budget as budget
    raw = [event(symbol='BRK.B', providerExtra={'retained': True})]
    encoded = json.dumps(raw).encode()
    requests, reservations = [], []
    class Response:
        status_code = 200
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def iter_content(self, size):
            yield encoded[:12]
            yield encoded[12:]
    monkeypatch.setenv('FINNHUB_API_KEY', 'synthetic-test-key')
    monkeypatch.setattr(budget, 'reserve', lambda: reservations.append(True))
    monkeypatch.setattr(transport.requests, 'get', lambda url, **kw: requests.append((url, kw)) or Response())
    prep.prepare(db, 'BRK-B', **WINDOW)
    db.commit()
    row = db.scalar(select(InsightsSnapshot))
    before = (row.payload_json, row.fetched_at)
    saved = json.loads(row.payload_json)
    assert saved['raw'] == raw
    assert saved['normalized']['providerSymbol'] == 'BRK.B'
    assert prep.prepare(db, 'BRK.B', **WINDOW)['status'] == 'cached'
    assert before == (row.payload_json, row.fetched_at)
    assert len(requests) == len(reservations) == 1
    url, options = requests[0]
    assert url == 'https://finnhub.io/api/v1/stock/upgrade-downgrade'
    assert options['params'] == {'symbol': 'BRK.B', 'from': WINDOW['start'], 'to': WINDOW['end']}
    assert options['headers'] == {'X-Finnhub-Token': 'synthetic-test-key'}
    assert options['allow_redirects'] is False
    assert not prep.read_prepared(db, 'BRK.B', **WINDOW)['publicationEligible']


def test_staging_preserves_existing_monthly_recommendation_consumer(db, monkeypatch):
    from app.services import replacement_analysts
    now = datetime.now(timezone.utc)
    raw = [{'symbol': 'AAPL', 'period': now.date().replace(day=1).isoformat(),
            'strongBuy': 2, 'buy': 3, 'hold': 4, 'sell': 1, 'strongSell': 0}]
    monthly = transport.normalize_recommendations(raw, 'AAPL', observed_at=now)
    db.add(InsightsSnapshot(kind='finnhub:recommendations:AAPL', source='finnhub',
                           fetched_at=now, payload_json=json.dumps(monthly)))
    db.commit()
    before = replacement_analysts.current_payload(db, 'AAPL', include_details=True)
    monkeypatch.setattr(transport, 'request_json', lambda *a: [event()])
    prep.prepare(db, 'AAPL', **WINDOW)
    db.commit()
    def forbidden(*args, **kwargs): pytest.fail('Unexpected live request')
    monkeypatch.setattr(transport.requests, 'get', forbidden)
    assert replacement_analysts.current_payload(db, 'AAPL', include_details=True) == before
    assert before['currentSummary']['consensusImpliedUpsidePct'] is None
