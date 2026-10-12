from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import json

import pytest
from sqlalchemy import create_engine, select, func
from sqlalchemy.orm import sessionmaker

from app.db import Base
from app.models import InsightsSnapshot, Event, FundamentalsCache, ResearchSourceDocument
from app.services import yahoo_financial_collection as collection
from app.services.direct_feed_store import DirectFeedDocument, DirectFeedRevision
from test_yahoo_financial_evidence import example

NOW=datetime(2026,10,12,tzinfo=timezone.utc)


@pytest.fixture
def scenario(monkeypatch):
    engine=create_engine('sqlite://')
    Base.metadata.create_all(engine)
    factory=sessionmaker(bind=engine)
    monkeypatch.setattr(collection,'SessionLocal',factory)
    monkeypatch.setenv('YAHOO_FINANCIAL_PREPARATION_ENABLED','1')
    calls=[]
    def fetch(url,now):calls.append(url);return example()['raw']
    monkeypatch.setattr(collection,'_fetch',fetch)
    yield factory,calls
    engine.dispose()


def test_collect_read_repeat_and_next_day_preserve_one_original(scenario):
    factory,calls=scenario
    first=collection.collect('MSFT','income',now=NOW)
    assert first['canonical_eligible'] is False
    assert first==collection.cached('MSFT','income',now=NOW+timedelta(hours=1))
    assert first==collection.collect('MSFT','income',now=NOW+timedelta(hours=1))
    assert len(calls)==1
    with factory() as db:
        revision=db.get(DirectFeedRevision,first['capture']['revision_id'])
        original=(revision.source_bytes,revision.parsed_json,revision.fetched_at)
    second=collection.collect('MSFT','income',now=NOW+timedelta(days=1))
    assert second['capture']['revision_id']==first['capture']['revision_id'] and len(calls)==2
    assert second['capture']['observed_at']!=first['capture']['observed_at']
    with factory() as db:
        revision=db.get(DirectFeedRevision,first['capture']['revision_id'])
        assert (revision.source_bytes,revision.parsed_json,revision.fetched_at)==original
        assert db.scalar(select(func.count()).select_from(DirectFeedDocument))==1
        assert db.scalar(select(func.count()).select_from(DirectFeedRevision))==1
        for model in (Event,FundamentalsCache,ResearchSourceDocument):
            assert db.scalar(select(func.count()).select_from(model))==0


def test_changed_original_adds_revision_without_overwriting_old(scenario,monkeypatch):
    factory,calls=scenario
    first=collection.collect('MSFT','income',now=NOW)
    raw=json.loads(example()['raw']);raw['timeseries']['result'][0]['annualEBITDA'][0]['reportedValue']['raw']=99
    monkeypatch.setattr(collection,'_fetch',lambda *args:json.dumps(raw).encode())
    second=collection.collect('MSFT','income',now=NOW+timedelta(days=1))
    assert first['capture']['revision_id']!=second['capture']['revision_id']
    with factory() as db:
        assert db.scalar(select(func.count()).select_from(DirectFeedRevision))==2
        assert db.get(DirectFeedRevision,first['capture']['revision_id']).source_bytes==example()['raw']
    assert second['bundle']['comparisons'][0]['status']=='differs'


@pytest.mark.parametrize('target',['raw','cache','document','source'])
def test_corrupt_evidence_is_not_returned_and_reader_never_fetches(scenario,target):
    factory,calls=scenario
    value=collection.collect('MSFT','income',now=NOW)
    with factory() as db:
        if target=='raw':db.get(DirectFeedRevision,value['capture']['revision_id']).source_bytes=b'changed'
        elif target=='document':db.get(DirectFeedDocument,value['capture']['document_id']).feed='other'
        else:
            row=db.get(InsightsSnapshot,collection._key('MSFT','income'))
            if target=='source':row.source='fmp'
            else:
                data=json.loads(row.payload_json);data['bundle']['observations'][0]['point']['value']=42
                row.payload_json=json.dumps(data)
        db.commit()
    assert collection.cached('MSFT','income',now=NOW) is None and len(calls)==1


def test_disabled_and_busy_make_no_request_or_budget_write(scenario,monkeypatch):
    factory,calls=scenario
    monkeypatch.setenv('YAHOO_FINANCIAL_PREPARATION_ENABLED','0')
    with pytest.raises(collection.YahooCollectionUnavailable,match='disabled'):collection.collect('MSFT','income',now=NOW)
    monkeypatch.setenv('YAHOO_FINANCIAL_PREPARATION_ENABLED','1')
    @contextmanager
    def busy(*args):yield False
    monkeypatch.setattr(collection,'_refresh_lock',busy)
    with pytest.raises(collection.YahooCollectionUnavailable,match='busy'):collection.collect('MSFT','income',now=NOW)
    with factory() as db:assert db.get(InsightsSnapshot,collection.BUDGET_KEY) is None
    assert not calls


def test_shared_reservation_counts_failures_and_prevents_second_request(scenario,monkeypatch):
    factory,calls=scenario
    def fail(*args):calls.append('failure');raise collection.YahooCollectionUnavailable('rate_limited',3600)
    monkeypatch.setattr(collection,'_fetch',fail)
    with pytest.raises(collection.YahooCollectionUnavailable,match='rate_limited'):collection.collect('MSFT','income',now=NOW)
    with pytest.raises(collection.YahooCollectionUnavailable,match='provider_cooldown'):collection.collect('NVDA','income',now=NOW+timedelta(minutes=5))
    with factory() as db:
        state=json.loads(db.get(InsightsSnapshot,collection.BUDGET_KEY).payload_json)
        assert len(state['reservations'])==1
        assert db.scalar(select(func.count()).select_from(DirectFeedRevision))==0
    assert calls==['failure']


def test_shared_minute_and_daily_limits(scenario):
    factory,calls=scenario
    collection.collect('MSFT','income',now=NOW)
    with pytest.raises(collection.YahooCollectionUnavailable,match='budget_exhausted'):collection.collect('NVDA','income',now=NOW+timedelta(seconds=59))
    with factory() as db:
        row=db.get(InsightsSnapshot,collection.BUDGET_KEY)
        row.payload_json=json.dumps({'reservations':[(NOW-timedelta(minutes=i+2)).timestamp() for i in range(30)]});db.commit()
    with pytest.raises(collection.YahooCollectionUnavailable,match='budget_exhausted'):collection.collect('NVDA','income',now=NOW)
    assert len(calls)==1


def test_capacity_stop_precedes_request_and_preserves_existing_source(scenario,monkeypatch):
    factory,calls=scenario
    first=collection.collect('MSFT','income',now=NOW)
    monkeypatch.setattr(collection,'MAX_REVISIONS_PER_DOCUMENT',1)
    with pytest.raises(collection.YahooCollectionUnavailable,match='revision_capacity'):collection.collect('MSFT','income',now=NOW+timedelta(days=1))
    monkeypatch.setattr(collection,'MAX_STORED_BYTES',1)
    with pytest.raises(collection.YahooCollectionUnavailable,match='archive_capacity'):collection.collect('NVDA','income',now=NOW+timedelta(days=1))
    assert len(calls)==1
    with factory() as db:assert db.get(DirectFeedRevision,first['capture']['revision_id']).source_bytes==example()['raw']


def test_malformed_success_is_held_not_saved(scenario,monkeypatch):
    factory,calls=scenario
    monkeypatch.setattr(collection,'_fetch',lambda *args:b'<html>unavailable</html>')
    with pytest.raises(collection.YahooCollectionUnavailable,match='invalid_statement_response'):collection.collect('MSFT','income',now=NOW)
    with factory() as db:
        assert db.scalar(select(func.count()).select_from(DirectFeedRevision))==0
        assert db.get(InsightsSnapshot,collection._key('MSFT','income')) is None


def test_future_and_expired_cache_never_look_fresh(scenario):
    collection.collect('MSFT','income',now=NOW)
    assert collection.cached('MSFT','income',now=NOW-timedelta(seconds=1)) is None
    assert collection.cached('MSFT','income',now=NOW+timedelta(days=1)) is None


class Response:
    def __init__(self,status=200,raw=b'{}',headers=None):self.status_code=status;self.raw=raw;self.headers=headers or {}
    def __enter__(self):return self
    def __exit__(self,*args):pass
    def iter_content(self,size):yield self.raw


@pytest.mark.parametrize('status,reason,seconds',[(302,'provider_unavailable',900),(403,'access_denied',86400),(429,'rate_limited',7200)])
def test_transport_denials_do_not_follow_redirects_or_retry(monkeypatch,status,reason,seconds):
    calls=[]
    def request(url,**kwargs):calls.append(kwargs);return Response(status,headers={'Retry-After':'7200'})
    monkeypatch.setattr(collection.requests,'get',request)
    with pytest.raises(collection.YahooCollectionUnavailable,match=reason) as error:collection._fetch('https://query1.finance.yahoo.com/',NOW)
    assert error.value.cooldown_seconds==seconds and len(calls)==1
    assert calls[0]['allow_redirects'] is False and 'Authorization' not in calls[0]['headers']


def test_transport_size_timeout_and_http_date_cooldown(monkeypatch):
    monkeypatch.setattr(collection.requests,'get',lambda *a,**k:Response(raw=b'x'*1_000_001))
    with pytest.raises(collection.YahooCollectionUnavailable,match='response_too_large'):collection._fetch('https://query1.finance.yahoo.com/',NOW)
    def timeout(*a,**k):raise collection.requests.Timeout()
    monkeypatch.setattr(collection.requests,'get',timeout)
    with pytest.raises(collection.YahooCollectionUnavailable,match='provider_timeout'):collection._fetch('https://query1.finance.yahoo.com/',NOW)
    assert collection._retry_seconds('Mon, 12 Oct 2026 02:00:00 GMT',NOW)==7200
    assert collection._retry_seconds('invalid',NOW)==3600


def test_request_scope_is_fixed_to_supported_metrics():
    from urllib.parse import urlsplit,parse_qs
    income=urlsplit(collection.request_url('MSFT','income',NOW));query=parse_qs(income.query)
    assert income.hostname=='query1.finance.yahoo.com' and income.scheme=='https'
    assert len(query['type'][0].split(','))==48
    assert 'trailing' not in parse_qs(urlsplit(collection.request_url('MSFT','balance',NOW)).query)['type'][0]
    with pytest.raises(collection.YahooCollectionUnavailable):collection.request_url('../MSFT','income',NOW)


def test_failed_refresh_preserves_last_good_cache_without_marking_it_fresh(scenario,monkeypatch):
    factory,calls=scenario
    first=collection.collect('MSFT','income',now=NOW)
    def fail(*args):raise collection.YahooCollectionUnavailable('transport_failed')
    monkeypatch.setattr(collection,'_fetch',fail)
    with pytest.raises(collection.YahooCollectionUnavailable,match='transport_failed'):
        collection.collect('MSFT','income',now=NOW+timedelta(days=1))
    with factory() as db:
        stored=json.loads(db.get(InsightsSnapshot,collection._key('MSFT','income')).payload_json)
        assert stored==first
        assert db.scalar(select(func.count()).select_from(DirectFeedRevision))==1
    assert collection.cached('MSFT','income',now=NOW+timedelta(days=1)) is None
