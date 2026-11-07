import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from starlette.requests import Request

from app.models import TickerContextBundleCache
from app.services.public_ticker_snapshot import prepared_snapshot, MAX_AGE
from app.services.confirmation_score import CONFIRMATION_SCORING_VERSION
import app.main as main

NOW=datetime.now(timezone.utc)


def snapshot(age=timedelta(hours=1)):
    payload=dict(symbol='AAPL',generated_at=(NOW-age).isoformat(),
        quote={'current_price':100,'stale':True},
        signals_summary={'price_volume':{'latest_date':'2026-11-05'}},
        source_cards={k:{} for k in ('price_volume','fundamentals','insiders','congress','government_contracts')},
        confirmation_score_bundle={'scoring_version':CONFIRMATION_SCORING_VERSION})
    return SimpleNamespace(symbol='AAPL',user_segment='canonical',generated_at=NOW-age,payload_json=json.dumps(payload))


@pytest.mark.parametrize('age',[timedelta(0),timedelta(minutes=4),timedelta(hours=1),MAX_AGE])
def test_dated_snapshot_preserves_entire_payload_and_source_dates(age):
    row=snapshot(age)
    assert prepared_snapshot(row,symbol='AAPL',now=NOW)==json.loads(row.payload_json)


@pytest.mark.parametrize('age',[timedelta(seconds=-1),MAX_AGE+timedelta(seconds=1)])
def test_future_and_expired_snapshots_are_not_relabelled_as_current(age):
    assert prepared_snapshot(snapshot(age),symbol='AAPL',now=NOW) is None


@pytest.mark.parametrize('field,value',[
    ('symbol','NVDA'),('user_segment','pro'),('generated_at',None),
    ('payload_json','[]'),('payload_json','bad json'),('payload_json','{"symbol":"NVDA"}'),
])
def test_invalid_identity_partition_or_payload_is_rejected(field,value):
    row=snapshot();setattr(row,field,value)
    assert prepared_snapshot(row,symbol='AAPL',now=NOW) is None


def test_actual_cache_only_endpoint_uses_prepared_row_with_anonymous_projection(monkeypatch):
    engine=create_engine('sqlite://')
    TickerContextBundleCache.__table__.create(engine)
    row=snapshot();key=main._ticker_context_bundle_cache_key('AAPL',user_segment='canonical',side='all',limit=3,lookback_days=main.CONFIRMATION_SIGNAL_WINDOW_DAYS)
    monkeypatch.setattr(main,'_ticker_context_bundle_memory_cache_get',lambda *a,**k:None)
    monkeypatch.setattr(main,'_build_ticker_context_bundle',lambda **k:pytest.fail('No foreground research build'))
    projected=[]
    def project(payload,**kwargs):
        assert kwargs['source_entitlements']==main._ticker_context_source_entitlements(None,authenticated=False)
        projected.append(payload)
        return {'public':True,'price_date':payload['signals_summary']['price_volume']['latest_date']}
    monkeypatch.setattr(main,'_project_ticker_context_bundle_for_entitlements',project)
    request=Request({'type':'http','method':'GET','path':'/api/tickers/AAPL/context-bundle','query_string':b'cached_only=true','headers':[]})
    with Session(engine) as db:
        db.add(TickerContextBundleCache(cache_key=key,**vars(row),stale_after=NOW-timedelta(minutes=50),expires_at=NOW-timedelta(minutes=45)))
        db.commit()
        result=main.ticker_context_bundle(request,'AAPL',side='all',limit=3,lookback_days=30,cached_only=True,db=db)
        assert result=={'public':True,'price_date':'2026-11-05'}
        assert projected==[json.loads(row.payload_json)]
        assert main._ticker_context_bundle_cached_for_segment(db,symbol='AAPL',user_segment='canonical',side='all',limit=3,lookback_days=30,started_at=0) is None
        stored=db.get(TickerContextBundleCache,key)
        old=json.loads(stored.payload_json);old['confirmation_score_bundle']['scoring_version']='obsolete'
        stored.payload_json=json.dumps(old);db.commit()
        assert main._ticker_context_bundle_cached_for_segment(db,symbol='AAPL',user_segment='canonical',side='all',limit=3,lookback_days=30,started_at=0,public_snapshot=True) is None
    engine.dispose()
