from datetime import date, datetime, timedelta, timezone
from copy import deepcopy
import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session
from app.models import FredObservation, FredSeriesRefresh
from app.services import fred_macro_cache as fred


@pytest.mark.parametrize('body', [
    'observation_date,DGS2\n2026-10-14,4.0\n',
    'observation_date,DGS10,DGS10\n2026-10-14,4.0,5.0\n',
    'observation_date,DGS10,DGS2\n2026-10-14,4.0,5.0\n',
    'observation_date,DGS10\n2026-10-14,4.0\n2026-10-14,5.0\n',
    *['observation_date,DGS10\n2026-10-14,'+v+'\n' for v in ('Infinity','-Infinity','NaN','1e309','unknown')],
    'observation_date,DGS10\n2026-10-14,4.0,extra\n',
    'observation_date,DGS10\n2026-10-14\n',
    'observation_date,DGS10\n2026-02-30,4.0\n',
    'observation_date,DGS10\n2026-10-14T12:00:00,4.0\n',
    'observation_date,DGS10\n20261014,4.0\n',
    '<html>Not a series</html>', '',
])
def test_wrong_or_conflicting_source_fails_entire_response(body):
    with pytest.raises(ValueError): fred.parse_fred_csv('DGS10', body)


def test_valid_zero_negative_missing_and_identical_duplicate_values_are_preserved():
    body='\ufeffDATE,dgs10\n2026-10-09,4.00\n2026-10-12,.\n2026-10-13,0\n2026-10-14,-0.25\n2026-10-09,4\n'
    assert fred.parse_fred_csv('DGS10', body) == [
        {'series_id':'DGS10','observation_date':date(2026,10,14),'value':-0.25},
        {'series_id':'DGS10','observation_date':date(2026,10,13),'value':0.0},
        {'series_id':'DGS10','observation_date':date(2026,10,9),'value':4.0}]


def test_conflict_outside_retained_history_is_not_ignored(monkeypatch):
    monkeypatch.setenv('FRED_MAX_OBSERVATIONS_PER_SERIES','2')
    body='observation_date,DGS10\n2026-10-14,4\n2026-10-13,5\n2026-10-09,6\n2026-10-09,7\n'
    with pytest.raises(ValueError,match='conflict'):fred.parse_fred_csv('DGS10',body)


def test_invalid_refresh_preserves_existing_observations_and_marks_stale(monkeypatch):
    engine=create_engine('sqlite:///:memory:')
    for model in (FredObservation,FredSeriesRefresh):model.__table__.create(engine)
    now=datetime(2026,10,16,tzinfo=timezone.utc)
    monkeypatch.setattr(fred,'_utcnow',lambda:now)
    body='observation_date,DGS10\n2026-10-13,4.0\n2026-10-14,4.1\n'
    calls=[]
    monkeypatch.setattr(fred,'_fetch_fred_csv',lambda series:calls.append(series) or body)
    with Session(engine) as db:
        assert fred.refresh_fred_macro_cache(db,series_ids=('DGS10',))['status']=='ok'
        rows=db.scalars(select(FredObservation).order_by(FredObservation.observation_date)).all()
        before=[{c.name:getattr(row,c.name) for c in FredObservation.__table__.columns} for row in rows]
        treasury=deepcopy(fred.build_fred_treasury(db))
        assert fred.refresh_fred_macro_cache(db,series_ids=('DGS10',))['status']=='skipped'
        assert len(calls)==1
        monkeypatch.setattr(fred,'_utcnow',lambda:now+timedelta(days=1))
        monkeypatch.setattr(fred,'_fetch_fred_csv',lambda series:'observation_date,DGS2\n2026-10-15,9\n')
        result=fred.refresh_fred_macro_cache(db,series_ids=('DGS10',))
        assert result['status']=='unavailable' and result['failed_series']==1
        db.expire_all()
        after=db.scalars(select(FredObservation).order_by(FredObservation.observation_date)).all()
        assert before==[{c.name:getattr(row,c.name) for c in FredObservation.__table__.columns} for row in after]
        state=db.get(FredSeriesRefresh,'DGS10')
        assert state.status=='error' and state.latest_observation_date==date(2026,10,14)
        retained=next(row for row in fred.build_fred_treasury(db) if row['label']=='10Y Treasury')
        original=next(row for row in treasury if row['label']=='10Y Treasury')
        assert retained['value']==original['value']==4.1
        assert retained['change']==original['change']
        assert retained['cache_status']=='stale' and retained['source']=='fred'
    engine.dispose()
