from datetime import datetime, timedelta, timezone
import json

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from app.db import Base
from app.models import InsightsSnapshot, TickerFinancialsCache
from app import main as api
from app.services import research_briefs, ticker_hydration


@pytest.fixture
def db(monkeypatch):
    monkeypatch.setenv('FINANCIAL_STATEMENTS_PROVIDER', 'sec_edgar')
    monkeypatch.setenv('FMP_PROVIDER_DISABLED', '0')
    engine = create_engine('sqlite:///:memory:')
    Base.metadata.create_all(engine, tables=[InsightsSnapshot.__table__, TickerFinancialsCache.__table__])
    with Session(engine) as session:
        session.add(TickerFinancialsCache(symbol='ABC', status='ok', fetched_at=datetime.now(timezone.utc),
            payload_json=json.dumps({'symbol': 'ABC', 'summary': {'revenueTtm': 10, 'forwardPE': 30}})))
        session.commit()
        yield session
    engine.dispose()


def _save(db, *, age=timedelta(0), source='sec_edgar', symbol='ABC'):
    db.add(InsightsSnapshot(kind='sec-financials:ABC:v1', source=source,
        fetched_at=datetime.now(timezone.utc)-age, payload_json=json.dumps({
            'symbol': symbol, 'source': source, 'status': 'partial', 'summary': {'revenueTtm': 100}})))
    db.commit()


def test_comparison_reads_selected_fresh_cache_without_legacy_forecasts_and_rollback_preserves_history(db, monkeypatch):
    _save(db)
    result = api._peer_compare_financials_fallbacks(db, 'ABC')
    assert result['revenue_ttm'] == 100 and result['forward_pe'] is None
    before = db.get(TickerFinancialsCache, 'ABC').payload_json
    monkeypatch.setenv('FINANCIAL_STATEMENTS_PROVIDER', 'fmp')
    assert api._peer_compare_financials_fallbacks(db, 'ABC')['revenue_ttm'] == 10
    assert db.get(TickerFinancialsCache, 'ABC').payload_json == before


@pytest.mark.parametrize('kwargs', [
    {'age': timedelta(hours=25)}, {'age': timedelta(hours=-1)},
    {'source': 'fmp'}, {'symbol': 'DEF'},
])
def test_invalid_or_stale_selected_cache_cannot_fall_back_to_retired_provider(db, kwargs):
    _save(db, **kwargs)
    assert api._peer_compare_financials_fallbacks(db, 'ABC') == {}


def test_missing_selected_cache_and_global_retirement_cannot_reuse_old_financials(db, monkeypatch):
    assert api._peer_compare_financials_fallbacks(db, 'ABC') == {}
    monkeypatch.setenv('FINANCIAL_STATEMENTS_PROVIDER', 'fmp')
    monkeypatch.setenv('FMP_PROVIDER_DISABLED', '1')
    assert api._peer_compare_financials_fallbacks(db, 'ABC') == {}


@pytest.mark.parametrize('age', [timedelta(0), timedelta(hours=25)])
def test_research_hydration_and_diagnostics_use_selected_source(db, monkeypatch, age):
    _save(db, age=age)
    current = age == timedelta(0)
    snapshot = research_briefs._cached_financials_snapshot(db, 'ABC')
    assert (snapshot is not None) == current
    if snapshot:
        assert snapshot['source'] == 'sec_edgar' and snapshot['status'] == 'partial'
    assert (ticker_hydration._financials_content_state(db, 'ABC', {}, {}) == 'ok') == current
    assert api._ticker_debug_financials_status(db, 'ABC')['present'] == current
    monkeypatch.setenv('FINANCIAL_STATEMENTS_PROVIDER', 'fmp')
    assert research_briefs._cached_financials_snapshot(db, 'ABC')['status'] == 'ok'
    assert ticker_hydration._financials_content_state(db, 'ABC', {}, {}) == 'ok'
    monkeypatch.setenv('FMP_PROVIDER_DISABLED', '1')
    assert research_briefs._cached_financials_snapshot(db, 'ABC') is None
    assert ticker_hydration._financials_content_state(db, 'ABC', {}, {}) == 'unavailable'
    assert api._ticker_debug_financials_status(db, 'ABC')['present'] is False
