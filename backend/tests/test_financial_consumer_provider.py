"""All live financial readers obey the same explicit provider selection."""
from datetime import datetime, timedelta, timezone
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from app.db import Base
from app.models import FundamentalsCache, Security
from app.services import custom_alert_rules, research_memory, research_briefs, macro_positioning, ticker_hydration

READERS = {
    'watchlist': lambda db, security: getattr(custom_alert_rules._fundamentals_row(db, 'TEST'), 'revenue_growth', None),
    'research_memory': lambda db, security: research_memory._evidence_state(db, security)['fundamentals']['revenue_growth'],
    'research_brief': lambda db, security: (research_briefs._latest_fundamentals(db, 'TEST') or {}).get('revenue_growth'),
    'research_identity': lambda db, security: research_briefs.normalize_supported_symbol(db, 'TEST')[1]['industry'],
    'macro': lambda db, security: macro_positioning._profile_for_symbol(db, 'TEST')['price_to_sales'],
    'hydration': lambda db, security: getattr(ticker_hydration._latest_fundamentals(db, 'TEST'), 'revenue_growth', None),
}

@pytest.fixture
def scenario(monkeypatch):
    monkeypatch.setenv('FMP_PROVIDER_DISABLED', '1')
    engine=create_engine('sqlite://')
    Base.metadata.create_all(engine)
    with Session(engine) as db:
        now=datetime(2026,10,14,17,tzinfo=timezone.utc)
        security=Security(symbol='TEST',name='Test',asset_class='equity')
        db.add(security)
        for index, provider in enumerate(('fmp','sec_edgar','free_direct'),1):
            db.add(FundamentalsCache(symbol='TEST',provider=provider,status='ok',fetched_at=now+timedelta(minutes=index),
                revenue_growth=index*10, price_to_sales=index*10,industry=str(index*10)))
        db.commit()
        yield db,security
    engine.dispose()

@pytest.mark.parametrize('reader',READERS)
@pytest.mark.parametrize('provider,index',[('fmp',1),('sec_edgar',2),('free_direct',3)])
def test_selected_provider_survives_newer_other_rows_and_rollback(scenario,monkeypatch,reader,provider,index):
    db,security=scenario
    monkeypatch.setenv('FUNDAMENTALS_PROVIDER',provider)
    value=READERS[reader](db,security)
    assert value==(str(index*10) if reader=='research_identity' else index*10)
    # Refreshing a nonselected provider must never perform a source switch.
    for row in db.query(FundamentalsCache).filter(FundamentalsCache.provider!=provider):
        row.fetched_at=datetime(2026,10,15,tzinfo=timezone.utc)
    db.commit()
    assert READERS[reader](db,security)==value

@pytest.mark.parametrize('reader',READERS)
@pytest.mark.parametrize('missing',[True,False])
def test_missing_or_failed_selected_provider_does_not_fall_back(scenario,monkeypatch,reader,missing):
    db,security=scenario
    monkeypatch.setenv('FUNDAMENTALS_PROVIDER','free_direct')
    row=db.query(FundamentalsCache).filter_by(provider='free_direct').one()
    if missing:db.delete(row)
    else:row.status='error'
    db.commit()
    assert READERS[reader](db,security) is None
