"""Actual retained revenue proof through preparation, cache and alert recovery."""
import json
import socket
from datetime import datetime, timedelta
from unittest.mock import patch

from sqlalchemy import create_engine, select, func
from sqlalchemy.orm import sessionmaker

from test_sec_rental_revenue import NOW, CIK, Client, acquired, project


def test_preparation_cache_and_alert_recovery_preserve_history(monkeypatch):
    from app.db import Base
    from app.models import (FundamentalsCache, Event, EmailDelivery, MonitoringAlert,
        Security, UserAccount, Watchlist, WatchlistItem, WatchlistAlertRule)
    from app.services import sec_fundamentals_preparation as prep, sec_inline_financials as inline
    from app.services.custom_alert_rules import evaluate_watchlist_custom_alerts, validate_conditions

    supplement = acquired()
    expected = project(supplement)
    engine = create_engine('sqlite://')
    Base.metadata.create_all(engine)
    factory = sessionmaker(bind=engine)
    client = Client()

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return NOW

    monkeypatch.setenv('SEC_FUNDAMENTALS_WARMING_ENABLED', '1')
    with patch.object(socket.socket, 'connect', side_effect=AssertionError('Offline only')), \
         patch.object(prep, 'SessionLocal', factory), patch.object(prep, 'datetime', Clock), \
         patch.object(prep, 'DirectSourceClient', return_value=client), \
         patch.object(prep.sec_directory, 'directory', return_value={'VMRK': {'cik': CIK}}), \
         patch.object(inline, 'acquire', return_value=supplement):
        payload = prep.prepare('VMRK')
        assert payload['status'] == 'partial'
        assert payload['values'] == {k: expected[k] for k in prep.METRICS if k in expected}
        request_count = len(client.calls)
        assert prep.prepare('VMRK') == payload and len(client.calls) == request_count
        with factory() as db:
            assert db.scalar(select(func.count()).select_from(FundamentalsCache)) == 0
            assert db.scalar(select(func.count()).select_from(Event)) == 0

    monkeypatch.setenv('FUNDAMENTALS_PROVIDER', 'sec_edgar')
    with factory() as db:
        user = UserAccount(email='rental-proof@example.test')
        security = Security(symbol='VMRK', name='Rental proof', asset_class='equity')
        db.add_all([user, security]); db.flush()
        watch = Watchlist(name='Rental proof', owner_user_id=user.id)
        db.add(watch); db.flush()
        db.add(WatchlistItem(watchlist_id=watch.id, security_id=security.id, target_type='ticker'))
        threshold = expected['revenue_growth'] - 1
        rule = WatchlistAlertRule(user_id=user.id, watchlist_id=watch.id, name='Revenue growth',
            enabled=True, scope_type='any_watchlist_ticker', match_type='all', delivery='both',
            conditions_json=json.dumps(validate_conditions([dict(metric='revenue_growth',
                operator='gt', comparison_type='value', comparison_value=threshold)])))
        row = FundamentalsCache(symbol='VMRK', provider='sec_edgar', status='ok', fetched_at=NOW,
            source_evidence_json=json.dumps({'financial_status': 'unavailable', 'calculations': {}}))
        db.add_all([rule, row]); db.commit()

        def run(minutes):
            result = evaluate_watchlist_custom_alerts(db, user_id=user.id,
                watchlist_id=watch.id, now=NOW+timedelta(minutes=minutes))
            db.commit()
            return result['triggered']

        assert run(0) == 0
        for key, value in payload['values'].items():
            setattr(row, key, value)
        row.source_evidence_json = expected['source_evidence_json']
        assert run(5) == run(10) == 0
        row.revenue_growth = threshold - 1
        assert run(15) == 0
        row.revenue_growth = expected['revenue_growth']
        assert run(20) == 1 and run(25) == 0
        row.revenue_growth = None
        row.source_evidence_json = json.dumps({'financial_status': 'unavailable', 'calculations': {}})
        assert run(30) == 0
        assert db.scalar(select(func.count()).select_from(MonitoringAlert)) == 1
        assert db.scalar(select(func.count()).select_from(EmailDelivery)) == 0
        assert db.scalar(select(func.count()).select_from(Event)) == 0
    engine.dispose()
