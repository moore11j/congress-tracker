"""A source migration must initialize monitoring, not fabricate market alerts."""
import json
import os
from pathlib import Path
import subprocess
import sys
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from app.db import Base
from app.models import ConfirmationMonitoringEvent, ConfirmationMonitoringSnapshot
from app.services.confirmation_monitoring import refresh_watchlist_confirmation_monitoring


@pytest.mark.parametrize('provider,suffix', [('fmp',''), ('sec_edgar','_sec_fundamentals_v1')])
@pytest.mark.parametrize('retired', [False, True])
def test_fresh_workers_share_provider_version(provider, suffix, retired):
    env = {**os.environ, 'DATABASE_URL': 'sqlite:///:memory:', 'FUNDAMENTALS_PROVIDER': provider,
           'FMP_PROVIDER_DISABLED': '1' if retired else '0'}
    if retired:
        suffix += '_fmp_retired_v1'
    result = subprocess.run([sys.executable, '-c',
        'import json; from app.services import confirmation_score as s, confirmation_monitoring as m; '
        'print(json.dumps([s.CONFIRMATION_CLASSIFICATION_VERSION,s.CONFIRMATION_SCORING_VERSION,m.CONFIRMATION_CLASSIFICATION_VERSION]))'],
        cwd=Path(__file__).resolve().parents[1], env=env, check=True, capture_output=True, text=True, timeout=45)
    versions = json.loads(result.stdout.splitlines()[-1])
    assert versions == ['confirmation_direction_v10_macro_freshness'+suffix,
                        'confirmation_score_v9_macro_freshness'+suffix, 'confirmation_direction_v10_macro_freshness'+suffix]


@pytest.mark.parametrize('suffix', ['_sec_fundamentals_v1', '_sec_fundamentals_v1_fmp_retired_v1'])
def test_sec_coverage_transition_preserves_history_and_does_not_alert(monkeypatch, suffix):
    before_version = 'confirmation_direction_v10_macro_freshness'
    after_version = before_version + suffix
    now = datetime.now(timezone.utc)
    bundle = {'score': 35, 'band': 'weak', 'direction': 'mixed', 'classification_version': after_version,
              'sources': {'fundamentals': {'present': False, 'direction': 'neutral'}}}
    monkeypatch.setattr('app.services.confirmation_monitoring.get_confirmation_score_bundles_for_tickers', lambda *a, **k: {'TSM': bundle})
    engine = create_engine('sqlite:///:memory:')
    Base.metadata.create_all(engine)
    with Session(engine) as db:
        snapshot = ConfirmationMonitoringSnapshot(user_id=1, watchlist_id=1, ticker='TSM', score=80,
            band='strong', direction='bullish', source_count=4, status='4-source bullish', observed_at=now-timedelta(days=1),
            source_states_json=json.dumps({'__methodology': {'classification_version': before_version}}))
        history = ConfirmationMonitoringEvent(user_id=1, watchlist_id=1, ticker='TSM', event_type='confirmation_upgraded',
            title='Recorded past event', score_after=80, band_after='strong', direction_after='bullish',
            source_count_after=4, created_at=now-timedelta(days=1), payload_json='{}')
        db.add_all([snapshot, history]); db.commit()
        historical = {c.name: getattr(history,c.name) for c in history.__table__.columns}
        first = refresh_watchlist_confirmation_monitoring(db,user_id=1,watchlist_id=1,tickers=['TSM'],now=now)
        assert first['initialized'] == 1 and first['generated'] == 0
        second = refresh_watchlist_confirmation_monitoring(db,user_id=1,watchlist_id=1,tickers=['TSM'],now=now)
        assert second['initialized'] == 0 and second['generated'] == 0
        db.commit(); db.refresh(history)
        # SQLite normalizes timestamp timezone on reload; compare stored values.
        for key in historical:
            value = historical[key]
            if isinstance(value, datetime): historical[key] = value.replace(tzinfo=None)
        assert {c.name: getattr(history,c.name) for c in history.__table__.columns} == historical
        assert db.query(ConfirmationMonitoringEvent).count() == 1
        assert json.loads(snapshot.source_states_json)['__methodology']['classification_version'] == after_version
    engine.dispose()


@pytest.mark.parametrize('sec,analysts,retired', [
    (sec,analysts,retired) for sec in [False,True] for analysts in [False,True] for retired in [False,True]])
def test_combined_provider_methodologies_are_distinct_in_new_workers(sec,analysts,retired):
    env={**os.environ,'DATABASE_URL':'sqlite:///:memory:',
         'FUNDAMENTALS_PROVIDER':'sec_edgar' if sec else 'fmp',
         'ANALYST_PROVIDER':'finnhub' if analysts else 'fmp',
         'FMP_PROVIDER_DISABLED':'1' if retired else '0'}
    suffix=('_sec_fundamentals_v1' if sec else '')+('_analyst_display_only_v1' if analysts else '')+('_fmp_retired_v1' if retired else '')
    r=subprocess.run([sys.executable,'-c',
        'import json;from app.services.confirmation_score import CONFIRMATION_SCORING_VERSION as s;'
        'from app.services.outcome_ledger import CURRENT_CONFIRMATION_METHODOLOGY_VERSION as o;'
        'print(json.dumps([s,o]))'],cwd=Path(__file__).resolve().parents[1],env=env,
        check=True,capture_output=True,text=True,timeout=45)
    assert json.loads(r.stdout.splitlines()[-1])==['confirmation_score_v9_macro_freshness'+suffix,
        'confirmation-v8-agreement-coverage'+suffix]
