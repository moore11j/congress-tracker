from datetime import date, datetime, timedelta, timezone
import json

import pytest
from sqlalchemy import event
from sqlalchemy.orm import Session

from app.services import historical_similarity as similarity
from app.services.outcome_ledger import current_confirmation_methodology
from app.services.outcome_integrity import materialize_outcome_entry, materialize_outcome_horizons
from test_outcome_integrity import _engine, _bar
from app.models import ConfirmationScoreSnapshot, OutcomeEvidenceProvenance
from test_historical_similarity import _source


@pytest.mark.parametrize('direction', ['bullish', 'bearish'])
def test_bulk_similarity_preserves_canonical_legacy_and_missing_outcomes(monkeypatch, direction):
    monkeypatch.setenv('OUTCOME_LEDGER_ALLOW_UNAUDITED_LEGACY', 'false')
    engine = _engine()
    day = date(2026, 1, 5)
    sources = {'fundamentals': _source(direction, 14), 'price_volume': _source(direction, 11)}
    current = {'score': 72, 'direction': direction, 'sources': sources}
    with Session(engine) as db:
        methodology = current_confirmation_methodology(db)
        for when, price in [(day, 500), (day + timedelta(days=7), 505)]:
            db.merge(_bar('SPY', when, price))
        for index in range(6):
            symbol = f'C{index}' if index != 5 else 'C0'
            when = day if index != 5 else day + timedelta(days=10)
            stamp = datetime.combine(when, datetime.min.time(), tzinfo=timezone.utc).replace(hour=13)
            snapshot = ConfirmationScoreSnapshot(
                security_id=index+1, ticker_at_time=symbol, calculated_at=stamp, market_date=when,
                score=72, direction=direction, strength='strong', reference_price=100,
                reference_price_at=stamp,
                reference_price_source='unverified-historical-provider' if index == 3 else 'test',
                active_source_count=2, active_sources_json=json.dumps(list(sources)),
                source_contributions_json=json.dumps(sources),
                source_freshness_json=json.dumps({'fundamentals': {'freshness_days': 4}, 'price_volume': {'freshness_days': 2}}),
                input_hash=f'fixture-{index}', methodology_version_id=methodology.id,
                calculation_type='historical_reconstruction' if index == 4 else 'live', created_at=stamp,
            )
            db.add(snapshot); db.flush()
            for source_key in sources:
                db.add(OutcomeEvidenceProvenance(snapshot_id=snapshot.id, source_key=source_key,
                    evidence_id=f'event-{snapshot.id}-{source_key}', available_at=stamp-timedelta(minutes=1),
                    qualifying_event_at=stamp, source_timestamp=stamp-timedelta(minutes=1), source_payload_hash='fixture'))
            db.flush()
            if index < 3:
                db.merge(_bar(symbol, day, 100))
                db.merge(_bar(symbol, day + timedelta(days=7), 110 if direction == 'bullish' else 90))
            if index < 2:
                entry = materialize_outcome_entry(db, snapshot)
                assert entry is not None
                if index == 0:
                    assert materialize_outcome_horizons(db, entry, as_of=day + timedelta(days=8))
        db.commit()

        queries = []
        def record(_conn, _cursor, statement, _params, _context, _many):
            queries.append(statement.lower())
        event.listen(engine, 'before_cursor_execute', record)
        actual = similarity.build_similar_historical_setups(db, symbol='CURRENT', confirmation_bundle=current)
        assert actual['match_count'] == 4
        assert sum('from outcome_entries' in sql for sql in queries) == 1
        assert sum('from outcome_horizon_observations' in sql for sql in queries) == 1
        assert not any(sql.lstrip().startswith(('insert', 'update', 'delete')) for sql in queries)
        by_ticker = {item['ticker']: item for item in actual['top_matches']}
        assert by_ticker['C0']['outcomes']['7D']['status'] == 'matured'
        assert by_ticker['C1']['outcomes']['7D']['status'] == 'missing_price'
        assert by_ticker['C2']['outcomes']['7D']['status'] == 'matured'
        assert by_ticker['C3']['outcomes']['7D']['status'] == 'requires_reconstruction'
        assert by_ticker['C0']['market_date'] == day.isoformat()
        # Independently use the prior per-snapshot database lookup path. Compare
        # the entire result, including audit fields, horizons, ranks and previews.
        original = similarity._snapshot_outcomes
        def individual(session, snapshot, **kwargs):
            kwargs.pop('entry_by_snapshot', None)
            kwargs.pop('observations_by_entry', None)
            return original(session, snapshot, **kwargs)
        monkeypatch.setattr(similarity, '_snapshot_outcomes', individual)
        expected = similarity.build_similar_historical_setups(db, symbol='CURRENT', confirmation_bundle=current)
        assert actual == expected
        for details in [True, False]:
            assert similarity.public_similar_historical_setups(actual, include_details=details) == similarity.public_similar_historical_setups(expected, include_details=details)
