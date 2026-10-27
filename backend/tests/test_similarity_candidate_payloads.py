import json
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import event, select
from sqlalchemy.orm import Session

from app.models import ConfirmationScoreSnapshot as Snapshot
from app.services import historical_similarity as similarity
from app.services.outcome_ledger import current_confirmation_methodology
from test_historical_similarity import _source
from test_outcome_integrity import _engine


def baseline(db, methodology_id, today, direction):
    rows = db.execute(select(Snapshot).where(
        Snapshot.calculation_type == 'live', Snapshot.methodology_version_id == methodology_id,
        Snapshot.market_date < today,
    ).order_by(Snapshot.market_date.desc(), Snapshot.id.desc()).limit(similarity.MAX_CANDIDATES)).scalars().all()
    return [r for r in similarity._episode_deduped(rows) if similarity._directional_side(r.direction) == direction]


@pytest.fixture
def cohort():
    engine = _engine()
    today = datetime.now(timezone.utc).date()
    with Session(engine) as db:
        method = current_confirmation_methodology(db)
        method_id = method.id
        for i in range(126):
            direction = ('bullish', 'bearish', 'neutral')[i % 3]
            day = today - timedelta(days=100) + timedelta(days=(i // 5) * 3)
            if i == 124:
                day = today
            stamp = datetime.combine(day, datetime.min.time(), timezone.utc) + timedelta(hours=i % 5)
            sources = {'fundamentals': _source(direction, 14), 'price_volume': _source(direction, 11)}
            db.add(Snapshot(
                security_id=i % 5 + 1, ticker_at_time=('S' if i % 2 else 's') + str(i % 5),
                calculated_at=stamp, market_date=day, score=72, direction=direction,
                strength='strong', reference_price=100, reference_price_at=stamp,
                reference_price_source='test', active_source_count=2,
                active_sources_json=json.dumps(list(sources)),
                source_contributions_json=json.dumps({**sources, 'unused': 'x' * 15000}),
                source_freshness_json='{}', input_hash=f'candidate-{i}',
                methodology_version_id=method_id + (1 if i == 125 else 0),
                calculation_type='historical_reconstruction' if i == 123 else 'live',
            ))
        db.commit()
    yield engine, method_id, today
    engine.dispose()


@pytest.mark.parametrize('direction', ['bullish', 'bearish'])
def test_bounded_window_episode_order_and_full_results_match_original(cohort, monkeypatch, direction):
    engine, method, today = cohort
    monkeypatch.setattr(similarity, 'MAX_CANDIDATES', 53)
    current = {'score': 72, 'direction': direction, 'sources': {
        'fundamentals': _source(direction, 14), 'price_volume': _source(direction, 11)}}
    with Session(engine) as db:
        expected_ids = [r.id for r in baseline(db, method, today, direction)]
    queries = []
    def record(conn, cursor, statement, params, context, many):
        queries.append(statement.lower())
    event.listen(engine, 'before_cursor_execute', record)
    with Session(engine) as db:
        rows = similarity._episode_candidates(db, method, today, direction)
        assert [r.id for r in rows] == expected_ids
        assert len(rows) < 53
        assert all(len(r.source_contributions_json) > 15000 for r in rows)
        assert len(queries) == 2
        assert 'source_contributions_json' not in queries[0]
        assert 'source_contributions_json' in queries[1]
    event.remove(engine, 'before_cursor_execute', record)
    with Session(engine) as db:
        actual = similarity.build_similar_historical_setups(db, symbol='CURRENT', confirmation_bundle=current)
    monkeypatch.setattr(similarity, '_episode_candidates', baseline)
    with Session(engine) as db:
        expected = similarity.build_similar_historical_setups(db, symbol='CURRENT', confirmation_bundle=current)
    assert actual == expected
    for details in (False, True):
        assert similarity.public_similar_historical_setups(actual, include_details=details) == similarity.public_similar_historical_setups(expected, include_details=details)


def test_empty_matching_direction_never_fetches_evidence(cohort):
    engine, method, today = cohort
    queries = []
    def record(conn, cursor, statement, params, context, many):
        queries.append(statement.lower())
    event.listen(engine, 'before_cursor_execute', record)
    with Session(engine) as db:
        assert similarity._episode_candidates(db, method, today, 'not_directional') == []
    assert len(queries) == 1 and 'source_contributions_json' not in queries[0]


@pytest.mark.parametrize('operation', ['delete', 'change_direction'])
def test_out_of_band_change_reloads_coherent_original_window(cohort, operation):
    engine, method, today = cohort
    with Session(engine) as db:
        target = baseline(db, method, today, 'bullish')[0].id
    fired = []
    def intervene(conn, cursor, statement, params, context, many):
        if not fired and statement.lstrip().upper().startswith('SELECT') and 'source_contributions_json' in statement:
            fired.append(True)
            # Simulate an external writer bypassing the immutable ORM model.
            if operation == 'delete':
                conn.exec_driver_sql('DELETE FROM confirmation_score_snapshots WHERE id=?', (target,))
            else:
                conn.exec_driver_sql("UPDATE confirmation_score_snapshots SET direction='bearish' WHERE id=?", (target,))
    event.listen(engine, 'before_cursor_execute', intervene)
    with Session(engine) as db:
        actual = similarity._episode_candidates(db, method, today, 'bullish')
        expected = baseline(db, method, today, 'bullish')
        assert fired and [r.id for r in actual] == [r.id for r in expected]
        assert target not in {r.id for r in actual}
