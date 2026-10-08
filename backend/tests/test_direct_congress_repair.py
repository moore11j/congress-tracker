import copy
from datetime import date, datetime, timezone
import hashlib
import json
from pathlib import Path

import pytest
from sqlalchemy import create_engine, select, func, Date, DateTime
from sqlalchemy.orm import Session

from app.db import Base
from app.models import (Event, Filing, Member, Security, Transaction, TradeOutcome,
    ReplicatedPortfolioPosition, MonitoringAlert, DataEnrichmentJob)
from app.services import direct_congress_repair as repair
from app.services.replicated_portfolios import _event_context_by_id


@pytest.fixture
def case():
    return json.loads((Path(__file__).with_name('fixtures') / 'congress_whitehouse_reconciliation.json').read_text())


def load_rows(db, model, rows):
    for row in rows:
        values = dict(row)
        for col in model.__table__.columns:
            if isinstance(values.get(col.name), str):
                if isinstance(col.type, DateTime):
                    values[col.name] = datetime.fromisoformat(values[col.name])
                elif isinstance(col.type, Date):
                    values[col.name] = date.fromisoformat(values[col.name])
        db.add(model(**values))
    db.flush()


@pytest.fixture
def setup(case, monkeypatch):
    engine = create_engine('sqlite:///:memory:')
    Base.metadata.create_all(engine)
    with Session(engine, autoflush=False) as db:
        for name, model in [('members', Member), ('securities', Security), ('filings', Filing),
                ('transactions', Transaction), ('events', Event)]:
            load_rows(db, model, case[name])
        # Actual public fixture plus isolated derived/history records.
        db.add(TradeOutcome(event_id=475000, member_id='W000802', symbol='JPM', return_pct=12.5))
        db.add(ReplicatedPortfolioPosition(run_id=1, source_event_id=475000, symbol='JPM',
            status='closed', entry_price=100, exit_price=120, return_pct=20, source_reason='recorded history'))
        db.commit()
        raw = b'synthetic transport for parser-isolated unit test'
        document = {'raw': raw, 'content_hash': hashlib.sha256(raw).hexdigest(),
            'feed': 'senate_ptr', 'metadata': case['parsed']['metadata']}
        monkeypatch.setattr(repair, 'parse_document', lambda *args: ('source', copy.deepcopy(case['parsed']), []))
        monkeypatch.setattr(repair, 'resolve_direct_member', lambda *args: {'status': 'resolved', 'member': case['members'][0]})
        yield db, document, repair._digest(repair.filing_state(db, document['metadata']['url']))
    engine.dispose()


def apply(setup):
    db, document, before = setup
    return repair.rehearse_duplicate_repair(db, document, [], expected_before_hash=before)


def test_archival_repair_preserves_canonical_ids_and_recorded_portfolio(setup):
    db, document, _ = setup
    positions = list(db.scalars(select(ReplicatedPortfolioPosition)))
    before_positions = [repair._record(p) for p in positions]
    before_context = _event_context_by_id(db, positions)
    survivors = {e.id: repair._record(e) for e in db.scalars(select(Event).where(Event.id.between(461243, 461247)))}
    result = apply(setup)
    assert result == {'status': 'rehearsed', 'preserved_events': 5, 'withdrawn_events': 10,
        'withdrawn_transactions': 13, 'inserted_events': 0, 'emails': 0}
    db.commit()
    assert list(db.scalars(select(Event.id).order_by(Event.id))) == list(range(461243, 461248))
    assert list(db.scalars(select(Transaction.id).order_by(Transaction.id))) == list(range(23181, 23186))
    assert survivors == {e.id: repair._record(e) for e in db.scalars(select(Event))}
    assert before_positions == [repair._record(p) for p in db.scalars(select(ReplicatedPortfolioPosition))]
    after_context = _event_context_by_id(db, positions)
    assert after_context[475000]['trade_date'] == before_context[475000]['trade_date']
    assert after_context[475000]['report_date'] == before_context[475000]['report_date']
    assert after_context[475000]['source_status'] == 'withdrawn_duplicate'
    assert after_context[475000]['canonical_event_id'] == '461246'
    assert db.scalar(select(func.count()).select_from(repair.CongressRepairArchive)) == 24
    assert db.scalar(select(func.count()).select_from(repair.CongressRowBinding)) == 5
    assert apply(setup)['status'] == 'existing'
    assert db.scalar(select(func.count()).select_from(repair.CongressRepairArchive)) == 24


@pytest.mark.parametrize('change', ['source_lot', 'wrong_stock', 'missing_source_trade', 'event_owner', 'event_document', 'event_member', 'missing_survivor'])
def test_planner_holds_unproven_repairs(case, change):
    if change == 'source_lot':
        case['parsed']['transactions'].append(copy.deepcopy(case['parsed']['transactions'][0]))
    elif change == 'wrong_stock':
        case['transactions'][0]['security_id'] = 999
    elif change == 'missing_source_trade':
        case['parsed']['transactions'].pop()
    elif change == 'event_owner':
        event = case['events'][0]
        payload = json.loads(event['payload_json']); payload['owner_type'] = 'dependent'
        event['payload_json'] = json.dumps(payload)
    elif change == 'event_document':
        case['events'][0]['source_document_url'] = 'https://example.com/other'
    elif change == 'event_member':
        payload = json.loads(case['events'][0]['payload_json'])
        payload['member']['bioguide_id'] = 'OTHER'
        case['events'][0]['payload_json'] = json.dumps(payload)
    else:
        for event in case['events']:
            event['ts'] = event['event_date'] = '2026-09-30'
    assert repair.plan_duplicate_repair(case['parsed'], case['members'][0], case)['status'] == 'held'


def test_stale_population_and_wrong_source_are_rejected(setup):
    db, document, _ = setup
    db.get(Transaction, 23181).amount_range_min = 999
    db.commit()
    with pytest.raises(ValueError, match='population changed'):
        apply(setup)
    document['raw'] = b'changed'
    with pytest.raises(ValueError, match='checksum'):
        apply(setup)
    assert db.scalar(select(func.count()).select_from(Event)) == 15


@pytest.mark.parametrize('reference', ['alert', 'job'])
def test_saved_alerts_and_active_jobs_hold_repair(setup, reference):
    db, _, _ = setup
    if reference == 'alert':
        db.add(MonitoringAlert(user_id=999, source_type='watchlist', source_id='1', source_name='Synthetic',
            event_id=475000, alert_type='trade', title='Existing alert', event_created_at=datetime.now(timezone.utc)))
    else:
        db.add(DataEnrichmentJob(job_type='feed_pnl_refresh', window_key='event:475000',
            dedupe_key='synthetic', status='running'))
    db.commit()
    assert apply(setup)['status'] == 'held'
    assert db.scalar(select(func.count()).select_from(Event)) == 15
    assert db.scalar(select(func.count()).select_from(repair.CongressRepairArchive)) == 0


def test_repeated_apply_checks_archived_evidence_and_binding_integrity(setup):
    db, _, _ = setup
    apply(setup); db.commit()
    db.scalar(select(repair.CongressRepairArchive)).record_json = '{}'
    db.commit()
    with pytest.raises(ValueError, match='state changed'):
        apply(setup)


def test_wrong_member_event_is_not_hidden_by_population_query(setup):
    db, document, _ = setup
    db.get(Event, 475000).member_bioguide_id = 'OTHER'
    db.commit()
    state = repair.filing_state(db, document['metadata']['url'])
    assert len(state['events']) == 15
    result = repair.rehearse_duplicate_repair(db, document, [], expected_before_hash=repair._digest(state))
    assert result['status'] == 'held'


def test_archive_failure_rolls_back_entire_repair(setup, monkeypatch):
    db, _, _ = setup
    original = db.flush
    def fail(*args, **kwargs):
        if any(isinstance(row, repair.CongressRowBinding) for row in db.new):
            raise RuntimeError('Injected binding failure')
        return original(*args, **kwargs)
    monkeypatch.setattr(db, 'flush', fail)
    with pytest.raises(RuntimeError, match='Injected'):
        apply(setup)
    monkeypatch.setattr(db, 'flush', original)
    assert db.scalar(select(func.count()).select_from(Event)) == 15
    assert db.scalar(select(func.count()).select_from(Transaction)) == 18
    assert db.scalar(select(func.count()).select_from(repair.CongressRepairArchive)) == 0


def add_pnl_job(db, event_id=475000):
    from app.services.data_enrichment_queue import build_dedupe_key
    event = db.get(Event, event_id)
    payload = {'event_id': event_id, 'event_type': 'congress_trade', 'symbol': event.symbol, 'trade_date': '2026-09-04'}
    job = DataEnrichmentJob(job_type='pnl_refresh', window_key=f'event:{event_id}', symbol=event.symbol,
        date_key='2026-09-04', payload_json=json.dumps(payload), status='queued', error='original diagnostic',
        dedupe_key=build_dedupe_key(job_type='pnl_refresh', symbol=event.symbol, date_key='2026-09-04', window_key=f'event:{event_id}'))
    db.add(job); db.commit()
    return job


def job_hash(db):
    return repair._digest([repair._record(row) for row in repair.linked_jobs(db,
        [462045, 462046, 463971, 463972, 463973, 475000, 475001, 475002, 475003, 475004])])


def test_queued_retirement_is_explicit_archived_repeatable_and_preserves_other_jobs(setup):
    db, document, before = setup
    job = add_pnl_job(db)
    keeper_job = add_pnl_job(db, 461243)
    original = repair._record(job)
    keeper_original = repair._record(keeper_job)
    assert apply(setup)['status'] == 'held'
    result = repair.rehearse_duplicate_repair(db, document, [], expected_before_hash=before, expected_jobs_hash=job_hash(db))
    assert result['retired_jobs'] == 1
    db.commit()
    assert job.status == 'skipped' and job.reason == 'source_event_withdrawn_duplicate'
    archive = db.scalar(select(repair.CongressRepairArchive).where(repair.CongressRepairArchive.entity_type == 'enrichment_job'))
    assert archive.record_json == repair.dumps(original)
    assert repair._record(keeper_job) == keeper_original
    assert apply(setup)['status'] == 'existing'
    job.status = 'queued'; db.commit()
    with pytest.raises(ValueError, match='state changed'):
        apply(setup)


@pytest.mark.parametrize('change', ['running', 'wrong_type', 'wrong_event', 'wrong_symbol', 'wrong_date', 'wrong_dedupe', 'extra_scope'])
def test_unverified_job_retirement_is_held(setup, change):
    db, document, before = setup
    job = add_pnl_job(db)
    if change == 'running': job.status = 'running'
    elif change == 'wrong_type': job.job_type = 'quote'
    elif change == 'wrong_symbol': job.symbol = 'OTHER'
    elif change == 'wrong_date': job.date_key = '2026-09-05'
    elif change == 'wrong_dedupe': job.dedupe_key = 'wrong'
    else:
        payload = json.loads(job.payload_json)
        if change == 'wrong_event': payload['event_id'] = 461243
        else: payload['user_id'] = 123
        job.payload_json = json.dumps(payload)
    db.commit()
    assert repair.rehearse_duplicate_repair(db, document, [], expected_before_hash=before,
        expected_jobs_hash=job_hash(db))['status'] == 'held'
    assert db.scalar(select(func.count()).select_from(Event)) == 15


def test_job_population_drift_and_atomic_retirement_rollback(setup, monkeypatch):
    db, document, before = setup
    job = add_pnl_job(db)
    with pytest.raises(ValueError, match='Enrichment population changed'):
        repair.rehearse_duplicate_repair(db, document, [], expected_before_hash=before, expected_jobs_hash='old')
    original_flush = db.flush
    def fail(*args, **kwargs):
        if any(isinstance(row, repair.CongressRowBinding) for row in db.new):
            raise RuntimeError('injected atomic failure')
        return original_flush(*args, **kwargs)
    monkeypatch.setattr(db, 'flush', fail)
    with pytest.raises(RuntimeError, match='injected'):
        repair.rehearse_duplicate_repair(db, document, [], expected_before_hash=before, expected_jobs_hash=job_hash(db))
    monkeypatch.setattr(db, 'flush', original_flush)
    assert job.status == 'queued' and job.reason is None
    assert db.scalar(select(func.count()).select_from(Event)) == 15
    assert db.scalar(select(func.count()).select_from(repair.CongressRepairArchive)) == 0
