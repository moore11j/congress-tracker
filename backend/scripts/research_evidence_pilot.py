"""Operator-only bounded production pilot. Logs aggregates, never private thesis prose."""
import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone

sys.path.insert(0, '/app')
from sqlalchemy import func, inspect, select, text
from app.db import SessionLocal, engine
from app.models import ResearchSourceCoverage, ResearchSourceDocument, ResearchThesis, ResearchThesisClaim, ResearchClaimEvidenceMatch, Security, WatchlistItem
from app.services import operational_intelligence as ops
from app.services.ai_marketing import OPENAI_API_KEY, resolved_setting_value


def emit(kind, **values):
    print(json.dumps({'kind': kind, **values}, default=str, sort_keys=True), flush=True)


def verify_matching():
    """Exercise real matching against public evidence in disposable in-memory storage."""
    from sqlalchemy import create_engine
    from sqlalchemy.orm import Session
    from app.db import Base
    from app.models import ResearchEvidenceEvent, ResearchInvalidatorEvidenceMatch, ResearchClaimMatchCheckpoint, ResearchThesisInvalidator, UserAccount
    from app.services import research_claim_matching as matching
    with SessionLocal() as production:
        security = production.scalar(select(Security).where(Security.symbol == 'MU'))
        event = production.scalar(select(ResearchEvidenceEvent).where(ResearchEvidenceEvent.security_id == security.id, ResearchEvidenceEvent.superseded_at.is_(None), ResearchEvidenceEvent.event_type == 'commercial_milestone').order_by(ResearchEvidenceEvent.created_at.desc()))
        if event is None:
            raise RuntimeError('Pilot needs a real Micron commercial milestone first')
        event_values = {column.name: getattr(event, column.name) for column in event.__table__.columns}
        security_values = {'id': security.id, 'symbol': security.symbol, 'name': security.name, 'asset_class': security.asset_class}
        api_key = resolved_setting_value(production, OPENAI_API_KEY)
    isolated = create_engine('sqlite+pysqlite:///:memory:')
    Base.metadata.create_all(isolated, tables=[model.__table__ for model in (UserAccount, Security, ResearchThesis, ResearchThesisClaim, ResearchThesisInvalidator, ResearchEvidenceEvent, ResearchClaimEvidenceMatch, ResearchClaimMatchCheckpoint, ResearchInvalidatorEvidenceMatch)])
    original_resolver = matching.resolved_setting_value
    matching.resolved_setting_value = lambda _db, _key: api_key
    try:
        with Session(isolated) as db:
            user = UserAccount(email='isolated-pilot@example.test')
            other = UserAccount(email='other-isolated-pilot@example.test')
            db.add_all([user, other, Security(**security_values)]); db.flush()
            fixture = ResearchThesis(id='pilot-only', user_id=user.id, security_id=security_values['id'], ticker_at_creation='MU', title='Isolated test fixture', summary='Synthetic assertions for pipeline validation; never persisted to production', orientation='neutral', source_type='custom', status='active', started_monitoring_at=event_values['published_at']-timedelta(days=1))
            db.add(fixture)
            for name, subject in [('supports', 'Micron is advancing DDR5 server memory product capabilities.'), ('contradicts', 'Micron has stopped advancing DDR5 server memory product capabilities.')]:
                db.add(ResearchThesisClaim(id=name, thesis_id=fixture.id, claim_type='product_launch', subject=subject, importance='high', monitoring_mode='semantic', coverage_level='partially_monitored', user_confirmed=True))
            live_event = ResearchEvidenceEvent(**event_values)
            db.add(live_event); db.commit()
            budget = matching.MatchBudget(remaining=2)
            first = matching.process_event_matches(db, event=live_event, budget=budget)
            rows = matching.query_matches(db, user=user, thesis_id=fixture.id)
            relationships = {row['claim_id']: row['relationship'] for row in rows}
            assert relationships == {'supports': 'supports', 'contradicts': 'contradicts'}, relationships
            assert matching.query_matches(db, user=other, thesis_id=fixture.id) == []
            second = matching.process_event_matches(db, event=live_event, budget=budget)
            assert second['semantic'] == 0 and second['matches'] == 0
            emit('isolated_matching_verified', relationships=relationships, model_calls=budget.attempts, repeat_calls=second['semantic'], ownership_protected=True, production_theses_created=0)
    finally:
        matching.resolved_setting_value = original_resolver
        isolated.dispose()


def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else 'diagnose'
    if mode not in {'diagnose', 'pilot', 'verify_matching', 'probe_transcripts', 'pilot_transcripts'}:
        raise ValueError('Unsupported pilot mode')
    if mode == 'probe_transcripts':
        # Verify configured provider access without logging licensed text or
        # invoking an AI model. Explicit operator action only.
        for symbol in ('MU', 'NVDA', 'AAPL'):
            try:
                rows = ops._fmp_rows('earning-call-transcript-dates', {'symbol': symbol})
                periods = []
                for row in rows:
                    try:
                        year = int(row.get('year') or row.get('fiscalYear'))
                        quarter = int(str(row.get('quarter') or row.get('period') or '').upper().replace('Q', ''))
                        if year > 1990 and 1 <= quarter <= 4:
                            periods.append((year, quarter))
                    except (ValueError, TypeError):
                        continue
                if not periods:
                    emit('transcript_probe', symbol=symbol, status='no_periods')
                    continue
                year, quarter = max(periods)
                transcripts = ops._fmp_rows('earning-call-transcript', {'symbol': symbol, 'year': year, 'quarter': quarter})
                lengths = [len(str(row.get('content') or '')) for row in transcripts]
                emit('transcript_probe', symbol=symbol, year=year, quarter=quarter, status='available' if any(length >= 200 for length in lengths) else 'no_content', content_lengths=lengths)
            except Exception as exc:
                emit('transcript_probe', symbol=symbol, status='unavailable', error_type=type(exc).__name__)
        return
    started = datetime.now(timezone.utc)
    if mode == 'verify_matching':
        verify_matching()
    with SessionLocal() as db:
        emit('configuration', operational_enabled=ops.operational_intelligence_enabled(),
             matching_enabled=ops.claim_matching_enabled(), transcripts_enabled=ops.transcript_analysis_enabled(),
             fmp_configured=bool(os.getenv('FMP_API_KEY')), openai_configured=bool(resolved_setting_value(db, OPENAI_API_KEY)),
             thesis_status_counts=dict(db.execute(select(ResearchThesis.status, func.count()).group_by(ResearchThesis.status)).all()),
             claims=db.scalar(select(func.count()).select_from(ResearchThesisClaim)),
             scheduler_candidates=len(ops.candidate_securities(db, limit=250)))
        emit('worker_progress',
             coverage=[{'source': source, 'status': status, 'count': count, 'latest_attempt': latest} for source, status, count, latest in db.execute(select(ResearchSourceCoverage.source_type, ResearchSourceCoverage.status, func.count(), func.max(ResearchSourceCoverage.last_attempt_at)).group_by(ResearchSourceCoverage.source_type, ResearchSourceCoverage.status))],
             documents=[{'status': status, 'reason': reason, 'count': count} for status, reason, count in db.execute(select(ResearchSourceDocument.processing_status, ResearchSourceDocument.failure_reason, func.count()).group_by(ResearchSourceDocument.processing_status, ResearchSourceDocument.failure_reason))])
        for symbol in ('MU', 'NVDA', 'AAPL'):
            security = db.scalar(select(Security).where(Security.symbol == symbol))
            if not security:
                emit('missing_security', symbol=symbol)
                continue
            emit('ticker', symbol=symbol,
                 active_theses=db.scalar(select(func.count()).select_from(ResearchThesis).where(ResearchThesis.security_id == security.id, ResearchThesis.status == 'active')),
                 watchlist_entries=db.scalar(select(func.count()).select_from(WatchlistItem).where(WatchlistItem.security_id == security.id)),
                 coverage=ops.ticker_operational_intelligence(db, security=security)['coverage'],
                 documents=[{'status': status, 'reason': reason, 'count': count} for status, reason, count in db.execute(select(ResearchSourceDocument.processing_status, ResearchSourceDocument.failure_reason, func.count()).where(ResearchSourceDocument.security_id == security.id).group_by(ResearchSourceDocument.processing_status, ResearchSourceDocument.failure_reason))])
    if mode in {'pilot', 'pilot_transcripts'}:
        # Two recent documents per source, at most four extraction calls and two
        # private matching calls per symbol. Production licensing flags stay intact.
        os.environ['RESEARCH_OPERATIONAL_MAX_EXTRACTIONS_PER_RUN'] = '4'
        os.environ['RESEARCH_CLAIM_MATCHING_MAX_CALLS_PER_RUN'] = '2'
        originals = (ops.get_stock_news, ops.get_press_releases)
        def bounded(loader):
            def load(**kwargs):
                kwargs['limit'] = 2
                payload = loader(**kwargs)
                emit('provider', symbol=kwargs['symbol'], source=loader.__name__,
                     status=payload.get('status'), reason=payload.get('reason'),
                     cache_status=payload.get('cache_status'), items=len(payload.get('items') or []),
                     content_lengths=[len(item.get('summary') or '') for item in (payload.get('items') or [])[:2]])
                return {**payload, 'items': (payload.get('items') or [])[:2]}
            return load
        ops.get_stock_news, ops.get_press_releases = map(bounded, originals)
        with engine.connect() as guard:
            if not guard.scalar(text('SELECT pg_try_advisory_lock(84193639)')):
                emit('busy', reason='Scheduled refresh already owns the worker lock')
                return
            guard.detach()
            try:
                for symbol in ('MU', 'NVDA', 'AAPL'):
                    with SessionLocal() as db:
                        security = db.scalar(select(Security).where(Security.symbol == symbol))
                        if not security:
                            continue
                        tick = time.monotonic()
                        outcome = ops.refresh_operational_intelligence(db, security_id=security.id, source_types={'earnings_transcript'} if mode == 'pilot_transcripts' else None)
                        emit('pilot_result', symbol=symbol, seconds=round(time.monotonic()-tick, 2), **outcome)
                        emit('document_outcomes', symbol=symbol, results=[{'status': status, 'reason': reason, 'count': count} for status, reason, count in db.execute(select(ResearchSourceDocument.processing_status, ResearchSourceDocument.failure_reason, func.count()).where(ResearchSourceDocument.security_id == security.id).group_by(ResearchSourceDocument.processing_status, ResearchSourceDocument.failure_reason))])
                        emit('public_result', **ops.ticker_operational_intelligence(db, security=security))
            finally:
                guard.execute(text('SELECT pg_advisory_unlock(84193639)'))
    with SessionLocal() as db:
        if inspect(engine).has_table('openai_request_audit'):
            emit('audit_schema', columns=[{'name': c['name'], 'type': str(c['type'])} for c in inspect(engine).get_columns('openai_request_audit')],
                 pool_size=engine.pool.size(), pool_checked_out=engine.pool.checkedout(),
                 total=db.scalar(text('SELECT count(*) FROM openai_request_audit')),
                 latest=str(db.scalar(text('SELECT max(created_at) FROM openai_request_audit'))))
            rows = db.execute(text("SELECT feature, model, status_code, succeeded, duration_ms, usage_json FROM openai_request_audit WHERE feature IN ('research_evidence','research_claim_matching') AND created_at >= :since ORDER BY created_at DESC LIMIT 25"), {'since': (started-timedelta(hours=2) if mode == 'diagnose' else started).isoformat()}).mappings().all()
            emit('model_usage', requests=[dict(row) for row in rows])
            # Evidence extraction uses public source material, not private theses.
            # Keep private compiler/matching request errors out of operator logs.
            failures = db.execute(text("SELECT status_code, error FROM openai_request_audit WHERE feature = 'research_evidence' AND status_code >= 400 AND created_at >= :since ORDER BY created_at DESC LIMIT 5"), {'since': (started-timedelta(hours=2)).isoformat()}).mappings().all()
            emit('evidence_provider_failures', requests=[dict(row) for row in failures])
        emit('private_matching', total_matches=db.scalar(select(func.count()).select_from(ResearchClaimEvidenceMatch)),
             new_matches=db.scalar(select(func.count()).select_from(ResearchClaimEvidenceMatch).where(ResearchClaimEvidenceMatch.created_at >= started)))


if __name__ == '__main__':
    main()
