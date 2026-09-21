"""Operator-only bounded production pilot. Logs aggregates, never private thesis prose."""
import json
import os
import sys
import time
from datetime import datetime, timezone

sys.path.insert(0, '/app')
from sqlalchemy import func, inspect, select, text
from app.db import SessionLocal, engine
from app.models import ResearchSourceDocument, ResearchThesis, ResearchThesisClaim, ResearchClaimEvidenceMatch, Security, WatchlistItem
from app.services import operational_intelligence as ops
from app.services.ai_marketing import OPENAI_API_KEY, resolved_setting_value


def emit(kind, **values):
    print(json.dumps({'kind': kind, **values}, default=str, sort_keys=True), flush=True)


def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else 'diagnose'
    if mode not in {'diagnose', 'pilot'}:
        raise ValueError('Unsupported pilot mode')
    started = datetime.now(timezone.utc)
    with SessionLocal() as db:
        emit('configuration', operational_enabled=ops.operational_intelligence_enabled(),
             matching_enabled=ops.claim_matching_enabled(), transcripts_enabled=ops.transcript_analysis_enabled(),
             fmp_configured=bool(os.getenv('FMP_API_KEY')), openai_configured=bool(resolved_setting_value(db, OPENAI_API_KEY)),
             thesis_status_counts=dict(db.execute(select(ResearchThesis.status, func.count()).group_by(ResearchThesis.status)).all()),
             claims=db.scalar(select(func.count()).select_from(ResearchThesisClaim)),
             scheduler_candidates=len(ops.candidate_securities(db, limit=250)))
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
    if mode == 'pilot':
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
            try:
                for symbol in ('MU', 'NVDA', 'AAPL'):
                    with SessionLocal() as db:
                        security = db.scalar(select(Security).where(Security.symbol == symbol))
                        if not security:
                            continue
                        tick = time.monotonic()
                        outcome = ops.refresh_operational_intelligence(db, security_id=security.id)
                        emit('pilot_result', symbol=symbol, seconds=round(time.monotonic()-tick, 2), **outcome)
                        emit('public_result', **ops.ticker_operational_intelligence(db, security=security))
            finally:
                guard.execute(text('SELECT pg_advisory_unlock(84193639)'))
    with SessionLocal() as db:
        if inspect(engine).has_table('openai_request_audit'):
            rows = db.execute(text("SELECT feature, model, status_code, succeeded, duration_ms, usage_json FROM openai_request_audit WHERE feature IN ('research_evidence','research_claim_matching') AND created_at >= :since ORDER BY created_at DESC LIMIT 25"), {'since': started.isoformat()}).mappings().all()
            emit('model_usage', requests=[dict(row) for row in rows])
        emit('private_matching', total_matches=db.scalar(select(func.count()).select_from(ResearchClaimEvidenceMatch)),
             new_matches=db.scalar(select(func.count()).select_from(ResearchClaimEvidenceMatch).where(ResearchClaimEvidenceMatch.created_at >= started)))


if __name__ == '__main__':
    main()
