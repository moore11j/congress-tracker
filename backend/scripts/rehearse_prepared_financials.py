"""Exercise selected financial consumers using a read-only public-cache receipt."""
import argparse
from collections import Counter
from contextlib import ExitStack
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import sys
from unittest.mock import patch

p = argparse.ArgumentParser()
p.add_argument('--backend', type=Path, required=True)
p.add_argument('--receipt', type=Path, required=True)
p.add_argument('--output', type=Path, required=True)
a = p.parse_args()
assert not a.output.exists()
sys.path.insert(0, str(a.backend.resolve()))
os.environ.update(DATABASE_URL='sqlite:///:memory:', FINANCIAL_STATEMENTS_PROVIDER='sec_edgar', FMP_PROVIDER_DISABLED='1')
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from app import main as api
from app.db import Base
from app.models import InsightsSnapshot, TickerFinancialsCache
from app.request_priority import set_request_context, reset_request_context
from app.services import sec_financial_statements as sec
from app.services import research_briefs, ticker_hydration

receipt = json.loads(a.receipt.read_text(encoding='utf-8'))
assert receipt['transaction_read_only'] and not receipt['caches_truncated']
engine = create_engine('sqlite:///:memory:')
Base.metadata.create_all(engine, tables=[InsightsSnapshot.__table__, TickerFinancialsCache.__table__])
factory = sessionmaker(bind=engine)
legacy = json.dumps({'symbol': 'AAPL', 'summary': {'revenueTtm': 123, 'forwardPE': 25}})
with factory() as db:
    for row in receipt['public_caches']:
        db.add(InsightsSnapshot(kind=row['kind'], source='sec_edgar', fetched_at=datetime.fromisoformat(row['fetched_at']), payload_json=json.dumps(row['payload'])))
    db.add(TickerFinancialsCache(symbol='AAPL', status='ok', fetched_at=datetime.now(timezone.utc), payload_json=legacy))
    db.commit()

panels = {}
with ExitStack() as stack:
    stack.enter_context(patch.object(sec, 'SessionLocal', factory))
    stack.enter_context(patch('app.services.data_enrichment_queue.enqueue_data_enrichment_job', side_effect=AssertionError('Unexpected queue write')))
    stack.enter_context(patch('requests.sessions.Session.request', side_effect=AssertionError('Unexpected HTTP')))
    stack.enter_context(patch.object(sec.DirectSourceClient, 'get', side_effect=AssertionError('Unexpected source read')))
    token = set_request_context({'path': '/api/tickers/AAPL/financials', 'request_source': 'client', 'route_family': 'ticker', 'panel': 'TickerFinancialsPanel'})
    try:
        with factory() as db:
            for row in receipt['public_caches']:
                expected = row['payload']
                symbol = expected['symbol']
                result = api.ticker_financials(symbol)
                assert result['symbol'] == symbol and result['source'] == 'sec_edgar'
                assert result == api.ticker_financials(symbol)
                comparison = api._peer_compare_financials_fallbacks(db, symbol)
                assert comparison.get('forward_pe') is None
                context = research_briefs._cached_financials_snapshot(db, symbol)
                assert context['source'] == 'sec_edgar' and context['status'] == expected['status']
                state = ticker_hydration._financials_content_state(db, symbol, {}, {})
                assert state == ('ok' if expected['status'] in {'ok', 'partial'} else 'unavailable')
                diagnostic = api._ticker_debug_financials_status(db, symbol)
                assert diagnostic['present'] and diagnostic['status'] == expected['status']
                assert result.get('estimates', []) == []
                panels[symbol] = {'prepared_status': expected['status'], 'public_status': result['status'], 'annual_periods': len(result.get('annual', [])), 'quarterly_periods': len(result.get('quarterly', [])), 'comparison': comparison}
            assert db.get(TickerFinancialsCache, 'AAPL').payload_json == legacy
            with patch.dict(os.environ, FINANCIAL_STATEMENTS_PROVIDER='fmp', FMP_PROVIDER_DISABLED='0'):
                assert api._peer_compare_financials_fallbacks(db, 'AAPL')['revenue_ttm'] == 123
            assert db.get(TickerFinancialsCache, 'AAPL').payload_json == legacy
    finally:
        reset_request_context(token)

report = {'status': 'passed', 'observed_source_at': receipt['observed_at'], 'panels_checked': len(panels), 'public_status_counts': dict(Counter(v['public_status'] for v in panels.values())), 'panels': panels, 'legacy_cache_preserved': True, 'repeat_stable': True, 'http_calls': 0, 'queue_writes': 0, 'emails': 0, 'production_writes': 0}
a.output.write_text(json.dumps(report, indent=2), encoding='utf-8')
print(json.dumps({k: v for k, v in report.items() if k != 'panels'}))
engine.dispose()
