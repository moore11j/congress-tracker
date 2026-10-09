"""Read-only SEC preparation evidence, exporting public financial caches only."""
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import platform

files = ['app/jobs/warm_sec_research.py', 'app/services/sec_directory.py',
         'app/services/sec_financial_statements.py', 'app/services/sec_fundamentals.py',
         'app/services/ticker_financials.py', 'crontab']
report = {'observed_at': datetime.now(timezone.utc).isoformat(), 'runtime': platform.python_version(),
    'hashes': {name: hashlib.sha256(Path('/app', name).read_bytes()).hexdigest() for name in files},
    'flags': {key: os.getenv(key) for key in ('SEC_RESEARCH_WARMING_ENABLED',
        'FINANCIAL_STATEMENTS_PROVIDER', 'COMPANY_METADATA_PROVIDER', 'FUNDAMENTALS_PROVIDER', 'STOCK_PRICE_PROVIDER')}}
if os.getenv('SEC_FINANCIAL_HASH_ONLY') != '1':
    from sqlalchemy import select, text
    from sqlalchemy.orm import Session
    from app.db import engine
    from app.models import InsightsSnapshot
    assert engine.dialect.name == 'postgresql'
    with engine.connect() as conn, conn.begin():
        conn.execute(text('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY'))
        conn.execute(text("SET LOCAL statement_timeout='20s'"))
        assert conn.scalar(text('SHOW transaction_read_only')) == 'on'
        with Session(bind=conn) as db:
            row = db.get(InsightsSnapshot, 'sec-research:warming:v1')
            state = json.loads(row.payload_json) if row else {}
            report['runs'] = state.get('runs', [])[-5:]
            report['lease_until'] = (state.get('lease') or {}).get('until')
            report['attempted_symbols'] = sorted(state.get('attempted_at', {}))
            rows = list(db.scalars(select(InsightsSnapshot).where(
                InsightsSnapshot.kind.like('sec-financials:%'), InsightsSnapshot.source == 'sec_edgar')
                .order_by(InsightsSnapshot.kind).limit(101)))
            report['public_caches'] = [{'kind': row.kind, 'fetched_at': str(row.fetched_at),
                                       'payload': json.loads(row.payload_json)} for row in rows]
            report['cache_count'] = len(rows)
            report['caches_truncated'] = len(rows) == 101
            report['coverage_counts'] = dict(Counter(row['payload']['status'] for row in report['public_caches']))
            report.update(transaction_read_only=True, database_writes=0, customer_rows_exported=0)
print('SEC_FINANCIAL_RECEIPT=' + json.dumps(report))
