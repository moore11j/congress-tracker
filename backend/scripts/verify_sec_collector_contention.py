"""Aggregate-only read of the shared SEC collector lock and recent runs."""
from datetime import datetime, timezone
import json
from sqlalchemy import select, text
from sqlalchemy.orm import Session
from app.db import engine
from app.services.direct_feed_store import DirectFeedRun

with engine.connect() as conn, conn.begin():
    conn.execute(text('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY'))
    conn.execute(text("SET LOCAL statement_timeout='20s'"))
    assert conn.scalar(text('SHOW transaction_read_only')) == 'on'
    holders = conn.execute(text("""SELECT a.pid, a.application_name, a.backend_start, a.state, a.wait_event_type,
        extract(epoch from (now()-a.xact_start)) AS transaction_age_seconds,
        extract(epoch from (now()-a.state_change)) AS state_age_seconds
        FROM pg_locks l JOIN pg_stat_activity a ON a.pid=l.pid
        WHERE l.locktype='advisory' AND l.objid=84193647 AND l.granted""")).mappings().all()
    with Session(bind=conn) as db:
        runs = list(db.scalars(select(DirectFeedRun).order_by(DirectFeedRun.id.desc()).limit(8)))
        result = {'observed_at': datetime.now(timezone.utc).isoformat(),
            'lock_holders': [dict(x) for x in holders],
            'runs': [{'id': x.id, 'started_at': x.started_at, 'finished_at': x.finished_at,
                      'status': x.status, 'sources': json.loads(x.report_json).get('sources'),
                      'report_keys': list(json.loads(x.report_json).keys())} for x in runs],
            'transaction_read_only': True, 'database_writes': 0, 'customer_records': 0}
print('COLLECTOR_RECEIPT=' + json.dumps(result, default=str))
