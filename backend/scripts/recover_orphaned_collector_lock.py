"""One observed dead-boot lock session only; preview unless explicitly enabled."""
from datetime import datetime, timezone
import json
import os
from pathlib import Path
from sqlalchemy import text
from app.db import engine

assert os.getenv('FLY_MACHINE_ID') == '807d42ce974018'
assert os.getenv('FLY_PROCESS_GROUP') == 'cron'
assert engine.dialect.name == 'postgresql'
expected = datetime.fromisoformat('2026-10-09T22:20:03.359445+00:00')
boot = datetime.fromtimestamp(int(next(x.split()[1] for x in Path('/proc/stat').read_text().splitlines() if x.startswith('btime '))), timezone.utc)
assert boot > expected, 'Current boot does not prove owner death'
apply = os.getenv('APPLY_ORPHANED_COLLECTOR_RECOVERY') == '1'
query = """SELECT a.pid FROM pg_stat_activity a
    WHERE a.pid=18012 AND a.application_name='walnut:cron:807d42ce974018:751'
      AND a.backend_start=:expected AND a.usename=current_user
      AND a.state='idle in transaction'
      AND trim(a.query)='SELECT pg_try_advisory_lock(84193647)'
      AND EXISTS (SELECT 1 FROM pg_locks l WHERE l.pid=a.pid
                  AND l.locktype='advisory' AND l.objid=84193647 AND l.granted)"""
with engine.connect() as conn, conn.begin():
    if not apply:
        conn.execute(text('SET TRANSACTION READ ONLY'))
    conn.execute(text("SET LOCAL statement_timeout='10s'"))
    pid = conn.scalar(text(query), {'expected': expected})
    report = {'observed_at': datetime.now(timezone.utc).isoformat(), 'machine_boot': boot.isoformat(),
        'expected_session_start': expected.isoformat(), 'mode': 'apply' if apply else 'preview',
        'status': 'eligible' if pid else 'no_matching_orphan', 'terminated_sessions': 0,
        'canonical_writes': 0, 'emails': 0}
    if pid and apply:
        result = conn.scalar(text('SELECT pg_terminate_backend(pid) FROM (' + query + ') target'), {'expected': expected})
        assert result is True
        report.update(status='recovered', terminated_sessions=1)
print('COLLECTOR_RECOVERY=' + json.dumps(report))
