"""Real PostgreSQL session recovery, restricted to the disposable loopback DB."""
import argparse
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import sys
import time
from unittest.mock import patch

p = argparse.ArgumentParser()
p.add_argument('--backend', type=Path, required=True)
p.add_argument('--output', type=Path, required=True)
p.add_argument('--port', type=int, choices=[61982, 61983], default=61982)
a = p.parse_args()
assert not a.output.exists()
sys.path.insert(0, str(a.backend.resolve()))
os.environ['DATABASE_URL'] = 'sqlite:///:memory:'
from sqlalchemy import create_engine, text
from app.jobs import collect_direct_feeds as job

url = f'postgresql+psycopg://postgres@127.0.0.1:{a.port}/walnut_sec_replay'
control = create_engine(url, connect_args={'connect_timeout': 5})
owner_engine = create_engine(url, connect_args={'application_name': 'walnut:cron:testmachine:123'})
owner = owner_engine.connect()
checks = []
try:
    assert owner.scalar(text('SELECT current_database()')) == 'walnut_sec_replay'
    assert owner.scalar(text('SELECT pg_try_advisory_lock(84193647)'))
    with control.connect() as guard, patch.dict(os.environ, FLY_PROCESS_GROUP='cron', FLY_MACHINE_ID='testmachine'):
        old_boot = int((datetime.now(timezone.utc)-timedelta(days=1)).timestamp())
        new_boot = int((datetime.now(timezone.utc)+timedelta(seconds=5)).timestamp())
        with patch.object(job.Path, 'read_text', return_value=f'btime {old_boot}\n'):
            assert job._recover_previous_boot_collector(guard) is False
            checks.append('live_boot_preserved')
        with patch.object(job.Path, 'read_text', return_value=f'btime {new_boot}\n'):
            with job.collector_lock(control, recover_orphaned=False) as acquired:
                assert acquired is False
            checks.append('read_only_preview_does_not_recover')
            with patch.dict(os.environ, FLY_MACHINE_ID='othermachine'):
                assert job._recover_previous_boot_collector(guard) is False
                checks.append('other_machine_preserved')
            owner.scalar(text('SELECT 1'))
            guard.execute(text('SELECT pg_stat_clear_snapshot()'))
            assert job._recover_previous_boot_collector(guard) is False
            checks.append('non_lock_session_preserved')
            # Avoid taking the same session lock twice.
            assert owner.scalar(text('SELECT pg_advisory_unlock(84193647)'))
            assert owner.scalar(text('SELECT pg_try_advisory_lock(84193647)'))
            guard.execute(text('SELECT pg_stat_clear_snapshot()'))
            assert job._recover_previous_boot_collector(guard) is True
            checks.append('matching_dead_boot_recovered')
            for _ in range(20):
                if guard.scalar(text('SELECT pg_try_advisory_lock(84193647)')):
                    break
                time.sleep(.05)
            else:
                raise AssertionError('Recovered lock remained unavailable')
            assert guard.scalar(text('SELECT pg_advisory_unlock(84193647)'))
            checks.append('lock_reacquired')
            guard.execute(text('SELECT pg_stat_clear_snapshot()'))
            assert job._recover_previous_boot_collector(guard) is False
            checks.append('repeat_noop')
finally:
    owner.invalidate()
    owner.close()
    owner_engine.dispose()
    control.dispose()
report = {'status': 'passed', 'database_scope': 'disposable_loopback', 'checks': checks,
          'production_writes': 0, 'http_calls': 0, 'emails': 0}
a.output.write_text(json.dumps(report, indent=2), encoding='utf-8')
print(json.dumps(report))
