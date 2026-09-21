from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine
from sqlalchemy.pool import QueuePool

from app.jobs.refresh_research_operational_intelligence import operational_refresh_lock


@pytest.mark.parametrize('raises', [False, True])
def test_lock_leaves_both_cron_pool_slots_available_and_closes_on_failure(raises):
    pool = create_engine('sqlite://', poolclass=QueuePool, pool_size=2, max_overflow=0, pool_timeout=0.1)
    calls = []
    class Guard:
        def __enter__(self):
            self.connection = pool.connect()
            return self
        def scalar(self, query):
            calls.append('lock')
            return True
        def detach(self):
            self.connection.detach()
        def execute(self, query):
            calls.append('unlock')
        def __exit__(self, *_args):
            self.connection.close()
    bind = SimpleNamespace(dialect=SimpleNamespace(name='postgresql'), connect=Guard)
    try:
        try:
            with operational_refresh_lock(bind) as acquired:
                assert acquired
                with pool.connect() as work, pool.connect() as audit:
                    assert not work.closed and not audit.closed
                    if raises:
                        raise ValueError('simulated worker failure')
        except ValueError:
            assert raises
        assert calls == ['lock', 'unlock']
        assert pool.pool.checkedout() == 0
    finally:
        pool.dispose()
