"""Keep persisted issuer fixture receipts on their simulated discovery clock."""
import pytest
from sqlalchemy import event
from app.services import issuer_discovery as discovery, direct_feed_store as store
from app.services.direct_feed_store import DirectFeedRevision


@pytest.fixture(autouse=True)
def source_clock(monkeypatch):
    monkeypatch.setattr(store, 'utcnow', lambda: discovery.utcnow())
    def stamp(mapper, connection, target):
        target.fetched_at = discovery.utcnow()
    event.listen(DirectFeedRevision, 'before_insert', stamp)
    try:
        yield
    finally:
        event.remove(DirectFeedRevision, 'before_insert', stamp)
