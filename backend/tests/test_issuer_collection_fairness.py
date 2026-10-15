"""Reviewed pages must not starve behind discovery, within one shared budget."""
from datetime import date, timedelta
from unittest.mock import patch
import json
import socket

import pytest
from sqlalchemy import select
from app.clients.direct_sources import DirectSourceError
from app.services import direct_feed_collection as collection, direct_feed_store as store
from app.services.direct_feed_store import DirectFeedDocument as Doc
from test_issuer_discovery import setup, company, Client, NOW, discovery
from test_issuer_pdf import pdf


class FairClient(Client):
    def __init__(self, db, *, fail_static=False):
        super().__init__(db)
        self.static = company()['documents'][0]['url']
        self.fail_static = fail_static

    def get(self, url):
        if url != self.static:
            return super().get(url)
        assert not self.db.in_transaction()
        self.calls.append(url)
        if self.fail_static:
            raise DirectSourceError('Source HTTP 429: synthetic reviewed page')
        return pdf("TRANSCRIPT 07 - 30 - 2026 Reddit, Inc. Reddit Q2'26 Earnings Call "
            'Operator: (Operator Instructions) Our first question comes from Tom Champion from Piper Sandler. '
            + 'Synthetic earnings evidence. '*60)


@pytest.fixture(autouse=True)
def clocks(monkeypatch):
    monkeypatch.setenv('ISSUER_TRANSCRIPT_DISCOVERY_ENABLED', '1')
    monkeypatch.setattr(collection, 'utcnow', lambda: NOW)
    monkeypatch.setattr(store, 'utcnow', lambda: NOW)
    def deny(*a, **kw): raise AssertionError('Network not allowed')
    monkeypatch.setattr(socket.socket, 'connect', deny)


def run(db, client, **kwargs):
    return collection.collect_direct_feeds(db, client, sources=['issuer_earnings'],
        start=date(2026,10,10), end=date(2026,10,10), issuer_registry=[company()],
        limit=kwargs.pop('limit',2), **kwargs)


def static_row(db):
    return db.scalar(select(Doc).where(Doc.source_key=='RDDT:earnings_transcript:2026:Q2'))


def test_new_reviewed_page_gets_budget_while_discovery_remains_pending(setup):
    client = FairClient(setup)
    first = run(setup,client)
    assert len(client.calls)==2 and client.calls[-1]==client.static
    assert first['issuer_discovery']['attempted']==1
    assert first['issuer_discovery']['reserved_static_requests']==1
    assert static_row(setup).status=='parsed'
    assert first['public_writes']==0 and not first['errors']
    second = run(setup,client)
    assert second['issuer_discovery']['attempted']==2
    assert second['issuer_discovery']['reserved_static_requests']==0
    assert client.calls.count(client.static)==1 and len(client.calls)==4
    before = [(r.id,r.content_hash,r.attempts,r.checked_at) for r in setup.scalars(select(Doc))]
    third = run(setup,client)
    assert third['processed']==0 and len(client.calls)==4
    assert before==[(r.id,r.content_hash,r.attempts,r.checked_at) for r in setup.scalars(select(Doc))]


@pytest.mark.parametrize('limit',[1,2])
def test_shared_limit_and_failed_reviewed_page_daily_attempt_are_preserved(setup,limit):
    client=FairClient(setup,fail_static=True)
    first=run(setup,client,limit=limit,recheck_hours=1,retry_failed=True)
    assert len(client.calls)==limit and client.calls.count(client.static)==1
    assert first['issuer_discovery']['attempted']==limit-1
    row=static_row(setup)
    assert row.status=='failed' and row.attempts==1
    previous=len(client.calls)
    run(setup,client,limit=limit,recheck_hours=1,retry_failed=True)
    assert len(client.calls)-previous<=limit and client.calls.count(client.static)==1
    assert static_row(setup).attempts==1


@pytest.mark.parametrize('hours,due',[(23.9,False),(24,True),(47.9,True)])
def test_reviewed_reservation_obeys_daily_boundary(setup,hours,due):
    client=FairClient(setup);run(setup,client)
    row=static_row(setup);row.checked_at=NOW-timedelta(hours=hours);setup.commit()
    assert collection._issuer_static_due(setup,[company()],recheck_hours=1) is due


def test_configured_longer_recheck_and_removed_registry_keep_their_scope(setup):
    client=FairClient(setup);run(setup,client)
    row=static_row(setup);row.checked_at=NOW-timedelta(hours=30);setup.commit()
    assert not collection._issuer_static_due(setup,[company()],recheck_hours=48)
    assert not collection._issuer_static_due(setup,[],recheck_hours=24)
    row.checked_at=NOW-timedelta(hours=48);setup.commit()
    assert collection._issuer_static_due(setup,[company()],recheck_hours=48)


def test_discovery_cooldown_does_not_spend_reserved_reviewed_slot(setup):
    client=FairClient(setup)
    def denied(url):
        client.calls.append(url)
        raise DirectSourceError('Source HTTP 429: synthetic index')
    client.get=denied
    result=run(setup,client)
    assert len(client.calls)==1 and client.static not in client.calls
    assert result['issuer_discovery']['reserved_static_requests']==1
    assert result['issuer_discovery']['stop_reason']
    assert static_row(setup).checked_at is None


def test_discovery_disabled_retains_existing_reviewed_only_behavior(setup,monkeypatch):
    monkeypatch.delenv('ISSUER_TRANSCRIPT_DISCOVERY_ENABLED')
    client=FairClient(setup)
    with patch.object(collection,'_issuer_static_due',side_effect=AssertionError('Unexpected discovery reservation')):
        result=run(setup,client)
    assert client.calls==[client.static] and not result['errors']
    assert result['issuer_discovery']['attempted']==0
