from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
import gzip
import hashlib
import json
from pathlib import Path

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from app.models import IndexMembership
from app.services import index_memberships as service

ROOT = Path(__file__).parent / 'fixtures' / 'index_membership_originals'
NOW = datetime(2026, 11, 7, 6, 32, 37, tzinfo=timezone.utc)


@pytest.fixture
def db():
    engine = create_engine('sqlite://')
    IndexMembership.__table__.create(engine)
    with Session(engine) as session:
        yield session
    engine.dispose()


def original(code='sp500'):
    manifest = json.loads((ROOT / 'sources.json').read_text(encoding='utf-8'))
    source = next(item for item in manifest['sources'] if item['code'] == code)
    content = gzip.decompress((ROOT / (code + '.json.gz')).read_bytes())
    assert len(content) == source['bytes']
    assert hashlib.sha256(content).hexdigest() == source['sha256']
    return service.parse_wikipedia_membership_payload(code, json.loads(content),
        source_page=service.WIKIPEDIA_PAGES[code], retrieved_at=NOW)


def fingerprint(db):
    return [(row.id, row.symbol, row.effective_from, row.effective_to, row.source,
             row.source_revision_id, row.source_as_of, row.refreshed_at, row.is_active)
            for row in db.query(IndexMembership).order_by(IndexMembership.id)]


def test_complete_current_sources_replace_captured_membership_preserving_history_and_repeat(db):
    from app.services.market_pressure import _resolve_universe_symbols, _apply_universe_symbol_policy
    manifest = json.loads((ROOT / 'sources.json').read_text(encoding='utf-8'))['active_baseline']
    body = gzip.decompress((ROOT / 'active-baseline.json.gz').read_bytes())
    assert len(body) == manifest['bytes'] and hashlib.sha256(body).hexdigest() == manifest['sha256']
    rows = json.loads(body)['rows']
    assert len(rows) == manifest['rows'] == 606
    for raw in rows:
        value = dict(raw)
        for key in ['effective_from', 'effective_to', 'source_as_of']:
            if value[key]:
                value[key] = date.fromisoformat(value[key])
        value['refreshed_at'] = datetime.fromisoformat(value['refreshed_at'])
        db.add(IndexMembership(**value))
    db.commit()
    before = {(row.index_code, row.symbol): (row.id, row.effective_from) for row in db.query(IndexMembership)}
    for code in service.INDEX_UNIVERSES:
        data = original(code)
        result = service.refresh_index_memberships_from_dataset(db, data)
        assert result.status == 'ok'
        active = db.query(IndexMembership).filter_by(index_code=code, is_active=True).all()
        assert {row.symbol for row in active} == set(data.symbols)
        for row in active:
            if (code, row.symbol) in before:
                assert (row.id, row.effective_from) == before[(code, row.symbol)]
            else:
                assert row.effective_from == NOW.date()
        ended = db.query(IndexMembership).filter_by(index_code=code, is_active=False).all()
        assert {row.symbol for row in ended} == {symbol for scope, symbol in before if scope == code} - set(data.symbols)
        assert all(row.effective_to == NOW.date() for row in ended)
        expected = set(_apply_universe_symbol_policy(code, sorted(data.symbols)))
        assert set(_resolve_universe_symbols(db, code, user=None)) == expected
        after = fingerprint(db)
        assert service.refresh_index_memberships_from_dataset(db, data).status == 'unchanged'
        assert fingerprint(db) == after


@pytest.mark.parametrize('code,count', [('sp500', 503), ('nasdaq100', 101)])
def test_complete_sources_repeat_preserves_ids_and_renews_verified_clock(db, code, count):
    current = original(code)
    old = replace(current, retrieved_at=NOW-timedelta(days=10), source_as_of=(NOW-timedelta(days=10)).date())
    assert service.refresh_index_memberships_from_dataset(db, old).status == 'ok'
    before = fingerprint(db)
    assert service.active_index_membership_snapshot(db, code, now=NOW).status == 'stale'
    assert service.refresh_index_memberships_from_dataset(db, current).status == 'unchanged'
    after = fingerprint(db)
    assert len(after) == count
    assert [row[:7] + row[8:] for row in after] == [row[:7] + row[8:] for row in before]
    assert service.active_index_membership_snapshot(db, code, now=NOW).status == 'available'
    assert all(row[7] == NOW.replace(tzinfo=None) for row in after)
    assert service.refresh_index_memberships_from_dataset(db, current).status == 'unchanged'
    assert fingerprint(db) == after
    service.refresh_index_memberships_from_dataset(db, old)
    assert fingerprint(db) == after


@pytest.mark.parametrize('revision', [None, 'same', 'new'])
def test_dry_run_never_updates_freshness_or_memberships(db, revision):
    data = original()
    service.refresh_index_memberships_from_dataset(db, data)
    before = fingerprint(db)
    candidate = replace(data, retrieved_at=NOW+timedelta(days=10))
    if revision == 'new':
        candidate = replace(candidate, source_revision_id='different')
    elif revision is None:
        candidate = replace(candidate, records=[])
    service.refresh_index_memberships_from_dataset(db, candidate, dry_run=True)
    assert fingerprint(db) == before


def test_failed_unchanged_clock_commit_rolls_back(db, monkeypatch):
    data = original()
    service.refresh_index_memberships_from_dataset(db, data)
    before = fingerprint(db)
    def fail():
        raise RuntimeError('commit failed')
    monkeypatch.setattr(db, 'commit', fail)
    with pytest.raises(RuntimeError):
        service.refresh_index_memberships_from_dataset(db, replace(data, retrieved_at=NOW+timedelta(days=10)))
    assert fingerprint(db) == before


def test_revision_collision_between_providers_does_not_skip_metadata(db):
    data = original()
    service.refresh_index_memberships_from_dataset(db, replace(data, source='fixture', source_kind='fixture'))
    assert service.refresh_index_memberships_from_dataset(db, data).status == 'ok'
    assert service.active_index_membership_snapshot(db, 'sp500', now=NOW).source == 'wikipedia'


def test_due_checks_skip_fresh_unselected_paid_and_etf_scopes(db, monkeypatch):
    data = original()
    service.refresh_index_memberships_from_dataset(db, data)
    calls = []
    def fetch(self, code):
        assert self.max_retries == 0
        calls.append(code)
        return replace(original(code), retrieved_at=NOW+timedelta(hours=24))
    monkeypatch.setattr(service.WikipediaIndexMembershipProvider, 'fetch_memberships', fetch)
    assert service.refresh_due_wikipedia_memberships(db, universes=['etf'], now=NOW) == []
    assert service.refresh_due_wikipedia_memberships(db, universes=['sp500'], now=NOW+timedelta(hours=23)) == []
    monkeypatch.setenv('INDEX_MEMBERSHIP_NASDAQ100_SOURCE', 'fmp')
    assert service.refresh_due_wikipedia_memberships(db, now=NOW+timedelta(hours=24))[0].status == 'unchanged'
    assert calls == ['sp500']
    assert service.refresh_due_wikipedia_memberships(db, now=NOW+timedelta(hours=25)) == []


def test_failed_source_keeps_stale_data_and_does_not_claim_refresh(db, monkeypatch):
    data = original()
    service.refresh_index_memberships_from_dataset(db, data)
    before = fingerprint(db)
    def fail(self, code):
        raise ValueError('wikipedia_http_error:429')
    monkeypatch.setattr(service.WikipediaIndexMembershipProvider, 'fetch_memberships', fail)
    results = service.refresh_due_wikipedia_memberships(db, universes=['sp500'], now=NOW+timedelta(days=10))
    assert results[0].status == 'failed'
    assert fingerprint(db) == before
    assert service.active_index_membership_snapshot(db, 'sp500', now=NOW+timedelta(days=10)).status == 'stale'


@pytest.mark.parametrize('opened,force,market_only,expected', [
    (True, False, True, ['memberships', 'prices']),
    (False, False, True, ['prices']),
    (False, True, True, ['memberships', 'prices']),
    (False, False, False, ['memberships', 'prices']),
])
def test_existing_background_job_checks_memberships_before_prices_only_when_running(db, monkeypatch, opened, force, market_only, expected):
    from app.jobs import refresh_market_pressure_snapshots as job
    calls = []
    monkeypatch.setattr(job.Base.metadata, 'create_all', lambda **kwargs: None)
    for name in ['ensure_market_pressure_snapshot_schema', 'ensure_quote_cache_market_cap_schema', 'ensure_price_cache_volume_columns']:
        monkeypatch.setattr(job, name, lambda *_: None)
    monkeypatch.setattr(job, 'SessionLocal', lambda: db)
    monkeypatch.setattr(job, 'is_market_hours', lambda: opened)
    def members(session, *, universes):
        assert session is db and universes == ['nasdaq100']
        calls.append('memberships')
        return []
    def prices(session, **kwargs):
        assert session is db and kwargs['universes'] == ['nasdaq100']
        calls.append('prices')
        return []
    monkeypatch.setattr(job, 'refresh_due_wikipedia_memberships', members)
    monkeypatch.setattr(job, 'refresh_market_pressure_snapshots', prices)
    assert job.run(universes=['nasdaq100'], force=force, market_hours_only=market_only) == []
    assert calls == expected


@pytest.mark.parametrize('status,body,reason', [
    (429, b'', 'wikipedia_http_error:429'),
    (403, b'', 'wikipedia_http_error:403'),
    (302, b'', 'wikipedia_http_error:302'),
    (200, b'not-json', 'wikipedia_invalid_json'),
    (200, b'x' * (service.WIKIPEDIA_MAX_RESPONSE_BYTES+1), 'wikipedia_response_too_large'),
], ids=['rate-limit', 'forbidden', 'redirect', 'invalid-json', 'oversize'])
def test_bounded_source_transport_stops_on_denial_malformed_and_oversize(status, body, reason):
    closed, calls = [], []
    class Response:
        status_code = status
        def iter_content(self, size):
            for i in range(0, len(body), size):
                yield body[i:i+size]
        def close(self):
            closed.append(True)
    class Transport:
        def get(self, *args, **kwargs):
            assert kwargs['allow_redirects'] is False and kwargs['stream'] is True
            calls.append(True)
            return Response()
    with pytest.raises(ValueError, match=reason):
        service.WikipediaIndexMembershipProvider(max_retries=0, session=Transport()).fetch_memberships('sp500')
    assert len(calls) == len(closed) == 1
