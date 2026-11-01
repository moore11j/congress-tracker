import copy
from dataclasses import FrozenInstanceError
import hashlib
import json

import pytest
from sqlalchemy import func, select

from app.clients.direct_sources import DirectSourceError
from app.models import Event
from app.services import direct_13f_batch as batch
from app.services import direct_13f_evidence as evidence
from app.services.direct_feed_collection import parse_document
from app.services.direct_feed_store import DirectFeedRevision
from app.services.direct_feed_worker import DirectFeedPublication
from test_direct_13f_worker import db, stage, pair
from test_direct_13f_publication import document
from test_direct_13f_batch import _independent_peer


def prepare(sources, target):
    _, parsed, _ = parse_document('sec_13f', target['raw'], target['metadata'])
    cohort = evidence.prepare_streamed_peer_groups(iter(sources), scopes={
        parsed['metadata']['report_period']: {r['cusip'] for r in parsed['positions']}
    })[parsed['metadata']['report_period']]
    return parsed, cohort, evidence.Prepared13FEvidence.build_for_cohort([], cohort)


@pytest.mark.parametrize('scenario', ['one', 'duplicate_manager', 'future', 'disagreeing', 'confirmed'])
def test_streamed_values_equal_full_original_decisions(scenario):
    target = document(rows=[('000361105', 100, 100, '')])
    first = _independent_peer('0000000001', 2)
    sources = {'one': [first], 'duplicate_manager': [first, first],
        'future': [first, _independent_peer('0000000002', 3, filed='2026-10-07')],
        'disagreeing': [first, document(serial=3)],
        'confirmed': [first, _independent_peer('0000000002', 3)]}[scenario]
    parsed, cohort, prepared = prepare(sources, target)
    assert evidence.value_consistency_issues(parsed, cohort, prepared=prepared) == evidence.value_consistency_issues(parsed, sources)
    assert [r['content_hash'] for r in cohort] == [r['content_hash'] for r in sources]
    assert all('raw' not in r for r in cohort)


def test_streamed_summary_is_immutable_and_rejects_out_of_scope_targets():
    target = document(rows=[('000361105', 100, 100, '')])
    parsed, cohort, prepared = prepare([target], target)
    with pytest.raises(FrozenInstanceError): cohort.period = 'changed'
    with pytest.raises(FrozenInstanceError): cohort.records[0].compact = None
    rendered = next(iter(cohort)); rendered['metadata']['key'] = 'changed'
    assert next(iter(cohort))['metadata']['key'] == target['metadata']['key']
    with pytest.raises(DirectSourceError, match='do not match'):
        evidence.value_consistency_issues(parsed, cohort)
    changed = copy.deepcopy(parsed); changed['metadata']['report_period'] = '2026-06-30'
    with pytest.raises(DirectSourceError, match='scope'):
        evidence.value_consistency_issues(changed, cohort, prepared=prepared)
    changed = copy.deepcopy(parsed); changed['positions'][0]['cusip'] = '111111111'
    with pytest.raises(DirectSourceError, match='scope'):
        evidence.value_consistency_issues(changed, cohort, prepared=prepared)


def test_more_than_500_same_quarter_peers_keep_older_correlated_hold(db):
    target = document(rows=[('000361105', 100, 100, '')])
    target_id = stage(db, target)
    stage(db, _independent_peer('0000000001', 2))
    stage(db, _independent_peer('0000000002', 3))
    for serial in range(4, 505): stage(db, document(serial=serial))
    result = batch.publish_13f_batch(db, identifier_documents=[], limit=1)
    assert result['comparison_group_counts'] == {'2026-09-30': 504}
    assert result['results'][0]['document_id'] == target_id
    assert result['results'][0]['status'] == 'held'
    assert result['results'][0]['value_issues'][0]['peer_to_reported_unit_value_ratio'] == '1000'
    assert result['feed_events'] == 0


def test_complete_stream_passes_old_total_raw_bound_but_rejects_irrelevant_tamper(db, monkeypatch):
    prior, current = pair()
    stage(db, prior); stage(db, current)
    monkeypatch.setattr(batch, 'MAX_SCAN_BYTES', 1)
    with pytest.raises(ValueError, match='byte bounds'):
        batch.staged_comparison_groups(db, byte_budget=100_000_000, report_periods={'2026-09-30'})
    result = batch.publish_13f_batch(db, identifier_documents=[], limit=1)
    assert result['processed'] == 1
    revision = db.scalar(select(DirectFeedRevision).where(DirectFeedRevision.content_hash == prior['content_hash']))
    revision.source_bytes += b'changed'; db.commit()
    before = db.scalar(select(func.count()).select_from(DirectFeedPublication))
    with pytest.raises(ValueError, match='checksum'):
        batch.publish_13f_batch(db, identifier_documents=[], limit=1)
    assert db.scalar(select(func.count()).select_from(DirectFeedPublication)) == before
    assert db.scalar(select(func.count()).select_from(Event)) == 0


def test_over_100mb_originals_retain_only_small_complete_summaries():
    target = document(rows=[('000361105', 100, 100, '')])
    def sources():
        for serial in range(2, 8):
            source = _independent_peer(f'{serial:010}', serial)
            source['raw'] += b'\n' * 18_000_000
            source['content_hash'] = hashlib.sha256(source['raw']).hexdigest()
            yield source
    parsed, cohort, prepared = prepare(sources(), target)
    assert cohort.source_bytes > 100_000_000
    assert len(cohort) == 6 and cohort.summary_bytes < 20_000
    issues = evidence.value_consistency_issues(parsed, cohort, prepared=prepared)
    assert len(issues) == 1 and len(issues[0]['peer_accessions']) == 6


def test_original_cusip_case_is_distinct_from_reference_lookup_normalization(db):
    target = document(rows=[('000361105', 100, 100, '')])
    sources = [target, _independent_peer('0000000001', 2), _independent_peer('0000000002', 3)]
    for source in sources:
        source['raw'] = source['raw'].replace(b'000361105', b'02079k305')
        source['content_hash'] = hashlib.sha256(source['raw']).hexdigest()
        stage(db, source)
    _, parsed, _ = parse_document('sec_13f', target['raw'], target['metadata'])
    expected = evidence.value_consistency_issues(parsed, sources)
    assert expected and expected[0]['cusip'] == '02079k305'
    result = batch.publish_13f_batch(db, identifier_documents=[], limit=1)
    assert result['results'][0]['status'] == 'held'
    assert result['results'][0]['value_issues'] == expected


def test_shared_identifiers_preserve_case_totals_and_caller_decimal_context():
    from decimal import localcontext
    target = document(rows=[('02079k305', 3, 10, ''), ('02079K305', 7, 20, ''),
                            ('02079k305', 6, 5, ''), ('000361105', 0, 0, '')])
    parsed, cohort, _ = prepare([target], target)
    ordinary = evidence.prepare_streamed_peer_groups([target], scopes=None)[None]
    assert cohort.cusip_names == ('000361105', '02079K305', '02079k305')
    assert list(cohort) == list(ordinary)
    for precision in (7, 28, 50):
        with localcontext() as context:
            context.prec = precision
            assert list(cohort.values_for(parsed)) == list(ordinary.values_for(parsed))
    with pytest.raises(TypeError): cohort.cusip_names[0] = 'changed'
    rendered = list(cohort.values_for(parsed)); rendered[0][2]['02079k305'] = 999
    assert list(cohort.values_for(parsed)) == list(ordinary.values_for(parsed))


def test_shared_identifier_pool_is_included_in_exact_global_budget(monkeypatch):
    prior = document(quarter=2, serial=2, rows=[('000361106', 5, 20, '')])
    current = document(rows=[('000361105', 3, 10, '')])
    sources = [prior, current]
    scopes = {'2026-06-30': {'000361106'}, '2026-09-30': {'000361105'}}
    groups = evidence.prepare_streamed_peer_groups(sources, scopes=scopes)
    bound = sum(group.summary_bytes for group in groups.values())
    monkeypatch.setattr(evidence, 'MAX_STREAM_SUMMARY_BYTES', bound)
    repeated = evidence.prepare_streamed_peer_groups(sources, scopes=scopes)
    assert repeated == groups
    for source in sources:
        _, parsed, _ = parse_document('sec_13f', source['raw'], source['metadata'])
        period = parsed['metadata']['report_period']
        ordinary = evidence.prepare_streamed_peer_groups([source], scopes=None)[None]
        assert list(groups[period].values_for(parsed)) == list(ordinary.values_for(parsed))
    monkeypatch.setattr(evidence, 'MAX_STREAM_SUMMARY_BYTES', bound - 1)
    with pytest.raises(ValueError, match='memory budget'):
        evidence.prepare_streamed_peer_groups(sources, scopes=scopes)
    monkeypatch.setattr(evidence, 'MAX_STREAM_SUMMARY_BYTES', 1)
    with pytest.raises(ValueError, match='memory budget'):
        evidence.prepare_streamed_peer_groups(iter(()), scopes=scopes)


def test_shared_identifiers_do_not_omit_invalid_originals_from_other_periods():
    prior = document(quarter=2, serial=2, rows=[('000361106', 5, 20, '')])
    current = document(rows=[('000361105', 3, 10, '')])
    invalid = copy.deepcopy(current)
    invalid['raw'] = invalid['raw'].replace(b'ACCESSION NUMBER:', b'INVALID HEADER:')
    invalid['content_hash'] = hashlib.sha256(invalid['raw']).hexdigest()
    scopes = {'2026-06-30': {'000361106'}, '2026-09-30': {'000361105'}}
    groups = evidence.prepare_streamed_peer_groups([prior, current, invalid], scopes=scopes)
    for group in groups.values():
        assert len(group) == 2
        assert any(record.content_hash == invalid['content_hash'] and record.compact is None for record in group.records)


def test_repeated_identifiers_share_storage_without_changing_any_peer_value():
    target = document(rows=[('000361105', 100, 100, '')])
    sources = [_independent_peer(str(i).zfill(10), i) for i in range(2, 22)]
    parsed, cohort, _ = prepare(sources, target)
    ordinary = evidence.prepare_streamed_peer_groups(sources, scopes=None)[None]
    assert len(cohort) == len(ordinary) == len(sources)
    assert list(cohort) == list(ordinary)
    assert list(cohort.values_for(parsed)) == list(ordinary.values_for(parsed))
    assert cohort.summary_bytes < ordinary.summary_bytes


def _spilled_fixture(monkeypatch, *, invalid=False):
    prior = document(quarter=2, serial=2, rows=[('02079k305', 3, 10, '')])
    current = document(rows=[('02079K305', 7, 20, ''), ('000361105', 0, 0, '')])
    sources = [prior, current]
    if invalid:
        bad = copy.deepcopy(current)
        bad['raw'] = bad['raw'].replace(b'ACCESSION NUMBER:', b'INVALID HEADER:')
        bad['content_hash'] = hashlib.sha256(bad['raw']).hexdigest()
        sources.append(bad)
    scopes = {'2026-06-30': {'02079k305'}, '2026-09-30': {'02079K305', '000361105'}}
    ordinary = evidence.prepare_streamed_peer_groups(sources, scopes=scopes)
    bound = sum(g.summary_bytes for g in ordinary.values()) - 1
    monkeypatch.setattr(evidence, 'MAX_STREAM_SUMMARY_BYTES', bound)
    spilled = evidence.prepare_streamed_peer_groups(sources, scopes=scopes, spill_to_disk=True)
    return sources, ordinary, spilled, bound


def test_spilled_complete_mixed_peers_preserve_exact_values_and_source_identity(monkeypatch):
    from decimal import localcontext
    sources, ordinary, spilled, bound = _spilled_fixture(monkeypatch, invalid=True)
    spool = next(iter(spilled.values())).spool
    try:
        assert spool is not None and spool.sealed
        assert sum(g.summary_bytes for g in spilled.values()) <= bound
        assert 0 < spool.size <= evidence.MAX_STREAM_SPOOL_BYTES
        for period, group in spilled.items():
            assert list(group) == list(ordinary[period])
            assert group.source_bytes == ordinary[period].source_bytes
            assert len(group) == len(ordinary[period]) == 2
        for source in sources[:2]:
            _, parsed, _ = parse_document('sec_13f', source['raw'], source['metadata'])
            period = parsed['metadata']['report_period']
            for precision in (7, 28, 50):
                with localcontext() as context:
                    context.prec = precision
                    assert list(spilled[period].values_for(parsed)) == list(ordinary[period].values_for(parsed))
            changed = list(spilled[period].values_for(parsed))
            changed[0][0]['report_period'] = 'changed'
            assert list(spilled[period].values_for(parsed)) == list(ordinary[period].values_for(parsed))
        with pytest.raises(ValueError, match='sealed'):
            spool.append(('{}', '[]', ()))
    finally:
        for group in spilled.values(): group.close()
    assert spool.file.closed


@pytest.mark.parametrize('damage', ['corrupt', 'truncate'])
def test_spilled_summary_fails_closed_on_storage_damage(monkeypatch, damage):
    sources, ordinary, spilled, _ = _spilled_fixture(monkeypatch)
    group = spilled['2026-06-30']
    _, parsed, _ = parse_document('sec_13f', sources[0]['raw'], sources[0]['metadata'])
    spool = group.spool
    try:
        spool.file.seek(0)
        if damage == 'corrupt': spool.file.write(b'!')
        else: spool.file.truncate(1)
        with pytest.raises(DirectSourceError, match='checksum'):
            list(group.values_for(parsed))
    finally:
        group.close()


def test_spool_limits_and_original_tamper_release_temporary_storage(monkeypatch):
    created = []
    original = evidence._PeerSummarySpool
    def capture():
        spool = original(); created.append(spool); return spool
    monkeypatch.setattr(evidence, '_PeerSummarySpool', capture)
    sources, _, groups, bound = _spilled_fixture(monkeypatch)
    for group in groups.values(): group.close()
    scopes = {'2026-06-30': {'02079k305'}, '2026-09-30': {'02079K305', '000361105'}}
    tampered = copy.deepcopy(sources[-1]); tampered['raw'] += b'changed'
    with pytest.raises(DirectSourceError, match='checksum'):
        evidence.prepare_streamed_peer_groups([*sources, tampered], scopes=scopes, spill_to_disk=True)
    assert created[-1].file.closed
    monkeypatch.setattr(evidence, 'MAX_STREAM_SPOOL_BYTES', 1)
    with pytest.raises(ValueError, match='temporary storage budget'):
        evidence.prepare_streamed_peer_groups(sources, scopes=scopes, spill_to_disk=True)
    assert created[-1].file.closed
    monkeypatch.setattr(evidence, 'MAX_STREAM_SUMMARY_BYTES', 1)
    with pytest.raises(ValueError, match='memory budget'):
        evidence.prepare_streamed_peer_groups(sources, scopes=scopes, spill_to_disk=True)


def test_ordinary_small_batch_does_not_create_temporary_storage(monkeypatch):
    def forbidden(): raise AssertionError('Unnecessary temporary storage')
    monkeypatch.setattr(evidence, '_PeerSummarySpool', forbidden)
    source = document(rows=[('000361105', 3, 10, '')])
    groups = evidence.prepare_streamed_peer_groups([source], scopes=None, spill_to_disk=True)
    assert groups[None].spool is None
    groups[None].close()


def test_spilled_publication_keeps_complete_results_and_event_identities(monkeypatch):
    from sqlalchemy import create_engine
    from sqlalchemy.orm import Session
    from app.db import Base
    from app.models import EmailDelivery
    from app.services.feed_source_control import select_feed_source
    from test_direct_13f_publication import mappings, SINCE
    from datetime import datetime, timezone
    from app.services import direct_13f_publication
    class FixedClock(datetime):
        @classmethod
        def now(cls, tz=None):
            value = cls(2026, 11, 1, 20, tzinfo=timezone.utc)
            return value.astimezone(tz) if tz else value.replace(tzinfo=None)
    monkeypatch.setattr(direct_13f_publication, 'datetime', FixedClock)
    def forbidden(*a, **k): pytest.fail('External transport in isolated fixture')
    monkeypatch.setattr('requests.sessions.Session.request', forbidden)
    monkeypatch.setattr('httpx.Client.send', forbidden)
    sources = pair()
    scopes = {'2026-06-30': {'000361105', '000361106'},
              '2026-09-30': {'000361105', '000361107'}}
    ordinary = evidence.prepare_streamed_peer_groups(sources, scopes=scopes)
    bound = sum(g.summary_bytes for g in ordinary.values()) - 1
    results = []
    created = []
    original = evidence._PeerSummarySpool
    def capture():
        spool = original(); created.append(spool); return spool
    monkeypatch.setattr(evidence, '_PeerSummarySpool', capture)
    for limit in (20_000_000, bound):
        monkeypatch.setattr(evidence, 'MAX_STREAM_SUMMARY_BYTES', limit)
        engine = create_engine('sqlite:///:memory:')
        Base.metadata.create_all(engine)
        try:
            with Session(engine) as session:
                mappings(session)
                select_feed_source(session, feed='sec_13f', provider='sec_edgar', publish_since=SINCE,
                                   expected_generation=0, reason='Isolated fixture')
                session.commit()
                for source in sources: stage(session, source)
                result = batch.publish_13f_batch(session, identifier_documents=[], limit=2)
                identities = [(row.id, row.event_type, row.symbol, row.source, row.source_filing_id, row.source_document_url, row.parser_version, row.payload_json) for row in session.scalars(select(Event).order_by(Event.id))]
                assert session.scalar(select(func.count()).select_from(EmailDelivery)) == 0
                results.append((result, identities))
                assert batch.publish_13f_batch(session, identifier_documents=[], limit=2)['processed'] == 0
        finally:
            engine.dispose()
    assert results[0] == results[1]
    assert results[0][0]['feed_events'] > 0
    assert created and all(s.file.closed for s in created)


@pytest.mark.parametrize('failure', ['after_prepare', 'publication'])
def test_spooled_batch_closes_file_on_downstream_failure(db, monkeypatch, failure):
    sources = pair()
    ids = [stage(db, source) for source in sources]
    scopes = {'2026-06-30': {'000361105', '000361106'},
              '2026-09-30': {'000361105', '000361107'}}
    ordinary = evidence.prepare_streamed_peer_groups(sources, scopes=scopes)
    monkeypatch.setattr(evidence, 'MAX_STREAM_SUMMARY_BYTES', sum(g.summary_bytes for g in ordinary.values()) - 1)
    created = []
    original = evidence._PeerSummarySpool
    def capture():
        spool = original(); created.append(spool); return spool
    monkeypatch.setattr(evidence, '_PeerSummarySpool', capture)
    def failure_after(*a, **k): raise RuntimeError('Injected downstream failure')
    if failure == 'after_prepare':
        monkeypatch.setattr(evidence.Prepared13FEvidence, 'build_for_cohort', failure_after)
        with pytest.raises(RuntimeError, match='Injected'):
            batch.prepare_13f_batch(db, ids=ids, identifier_documents=[])
    else:
        monkeypatch.setattr(batch, 'publish_13f_document', failure_after)
        with pytest.raises(RuntimeError, match='Injected'):
            batch.publish_13f_batch(db, identifier_documents=[], limit=2)
    assert created and all(s.file.closed for s in created)
    assert db.scalar(select(func.count()).select_from(DirectFeedPublication)) == 0
