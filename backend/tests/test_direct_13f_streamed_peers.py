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
