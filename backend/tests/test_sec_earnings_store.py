from datetime import datetime, timezone
import hashlib
import json

import pytest
from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import Session

from app.db import Base
from app.models import Event, ResearchSourceDocument, Security
from app.services.direct_feed_store import DirectFeedDocument, DirectFeedRevision
from app.services.sec_earnings_materials import prepare_earnings_release, verify_filing_availability
from app.services.sec_earnings_store import reconcile_research_document, reconcile_staged_earnings, stage_earnings_material
from test_sec_earnings_materials import company, filing, submission


def index(accepted='2026-07-30 16:00:00'):
    return f'''<html><body>CIK: 0001234567
<a href="0001234567-26-000001.txt">Complete submission</a>
<div class="infoHead">Filing Date</div><div>2026-07-30</div>
<div class="infoHead">Accepted</div><div>{accepted}</div>
</body></html>'''.encode()


@pytest.fixture
def db():
    engine = create_engine('sqlite:///:memory:')
    Base.metadata.create_all(engine)
    with Session(engine) as db:
        db.add(Security(id=1, symbol='TEST', name='Test', asset_class='stock'))
        db.commit()
        yield db
    engine.dispose()


def staged(db):
    return stage_earnings_material(db, company_raw=company(), submission_raw=submission(),
        index_raw=index(), symbol='TEST', cik=1234567, accession=filing()['accession_number'])


def receipt():
    return verify_filing_availability(prepare_earnings_release(submission(), filing=filing()), index())


def existing(db, **overrides):
    row = ResearchSourceDocument(id='existing', security_id=1, document_type='press_release',
        source_provider='fmp', external_id='legacy-stable', title='Retained headline',
        content_hash=receipt()['release']['text_sha256'], source_url='https://issuer.test/news/results',
        processing_status='processed', processing_version='retained-version',
        published_at=datetime(2026, 7, 30, 19, 55, tzinfo=timezone.utc))
    for key, value in overrides.items():
        setattr(row, key, value)
    db.add(row); db.commit()
    return row


def test_acceptance_sources_agree_without_claiming_public_availability():
    row = receipt()
    assert row['sec_accepted_at'] == '2026-07-30T20:00:00+00:00'
    assert row['availability_status'] == 'verified_sec_acceptance'
    assert row['publication_eligible'] is False
    assert 'edgar_available_at' not in row


def test_source_clock_conflict_is_retained():
    row = prepare_earnings_release(submission(), filing=filing())
    row['submissions_accepted_at'] = '2026-07-31T00:00:00Z'
    result = verify_filing_availability(row, index())
    assert result['availability_status'] == 'verified_sec_acceptance'
    assert result['sec_accepted_at'] == '2026-07-30T20:00:00+00:00'
    assert result['submissions_accepted_at'] == '2026-07-31T00:00:00Z'
    assert result['acceptance_evidence']['submissions_comparison'] == 'conflict'
    assert result['acceptance_evidence']['submissions_delta_seconds'] == 14400
    assert result['acceptance_evidence']['observation_not_before'] == '2026-07-31T00:00:00+00:00'
    assert result['publication_eligible'] is False


@pytest.mark.parametrize('supplied,comparison', [(None, 'missing'), ('2026-07-30T16:00:00', 'timezone_missing'),
    ('2026-07-30T16:00:00Z', 'conflict'), ('2026-07-30T20:00:00Z', 'agrees')])
def test_header_index_clock_does_not_infer_api_timezone(supplied, comparison):
    row = prepare_earnings_release(submission(), filing=filing())
    row['submissions_accepted_at'] = supplied
    result = verify_filing_availability(row, index())
    assert result['sec_accepted_at'] == '2026-07-30T20:00:00+00:00'
    assert result['acceptance_evidence']['submissions_raw'] == supplied
    assert result['acceptance_evidence']['submissions_comparison'] == comparison
    assert result['acceptance_evidence']['observation_not_before'] == '2026-07-30T20:00:00+00:00'


@pytest.mark.parametrize('day,wall,utc', [('2026-01-30', '16:00:00', '2026-01-30T21:00:00+00:00'),
    ('2026-07-30', '16:00:00', '2026-07-30T20:00:00+00:00')])
def test_header_index_conversion_observes_eastern_season(day, wall, utc):
    row = prepare_earnings_release(submission(), filing=filing())
    row.update(filing_date=day, header_accepted_local_raw=day.replace('-', '')+wall.replace(':', ''),
               submissions_accepted_at=None)
    page = index(f'{day} {wall}').replace(b'2026-07-30</div>', (day+'</div>').encode())
    assert verify_filing_availability(row, page)['sec_accepted_at'] == utc


@pytest.mark.parametrize('day,wall', [('2026-11-01', '01:30:00'), ('2026-03-08', '02:30:00')])
def test_ambiguous_or_nonexistent_eastern_acceptance_is_not_guessed(day, wall):
    row = prepare_earnings_release(submission(), filing=filing())
    row.update(filing_date=day, header_accepted_local_raw=day.replace('-', '')+wall.replace(':', ''),
               submissions_accepted_at=None)
    page = index(f'{day} {wall}').replace(b'2026-07-30</div>', (day+'</div>').encode())
    with pytest.raises(ValueError, match='SEC acceptance local time'):
        verify_filing_availability(row, page)


def test_official_complete_submission_parent_alias():
    raw = index().replace(b'href="0001234567-26-000001.txt"',
                         b'href="/Archives/edgar/data/1234567/0001234567-26-000001.txt"')
    result = verify_filing_availability(prepare_earnings_release(submission(), filing=filing()), raw)
    assert result['availability_status'] == 'verified_sec_acceptance'


def test_sec_index_cik_hyperlink_separates_colon():
    raw = index().replace(b'CIK: 0001234567', b'CIK <b>:</b> <a href="issuer">0001234567</a>')
    assert verify_filing_availability(prepare_earnings_release(submission(), filing=filing()), raw)['availability_status'] == 'verified_sec_acceptance'


@pytest.mark.parametrize('before,after', [(b'0001234567.txt', b'0001234568.txt'),
    (b'CIK: 0001234567', b'CIK: 0001234568'),
    (b'2026-07-30</div>', b'2026-07-29</div>'),
    (b'16:00:00', b'17:00:00')])
def test_index_identity_and_time_rejections(before, after):
    raw = index()
    if before == b'0001234567.txt':
        before, after = b'0001234567-26-000001.txt', b'0001234567-26-000002.txt'
    with pytest.raises(ValueError):
        verify_filing_availability(prepare_earnings_release(submission(), filing=filing()), raw.replace(before, after))


def test_stage_reconcile_repeat_without_public_writes(db):
    doc = staged(db); db.commit()
    first = reconcile_staged_earnings(db, doc.id, security_id=1); db.commit()
    assert first['status'] == 'new_candidate'
    doc2 = staged(db); db.commit()
    assert doc.id == doc2.id
    assert reconcile_staged_earnings(db, doc2.id, security_id=1) == first
    assert db.scalar(select(func.count()).select_from(DirectFeedDocument)) == 3
    assert db.scalar(select(func.count()).select_from(DirectFeedRevision)) == 3
    assert db.scalar(select(func.count()).select_from(ResearchSourceDocument)) == 0
    assert db.scalar(select(func.count()).select_from(Event)) == 0


def test_exact_existing_document_is_adoptable_without_mutation(db):
    row = existing(db)
    before = {c.name: getattr(row, c.name) for c in row.__table__.columns}
    doc = staged(db); db.commit()
    result = reconcile_staged_earnings(db, doc.id, security_id=1)
    assert result['status'] == 'matched' and result['existing_document_id'] == row.id
    assert {c.name: getattr(row, c.name) for c in row.__table__.columns} == before
    assert db.scalar(select(func.count()).select_from(ResearchSourceDocument)) == 1


@pytest.mark.parametrize('changes', [
    {'content_hash': 'different-text'},
    {'content_hash': 'different-text', 'source_url': receipt()['release']['url']},
    {'content_hash': 'different-text', 'published_at': None},
])
def test_possible_provider_projection_is_held(db, changes):
    existing(db, **changes)
    assert reconcile_research_document(db, receipt(), security_id=1)['reason'] == 'ambiguous_existing_material'


def test_existing_duplicate_population_is_held(db):
    existing(db)
    existing(db, id='second', external_id='second')
    assert reconcile_research_document(db, receipt(), security_id=1)['reason'] == 'ambiguous_existing_material'


@pytest.mark.parametrize('target', ['bytes', 'parsed', 'metadata'])
def test_reconciliation_reparses_source_and_rejects_tampering(db, target):
    doc = staged(db); db.commit()
    if target == 'bytes':
        row = db.scalar(select(DirectFeedRevision).where(DirectFeedRevision.document_id == doc.id))
        row.source_bytes = b'tampered'
    elif target == 'parsed':
        doc.parsed_json = '{}'
    else:
        metadata = json.loads(doc.metadata_json); metadata['filing']['symbol'] = 'OTHER'
        doc.metadata_json = json.dumps(metadata)
    db.commit()
    with pytest.raises(ValueError):
        reconcile_staged_earnings(db, doc.id, security_id=1)


def test_wrong_security_rejected(db):
    db.add(Security(id=2, symbol='OTHER', name='Other', asset_class='stock')); db.commit()
    doc = staged(db); db.commit()
    with pytest.raises(ValueError, match='security mismatch'):
        reconcile_staged_earnings(db, doc.id, security_id=2)


def test_caller_rollback_removes_complete_bundle(db):
    staged(db)
    db.rollback()
    assert db.scalar(select(func.count()).select_from(DirectFeedDocument)) == 0
    assert db.scalar(select(func.count()).select_from(DirectFeedRevision)) == 0


def test_cross_filing_staged_content_cannot_become_new_candidate(db):
    first = staged(db); db.commit()
    second = stage_earnings_material(db,
        company_raw=company().replace(b'26-000001', b'26-000002'),
        submission_raw=submission().replace(b'26-000001', b'26-000002'),
        index_raw=index().replace(b'26-000001', b'26-000002'),
        symbol='TEST', cik=1234567, accession='0001234567-26-000002')
    db.commit()
    for doc in (first, second):
        assert reconcile_staged_earnings(db, doc.id, security_id=1)['reason'] == 'duplicate_staged_content'


def test_staged_api_clock_conflict_retains_header_index_evidence(db):
    doc = stage_earnings_material(db,
        company_raw=company(acceptanceDateTime=['2026-07-31T00:00:00Z']),
        submission_raw=submission(), index_raw=index(), symbol='TEST', cik=1234567,
        accession=filing()['accession_number'])
    db.commit()
    result = reconcile_staged_earnings(db, doc.id, security_id=1)
    assert result['status'] == 'new_candidate'
    assert result['publication_eligible'] is False
    parsed = json.loads(doc.parsed_json)
    assert parsed['acceptance_evidence']['submissions_comparison'] == 'conflict'
    assert parsed['acceptance_evidence']['submissions_raw'] == '2026-07-31T00:00:00Z'
    assert db.scalar(select(func.count()).select_from(Event)) == 0
