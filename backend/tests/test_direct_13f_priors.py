import copy
import json
from datetime import date, timedelta

import pytest
from sqlalchemy import func, select

from app.clients.direct_sources import DirectSourceError
from app.models import Event, InstitutionalFiling, InstitutionalPosition
from app.services.direct_13f_priors import collect_13f_priors, prior_metadata, pending_prior_documents
from app.services.direct_feed_collection import parse_document
from app.services.direct_feed_store import DirectFeedDocument, DirectFeedRevision, discover, record_document, utcnow
from test_direct_sec_collection import db, META, submission


def inputs(db):
    raw = submission()
    text, parsed, _ = parse_document('sec_13f', raw, META)
    doc = discover(db, 'sec_13f', META); record_document(db, doc, raw, text, parsed); db.commit()
    doc_id = doc.id; db.rollback()
    accession = '0001096906-26-000999'
    history = {'cik': 1092903, 'name': 'Fixture manager', 'filings': {'recent': {
        'form': ['13F-HR', '13F-HR'], 'reportDate': ['2026-09-30', '2026-06-30'],
        'filingDate': ['2026-10-06', '2026-07-10'], 'accessionNumber': [META['key'], accession]}}}
    prior = raw.replace(META['key'].encode(), accession.encode()).replace(b'20261006', b'20260710').replace(b'09-30-2026', b'06-30-2026')
    return doc_id, parsed, history, prior


def test_prior_collection_and_repeat_preserve_canonical_and_staged_identity(db):
    doc_id, current, history, prior = inputs(db)
    calls = []
    class Client:
        def get(self, url):
            assert not db.in_transaction()
            calls.append(url)
            return json.dumps(history).encode() if url.endswith('.json') else prior
    result = collect_13f_priors(db, Client(), document_ids=[doc_id])
    assert result['status'] == 'collected'
    prior_id = result['results'][0]['prior_document_id']
    staged = db.get(DirectFeedDocument, prior_id)
    staged.reconciliation_json = '{"retained":true}'; db.commit()
    count = db.scalar(select(func.count()).select_from(DirectFeedRevision)); db.rollback()
    repeat = collect_13f_priors(db, Client(), document_ids=[doc_id])
    assert repeat['results'] == result['results'] and len(calls) == 3
    assert db.scalar(select(func.count()).select_from(DirectFeedRevision)) == count == 3
    assert db.get(DirectFeedDocument, prior_id).reconciliation_json == '{"retained":true}'
    for model in [Event, InstitutionalFiling, InstitutionalPosition]:
        assert db.scalar(select(func.count()).select_from(model)) == 0


@pytest.mark.parametrize('change', ['cik', 'amendment', 'duplicate', 'columns', 'current', 'accession', 'missing'])
def test_prior_discovery_fails_closed(db, change):
    _, current, history, _ = inputs(db)
    rows = history['filings']['recent']
    if change == 'cik': history['cik'] = 42
    elif change == 'amendment': rows['form'][1] = '13F-HR/A'
    elif change == 'duplicate':
        for values in rows.values(): values.append(values[1])
    elif change == 'columns': rows['form'].pop()
    elif change == 'current': rows['reportDate'][0] = '2026-06-30'
    elif change == 'accession': rows['accessionNumber'][1] = '../../unsafe'
    else: rows['reportDate'][1] = '2026-03-31'
    with pytest.raises(DirectSourceError): prior_metadata(history, current)


def test_future_amendment_is_not_used_as_point_in_time_prior(db):
    _, current, history, _ = inputs(db)
    future = {'form': '13F-HR/A', 'reportDate': '2026-06-30', 'filingDate': '2026-10-07',
              'accessionNumber': '0001096906-26-001599'}
    for key, value in future.items(): history['filings']['recent'][key].append(value)
    assert prior_metadata(history, current)[0]['key'] == '0001096906-26-000999'


def test_transport_denial_stops_bounded_batch_without_publication(db):
    doc_id, _, _, _ = inputs(db)
    class Client:
        def get(self, url): raise DirectSourceError('Source HTTP 403: denied')
    result = collect_13f_priors(db, Client(), document_ids=[doc_id, 9999])
    assert result['status'] == 'partial' and result['processed'] == 1
    assert db.scalar(select(func.count()).select_from(DirectFeedDocument)) == 1


def test_prior_period_mismatch_does_not_stage_source(db):
    doc_id, _, history, prior = inputs(db)
    class Client:
        def get(self, url):
            return json.dumps(history).encode() if url.endswith('.json') else prior.replace(b'06-30-2026', b'03-31-2026')
    result = collect_13f_priors(db, Client(), document_ids=[doc_id])
    assert result['status'] == 'partial' and 'period differs' in result['results'][0]['reason']
    assert db.scalar(select(func.count()).select_from(DirectFeedDocument)) == 1


def additional_source(db, *, filed='2026-10-06', status='parsed'):
    meta = {**META, 'key': '0001096906-26-009999', 'filing_date': filed}
    meta['url'] = META['url'].replace(META['key'], meta['key'])
    row = DirectFeedDocument(feed='sec_13f', source_key=meta['key'], source_url=meta['url'],
                            metadata_json=json.dumps(meta), content_hash='other', status=status)
    db.add(row); db.flush(); ident = row.id; db.commit()
    return ident


def test_held_source_cools_down_without_starving_new_work_or_falsifying_freshness(db):
    doc_id, _, _, _ = inputs(db)
    original = db.get(DirectFeedDocument, doc_id).checked_at
    db.get(DirectFeedDocument, doc_id).reconciliation_json = '{"matched":[12]}'
    db.commit()
    class Client:
        def get(self, url): raise DirectSourceError('Source HTTP 403: denied')
    collect_13f_priors(db, Client(), document_ids=[doc_id])
    doc = db.get(DirectFeedDocument, doc_id)
    assert doc.checked_at == original and json.loads(doc.reconciliation_json)['matched'] == [12]
    second = additional_source(db)
    now = utcnow()
    assert pending_prior_documents(db, since=date(2026,10,1), now=now)['document_ids'] == [second]
    assert pending_prior_documents(db, since=date(2026,10,1), now=now+timedelta(hours=25))['document_ids'] == [second, doc_id]
    doc = db.get(DirectFeedDocument, doc_id); doc.content_hash = 'changed'; db.commit()
    assert doc_id in pending_prior_documents(db, since=date(2026,10,1), now=now)['document_ids']


def test_successful_source_reuses_verified_prior_and_rechecks_invalidated_evidence(db):
    doc_id, _, history, prior = inputs(db)
    class Client:
        def get(self, url): return json.dumps(history).encode() if url.endswith('.json') else prior
    result = collect_13f_priors(db, Client(), document_ids=[doc_id])
    now = utcnow()
    assert pending_prior_documents(db, since=date(2026,10,1), now=now)['document_ids'] == []
    second = additional_source(db)
    assert pending_prior_documents(db, since=date(2026,10,1), now=now+timedelta(days=8))['document_ids'] == [second, doc_id]
    prior_doc = db.get(DirectFeedDocument, result['results'][0]['prior_document_id'])
    prior_doc.status = 'quarantined'; db.commit()
    assert doc_id in pending_prior_documents(db, since=date(2026,10,1), now=now)['document_ids']


@pytest.mark.parametrize('change', ['name', 'hash'])
def test_changed_current_identity_is_retried_immediately(db, change):
    doc_id, _, _, _ = inputs(db)
    class Client:
        def get(self, url): raise DirectSourceError('Missing prior')
    collect_13f_priors(db, Client(), document_ids=[doc_id])
    row = db.get(DirectFeedDocument, doc_id)
    if change == 'name':
        meta = json.loads(row.metadata_json); meta['name'] = 'Updated manager'; row.metadata_json = json.dumps(meta)
    else: row.content_hash = 'updated'
    db.commit()
    assert pending_prior_documents(db, since=date(2026,10,1))['document_ids'] == [doc_id]


@pytest.mark.parametrize('filed,status', [('2026-09-01','parsed'),('2026-10-06','pending'),('2027-10-06','parsed')])
def test_automatic_prior_selection_obeys_boundary_and_staging_status(db, filed, status):
    additional_source(db, filed=filed, status=status)
    assert pending_prior_documents(db, since=date(2026,10,1))['document_ids'] == []


def test_automatic_preview_opens_no_transport_and_writes_no_receipt(db, monkeypatch, capsys):
    from contextlib import nullcontext
    from types import SimpleNamespace
    from app.jobs import collect_direct_13f_priors as job
    from app.services.direct_feed_store import DirectFeedRun
    doc_id, _, _, _ = inputs(db)
    monkeypatch.setenv('DIRECT_FEEDS_MODE', 'shadow')
    monkeypatch.setattr('sys.argv', ['collect_direct_13f_priors', '--since', '2026-10-01', '--preview'])
    monkeypatch.setattr(job, 'check_background_job_guard', lambda _: SimpleNamespace(proceed=True))
    lock_options = []
    monkeypatch.setattr(job, 'collector_lock', lambda **kw: (lock_options.append(kw), nullcontext(True))[1])
    monkeypatch.setattr(job, 'SessionLocal', lambda: db)
    monkeypatch.setattr(job, 'DirectSourceClient', lambda: pytest.fail('Preview requested transport'))
    job.main()
    assert lock_options == [{'recover_orphaned': False}]
    report = json.loads(capsys.readouterr().out)
    assert report['status'] == 'preview' and report['selection']['document_ids'] == [doc_id]
    assert db.scalar(select(func.count()).select_from(DirectFeedRun)) == 0
