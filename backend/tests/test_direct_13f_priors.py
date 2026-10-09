import copy
import json

import pytest
from sqlalchemy import func, select

from app.clients.direct_sources import DirectSourceError
from app.models import Event, InstitutionalFiling, InstitutionalPosition
from app.services.direct_13f_priors import collect_13f_priors, prior_metadata
from app.services.direct_feed_collection import parse_document
from app.services.direct_feed_store import DirectFeedDocument, DirectFeedRevision, discover, record_document
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
