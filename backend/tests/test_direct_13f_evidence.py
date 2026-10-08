from datetime import date
import hashlib
import json

import pytest
from sqlalchemy import select, func

from app.clients.direct_sources import DirectSourceError
from app.models import InstitutionalFiling, InstitutionalPosition, Event
from app.services.direct_13f_evidence import nport_identifiers, value_consistency_issues
from app.services.direct_13f_publication import rehearse_new_13f, RECEIPT_KEY
from app.services.direct_feed_collection import parse_document
from test_direct_13f_publication import db, document, CIK, SINCE


def nport(*, symbol='AAA', filed='20260601', asset='EC', derivative=False):
    raw = f'''<SEC-HEADER>
ACCESSION NUMBER: 0001592900-26-002759
FILED AS OF DATE: {filed}
CONFORMED SUBMISSION TYPE: NPORT-P
CENTRAL INDEX KEY: 0001592900
</SEC-HEADER><XML><edgarSubmission><submissionType>NPORT-P</submissionType>
<regCik>1592900</regCik><repPdDate>2026-03-31</repPdDate><invstOrSec><name>Example</name>
<cusip>000361105</cusip><identifiers><ticker value="{symbol}"/></identifiers>
<assetCat>{asset}</assetCat><units>NS</units>{'<derivativeInfo/>' if derivative else ''}
</invstOrSec></edgarSubmission></XML>'''.encode()
    return {'raw': raw, 'content_hash': hashlib.sha256(raw).hexdigest(),
            'url': 'https://www.sec.gov/Archives/edgar/data/1592900/0001592900-26-002759.txt'}


def peer(cik, *, value=100_000, serial=2, filed=None):
    doc = document(serial=serial, rows=[('000361105', 100, value, '')], filed=filed)
    doc['raw'] = doc['raw'].replace(CIK.encode(), cik.encode())
    doc['content_hash'] = hashlib.sha256(doc['raw']).hexdigest()
    doc['metadata']['cik'] = cik
    return doc


def test_nport_exact_identifier_and_point_in_time_evidence():
    rows = nport_identifiers(nport(symbol='BRK/B'), available_by=SINCE)
    assert rows[0]['symbol'] == 'BRK-B' and rows[0]['cusip'] == '000361105'
    assert rows[0]['filing_date'] == '2026-06-01' and rows[0]['source_sha256']
    assert nport_identifiers(nport(filed='20261007'), available_by=SINCE) == []


@pytest.mark.parametrize('kwargs', [{'asset': 'DBT'}, {'derivative': True}, {'symbol': 'INVALID CONTRACT'}])
def test_nport_does_not_map_non_equity_or_invalid_identifiers(kwargs):
    assert nport_identifiers(nport(**kwargs), available_by=SINCE) == []


def test_nport_checksum_and_url_identity_are_enforced():
    doc = nport()
    with pytest.raises(DirectSourceError, match='checksum'):
        nport_identifiers({**doc, 'raw': doc['raw'] + b'changed'}, available_by=SINCE)
    with pytest.raises(DirectSourceError, match='identity'):
        nport_identifiers({**doc, 'url': doc['url'].replace('1592900/', '1/')}, available_by=SINCE)


def test_direct_identifiers_enable_real_pipeline_and_keep_evidence_on_repeat(db):
    sources = [nport()]
    for doc in [document(quarter=2, serial=2), document()]:
        result = rehearse_new_13f(db, doc, publish_since=SINCE, identifier_documents=sources)
        db.commit()
    assert result['derived_state'] == 'published'
    assert all(p.normalized_symbol == 'AAA' for p in db.scalars(select(InstitutionalPosition)))
    before = [f.raw_metadata_json for f in db.scalars(select(InstitutionalFiling))]
    assert all(json.loads(raw)[RECEIPT_KEY]['identifier_evidence'] for raw in before)
    rehearse_new_13f(db, document(), publish_since=SINCE, identifier_documents=sources)
    db.commit()
    assert [f.raw_metadata_json for f in db.scalars(select(InstitutionalFiling))] == before


def test_conflicting_nport_tickers_cannot_enable_equity_events(db):
    for doc in [document(quarter=2, serial=2), document()]:
        result = rehearse_new_13f(db, doc, publish_since=SINCE, identifier_documents=[nport(), nport(symbol='BBB')])
        db.commit()
    assert result['derived_state'] == 'waiting_equity_symbol_mapping'
    assert db.scalar(select(func.count()).select_from(Event)) == 0


def test_unit_discrepancy_requires_two_independent_available_agreeing_peers(db):
    target = document(rows=[('000361105', 100, 100, '')])
    _, parsed, _ = parse_document('sec_13f', target['raw'], target['metadata'])
    first, second = peer('0000000001'), peer('0000000002', serial=3)
    assert not value_consistency_issues(parsed, [first])
    assert not value_consistency_issues(parsed, [first, first])
    assert not value_consistency_issues(parsed, [first, peer('0000000002', filed='2026-10-07')])
    assert not value_consistency_issues(parsed, [first, peer('0000000002', value=10)])
    issues = value_consistency_issues(parsed, [first, second])
    assert len(issues) == 1 and float(issues[0]['peer_to_reported_unit_value_ratio']) == 1000
    result = rehearse_new_13f(db, target, publish_since=SINCE, comparison_documents=[first, second])
    assert result['status'] == 'held' and result['value_issues'] == issues
    assert db.scalar(select(func.count()).select_from(InstitutionalFiling)) == 0


def test_bad_peer_checksum_fails_closed():
    target = document()
    _, parsed, _ = parse_document('sec_13f', target['raw'], target['metadata'])
    source = peer('0000000001')
    source['content_hash'] = 'bad'
    with pytest.raises(DirectSourceError, match='checksum'):
        value_consistency_issues(parsed, [source])
