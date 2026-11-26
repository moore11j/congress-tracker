from copy import deepcopy
from pathlib import Path
import gzip
import hashlib
import html
import json
import re

import pytest

from app.services.institutional_reference import capture_reference, reference_identity, resolve_reference_candidate

FIXTURES = Path(__file__).parent / 'fixtures' / 'reviewed_ordinary_identities'


def retained(symbol):
    return json.loads((FIXTURES / f'{symbol}-reference.json').read_bytes())


def identity(doc):
    return reference_identity(doc, cusip=doc['query']['cusip'], report_period=doc['query']['date'],
                              observed_by=doc['observed_at'])


@pytest.mark.parametrize('symbol,source,old_cusip,effective,cik', [
    ('TEVA', 'teva-conversion-2026', '881624209', '20260914', '0000818686'),
    ('MRX', 'marex-redomiciliation-2026', 'G5S37H101', '20260701', '0001997464'),
])
def test_complete_issuer_original_and_retained_response_qualify_exact_new_ordinary_class(symbol, source, old_cusip, effective, cik):
    doc = retained(symbol)
    original = deepcopy(doc)
    result = identity(doc)
    assert result['status'] == 'verified' and result['symbol'] == symbol
    assert doc == original
    assert result['share_class_figi'] is None
    assert result['source_sha256'] == doc['response_sha256']
    assert result['observed_at'] == doc['observed_at']
    assert result['source_availability_basis'] == 'reference_observed_at'
    proof, = result['reviewed_identity_evidence']
    raw = gzip.decompress((FIXTURES / f'{source}.source.gz').read_bytes())
    assert hashlib.sha256(raw).hexdigest() == proof['source_sha256']
    assert result['reviewed_sources'] == [proof['source_url']]
    text = raw.decode('utf-8')
    assert f'FILED AS OF DATE:\t\t{effective}' in text
    assert f'CENTRAL INDEX KEY:\t\t\t{cik}' in text
    assert proof['cusip'] == doc['query']['cusip']
    assert proof['effective_date'].replace('-', '') == effective
    plain = ' '.join(html.unescape(re.sub('<[^>]+>', ' ', text)).split())
    assert proof['cusip'] in plain
    if symbol == 'TEVA':
        assert old_cusip in plain
        assert 'continued listing on NYSE of the Ordinary Shares' in plain
        assert 'Effective September 14, 2026' in plain
        assert 'Each ADS was exchanged for one ordinary share.' in plain
    else:
        assert 'Ordinary Shares began trading on Nasdaq as of market open on July 1, 2026' in plain
        assert 'new CUSIP number G5T40M104' in plain
        assert 'symbol “MRX,”' in plain
    assert resolve_reference_candidate(result, set()) == symbol
    assert resolve_reference_candidate(result, {'UNRELATED'}) is None
    assert reference_identity(doc, cusip=doc['query']['cusip'], report_period=doc['query']['date'],
                              observed_by='2026-09-30T23:59:59Z')['reason'] == 'not_yet_observed'


@pytest.mark.parametrize('symbol', ['TEVA', 'MRX'])
@pytest.mark.parametrize('mutation', ['period', 'old_class', 'cusip', 'cik', 'exchange', 'ticker', 'type',
    'figi', 'composite', 'market', 'locale', 'currency', 'inactive'])
def test_no_missing_figi_waiver_for_other_period_class_issuer_exchange_or_instrument(symbol, mutation):
    saved = retained(symbol)
    payload = deepcopy(saved['response'])
    row = payload['results'][0]
    cusip, period = saved['query']['cusip'], saved['query']['date']
    changes = {'cik': ('cik', '0000000001'), 'exchange': ('primary_exchange', 'OTHER'),
        'ticker': ('ticker', 'OTHER'), 'type': ('type', 'ADRC'), 'figi': ('share_class_figi', 'BAD'),
        'composite': ('composite_figi', 'BBG000000001'), 'market': ('market', 'crypto'),
        'locale': ('locale', 'global'), 'currency': ('currency_name', 'eur'), 'inactive': ('active', False)}
    if mutation in changes:
        key, value = changes[mutation]
        row[key] = value
    if mutation == 'period': period = '2026-06-30'
    if mutation == 'cusip': cusip = '000000000'
    if mutation == 'old_class': cusip = '881624209' if symbol == 'TEVA' else 'G5S37H101'
    class Client:
        def get(self, path, params): return payload
    doc = capture_reference(Client(), cusip=cusip, report_period=period, observed_at=saved['observed_at'])
    assert identity(doc) == {'status': 'held', 'reason': 'unsupported_reference_security'}


@pytest.mark.parametrize('symbol', ['TEVA', 'MRX'])
def test_actual_reference_staging_reinterpretation_preserves_original_and_observation(symbol):
    from sqlalchemy import create_engine, select, func
    from sqlalchemy.orm import Session
    from app.db import Base
    from app.services.direct_feed_store import DirectFeedDocument, DirectFeedRevision
    from app.services.institutional_reference import stage_reference, load_reference
    engine = create_engine('sqlite:///:memory:')
    Base.metadata.create_all(engine, tables=[DirectFeedDocument.__table__, DirectFeedRevision.__table__])
    try:
        with Session(engine) as db:
            doc = retained(symbol)
            stage_reference(db, doc); db.commit()
            row = db.scalar(select(DirectFeedDocument)); revision = db.scalar(select(DirectFeedRevision))
            before = (row.content_hash, row.checked_at, revision.source_bytes, revision.parsed_json)
            row.parsed_json = json.dumps({'identity': {'status': 'held', 'reason': 'unsupported_reference_security'}})
            db.commit()
            assert stage_reference(db, doc)['status'] == 'reused'; db.commit()
            assert json.loads(row.parsed_json)['identity'] == identity(doc)
            assert (row.content_hash, row.checked_at, revision.source_bytes, revision.parsed_json) == before
            assert load_reference(db, cusip=doc['query']['cusip'], report_period=doc['query']['date']) == doc
            assert db.scalar(select(func.count()).select_from(DirectFeedRevision)) == 1
    finally:
        engine.dispose()


@pytest.mark.parametrize('period', ['2026-06-30', '2026-09-30'])
def test_dpc_complete_class_and_listing_originals_support_both_retained_quarters(period):
    doc = retained('DPC-' + period)
    original = deepcopy(doc)
    result = identity(doc)
    assert result['status'] == 'verified' and result['symbol'] == 'DPC'
    assert doc == original and result['source_sha256'] == doc['response_sha256']
    assert result['observed_at'] == doc['observed_at']
    proof, = result['reviewed_identity_evidence']
    assert proof['effective_date'] == '2026-06-26' and proof['cik'] == '2107018'
    assert proof['cusip'] == 'G2R11M108' and proof['exchange'] == 'XNYS'
    raw_class = gzip.decompress((FIXTURES / 'dpc-june-class.source.gz').read_bytes())
    assert hashlib.sha256(raw_class).hexdigest() == proof['source_sha256']
    class_text = raw_class.decode('utf-8')
    assert '<issuerCik>0002107018</issuerCik>' in class_text
    assert '<issuerCusipNumber>G2R11M108</issuerCusipNumber>' in class_text
    assert '<securitiesClassTitle>Ordinary Shares</securitiesClassTitle>' in class_text
    assert '<eventDateRequiresFilingThisStatement>06/30/2026</eventDateRequiresFilingThisStatement>' in class_text
    listing, = proof['supporting_sources']
    raw_listing = gzip.decompress((FIXTURES / 'dpc-june-quarter.source.gz').read_bytes())
    assert hashlib.sha256(raw_listing).hexdigest() == listing['source_sha256']
    plain = ' '.join(html.unescape(re.sub('<[^>]+>', ' ', raw_listing.decode('utf-8'))).split())
    assert 'Ordinary shares, no par value' in plain and 'New York Stock Exchange' in plain
    assert 'On June 26, 2026, we completed our Initial Public Offering' in plain
    assert re.search(r'Ordinary shares, no par value\s+DPC\s+New York Stock Exchange', plain.replace('\u200b', ' '))
    assert result['reviewed_sources'] == [proof['source_url'], listing['source_url']]
    assert resolve_reference_candidate(result, set()) == 'DPC'
    assert resolve_reference_candidate(result, {'UNRELATED'}) is None
    assert reference_identity(doc, cusip='G2R11M108', report_period=period,
                              observed_by=period + 'T23:59:59Z')['reason'] == 'not_yet_observed'


@pytest.mark.parametrize('mutation', ['period', 'cusip', 'cik', 'exchange', 'ticker', 'type',
    'figi', 'composite', 'market', 'locale', 'currency', 'inactive'])
def test_dpc_review_never_waives_other_identity_or_time_guards(mutation):
    saved = retained('DPC-2026-06-30')
    payload = deepcopy(saved['response'])
    row = payload['results'][0]
    cusip, period = 'G2R11M108', '2026-06-30'
    changes = {'cik': ('cik', '0000000001'), 'exchange': ('primary_exchange', 'XNAS'),
        'ticker': ('ticker', 'OTHER'), 'type': ('type', 'FUND'), 'figi': ('share_class_figi', 'BAD'),
        'composite': ('composite_figi', 'BBG000000001'), 'market': ('market', 'crypto'),
        'locale': ('locale', 'global'), 'currency': ('currency_name', 'eur'), 'inactive': ('active', False)}
    if mutation in changes:
        field, value = changes[mutation]; row[field] = value
    if mutation == 'period': period = '2026-03-31'
    if mutation == 'cusip': cusip = '000000000'
    class Client:
        def get(self, path, params): return payload
    doc = capture_reference(Client(), cusip=cusip, report_period=period, observed_at=saved['observed_at'])
    assert identity(doc) == {'status': 'held', 'reason': 'unsupported_reference_security'}


@pytest.mark.parametrize('symbol', ['DPC-2026-06-30', 'DPC-2026-09-30'])
def test_dpc_staging_preserves_whole_original_and_repeats_without_new_revision(symbol):
    test_actual_reference_staging_reinterpretation_preserves_original_and_observation(symbol)
