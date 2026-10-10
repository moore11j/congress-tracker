"""Reviewed registry identity, transport and consumer isolation checks."""
import json

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from app.db import Base
from app.clients.direct_sources import parse_issuer_material, DirectSourceError
from app.services import issuer_transcripts as issuer
from app.services.direct_feed_store import discover, record_document
from test_issuer_pdf import pdf


@pytest.mark.parametrize('symbol,heading', [
    ('META', 'Meta Platforms, Inc. Second Quarter 2026 July 29th, 2026 prepared remarks'),
    ('GOOGL', 'Alphabet Second Quarter 2026 July 22, 2026 transcript'),
])
@pytest.mark.parametrize('case', ['valid', 'company', 'date', 'period'])
def test_reviewed_company_reader_rejects_wrong_identity(symbol, heading, case):
    metadata = issuer._reviewed_latest(symbol)
    if case == 'company':
        heading = heading.replace('Meta Platforms', 'Other Company').replace('Alphabet', 'Other Company')
    if case == 'date':
        heading = heading.replace('July', 'June')
    if case == 'period':
        heading = heading.replace('Second Quarter 2026', 'First Quarter 2025')
    text = heading + ' earnings question-and-answer ' + ('Synthetic evidence for identity checks. ' * 45)
    raw = pdf(text) if symbol == 'META' else ('<main>'+text+'</main>').encode()
    engine = create_engine('sqlite:///:memory:')
    Base.metadata.create_all(engine)
    try:
        with Session(engine) as db:
            if case == 'period' or (symbol == 'META' and case != 'valid'):
                with pytest.raises(DirectSourceError):
                    parse_issuer_material(raw, metadata)
                return
            parsed_text, parsed = parse_issuer_material(raw, metadata)
            row = discover(db, 'issuer_earnings', metadata)
            record_document(db, row, raw, parsed_text, parsed)
            db.commit()
            result = issuer.prepared_transcript(db, symbol)
            if case == 'valid':
                assert result['status'] == 'prepared' and result['has_qa']
                assert result['symbol'] == symbol
                assert result['published_date'] == metadata['published_at']
                assert result['canonical_key'] == f'{symbol}:earnings_transcript:2026:Q2'
                assert result['transcript_publisher'] == metadata['transcript_publisher']
            else:
                assert result['reason'] == 'reviewed_source_identity_incomplete'
    finally:
        engine.dispose()


def test_registry_has_one_source_per_issuer_period_and_no_implicit_share_class():
    rows = json.loads(issuer.REGISTRY.read_bytes())
    keys = [(row['symbol'], d['document_type'], d['fiscal_year'], d['fiscal_quarter'])
            for row in rows for d in row['documents']]
    assert len(keys) == len(set(keys))
    assert issuer._reviewed_latest('GOOG') is None
    alphabet = issuer._reviewed_latest('GOOGL')
    assert alphabet['url'].startswith('https://abc.xyz/investor/events/')
    meta = issuer._reviewed_latest('META')
    assert meta['url'].startswith('https://investor.atmeta.com/')
    assert meta['redirect_urls'] == ['https://s21.q4cdn.com/399680738/files/doc_financials/2026/q2/META-Q2-2026-Earnings-Call-Transcript.pdf']


@pytest.mark.parametrize('case', ['valid', 'missing_page', 'foreign_page', 'http_page', 'missing_hash', 'invalid_hash', 'non_pdf'])
def test_external_document_requires_reviewed_official_link_evidence(case):
    from app.clients.direct_sources import reviewed_issuer_document_urls
    rows = json.loads(issuer.REGISTRY.read_bytes())
    company = next(row for row in rows if row['symbol'] == 'SOFI')
    item = company['documents'][0]
    if case == 'missing_page': item.pop('source_page_url')
    if case == 'foreign_page': item['source_page_url'] = 'https://unrelated.example/page'
    if case == 'http_page': item['source_page_url'] = 'http://investors.sofi.com/page'
    if case == 'missing_hash': item.pop('source_page_sha256')
    if case == 'invalid_hash': item['source_page_sha256'] = 'unverified'
    if case == 'non_pdf': item['source_format'] = 'html'
    if case == 'valid':
        assert reviewed_issuer_document_urls([company]) == {item['url']}
    else:
        with pytest.raises(DirectSourceError, match='reviewed official page'):
            reviewed_issuer_document_urls([company])


@pytest.mark.parametrize('case', ['exact', 'sibling', 'other_tenant', 'http', 'credentials', 'redirect'])
def test_external_permission_grants_only_one_reviewed_document(monkeypatch, case):
    from app.clients.direct_sources import DirectSourceClient
    allowed = 'https://cdn.example.test/company/transcript.pdf'
    url = {'sibling':'https://cdn.example.test/company/other.pdf',
           'other_tenant':'https://cdn.example.test/other/transcript.pdf',
           'http':'http://cdn.example.test/company/transcript.pdf',
           'credentials':'https://user@cdn.example.test/company/transcript.pdf'}.get(case, allowed)
    calls = []
    class Response:
        status_code = 302 if case == 'redirect' else 200
        is_redirect = case == 'redirect'
        headers = {'Location':'https://cdn.example.test/other/transcript.pdf'}
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def iter_content(self, *args): yield b'exact reviewed content'
    class Session:
        headers = {}
        def get(self, url, **kwargs):
            calls.append(url)
            return Response()
    monkeypatch.setattr('app.clients.direct_sources.time.sleep', lambda _:None)
    client = DirectSourceClient(issuer_document_urls=[allowed], session=Session())
    if case == 'exact':
        assert client.get(url) == b'exact reviewed content'
    else:
        with pytest.raises(DirectSourceError, match='approved HTTPS'):
            client.get(url)
        assert len(calls) == (1 if case == 'redirect' else 0)
