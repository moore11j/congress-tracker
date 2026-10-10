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
