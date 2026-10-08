import copy
import hashlib
import json
from pathlib import Path

import pytest

from app.clients import house_ptr_review
from app.clients.direct_sources import DirectSourceError, parse_house_pdf
from app.services.direct_feed_collection import parse_document
from app.services.official_congress import congress_transaction_hash


@pytest.fixture
def source():
    registry = json.loads(house_ptr_review.REGISTRY.read_bytes())
    raw = (Path(__file__).with_name('fixtures') / 'house_9116361.pdf').read_bytes()
    assert hashlib.sha256(raw).hexdigest() == '438b1c89ec6f78c9ec39f30799b499a8e2fb734cd53dd5b2787f71a6d3b01385'
    return raw, registry['reviews'][0]['metadata'], registry


def test_real_review_preserves_economics_and_unprinted_tickers(source):
    raw, metadata, _ = source
    text, parsed, holds = parse_document('house_ptr', raw, metadata)
    assert text.startswith('Reviewed transcription')
    assert holds == []
    rows = parsed['transactions']
    assert [r['ticker_normalized'] for r in rows] == ['NOW', 'TTD', 'UPST', 'CMG']
    assert [r['amount_low'] for r in rows] == [50001, 15001, 15001, 15001]
    assert [r['amount_high'] for r in rows] == [100000, 50000, 50000, 50000]
    assert len({r['normalized_hash'] for r in rows}) == 4
    for row in rows:
        assert row['ticker_raw'] is None
        assert row['owner_normalized'] == 'joint'
        assert row['transaction_type_raw'] == 'Partial Sale'
        assert row['transaction_type_normalized'] == 'sale'
        assert str(row['transaction_date']) == '2026-09-17'
        assert str(row['disclosure_date']) == '2026-10-02'
        assert row['symbol_resolution_status'] == 'reviewed_sec_listing'
        assert congress_transaction_hash({**row, 'ticker_normalized': None}) == row['normalized_hash']
    assert parse_document('house_ptr', raw, metadata)[1] == parsed
    assert all(r['symbol'] is None for r in parsed['source_report']['transactions'])
    assert parsed['source_report']['scan_review']['row_count'] == 4


@pytest.mark.parametrize('field,value', [
    ('filing_id', '99999'), ('filing_date', '2026-10-01'), ('member_name', 'Another Member'),
    ('district', 'WI01'), ('form', 'PTR/A'), ('url', 'https://example.com/file.pdf'), ('key', '2025:9116361'),
])
def test_review_cannot_be_attached_to_another_discovery(source, field, value):
    raw, metadata, _ = source
    with pytest.raises(DirectSourceError, match='differs from discovery'):
        parse_house_pdf(raw, {**metadata, field: value})


def test_changed_bytes_and_unreviewed_scans_stay_held(source):
    raw, metadata, _ = source
    with pytest.raises(DirectSourceError, match='scanned form/OCR review required'):
        parse_house_pdf(raw + b'\n% changed source', metadata)
    assert house_ptr_review.reviewed_house_scan(b'%PDF unknown', metadata, 1) is None
    with pytest.raises(ValueError, match='page population'):
        house_ptr_review.reviewed_house_scan(raw, metadata, 2)


@pytest.mark.parametrize('mutation', ['row_missing', 'duplicate_row', 'unknown_action', 'unknown_amount',
    'missing_mapping', 'future_mapping', 'duplicate_review', 'wrong_page', 'amendment'])
def test_incomplete_review_is_rejected(source, tmp_path, monkeypatch, mutation):
    raw, metadata, registry = source
    review = registry['reviews'][0]
    row = review['rows'][0]
    if mutation == 'row_missing':
        review['rows'].pop()
    elif mutation == 'duplicate_row':
        review['rows'][1] = copy.deepcopy(row)
    elif mutation == 'unknown_action':
        row['action_checkbox'] = 'Unreadable'
    elif mutation == 'unknown_amount':
        row['amount_checkbox'] = '?'
    elif mutation == 'missing_mapping':
        row['security_mapping']['source_url'] = ''
    elif mutation == 'future_mapping':
        row['security_mapping']['report_period'] = '2027-01-01'
    elif mutation == 'duplicate_review':
        registry['reviews'].append(copy.deepcopy(review))
    elif mutation == 'wrong_page':
        row['page'] = 2
    else:
        review['amendment_flag'] = True
    path = tmp_path / 'reviews.json'
    path.write_text(json.dumps(registry), encoding='utf-8')
    monkeypatch.setattr(house_ptr_review, 'REGISTRY', path)
    with pytest.raises(DirectSourceError):
        parse_house_pdf(raw, metadata)
