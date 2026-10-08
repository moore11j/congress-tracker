import json
from pathlib import Path

import pytest

from app.clients import direct_sources
from app.clients.house_ptr_pdf import extract_house_columns
from app.services.direct_feed_collection import parse_document
from app.services.official_congress import congress_transaction_hash


@pytest.fixture
def columns():
    return json.loads((Path(__file__).with_name('fixtures') / 'house_hern_columns.json').read_text())['pages']


def test_real_split_row_and_identical_account_trades(columns):
    rows = extract_house_columns(columns)
    assert len(rows) == 4
    assert rows[0]['asset'] == rows[1]['asset'] == 'Apple Inc. - Common Stock (AAPL)'
    assert rows[0]['owner'] == rows[1]['owner'] == 'DC'
    assert 'Kelby Austin Hern Trust' in rows[0]['source_notes']
    assert 'Kaden Everett Hern Trust' in rows[1]['source_notes']
    assert rows[2]['asset'] == 'Devon Energy Corporation Common Stock (DVN)'
    assert rows[2]['amount'] == '$100,001 - $250,000'
    assert rows[2]['date'] == '09/02/2026'
    assert rows[3]['date'] == '09/04/2026'
    assert all(row['type'] == 'S (partial)' for row in rows)


@pytest.mark.parametrize('mutation', ['missing_upper_amount', 'bad_action', 'missing_notice', 'missing_status', 'missing_heading'])
def test_incomplete_or_changed_layout_is_held(columns, mutation):
    if mutation == 'missing_upper_amount':
        columns[2] = [token for token in columns[2] if token[2] != '$250,000']
    elif mutation == 'bad_action':
        next(token for token in columns[0] if token[2] == 'S (partial)')[2] = 'UNKNOWN'
    elif mutation == 'missing_notice':
        columns[0] = [token for token in columns[0] if token[2] != '09/15/2026']
    elif mutation == 'missing_status':
        columns[0] = [token for token in columns[0] if token[2] != 'F S:']
    else:
        columns[2] = [token for token in columns[2] if token[2] != 'Asset']
    with pytest.raises(ValueError):
        extract_house_columns(columns)


def reader_fixture(monkeypatch, columns, *, name='Hon. Kevin Hern', filing='20035491'):
    class Page:
        def __init__(self, tokens, first):
            self.tokens, self.first = tokens, first

        def extract_text(self, *, visitor_text):
            for x, y, value in self.tokens:
                visitor_text(value, [1, 0, 0, 1, 0, 0], [1, 0, 0, 1, x, y], None, 10)
            prefix = f'Name: {name}\nFiling ID #{filing}\n' if self.first else ''
            return prefix + '\n'.join(token[2] for token in self.tokens)
    class Reader:
        pages = [Page(tokens, i == 0) for i, tokens in enumerate(columns)]
    monkeypatch.setattr(direct_sources, 'PdfReader', lambda raw: Reader())
    return {'filing_id': '20035491', 'filing_date': '2026-09-21', 'member_name': 'Kevin Hern',
            'url': 'https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/2026/20035491.pdf'}


def test_full_normalization_preserves_lots_and_source_dates(monkeypatch, columns):
    metadata = reader_fixture(monkeypatch, columns)
    _, report = direct_sources.parse_house_pdf(b'%PDF fixture', metadata)
    assert [r['source_line_ref'] for r in report['transactions']] == ['1', '2', '3', '4']
    assert {r['source_line_ref_kind'] for r in report['transactions']} == {'document_row_ordinal'}
    assert report['transactions'][2]['notification_date'] == '2026-09-15'
    _, normalized, reasons = parse_document('house_ptr', b'%PDF fixture', metadata)
    assert not reasons
    rows = normalized['transactions']
    assert len(rows) == len({r['normalized_hash'] for r in rows}) == 4
    assert rows[2]['amount_low'] == 100001
    assert rows[2]['amount_high'] == 250000
    assert rows[2]['transaction_type_raw'] == 'S (partial)'
    assert rows[2]['transaction_type_normalized'] == 'sale'
    assert parse_document('house_ptr', b'%PDF fixture', metadata)[1] == normalized
    assert congress_transaction_hash({**rows[0], 'member_id': 'H001082',
        'ticker_normalized': 'CORRECTED', 'amount_low': 1200}) == rows[0]['normalized_hash']
    with pytest.raises(ValueError, match='identity'):
        congress_transaction_hash({**rows[0], 'source_line_ref': None})


@pytest.mark.parametrize('override', [{'name': 'Hon. Different Member'}, {'filing': '99999'}])
def test_wrong_document_body_is_rejected(monkeypatch, columns, override):
    metadata = reader_fixture(monkeypatch, columns, **override)
    with pytest.raises(direct_sources.DirectSourceError, match='differs from discovery'):
        direct_sources.parse_house_pdf(b'%PDF fixture', metadata)


def test_printed_ids_are_preserved_and_duplicates_rejected(monkeypatch, columns):
    next_id = 41
    for page in columns:
        for x, y, value in list(page):
            if value == 'S (partial)':
                page.append([25.2, y, str(next_id)])
                next_id += 1
    metadata = reader_fixture(monkeypatch, columns)
    _, report = direct_sources.parse_house_pdf(b'%PDF fixture', metadata)
    assert [r['source_line_ref'] for r in report['transactions']] == ['41', '42', '43', '44']
    assert {r['source_line_ref_kind'] for r in report['transactions']} == {'printed'}
    next(token for token in columns[0] if token[2] == '42')[2] = '41'
    with pytest.raises(ValueError, match='identity'):
        extract_house_columns(columns)


@pytest.mark.parametrize('code', ['4K', 'ZZ'])
def test_unknown_asset_codes_cannot_become_stock_from_a_ticker(monkeypatch, columns, code):
    for page in columns:
        for token in page:
            token[2] = token[2].replace('[ST]', f'[{code}]')
    metadata = reader_fixture(monkeypatch, columns)
    _, report, reasons = parse_document('house_ptr', b'%PDF fixture', metadata)
    assert len(report['transactions']) == 4
    assert all(r['asset_type_normalized'] == 'unresolved' for r in report['transactions'])
    assert 'Unresolved asset classification' in reasons
