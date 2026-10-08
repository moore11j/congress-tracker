import copy
import json
from pathlib import Path

import pytest

from app.services.direct_congress_reconciliation import (
    document_identity, reconcile_direct_congress, resolve_direct_member,
)


@pytest.fixture
def real_case():
    return json.loads((Path(__file__).with_name('fixtures') / 'congress_whitehouse_reconciliation.json').read_text())


def plan(case):
    return reconcile_direct_congress(case['parsed'], case['members'][0], **{
        key: case[key] for key in ('filings', 'transactions', 'securities', 'members', 'events')})


def test_actual_five_row_filing_holds_eighteen_legacy_rows_without_changes(real_case):
    before = copy.deepcopy(real_case)
    result = plan(real_case)
    assert result['status'] == 'held'
    assert len(real_case['parsed']['transactions']) == 5
    assert len(real_case['transactions']) == 18
    assert len(result['document_event_ids']) == 15
    assert len(result['ambiguous_source_rows']) == 5
    assert len(result['existing_only_ids']) == 3
    assert not result['new_source_rows']
    assert real_case == before and plan(real_case) == result


@pytest.fixture
def clean_case(real_case):
    # The five earliest already-correct rows and their existing public IDs.
    real_case['transactions'] = [r for r in real_case['transactions'] if 23181 <= r['id'] <= 23185]
    real_case['events'] = [e for e in real_case['events'] if 461243 <= e['id'] <= 461247]
    return real_case


def test_exact_one_to_one_preserves_both_existing_layers(clean_case):
    result = plan(clean_case)
    assert result['status'] == 'existing'
    assert {row['transaction_id'] for row in result['matched']} == set(range(23181, 23186))
    assert {i for row in result['matched'] for i in row['event_ids']} == set(range(461243, 461248))
    assert not result['reasons']


@pytest.mark.parametrize('mutation', ['source_lot', 'provider_label', 'date', 'owner', 'instrument', 'missing_event', 'orphan_event', 'extra_filing'])
def test_discrepancies_never_become_new_alerts(clean_case, mutation):
    if mutation == 'source_lot':
        row = copy.deepcopy(clean_case['parsed']['transactions'][0])
        row.update(source_line_ref='6', normalized_hash='distinct-source-lot')
        clean_case['parsed']['transactions'].append(row)
    elif mutation == 'provider_label':
        row = {**clean_case['transactions'][0], 'id': 90000, 'transaction_type': 'sale (partial)'}
        clean_case['transactions'].append(row)
    elif mutation == 'date':
        clean_case['filings'][0]['filing_date'] = '2026-09-30'
    elif mutation == 'owner':
        clean_case['transactions'][0]['owner_type'] = 'dependent'
    elif mutation == 'instrument':
        clean_case['securities'][0]['asset_class'] = 'Option'
    elif mutation == 'missing_event':
        clean_case['events'].pop()
    elif mutation == 'orphan_event':
        clean_case['transactions'].pop()
    else:
        clean_case['filings'].append({**clean_case['filings'][0], 'id': 90001})
    result = plan(clean_case)
    assert result['status'] == 'held' and result['reasons']


def test_orphan_document_event_blocks_apparently_new_filing(clean_case):
    clean_case['filings'] = clean_case['transactions'] = []
    result = plan(clean_case)
    assert result['status'] == 'held'
    assert len(result['orphan_event_ids']) == 5


@pytest.mark.parametrize('url', ['https://evil.example/public_disc/ptr-pdfs/2026/1.pdf',
    'https://efdsearch.senate.gov:bad/search/view/ptr/123', 'https://clerk.house.gov/public_disc/ptr-pdfs/2026/1.pdf?redirect=evil'])
def test_source_document_identity_rejects_unreviewed_urls(url):
    assert document_identity(url) is None


def test_new_filing_without_existing_records_is_only_a_plan(clean_case):
    clean_case['filings'] = clean_case['transactions'] = clean_case['events'] = []
    assert plan(clean_case)['status'] == 'new'
    assert len(plan(clean_case)['new_source_rows']) == 5


def test_economic_overlap_blocks_new_filing_even_without_document_link(clean_case):
    clean_case['filings'] = clean_case['transactions'] = []
    row = clean_case['parsed']['transactions'][0]
    clean_case['events'] = [{'id': 123, 'member_bioguide_id': 'W000802', 'chamber': 'senate',
        'symbol': row['ticker_normalized'], 'trade_type': 'sale (partial)',
        'amount_min': row['amount_low'], 'amount_max': row['amount_high'],
        'payload_json': json.dumps({'trade_date': row['transaction_date'], 'owner_type': row['owner_normalized']})}]
    result = plan(clean_case)
    assert result['status'] == 'held' and result['overlap_event_ids'] == [123]


def member_directory():
    return [{'id': {'bioguide': 'H001082'}, 'name': {'first': 'Kevin', 'middle': 'R.', 'last': 'Hern', 'official_full': 'Kevin Hern'},
             'terms': [{'type': 'rep', 'state': 'OK', 'district': 1, 'party': 'Republican', 'start': '2025-01-03', 'end': '2027-01-03'}]}]


def test_member_requires_name_district_chamber_and_current_term():
    meta = {'member_name': 'Kevin Hern', 'district': 'OK01', 'filing_date': '2026-09-21'}
    assert resolve_direct_member(meta, 'house', member_directory())['member']['bioguide_id'] == 'H001082'
    for changed in [{'member_name': 'Someone Hern'}, {'district': 'OK02'}, {'filing_date': '2027-01-03'}]:
        assert resolve_direct_member({**meta, **changed}, 'house', member_directory())['status'] == 'held'
    assert resolve_direct_member(meta, 'senate', member_directory())['status'] == 'held'
    assert resolve_direct_member(meta, 'house', member_directory() * 2)['status'] == 'held'


def test_repeated_source_identity_rejected(clean_case):
    clean_case['parsed']['transactions'].append(copy.deepcopy(clean_case['parsed']['transactions'][0]))
    with pytest.raises(ValueError, match='identity'):
        plan(clean_case)
