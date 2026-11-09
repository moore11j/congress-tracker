from copy import deepcopy
import gzip
import hashlib
import json
from pathlib import Path
import socket

from lxml import html
import pytest

from app.services import sec_lease_debt_evidence as subject

ROOT = Path(__file__).parent/'fixtures/sec_lease_debt'
MANIFEST = json.loads((ROOT/'manifest.json').read_text())


def original(symbol='AZO'):
    meta = MANIFEST[symbol]
    raw = gzip.decompress((ROOT/meta['file']).read_bytes())
    assert len(raw) == meta['bytes'] and hashlib.sha256(raw).hexdigest() == meta['sha256']
    assert meta['complete_original'] is True
    return raw


def project(raw=None, symbol='AZO', **overrides):
    return subject.qualify(original(symbol) if raw is None else raw,
        **{**MANIFEST[symbol]['arguments'], **overrides})


def test_complete_original_reconciles_principal_and_separate_leases_without_selecting_ratio(monkeypatch):
    monkeypatch.setattr(socket.socket, 'connect', lambda *a, **k: pytest.fail('Offline evidence requested network'))
    raw = original()
    proof = project(raw)
    assert proof == project(raw)
    assert proof['source_sha256'] == hashlib.sha256(raw).hexdigest()
    assert proof['borrowings']['total']['value'] == 9_125_000_000
    assert len(proof['borrowings']['components']) == 15
    assert sum(p['value'] for p in proof['borrowings']['components']) == 9_125_000_000
    assert proof['finance_leases']['current']['fact']['value'] == 114_873_000
    assert proof['finance_leases']['noncurrent']['fact']['value'] == 302_455_000
    assert proof['finance_lease_total'] == 417_328_000
    assert proof['gross_borrowings_plus_finance_leases'] == 9_542_328_000
    assert proof['canonical_selection'] is False
    assert 'net_debt_to_ebitda' not in proof
    assert proof['borrowings']['basis'] == 'reported gross principal before discounts and debt issuance costs'
    assert 'operating' not in json.dumps(proof['finance_leases']).lower()


@pytest.mark.parametrize('symbol', ['FOX', 'FWONK'])
def test_other_complete_originals_do_not_infer_missing_finance_leases_as_zero(symbol):
    with pytest.raises(subject.LeaseDebtEvidenceError, match='complete_disjoint_disclosure_unavailable'):
        project(symbol=symbol)


@pytest.mark.parametrize('change', ['cik', 'accession', 'report_date', 'form', 'url'])
def test_original_identity_must_match(change):
    invalid = dict(cik='1', accession='0000000001-26-000001', report_date='2025-08-30',
                   form='10-Q', url='https://example.com/filing.htm')
    with pytest.raises(ValueError):
        project(**{change: invalid[change]})


def _current(tree, tag):
    return next(n for n in tree.iter('ix:nonfraction') if n.get('name') == tag
        and n.get('contextref', '').startswith('As_Of_8_29_2026_'))


def test_mutation_transport_control_preserves_an_unchanged_document():
    baseline = project()
    # Preserve the original document's declared encoding. ASCII character
    # references round-trip without turning whitespace into mojibake.
    roundtrip = project(html.tostring(html.fromstring(original()), encoding='ascii'))
    baseline.pop('source_sha256'); roundtrip.pop('source_sha256')
    assert baseline == roundtrip


@pytest.mark.parametrize('change', ['remove_instrument', 'duplicate_instrument', 'wrong_unit', 'negative',
    'wrong_period', 'nil_lease', 'missing_lease', 'overlap_location', 'unknown_location', 'carrying_label',
    'component_value', 'duplicate_table', 'unsupported_instrument'])
def test_partial_ambiguous_or_conflicting_original_is_held(change):
    tree = html.fromstring(original())
    component = _current(tree, 'us-gaap:SeniorNotes')
    lease = _current(tree, subject.LEASES['current'])
    if change == 'remove_instrument': component.getparent().remove(component)
    elif change == 'duplicate_instrument': component.addnext(deepcopy(component))
    elif change == 'wrong_unit': component.set('unitref', 'missing')
    elif change == 'negative': component.set('sign', '-')
    elif change == 'wrong_period': component.set('contextref', 'As_Of_8_30_2025_rTWJYqbnVEO-rcClLxny5w')
    elif change == 'component_value': component.text = '600,001'
    elif change == 'unsupported_instrument': component.set('name', 'us-gaap:FinanceLeaseLiability')
    elif change == 'nil_lease':
        lease.text = ''; lease.set('xsi:nil', 'true')
    elif change == 'missing_lease':
        row = lease.xpath('ancestor::tr')[0]; row.getparent().remove(row)
    elif change in {'overlap_location', 'unknown_location'}:
        row = lease.xpath('ancestor::tr')[0]
        cells = [cell for cell in row.xpath('./td|./th') if subject._text(cell)]
        cells[1].clear(); cells[1].text = 'Borrowings' if change == 'overlap_location' else 'Other'
    elif change == 'carrying_label':
        fact = _current(tree, subject.TOTAL); row = fact.xpath('ancestor::tr')[0]
        label = next(cell for cell in row.xpath('./td|./th') if subject._text(cell))
        label.clear(); label.text = 'Total debt carrying amount'
    elif change == 'duplicate_table':
        table = lease.xpath('ancestor::table')[0]; table.addnext(deepcopy(table))
    with pytest.raises(ValueError):
        project(html.tostring(tree, encoding='ascii'))


def test_reported_borrowing_components_are_not_mutated():
    result = project()
    assert len({p['member'] for p in result['borrowings']['components']}) == 15
    assert result['period_date'] == '2026-08-29'
    assert result['filed'] == '2026-10-26'
    assert result['unit'] == 'USD'
