"""Whole-original candidate qualification, including semantic corruption."""
from copy import deepcopy
from functools import lru_cache
from pathlib import Path
import gzip
import hashlib
import json
import os
import subprocess
import sys

from lxml import html
import pytest
from app.services import sec_primary_operating_subtotal as subtotal

ROOT = Path(__file__).parent / 'fixtures/sec_primary_operating_subtotal'
SOURCES = json.loads((ROOT / 'sources.json').read_text())
EXPECTED = json.loads((ROOT / 'expected.json').read_text(encoding='utf-8'))


@lru_cache(maxsize=6)
def original(index):
    entry = SOURCES[index]
    raw = gzip.decompress((ROOT / entry['file']).read_bytes())
    assert hashlib.sha256(raw).hexdigest() == entry['source']['sha256']
    assert len(raw) == entry['source']['bytes']
    return raw


@pytest.mark.parametrize('index', range(6))
def test_complete_original_reconciliation(index):
    entry = SOURCES[index]
    result = subtotal.qualify(original(index), entry['source'], entry['company'])
    assert result == EXPECTED[entry['source']['symbol'] + '-' + entry['source']['role']]
    periods = result['proof']['periods']
    assert len(periods) in (3, 4)
    assert result['geometry']['checked_numeric_cells'] > 0
    assert result['geometry']['sourcewide_matching_facts'] >= result['geometry']['checked_numeric_cells']
    for period in periods:
        assert period['operating_subtotal'] == period['revenue']['value'] - period['cost_total']['value']
    if entry['source']['symbol'] == 'DMLP':
        assert all(p['operating_subtotal'] == p['bridge']['net_income']['value'] for p in periods)
    else:
        assert all(p['operating_subtotal'] + p['bridge']['nonoperating']['value'] == p['bridge']['pretax']['value'] for p in periods)


def changed(index, mutation):
    entry = SOURCES[index]
    raw = original(index)
    proof = subtotal.qualify(raw, entry['source'], entry['company'])['proof']
    tree = html.fromstring(raw, parser=html.HTMLParser(encoding='utf-8'))
    table = next(t for t in tree.iter('table') if not t.xpath('.//table')
                 and subtotal.preceding_text(t, nonempty=True)[-5:] == proof['headings']
                 and any(n.get('name') == 'us-gaap:CostsAndExpenses' for n in subtotal.nodes(t, 'nonfraction')))
    rows = table.xpath('./tr|./tbody/tr|./thead/tr|./tfoot/tr')
    fact = proof['periods'][0]['expenses'][0]
    node = next(n for n in subtotal.nodes(rows[fact['row']], 'nonfraction') if n.get('contextref') == fact['context'])
    if mutation == 'wrong_year':
        cell = next(c for c in table.xpath('.//td|.//th') if c.text_content().strip() == '2026')
        for child in list(cell):
            cell.remove(child)
        cell.text = '2024'
    elif mutation == 'untagged_expense':
        node.drop_tag()
    elif mutation == 'nil_duplicate':
        duplicate = deepcopy(node)
        duplicate.set('xsi:nil', 'true')
        duplicate.set('style', 'display:none')
        tree.append(duplicate)
    elif mutation == 'wrong_dimension':
        context = next(c for c in subtotal.nodes(tree, 'context') if c.get('id') == fact['context'])
        member = html.Element('xbrldi:explicitmember')
        member.set('dimension', 'us-gaap:RelatedPartyTransactionsByRelatedPartyAxis')
        member.text = 'us-gaap:RelatedPartyMember'
        context.append(member)
    elif mutation == 'expense_value':
        node.text = '999,999'
    elif mutation == 'hidden_expense':
        node.set('style', 'display:none')
    elif mutation == 'wrong_unit':
        node.set('unitref', 'nonexistent')
    elif mutation == 'wrong_scale':
        node.set('scale', '6')
    elif mutation == 'duplicate_context':
        tree.append(deepcopy(subtotal.nodes(tree, 'context')[0]))
    elif mutation == 'rowspan':
        rows[fact['row']][0].set('rowspan', '2')
    elif mutation == 'wrong_owner':
        owners = [n for n in tree.iter() if isinstance(n.tag, str) and not len(n)
                  and subtotal.company_key(n.text_content()) == subtotal.company_key(entry['company']['name'])]
        assert owners
        for owner in owners:
            owner.text = 'Different Corporation'
    elif mutation != 'roundtrip':
        raise AssertionError(mutation)
    raw = html.tostring(tree, encoding='utf-8')
    return raw, {**entry['source'], 'sha256': hashlib.sha256(raw).hexdigest()}, entry['company']


CURRENT = [i for i, e in enumerate(SOURCES) if e['source']['role'] == 'current']


@pytest.mark.parametrize('index', CURRENT)
def test_serialization_control(index):
    assert subtotal.qualify(*changed(index, 'roundtrip'))['proof']['periods']


@pytest.mark.parametrize('index', CURRENT)
@pytest.mark.parametrize('mutation', ['wrong_year', 'untagged_expense', 'nil_duplicate',
    'wrong_dimension', 'expense_value', 'hidden_expense', 'wrong_unit', 'wrong_scale',
    'duplicate_context', 'rowspan', 'wrong_owner'])
def test_semantic_mutation_with_matching_checksum_rejected(index, mutation):
    with pytest.raises(ValueError):
        subtotal.qualify(*changed(index, mutation))


def test_checksum_and_company_are_required():
    e = SOURCES[0]
    with pytest.raises(ValueError):
        subtotal.qualify(original(0) + b' ', e['source'], e['company'])
    with pytest.raises(ValueError):
        subtotal.qualify(original(0), e['source'], {**e['company'], 'cik': '9999999999'})


def test_symbol_is_not_a_qualification_whitelist():
    entry = SOURCES[0]
    result = subtotal.qualify(original(0), {**entry['source'], 'symbol': 'UNLISTED_TEST'}, entry['company'])
    assert result['proof']['symbol'] == 'UNLISTED_TEST'
    assert result['proof']['periods'] == EXPECTED['DMLP-current']['proof']['periods']


@pytest.mark.parametrize('field,value', [
    ('url', 'https://example.com/other.htm'),
    ('primaryDocument', '../wrong.htm'),
    ('accessionNumber', 'not-an-accession'),
    ('form', '8-K'),
    ('filingDate', '2020-01-01'),
])
def test_source_metadata_must_bind_same_filing(field, value):
    entry = SOURCES[0]
    with pytest.raises(ValueError):
        subtotal.qualify(original(0), {**entry['source'], field: value}, entry['company'])


@pytest.mark.parametrize('raw', [b'', 'not bytes', b'x' * (subtotal.MAX_INLINE_BYTES + 1)], ids=['empty', 'wrong-type', 'oversize'])
def test_size_checked_before_html(raw):
    with pytest.raises(ValueError, match='filing_size'):
        subtotal.qualify(raw, {}, {})


def test_optimized_python_keeps_semantic_guards(tmp_path):
    raw, source, company = changed(CURRENT[0], 'untagged_expense')
    (tmp_path / 'source.html').write_bytes(raw)
    (tmp_path / 'metadata.json').write_text(json.dumps([source, company]))
    code = '''import json, pathlib, sys
from app.services.sec_primary_operating_subtotal import qualify
p = pathlib.Path(sys.argv[1])
try:
    qualify((p/'source.html').read_bytes(), *json.loads((p/'metadata.json').read_text()))
except ValueError:
    print('semantic_rejection_under_optimization')
else:
    raise RuntimeError('Unsafe acceptance under optimization')
'''
    result = subprocess.run([sys.executable, '-O', '-c', code, str(tmp_path)],
        env={**os.environ, 'PYTHONPATH': str(Path(__file__).parents[1])},
        capture_output=True, text=True, timeout=45)
    assert result.returncode == 0, result.stderr
    assert 'semantic_rejection_under_optimization' in result.stdout
