"""Parent income recovered only from exact reviewed original statements.

Do not generalize parent pretax income, common-shareholder income or a partial
minority allocation into parent net income. Each reviewed calculation has an
independent statement reconciliation and remains bound to its filing values.
"""
from copy import deepcopy
from datetime import date
from functools import lru_cache
import json
import math
from pathlib import Path

VERSION = 'sec_parent_statement_review_v1'
SYMBOLS = frozenset({'FLEX', 'FOX', 'JEF'})
TAGS = {'reviewed_parent_nonredeemable': ('NetIncomeLossAttributableToNonredeemableNoncontrollingInterest',)}


@lru_cache(maxsize=1)
def reviews():
    return json.loads(Path(__file__).with_name('sec_parent_statement_reviews.json').read_text(encoding='utf-8'))


def apply(records, *, symbol, cik):
    if symbol not in SYMBOLS:
        return records, None
    profile = reviews()[symbol]
    if str(cik).zfill(10) != profile['cik']:
        raise ValueError('parent_statement_review_cik_mismatch')
    parent = dict(records.get('net_income', {}))
    accepted, holds = [], {}
    for proof in profile['periods']:
        key = (date.fromisoformat(proof['start']), date.fromisoformat(proof['end']))
        # A reported parent amount, including a conflict, always takes priority.
        if key in parent:
            continue
        inputs, reason = [], None
        # Reject changed or newly reported allocations even when they are not
        # part of the arithmetic. A reviewed overlap is not a generic waiver.
        for metric, expected in proof['allocation_guards'].items():
            rows = records.get(metric, {})
            actual = rows.get(key)
            if (expected is None and key in rows) or (expected is not None and (
                    not actual or any(actual.get(k) != v for k,v in expected.items()))):
                reason = 'changed_allocation_evidence'
                break
        if reason:
            holds[reason] = holds.get(reason, 0)+1
            continue
        for expected in proof['inputs']:
            fact = records.get(expected['metric'], {}).get(key)
            if not fact:
                reason = 'missing_or_conflicting_component'
            elif fact.get('derived') or fact.get('tag') != expected['tag']:
                reason = 'reported_component_required'
            elif any(fact.get(k) != proof[k] for k in ('start', 'end', 'filed', 'accession', 'unit')):
                reason = 'unreviewed_filing_or_period'
            elif fact.get('source_cik', profile['cik']) != profile['cik']:
                reason = 'source_identity_mismatch'
            elif type(fact.get('value')) not in (int, float) or not math.isfinite(fact['value']) or fact['value'] != expected['value']:
                reason = 'unreviewed_component_value'
            elif any(k in fact and fact[k] != expected_value for k, expected_value in (
                    ('source_sha256', proof['source_sha256']), ('source_url', proof['url']), ('context_id', expected['context']))):
                reason = 'original_source_mismatch'
            if reason:
                break
            inputs.append(fact)
        if reason is None:
            value = sum(f['value']*e['coefficient'] for f,e in zip(inputs, proof['inputs']))
            if not math.isfinite(value) or value != proof['value']:
                reason = 'statement_reconciliation_mismatch'
        if reason:
            holds[reason] = holds.get(reason, 0)+1
            continue
        evidence = {**deepcopy(proof), 'cik': profile['cik'], 'reviewed_at': profile['reviewed_at'], 'methodology': VERSION}
        parent[key] = {**inputs[0], 'value': value, 'tag': 'reviewed_parent_net_income',
                       'derived': True, 'derivation': VERSION,
                       'formula': proof['basis'], 'inputs': deepcopy(inputs),
                       'parent_statement_review': evidence}
        accepted.append(evidence)
    return {**records, 'net_income': parent}, {
        'methodology': VERSION, 'accepted_periods': accepted, 'holds': holds,
        'future_filings': 'Require a new original-statement review.',
        'missing_allocations': 'Never interpreted as zero.'}


def is_reviewed(point):
    return bool(point and any(f.get('derivation') == VERSION for f in point.get('inputs', [])))
