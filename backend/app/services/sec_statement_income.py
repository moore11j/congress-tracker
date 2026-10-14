"""Exact original-statement reviews for otherwise ambiguous net-income tags.

The source review is filing- and value-bound. Future filings require a new
review; missing allocations alone never authorize a consolidated substitute.
"""
from copy import deepcopy
from datetime import date
from functools import lru_cache
import json
import math
from pathlib import Path

VERSION = 'sec_statement_income_review_v1'
SYMBOLS = frozenset({'NVDA', 'JNJ'})


@lru_cache(maxsize=1)
def _reviews():
    return json.loads(Path(__file__).with_name('sec_statement_income_reviews.json').read_text())


def apply(records, *, symbol, cik):
    profile = _reviews().get(symbol) if symbol in SYMBOLS else None
    if profile is None:
        return records, None
    if str(cik).zfill(10) != profile['cik']:
        raise ValueError('statement_income_review_cik_mismatch')
    result = dict(records)
    consolidated = dict(records.get('leverage_net_income', {}))
    accepted, holds = [], {}
    for proof in profile['periods']:
        key = (date.fromisoformat(proof['start']), date.fromisoformat(proof['end']))
        # Explicit consolidated observations, including conflicts, take priority.
        if key in consolidated:
            continue
        fact = records.get('net_income', {}).get(key)
        reason = None
        if not fact:
            reason = 'missing_or_conflicting_parent_observation'
        elif fact.get('derived') or fact.get('tag') != 'NetIncomeLoss':
            reason = 'reported_net_income_required'
        elif any(fact.get(k) != proof[k] for k in ('start', 'end', 'filed', 'accession', 'value')):
            reason = 'unreviewed_filing_or_value'
        elif fact.get('unit') != 'USD' or fact.get('source_cik', profile['cik']) != profile['cik']:
            reason = 'source_identity_mismatch'
        elif any(k in fact and fact[k] != proof[p] for k,p in
                 [('source_sha256','source_sha256'), ('source_url','url'), ('context_id','context')]):
            reason = 'original_source_mismatch'
        elif type(fact.get('value')) not in (int,float) or not math.isfinite(fact['value']):
            reason = 'nonfinite_income'
        elif any(key in records.get(name,{}) and (
                not records[name][key] or records[name][key].get('value') != 0)
                for name in ('parent_income_noncontrolling','parent_income_redeemable')):
            reason = 'conflicting_reported_allocation'
        if reason:
            holds[reason] = holds.get(reason,0)+1
            continue
        evidence = {**deepcopy(proof), 'cik':profile['cik'],
                    'reviewed_at':profile['reviewed_at'], 'basis':profile['basis'],
                    'methodology':VERSION}
        consolidated[key] = {**fact, 'consolidated_statement_review':evidence}
        accepted.append(evidence)
    result['leverage_net_income'] = consolidated
    return result, {'methodology':VERSION, 'accepted_periods':accepted, 'holds':holds,
                   'future_filings':'Require a new original-statement review.',
                   'missing_allocations':'Never interpreted as zero.'}
