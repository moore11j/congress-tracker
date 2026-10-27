"""Offline contract audit for saved Massive Financials responses.

No transport, database, provider selection or publication. Run only on responses
obtained during a separately authorized subscription validation. Candidate values
are comparison inputs, never approved Walnut fundamentals.
"""
from __future__ import annotations

import argparse
from datetime import date
import hashlib
import json
import math
from pathlib import Path
import re

MAX_BYTES = 4 * 1024 * 1024
MAX_ROWS = 100
KINDS = {'ratios', 'income-statements', 'balance-sheets', 'cash-flow-statements'}
RATIOS = {
    'current': ('current_ratio', 1), 'return_on_equity': ('roe', 100),
    'ev_to_ebitda': ('ev_to_ebitda', 1), 'price_to_earnings': ('trailing_pe', 1),
    'price_to_sales': ('price_to_sales', 1),
}
IDENTITY = {'cik', 'ticker', 'tickers', 'date', 'period_end', 'filing_date',
            'fiscal_year', 'fiscal_quarter', 'timeframe'}


class ContractError(ValueError):
    pass


def fail(reason):
    raise ContractError(reason)


def day(value):
    if not isinstance(value, str) or not re.fullmatch(r'\d{4}-\d{2}-\d{2}', value):
        fail('invalid_date')
    try:
        return date.fromisoformat(value)
    except ValueError:
        fail('invalid_date')


def cik_id(value):
    if not isinstance(value, str) or not re.fullmatch(r'\d{1,10}', value) or int(value) == 0:
        fail('invalid_cik')
    return value.zfill(10)


def number(value):
    if value is None:
        return None
    try:
        valid = type(value) in (int, float) and math.isfinite(value)
    except OverflowError:
        valid = False
    if not valid:
        fail('invalid_number')
    return value


def normalize(raw, kind, *, symbol, cik, observed_date):
    """Validate a complete bounded response; preserve provider definitions."""
    observed = day(observed_date)
    if kind not in KINDS or not isinstance(symbol, str) or not re.fullmatch(r'[A-Z]{1,6}(?:[.-][A-Z])?', symbol):
        fail('invalid_scope')
    expected_symbol, expected_cik = symbol.replace('-', '.'), cik_id(cik)
    if not isinstance(raw, dict) or raw.get('status') != 'OK' or raw.get('error'):
        fail('invalid_response')
    if raw.get('next_url'):
        fail('truncated_response')
    rows = raw.get('results')
    if not isinstance(rows, list) or len(rows) > MAX_ROWS:
        fail('invalid_rows')
    try:
        encoded = json.dumps(raw, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()
    except (ValueError, TypeError, OverflowError):
        fail('invalid_json_value')
    if len(encoded) > MAX_BYTES:
        fail('response_too_large')
    result = {}
    for item in rows:
        if not isinstance(item, dict) or cik_id(item.get('cik')) != expected_cik:
            fail('issuer_mismatch')
        if kind == 'ratios':
            if item.get('ticker') != expected_symbol:
                fail('ticker_mismatch')
            stamp = day(item.get('date'))
            if stamp > observed:
                fail('future_snapshot')
            key = stamp.isoformat()
            record = {'snapshotDate': key, 'financialPeriod': None,
                      'stale': (observed - stamp).days > 5}
        else:
            symbols = item.get('tickers')
            if not isinstance(symbols, list) or any(not isinstance(s, str) for s in symbols) or expected_symbol not in symbols:
                fail('ticker_mismatch')
            period, filed = day(item.get('period_end')), day(item.get('filing_date'))
            if period > filed or filed > observed:
                fail('invalid_statement_dates')
            frequency = item.get('timeframe')
            allowed = {'annual', 'quarterly'} | ({'trailing_twelve_months'} if kind != 'balance-sheets' else set())
            if frequency not in allowed:
                fail('invalid_timeframe')
            year, quarter = item.get('fiscal_year'), item.get('fiscal_quarter')
            if type(year) is not int or not 1900 <= year <= observed.year + 1:
                fail('invalid_fiscal_year')
            if quarter is not None and (type(quarter) is not int or not 1 <= quarter <= 4):
                fail('invalid_fiscal_quarter')
            if frequency == 'quarterly' and quarter is None:
                fail('missing_fiscal_quarter')
            key = f'{period}:{frequency}'
            record = {'periodEnd': period.isoformat(), 'latestReportingFilingDate': filed.isoformat(),
                      'originalAvailabilityDate': None, 'timeframe': frequency,
                      'fiscalYear': year, 'fiscalQuarter': quarter,
                      'stale': (observed - period).days > (550 if frequency == 'annual' else 180)}
        # Keep separate consolidated/common income, parent/total equity and signed
        # cash flows. Never infer currency or rename provider EPS as reported EPS.
        values = {name: number(value) for name, value in item.items() if name not in IDENTITY}
        record['providerValues'] = values
        if kind == 'ratios':
            record['candidateValues'] = {target: number(values[source] * scale)
                for source, (target, scale) in RATIOS.items() if values.get(source) is not None}
        if key in result and result[key] != record:
            fail('conflicting_period')
        result[key] = record
    if kind == 'ratios' and len(result) > 1:
        fail('multiple_current_snapshots')
    return {'version': 'massive_financials_contract_v1', 'kind': kind, 'symbol': symbol,
            'cik': expected_cik, 'observedDate': observed_date,
            'responseSha256': hashlib.sha256(encoded).hexdigest(),
            'publicationEligible': False, 'currency': None, 'accountingBasis': 'unverified',
            'status': 'empty' if not result else 'requires_source_comparison',
            'items': [result[key] for key in sorted(result)],
            'holds': ['live_coverage_unverified', 'currency_and_accounting_basis_unverified',
                      'original_publication_time_unverified', 'walnut_definitions_unverified']}


def audit(bundle):
    if not isinstance(bundle, dict) or not isinstance(bundle.get('responses'), dict):
        fail('invalid_bundle')
    responses = bundle['responses']
    if not responses or not set(responses) <= KINDS:
        fail('invalid_endpoints')
    output = {kind: normalize(raw, kind, symbol=bundle.get('symbol'), cik=bundle.get('cik'),
                             observed_date=bundle.get('observed_date'))
              for kind, raw in sorted(responses.items())}
    return {'publicationEligible': False, 'source': 'massive', 'endpoints': output,
            'missingEndpoints': sorted(KINDS - set(responses)),
            'networkRequests': 0, 'databaseWrites': 0}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('bundle', type=Path, help='Local JSON with symbol, cik, observed_date, responses')
    args = parser.parse_args()
    with args.bundle.open('rb') as source:
        raw = source.read(MAX_BYTES + 1)
    if len(raw) > MAX_BYTES:
        fail('bundle_too_large')
    def reject_duplicate(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                fail('duplicate_json_key')
            result[key] = value
        return result
    parsed = json.loads(raw, object_pairs_hook=reject_duplicate)
    print(json.dumps(audit(parsed), sort_keys=True, allow_nan=False))


if __name__ == '__main__':
    main()
