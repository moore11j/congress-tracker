"""Independently supported USD GAAP metrics when revenue coverage is missing.

Revenue-dependent calculations remain unavailable. No synthetic zero amounts.
"""
from datetime import date, timedelta
import hashlib
import json
import math

VERSION = 'sec_independent_financials_v3'
# Legacy per-symbol cache versions only, never an eligibility allowlist.
PROFILES = {'ANIX': '0000715446', 'BCDA': '0000925741'}


def project(records, company, *, symbol, cik, facts_raw, company_raw, observed_at, available_by, filing_supplement=None, currency="USD", namespace="us-gaap", source_context=None, native_selection=None):
    from app.services.sec_fundamentals import _ttm, _prior_end, SecFundamentalsCoverageError
    if native_selection is None and (currency != 'USD' or namespace != 'us-gaap'):
        raise SecFundamentalsCoverageError('Independent financial metrics require supported reporting currency and taxonomy')
    # The full reader already accepts evidence-qualified USD GAAP foreign
    # filings. Revalidate that same original-bound selection here instead of
    # falling back to the domestic-only directory filter when revenue is absent.
    standard_selection = None
    if native_selection is None and (filing_supplement or {}).get('evidence', {}).get('standard_foreign_reporting'):
        from app.services.sec_tagged_foreign import authorized
        standard_selection = authorized(json.loads(facts_raw), company, symbol=symbol, cik=cik,
            available_by=available_by, supplement=filing_supplement)
        if standard_selection is None:
            raise SecFundamentalsCoverageError('Unverified standard foreign partial-financial basis')
    reporting_selection = native_selection or standard_selection
    recent = company.get('filings', {}).get('recent', {})
    filings = []
    for i, form in enumerate(recent.get('form', [])):
        if form not in {'10-K', '10-Q', '10-K/A', '10-Q/A', '10-KT', '10-QT', '10-KT/A', '10-QT/A', '20-F', '20-F/A', '40-F', '40-F/A'}:
            continue
        try:
            end = date.fromisoformat(recent['reportDate'][i])
            filed = date.fromisoformat(recent['filingDate'][i])
            accession = recent['accessionNumber'][i]
        except (KeyError, IndexError, ValueError, TypeError):
            raise SecFundamentalsCoverageError('Incomplete partial-financial filing identity') from None
        if end <= filed <= available_by and accession:
            filings.append((end, filed, accession, form))
    if native_selection is not None:
        from app.services.sec_statement_currency import REPORTING_POLICY
        if (native_selection.get('currency') != currency or native_selection.get('namespace') != namespace
                or native_selection.get('currency_policy') != REPORTING_POLICY):
            raise SecFundamentalsCoverageError('Unverified native partial-financial basis')
    if reporting_selection is not None:
        current_filing = reporting_selection['current']
        filings = [(date.fromisoformat(current_filing['report_date']),date.fromisoformat(current_filing['filed']),
                    current_filing['accession'],current_filing['form'])]
    if not filings:
        raise SecFundamentalsCoverageError('No completed partial-financial filing')
    end, filed, accession, form = max(filings)
    if reporting_selection is None and form not in {'10-K', '10-Q', '10-K/A', '10-Q/A'}:
        raise SecFundamentalsCoverageError('Latest partial-financial reporting form requires supported period rules')
    if (available_by-end).days > 180:
        raise SecFundamentalsCoverageError('Stale partial-financial filing')
    candidates = [r for r in filings if r[:2] == (end, filed)]
    if len({r[2] for r in candidates}) != 1:
        raise SecFundamentalsCoverageError('Ambiguous partial-financial filing')

    def instant(name, at=end):
        fact = records[name].get((None, at))
        if at == end and fact and (fact['accession'] != accession or fact['filed'] != str(filed)):
            return None
        return fact

    def trailing(name):
        fact = _ttm(records[name], end)
        if fact and not any(f['end'] == str(end) and f['accession'] == accession
                            and f['filed'] == str(filed) for f in fact['inputs']):
            return None
        return fact

    current = {name: trailing(name) for name in ('net_income', 'operating_cash_flow', 'capex')}
    current['revenue'] = None
    balance = {name: instant(name) for name in ('equity', 'current_assets', 'current_liabilities')}
    income, cash, capex = (current[k] for k in ('net_income', 'operating_cash_flow', 'capex'))
    values = {'symbol': symbol, 'provider': 'sec_edgar', 'company_name': company['name'],
              'fetched_at': observed_at, 'period_date': end, 'status': 'ok', 'error': None}
    tickers, exchanges = company.get('tickers', []), company.get('exchanges', [])
    values['exchange'] = exchanges[tickers.index(symbol)] if symbol in tickers and len(exchanges) == len(tickers) else None
    calculations = {}
    prior = {}
    def save(name, value, formula):
        if math.isfinite(value):
            values[name] = value
            calculations[name] = formula

    assets, liabilities = balance['current_assets'], balance['current_liabilities']
    if assets and liabilities and assets['value'] >= 0 and liabilities['value'] > 0:
        save('current_ratio', assets['value']/liabilities['value'], 'period-end current assets / current liabilities')
    if income and balance['equity']:
        beginning = date.fromisoformat(income['start'])-timedelta(days=1)
        balance['beginning_equity'] = instant('equity', beginning)
        old = balance['beginning_equity']
        if old and min(old['value'], balance['equity']['value']) > 0:
            save('roe', income['value']/((old['value']+balance['equity']['value'])/2)*100,
                 'TTM net income / average beginning and ending equity * 100')
    if currency == 'USD' and cash and capex and cash['start'] == capex['start'] and capex['value'] >= 0:
        save('free_cash_flow', cash['value']-capex['value'],
             'TTM operating cash flow minus TTM cash paid for property, plant and equipment')
        prior_end = _prior_end(records['operating_cash_flow'], end)
        if prior_end is not None:
            prior = {name: _ttm(records[name], prior_end) for name in ('operating_cash_flow', 'capex')}
            old_cash, old_capex = prior['operating_cash_flow'], prior['capex']
            if (old_cash and old_capex and old_cash['start'] == old_capex['start']
                    and 350 <= (date.fromisoformat(cash['start'])-date.fromisoformat(old_cash['start'])).days <= 378
                    and old_capex['value'] >= 0):
                old_fcf = old_cash['value']-old_capex['value']
                if old_fcf > 0 and 'free_cash_flow' in values:
                    save('fcf_growth', (values['free_cash_flow']/old_fcf-1)*100,
                         '(TTM free cash flow / prior TTM free cash flow - 1) * 100')
    if not calculations:
        raise SecFundamentalsCoverageError('No independently supported partial financial metrics')
    evidence = {'methodology': VERSION, 'financial_status': 'partial', 'cik': cik,
        'available_by': str(available_by), 'availability_basis': 'filed date, available after completed New York filing day',
        'facts': {'url': f'https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json', 'sha256': hashlib.sha256(facts_raw).hexdigest()},
        'company': {'url': f'https://data.sec.gov/submissions/CIK{cik}.json', 'sha256': hashlib.sha256(company_raw).hexdigest()},
        'reporting_currency': currency, 'period_end': str(end),
        'period_start': (income or cash or {}).get('start'),
        'current': current, 'prior': prior, 'instant': balance, 'calculations': calculations,
        'partial_financial_basis': {'accession': accession, 'filed': str(filed), 'form': form,
            'reason': 'current_revenue_unavailable', 'eligibility': 'verified_identity_and_current_filing_inputs'},
        'limitations': ['Current revenue and revenue-dependent ratios are unavailable, not zero.',
            f'Only independently aligned {currency} {namespace} balance-sheet and cash-flow metrics are included.',
            'Negative income and cash flow remain negative; missing capital expenditure is not zero.']}
    if native_selection is not None:
        evidence['native_reporting'] = native_selection
        evidence['partial_financial_basis']['reason'] = 'current_ttm_revenue_unavailable'
        evidence['limitations'][0] = 'Current trailing-twelve-month revenue is unavailable; reported shorter periods may remain available in the financial statements.'
        evidence['limitations'].append('No inferred foreign-exchange, ADS or per-share mapping; native cash amounts do not populate USD caches.')
    if standard_selection is not None:
        evidence['standard_foreign_reporting'] = standard_selection
        evidence['standard_partial_policy'] = 'qualified_us_gaap_partial_financials_v1'
        evidence['partial_financial_basis']['reason'] = 'current_ttm_revenue_unavailable'
        evidence['limitations'][0] = 'Current trailing-twelve-month revenue is unavailable; reported shorter periods may remain available in the financial statements.'
    if source_context:
        evidence.update({key: value for key, value in source_context.items() if value is not None})
    if filing_supplement is not None:
        evidence['inline_filing_supplement'] = filing_supplement['evidence']
    values['source_evidence_json'] = json.dumps(evidence, sort_keys=True)
    return values
