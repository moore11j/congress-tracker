"""Common-shareholder earnings for common-equity market valuation.

Parent earnings still support parent-equity ROE. Neither absent preferred
dividends nor absent participating interests establishes a common numerator.
"""
from copy import deepcopy
from datetime import date, timedelta
from zoneinfo import ZoneInfo
import hashlib
import json
import math

VERSION = 'sec_common_income_v1'
TAGS = {'common_income': ('NetIncomeLossAvailableToCommonStockholdersBasic',),
        'common_preferred_dividends': ('PreferredStockDividendsIncomeStatementImpact',)}


def resolve(facts_raw, company_raw, evidence, *, symbol, cik, observed_at,
            history_raw=None, filing_supplement=None):
    from app.services import sec_fundamentals as sec
    from app.services.sec_financial_continuity import record_sets
    from app.services import sec_parent_statement_income as reviewed
    result = {'methodology': VERSION, 'basis': 'income available to common shareholders',
              'status': 'unavailable', 'point': None}
    if evidence.get('reporting_currency', 'USD') != 'USD':
        return {**result, 'reason': 'common_income_currency_not_usd'}
    if not evidence.get('period_end') or not evidence.get('period_start'):
        return {**result, 'reason': 'no_current_financial_period'}
    payload, company = json.loads(facts_raw), json.loads(company_raw)
    cik = str(cik).zfill(10)
    if any(str(x).zfill(10) != cik for x in (payload.get('cik'),company.get('cik'),evidence.get('cik'))):
        raise ValueError('common_income_cik_mismatch')
    from app.utils.symbols import canonical_symbol
    if symbol not in [canonical_symbol(x) for x in company.get('tickers',[])]:
        raise ValueError('common_income_symbol_mismatch')
    available = date.fromisoformat(evidence['available_by'])
    if (observed_at.tzinfo is None or available >
            observed_at.astimezone(ZoneInfo('America/New_York')).date() - timedelta(days=1)):
        raise ValueError('common_income_availability_mismatch')
    if evidence['facts']['sha256'] != hashlib.sha256(facts_raw).hexdigest():
        raise ValueError('common_income_facts_mismatch')
    rows, continuity = record_sets(payload, symbol=symbol, cik=cik, available_by=available,
        observed_at=observed_at, history_raw=history_raw, filing_supplement=filing_supplement,
        # These are income-only records; do not run the unrelated bank-revenue projection.
        metrics={m:(tags,'USD') for m,tags in TAGS.items()})
    common = dict(rows['common_income'])
    # JEF's reviewed original statements independently establish common earnings
    # and preferred dividends. Reuse only an already accepted exact parent proof.
    # Reported common values/conflicts always win over this original review.
    parent = evidence.get('current',{}).get('net_income') or {}
    profile = reviewed.reviews().get(symbol) if symbol == 'JEF' else None
    if profile and profile['cik'] == cik:
        for fact in parent.get('inputs',[]):
            proof = fact.get('parent_statement_review')
            if not proof or fact.get('derivation') != reviewed.VERSION:
                continue
            matching = next((p for p in profile['periods'] if all(proof.get(k)==v for k,v in p.items())),None)
            if (matching is None or fact.get('value') != matching['value']
                    or any(fact.get(k) != matching[k] for k in ('start','end','filed','accession','unit'))):
                continue
            key = (date.fromisoformat(matching['start']),date.fromisoformat(matching['end']))
            if key in common:
                continue
            dividend = rows['common_preferred_dividends'].get(key)
            expected = next(x for x in matching['statement_outputs'] if x['tag']=='PreferredStockDividendsIncomeStatementImpact')
            if (not dividend or any(dividend.get(k) != matching[k] for k in ('start','end','filed','accession','unit'))
                    or dividend.get('value') != expected['value'] or dividend.get('derived')
                    or dividend.get('source_cik', cik) != cik
                    or any(k in dividend and dividend[k] != value for k,value in (
                        ('source_sha256', matching['source_sha256']),
                        ('source_url', matching['url']), ('context_id', expected['context'])))):
                continue
            reported = next(x for x in matching['statement_outputs'] if x['namespace']=='jef')
            value = fact['value'] - dividend['value']
            if not math.isfinite(value) or value != reported['value']:
                continue
            common[key] = {**deepcopy(fact), 'value':value, 'tag':reported['tag'],
                           'derivation':VERSION, 'formula':'reviewed parent income minus explicitly reported preferred dividends; equals original common-shareholder income',
                           'inputs':[deepcopy(fact),deepcopy(dividend)],
                           'original_common_context':reported['context']}
    end = date.fromisoformat(evidence['period_end'])
    point = sec._ttm(common,end)
    if point is None or point['start'] != evidence['period_start']:
        return {**result,'reason':'incomplete_or_unverified_common_shareholder_income'}
    return {**result,'status':'available','point':point,
            'source_cik':cik,'facts_sha256':evidence['facts']['sha256'],
            'financial_continuity':continuity}
