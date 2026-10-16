"""Reviewed Novo Nordisk DKK statements and comparable cash-flow ratios."""
from copy import deepcopy
from datetime import date, timedelta
import hashlib
import json
from pathlib import Path
import re
from zoneinfo import ZoneInfo

from app.services.sec_fundamentals import SecFundamentalsCoverageError

VERSION = 'sec_nvo_dkk_statements_v1'
CIK = '0000353278'
END = date(2026, 6, 30)
MAX_BYTES = 15_000_000
REGISTRY = json.loads(Path(__file__).with_name('sec_nvo_sources.json').read_text(encoding='utf-8'))
SOURCES = REGISTRY['sources']
TAGS = {'revenue': 'Revenue', 'gross_profit': 'GrossProfit',
    'operating_income': 'ProfitLossFromOperatingActivities', 'net_income': 'ProfitLoss',
    'equity': 'Equity', 'current_assets': 'CurrentAssets', 'current_liabilities': 'CurrentLiabilities',
    'operating_cash_flow': 'CashFlowsFromUsedInOperatingActivities',
    'capex': 'PurchaseOfPropertyPlantAndEquipmentClassifiedAsInvestingActivities',
    'interest_paid': 'InterestPaidClassifiedAsOperatingActivities',
    'interest_received': 'InterestReceivedClassifiedAsOperatingActivities'}
INCOME = {'revenue': 'Net sales', 'gross_profit': 'Gross profit', 'operating_income': 'Operating profit',
    'net_income': 'NET PROFIT', 'cost': 'Cost of goods sold', 'selling': 'Sales and distribution costs',
    'research': 'Research and development costs', 'administration': 'Administrative costs',
    'other': 'Other operating income and expenses', 'finance_income': 'Financial income',
    'finance_expense': 'Financial expenses', 'pretax': 'Profit before income taxes', 'tax': 'Income taxes'}
BALANCE = {'equity': 'TOTAL EQUITY', 'current_assets': 'TOTAL CURRENT ASSETS',
    'current_liabilities': 'Total current liabilities', 'assets': 'TOTAL ASSETS',
    'liabilities': 'TOTAL LIABILITIES', 'total_balance': 'TOTAL EQUITY AND LIABILITIES',
    'noncurrent_assets': 'TOTAL NON-CURRENT ASSETS', 'noncurrent_liabilities': 'Total non-current liabilities',
    'capital': 'Share capital', 'treasury': 'Treasury shares', 'retained': 'Retained earnings', 'reserves': 'Other reserves'}
CASH = {'operating_cash_flow': 'Net cash flows from operating activities',
    'capex': 'Purchase of property, plant and equipment', 'profit': 'Net profit',
    'tax': 'Income taxes in the income statement', 'da_impairment': 'Depreciation, amortisation and impairment losses',
    'other': 'Other non-cash items', 'working': 'Change in working capital', 'tax_paid': 'Income taxes paid',
    'investing': 'Net cash flows from investing activities', 'financing': 'Net cash flows from financing activities',
    'net_cash': 'Net cash generated from activities', 'opening': 'Cash and cash equivalents at the beginning of the year',
    'fx': 'Exchange gain/(loss) on cash and cash equivalents', 'closing': 'Cash and cash equivalents at the end of the period'}


class NvoFinancialError(SecFundamentalsCoverageError):
    pass


def available_day(observed_at):
    if observed_at.tzinfo is None:
        raise NvoFinancialError('nvo_observation_timezone')
    return observed_at.astimezone(ZoneInfo('America/New_York')).date() - timedelta(days=1)


def validate_company(company, *, symbol, cik, observed_at):
    if (symbol != 'NVO' or str(cik).zfill(10) != CIK or str(company.get('cik', '')).zfill(10) != CIK
            or company.get('tickers', []).count('NVO') != 1 or not company.get('name')):
        raise NvoFinancialError('nvo_company_identity')
    available = available_day(observed_at)
    if not 0 <= (available - END).days <= 180:
        raise NvoFinancialError('nvo_period_stale_or_future')
    fields = ('accessionNumber', 'form', 'filingDate', 'reportDate', 'primaryDocument')
    recent = company.get('filings', {}).get('recent', {})
    arrays = [recent.get(k, []) for k in fields]
    if any(not isinstance(a, list) for a in arrays) or len({len(a) for a in arrays}) != 1:
        raise NvoFinancialError('nvo_directory_shape')
    filings = list(zip(*arrays))
    reviewed = {tuple(s[k] for k in fields) for s in SOURCES.values()}
    for row in reviewed:
        if filings.count(row) != 1 or date.fromisoformat(row[2]) > available:
            raise NvoFinancialError('nvo_original_not_available')
    later = {tuple(s[k] for k in fields) for s in REGISTRY['reviewed_later']}
    for row in filings:
        acc, form, filed, report, name = row
        if form not in ('6-K', '6-K/A', '20-F', '20-F/A') or filed > str(available) or row in reviewed or row in later:
            continue
        if filed >= SOURCES['current']['filingDate'] or (form.endswith('/A') and filed >= SOURCES['prior']['filingDate']):
            raise NvoFinancialError('nvo_new_or_amended_report_requires_review')


def amount(value):
    # A reported dash in a reviewed numeric cell denotes zero; empty is missing.
    if value == '—':
        return 0
    if not isinstance(value, str) or not re.fullmatch(r'(?:[0-9]+|[0-9]{1,3}(?:,[0-9]{3})+|\((?:[0-9]+|[0-9]{1,3}(?:,[0-9]{3})+)\))', value):
        raise NvoFinancialError('nvo_numeric_format')
    result = int(value.strip('()').replace(',', '')) * (-1 if value.startswith('(') else 1) * 1_000_000
    if abs(result) > 10**15:
        raise NvoFinancialError('nvo_numeric_bound')
    return result


def reconciles(total, *components):
    return abs(total - sum(components)) <= (len(components) + 1) * 500_000


def original_tree(raw, key):
    from lxml import html
    if not isinstance(raw, bytes) or not raw or len(raw) > MAX_BYTES:
        raise NvoFinancialError('nvo_document_size')
    if hashlib.sha256(raw).hexdigest() != SOURCES[key]['sha256']:
        raise NvoFinancialError('nvo_original_changed')
    try:
        return html.fromstring(raw)
    except (ValueError, html.etree.ParserError) as exc:
        raise NvoFinancialError('nvo_invalid_html') from exc


def proof(key):
    s = SOURCES[key]
    return {'source_url': s['url'], 'source_sha256': s['sha256'], 'accession': s['accessionNumber'],
        'filed': s['filingDate'], 'currency': 'DKK', 'accounting_basis': 'IFRS'}


def parse_interim(raw, *, key):
    tree = original_tree(raw, key)
    year = int(SOURCES[key]['reportDate'][:4])
    candidates = {'income': [], 'balance': [], 'cash': []}
    for table in tree.xpath('//table[not(.//table)]'):
        rows = [[v for c in row.xpath('./td|./th') if (v := ' '.join(c.text_content().split()))]
            for row in table.xpath('.//tr')]
        rows = [r for r in rows if r]
        labels = {r[0] for r in rows}
        for kind, required in [('income', {'NET PROFIT', 'Gross profit', 'Profit before income taxes'}),
                ('balance', {'TOTAL EQUITY', 'TOTAL CURRENT ASSETS', 'Total current liabilities'}),
                ('cash', {'Net cash flows from operating activities', 'Income taxes paid', 'Net cash flows from investing activities'})]:
            if required <= labels:
                candidates[kind].append(rows)
    if any(len(v) != 1 for v in candidates.values()):
        raise NvoFinancialError('nvo_statements_missing_or_ambiguous')
    inc, bal, cash = (candidates[k][0] for k in ('income', 'balance', 'cash'))
    income_header = (['DKK million', f'Q2 {year}', f'Q2 {year-1}', f'H1 {year}', f'H1 {year-1}'] if key == 'current'
        else ['DKK million', f'H1 {year}', f'H1 {year-1}', f'Q2 {year}', f'Q2 {year-1}'])
    if (inc[0] != income_header or bal[0] != ['DKK million', f'30 Jun {year}', f'31 Dec {year-1}']
            or cash[0] != ['DKK million', f'H1 {year}', f'H1 {year-1}']):
        raise NvoFinancialError('nvo_currency_or_period_columns')
    title = 'Condensed income statement' if key == 'current' else 'Income statement'
    if inc[1] != [title]:
        raise NvoFinancialError('nvo_statement_title')
    def selected(rows, label, width, index=1):
        found = [r for r in rows if r[0] == label]
        if len(found) != 1:
            raise NvoFinancialError('nvo_row_missing_or_duplicate:' + label)
        row = found[0]
        # The complete 2025 SEC original contains an additional unscaled copy
        # in these two rows. Re-fetch matched exactly; only this reviewed shape
        # and exact numeric equivalence are accepted, never arbitrary columns.
        if key == 'prior' and rows is inc and label in ('Operating profit', 'NET PROFIT'):
            if len(row) != 6 or not row[3].isdigit() or int(row[3]) != amount(row[2]):
                raise NvoFinancialError('nvo_prior_duplicate_scale_cell')
            row = row[:3] + row[4:]
        if len(row) != width:
            raise NvoFinancialError('nvo_row_width:' + label)
        return [amount(v) for v in row[index:index+2]]
    income = {k: selected(inc, label, 5, 3 if key == 'current' else 1) for k, label in INCOME.items()}
    balance = {k: selected(bal, label, 3) for k, label in BALANCE.items()}
    cash_values = {k: selected(cash, label, 3) for k, label in CASH.items()}
    for metric in ('interest_paid', 'interest_received'):
        cash_values[metric] = selected(cash, metric.replace('_', ' ').capitalize() + ('1' if key == 'current' else ''), 3)
    if key == 'current':
        text = ' '.join(tree.text_content().split())
        if not all(s in text for s in ("Effective 1 January 2026", "In prior years, these amounts were included in 'Net cash flows from operating activities'")):
            raise NvoFinancialError('nvo_cash_reclassification_basis_missing')
    for i in range(2):
        v = lambda k: income[k][i]
        if not all((reconciles(v('gross_profit'), v('revenue'), v('cost')),
                reconciles(v('operating_income'), v('gross_profit'), *(v(k) for k in ('selling','research','administration','other'))),
                reconciles(v('pretax'), v('operating_income'), v('finance_income'), v('finance_expense')),
                reconciles(v('net_income'), v('pretax'), v('tax')))):
            raise NvoFinancialError('nvo_income_reconciliation')
        v = lambda k: balance[k][i]
        if not all((reconciles(v('equity'), *(v(k) for k in ('capital','treasury','retained','reserves'))),
                reconciles(v('assets'), v('current_assets'), v('noncurrent_assets')),
                reconciles(v('liabilities'), v('current_liabilities'), v('noncurrent_liabilities')),
                reconciles(v('assets'), v('equity'), v('liabilities')), v('assets') == v('total_balance'))):
            raise NvoFinancialError('nvo_balance_reconciliation')
        v = lambda k: cash_values[k][i]
        operating_parts = [v(k) for k in ('profit','tax','da_impairment','other','working','tax_paid')]
        if key == 'prior':
            operating_parts += [v('interest_received'), v('interest_paid')]
        if not all((reconciles(v('operating_cash_flow'), *operating_parts),
                reconciles(v('net_cash'), v('operating_cash_flow'), v('investing'), v('financing')),
                reconciles(v('closing'), v('opening'), v('fx'), v('net_cash')),
                v('profit') == income['net_income'][i], v('capex') <= 0, v('interest_paid') <= 0, v('interest_received') >= 0)):
            raise NvoFinancialError('nvo_cash_reconciliation')
    def fact(value, start, end, label):
        return {'value': value, 'start': start, 'end': end, 'unit': 'DKK', 'label': label,
            'source_kind': 'reviewed_sec_html_table', **proof(key)}
    facts = {k: [fact(income[k][i], f'{y}-01-01', f'{y}-06-30', INCOME[k]) for i,y in enumerate((year,year-1))]
        for k in ('revenue','gross_profit','operating_income','net_income')}
    for k in ('equity','current_assets','current_liabilities'):
        facts[k] = [fact(balance[k][i], None, end, BALANCE[k]) for i,end in enumerate((f'{year}-06-30',f'{year-1}-12-31'))]
    for k in ('operating_cash_flow','capex','interest_received','interest_paid'):
        facts[k] = [fact(cash_values[k][i] * (-1 if k in ('capex','interest_paid') else 1), f'{y}-01-01', f'{y}-06-30', k)
            for i,y in enumerate((year,year-1))]
    return facts


def parse_annual(raw, cover_raw):
    from lxml import etree
    from app.services import sec_inline_financials as inline
    tree, cover = original_tree(raw, 'annual'), original_tree(cover_raw, 'cover')
    if not any(a.get('href') == SOURCES['annual']['file'] and a.text_content().strip() == '15.1' for a in cover.xpath('//a[@href]')):
        raise NvoFinancialError('nvo_annual_exhibit_link')
    if list(tree.iter('xbrli:context')) or list(tree.iter('xbrli:unit')):
        raise NvoFinancialError('nvo_unexpected_exhibit_resources')
    for namespace in ('xmlns:ix','xmlns:xbrli','xmlns:ifrs-full','xmlns:dei','xmlns:iso4217'):
        if not tree.get(namespace) or tree.get(namespace) != cover.get(namespace):
            raise NvoFinancialError('nvo_exhibit_namespace_mismatch')
    for node in cover.iter():
        if node.tag in ('xbrli:context','xbrli:unit') or node.tag == 'ix:nonnumeric' and node.get('name','').startswith('dei:'):
            tree.append(deepcopy(node))
    s = SOURCES['annual']
    try:
        parsed = inline.parse(etree.tostring(tree, method='html'), cik=CIK, accession=s['accessionNumber'],
            filed=s['filingDate'], report_date=s['reportDate'], form='20-F', url=s['url'],
            basis={'namespace':'ifrs-full','currency':'DKK','tags':{k:(v,) for k,v in TAGS.items()}})
    except inline.InlineFactsError as exc:
        raise NvoFinancialError('nvo_annual_filing_invalid:' + str(exc)) from exc
    facts = {}
    for metric, tag in TAGS.items():
        facts[metric] = []
        for row in parsed['facts'].get(tag, {}).get('units', {}).get('DKK', []):
            point = {'value':row['val'], 'start':row.get('start'), 'end':row['end'], 'unit':'DKK', 'tag':tag,
                **parsed['provenance'][tag,'DKK',row.get('start'),row['end']], **proof('annual'),
                'cover_url':SOURCES['cover']['url'], 'cover_sha256':SOURCES['cover']['sha256']}
            facts[metric].append(point)
    return facts


def acquire(company_raw, *, symbol, cik, observed_at, client):
    validate_company(json.loads(company_raw), symbol=symbol, cik=cik, observed_at=observed_at)
    return {'nvo_originals': {k:client.get(s['url']) for k,s in SOURCES.items()}}


def project(facts_raw, company_raw, *, symbol, cik, observed_at, supplement):
    company = json.loads(company_raw)
    validate_company(company, symbol=symbol, cik=cik, observed_at=observed_at)
    if str(json.loads(facts_raw).get('cik','')).zfill(10) != CIK:
        raise NvoFinancialError('nvo_companyfacts_identity')
    if supplement and supplement.get('nvo_error'):
        raise NvoFinancialError(supplement['nvo_error'])
    originals = supplement.get('nvo_originals', {}) if supplement else {}
    if set(originals) != set(SOURCES):
        raise NvoFinancialError('nvo_source_bundle_incomplete')
    docs = {'annual':parse_annual(originals['annual'], originals['cover']),
        **{k:parse_interim(originals[k], key=k) for k in ('prior','current')}}
    # Reclassify complete annual/prior cash flows to the current presentation.
    # Preserve all three original inputs; prove against the independently
    # reported current comparative before using a trailing cash-flow ratio.
    reclassifications = []
    for key in ('annual','prior'):
        for row in docs[key]['operating_cash_flow']:
            inputs = [deepcopy(row)]
            for metric in ('interest_received','interest_paid'):
                matches = [p for p in docs[key][metric] if (p['start'],p['end']) == (row['start'],row['end'])]
                if len(matches) != 1 or matches[0]['value'] < 0:
                    raise NvoFinancialError('nvo_cash_basis_inputs')
                inputs.append(deepcopy(matches[0]))
            row['value'] = inputs[0]['value'] - inputs[1]['value'] + inputs[2]['value']
            row['reclassification'] = {'formula':'reported CFO - operating interest received + operating interest paid', 'inputs':inputs}
            reclassifications.append(deepcopy(row))
    series, unused = {}, []
    for key in ('annual','prior','current'):
        for metric in ('revenue','gross_profit','operating_income','net_income','equity','current_assets','current_liabilities','operating_cash_flow','capex'):
            for row in docs[key][metric]:
                period = (date.fromisoformat(row['start']) if row['start'] else None, date.fromisoformat(row['end']))
                old = series.setdefault(metric, {}).get(period)
                if old and old['value'] != row['value']:
                    if period != (None,date(2024,12,31)) or metric not in ('current_assets','current_liabilities'):
                        raise NvoFinancialError('nvo_consumed_comparative_conflict:' + metric)
                    unused.append({'metric':metric,'period':str(period[1]),'observations':[old,row], 'use':'not used in published ratios'})
                if old is None or row['filed'] > old['filed']:
                    series[metric][period] = row
    from app.services.sec_fundamentals import _ttm
    metrics = ('revenue','gross_profit','operating_income','net_income','operating_cash_flow','capex')
    current = {k:_ttm(series[k], END) for k in metrics}
    prior = {k:_ttm(series[k], date(2025,6,30)) for k in metrics}
    if any(p is None for group in (current,prior) for p in group.values()):
        raise NvoFinancialError('nvo_incomplete_trailing_period')
    revenue, old_revenue = current['revenue']['value'], prior['revenue']['value']
    if min(revenue, old_revenue) <= 0:
        raise NvoFinancialError('nvo_nonpositive_revenue')
    instant = {k:series[k][None,END] for k in ('equity','current_assets','current_liabilities')}
    beginning = series['equity'][None,date(2025,6,30)]
    values = {'symbol':'NVO','provider':'sec_edgar','company_name':company['name'],'fetched_at':observed_at,
        'period_date':END,'status':'ok','error':None,'revenue_growth':(revenue/old_revenue-1)*100,
        'operating_margin_expansion':(current['operating_income']['value']/revenue-prior['operating_income']['value']/old_revenue)*100}
    for name, metric in [('gross_margin','gross_profit'),('operating_margin','operating_income'),('net_margin','net_income')]:
        values[name] = current[metric]['value']/revenue*100
    if instant['current_liabilities']['value'] > 0:
        values['current_ratio'] = instant['current_assets']['value']/instant['current_liabilities']['value']
    if min(instant['equity']['value'],beginning['value']) > 0:
        values['roe'] = current['net_income']['value']/((instant['equity']['value']+beginning['value'])/2)*100
    cash = current['operating_cash_flow']['value'] - current['capex']['value']
    old_cash = prior['operating_cash_flow']['value'] - prior['capex']['value']
    values['fcf_margin'] = cash/revenue*100
    if old_cash > 0:
        values['fcf_growth'] = (cash/old_cash-1)*100
    exchanges = company.get('exchanges', [])
    values['exchange'] = exchanges[company['tickers'].index('NVO')] if len(exchanges) == len(company['tickers']) else None
    calculations = {'revenue_growth':'(TTM reported IFRS revenue / prior TTM reported IFRS revenue - 1) * 100',
        'operating_margin_expansion':'current TTM operating margin minus prior TTM operating margin in percentage points',
        'gross_margin':'TTM gross profit / TTM revenue * 100; DKK IFRS',
        'operating_margin':'TTM operating profit / TTM revenue * 100; DKK IFRS',
        'net_margin':'TTM consolidated net profit / TTM revenue * 100; DKK IFRS',
        'current_ratio':'current assets / current liabilities; DKK IFRS',
        'roe':'TTM consolidated net profit / average beginning and ending consolidated equity * 100; DKK IFRS',
        'fcf_margin':'(TTM CFO excluding interest received/paid - TTM PPE purchases) / TTM revenue * 100; DKK',
        'fcf_growth':'(current TTM FCF / positive prior TTM FCF - 1) * 100; both CFO series exclude interest received/paid'}
    evidence = {'methodology':VERSION,'cik':CIK,'reporting_currency':'DKK','accounting_basis':'IFRS',
        'available_by':str(available_day(observed_at)),'availability_basis':'after completed New York filing day',
        'company':{'sha256':hashlib.sha256(company_raw).hexdigest()},'facts':{'sha256':hashlib.sha256(facts_raw).hexdigest()},
        'period_start':current['revenue']['start'],'period_end':str(END),'current':current,'prior':prior,
        'instant':instant,'beginning_equity':beginning,'sources':[proof(k) for k in SOURCES],
        'reclassifications':reclassifications,'unused_balance_revisions':unused,
        'calculations':{k:v for k,v in calculations.items() if k in values},
        'native_cash_flow':{'currency':'DKK','value':cash,'prior_value':old_cash,'formula':'TTM CFO excluding interest received/paid minus PPE purchases'},
        'limitations':['Reported IFRS amounts include the 2026 DKK 26,760 million 340B provision reversal; no adjusted or constant-currency substitution.',
            'Cash flow is aligned to the 2026 presentation using disclosed interest reclassifications and independently reconciled comparative CFO.',
            'DKK cash amounts are evidence only; no USD free_cash_flow, currency conversion, ADR EPS or valuation inference.',
            'ROE uses consolidated net profit and fully reconciled consolidated equity; no EBITDA from combined depreciation and impairment.',
            'Prior original contains two explicitly validated duplicate-scale income cells; unused 2024 current-balance revisions remain recorded.',
            'New or amended filings require review; this adapter does not provide complete historical statement panels.']}
    values['source_evidence_json'] = json.dumps(evidence, sort_keys=True)
    return values
