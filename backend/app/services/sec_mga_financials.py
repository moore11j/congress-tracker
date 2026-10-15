"""Reviewed Magna US GAAP statements; no adjusted-income or EPS proxies."""
from datetime import date, timedelta
from decimal import Decimal
import hashlib
import json
import re
from zoneinfo import ZoneInfo
from app.services.sec_fundamentals import SecFundamentalsCoverageError

VERSION = 'sec_mga_usd_statements_v2'
CIK = '0000749098'
END = date(2026, 6, 30)
MAX_BYTES = 4_000_000
SOURCES = {'current': {'accession': '0001104659-26-088923', 'filed': '2026-07-31', 'end': '2026-06-30', 'report': '2026-06-30', 'year': 2026, 'cover': 'tm2618646d1_6k.htm', 'filename': 'tm2618646d1_ex99-2.htm', 'form': '6-K', 'sha256': '6fe6b3979c762cff19a0e1a5a5258fb36e064400040d624713b39fc00a8d85ab', 'cover_sha256': '9bbf3f5f7a65c08dc8f5a39c4a9c862a655bb369cb3cdfd3b8294683da74213b'}, 'prior': {'accession': '0001104659-25-072844', 'filed': '2025-08-01', 'end': '2025-06-30', 'report': '2025-08-01', 'year': 2025, 'cover': 'tm2519795d1_6k.htm', 'filename': 'tm2519795d1_ex99-2.htm', 'form': '6-K', 'sha256': 'c56fdd9b85ecdcb2902837577e2ac9d400307aa25505b2f42970316e3c6a42d8', 'cover_sha256': '126105640351ffb3c3484fc0c4752d39790b2d460cb83f99b25f252f834f8e5f'}, 'annual': {'accession': '0001193125-26-128771', 'filed': '2026-03-27', 'end': '2025-12-31', 'report': '2025-12-31', 'year': 2025, 'cover': 'd20215d40f.htm', 'filename': 'd20215dex993.htm', 'form': '40-F', 'sha256': 'e15ff384e3db06c25fc0c5897143e06c2397d5dd12d7b214375aa94e585cebe0', 'cover_sha256': 'c15b6db1d6b1e331481e629012d091f7344d7a7bbf93fd3ed7c3e88f67464930'}}
# Reviewed same-day companion: financial review, webcast slides and transcript.
# It is not substituted for the audited/interim statement bundle.
COMPANION = ('0001104659-26-089231', '6-K', '2026-07-31', '2026-07-31', 'tm2618646d2_6k.htm')
COMPANION_SHA256 = '2f02a7760add0e7fc9430f0f10167a17c4bc6b4d56f25b1008591996e5e98bae'
INCOME = {
    'revenue': 'Sales', 'cost': 'Cost of goods sold',
    'sga': 'Selling, general and administrative', 'depreciation': 'Depreciation',
    'amortization': 'Amortization of acquired intangible assets',
    'net_interest': 'Interest expense, net', 'equity_income_expense': 'Equity income',
    'other_expense': 'Other expense, net', 'pretax': 'Income from operations before income taxes',
    'tax': 'Income taxes', 'consolidated_income': 'Net income',
    'minority_deduction': 'Income attributable to non-controlling interests',
    'net_income': 'Net income attributable to Magna International Inc.',
}
CASH = {'cash_income': 'Net income', 'noncash': 'Items not involving current cash flows',
        'working_capital': 'Changes in operating assets and liabilities',
        'operating_cash_flow': 'Cash provided from operating activities',
        'capex_outflow': 'Fixed asset additions'}
TOTALS = ('current_assets', 'assets', 'current_liabilities', 'liabilities',
          'equity', 'total_equity', 'balance_total')

class MgaFinancialError(SecFundamentalsCoverageError):
    pass


def url_for(source, *, cover=False):
    name = source['cover'] if cover else source['filename']
    return f"https://www.sec.gov/Archives/edgar/data/749098/{source['accession'].replace('-', '')}/{name}"


def available_day(observed_at):
    if observed_at.tzinfo is None:
        raise MgaFinancialError('mga_observation_timezone')
    return observed_at.astimezone(ZoneInfo('America/New_York')).date()-timedelta(days=1)


def validate_company(company, *, symbol, cik, observed_at):
    if (symbol != 'MGA' or str(cik).zfill(10) != CIK
            or str(company.get('cik', '')).zfill(10) != CIK
            or company.get('tickers', []).count('MGA') != 1 or not company.get('name')):
        raise MgaFinancialError('mga_company_identity')
    available = available_day(observed_at)
    if not 0 <= (available-END).days <= 180:
        raise MgaFinancialError('mga_current_period_stale_or_future')
    recent = company.get('filings', {}).get('recent', {})
    arrays = [recent.get(k, []) for k in ('accessionNumber','form','filingDate','reportDate','primaryDocument')]
    if len({len(a) for a in arrays}) != 1:
        raise MgaFinancialError('mga_filing_directory_shape')
    filings = list(zip(*arrays))
    for source in SOURCES.values():
        expected = (source['accession'], source['form'], source['filed'], source['report'], source['cover'])
        if filings.count(expected) != 1 or date.fromisoformat(source['filed']) > available:
            raise MgaFinancialError('mga_reviewed_filing_not_available')
    # The prior 6-K directory reportDate is its publication date. It is not
    # used as the financial period, which is checked against statement headers.
    # Any subsequent 6-K might contain amended/new financials: review it first.
    latest = SOURCES['current']['filed']
    for acc, form, filed, end, name in filings:
        if (form in ('6-K','6-K/A','40-F','40-F/A') and filed <= str(available)
                and (filed > latest or (form.endswith('/A') and filed >= min(s['filed'] for s in SOURCES.values()))
                     or (filed == latest and acc != SOURCES['current']['accession']
                         and (acc, form, filed, end, name) != COMPANION))):
            raise MgaFinancialError('mga_newer_or_amended_report_requires_review')


def _amounts(cells, count):
    text = ' '.join(cells).replace('$', '').strip()
    pattern = r'\(\s*[0-9,]+\s*\)|[0-9,]+'
    tokens = re.findall(pattern, text)
    if re.sub(pattern, '', text).strip() or len(tokens) != count:
        raise MgaFinancialError('mga_numeric_column_shape')
    result = []
    for token in tokens:
        clean = token.replace(' ', '')
        if not re.fullmatch(r'\(?[0-9]{1,3}(?:,[0-9]{3})*\)?|\(?[0-9]+\)?', clean):
            raise MgaFinancialError('mga_numeric_format')
        if clean.startswith('(') != clean.endswith(')'):
            raise MgaFinancialError('mga_numeric_sign')
        value = int(Decimal(clean.replace(',', '').replace('(', '-').replace(')', '')))*1_000_000
        if abs(value) > 10**15:
            raise MgaFinancialError('mga_numeric_bound')
        result.append(value)
    return result


def parse(raw, *, source_key):
    source = SOURCES[source_key]
    if not isinstance(raw, bytes) or not raw or len(raw) > MAX_BYTES:
        raise MgaFinancialError('mga_document_size')
    digest = hashlib.sha256(raw).hexdigest()
    if digest != source['sha256']:
        raise MgaFinancialError('mga_reviewed_original_changed')
    from lxml import html
    try:
        tree = html.fromstring(raw)
    except (ValueError, html.etree.ParserError) as exc:
        raise MgaFinancialError('mga_invalid_html') from exc
    candidates = {k: [] for k in ('income','cash','balance')}
    for table in tree.xpath('//table[not(.//table)]'):
        rows = [[' '.join(c.text_content().split()) for c in row.xpath('./td|./th')]
                for row in table.xpath('.//tr')]
        rows = [r for r in rows if any(r)]
        labels = {r[0] for r in rows}
        for kind, markers in [('income', {'Sales', INCOME['net_income']}),
                ('cash', {'Fixed asset additions','OPERATING ACTIVITIES'}),
                ('balance', {'Retained earnings','Inventories'})]:
            if markers <= labels:
                prefix = ' '.join(' '.join(table.xpath('preceding::text()')).split())
                candidates[kind].append((rows, prefix))
    if any(len(v) != 1 for v in candidates.values()):
        raise MgaFinancialError('mga_statement_missing_or_ambiguous')
    annual = source_key == 'annual';year = source['year'];count = 2 if annual else 4
    titles = {'income': 'CONSOLIDATED STATEMENTS OF INCOME',
              'cash': 'CONSOLIDATED STATEMENTS OF CASH FLOWS', 'balance': 'CONSOLIDATED BALANCE SHEETS'}
    extracted = {};balances = {}
    for kind, matches in candidates.items():
        rows, prefix = matches[0]
        unit = ('U.S. dollars in millions, except per share figures' if kind == 'income' else
                'U.S. dollars in millions, except shares issued' if annual and kind == 'balance' else 'U.S. dollars in millions')
        title = 'MAGNA INTERNATIONAL INC. '+titles[kind]+('' if annual else ' [Unaudited]')+' ['+unit+']'
        if annual: title += ' As at December 31,' if kind == 'balance' else ' Years ended December 31,'
        if not prefix.endswith(title):
            raise MgaFinancialError('mga_statement_identity_or_units')
        n = 2 if kind == 'balance' else count
        years = [str(year), str(year-1)]*(1 if n == 2 else 2)
        headers = [i for i,r in enumerate(rows) if [c for c in r if c] == ['Note']+years]
        if len(headers) != 1:
            raise MgaFinancialError('mga_period_columns')
        h = headers[0];start = rows[h].index(str(year))
        preceding = [[c for c in r if c] for r in rows[:h]]
        expected = [] if annual else ([['As at','As at'],['June 30,','December 31,']] if kind == 'balance' else
            [['Three months ended','Six months ended'],['June 30,','June 30,']])
        if not annual and kind == 'cash' and source_key == 'prior':
            expected = [['Three months ended June 30,','Six months ended June 30,']]
        if preceding != expected:
            raise MgaFinancialError('mga_duration_headers')
        def selected(label):
            found = [r for r in rows[h+1:] if r[0] == label]
            if len(found) != 1:
                raise MgaFinancialError('mga_row_missing_or_duplicate:'+label)
            return _amounts(found[0][start:], n)
        if kind != 'balance':
            extracted.update({metric: selected(label) for metric,label in (INCOME if kind == 'income' else CASH).items()})
        else:
            totals = [r for r in rows[h+1:] if not r[0] and any(r[start:])]
            if len(totals) != len(TOTALS):
                raise MgaFinancialError('mga_balance_total_shape')
            balances = {key:_amounts(row[start:],2) for key,row in zip(TOTALS,totals)}
            components = [selected(label) for label in ('Contributed surplus','Retained earnings','Accumulated other comprehensive loss')]
            common = [r for r in rows[h+1:] if (r[0].startswith('Common Shares') or r[0].startswith('[issued:')) and any(r[start:])]
            if len(common) != 1:
                raise MgaFinancialError('mga_parent_equity_components')
            components.append(_amounts(common[0][start:],2))
            minority = selected('Non-controlling interests')
            for i in range(2):
                v = lambda k: balances[k][i]
                if (sum(c[i] for c in components) != v('equity')
                        or v('equity')+minority[i] != v('total_equity')
                        or v('liabilities')+v('total_equity') != v('assets')
                        or v('assets') != v('balance_total')
                        or not 0 <= v('current_assets') <= v('assets')
                        or not 0 <= v('current_liabilities') <= v('liabilities')):
                    raise MgaFinancialError('mga_balance_reconciliation')
    for i in range(count):
        v = lambda k: extracted[k][i]
        costs = sum(v(k) for k in ('cost','sga','depreciation','amortization','net_interest','equity_income_expense','other_expense'))
        if (v('revenue')-costs != v('pretax') or v('pretax')-v('tax') != v('consolidated_income')
                or v('consolidated_income')+v('minority_deduction') != v('net_income')
                or v('cash_income') != v('consolidated_income')
                or v('cash_income')+v('noncash')+v('working_capital') != v('operating_cash_flow')
                or v('capex_outflow') > 0):
            raise MgaFinancialError('mga_income_or_cash_reconciliation')
    periods = ([(f'{y}-01-01',f'{y}-12-31') for y in (year,year-1)] if annual else
               [(f'{y}-{m}-01',f'{y}-06-30') for m in ('04','01') for y in (year,year-1)])
    instants = [f'{year}-12-31',f'{year-1}-12-31'] if annual else [f'{year}-06-30',f'{year-1}-12-31']
    def fact(value,start,end,label):
        return {'value':value,'start':start,'end':end,'unit':'USD','filed':source['filed'],
                'accession':source['accession'],'source_url':url_for(source),'source_sha256':digest,
                'source_kind':'reviewed_sec_html_table','label':label}
    facts = {k:[fact(v,*periods[i],(INCOME|CASH)[k]) for i,v in enumerate(values)] for k,values in extracted.items()}
    facts.update({k:[fact(v,None,instants[i],k) for i,v in enumerate(values)] for k,values in balances.items()})
    return {'facts':facts,'evidence':{'url':url_for(source),'sha256':digest,'accession':source['accession'],
        'filed':source['filed'],'cover_url':url_for(source,cover=True),'cover_sha256':source['cover_sha256'],
        'accounting_basis':'US GAAP','currency':'USD','scale':1_000_000}}


def acquire(company_raw, *, symbol, cik, observed_at, client):
    validate_company(json.loads(company_raw),symbol=symbol,cik=cik,observed_at=observed_at)
    # Raw originals exist only in this bounded in-process handoff. The projector
    # verifies hashes and parses them; saved evidence contains values/provenance.
    return {'mga_originals':{key:client.get(url_for(source)) for key,source in SOURCES.items()}}


def project(facts_raw, company_raw, *, symbol, cik, observed_at, supplement):
    company = json.loads(company_raw)
    validate_company(company,symbol=symbol,cik=cik,observed_at=observed_at)
    if str(json.loads(facts_raw).get('cik','')).zfill(10) != CIK:
        raise MgaFinancialError('mga_companyfacts_identity')
    if supplement and supplement.get('mga_error'):
        raise MgaFinancialError(supplement['mga_error'])
    originals = supplement.get('mga_originals',{}) if supplement else {}
    if set(originals) != set(SOURCES):
        raise MgaFinancialError('mga_incomplete_source_bundle')
    series = {};proofs = []
    for key,raw in originals.items():
        document = parse(raw,source_key=key);proofs.append(document['evidence'])
        for metric,rows in document['facts'].items():
            for row in rows:
                period = (date.fromisoformat(row['start']) if row['start'] else None,date.fromisoformat(row['end']))
                previous = series.setdefault(metric,{}).get(period)
                if previous and previous['value'] != row['value']:
                    raise MgaFinancialError('mga_comparative_restatement_requires_review')
                if previous is None or row['filed'] > previous['filed']:
                    series[metric][period] = row
    from app.services.sec_fundamentals import _ttm
    metrics = ('revenue','net_income','consolidated_income','operating_cash_flow','capex_outflow')
    current = {k:_ttm(series[k],END) for k in metrics}
    prior = {k:_ttm(series[k],date(2025,6,30)) for k in metrics}
    if any(v is None for group in (current,prior) for v in group.values()):
        raise MgaFinancialError('mga_incomplete_trailing_period')
    revenue = current['revenue']['value'];old_revenue = prior['revenue']['value']
    if min(revenue,old_revenue) <= 0:
        raise MgaFinancialError('mga_nonpositive_revenue')
    instants = {k:series[k][None,END] for k in ('equity','current_assets','current_liabilities')}
    beginning = series['equity'][None,date(2025,6,30)]
    fcf = current['operating_cash_flow']['value']+current['capex_outflow']['value']
    previous_fcf = prior['operating_cash_flow']['value']+prior['capex_outflow']['value']
    values = {'symbol':'MGA','provider':'sec_edgar','company_name':company['name'],
        'fetched_at':observed_at,'period_date':END,'status':'ok','error':None,
        'revenue_growth':(revenue/old_revenue-1)*100,'net_margin':current['net_income']['value']/revenue*100,
        'free_cash_flow':fcf,'fcf_margin':fcf/revenue*100}
    exchanges = company.get('exchanges',[])
    values['exchange'] = exchanges[company['tickers'].index('MGA')] if len(exchanges)==len(company['tickers']) else None
    if previous_fcf > 0:
        values['fcf_growth'] = (fcf/previous_fcf-1)*100
    if instants['current_liabilities']['value'] > 0:
        values['current_ratio'] = instants['current_assets']['value']/instants['current_liabilities']['value']
    if min(instants['equity']['value'],beginning['value']) > 0:
        values['roe'] = current['net_income']['value']/((instants['equity']['value']+beginning['value'])/2)*100
    evidence = {'methodology':VERSION,'cik':CIK,'reporting_currency':'USD','accounting_basis':'US GAAP',
        'available_by':str(available_day(observed_at)),
        'availability_basis':'filed date, available after completed New York filing day',
        'company':{'sha256':hashlib.sha256(company_raw).hexdigest()},'facts':{'sha256':hashlib.sha256(facts_raw).hexdigest()},
        'period_start':current['revenue']['start'],'period_end':str(END),'current':current,'prior':prior,
        'instant':instants,'beginning_equity':beginning,'sources':proofs,
        'calculations':{'revenue_growth':'(TTM revenue / prior TTM revenue - 1) * 100',
            'net_margin':'TTM parent net income / revenue * 100',
            'roe':'TTM parent net income / average beginning and ending parent equity * 100',
            'current_ratio':'current assets / current liabilities',
            'free_cash_flow':'TTM operating cash flow less fixed asset additions; USD',
            'fcf_margin':'TTM free cash flow / revenue * 100',
            'fcf_growth':'(TTM free cash flow / prior positive TTM free cash flow - 1) * 100'},
        'limitations':['Reviewed current, prior interim and annual US GAAP reports only; new filings require review.',
            'Pretax income includes net interest and equity income; it is not operating income.',
            'No operating/gross margin, adjusted EBIT, EBITDA, debt ratio or reconstructed EPS.',
            'Free cash flow deducts fixed asset additions only; separate mixed investment/intangible purchases are excluded.',
            'Net margin and ROE use parent earnings and parent equity; consolidated earnings are retained separately.']}
    values['source_evidence_json'] = json.dumps(evidence,sort_keys=True)
    return values
