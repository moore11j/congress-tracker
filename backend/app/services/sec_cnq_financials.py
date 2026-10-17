"""Reviewed CNQ CAD IFRS ratios, with incompatible cash/leverage fields held."""
from datetime import date, timedelta
import hashlib
import json
import re
from zoneinfo import ZoneInfo
from app.services.sec_fundamentals import SecFundamentalsCoverageError
from app.services.sec_original_integrity import verified_original

VERSION = 'sec_cnq_cad_statements_v2'
CIK = '0001017413'
END = date(2026, 6, 30)
MAX_BYTES = 10_000_000
SOURCES = {'current': {'accession': '0001017413-26-000043', 'form': '6-K', 'filed': '2026-08-06', 'end': '2026-06-30', 'cover': 'a063020266-kcover.htm', 'url': 'https://www.sec.gov/Archives/edgar/data/1017413/000101741326000043/a06302026q2fs.htm', 'sha256': '8a212e3907e172c591a44d28a815b6d3f855788bb94ad009a568d86c11a66aba', 'cover_sha256': '8e687ac63512c02b41ac83591a5b19b6d4ef0fa60558eea03d26d7f71559a5e6', 'document_sha256': '21b1bf8c938e36fa234b0a11b1ba298b2080d7ce0a899126ecac9ec22df3ddf6', 'document_integrity_policy': 'sec_terminal_delivery_script_path_v1'}, 'annual': {'accession': '0001017413-26-000018', 'form': '40-F', 'filed': '2026-03-26', 'end': '2025-12-31', 'cover': 'cnq-20251231.htm', 'url': 'https://www.sec.gov/Archives/edgar/data/1017413/000101741326000018/cnq-20251231.htm', 'sha256': '506ecaff6f6018648b34ece6ed34413bbaac4f27f1b32bf7c85ec2e541a1e322', 'cover_sha256': '506ecaff6f6018648b34ece6ed34413bbaac4f27f1b32bf7c85ec2e541a1e322', 'document_sha256': 'c66a78e29809dd1d9d6446c68cbe959e88638ffeed4ae115568613a9c2992c5d', 'document_integrity_policy': 'sec_terminal_delivery_script_path_v1'}, 'prior': {'accession': '0001017413-25-000043', 'form': '6-K', 'filed': '2025-08-07', 'end': '2025-06-30', 'cover': 'a063020256-kcover.htm', 'url': 'https://www.sec.gov/Archives/edgar/data/1017413/000101741325000043/a06302025q2fs.htm', 'sha256': '53c328d16d5ca0337cc3ea3328e02a5ab2dfaa5eca1091f255d5ab7e1cf2c0c8', 'cover_sha256': 'a42d54e38e24d0dca3e1f6afcda12257cb76cc1bec3733ac01833c4546579bdd', 'document_sha256': '3f9d18f22fa3a1bcab63a69d74f5233e65edfcce54eff3afaaa01e8eae9c1bb7', 'document_integrity_policy': 'sec_terminal_delivery_script_path_v1'}}
REVIEWED_DIVIDEND = {'form': '6-K', 'filingDate': '2026-08-06', 'reportDate': '2026-08-06', 'accessionNumber': '0001017413-26-000045', 'primaryDocument': 'a08062026dividendcover.htm', 'url': 'https://www.sec.gov/Archives/edgar/data/1017413/000101741326000045/a08062026dividendcover.htm', 'sha256': '582402ababab6fc1dd3b732cf3fb74ae05ebf3138bfa99789c82b94335cbc5fc'}
TAGS = {'revenue': ('Revenue',), 'net_income': ('ProfitLoss',),
        'equity': ('Equity',), 'current_assets': ('CurrentAssets',),
        'current_liabilities': ('CurrentLiabilities',)}
ASSET_COMPONENTS = ('Cash and cash equivalents', 'Accounts receivable', 'Inventory',
                    'Prepaids and other', 'Current portion of other long-term assets')
LIABILITY_COMPONENTS = ('Accounts payable', 'Accrued liabilities', 'Current income taxes payable',
                        'Current portion of long-term debt', 'Current portion of other long-term liabilities')
EQUITY_COMPONENTS = ('Share capital', 'Retained earnings', 'Accumulated other comprehensive income')


class CnqFinancialError(SecFundamentalsCoverageError):
    pass


def available_day(observed_at):
    if observed_at.tzinfo is None:
        raise CnqFinancialError('cnq_observation_timezone')
    return observed_at.astimezone(ZoneInfo('America/New_York')).date() - timedelta(days=1)


def validate_company(company, *, symbol, cik, observed_at):
    if (symbol != 'CNQ' or str(cik).zfill(10) != CIK or str(company.get('cik', '')).zfill(10) != CIK
            or company.get('tickers', []).count('CNQ') != 1 or not company.get('name')):
        raise CnqFinancialError('cnq_company_identity')
    available = available_day(observed_at)
    if not 0 <= (available - END).days <= 180:
        raise CnqFinancialError('cnq_current_period_stale_or_future')
    recent = company.get('filings', {}).get('recent', {})
    fields = ('accessionNumber', 'form', 'filingDate', 'reportDate', 'primaryDocument')
    arrays = [recent.get(k, []) for k in fields]
    if any(not isinstance(a, list) for a in arrays) or len({len(a) for a in arrays}) != 1:
        raise CnqFinancialError('cnq_filing_directory_shape')
    filings = list(zip(*arrays))
    reviewed = {(s['accession'], s['form'], s['filed'], s['end'], s['cover']) for s in SOURCES.values()}
    if any(filings.count(row) != 1 or date.fromisoformat(row[2]) > available for row in reviewed):
        raise CnqFinancialError('cnq_reviewed_filing_not_available')
    dividend = tuple(REVIEWED_DIVIDEND[k] for k in fields)
    for row in filings:
        acc, form, filed, report, name = row
        if form not in ('6-K', '6-K/A', '40-F', '40-F/A') or filed > str(available):
            continue
        if row in reviewed or row == dividend:
            continue
        if filed >= SOURCES['current']['filed'] or (form.endswith('/A') and filed >= min(s['filed'] for s in SOURCES.values())):
            raise CnqFinancialError('cnq_unreviewed_new_or_amended_report')


def amount(text):
    if not isinstance(text, str) or not re.fullmatch(r'(?:[0-9]+|[0-9]{1,3}(?:,[0-9]{3})+|\((?:[0-9]+|[0-9]{1,3}(?:,[0-9]{3})+)\))', text):
        raise CnqFinancialError('cnq_numeric_format')
    value = int(text.strip('()').replace(',', '')) * (-1 if text.startswith('(') else 1) * 1_000_000
    if abs(value) > 10**15:
        raise CnqFinancialError('cnq_numeric_bound')
    return value


def parse(raw, *, source_key):
    s = SOURCES[source_key]
    if not isinstance(raw, bytes) or not raw or len(raw) > MAX_BYTES:
        raise CnqFinancialError('cnq_document_size')
    try: receipt = verified_original(raw, s)
    except ValueError as exc: raise CnqFinancialError('cnq_reviewed_original_changed') from exc
    digest = receipt['source_sha256']
    proof = {**receipt, 'url': s['url'], 'sha256': digest, 'accession': s['accession'], 'filed': s['filed'],
             'cover_sha256': s['cover_sha256'], 'currency': 'CAD', 'accounting_basis': 'IFRS'}
    if source_key == 'annual':
        from app.services import sec_inline_financials as inline
        result = inline.parse(raw, cik=CIK, accession=s['accession'], filed=s['filed'],
            report_date=s['end'], form=s['form'], url=s['url'],
            basis={'namespace': 'ifrs-full', 'currency': 'CAD', 'tags': TAGS})
        facts = {k: [] for k in TAGS}
        for k, tags in TAGS.items():
            for tag in tags:
                for row in result['facts'].get(tag, {}).get('units', {}).get('CAD', []):
                    evidence = result['provenance'][tag, 'CAD', row.get('start'), row['end']]
                    facts[k].append({'value': row['val'], 'start': row.get('start'), 'end': row['end'],
                        'unit': 'CAD', 'filed': row['filed'], 'accession': row['accn'], **evidence})
        return {'facts': facts, 'evidence': proof}
    from lxml import html
    try:
        tree = html.fromstring(raw)
    except (ValueError, html.etree.ParserError) as exc:
        raise CnqFinancialError('cnq_invalid_html') from exc
    tables = {}
    for table in tree.xpath('//table[not(.//table)]'):
        prefix = ' '.join(' '.join(table.xpath('preceding::text()')).split())
        for kind, title in [('income', 'CONSOLIDATED STATEMENTS OF EARNINGS'), ('balance', 'CONSOLIDATED BALANCE SHEETS')]:
            if prefix.endswith(title):
                if kind in tables:
                    raise CnqFinancialError('cnq_ambiguous_statement')
                rows = [[' '.join(c.text_content().split()) for c in row.xpath('./td|./th')] for row in table.xpath('.//tr')]
                tables[kind] = [(r[0], r[1] if len(r) > 1 else '', [v for v in r[2:] if v and v != '$']) for r in rows if any(r)]
    if set(tables) != {'income', 'balance'}:
        raise CnqFinancialError('cnq_missing_statement')
    income, balance = tables['income'], tables['balance']
    year = int(s['end'][:4])
    income_unit = '(millions of Canadian dollars, except per common share amounts, unaudited)'
    dates = [f'Jun 30{year}', f'Jun 30{year-1}'] * 2
    if (sum(r[0] == income_unit for r in income) != 1 or sum(r[2] == dates for r in income) != 1
            or sum(r[2] == ['Three Months Ended', 'Six Months Ended'] for r in income) != 1
            or balance[0] != ('As at', 'Note', [f'Jun 30{year}', f'Dec 31{year-1}'])
            or sum(r[0] == '(millions of Canadian dollars, unaudited)' for r in balance) != 1):
        raise CnqFinancialError('cnq_statement_columns_or_currency')
    def row_values(rows, label, count):
        found = [r for r in rows if r[0] == label]
        if len(found) != 1 or len(found[0][2]) != count:
            raise CnqFinancialError('cnq_missing_or_duplicate_row:' + label)
        return [amount(v) for v in found[0][2]]
    def subtotal_after(label):
        indexes = [i for i, r in enumerate(balance) if r[0] == label]
        if len(indexes) != 1 or indexes[0] + 1 >= len(balance):
            raise CnqFinancialError('cnq_missing_subtotal')
        row = balance[indexes[0] + 1]
        if row[0] or row[1] or len(row[2]) != 2:
            raise CnqFinancialError('cnq_subtotal_structure')
        return [amount(v) for v in row[2]]
    labels = ['Product sales', 'Less: royalties', 'Revenue', 'Earnings before taxes', 'Current income tax expense',
              'Deferred income tax expense (recovery)' if year == 2026 else 'Deferred income tax (recovery) expense', 'Net earnings']
    inc = {label: row_values(income, label, 4) for label in labels}
    for i in range(4):
        if (inc['Product sales'][i] + inc['Less: royalties'][i] != inc['Revenue'][i]
                or inc['Earnings before taxes'][i] - inc['Current income tax expense'][i] - inc[labels[5]][i] != inc['Net earnings'][i]):
            raise CnqFinancialError('cnq_income_reconciliation')
    assets = subtotal_after(ASSET_COMPONENTS[-1]); liabilities = subtotal_after(LIABILITY_COMPONENTS[-1]); equity = subtotal_after(EQUITY_COMPONENTS[-1])
    total_assets = subtotal_after('Other long-term assets'); total_liabilities = subtotal_after('Deferred income taxes')
    components = {label: row_values(balance, label, 2) for label in (*ASSET_COMPONENTS, *LIABILITY_COMPONENTS, *EQUITY_COMPONENTS,
        'Exploration and evaluation assets', 'Property, plant and equipment', 'Lease assets', 'Other long-term assets',
        'Long-term debt', 'Other long-term liabilities', 'Deferred income taxes')}
    last = balance[-1]
    if last[0] or last[1] or len(last[2]) != 2 or [amount(v) for v in last[2]] != total_assets:
        raise CnqFinancialError('cnq_balance_total')
    for i in range(2):
        def sum_rows(labels):
            return sum(components[k][i] for k in labels)
        if (assets[i] != sum_rows(ASSET_COMPONENTS) or liabilities[i] != sum_rows(LIABILITY_COMPONENTS)
                or equity[i] != sum_rows(EQUITY_COMPONENTS)
                or total_assets[i] != assets[i] + sum_rows(('Exploration and evaluation assets','Property, plant and equipment','Lease assets','Other long-term assets'))
                or total_liabilities[i] != liabilities[i] + sum_rows(('Long-term debt','Other long-term liabilities','Deferred income taxes'))
                or total_assets[i] != total_liabilities[i] + equity[i]):
            raise CnqFinancialError('cnq_balance_reconciliation')
    def fact(value, start, end, label):
        return {'value': value, 'start': start, 'end': end, 'unit': 'CAD', 'filed': s['filed'],
            'accession': s['accession'], 'source_url': s['url'], 'source_sha256': digest,
            'source_kind': 'reviewed_sec_html_table', 'label': label}
    facts = {k: [fact(inc[label][i+2], f'{y}-01-01', f'{y}-06-30', label) for i, y in enumerate((year, year-1))]
             for k, label in [('revenue','Revenue'), ('net_income','Net earnings')]}
    for k, vals, label in [('equity', equity, EQUITY_COMPONENTS[-1]), ('current_assets', assets, ASSET_COMPONENTS[-1]), ('current_liabilities', liabilities, LIABILITY_COMPONENTS[-1])]:
        facts[k] = [fact(vals[i], None, end, 'Reconciled reported subtotal after '+label) for i,end in enumerate((f'{year}-06-30',f'{year-1}-12-31'))]
    return {'facts': facts, 'evidence': proof}


def acquire(company_raw, *, symbol, cik, observed_at, client):
    validate_company(json.loads(company_raw), symbol=symbol, cik=cik, observed_at=observed_at)
    return {'cnq_originals': {key: client.get(s['url']) for key,s in SOURCES.items()}}


def project(facts_raw, company_raw, *, symbol, cik, observed_at, supplement):
    company = json.loads(company_raw)
    validate_company(company, symbol=symbol, cik=cik, observed_at=observed_at)
    if str(json.loads(facts_raw).get('cik', '')).zfill(10) != CIK:
        raise CnqFinancialError('cnq_companyfacts_identity')
    if supplement and supplement.get('cnq_error'):
        raise CnqFinancialError(supplement['cnq_error'])
    originals = supplement.get('cnq_originals', {}) if supplement else {}
    if set(originals) != set(SOURCES):
        raise CnqFinancialError('cnq_incomplete_source_bundle')
    series = {}; proofs = []
    for key, raw in originals.items():
        doc = parse(raw, source_key=key); proofs.append(doc['evidence'])
        for metric, rows in doc['facts'].items():
            for row in rows:
                period = (date.fromisoformat(row['start']) if row['start'] else None, date.fromisoformat(row['end']))
                previous = series.setdefault(metric, {}).get(period)
                if previous and previous['value'] != row['value']:
                    raise CnqFinancialError('cnq_comparative_conflict')
                if previous is None or row['filed'] > previous['filed']:
                    series[metric][period] = row
    from app.services.sec_fundamentals import _ttm
    current = {k: _ttm(series[k], END) for k in ('revenue','net_income')}
    prior = _ttm(series['revenue'], date(2025,6,30))
    if prior is None or any(v is None for v in current.values()):
        raise CnqFinancialError('cnq_incomplete_trailing_period')
    instant = {k: series[k][None,END] for k in ('equity','current_assets','current_liabilities')}
    beginning = series['equity'][None,date(2025,6,30)]
    if min(current['revenue']['value'], prior['value'], instant['current_liabilities']['value'], instant['equity']['value'], beginning['value']) <= 0:
        raise CnqFinancialError('cnq_nonpositive_denominator')
    values = {'symbol': 'CNQ', 'provider': 'sec_edgar', 'company_name': company['name'], 'fetched_at': observed_at,
        'period_date': END, 'status': 'ok', 'error': None,
        'revenue_growth': (current['revenue']['value']/prior['value']-1)*100,
        'net_margin': current['net_income']['value']/current['revenue']['value']*100,
        'roe': current['net_income']['value']/((instant['equity']['value']+beginning['value'])/2)*100,
        'current_ratio': instant['current_assets']['value']/instant['current_liabilities']['value']}
    exchanges = company.get('exchanges', [])
    values['exchange'] = exchanges[company['tickers'].index('CNQ')] if len(exchanges) == len(company['tickers']) else None
    evidence = {'methodology': VERSION, 'cik': CIK, 'reporting_currency': 'CAD', 'accounting_basis': 'IFRS',
        'available_by': str(available_day(observed_at)), 'availability_basis': 'completed New York filing day',
        'facts': {'sha256': hashlib.sha256(facts_raw).hexdigest()}, 'company': {'sha256': hashlib.sha256(company_raw).hexdigest()},
        'period_start': current['revenue']['start'], 'period_end': str(END), 'current': current, 'prior_revenue': prior,
        'instant': instant, 'beginning_equity': beginning, 'sources': proofs, 'reviewed_nonfinancial_filing': REVIEWED_DIVIDEND,
        'calculations': {'revenue_growth': '(TTM net revenue / prior TTM net revenue - 1) * 100',
            'net_margin': 'TTM net earnings / TTM revenue after royalties * 100',
            'roe': 'TTM net earnings / average beginning and ending shareholders equity * 100',
            'current_ratio': 'reported current assets / reported current liabilities'},
        'limitations': ['CAD IFRS ratios only; no FX conversion, USD cash values or valuation inference.',
            'Revenue is reported after royalties, not gross product sales.',
            'Operating income and gross profit are not inferred from heterogeneous expense lines.',
            'Net PPE expenditures are not gross PPE cash purchases; no canonical FCF proxy.',
            '2026 IFRS 9 adoption changes opening cash without restating comparative cash flows.',
            'Combined financing expense is not gross interest; IFRS lease/depletion/impairment compatibility remains unverified.',
            'No EBITDA, leverage, EPS or complete confirmation-classification claim.',
            'New or amended filings require review before further preparation.']}
    values['source_evidence_json'] = json.dumps(evidence, sort_keys=True)
    return values
