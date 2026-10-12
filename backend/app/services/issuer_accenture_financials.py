"""Reviewed Accenture GAAP earnings tables, before the annual SEC report arrives."""
import hashlib
import re
from zoneinfo import ZoneInfo

from app.services.issuer_release_financials import IssuerFinancialError, _cells, _numbers

URL = 'https://www.sec.gov/Archives/edgar/data/1467373/000146737326000037/q4fy26earnings8-kexhibit.htm'
VERSION = 'acn_annual_release_v1'


def prepare_accenture_annual(raw, *, source_url, observed_at):
    if source_url != URL or not raw or len(raw) > 3_000_000 or observed_at.tzinfo is None:
        raise IssuerFinancialError('Unreviewed Accenture source or observation')
    if observed_at.astimezone(ZoneInfo('America/New_York')).date().isoformat() <= '2026-10-01':
        raise IssuerFinancialError('Publication day not completed')
    from lxml import html
    try:
        tree = html.fromstring(raw.decode('utf-8'))
    except (UnicodeError, ValueError, html.etree.ParserError) as exc:
        raise IssuerFinancialError('Invalid release HTML') from exc
    for node in tree.xpath('//script|//style'):
        node.drop_tree()
    text = ' '.join(tree.text_content().split())
    if not re.search(r'NEW YORK; October 1, 2026 .{1,3} Accenture \(NYSE: ACN\)', text):
        raise IssuerFinancialError('Issuer identity or publication date mismatch')
    tables = []
    for table in tree.xpath('//table[not(.//table)]'):
        rows = [[c for c in _cells(row) if c] for row in table.xpath('.//tr')]
        # Bind units and statement title to this table, not a different page.
        prefix = ' '.join(' '.join(table.xpath('preceding::text()')).split())
        tables.append((rows, prefix))

    def statement(marker, caption, annual=True):
        matches = [(rows,prefix) for rows,prefix in tables if any(r and r[0] == marker for r in rows)]
        if len(matches) != 1:
            raise IssuerFinancialError('Missing or ambiguous statement')
        rows, prefix = matches[0]
        if not prefix.endswith(caption):
            raise IssuerFinancialError('Statement title or units mismatch')
        nonempty = [r for r in rows if r]
        if annual:
            if nonempty[0] != ['Three Months Ended', 'Year Ended']:
                raise IssuerFinancialError('Annual period heading mismatch')
            header = [x.replace(' ', '') for x in nonempty[1] if x != '% of Revenues']
            if header != ['August31,2026','August31,2025'] * 2:
                raise IssuerFinancialError('Annual period order mismatch')
        elif nonempty[0] != ['August 31, 2026','August 31, 2025']:
            raise IssuerFinancialError('Balance sheet period mismatch')
        return rows

    income = statement('Cost of services', 'Accenture plc Consolidated Income Statements '
        '(In thousands of U.S. dollars, except share and per share amounts) (Unaudited)')
    cash = statement('CASH FLOWS FROM OPERATING ACTIVITIES:', 'Accenture plc Consolidated Cash Flows Statements '
        '(In thousands of U.S. dollars) (Unaudited)')
    balance = statement('Total current assets', 'Accenture plc Consolidated Balance Sheets '
        '(In thousands of U.S. dollars)', annual=False)
    selected = {}

    def pair(rows, label, *, percents=False, annual=True, eps=False):
        matches = [r[1:] for r in rows if r and r[0] == label]
        if len(matches) != 1:
            raise IssuerFinancialError('Missing or duplicate financial row: '+label)
        cells = matches[0]
        if cells.count('%') != (4 if percents else 0):
            raise IssuerFinancialError('Financial column layout mismatch')
        values = _numbers([c for c in cells if c != '%'])
        expected = 8 if percents else 4 if annual else 2
        if len(values) != expected:
            raise IssuerFinancialError('Missing or ambiguous financial columns')
        values = values[::2] if percents else values
        return [v * (1 if eps else 1000) for v in values[-2:]]

    for key,label,pct in [
        ('revenue','Revenues',True), ('cost_of_sales','Cost of services',True),
        ('operating_expenses','Total operating expenses',False),
        ('operating_income','OPERATING INCOME',True),
        ('interest_income','Interest income',False), ('interest_expense_signed','Interest expense',False),
        ('other_income','Other income (expense), net',False),
        ('pretax_income','INCOME BEFORE INCOME TAXES',True), ('tax','Income tax expense',False),
        ('consolidated_net_income','NET INCOME',True),
        ('net_income','NET INCOME ATTRIBUTABLE TO ACCENTURE PLC',True),
        ('canada_nci_signed','Net income attributable to noncontrolling interest in Accenture Canada Holdings Inc.',False)]:
        selected[key] = pair(income,label,percents=pct)
    other_labels = [r[0] for r in income if r and r[0].startswith('Net income attributable to noncontrolling interests ')
                    and r[0].endswith('other (1)')]
    if len(other_labels) != 1:
        raise IssuerFinancialError('Noncontrolling interest identity mismatch')
    selected['other_nci_signed'] = pair(income,other_labels[0])
    markers = [i for i,r in enumerate(income) if r == ['EARNINGS PER SHARE:']]
    if len(markers) != 1:
        raise IssuerFinancialError('EPS section mismatch')
    for key,label in [('basic_eps','Basic'),('diluted_eps','Diluted')]:
        selected[key] = pair(income[markers[0]+1:],label,eps=True)
    for key,label in [('operating_cash_flow','Net cash provided by (used in) operating activities'),
        ('capex_signed','Purchases of property and equipment'),
        ('investing_cash_flow','Net cash provided by (used in) investing activities'),
        ('financing_cash_flow','Net cash provided by (used in) financing activities'),
        ('cash_fx','Effect of exchange rate changes on cash and cash equivalents'),
        ('cash_change','NET INCREASE (DECREASE) IN CASH AND CASH EQUIVALENTS'),
        ('cash_start','CASH AND CASH EQUIVALENTS, beginning of period'),
        ('cash_end','CASH AND CASH EQUIVALENTS, end of period')]:
        selected[key] = pair(cash,label)
    for key,label in [('current_assets','Total current assets'),('current_liabilities','Total current liabilities'),
                      ('balance_cash','Cash and cash equivalents')]:
        selected[key] = pair(balance,label,annual=False)
    selected['gross_profit'] = [r-c for r,c in zip(selected['revenue'],selected['cost_of_sales'])]
    selected['free_cash_flow'] = [o+c for o,c in zip(selected['operating_cash_flow'],selected['capex_signed'])]
    for i in (0,1):
        def v(key): return selected[key][i]
        if (v('revenue') <= 0 or v('current_liabilities') <= 0 or v('current_assets') < 0
                or v('cost_of_sales') < 0 or v('capex_signed') > 0):
            raise IssuerFinancialError('Unsupported financial inputs')
        checks = [(v('revenue')-v('operating_expenses'),v('operating_income')),
            (v('operating_income')+v('interest_income')+v('interest_expense_signed')+v('other_income'),v('pretax_income')),
            (v('pretax_income')-v('tax'),v('consolidated_net_income')),
            (v('consolidated_net_income')+v('canada_nci_signed')+v('other_nci_signed'),v('net_income')),
            (v('operating_cash_flow')+v('investing_cash_flow')+v('financing_cash_flow')+v('cash_fx'),v('cash_change')),
            (v('cash_start')+v('cash_change'),v('cash_end')), (v('cash_end'),v('balance_cash'))]
        if any(abs(a-b) > 1000 for a,b in checks):
            raise IssuerFinancialError('Annual accounting reconciliation failed')
    values, formulas = {}, {}
    def save(key, number, formula):
        if number.is_finite():
            values[key],formulas[key] = float(number),formula
    for metric,numerator in [('gross_margin','gross_profit'),('operating_margin','operating_income'),('net_margin','net_income')]:
        save(metric,selected[numerator][0]/selected['revenue'][0]*100,
             f'annual GAAP {numerator} / revenue * 100; net income attributable to parent')
    for metric,key in [('revenue_growth','revenue'),('eps_growth','basic_eps'),('fcf_growth','free_cash_flow')]:
        if selected[key][1] > 0:
            save(metric,(selected[key][0]/selected[key][1]-1)*100,
                 f'(annual {key} / prior comparable annual {key} - 1) * 100')
    save('operating_margin_expansion',(selected['operating_income'][0]/selected['revenue'][0]-
        selected['operating_income'][1]/selected['revenue'][1])*100,'current minus prior annual GAAP operating margin, percentage points')
    save('free_cash_flow',selected['free_cash_flow'][0],'annual operating cash flow + signed purchases of property and equipment, USD')
    save('fcf_margin',selected['free_cash_flow'][0]/selected['revenue'][0]*100,'annual free cash flow / revenue * 100')
    save('current_ratio',selected['current_assets'][0]/selected['current_liabilities'][0],'current assets / current liabilities at annual period end')
    return {'version':VERSION,'symbol':'ACN','cik':'0001467373','source':'issuer_sec_exhibit',
        'source_url':source_url,'source_sha256':hashlib.sha256(raw).hexdigest(),
        'observed_at':observed_at.isoformat(),'published_date':'2026-10-01',
        'availability_basis':'publication day only; no intraday historical availability established',
        'status':'prepared','publication_eligible':False,'values':values,
        'period_start':'2025-09-01','period_end':'2026-08-31',
        'prior_period_start':'2024-09-01','prior_period_end':'2025-08-31',
        'period_basis':'annual GAAP; balance sheet at period end','currency':'USD',
        'inputs':{k:{'current':float(v[0]),'prior':float(v[1]),
            'unit':'USD/share' if k.endswith('_eps') else 'USD',
            **({'period_type':'instant'} if k in {'current_assets','current_liabilities','balance_cash'} else {}),
            **({'derivation':'revenue - cost_of_sales'} if k == 'gross_profit' else
               {'derivation':'operating_cash_flow + capex_signed'} if k == 'free_cash_flow' else {})}
            for k,v in selected.items()},'calculations':formulas,
        'coverage':{'complete':False,'available_metrics':sorted(values)},
        'limitations':['Reviewed FY2026 source only; future periods require review.',
            'GAAP parent income and operating income; not adjusted earnings or FMP-equivalent by assumption.',
            'No valuation, debt/EBITDA, consensus or return-on-equity fields prepared.',
            'Cash-flow depreciation includes other items and cannot establish EBITDA.',
            'Prepared evidence only; canonical cache, score and alerts are unchanged.']}
