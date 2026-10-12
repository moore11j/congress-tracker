"""Reviewed issuer earnings tables, preparation only; no network or canonical writes."""
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import re
from zoneinfo import ZoneInfo

VERSION = 'issuer_release_financials_v1'
AZO_URL = 'https://www.sec.gov/Archives/edgar/data/866787/000117184326006159/exh_991.htm'
AMOUNTS = {
    'Net sales': 'revenue', 'Cost of sales': 'cost_of_sales',
    'Gross profit': 'gross_profit', 'Operating, SG&A expenses': 'operating_expenses',
    'Operating profit (EBIT)': 'operating_income', 'Interest expense, net': 'net_interest_expense',
    'Income before taxes': 'pretax_income', 'Income tax expense': 'tax', 'Net income': 'net_income',
}


class IssuerFinancialError(ValueError):
    pass


def _cells(row):
    return [' '.join(c.text_content().split()) for c in row.xpath('./td|./th')]


def _numbers(cells):
    # Dollar signs and parentheses can be separate HTML cells.
    text = ' '.join(cells).replace('$', '').strip()
    tokens = re.findall(r'\(\s*[\d,]+(?:\.\d+)?\s*\)|[\d,]+(?:\.\d+)?', text)
    remainder = re.sub(r'\(\s*[\d,]+(?:\.\d+)?\s*\)|[\d,]+(?:\.\d+)?', '', text)
    if remainder.strip():
        raise IssuerFinancialError('Unreviewed numeric cell')
    try:
        return [Decimal(t.replace(',', '').replace(' ', '').replace('(', '-').replace(')', '')) for t in tokens]
    except InvalidOperation as exc:
        raise IssuerFinancialError('Invalid financial number') from exc


def prepare_autozone_annual(raw: bytes, *, source_url: str, observed_at: datetime):
    """The reviewed FY2026 release only. New periods require source review.

    Use the actual annual GAAP table, never the quarter or adjusted EBITDAR.
    EPS growth is annual reported diluted EPS growth, not analyst consensus.
    """
    if source_url != AZO_URL:
        raise IssuerFinancialError('Unreviewed issuer release')
    if observed_at.tzinfo is None or not raw or len(raw) > 3_000_000:
        raise IssuerFinancialError('Invalid observation or document size')
    from lxml import html
    try:
        tree = html.fromstring(raw)
    except (ValueError, html.etree.ParserError) as exc:
        raise IssuerFinancialError('Invalid release HTML') from exc
    for node in tree.xpath('//script|//style'):
        node.drop_tree()
    text = ' '.join(tree.text_content().split())
    if not re.search(r'AutoZone,?\s+Inc\.\s*\(NYSE:\s*AZO\)', text):
        raise IssuerFinancialError('Issuer identity mismatch')
    if not re.search(r'MEMPHIS, Tenn\., Sept\. 22, 2026', text):
        raise IssuerFinancialError('Publication date mismatch')
    publication = date(2026, 9, 22)
    # Only the day is supplied; do not claim an intraday historical timestamp.
    if observed_at.astimezone(ZoneInfo("America/New_York")).date() <= publication:
        raise IssuerFinancialError('Publication day not completed')
    candidates = []
    for table in tree.xpath('//table[not(.//table)]'):
        rows = [_cells(row) for row in table.xpath('.//tr')]
        markers = [i for i, row in enumerate(rows) if row and row[0] == 'Fiscal Year 2026']
        if markers:
            if len(markers) != 1:
                raise IssuerFinancialError('Duplicate annual heading')
            candidates.append(rows[markers[0]:])
    if len(candidates) != 1:
        raise IssuerFinancialError('Missing or ambiguous annual statement')
    rows = candidates[0]
    header = [' | '.join(c for c in row if c) for row in rows[:5]]
    if header != ['Fiscal Year 2026', '(in thousands, except per share data)',
                  'GAAP Results', '52 Weeks Ended | 52 Weeks Ended',
                  'August 29, 2026 | August 30, 2025']:
        raise IssuerFinancialError('Annual period, basis or unit mismatch')
    selected = {}
    group = None
    for row in rows[5:]:
        if not row or not row[0]:
            continue
        label = row[0]
        if label == 'Selected Balance Sheet Information':
            break
        if label in {'Net income per share:', 'Weighted average shares outstanding:'}:
            group = label
            continue
        metric = AMOUNTS.get(label)
        if label == 'Diluted' and group == 'Net income per share:':
            metric = 'diluted_eps'
        if not metric:
            continue
        values = _numbers(row[1:])
        if len(values) != 2 or metric in selected:
            raise IssuerFinancialError('Missing or duplicate annual values')
        scale = 1 if metric == 'diluted_eps' else 1000
        selected[metric] = [value * scale for value in values]
    if set(selected) != {*AMOUNTS.values(), 'diluted_eps'}:
        raise IssuerFinancialError('Incomplete annual statement')
    for i in (0, 1):
        def value(key): return selected[key][i]
        if value('revenue') <= 0 or any(value(k) < 0 for k in ('cost_of_sales', 'operating_expenses')):
            raise IssuerFinancialError('Unsupported annual inputs')
        # Reported amounts are rounded to thousands; allow only that precision.
        equalities = [(value('revenue')-value('cost_of_sales'), value('gross_profit')),
            (value('gross_profit')-value('operating_expenses'), value('operating_income')),
            (value('operating_income')-value('net_interest_expense'), value('pretax_income')),
            (value('pretax_income')-value('tax'), value('net_income'))]
        if any(abs(a-b) > 1000 for a,b in equalities):
            raise IssuerFinancialError('Annual accounting reconciliation failed')
    end, prior_end = date(2026,8,29), date(2025,8,30)
    values, formulas = {}, {}
    def save(metric, number, formula):
        if number.is_finite():
            values[metric], formulas[metric] = float(number), formula
    for metric, numerator in [('gross_margin','gross_profit'), ('operating_margin','operating_income'), ('net_margin','net_income')]:
        save(metric, selected[numerator][0]/selected['revenue'][0]*100,
             f'52-week GAAP {numerator} / revenue * 100')
    save('revenue_growth', (selected['revenue'][0]/selected['revenue'][1]-1)*100,
         '(52-week GAAP revenue / prior comparable 52-week revenue - 1) * 100')
    if selected['diluted_eps'][1] > 0:
        save('eps_growth', (selected['diluted_eps'][0]/selected['diluted_eps'][1]-1)*100,
             '(52-week GAAP diluted EPS / prior comparable 52-week diluted EPS - 1) * 100')
    save('operating_margin_expansion', (selected['operating_income'][0]/selected['revenue'][0]-
         selected['operating_income'][1]/selected['revenue'][1])*100,
         'current minus prior comparable 52-week GAAP operating margin, percentage points')
    return {'version': VERSION, 'symbol': 'AZO', 'cik': '0000866787', 'source': 'issuer_sec_exhibit',
        'source_url': source_url, 'source_sha256': hashlib.sha256(raw).hexdigest(),
        'observed_at': observed_at.isoformat(), 'published_date': str(publication),
        'availability_basis': 'publication day only; no intraday historical availability established',
        'status': 'prepared', 'publication_eligible': False, 'values': values,
        'period_start': str(end-timedelta(days=363)), 'period_end': str(end),
        'prior_period_start': str(prior_end-timedelta(days=363)), 'prior_period_end': str(prior_end),
        'period_basis': '52-week annual GAAP', 'currency': 'USD',
        'inputs': {k: {'current':float(v[0]), 'prior':float(v[1]),
            'unit':'USD/share' if k=='diluted_eps' else 'USD'} for k,v in selected.items()},
        'calculations': formulas, 'coverage': {'complete':False, 'available_metrics':sorted(values)},
        'limitations': ['Reviewed FY2026 source only; future periods require review.',
            'No valuation, cash-flow, balance-sheet or consensus fields published.',
            'Adjusted debt/EBITDAR is not net debt/EBITDA.',
            'Prepared evidence only; canonical cache, score and alerts are unchanged.']}
