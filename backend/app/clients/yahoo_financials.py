"""Parse public Yahoo statement evidence without choosing canonical Walnut metrics.

This module neither fetches pages nor imports the database. Provider dates and
definitions remain explicit; collection time is not historical filing availability.
"""
from datetime import date, datetime, timezone
from html.parser import HTMLParser
import hashlib
import json
import math
import re
from urllib.parse import urlsplit, parse_qs

VERSION = 'yahoo_public_statement_evidence_v1'
PATHS = {'income': 'financials', 'balance': 'balance-sheet', 'cash_flow': 'cash-flow'}
METRICS = {
    'income': frozenset(('TotalRevenue', 'GrossProfit', 'OperatingIncome',
        'NetIncome', 'NetIncomeCommonStockholders', 'NetIncomeIncludingNoncontrollingInterests',
        'PretaxIncome', 'TaxProvision', 'InterestExpense', 'InterestIncome',
        'NetNonOperatingInterestIncomeExpense', 'EBIT', 'EBITDA', 'NormalizedEBITDA',
        'ReconciledDepreciation', 'DepreciationAndAmortizationInIncomeStatement')),
    'balance': frozenset(('TotalAssets', 'CurrentAssets', 'CurrentLiabilities',
        'TotalLiabilitiesNetMinorityInterest', 'StockholdersEquity', 'CommonStockEquity',
        'TotalEquityGrossMinorityInterest', 'CashAndCashEquivalents',
        'CashCashEquivalentsAndShortTermInvestments', 'TotalDebt', 'NetDebt',
        'CurrentDebt', 'LongTermDebt', 'CurrentDebtAndCapitalLeaseObligation',
        'LongTermDebtAndCapitalLeaseObligation', 'CapitalLeaseObligations')),
    'cash_flow': frozenset(('OperatingCashFlow', 'CapitalExpenditure', 'FreeCashFlow',
        'DepreciationAndAmortization', 'DepreciationAmortizationDepletion',
        'InvestingCashFlow', 'FinancingCashFlow', 'EndCashPosition')),
}


class YahooStatementError(ValueError):
    pass


def _object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise YahooStatementError('duplicate_json_key')
        result[key] = value
    return result


def _json(value):
    try:
        return json.loads(value, object_pairs_hook=_object,
            parse_constant=lambda _: (_ for _ in ()).throw(YahooStatementError('nonfinite_json')))
    except (TypeError, ValueError, RecursionError) as exc:
        raise YahooStatementError('invalid_statement_json') from exc


class _Page(HTMLParser):
    def __init__(self, symbol):
        super().__init__()
        self.symbol = symbol
        self.canonicals = []
        self.payloads = []
        self.parts = None

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if tag == 'link' and attrs.get('rel') == 'canonical':
            self.canonicals.append(attrs.get('href'))
        if tag == 'script':
            self.parts = None
            if attrs.get('type') != 'application/json' or 'data-sveltekit-fetched' not in attrs:
                return
            url = urlsplit(attrs.get('data-url', ''))
            if (url.scheme == 'https' and url.netloc == 'query1.finance.yahoo.com'
                    and url.path == '/ws/fundamentals-timeseries/v1/finance/timeseries/'+self.symbol):
                self.parts = []

    def handle_data(self, data):
        if self.parts is not None:
            self.parts.append(data)

    def handle_endtag(self, tag):
        if tag == 'script' and self.parts is not None:
            self.payloads.append(''.join(self.parts))
            self.parts = None


def parse_statement(raw, *, symbol, statement, source_url, observed_at):
    if not isinstance(raw, bytes) or not 0 < len(raw) <= 4_000_000:
        raise YahooStatementError('statement_size_invalid')
    if not isinstance(symbol, str) or not re.fullmatch(r'[A-Z][A-Z0-9.-]{0,14}', symbol):
        raise YahooStatementError('symbol_invalid')
    if statement not in PATHS:
        raise YahooStatementError('statement_kind_invalid')
    if not isinstance(observed_at, datetime) or observed_at.utcoffset() is None:
        raise YahooStatementError('observation_time_required')
    observed_at = observed_at.astimezone(timezone.utc)
    expected_url = f'https://finance.yahoo.com/quote/{symbol}/{PATHS[statement]}/'
    if source_url != expected_url:
        raise YahooStatementError('statement_url_mismatch')
    page = _Page(symbol)
    try:
        page.feed(raw.decode('utf-8'))
        page.close()
    except (UnicodeError, ValueError, RecursionError) as exc:
        raise YahooStatementError('statement_html_invalid') from exc
    if page.canonicals != [expected_url] or len(page.payloads) != 1:
        raise YahooStatementError('statement_identity_or_payload_missing')
    wrapper = _json(page.payloads[0])
    if not isinstance(wrapper, dict) or type(wrapper.get('status')) is not int or wrapper['status'] != 200:
        raise YahooStatementError('statement_upstream_error')
    body = _json(wrapper.get('body'))
    return _project(body, raw=raw, symbol=symbol, statement=statement, source_url=source_url, observed_at=observed_at)


def parse_timeseries(raw, *, symbol, statement, source_url, observed_at):
    if not isinstance(raw, bytes) or not 0 < len(raw) <= 1_000_000:
        raise YahooStatementError('statement_size_invalid')
    if not isinstance(symbol, str) or not re.fullmatch(r'[A-Z][A-Z0-9.-]{0,14}', symbol):
        raise YahooStatementError('symbol_invalid')
    if statement not in PATHS:
        raise YahooStatementError('statement_kind_invalid')
    if not isinstance(observed_at, datetime) or observed_at.utcoffset() is None:
        raise YahooStatementError('observation_time_required')
    url = urlsplit(source_url)
    query = parse_qs(url.query, keep_blank_values=True)
    if (url.scheme != 'https' or url.netloc != 'query1.finance.yahoo.com' or url.fragment
            or url.path != '/ws/fundamentals-timeseries/v1/finance/timeseries/'+symbol
            or set(query) != {'merge','padTimeSeries','period1','period2','type','lang','region'}
            or any(len(values) != 1 for values in query.values())
            or query['merge'] != ['false'] or query['padTimeSeries'] != ['true']
            or query['lang'] != ['en-US'] or query['region'] != ['US']):
        raise YahooStatementError('statement_url_mismatch')
    try:
        first, last = int(query['period1'][0]), int(query['period2'][0])
    except (TypeError, ValueError) as exc:
        raise YahooStatementError('statement_request_period_invalid') from exc
    if not 0 <= first < last:
        raise YahooStatementError('statement_request_period_invalid')
    requested = query['type'][0].split(',')
    if (not requested or len(requested)>1000 or len(set(requested)) != len(requested)
            or any(not re.fullmatch(r'(annual|trailing|quarterly)[A-Z][A-Za-z0-9]+', key) for key in requested)):
        raise YahooStatementError('statement_request_metrics_invalid')
    body = _json(raw)
    return _project(body, raw=raw, symbol=symbol, statement=statement, source_url=source_url,
        observed_at=observed_at.astimezone(timezone.utc), requested=set(requested))


def _project(body, *, raw, symbol, statement, source_url, observed_at, requested=None):
    series = body.get('timeseries') if isinstance(body, dict) else None
    if not isinstance(series, dict) or series.get('error') is not None:
        raise YahooStatementError('statement_upstream_error')
    rows = series.get('result')
    if not isinstance(rows, list) or not 0 < len(rows) <= 1000:
        raise YahooStatementError('statement_series_invalid')
    points, missing, nulls = {}, set(), {}
    returned_keys = set()
    for row in rows:
        meta = row.get('meta') if isinstance(row, dict) else None
        if not isinstance(meta, dict) or meta.get('symbol') != [symbol]:
            raise YahooStatementError('statement_symbol_mismatch')
        kinds = meta.get('type')
        if not isinstance(kinds, list) or len(kinds) != 1 or not isinstance(kinds[0], str):
            raise YahooStatementError('statement_metric_invalid')
        key = kinds[0]
        returned_keys.add(key)
        if requested is not None and key not in requested:
            raise YahooStatementError('unrequested_statement_metric')
        match = re.fullmatch(r'(annual|trailing|quarterly)([A-Z][A-Za-z0-9]+)', key)
        if not match or match[2] not in METRICS[statement]:
            continue
        frequency, metric = match.groups()
        if statement == 'balance' and frequency == 'trailing':
            continue
        values = row.get(key, [])
        if not isinstance(values, list) or len(values) > 100:
            raise YahooStatementError('statement_points_invalid')
        if not values:
            missing.add(key)
        for point in values:
            if point is None:
                nulls[key] = nulls.get(key, 0)+1
                continue
            if not isinstance(point, dict):
                raise YahooStatementError('statement_point_invalid')
            end = point.get('asOfDate')
            if not isinstance(end, str) or not re.fullmatch(r'\d{4}-\d{2}-\d{2}', end):
                raise YahooStatementError('statement_period_invalid')
            try:
                period = date.fromisoformat(end)
            except ValueError as exc:
                raise YahooStatementError('statement_period_invalid') from exc
            if period > observed_at.date() or point.get('periodType') != {'annual':'12M','trailing':'TTM','quarterly':'3M'}[frequency]:
                raise YahooStatementError('statement_period_mismatch')
            currency = point.get('currencyCode')
            if not isinstance(currency, str) or not re.fullmatch(r'[A-Z]{3}', currency):
                raise YahooStatementError('statement_currency_invalid')
            reported = point.get('reportedValue')
            if not isinstance(reported, dict) or type(reported.get('raw')) not in (int, float):
                raise YahooStatementError('statement_value_invalid')
            value = reported['raw']
            try:
                finite = math.isfinite(value)
            except OverflowError:
                finite = False
            if not finite:
                raise YahooStatementError('statement_value_invalid')
            item = {'metric': metric, 'frequency': frequency, 'provider_period_type': point['periodType'],
                'provider_as_of_date': end, 'value': value, 'currency': currency,
                'source_metric': key, 'period_basis': 'instant' if statement == 'balance' else 'duration'}
            identity = (key, end)
            if identity in points and points[identity] != item:
                raise YahooStatementError('conflicting_statement_point')
            points[identity] = item
    if requested is not None:
        for key in requested - returned_keys:
            match = re.fullmatch(r'(annual|trailing|quarterly)([A-Z][A-Za-z0-9]+)', key)
            if match and match[2] in METRICS[statement] and not (statement == 'balance' and match[1] == 'trailing'):
                missing.add(key)
    if not points:
        raise YahooStatementError('statement_has_no_supported_values')
    return {'version': VERSION, 'provider': 'yahoo_finance', 'status': 'evidence_only',
        'symbol': symbol, 'statement': statement, 'source_url': source_url,
        'source_sha256': hashlib.sha256(raw).hexdigest(), 'observed_at': observed_at.isoformat(),
        'values': [points[key] for key in sorted(points)], 'unavailable_series': sorted(missing - {item['source_metric'] for item in points.values()}),
        'null_observation_counts': dict(sorted(nulls.items())),
        'limitations': ['Provider period dates are not verified issuer fiscal-end or filing dates.',
            'Values are raw currency amounts, not the displayed thousands.',
            'EBITDA, NormalizedEBITDA and ReconciledDepreciation retain distinct provider definitions.',
            'No SEC component, public ratio, score or alert is replaced by this evidence.',
            'Historical publication availability is unknown; observation time is recorded.']}
