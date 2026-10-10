"""Current SEC filing links with exact issuer identity and bounded coverage."""
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
import logging
import re

from app.services.sec_directory import _symbol_key as sec_symbol

PROVIDER = 'sec_edgar_submissions'
logger = logging.getLogger(__name__)


def parse_company_filings(raw, *, symbol, cik):
    from app.services.fmp_news import _sec_filing_title
    company = json.loads(raw)
    symbol = sec_symbol(symbol)
    cik = str(cik).zfill(10)
    if (not cik.isdigit() or str(company.get('cik', '')).zfill(10) != cik
            or [sec_symbol(s) for s in company.get('tickers', [])].count(symbol) != 1):
        raise ValueError('SEC filing issuer identity mismatch')
    filings = company.get('filings', {})
    recent = filings.get('recent', {})
    required = ['accessionNumber', 'filingDate', 'form', 'primaryDocument']
    if any(not isinstance(recent.get(key), list) for key in required):
        raise ValueError('SEC recent filing columns missing')
    size = len(recent['accessionNumber'])
    if size > 50000 or any(len(recent[key]) != size for key in required):
        raise ValueError('SEC recent filing columns misaligned or oversized')
    accepted = recent.get('acceptanceDateTime', [None] * size)
    if not isinstance(accepted, list) or len(accepted) != size:
        raise ValueError('SEC acceptance column misaligned')
    by_accession = {}
    dates = [date.fromisoformat(value).isoformat() for value in recent['filingDate']]
    selected = sorted(range(size), key=lambda i: (dates[i], str(recent['accessionNumber'][i])), reverse=True)[:2000]
    for i in selected:
        accession, document, form = (recent[key][i] for key in ['accessionNumber','primaryDocument','form'])
        filed = dates[i]
        if (not re.fullmatch(r'\d{10}-\d{2}-\d{6}', str(accession))
                or not isinstance(form, str) or not form.strip()):
            raise ValueError('SEC filing identity malformed')
        if accepted[i]:
            datetime.fromisoformat(accepted[i].replace('Z', '+00:00'))
        # Some official entries lack a primary filename. The accession index
        # remains the canonical source link; never guess an exhibit document.
        base = f'https://www.sec.gov/Archives/edgar/data/{int(cik)}/{accession.replace("-", "")}/'
        if document:
            parts = str(document).split('/')
            if not 1 <= len(parts) <= 4 or any(not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._-]*', part) or part in {'.', '..'} for part in parts):
                raise ValueError('SEC primary filename unsafe')
            url = base + document
        else:
            url = base + accession + '-index.html'
        row = {'symbol': symbol, 'filing_date': filed, 'accepted_date': accepted[i] or None,
               'form_type': form, 'title': _sec_filing_title(form, None), 'url': url,
               'source': PROVIDER, 'accession_number': accession}
        if accession in by_accession and by_accession[accession] != row:
            raise ValueError('Conflicting SEC accession rows')
        by_accession[accession] = row
    items = sorted(by_accession.values(), key=lambda r: (r['filing_date'], r['accession_number']), reverse=True)
    return {'items': items, 'status': 'ok' if items else 'empty', 'provider': PROVIDER,
            'source_url': f'https://data.sec.gov/submissions/CIK{cik}.json',
            'source_sha256': hashlib.sha256(raw).hexdigest(),
            'coverage': {'kind': 'recent_submissions', 'earliest_filing_date': min((r['filing_date'] for r in items), default=None),
                         'source_row_count': size, 'retained_row_limit': 2000, 'truncated': size > 2000,
                         'older_archives_available': bool(filings.get('files'))},
            'message': 'Recent filings from SEC EDGAR. Older archive coverage may be incomplete.'}


def get_company_filings(*, symbol, from_date=None, to_date=None, page=0, limit=100):
    from app.clients.direct_sources import DirectSourceClient
    from app.services.sec_fundamentals import company_directory
    from app.services import fmp_news as news
    from app.services.ticker_content_cache import db_ticker_content_cache_get, db_ticker_content_cache_set, paginate_ticker_content_payload
    today = datetime.now(timezone.utc).date()
    symbol = sec_symbol(symbol)
    page, limit = max(0, int(page)), min(100, max(1, int(limit)))
    try:
        start, end = date.fromisoformat(from_date) if from_date else today-timedelta(days=365), date.fromisoformat(to_date) if to_date else today
        if start > end:
            raise ValueError('Filing date window is reversed')
    except ValueError:
        return news._unavailable_payload(page=page, limit=limit, message='Invalid filing date range.', reason='invalid_date_range')
    key = news._cache_key('direct-sec-filings', {'symbol': symbol, 'from': str(start), 'to': str(end), 'page': page, 'limit': limit})
    category = 'news:sec-filings'
    cached = news._cache_get(key, category=category, symbol=symbol)
    if cached is not None:
        return cached
    saved = db_ticker_content_cache_get('sec_company_filings', symbol, page=page, limit=limit, from_date=str(start), to_date=str(end))
    if saved is not None and saved.get('provider') == PROVIDER and saved.get('cache_status') != 'stale':
        return news._cache_set(key, saved, ttl_seconds=news.SEC_FILINGS_TTL_SECONDS, category=category, symbol=symbol)
    context = news.get_request_context() or {}
    route = str(context.get('path') or '')
    if route.startswith('/api/') and not route.startswith('/api/admin/'):
        return news._public_cache_miss_payload(cache_key=key, category=category, symbol=symbol,
            page=page, limit=limit, job_type='sec_filings', stale_message='SEC filing list is being refreshed.',
            payload={'from_date': str(start), 'to_date': str(end), 'page': page, 'limit': limit})
    try:
        issuer = company_directory(str(today)).get(symbol)
        if issuer is None:
            payload = news._unavailable_payload(page=page, limit=limit,
                message='This symbol is not listed in the SEC company directory.',
                reason='symbol_absent_from_sec_directory')
            payload.update(provider=PROVIDER, reason='symbol_absent_from_sec_directory',
                coverage={'kind': 'recent_submissions', 'complete': False})
        else:
            raw = DirectSourceClient().get(f'https://data.sec.gov/submissions/CIK{issuer["cik"]}.json')
            payload = parse_company_filings(raw, symbol=symbol, cik=issuer['cik'])
        payload['updated_at'] = datetime.now(timezone.utc).isoformat()
        # Persist the entire bounded recent list once, so later pages/windows
        # do not mistake an earlier requested page for the complete source.
        db_ticker_content_cache_set('sec_company_filings', symbol, payload, window_key='sec_recent', source='sec_edgar')
        result = paginate_ticker_content_payload(payload, page=page, limit=limit, from_date=str(start), to_date=str(end))
        return news._cache_set(key, result, ttl_seconds=news.SEC_FILINGS_TTL_SECONDS, category=category, symbol=symbol)
    except Exception as exc:
        logger.warning('direct_sec_filing_list_unavailable symbol=%s error_type=%s', symbol, type(exc).__name__)
        if saved is not None and saved.get('provider') == PROVIDER:
            return {**saved, 'stale': True, 'cache_status': 'stale', 'message': 'SEC refresh unavailable; showing previously retrieved filings. Older archives may be incomplete.'}
        return news._unavailable_payload(page=page, limit=limit, message='SEC filings are temporarily unavailable.', reason='sec_source_unavailable')
