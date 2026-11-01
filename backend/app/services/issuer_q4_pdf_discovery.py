"""Reviewed Meta/SoFi public report indexes and independently validated PDFs."""
import calendar
from datetime import date, timedelta
import hashlib
import json
import re
from urllib.parse import urlencode

from app.clients.direct_sources import DirectSourceError, parse_issuer_material

KINDS = {'META': 'meta_q4_pdf_v1', 'SOFI': 'sofi_q4_pdf_v1'}
QUARTERS = ('First Quarter', 'Second Quarter', 'Third Quarter', 'Fourth Quarter')
MONTHS = ('January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December')
ENGLISH_DATE = r'(' + '|'.join(MONTHS) + r')\s+(\d{1,2})(?:st|nd|rd|th)?,\s*(20\d{2})'
NUMERIC_DATE = r'(\d{1,2})\s*[-\u2013]\s*(\d{1,2})\s*[-\u2013]\s*(20\d{2})'
POLICIES = {
    'META': {'base': 'https://investor.atmeta.com',
             'cdn': 'https://s21.q4cdn.com/399680738/files/doc_financials/',
             'category': 'Financial Report', 'title': 'Earnings Call Transcript',
             'company_pattern': r'\bMeta Platforms,?\s+Inc\.',
             'qa_pattern': r'\b(?:your|our)\s+first question comes from\b'},
    'SOFI': {'base': 'https://investors.sofi.com',
             'cdn': 'https://s27.q4cdn.com/749715820/files/doc_financials/',
             'category': 'transcript', 'title': 'Transcript',
             'company_pattern': r'\bSoFi Technologies\b',
             'qa_pattern': r'Operator[\^:]\s*(?:\(Operator Instructions\)\s*)?(?:And\s+)?(?:(?:Our|Your)\s+)?(?:first|\ufb01rst)\s+question(?:\s+today)?\s+comes\b'},
}


def policy(company):
    symbol = company.get('symbol')
    spec = POLICIES.get(symbol)
    if (spec is None or company.get('discovery', {}).get('kind') != KINDS[symbol]
            or company.get('investor_website') != spec['base']+'/'):
        raise DirectSourceError('Unreviewed issuer PDF discovery policy')
    return spec


def index_url(company, year):
    spec = policy(company)
    if type(year) is not int or not 2000 <= year <= 2100:
        raise DirectSourceError('Invalid issuer report year')
    params = [('LanguageId', 1), ('reportTypes', '|'.join(QUARTERS)),
              *[(key, label) for key in ('reportSubType', 'reportSubTypeList') for label in QUARTERS],
              ('pageSize', 10), ('pageNumber', 0), ('tagList', ''), ('includeTags', 'true'),
              ('year', year), ('excludeSelection', 1)]
    return spec['base']+'/feed/FinancialReport.svc/GetFinancialReportList?'+urlencode(params)


def publication_pattern(symbol, day):
    if symbol == 'META':
        return rf'{MONTHS[day.month-1]}\s+{day.day}(?:st|nd|rd|th)?,\s*{day.year}'
    if symbol == 'SOFI':
        return rf'\b(?:{day.month:02d}|{day.month})\s*[-\u2013]\s*(?:{day.day:02d}|{day.day})\s*[-\u2013]\s*{day.year}\b'
    raise DirectSourceError('Unreviewed issuer PDF date policy')


def parse_index(raw, company, year, observed):
    spec = policy(company)
    source_url = index_url(company, year)
    if not raw or len(raw) > 2_000_000:
        raise DirectSourceError('Issuer PDF index size invalid')
    try:
        reports = json.loads(raw)['GetFinancialReportListResult']
    except (ValueError, TypeError, KeyError) as exc:
        raise DirectSourceError('Invalid issuer PDF report index') from exc
    if not isinstance(reports, list) or len(reports) > 4:
        raise DirectSourceError('Issuer PDF report count exceeds fiscal-year bound')
    symbol, found, periods = company['symbol'], [], set()
    for report in reports:
        if not isinstance(report, dict):
            raise DirectSourceError('Invalid issuer PDF report row')
        label = report.get('ReportSubType')
        if (label not in QUARTERS or report.get('ReportYear') != year
                or report.get('ReportTitle') != f'{label} {year}' or report.get('LanguageId') != 1):
            raise DirectSourceError('Issuer PDF report fiscal identity mismatch')
        quarter = QUARTERS.index(label)+1
        if quarter in periods:
            raise DirectSourceError('Duplicate issuer PDF report period')
        periods.add(quarter)
        end = date(year, quarter*3, calendar.monthrange(year, quarter*3)[1])
        if report.get('ReportDate') != end.strftime('%m/%d/%Y 00:00:00') or end > observed.date():
            raise DirectSourceError('Issuer PDF report date mismatch')
        documents = report.get('Documents')
        if (not isinstance(documents, list) or len(documents) > 30
                or any(not isinstance(d, dict) for d in documents)):
            raise DirectSourceError('Issuer PDF index document bound or type invalid')
        # Meta separately publishes a Follow Up Call Transcript. Its label is
        # deliberately excluded; it cannot replace the earnings-call document.
        matches = [d for d in documents if d.get('DocumentTitle') == spec['title']]
        if len(matches) > 1:
            raise DirectSourceError('Ambiguous issuer earnings transcript')
        for item in matches:
            url = item.get('DocumentPath', '')
            subdir = r'(?:Transcripts/)?' if symbol == 'META' else ''
            allowed = re.escape(spec['cdn'])+rf'{year}/q{quarter}/'+subdir+r'[A-Za-z0-9_-]+\.pdf'
            # Meta's current investor index also links named transcripts from
            # its content_files directory. Keep the tenant, issuer, exact fiscal
            # period and earnings-call document type in the permitted filename.
            if symbol == 'META':
                content = (r'https://s21\.q4cdn\.com/399680738/files/content_files/'
                           + rf'META-Q{quarter}-{year}-Earnings-Call-Transcript\.pdf')
                allowed = rf'(?:{allowed}|{content})'
            elif symbol == 'SOFI':
                # The official index moved transcripts to content_files. Bind
                # the issuer tenant, spelled fiscal quarter/year and document
                # identity here; the complete PDF must still prove them again.
                content = (r'https://s27\.q4cdn\.com/749715820/files/content_files/'
                           + rf'SoFi-{label.replace(" ", "-")}-{year}-Earnings-Transcript_FINAL-docx\.pdf')
                allowed = rf'(?:{allowed}|{content})'
            if (not isinstance(url, str) or not re.fullmatch(allowed, url)
                    or item.get('DocumentCategory') != spec['category'] or item.get('DocumentFileType') != 'PDF'
                    or item.get('DocumentType') not in {'File', 'file'} or item.get('LanguageId') != 1):
                raise DirectSourceError('Unreviewed issuer PDF link, label or type')
            period = rf'\b{label} {year}\b' if symbol == 'META' else rf'\b(?:Q{quarter}|{label})\s+{year}\s+Earnings\b'
            found.append({'key': f'{symbol}:earnings_transcript:{year}:Q{quarter}', 'symbol': symbol,
                'document_type': 'earnings_transcript', 'fiscal_year': year, 'fiscal_quarter': quarter,
                'url': url, 'source_format': 'pdf', 'company_website': company['company_website'],
                'investor_website': company['investor_website'], 'company_pattern': spec['company_pattern'],
                'publisher_pattern': spec['company_pattern'],
                'transcript_publisher': 'Issuer-hosted transcript; publisher not identified',
                'period_pattern': period, 'qa_pattern': spec['qa_pattern'],
                'transcript_pattern': r'Results Conference Call' if symbol == 'META' else r'\bTRANSCRIPT\b',
                'publication_pattern': ENGLISH_DATE if symbol == 'META' else NUMERIC_DATE,
                'source_page_url': source_url, 'source_page_sha256': hashlib.sha256(raw).hexdigest(),
                'discovery_kind': KINDS[symbol], 'discovery_year': year, 'period_end': end.isoformat()})
    return found


def parse_candidate(raw, metadata, observed):
    symbol = metadata.get('symbol')
    if symbol not in KINDS or metadata.get('discovery_kind') != KINDS[symbol]:
        raise DirectSourceError('Unreviewed issuer PDF candidate')
    provisional = {**metadata, 'publication_pattern': ENGLISH_DATE if symbol == 'META' else NUMERIC_DATE}
    provisional.pop('published_at', None)
    text, parsed = parse_issuer_material(raw, provisional)
    year, quarter = metadata['fiscal_year'], metadata['fiscal_quarter']
    label = QUARTERS[quarter-1]
    if symbol == 'META':
        prefix = rf'^(?:1\s+)?Meta Platforms,\s+Inc\.\s+\(META\)\s+{label}\s+{year}\s+Results Conference Call\s+'
        match = re.match(prefix+ENGLISH_DATE, text)
    else:
        match = re.match(r'^TRANSCRIPT\s+'+NUMERIC_DATE+rf'\s+SoFi Technologies\s+(?:SOFI\s+)?(?:Q{quarter}|{label})\s+{year}\s+Earnings\b', text)
    if not match:
        raise DirectSourceError('Issuer PDF heading date or fiscal identity missing')
    try:
        month = MONTHS.index(match[1])+1 if symbol == 'META' else int(match[1])
        publication = date(int(match[3]), month, int(match[2]))
    except ValueError as exc:
        raise DirectSourceError('Invalid issuer PDF heading date') from exc
    end = date(year, quarter*3, calendar.monthrange(year, quarter*3)[1])
    if (metadata['period_end'] != end.isoformat()
            or not end <= publication <= min(observed.date(), end+timedelta(days=100))):
        raise DirectSourceError('Issuer PDF publication outside fiscal window')
    resolved = {**provisional, 'published_at': publication.isoformat(),
                'publication_pattern': publication_pattern(symbol, publication)}
    return resolved, text, {**parsed, **resolved}
