"""Reviewed Microsoft latest-event discovery; no fabricated fiscal or call dates."""
import calendar
from datetime import date, timedelta
import hashlib
import re
from urllib.parse import urljoin

from app.clients.direct_sources import DirectSourceError, parse_issuer_material

KIND = 'microsoft_events_v1'
INDEX_URL = 'https://www.microsoft.com/en-us/investor/default'
EVENT = re.compile(r'https://www\.microsoft\.com/en-us/investor/events/fy-(20\d{2})/earnings-fy-(20\d{2})-q([1-4])')
ORDINALS = ('First', 'Second', 'Third', 'Fourth')
MONTHS = ('January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December')
DATE = r'(' + '|'.join(MONTHS) + r')\s+(\d{1,2}),\s*(20\d{2})'
QA = r'\b(?:go|move)\s+(?:over\s+)?to\s+Q&A\b'


def publication_pattern(day):
    return rf'{MONTHS[day.month-1]}\s+{day.day},\s*{day.year}'


def fiscal_end(year, quarter):
    month = (9, 12, 3, 6)[quarter-1]
    calendar_year = year-1 if quarter <= 2 else year
    return date(calendar_year, month, calendar.monthrange(calendar_year, month)[1])


def parse_index(raw, company, observed):
    from lxml import html
    if (company.get('symbol') != 'MSFT' or company.get('discovery', {}).get('kind') != KIND
            or company.get('investor_website') != 'https://www.microsoft.com/en-us/Investor'):
        raise DirectSourceError('Unreviewed Microsoft discovery identity')
    if not raw or len(raw) > 2_000_000:
        raise DirectSourceError('Microsoft index size invalid')
    root = html.fromstring(raw)
    found = {}
    # Only literal issuer event links grant permission. Upcoming announcements,
    # notification buttons and inferred URL templates cannot create a period.
    for node in root.xpath('//a[@href]'):
        url = urljoin(INDEX_URL, node.get('href'))
        match = EVENT.fullmatch(url)
        if not match:
            continue
        year, repeated_year, quarter = map(int, match.groups())
        if year != repeated_year:
            raise DirectSourceError('Microsoft event fiscal identity mismatch')
        if not observed.year-1 <= year <= observed.year+1:
            continue
        end = fiscal_end(year, quarter)
        if end > observed.date():
            continue
        key = f'MSFT:earnings_transcript:{year}:Q{quarter}'
        if key in found:
            continue  # Duplicate navigation links are the same exact event URL.
        title = rf'Microsoft Fiscal Year {year} {ORDINALS[quarter-1]} Quarter Earnings Conference Call'
        found[key] = {'key': key, 'symbol': 'MSFT', 'document_type': 'earnings_transcript',
            'fiscal_year': year, 'fiscal_quarter': quarter, 'url': url, 'source_format': 'html',
            'company_website': company['company_website'], 'investor_website': company['investor_website'],
            'company_pattern': r'\bMicrosoft\b', 'period_pattern': title,
            'transcript_pattern': r'\bTranscript\s+Microsoft\s+FY', 'qa_pattern': QA,
            'transcript_publisher': 'Microsoft', 'publication_pattern': DATE,
            'source_page_url': INDEX_URL, 'source_page_sha256': hashlib.sha256(raw).hexdigest(),
            'discovery_kind': KIND, 'period_end': end.isoformat()}
    if not found or len(found) > 8:
        raise DirectSourceError('Microsoft index has no bounded completed earnings event links')
    return sorted(found.values(), key=lambda m: (m['fiscal_year'], m['fiscal_quarter']))


def parse_candidate(raw, metadata, observed):
    provisional = {**metadata, 'publication_pattern': DATE}
    provisional.pop('published_at', None)
    text, parsed = parse_issuer_material(raw, provisional)
    heading = re.match(re.escape(metadata['period_pattern']) + r'\s+(?:[A-Za-z]+,?\s+)?' + DATE, text)
    if not heading:
        raise DirectSourceError('Microsoft transcript heading date or fiscal identity missing')
    try:
        publication = date(int(heading[3]), MONTHS.index(heading[1])+1, int(heading[2]))
    except ValueError as exc:
        raise DirectSourceError('Invalid Microsoft transcript publication date') from exc
    end = fiscal_end(metadata['fiscal_year'], metadata['fiscal_quarter'])
    if metadata['period_end'] != end.isoformat() or not end <= publication <= min(observed.date(), end+timedelta(days=100)):
        raise DirectSourceError('Microsoft transcript publication outside fiscal window')
    resolved = {**provisional, 'published_at': publication.isoformat(),
                'publication_pattern': publication_pattern(publication)}
    return resolved, text, {**parsed, **resolved}
