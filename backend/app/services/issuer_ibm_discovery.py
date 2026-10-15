"""IBM completed-quarter index -> reviewed event page -> exact transcript PDF."""
import calendar
from datetime import date, timedelta
import hashlib
import re

from app.clients.direct_sources import DirectSourceError, parse_issuer_material

KIND = 'ibm_events_v1'
INDEX_URL = 'https://www.ibm.com/investor'
EVENT_FEED = 'issuer_earnings_event'
MONTHS = ('January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December')
DATE = r'('+'|'.join(MONTHS)+r') (\d{1,2}), (20\d{2})'


def tree(raw):
    from lxml import html
    if not raw or len(raw) > 2_000_000:
        raise DirectSourceError('IBM page size invalid')
    return html.fromstring(raw)


def parse_index(raw, company, observed):
    if (company.get('symbol') != 'IBM' or company.get('discovery', {}).get('kind') != KIND
            or company.get('investor_website') != 'https://www.ibm.com/investor/'):
        raise DirectSourceError('Unreviewed IBM discovery identity')
    found = {}
    # Only completed-quarter tiles, never the separate upcoming event banner.
    for block in tree(raw).xpath('//c4d-cta-block-item'):
        label = re.match(r'^(20\d{2}) Q([1-4])\b', ' '.join(block.text_content().split()))
        if not label:
            continue
        year, quarter = map(int, label.groups())
        if not observed.year-1 <= year <= observed.year:
            continue
        urls = {n.get('href') for n in block.xpath('.//a[@href]') if ' '.join(n.text_content().split()) == 'Earnings announcement'}
        expected = f'https://www.ibm.com/investor/events/earnings-{quarter}q{str(year)[2:]}'
        if urls != {expected}:
            raise DirectSourceError('IBM completed-quarter event link mismatch')
        end = date(year, quarter*3, calendar.monthrange(year, quarter*3)[1])
        if end > observed.date():
            raise DirectSourceError('IBM completed quarter is in the future')
        key = f'IBM:earnings_transcript:{year}:Q{quarter}'
        if key in found:
            raise DirectSourceError('Duplicate IBM completed quarter')
        found[key] = {'key': key, 'symbol': 'IBM', 'document_type': 'earnings_transcript',
            'fiscal_year': year, 'fiscal_quarter': quarter, 'url': expected,
            'company_website': company['company_website'], 'investor_website': company['investor_website'],
            'discovery_kind': KIND, 'discovery_stage': 'event', 'period_end': end.isoformat(),
            'source_page_url': INDEX_URL, 'source_page_sha256': hashlib.sha256(raw).hexdigest()}
    if not found or len(found) > 8:
        raise DirectSourceError('IBM index has no bounded completed-quarter links')
    return sorted(found.values(), key=lambda m: (m['fiscal_year'], m['fiscal_quarter']))


def publication_pattern(day):
    return rf'{MONTHS[day.month-1]}\s+{day.day},\s*{day.year}'


def parse_event(raw, metadata, observed):
    if metadata.get('discovery_kind') != KIND or metadata.get('discovery_stage') != 'event' or 'parent_event' in metadata:
        raise DirectSourceError('Unreviewed IBM event stage')
    root = tree(raw);year, quarter = metadata['fiscal_year'], metadata['fiscal_quarter']
    heading = [' '.join(n.text_content().split()) for n in root.xpath('//h1')]
    if heading != [f'IBM {quarter}Q {year} Earnings Announcement']:
        raise DirectSourceError('IBM event heading fiscal identity mismatch')
    dates = []
    for node in root.xpath('//p'):
        text = ' '.join(node.text_content().split())
        match = re.fullmatch(DATE+r'(?:, 5:00PM ET| \| 5pm ET)?', text)
        if match:
            try:
                dates.append(date(int(match[3]), MONTHS.index(match[1])+1, int(match[2])))
            except ValueError as exc:
                raise DirectSourceError('Invalid IBM event date') from exc
    if len(dates) != 1:
        raise DirectSourceError('Missing or ambiguous IBM event date')
    day = dates[0];end = date(year, quarter*3, calendar.monthrange(year, quarter*3)[1])
    if metadata['period_end'] != end.isoformat() or not end <= day <= min(observed.date(), end+timedelta(days=100)):
        raise DirectSourceError('IBM event outside completed fiscal window')
    urls = {n.get('href') for n in root.xpath('//*[@href]') if ' '.join(n.text_content().split()) == 'Webcast transcript'}
    if len(urls) != 1:
        raise DirectSourceError('IBM event has no single webcast transcript')
    url = urls.pop()
    if not re.fullmatch(r'https://www\.ibm\.com/downloads/documents/us-en/[0-9a-f]{16}', url):
        raise DirectSourceError('Unreviewed IBM transcript download')
    return {**metadata, 'url': url, 'source_format': 'pdf', 'discovery_stage': 'pdf',
        'parent_event': dict(metadata), 'source_page_url': metadata['url'],
        'source_page_sha256': hashlib.sha256(raw).hexdigest(), 'published_at': day.isoformat(),
        'publication_pattern': publication_pattern(day), 'company_pattern': r'\bIBM\b',
        'publisher_pattern': r'\bIBM\b', 'transcript_publisher': 'IBM',
        'period_pattern': rf'IBM {quarter}Q {year} Earnings Conference Call Transcript',
        'transcript_pattern': r'Earnings Conference Call Transcript',
        'qa_pattern': r'\bQUESTION AND ANSWER(?: SECTION)?\b'}


def parse_candidate(raw, metadata, observed):
    if metadata.get('discovery_kind') != KIND or metadata.get('discovery_stage') != 'pdf':
        raise DirectSourceError('Unreviewed IBM transcript stage')
    text, parsed = parse_issuer_material(raw, metadata)
    year, quarter = metadata['fiscal_year'], metadata['fiscal_quarter']
    day = date.fromisoformat(metadata['published_at'])
    heading = (rf'^IBM {quarter}Q{str(year)[2:]} Earnings Transcript\s+(?:1\s+)?'
               +re.escape(metadata['period_pattern'])+r'\s+IBM Participants:.{1,1000}?'
               +publication_pattern(day)+r'\s*/')
    qa = r'\bQUESTION AND ANSWER(?: SECTION)?\b.{0,2200}\bOperator:\s+.{0,650}\bfirst question\b'
    if not re.search(heading, text) or not re.search(qa, text, re.I) or day > observed.date():
        raise DirectSourceError('IBM PDF heading, publication or actual Q&A mismatch')
    return dict(metadata), text, parsed


def verified_pdf_metadata(db, company, metadata, verify_parent):
    """One explicit extra hop, both immutable originals verified independently."""
    from sqlalchemy import select
    from app.services.direct_feed_store import DirectFeedDocument, DirectFeedRevision, utcnow
    parent = metadata.get('parent_event')
    if not isinstance(parent, dict) or parent.get('discovery_stage') != 'event' or 'parent_event' in parent:
        raise DirectSourceError('Invalid IBM event evidence chain')
    parent = verify_parent(db, company, parent)
    if parent['key'] != metadata.get('key') or parent['url'] != metadata.get('source_page_url'):
        raise DirectSourceError('IBM event chain identity changed')
    row = db.scalar(select(DirectFeedDocument).where(DirectFeedDocument.feed == EVENT_FEED, DirectFeedDocument.source_key == parent['key']))
    if row is None or row.source_url != parent['url']:
        raise DirectSourceError('IBM event evidence missing')
    revision = db.scalar(select(DirectFeedRevision).where(DirectFeedRevision.document_id == row.id, DirectFeedRevision.content_hash == metadata.get('source_page_sha256')))
    if revision is None or not revision.source_bytes or hashlib.sha256(revision.source_bytes).hexdigest() != metadata.get('source_page_sha256'):
        raise DirectSourceError('IBM event checksum mismatch')
    observed = revision.fetched_at
    if observed.tzinfo is None:
        observed = observed.replace(tzinfo=utcnow().tzinfo)
    expected = parse_event(revision.source_bytes, parent, observed)
    if expected != metadata:
        raise DirectSourceError('IBM transcript metadata changed from event evidence')
    return expected
