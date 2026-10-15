"""Read Moderna's public literal event data without executing site JavaScript."""
import calendar
from datetime import date, timedelta
import hashlib
import json
import re

from app.clients.direct_sources import DirectSourceError, parse_issuer_material

KIND = 'moderna_events_v1'
INDEX_URL = 'https://investors.modernatx.com/quarterly-results'
MONTHS = ('January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December')
ORDINALS = ('First', 'Second', 'Third', 'Fourth')
TOKEN = re.compile(r'\s+|//[^\r\n]*|/\*[\s\S]*?\*/|"(?:\\.|[^"\\])*"|[A-Za-z_$][\w$]*|[][{}:=,;]')
PREFIX = re.compile(r'\s*const\s+eventsMRNA\s*=\s*\[')


def literal_events(script):
    """Only an array of flat string-valued objects; comments are not data."""
    if len(script) > 1_000_000:
        raise DirectSourceError('Moderna event data exceeds bound')
    tokens, pos = [], 0
    while pos < len(script):
        match = TOKEN.match(script, pos)
        if match is None:
            raise DirectSourceError('Unsupported Moderna event syntax')
        value, pos = match.group(), match.end()
        if value.isspace() or value.startswith(('//', '/*')):
            continue
        tokens.append(value)
        if len(tokens) > 30_000:
            raise DirectSourceError('Moderna event token bound exceeded')
    if tokens[:4] != ['const', 'eventsMRNA', '=', '[']:
        raise DirectSourceError('Missing Moderna literal event array')
    pos, events = 4, []

    def take(expected):
        nonlocal pos
        if pos >= len(tokens) or tokens[pos] != expected:
            raise DirectSourceError('Invalid Moderna literal structure')
        pos += 1

    def peek():
        return tokens[pos] if pos < len(tokens) else None

    while peek() != ']':
        take('{'); event = {}
        while peek() != '}':
            key = peek()
            if key is None or not re.fullmatch(r'[A-Za-z_$][\w$]*', key) or key in event:
                raise DirectSourceError('Invalid or duplicate Moderna event field')
            pos += 1; take(':')
            value = peek()
            if value is None or not value.startswith('"'):
                raise DirectSourceError('Moderna event values must be literal strings')
            try:
                event[key] = json.loads(value)
            except ValueError as exc:
                raise DirectSourceError('Invalid Moderna literal string') from exc
            pos += 1
            if len(event) > 30:
                raise DirectSourceError('Moderna event field bound exceeded')
            if peek() == '}':
                break
            take(',')
        take('}'); events.append(event)
        if len(events) > 300:
            raise DirectSourceError('Moderna event count exceeds bound')
        if peek() == ']':
            break
        take(',')
    take(']')
    if peek() == ';':
        take(';')
    if pos != len(tokens):
        raise DirectSourceError('Unexpected code after Moderna event data')
    return events


def publication_pattern(day):
    return rf'{MONTHS[day.month-1]}\s+0?{day.day},\s*{day.year}'


def parse_index(raw, company, observed):
    from lxml import html
    if (company.get('symbol') != 'MRNA' or company.get('discovery', {}).get('kind') != KIND
            or company.get('investor_website') != 'https://investors.modernatx.com/'):
        raise DirectSourceError('Unreviewed Moderna discovery identity')
    if not raw or len(raw) > 2_000_000:
        raise DirectSourceError('Moderna index size invalid')
    scripts = [n.text for n in html.fromstring(raw).xpath('//script[not(@src)]') if PREFIX.match(n.text or '')]
    if len(scripts) != 1:
        raise DirectSourceError('Missing or ambiguous Moderna event array')
    found = {}
    for event in literal_events(scripts[0]):
        match = re.fullmatch(r'Q([1-4]) (20\d{2})', event.get('quarterlyResults', ''))
        if not match or not event.get('transcriptURL'):
            continue
        quarter, year = map(int, match.groups())
        if not observed.year-1 <= year <= observed.year:
            continue
        expected_titles = (f'Moderna {ORDINALS[quarter-1]} Quarter {year} Conference Call', f'Moderna {quarter}Q {year} Earnings Call')
        if event.get('event') not in expected_titles:
            raise DirectSourceError('Moderna event fiscal title mismatch')
        stamp = re.fullmatch(r'('+'|'.join(MONTHS)+r') (\d{1,2}), (20\d{2})', event.get('date', ''))
        try:
            if not stamp:
                raise ValueError('missing date')
            day = date(int(stamp[3]), MONTHS.index(stamp[1])+1, int(stamp[2]))
        except ValueError as exc:
            raise DirectSourceError('Invalid Moderna event date') from exc
        end = date(year, quarter*3, calendar.monthrange(year, quarter*3)[1])
        if not end <= day <= min(observed.date(), end+timedelta(days=100)):
            raise DirectSourceError('Moderna event outside completed fiscal window')
        url = event['transcriptURL']
        allowed = (r'https://irp\.cdn-website\.com/(?:89dd0539|0a71bb4f)/files/uploaded/'
                   +re.escape('MRNA-USQ_Transcript_'+day.isoformat())+r'(?:-[1-9]\d?|\+%28[1-9]\d?%29)?\.pdf')
        if not re.fullmatch(allowed, url):
            raise DirectSourceError('Unreviewed Moderna transcript link')
        key = f'MRNA:earnings_transcript:{year}:Q{quarter}'
        if key in found:
            raise DirectSourceError('Duplicate Moderna fiscal period')
        # Captured 2025-call originals use Refinitiv; 2026-call originals use
        # LSEG, including fiscal2025Q4. The PDF must independently agree.
        publisher = 'Refinitiv' if day.year <= 2025 else 'LSEG'
        found[key] = {'key': key, 'symbol': 'MRNA', 'document_type': 'earnings_transcript',
            'fiscal_year': year, 'fiscal_quarter': quarter, 'url': url, 'source_format': 'pdf',
            'company_website': company['company_website'], 'investor_website': company['investor_website'],
            'company_pattern': r'\bModerna\b', 'period_pattern': rf'\bQ{quarter} {year} Moderna Inc Earnings Call\b',
            'publisher_pattern': publisher+r'\s+STREETEVENTS', 'transcript_publisher': publisher+' StreetEvents',
            'transcript_pattern': r'\bEDITED TRANSCRIPT\b', 'qa_pattern': r'\bQUESTIONS AND ANSWERS\s+Operator\b',
            'published_at': day.isoformat(), 'publication_pattern': publication_pattern(day),
            'source_page_url': INDEX_URL, 'source_page_sha256': hashlib.sha256(raw).hexdigest(),
            'discovery_kind': KIND, 'period_end': end.isoformat()}
    if not found or len(found) > 8:
        raise DirectSourceError('Moderna index has no bounded completed transcript links')
    return sorted(found.values(), key=lambda m: (m['fiscal_year'], m['fiscal_quarter']))


def parse_candidate(raw, metadata, observed):
    text, parsed = parse_issuer_material(raw, metadata)
    day = date.fromisoformat(metadata['published_at'])
    heading = (r'^'+metadata['publisher_pattern']+r'\s+EDITED TRANSCRIPT\s+MRNA\.OQ\s+-\s+'
               +metadata['period_pattern'].removeprefix(r'\b').removesuffix(r'\b')
               +r'\s+EVENT DATE/TIME:\s+'+publication_pattern(day)+r'\s*/')
    if not re.search(heading, text, re.I) or day > observed.date():
        raise DirectSourceError('Moderna transcript heading identity or date mismatch')
    return dict(metadata), text, parsed
