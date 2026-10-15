"""Unilever's official latest-results page and complete webcast transcript."""
import calendar
from datetime import date, timedelta
import hashlib
import json
import re
from urllib.parse import urljoin

from app.clients.direct_sources import DirectSourceError, parse_issuer_material

KIND = 'unilever_results_v1'
INDEX_URL = 'https://www.unilever.com/investors/results-events/results-events-webcasts/latest-results/'
MONTHS = ('January','February','March','April','May','June','July','August','September','October','November','December')
QA = r'\bJEMMA\s+SPALTON\s*:?\s+.{0,350}?\bour first question comes from\b'


def publication_pattern(day):
    return rf'\b{day.day}(?:st|nd|rd|th)?\s+{MONTHS[day.month-1]}\s+{day.year}\b'


def period_pattern(year, quarter):
    if quarter == 2:
        return rf'Q2\s*&\s*H\s*1\s+{year}\s*Results'
    if quarter == 4:
        return rf'Q4\s*&\s*(?:Full\s+Year|FY)\s+{year}\s*Results'
    return rf'Q{quarter}\s+{year}\s+Trading\s+Statement'


def parse_index(raw, company, observed):
    from lxml import html
    if (company.get('symbol') != 'UL' or company.get('discovery',{}).get('kind') != KIND
            or company.get('company_website') != 'https://www.unilever.com/'
            or company.get('investor_website') != 'https://www.unilever.com/investors/'):
        raise DirectSourceError('Unreviewed Unilever discovery identity')
    if not raw or len(raw) > 2_000_000:
        raise DirectSourceError('Unilever index size invalid')
    tree = html.fromstring(raw)
    blocks = tree.xpath('//section[contains(concat(" ",normalize-space(@class)," ")," uco-c-focal-section ")]')
    candidates = []
    for block in blocks:
        headings = [' '.join(n.text_content().split()) for n in block.xpath('.//h2')]
        if len(headings) != 1:
            continue
        match = re.fullmatch(r'Q([1-4])(?: and (Half-Year|Full-Year))? (20\d{2}) (Results|Trading Statement)', headings[0])
        if not match:
            continue
        quarter, year = int(match[1]), int(match[3])
        if (match[2], match[4]) != {1:(None,'Trading Statement'),2:('Half-Year','Results'),3:(None,'Trading Statement'),4:('Full-Year','Results')}[quarter]:
            raise DirectSourceError('Unilever heading period mismatch')
        if not observed.year-1 <= year <= observed.year:
            raise DirectSourceError('Unilever index fiscal year outside current window')
        end = date(year, quarter*3, calendar.monthrange(year, quarter*3)[1])
        release_label=headings[0].replace(' '+str(year), '', 1)
        dates=[]
        for node in block.xpath('.//p'):
            text=' '.join(node.text_content().split())
            m=re.fullmatch(r'Unilever released its '+re.escape(release_label)+r' on (\d{1,2}) ('+'|'.join(MONTHS)+r') (20\d{2})\.',text)
            if m:
                try: dates.append(date(int(m[3]),MONTHS.index(m[2])+1,int(m[1])))
                except ValueError as exc: raise DirectSourceError('Invalid Unilever publication date') from exc
        if len(dates)!=1 or not end <= dates[0] <= min(observed.date(),end+timedelta(days=100)):
            raise DirectSourceError('Unilever publication outside completed fiscal window')
        links=[n for n in block.xpath('.//a[@href]') if 'transcript' in n.text_content().lower()]
        if len(links)!=1:
            raise DirectSourceError('Unilever current quarter transcript missing or ambiguous')
        link=links[0]
        label=' '.join(link.text_content().split())
        if not re.fullmatch(rf'Q{quarter} {year} (?:Video )?Transcript(?: \(PDF [\d.]+ [KM]B\))?',label):
            raise DirectSourceError('Unilever transcript label fiscal mismatch')
        url=urljoin(INDEX_URL,link.get('href'))
        allowed={f'https://www.unilever.com/files/unilever-q{quarter}-{year}-transcript.pdf'}
        if quarter==4: allowed.add(f'https://www.unilever.com/files/q4-{year}-video-transcript.pdf')
        if url not in allowed:
            raise DirectSourceError('Unreviewed Unilever transcript URL')
        candidates.append({'key':f'UL:earnings_transcript:{year}:Q{quarter}', 'symbol':'UL',
            'document_type':'earnings_transcript','fiscal_year':year,'fiscal_quarter':quarter,
            'url':url,'source_format':'pdf','company_website':company['company_website'],
            'investor_website':company['investor_website'],'discovery_kind':KIND,
            'source_page_url':INDEX_URL,'source_page_sha256':hashlib.sha256(raw).hexdigest(),
            'period_end':end.isoformat(),'published_at':dates[0].isoformat(),
            'publication_pattern':publication_pattern(dates[0]),'company_pattern':r'\bUnilever\b',
            'publisher_pattern':r'\bUnilever\b','transcript_publisher':'Issuer-hosted transcript; publisher not identified',
            'period_pattern':period_pattern(year,quarter),
            'transcript_pattern':r'\b(?:Chief Executive Officer|FERNANDO\s+FERNANDEZ)\b',
            'qa_pattern':QA})
    if len(candidates)!=1:
        raise DirectSourceError('Unilever latest-results period missing or ambiguous')
    return candidates


def parse_candidate(raw, metadata, observed):
    if metadata.get('symbol')!='UL' or metadata.get('discovery_kind')!=KIND:
        raise DirectSourceError('Unreviewed Unilever transcript identity')
    text,parsed=parse_issuer_material(raw,metadata)
    # Date must occur in the opening call heading, not a later quoted event.
    heading=rf'^(?:Unilever\s+)?{metadata["period_pattern"]}.{{0,150}}?London,\s*.{{0,80}}?{metadata["publication_pattern"]}'
    if not re.search(heading,text,re.I) or date.fromisoformat(metadata['published_at'])>observed.date():
        raise DirectSourceError('Unilever PDF call heading or date mismatch')
    return dict(metadata),text,parsed


def latest_metadata(db, company, observed):
    """Failed/stale latest index and pending new periods cannot expose old results."""
    from sqlalchemy import select
    from app.services.direct_feed_store import DirectFeedDocument, DirectFeedRevision
    from app.services.issuer_discovery import INDEX_FEED, verified_metadata
    hold=lambda reason: {'preparation_hold':reason}
    row=db.scalar(select(DirectFeedDocument).where(DirectFeedDocument.feed==INDEX_FEED,
        DirectFeedDocument.source_key=='UL:reports:latest'))
    if row is None or row.status!='parsed':
        return hold('latest_issuer_index_not_verified')
    revision=db.scalar(select(DirectFeedRevision).where(DirectFeedRevision.document_id==row.id,
        DirectFeedRevision.content_hash==row.content_hash))
    if revision is None or not revision.source_bytes or hashlib.sha256(revision.source_bytes).hexdigest()!=row.content_hash:
        raise DirectSourceError('Unilever latest index checksum mismatch')
    fetched=revision.fetched_at
    if fetched.tzinfo is None: fetched=fetched.replace(tzinfo=observed.tzinfo)
    checked=row.checked_at
    if checked is not None and checked.tzinfo is None: checked=checked.replace(tzinfo=observed.tzinfo)
    if checked is None or fetched>observed or checked>observed or observed-checked>timedelta(hours=48):
        return hold('latest_issuer_index_not_fresh')
    metadata=parse_index(revision.source_bytes,company,fetched)[0]
    verified_metadata(db,company,metadata)
    document=db.scalar(select(DirectFeedDocument).where(DirectFeedDocument.feed=='issuer_earnings',
        DirectFeedDocument.source_key==metadata['key']))
    if document is not None:
        original=json.loads(document.metadata_json)
        comparable=lambda m: {k:v for k,v in m.items() if k!='source_page_sha256'}
        if comparable(original)==comparable(metadata):
            verified_metadata(db,company,original)
            return original
    return metadata
