"""Magna SEC submissions -> issuer 6-K cover -> fiscal transcript exhibit.

Filing dates establish source availability, never fiscal quarter or call date.
All permission is rebuilt from retained official originals; no generic scraper.
"""
import calendar
from datetime import date, timedelta, timezone
import hashlib
import json
import re
from urllib.parse import urljoin

from app.clients.direct_sources import DirectSourceError, parse_issuer_material

KIND = 'magna_sec_transcripts_v1'
CIK = '0000749098'
INDEX_URL = 'https://data.sec.gov/submissions/CIK0000749098.json'
INVESTOR_URL = 'https://www.magna.com/company/investors'
EVENT_FEED = 'issuer_earnings_event'
INDEX_FEED = 'issuer_earnings_index'
MAX_BYTES = 2_000_000
MAX_COVERS = 64
INDEX_MAX_AGE = timedelta(hours=48)
MONTHS = ('January','February','March','April','May','June','July','August','September','October','November','December')
QUARTERS = ('FIRST','SECOND','THIRD','FOURTH')
DATE_PATTERN = r'('+'|'.join(MONTHS)+r')\s+(\d{1,2}),\s*(20\d{2})'


def check_company(company):
    if (company.get('symbol') != 'MGA' or company.get('discovery') != {'kind':KIND,'cik':CIK}
            or company.get('company_website') != 'https://www.magna.com/'
            or company.get('investor_website') != INVESTOR_URL):
        raise DirectSourceError('Unreviewed Magna SEC issuer identity')


def available_day(observed):
    from zoneinfo import ZoneInfo
    if observed.tzinfo is None:
        raise DirectSourceError('SEC transcript observation must be timezone aware')
    return observed.astimezone(ZoneInfo('America/New_York')).date()-timedelta(days=1)


def parse_index(raw, company, observed):
    check_company(company)
    if not raw or len(raw)>MAX_BYTES:
        raise DirectSourceError('SEC transcript directory size invalid')
    try:
        data=json.loads(raw)
        if (str(data.get('cik','')).zfill(10)!=CIK or data.get('tickers',[]).count('MGA')!=1
                or data.get('name')!='MAGNA INTERNATIONAL INC'):
            raise DirectSourceError('SEC transcript directory issuer mismatch')
        recent=data['filings']['recent']
        arrays=[recent[k] for k in ('accessionNumber','form','filingDate','primaryDocument')]
        if any(not isinstance(a,list) for a in arrays) or len({len(a) for a in arrays})!=1 or len(arrays[0])>10000:
            raise DirectSourceError('SEC transcript directory columns invalid')
    except (ValueError,KeyError,TypeError) as exc:
        raise DirectSourceError('SEC transcript directory invalid') from exc
    # Discover same-day covers immediately; publication waits for the completed day.
    available=available_day(observed)+timedelta(days=1);earliest=date(available.year-1,1,1)
    found={};digest=hashlib.sha256(raw).hexdigest()
    for accession,form,filed,filename in zip(*arrays):
        if form not in ('6-K','6-K/A'):
            continue
        try: day=date.fromisoformat(filed)
        except (ValueError,TypeError) as exc:raise DirectSourceError('SEC transcript filing date invalid') from exc
        if day>available:
            raise DirectSourceError('SEC transcript directory contains future filing')
        if day<earliest:
            continue
        if (not isinstance(accession,str) or not re.fullmatch(r'[0-9]{10}-[0-9]{2}-[0-9]{6}',accession)
                or not isinstance(filename,str) or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,180}\.html?',filename)):
            raise DirectSourceError('SEC transcript filing identity invalid')
        key='MGA:sec-transcript-cover:'+accession
        if key in found:
            raise DirectSourceError('Duplicate SEC transcript filing accession')
        found[key]={'key':key,'symbol':'MGA','url':f'https://www.sec.gov/Archives/edgar/data/749098/{accession.replace("-", "")}/{filename}',
            'company_website':company['company_website'],'investor_website':company['investor_website'],
            'cik':CIK,'accession_number':accession,'filing_form':form,'filing_date':filed,
            'discovery_kind':KIND,'discovery_stage':'event',
            'source_page_url':INDEX_URL,'source_page_sha256':digest}
    if len(found)>MAX_COVERS:
        raise DirectSourceError('SEC transcript directory exceeds reviewed coverage bound')
    # Preserve all reviewed-window covers; new evidence is inspected first.
    return sorted(found.values(),key=lambda m:(m['filing_date'],m['accession_number']),reverse=True)


def tree(raw):
    from lxml import html
    if not raw or len(raw)>MAX_BYTES:
        raise DirectSourceError('SEC transcript cover size invalid')
    try:return html.fromstring(raw)
    except (ValueError,html.etree.ParserError) as exc:raise DirectSourceError('SEC transcript cover invalid HTML') from exc


def day_from_match(match):
    try:return date(int(match[3]),MONTHS.index(match[1])+1,int(match[2]))
    except (ValueError,TypeError) as exc:raise DirectSourceError('SEC transcript event date invalid') from exc


def parse_event(raw, metadata, observed):
    if metadata.get('discovery_kind')!=KIND or metadata.get('discovery_stage')!='event' or 'parent_event' in metadata:
        raise DirectSourceError('Unreviewed SEC transcript cover stage')
    root=tree(raw);text=' '.join(root.text_content().split())
    if (not re.search(r'\bMAGNA INTERNATIONAL INC\.',text) or not re.search(r'\bFORM\s+6-K\b',text)
            or not re.search(r'Commission File Number\s+001-11444\b',text)):
        raise DirectSourceError('SEC transcript cover issuer or form mismatch')
    dates=[day_from_match(m) for m in re.finditer(r'Date:\s*'+DATE_PATTERN,text)]
    if len(dates)!=1 or dates[0].isoformat()!=metadata['filing_date'] or dates[0]>available_day(observed):
        raise DirectSourceError('SEC transcript cover filing date mismatch')
    links=[a for a in root.xpath('//a[@href]') if re.search(r'transcript',' '.join(a.text_content().split()),re.I)]
    if not links:
        return []  # Verified cover contains no transcript; not a fabricated period.
    if len(links)!=1:
        raise DirectSourceError('Ambiguous SEC transcript exhibit links')
    link=links[0];label=' '.join(link.text_content().split())
    match=re.fullmatch(r'Q([1-4]) (20\d{2}) Results Webcast Transcript \('+DATE_PATTERN+r'\)',label)
    if not match:
        raise DirectSourceError('Unreviewed SEC transcript exhibit description')
    quarter,year=int(match[1]),int(match[2])
    try:
        event=date(int(match[5]),MONTHS.index(match[3])+1,int(match[4]))
    except ValueError as exc:
        raise DirectSourceError('SEC transcript event date invalid') from exc
    end=date(year,quarter*3,calendar.monthrange(year,quarter*3)[1]);filed=dates[0]
    if not observed.year-1<=year<=observed.year or not end<=event<=min(filed,end+timedelta(days=100)):
        raise DirectSourceError('SEC transcript fiscal period or call date invalid')
    url=urljoin(metadata['url'],link.get('href'));directory=metadata['url'].rsplit('/',1)[0]+'/'
    if not url.startswith(directory) or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,180}\.html?',url[len(directory):]):
        raise DirectSourceError('SEC transcript exhibit leaves its filing')
    period=(rf'(?:MAGNA INTERNATIONAL\s*-\s*{QUARTERS[quarter-1]} QUARTER {year} RESULTS WEBCAST'
            rf'|Magna International\s+Q{quarter} {year} Results)')
    stamp=f'{event.year%100:02d}{MONTHS[event.month-1][:3].upper()}{event.day:02d}'
    numeric=rf'0?{event.month}\s*[\u2013-]\s*0?{event.day}\s*-\s*{event.year}'
    return [{**metadata,'key':f'MGA:earnings_transcript:{year}:Q{quarter}',
        'url':url,'document_type':'earnings_transcript','fiscal_year':year,'fiscal_quarter':quarter,
        'period_end':end.isoformat(),'event_date':event.isoformat(),'published_at':filed.isoformat(),
        'source_publication_basis':'SEC filing date; webcast date verified separately; no intraday timestamp',
        'source_format':'html','discovery_stage':'transcript','parent_event':dict(metadata),
        'source_page_url':metadata['url'],'source_page_sha256':hashlib.sha256(raw).hexdigest(),
        'period_pattern':period,'company_pattern':r'\bMAGNA INTERNATIONAL\b',
        'publication_pattern':rf'(?:{re.escape(stamp)}|{numeric})','transcript_pattern':r'This transcript is derived from a recording of the webcast',
        'qa_pattern':r'\b(?:QUESTION\s*&\s*ANSWER|QUESTIONS)\s*:',
        'transcript_publisher':'Magna International Inc. (SEC-filed webcast transcript)'}]


def parse_candidate(raw, metadata, observed):
    if metadata.get('discovery_kind')!=KIND or metadata.get('discovery_stage')!='transcript':
        raise DirectSourceError('Unreviewed SEC transcript stage')
    text,parsed=parse_issuer_material(raw,metadata)
    # EDGAR may retain its SGML document envelope before the HTML body.
    filename=re.escape(metadata['url'].rsplit('/',1)[1])
    envelope=rf'(?:EX-99\.3\s+[0-9]+\s+{filename}\s+EXHIBIT 99\.3\s+)?'
    prefix=r'^'+envelope+r'(?:Exhibit\s+99\.[0-9]+\s+)?'
    heading=(prefix+r'(?:'+metadata['period_pattern']+r'\s*[\u2013\u2014-]\s*'+metadata['publication_pattern']
             +r'|TRANSCRIPT\s+'+metadata['publication_pattern']+r'\s+'+metadata['period_pattern']+r')\b')
    qa=metadata['qa_pattern']+r'.{0,500}\bOperator\^.{0,250}\bfirst question comes from\b'
    if (not re.search(heading,text,re.I) or not re.search(qa,text,re.I)
            or not re.search(r'Operator\^.{0,160}\b(?:does conclude today[\u2019\u0027]s conference call|This concludes today[\u2019\u0027]s call)\b',text,re.I)
            or not re.search(r'transcription may still contain inaccuracies, errors, or omissions',text,re.I)
            or date.fromisoformat(metadata['published_at'])>available_day(observed)):
        raise DirectSourceError('SEC transcript heading, date, Q&A, ending or disclaimer mismatch')
    return dict(metadata),text,parsed


def verified_transcript_metadata(db,company,metadata,verify_parent):
    from sqlalchemy import select
    from app.services.direct_feed_store import DirectFeedDocument,DirectFeedRevision
    parent=metadata.get('parent_event')
    if not isinstance(parent,dict) or parent.get('discovery_stage')!='event' or 'parent_event' in parent:
        raise DirectSourceError('Invalid SEC transcript cover evidence chain')
    parent=verify_parent(db,company,parent)
    if parent['accession_number']!=metadata.get('accession_number') or parent['url']!=metadata.get('source_page_url'):
        raise DirectSourceError('SEC transcript cover chain identity changed')
    row=db.scalar(select(DirectFeedDocument).where(DirectFeedDocument.feed==EVENT_FEED,DirectFeedDocument.source_key==parent['key']))
    if row is None or row.source_url!=parent['url']:
        raise DirectSourceError('SEC transcript cover evidence missing')
    revision=db.scalar(select(DirectFeedRevision).where(DirectFeedRevision.document_id==row.id,DirectFeedRevision.content_hash==metadata.get('source_page_sha256')))
    if not revision or not revision.source_bytes or hashlib.sha256(revision.source_bytes).hexdigest()!=metadata.get('source_page_sha256'):
        raise DirectSourceError('SEC transcript cover checksum mismatch')
    observed=revision.fetched_at.replace(tzinfo=timezone.utc) if revision.fetched_at.tzinfo is None else revision.fetched_at
    expected=parse_event(revision.source_bytes,parent,observed)
    if expected!=[metadata]:
        raise DirectSourceError('SEC transcript metadata changed from cover evidence')
    return dict(metadata)


def latest_metadata(db,company,*,observed):
    """A new uninspected cover or stale directory cannot silently serve old text."""
    from sqlalchemy import select, tuple_
    from app.services.direct_feed_store import DirectFeedDocument,DirectFeedRevision
    from app.services.issuer_discovery import index_metadata
    hold=lambda reason:{'preparation_hold':reason}
    check_company(company)
    if observed.tzinfo is None:raise DirectSourceError('SEC transcript observation must be timezone aware')
    index=db.scalar(select(DirectFeedDocument).where(DirectFeedDocument.feed==INDEX_FEED,DirectFeedDocument.source_key==index_metadata(company,None)['key']))
    if index is None or index.status!='parsed':return hold('sec_transcript_directory_not_prepared')
    if index.source_url!=INDEX_URL:raise DirectSourceError('SEC transcript directory URL changed')
    revision=db.scalar(select(DirectFeedRevision).where(DirectFeedRevision.document_id==index.id,DirectFeedRevision.content_hash==index.content_hash))
    if not revision or not revision.source_bytes or hashlib.sha256(revision.source_bytes).hexdigest()!=index.content_hash:
        raise DirectSourceError('SEC transcript directory checksum mismatch')
    checked=index.checked_at
    if checked is None:return hold('sec_transcript_directory_not_checked')
    if checked.tzinfo is None:checked=checked.replace(tzinfo=timezone.utc)
    if checked>observed or observed-checked>INDEX_MAX_AGE:return hold('sec_transcript_directory_stale_or_future')
    first=revision.fetched_at
    if first.tzinfo is None:first=first.replace(tzinfo=timezone.utc)
    if first>observed:return hold('source_not_yet_observed')
    original_covers=parse_index(revision.source_bytes,company,first)
    if json.loads(revision.parsed_json)!={'candidates':original_covers}:
        raise DirectSourceError('SEC transcript directory parse changed')
    covers=parse_index(revision.source_bytes,company,checked)
    if json.loads(index.parsed_json)!={'candidates':covers}:
        raise DirectSourceError('SEC transcript current directory parse changed')
    # Batch the bounded cohort and its exact immutable proofs. No per-cover
    # database query or repeated parsing of the same submissions original.
    rows={r.source_key:r for r in db.scalars(select(DirectFeedDocument).where(
        DirectFeedDocument.feed==EVENT_FEED,
        DirectFeedDocument.source_key.in_([m['key'] for m in covers])))}
    parsed_rows=[r for r in rows.values() if r.status=='parsed']
    metas={r.id:json.loads(r.metadata_json) for r in parsed_rows}
    pairs=[(r.id,r.content_hash) for r in parsed_rows]
    pairs.extend((index.id,m.get('source_page_sha256')) for m in metas.values())
    revisions={(r.document_id,r.content_hash):r for r in db.scalars(select(DirectFeedRevision).where(
        tuple_(DirectFeedRevision.document_id,DirectFeedRevision.content_hash).in_(pairs)))} if pairs else {}
    directories={revision.content_hash:{m['key']:m for m in original_covers}}
    pending=[];candidates=[]
    for current in covers:
        row=rows.get(current['key'])
        if row is None or row.status!='parsed':
            pending.append(current['filing_date']);continue
        meta=metas[row.id];digest=meta.get('source_page_sha256')
        if digest not in directories:
            proof=revisions.get((index.id,digest))
            if not proof or not proof.source_bytes or hashlib.sha256(proof.source_bytes).hexdigest()!=digest:
                raise DirectSourceError('Issuer index checksum mismatch')
            timestamp=proof.fetched_at.replace(tzinfo=timezone.utc) if proof.fetched_at.tzinfo is None else proof.fetched_at
            if timestamp>observed:raise DirectSourceError('Issuer index proof not yet observed')
            original=parse_index(proof.source_bytes,company,timestamp)
            if json.loads(proof.parsed_json)!={'candidates':original}:
                raise DirectSourceError('SEC transcript original directory parse changed')
            directories[digest]={m['key']:m for m in original}
        if directories[digest].get(meta.get('key'))!=meta:
            raise DirectSourceError('SEC transcript cover original metadata changed')
        stable=lambda m:{k:v for k,v in m.items() if k!='source_page_sha256'}
        if stable(meta)!=stable(current) or row.source_url!=current['url']:
            raise DirectSourceError('SEC transcript cover differs from latest directory')
        rev=revisions.get((row.id,row.content_hash))
        if not rev or not rev.source_bytes or hashlib.sha256(rev.source_bytes).hexdigest()!=row.content_hash:
            raise DirectSourceError('SEC transcript cover checksum mismatch')
        fetched=rev.fetched_at.replace(tzinfo=timezone.utc) if rev.fetched_at.tzinfo is None else rev.fetched_at
        if fetched>observed:pending.append(current['filing_date']);continue
        found=parse_event(rev.source_bytes,meta,fetched)
        if json.loads(rev.parsed_json)!={'candidates':found} or json.loads(row.parsed_json)!={'candidates':found}:
            raise DirectSourceError('SEC transcript cover parse changed')
        candidates.extend(found)
    if not candidates:return hold('sec_transcript_not_discovered')
    newest=max((m['fiscal_year'],m['fiscal_quarter']) for m in candidates)
    selected=[m for m in candidates if (m['fiscal_year'],m['fiscal_quarter'])==newest]
    if len(selected)!=1:return hold('sec_transcript_period_requires_reconciliation')
    latest=selected[0]
    if any(filed>=latest['filing_date'] for filed in pending):return hold('sec_transcript_newer_filing_not_reviewed')
    return latest
