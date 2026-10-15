"""Bounded discovery from reviewed issuer indexes, with pinned source evidence."""
from datetime import date, timedelta
import calendar
import hashlib
import json
import re

from sqlalchemy import select

from app.clients.direct_sources import DirectSourceError, parse_issuer_material
from app.services.direct_feed_store import (
    DirectFeedDocument, DirectFeedRevision, discover, dumps, record_document, utcnow,
)

KIND = 'reddit_q4_v1'
MSFT_KIND = 'microsoft_events_v1'
PDF_KINDS = {'META': 'meta_q4_pdf_v1', 'SOFI': 'sofi_q4_pdf_v1'}
INDEX_FEED = 'issuer_earnings_index'
QUARTERS = {'First Quarter': 1, 'Second Quarter': 2, 'Third Quarter': 3, 'Fourth Quarter': 4}
DATE_PATTERN = r'\b\d{2}\s*-\s*\d{2}\s*-\s*\d{4}\b'
PARSER_POLICY = 3
QA_PATTERNS = {
    # Version 1 remains byte-for-byte compatible with stored publication proofs.
    1: r'Operator\s*:\s*(?:(?:\(Operator Instructions\)\s+Our first)|First)\s+question comes from\b',
    2: r'Operator\s*:\s*(?:(?:\(Operator Instructions\)\s+(?:Our|Your) first)|First)\s+question comes from\b',
    3: r'Operator\s*:\s*(?:\(Operator Instructions\)\s+)?(?:(?:Our|Your)\s+)?(?:first|\ufb01rst)\s+question comes from\b',
}


def parser_policy(metadata):
    version = metadata.get('parser_policy', 1)
    if type(version) is not int or version not in QA_PATTERNS:
        raise DirectSourceError('Unreviewed issuer parser policy')
    return version


def format_policy(year, quarter, policy):
    parser_policy({'parser_policy': policy})
    period = rf"Reddit Q{quarter}['\u2019]{str(year)[2:]} Earnings Call"
    if policy >= 3:
        label = next(label for label, number in QUARTERS.items() if number == quarter)
        period = (rf"\bReddit(?:,?\s+Inc\.?)?\s+"
                  rf"(?:Q{quarter}['\u2019]{str(year)[2:]}|{label}\s+{year})\s+Earnings(?:\s+Call)?\b")
    return {'period_pattern': period, 'qa_pattern': QA_PATTERNS[policy],
            **({'parser_policy': policy} if policy != 1 else {})}


def enabled_company(company):
    return (company.get('symbol'), company.get('discovery', {}).get('kind')) in {
        ('RDDT', KIND), ('MSFT', MSFT_KIND), *PDF_KINDS.items()}


def index_url(company, year):
    if enabled_company(company) and company['symbol'] in PDF_KINDS:
        from app.services.issuer_q4_pdf_discovery import index_url as pdf_index_url
        return pdf_index_url(company, year)
    if enabled_company(company) and company['symbol'] == 'MSFT':
        from app.services.issuer_microsoft_discovery import INDEX_URL
        if year is not None:
            raise DirectSourceError('Microsoft index does not use calendar years')
        return INDEX_URL
    if not enabled_company(company) or type(year) is not int or not 2000 <= year <= 2100:
        raise DirectSourceError('Unreviewed issuer discovery policy')
    sources = {d.get('source_page_url', '') for d in company.get('documents', [])}
    sources = {s for s in sources if s.startswith('https://investor.redditinc.com/feed/FinancialReport.svc/GetFinancialReportList?')}
    if len(sources) != 1:
        raise DirectSourceError('Ambiguous reviewed issuer index')
    url, count = re.subn(r'([?&]year=)\d{4}(?=&|$)', lambda m: m[1]+str(year), sources.pop())
    if count != 1:
        raise DirectSourceError('Invalid reviewed index year')
    return url


def index_metadata(company, year):
    if enabled_company(company) and company['symbol'] == 'MSFT':
        return {'key': 'MSFT:reports:latest', 'url': index_url(company, year),
                'symbol': 'MSFT', 'discovery_kind': MSFT_KIND}
    return {'key': f"{company['symbol']}:reports:{year}", 'url': index_url(company, year),
            'symbol': company['symbol'], 'year': year, 'discovery_kind': company['discovery']['kind']}


def parse_index(raw, company, year, observed, *, policy=PARSER_POLICY):
    if enabled_company(company) and company['symbol'] in PDF_KINDS:
        from app.services.issuer_q4_pdf_discovery import parse_index as parse_pdf_index
        return parse_pdf_index(raw, company, year, observed)
    if enabled_company(company) and company['symbol'] == 'MSFT':
        from app.services.issuer_microsoft_discovery import parse_index as parse_msft_index
        if year is not None:
            raise DirectSourceError('Microsoft index does not use calendar years')
        return parse_msft_index(raw, company, observed)
    parser_policy({'parser_policy': policy})
    if not raw or len(raw) > 2_000_000:
        raise DirectSourceError('Issuer index size invalid')
    try:
        reports = json.loads(raw)['GetFinancialReportListResult']
    except (ValueError, KeyError, TypeError) as exc:
        raise DirectSourceError('Invalid issuer financial-report index') from exc
    if not isinstance(reports, list) or len(reports) > 10:
        raise DirectSourceError('Issuer index report bound exceeded')
    found = {}
    for report in reports:
        quarter = QUARTERS.get(report.get('ReportSubType'))
        if (report.get('ReportYear') != year or not quarter or report.get('LanguageId') != 1
                or report.get('ReportTitle') != f"{report['ReportSubType']} {year}"):
            raise DirectSourceError('Issuer index fiscal identity mismatch')
        end = date(year, quarter*3, calendar.monthrange(year, quarter*3)[1])
        if report.get('ReportDate') != end.strftime('%m/%d/%Y 00:00:00') or end > observed.date():
            raise DirectSourceError('Issuer index reporting date mismatch')
        docs = report.get('Documents')
        if not isinstance(docs, list) or len(docs) > 30:
            raise DirectSourceError('Issuer index document bound exceeded')
        matches = [d for d in docs if d.get('DocumentCategory') == 'transcript']
        if len(matches) > 1:
            raise DirectSourceError('Ambiguous issuer transcript period')
        for item in matches:
            url = item.get('DocumentPath', '')
            allowed = rf'https://s203\.q4cdn\.com/380862485/files/doc_financials/{year}/q{quarter}/[A-Za-z0-9_-]+\.pdf'
            if (not re.fullmatch(allowed, url) or item.get('DocumentTitle') != 'Earnings Call Transcript'
                    or item.get('DocumentFileType') != 'PDF' or item.get('DocumentType') != 'File'
                    or item.get('LanguageId') != 1):
                raise DirectSourceError('Unreviewed issuer transcript link or type')
            key = f"RDDT:earnings_transcript:{year}:Q{quarter}"
            if key in found:
                raise DirectSourceError('Duplicate issuer report period')
            found[key] = {'key': key, 'symbol': 'RDDT', 'document_type': 'earnings_transcript',
                'fiscal_year': year, 'fiscal_quarter': quarter, 'url': url, 'source_format': 'pdf',
                'company_website': company['company_website'], 'investor_website': company['investor_website'],
                'company_pattern': r'Reddit,?\s+Inc\b\.?', 'publisher_pattern': r'Reddit,?\s+Inc\b\.?',
                'transcript_publisher': 'Issuer-hosted transcript; publisher not identified',
                'transcript_pattern': r'\bTRANSCRIPT\b|CORPORATE SPEAKERS:.{1,3000}PARTICIPANTS:.{1,3000}PRESENTATION:',
                'publication_pattern': DATE_PATTERN,
                **format_policy(year, quarter, policy),
                'source_page_url': index_url(company, year), 'source_page_sha256': hashlib.sha256(raw).hexdigest(),
                'discovery_kind': KIND, 'discovery_year': year, 'period_end': end.isoformat()}
    return list(found.values())


def parse_candidate(raw, metadata, observed):
    if metadata.get('discovery_kind') in PDF_KINDS.values():
        from app.services.issuer_q4_pdf_discovery import parse_candidate as parse_pdf_candidate
        return parse_pdf_candidate(raw, metadata, observed)
    if metadata.get('discovery_kind') == MSFT_KIND:
        from app.services.issuer_microsoft_discovery import parse_candidate as parse_msft_candidate
        return parse_msft_candidate(raw, metadata, observed)
    provisional = {**metadata, 'publication_pattern': DATE_PATTERN}
    provisional.pop('published_at', None)
    text, parsed = parse_issuer_material(raw, provisional)
    heading = re.match(r'^(?:TRANSCRIPT\s+)?(\d{2})\s*-\s*(\d{2})\s*-\s*(\d{4})\s+Reddit,?\s+Inc\b\.?', text)
    if not heading:
        raise DirectSourceError('Issuer transcript heading date missing')
    try:
        publication = date(int(heading[3]), int(heading[1]), int(heading[2]))
    except ValueError as exc:
        raise DirectSourceError('Invalid issuer transcript publication date') from exc
    end = date.fromisoformat(metadata['period_end'])
    if not end <= publication <= min(observed.date(), end+timedelta(days=100)):
        raise DirectSourceError('Issuer transcript publication outside fiscal window')
    resolved = {**provisional, 'published_at': publication.isoformat(),
                'publication_pattern': rf'{publication:%m}\s*-\s*{publication:%d}\s*-\s*{publication:%Y}'}
    # The validated heading supplies the exact date. Reuse the bounded PDF
    # extraction rather than decoding every page a second time.
    return resolved, text, {**parsed, **resolved}


def verified_metadata(db, company, metadata):
    """Rebuild permission from immutable original index bytes and current policy."""
    year = metadata.get('discovery_year')
    expected_index = index_metadata(company, year)
    index = db.scalar(select(DirectFeedDocument).where(DirectFeedDocument.feed == INDEX_FEED,
        DirectFeedDocument.source_key == expected_index['key']))
    if index is None or index.source_url != expected_index['url']:
        raise DirectSourceError('Issuer index evidence missing')
    revision = db.scalar(select(DirectFeedRevision).where(DirectFeedRevision.document_id == index.id,
        DirectFeedRevision.content_hash == metadata.get('source_page_sha256')))
    if (revision is None or not revision.source_bytes or hashlib.sha256(revision.source_bytes).hexdigest()
            != metadata.get('source_page_sha256')):
        raise DirectSourceError('Issuer index checksum mismatch')
    observed = revision.fetched_at
    if observed.tzinfo is None:
        observed = observed.replace(tzinfo=utcnow().tzinfo)
    candidates = parse_index(revision.source_bytes, company, year, observed, policy=parser_policy(metadata))
    matches = [c for c in candidates if c['key'] == metadata.get('key')]
    if len(matches) != 1:
        raise DirectSourceError('Issuer transcript absent from original index')
    base = matches[0]
    expected = dict(base)
    if metadata.get('published_at'):
        d = date.fromisoformat(metadata['published_at'])
        if base.get('discovery_kind') in PDF_KINDS.values():
            from app.services.issuer_q4_pdf_discovery import publication_pattern
            pattern = publication_pattern(base['symbol'], d)
        elif base.get('discovery_kind') == MSFT_KIND:
            from app.services.issuer_microsoft_discovery import publication_pattern
            pattern = publication_pattern(d)
        else:
            pattern = rf'{d:%m}\s*-\s*{d:%d}\s*-\s*{d:%Y}'
        expected.update(published_at=d.isoformat(), publication_pattern=pattern)
    if metadata != expected:
        raise DirectSourceError('Issuer discovery metadata changed')
    return base


def latest_metadata(db, registry, symbol, static):
    company = next((c for c in registry if c['symbol'] == symbol and enabled_company(c)), None)
    if company is None:
        return static
    # At most two years/four periods each. Pending newest periods deliberately
    # block falling back to an older prepared quarter.
    rows = list(db.scalars(select(DirectFeedDocument).where(DirectFeedDocument.feed == 'issuer_earnings',
        DirectFeedDocument.source_key.like(symbol+':earnings_transcript:%')).limit(101)))
    if len(rows) > 100:
        raise DirectSourceError('Issuer prepared period bound exceeded')
    candidates = [static] if static else []
    for row in rows:
        m = json.loads(row.metadata_json)
        if m.get('discovery_kind') == company['discovery']['kind']:
            verified_metadata(db, company, m)
            candidates.append(m)
    return max(candidates, key=lambda m: (m['fiscal_year'], m['fiscal_quarter'])) if candidates else None


def collect_discovery(db, client, registry, *, limit, recheck_hours):
    """Spend the existing issuer budget on index pages and linked PDFs combined."""
    companies = {c['symbol']: c for c in registry if enabled_company(c)}
    now = utcnow()
    for company in companies.values():
        for year in ((None,) if company['symbol'] == 'MSFT' else (now.year, now.year-1)):
            discover(db, INDEX_FEED, index_metadata(company, year))
    db.commit()
    spent, processed, errors, stop_reason = 0, 0, [], None
    static_keys = {f"{c['symbol']}:{d['document_type']}:{d['fiscal_year']}:Q{d['fiscal_quarter']}"
                   for c in registry for d in c.get('documents', [])}
    while spent < min(limit, 2) and companies:
        rows = list(db.scalars(select(DirectFeedDocument).where(
            DirectFeedDocument.feed.in_([INDEX_FEED, 'issuer_earnings']),
            (DirectFeedDocument.checked_at.is_(None)) | (DirectFeedDocument.checked_at <= now-timedelta(hours=max(24,recheck_hours)))
        ).order_by(DirectFeedDocument.checked_at.asc().nullsfirst(), DirectFeedDocument.id.asc())))
        eligible = [(row, json.loads(row.metadata_json)) for row in rows]
        eligible = [(r,m) for r,m in eligible if m.get('symbol') in companies
                    and m.get('discovery_kind') == companies[m['symbol']]['discovery']['kind']
                    and r.error != 'Issuer period changed source URL; review required']
        if not eligible:
            db.commit()
            break
        row, metadata = eligible[0]
        row_id, feed, url = row.id, row.feed, row.source_url
        company = companies[metadata['symbol']]
        spent += 1
        row.attempts += 1
        row.checked_at, row.status = now, 'pending'
        db.commit()  # Persist the daily attempt even if this process is interrupted.
        try:
            if feed == INDEX_FEED:
                if metadata != index_metadata(company, metadata.get('year')) or url != metadata['url']:
                    raise DirectSourceError('Issuer index configuration changed')
                base = None
            else:
                base = verified_metadata(db, company, metadata)
                if url != base['url']:
                    raise DirectSourceError('Issuer document URL changed')
                # Only a never-parsed document can adopt a newer policy. Stored
                # revisions and publication receipts keep their original parser.
                has_revision = db.scalar(select(DirectFeedRevision.id).where(
                    DirectFeedRevision.document_id == row_id).limit(1)) is not None
                if (base.get('discovery_kind') == KIND and not has_revision and not metadata.get('published_at')
                        and parser_policy(base) < PARSER_POLICY):
                    base = {**base, **format_policy(base['fiscal_year'], base['fiscal_quarter'], PARSER_POLICY)}
                # Grant this one validated link, never its CDN host or siblings.
                client.issuer_document_urls = frozenset([*client.issuer_document_urls, url])
            db.commit()  # Release pooled connection/transaction before HTTP.
            raw = client.get(url)
            if feed == INDEX_FEED:
                candidates = parse_index(raw, company, metadata.get('year'), now)
                row = db.get(DirectFeedDocument, row_id)
                record_document(db, row, raw, raw.decode('utf-8'), {'candidates': candidates})
                for candidate in candidates:
                    if candidate['key'] in static_keys:
                        continue  # Existing reviewed period keeps its original proof.
                    existing = db.scalar(select(DirectFeedDocument).where(DirectFeedDocument.feed == 'issuer_earnings',
                        DirectFeedDocument.source_key == candidate['key']))
                    if existing is None:
                        discover(db, 'issuer_earnings', candidate)
                    elif existing.source_url != candidate['url']:
                        existing.status = 'quarantined'
                        existing.error = 'Issuer period changed source URL; review required'
                        existing.checked_at = now
                        errors.append({'feed': 'issuer_earnings', 'key': candidate['key'], 'error': existing.error})
            else:
                resolved, text, parsed = parse_candidate(raw, base, now)
                row = db.get(DirectFeedDocument, row_id)
                row.metadata_json = dumps(resolved)
                record_document(db, row, raw, text, parsed)
            db.commit()
            processed += 1
        except Exception as exc:
            db.rollback()
            row = db.get(DirectFeedDocument, row_id)
            row.status, row.checked_at = 'failed', now
            row.error = f'{type(exc).__name__}: {exc}'[:1000]
            errors.append({'feed': feed, 'key': row.source_key, 'error': row.error})
            db.commit()
            if isinstance(exc, DirectSourceError) and str(exc).startswith(('Source transport failed','Source cooldown','Source HTTP')):
                stop_reason = str(exc)
                break
    return {'attempted': spent, 'processed': processed, 'errors': errors, 'stop_reason': stop_reason}
