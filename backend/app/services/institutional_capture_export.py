"""Stream a bounded, repeatable read of public institutional cutover evidence.

Length-framed originals and JSON rows use a single outer gzip stream. The final digest/count receipt
is required: an interrupted stream is never a complete validation baseline.
No staging, canonical, subscription, or customer records are changed.
"""
from datetime import date, datetime, timezone
import hashlib
import json

from sqlalchemy import func, select, text

from app.models import (Event, InstitutionalFiling, InstitutionalPosition,
                        InstitutionalPositionChange, InstitutionalActivityEvent,
                        InstitutionalHolder, InstitutionalSymbolSummary)
from app.services.direct_feed_store import DirectFeedDocument, DirectFeedRevision, dumps


from app.services.institutional_capture_transport import MAX_DOCUMENTS, MAX_SOURCE_BYTES



def position_records(db, query, *, page_size=10000):
    """Keep sparse CIK filtering inside bounded primary-key ranges.

    A server-side cursor bounds rows returned, not rows scanned by a FETCH.
    Discover each next range without the CIK predicate so a sparse tail cannot
    turn a single FETCH into a scan of the rest of the positions table.
    The caller retains its total matching-row and whole-capture work limits.
    """
    if type(page_size) is not int or not 1 <= page_size <= 10000:
        raise ValueError('invalid_position_page_size')
    column=InstitutionalPosition.id
    after=None
    while True:
        frontier=select(column).order_by(column).limit(page_size)
        if after is not None:frontier=frontier.where(column>after)
        upper=db.scalar(select(func.max(frontier.subquery().c.id)))
        if upper is None:return
        bounded=query.where(column<=upper)
        if after is not None:bounded=bounded.where(column>after)
        yield from db.execute(bounded.execution_options(yield_per=100)).mappings()
        after=upper

def event_ids(db, variants, *, batch_size=100, maximum=100000):
    """Resolve complete manager matches through the existing member index.

    An ordered scan across thousands of manager variants can select the event
    primary-key index and scan unrelated history before returning a row. Small
    unordered member lookups collect IDs first; the caller then exports complete
    rows in the original primary-key order. The total matching cap is unchanged.
    """
    if type(batch_size) is not int or not 1 <= batch_size <= 100:
        raise ValueError('invalid_event_batch_size')
    if type(maximum) is not int or not 1 <= maximum <= 100000:
        raise ValueError('invalid_event_maximum')
    ordered=sorted(set(variants))
    identifiers=set()
    for offset in range(0,len(ordered),batch_size):
        query=select(Event.id).where(Event.member_bioguide_id.in_(
            ordered[offset:offset+batch_size])).limit(maximum-len(identifiers)+1)
        identifiers.update(db.scalars(query))
        if len(identifiers)>maximum:
            raise ValueError('events exceeds reviewed row bound')
    return sorted(identifiers)


def export_records(db, stream):
    """Caller owns one read-only snapshot; mapped ORM instances are never retained."""
    captured_at = datetime.now(timezone.utc)
    cusips, ciks, total, count = set(), set(), 0, 0
    target_periods=set()
    query = select(DirectFeedDocument.id, DirectFeedDocument.metadata_json,
        DirectFeedDocument.content_hash, DirectFeedDocument.status, DirectFeedDocument.error,
        DirectFeedDocument.reconciliation_json, DirectFeedRevision.source_bytes,
        DirectFeedRevision.parsed_json).outerjoin(DirectFeedRevision,
            (DirectFeedRevision.document_id == DirectFeedDocument.id) &
            (DirectFeedRevision.content_hash == DirectFeedDocument.content_hash)).where(
                DirectFeedDocument.feed == 'sec_13f').order_by(DirectFeedDocument.id)
    sizes=list(db.execute(query.with_only_columns(DirectFeedDocument.id,
        func.coalesce(func.octet_length(DirectFeedRevision.source_bytes),0).label('raw_size'),
        (func.coalesce(func.octet_length(DirectFeedRevision.parsed_json),0)+
         func.coalesce(func.octet_length(DirectFeedDocument.metadata_json),0)+
         func.coalesce(func.octet_length(DirectFeedDocument.reconciliation_json),0)).label('json_size')).limit(MAX_DOCUMENTS+1)).mappings())
    if not 1<=len(sizes)<=MAX_DOCUMENTS or sum(r['raw_size'] for r in sizes)>MAX_SOURCE_BYTES:
        raise ValueError('Source inventory exceeds reviewed document/byte bounds')
    if any(r['raw_size']>30_000_000 or r['raw_size']+r['json_size']>80_000_000 for r in sizes):
        raise ValueError('Source row exceeds reviewed memory bound')
    stream.write('metadata', dict(captured_at=captured_at, transport_version=3,
        transaction_read_only=True, filing_date='2026-10-07', document_count=len(sizes),
        summary_scope='original_source_report_periods_since_cutover', summary_history_in_receipt=True))
    def source_rows():
        # Avoid one network round trip per source without an unbounded fetch.
        # Ordinary chunks are <=8MB/25rows; a larger row is fetched alone.
        ids=[];size=0
        for item in sizes:
            next_size=item['raw_size']+item['json_size']
            if ids and (len(ids)>=25 or size+next_size>8_000_000):
                yield from db.execute(query.where(DirectFeedDocument.id.in_(ids))).mappings()
                ids=[];size=0
            ids.append(item['id']);size+=next_size
        if ids:
            yield from db.execute(query.where(DirectFeedDocument.id.in_(ids))).mappings()
    for row in source_rows():
        count += 1
        if count > MAX_DOCUMENTS:
            raise ValueError('Source cohort exceeds reviewed document bound')
        metadata = json.loads(row['metadata_json'])
        raw = row['source_bytes']
        if raw:
            total += len(raw)
            if len(raw) > 30_000_000 or total > MAX_SOURCE_BYTES:
                raise ValueError('Source cohort exceeds reviewed source byte bound')
            if hashlib.sha256(raw).hexdigest() != row['content_hash']:
                raise ValueError('Source checksum changed')
            # Cached parse only proposes query scope. The offline receiver must
            # fully reparse every original and reject omissions before acceptance.
            parsed = json.loads(row['parsed_json'] or '{}')
            cusips.update(r['cusip'] for r in parsed.get('positions', []))
        if metadata['filing_date'] >= '2026-10-07':
            ciks.add(metadata['cik'].lstrip('0'))
            if raw and row['status']=='parsed':
                period=date.fromisoformat(parsed['metadata']['report_period'])
                target_periods.add((period.year,(period.month-1)//3+1))
        if raw:del parsed
        stream.write_document(dict(id=row['id'], metadata=metadata,
            content_hash=row['content_hash'], status=row['status'], error=row['error'],
            prior_collection=json.loads(row['reconciliation_json'] or '{}').get('_prior_collection')),
            raw if raw else None)
    if not count:
        raise ValueError('No institutional source documents')
    # Drop the last source payload before canonical row export.
    del row, raw
    symbols, parent_ids = set(), set()
    summary_digest=hashlib.sha256()
    summary_count=0
    summary_excluded=0

    def records(name, model, condition, maximum=100000):
        nonlocal summary_count,summary_excluded
        query = select(*model.__table__.columns).where(condition).order_by(
            *model.__table__.primary_key.columns).limit(maximum+1)
        rows = (position_records(db, query) if name == 'positions' else
                db.execute(query.execution_options(yield_per=100)).mappings())
        for i, record in enumerate(rows, 1):
            if i > maximum:
                raise ValueError(name + ' exceeds reviewed row bound')
            values = dict(record)
            if name=='summaries':
                summary_digest.update(dumps(values).encode()+b'\n')
                summary_count+=1
                if (values['report_year'],values['report_quarter']) not in target_periods:
                    summary_excluded+=1
                    continue
            stream.write(name, values)
            if name in {'positions', 'mapping_positions'}:
                if values['normalized_symbol']:
                    symbols.add(values['normalized_symbol'])
                if name == 'mapping_positions':
                    parent_ids.add(values['filing_id'])

    records('filings', InstitutionalFiling, func.ltrim(InstitutionalFiling.cik,'0').in_(ciks),5000)
    records('positions', InstitutionalPosition, func.ltrim(InstitutionalPosition.cik,'0').in_(ciks))
    # Earliest actual mapping per candidate, including conflicts, as in legacy export.
    mapping_ids = []
    ordered = sorted(cusips)
    for offset in range(0,len(ordered),100):
        mapping_ids.extend(db.scalars(select(InstitutionalPosition.id).where(
            InstitutionalPosition.cusip.in_(ordered[offset:offset+100]),
            InstitutionalPosition.normalized_symbol.is_not(None),
            InstitutionalPosition.filing_date <= date(2026,10,7)).distinct(
                InstitutionalPosition.cusip,InstitutionalPosition.normalized_symbol).order_by(
                InstitutionalPosition.cusip,InstitutionalPosition.normalized_symbol,
                InstitutionalPosition.filing_date,InstitutionalPosition.id).limit(100001)))
        if len(mapping_ids)>100000:
            raise ValueError('Mapping cohort exceeds reviewed row bound')
    records('mapping_positions',InstitutionalPosition,InstitutionalPosition.id.in_(mapping_ids))
    records('mapping_filings',InstitutionalFiling,InstitutionalFiling.id.in_(parent_ids),5000)
    records('changes',InstitutionalPositionChange,func.ltrim(InstitutionalPositionChange.cik,'0').in_(ciks))
    records('activity',InstitutionalActivityEvent,func.ltrim(InstitutionalActivityEvent.cik,'0').in_(ciks))
    variants={cik.zfill(width) for cik in ciks for width in range(len(cik),11)}
    records('events',Event,Event.id.in_(event_ids(db,variants)))
    records('holders',InstitutionalHolder,func.ltrim(InstitutionalHolder.cik,'0').in_(ciks),5000)
    records('summaries',InstitutionalSymbolSummary,InstitutionalSymbolSummary.symbol.in_(symbols),150000)
    return dict(captured_at=captured_at, source_bytes=total, transaction_read_only=True,
                production_writes=0, query_scope=dict(ciks=sorted(ciks), cusips=sorted(cusips), periods=sorted(target_periods)), summary_evidence=dict(
                    complete_history_rows=summary_count,history_sha256=summary_digest.hexdigest(),
                    omitted_other_period_rows=summary_excluded,
                    exported_periods=sorted(target_periods)))
