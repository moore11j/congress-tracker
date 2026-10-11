"""Stream a bounded, repeatable read of public institutional cutover evidence.

Each compressed record is independently framed. The final digest/count receipt
is required: an interrupted stream is never a complete validation baseline.
No staging, canonical, subscription, or customer records are changed.
"""
import base64
from collections import Counter
from datetime import date, datetime, timezone
import hashlib
import json
import os
import sys
import zlib

from sqlalchemy import func, select, text

from app.db import SessionLocal
from app.models import (Event, InstitutionalFiling, InstitutionalPosition,
                        InstitutionalPositionChange, InstitutionalActivityEvent,
                        InstitutionalHolder, InstitutionalSymbolSummary)
from app.services.direct_feed_store import DirectFeedDocument, DirectFeedRevision, dumps
from app.services.direct_feed_collection import parse_document


class RecordStream:
    def __init__(self, output=None):
        self.output = output or sys.stdout
        self.digest = hashlib.sha256()
        self.counts = Counter()
        self.bytes = 0
        self.pending = []
        self.pending_bytes = 0

    def flush(self):
        if self.pending:
            encoded=base64.b64encode(zlib.compress(b'\n'.join(self.pending))).decode()
            print('13F_BATCH='+encoded,file=self.output,flush=True)
            self.pending.clear()
            self.pending_bytes=0

    def write(self, table, row):
        raw = dumps({'table': table, 'row': row}).encode()
        # One source is at most30MB; base64 plus parsed metadata stays below50MB.
        if len(raw) > 50_000_000 or self.bytes + len(raw) > 500_000_000:
            raise ValueError('Export exceeds record/total transport bound')
        self.digest.update(raw + b'\n')
        self.bytes += len(raw)
        self.counts[table] += 1
        if table in {'metadata','documents'}:
            self.flush()
            encoded = base64.b64encode(zlib.compress(raw)).decode()
            print('13F_RECORD=' + encoded, file=self.output, flush=True)
        else:
            self.pending.append(raw)
            self.pending_bytes+=len(raw)
            if len(self.pending)>=100 or self.pending_bytes>=256_000:
                self.flush()

    def finish(self, **extra):
        self.flush()
        receipt = dict(counts=dict(self.counts), bytes=self.bytes,
                       sha256=self.digest.hexdigest(), **extra)
        print('13F_COMPLETE=' + dumps(receipt), file=self.output, flush=True)
        return receipt


def export_records(db, stream):
    """Caller owns one read-only snapshot; mapped ORM instances are never retained."""
    captured_at = datetime.now(timezone.utc)
    stream.write('metadata', dict(captured_at=captured_at,
        transaction_read_only=True, filing_date='2026-10-07',
        summary_scope='original_source_report_periods_since_cutover', summary_history_in_receipt=True))
    cusips, ciks, total, count = set(), set(), 0, 0
    target_periods=set()
    query = select(DirectFeedDocument.id, DirectFeedDocument.metadata_json,
        DirectFeedDocument.content_hash, DirectFeedDocument.status, DirectFeedDocument.error,
        DirectFeedDocument.reconciliation_json, DirectFeedRevision.source_bytes,
        DirectFeedRevision.parsed_json).outerjoin(DirectFeedRevision,
            (DirectFeedRevision.document_id == DirectFeedDocument.id) &
            (DirectFeedRevision.content_hash == DirectFeedDocument.content_hash)).where(
                DirectFeedDocument.feed == 'sec_13f').order_by(DirectFeedDocument.id).limit(1001)
    for row in db.execute(query.execution_options(yield_per=1)).mappings():
        count += 1
        if count > 1000:
            raise ValueError('Source cohort exceeds1000document bound')
        metadata = json.loads(row['metadata_json'])
        raw = row['source_bytes']
        if raw:
            total += len(raw)
            if len(raw) > 30_000_000 or total > 100_000_000:
                raise ValueError('Source cohort exceeds reviewed100MB bound')
            if hashlib.sha256(raw).hexdigest() != row['content_hash']:
                raise ValueError('Source checksum changed')
            parsed = json.loads(row['parsed_json'])
            cusips.update(r['cusip'] for r in parsed.get('positions', []))
            del parsed
        if metadata['filing_date'] >= '2026-10-07':
            ciks.add(metadata['cik'].lstrip('0'))
            if raw and row['status']=='parsed':
                # Scope follows original bytes, never a mutable saved parse.
                _,original,_=parse_document('sec_13f',raw,metadata)
                period=date.fromisoformat(original['metadata']['report_period'])
                target_periods.add((period.year,(period.month-1)//3+1))
                del original
        stream.write('documents', dict(id=row['id'], metadata=metadata,
            content_hash=row['content_hash'], status=row['status'], error=row['error'],
            prior_collection=json.loads(row['reconciliation_json'] or '{}').get('_prior_collection'),
            raw_base64=base64.b64encode(raw).decode() if raw else None))
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
        for i, record in enumerate(db.execute(query.execution_options(yield_per=100)).mappings(), 1):
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
    records('events',Event,Event.member_bioguide_id.in_(variants))
    records('holders',InstitutionalHolder,func.ltrim(InstitutionalHolder.cik,'0').in_(ciks),5000)
    records('summaries',InstitutionalSymbolSummary,InstitutionalSymbolSummary.symbol.in_(symbols))
    return dict(captured_at=captured_at, source_bytes=total, transaction_read_only=True,
                production_writes=0,summary_evidence=dict(
                    complete_history_rows=summary_count,history_sha256=summary_digest.hexdigest(),
                    omitted_other_period_rows=summary_excluded,
                    exported_periods=sorted(target_periods)))


def main():
    os.nice(10)  # Yield diagnostic serialization CPU to the API and delivery worker.
    stream = RecordStream()
    with SessionLocal() as db:
        if db.get_bind().dialect.name != 'postgresql':
            raise ValueError('Export requires PostgreSQL repeatable-read snapshot')
        db.execute(text('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY'))
        db.execute(text("SET LOCAL statement_timeout='30s'"))
        db.execute(text("SET LOCAL idle_in_transaction_session_timeout='60s'"))
        if db.scalar(text('SHOW transaction_read_only')) != 'on':
            raise ValueError('Export is not read-only')
        receipt = export_records(db, stream)
    # Only a fully read and closed transaction earns the terminal receipt.
    import resource
    stream.finish(**receipt, peak_rss_kb=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)


if __name__ == '__main__':
    main()
