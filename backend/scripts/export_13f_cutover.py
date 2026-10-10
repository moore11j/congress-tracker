"""Bounded public institutional cohort, source bytes and mappings; read-only."""
import base64
from datetime import date, datetime, timezone
import hashlib
import json
import zlib

from sqlalchemy import func, select, text
from app.db import SessionLocal
from app.models import (Event, InstitutionalFiling, InstitutionalPosition,
                        InstitutionalPositionChange, InstitutionalActivityEvent,
                        InstitutionalHolder, InstitutionalSymbolSummary)
from app.services.direct_feed_store import DirectFeedDocument, DirectFeedRevision, dumps


def main():
    with SessionLocal() as db:
        assert db.get_bind().dialect.name == 'postgresql'
        db.execute(text('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY'))
        db.execute(text("SET LOCAL statement_timeout='30s'"))
        assert db.scalar(text('SHOW transaction_read_only')) == 'on'
        documents = list(db.scalars(select(DirectFeedDocument).where(
            DirectFeedDocument.feed == 'sec_13f').order_by(DirectFeedDocument.id).limit(501)))
        assert 0 < len(documents) <= 500
        sources, cusips, ciks, total = [], set(), set(), 0
        for doc in documents:
            metadata = json.loads(doc.metadata_json)
            revision = db.scalar(select(DirectFeedRevision).where(
                DirectFeedRevision.document_id == doc.id, DirectFeedRevision.content_hash == doc.content_hash))
            raw = revision.source_bytes if revision else None
            if raw:
                total += len(raw)
                assert total <= 100_000_000, 'Source cohort exceeds reviewed 100 MB bound'
                assert hashlib.sha256(raw).hexdigest() == doc.content_hash
                parsed = json.loads(revision.parsed_json)
                cusips.update(r['cusip'] for r in parsed.get('positions', []))
            if metadata['filing_date'] >= '2026-10-07':
                ciks.add(metadata['cik'].lstrip('0'))
            sources.append(dict(id=doc.id, metadata=metadata, content_hash=doc.content_hash,
                status=doc.status, error=doc.error,
                prior_collection=json.loads(doc.reconciliation_json or '{}').get('_prior_collection'),
                raw_base64=base64.b64encode(raw).decode() if raw else None))
        def records(model, query, maximum=100000):
            rows = list(db.scalars(query.limit(maximum+1)))
            assert len(rows) <= maximum, model.__tablename__+' exceeds reviewed row bound'
            return [{c.name:getattr(row,c.name) for c in model.__table__.columns} for row in rows]
        cik_variants = {cik.zfill(width) for cik in ciks for width in range(len(cik),11)}
        filings = records(InstitutionalFiling, select(InstitutionalFiling).where(
            func.ltrim(InstitutionalFiling.cik,'0').in_(ciks)), 5000)
        positions = records(InstitutionalPosition, select(InstitutionalPosition).where(
            func.ltrim(InstitutionalPosition.cik,'0').in_(ciks)))
        # One real row for every distinct historical symbol candidate. Keeping
        # conflicts is essential; selecting only a preferred symbol would hide them.
        mapping_ids = []
        ordered_cusips = sorted(cusips)
        for offset in range(0, len(ordered_cusips), 100):
            part = list(db.scalars(select(InstitutionalPosition.id).where(
                InstitutionalPosition.cusip.in_(ordered_cusips[offset:offset+100]),
                InstitutionalPosition.normalized_symbol.is_not(None),
                InstitutionalPosition.filing_date <= date(2026,10,7)).distinct(
                    InstitutionalPosition.cusip,InstitutionalPosition.normalized_symbol).order_by(
                    InstitutionalPosition.cusip,InstitutionalPosition.normalized_symbol,
                    InstitutionalPosition.filing_date,InstitutionalPosition.id).limit(100001)))
            mapping_ids.extend(part)
            assert len(mapping_ids) <= 100000
        assert len(mapping_ids) <= 100000
        # Select the earliest actual row for every symbol candidate, retaining
        # conflicts, original filing dates and real parent filings.
        mapping_positions = records(InstitutionalPosition, select(InstitutionalPosition).where(
            InstitutionalPosition.id.in_(mapping_ids)))
        symbols = {r['normalized_symbol'] for r in [*positions,*mapping_positions] if r['normalized_symbol']}
        payload = dict(captured_at=datetime.now(timezone.utc),transaction_read_only=True,filing_date='2026-10-07',
            documents=sources,filings=filings,positions=positions,mapping_positions=mapping_positions,
            mapping_filings=records(InstitutionalFiling,select(InstitutionalFiling).where(
                InstitutionalFiling.id.in_({r['filing_id'] for r in mapping_positions})),5000),
            changes=records(InstitutionalPositionChange,select(InstitutionalPositionChange).where(
                func.ltrim(InstitutionalPositionChange.cik,'0').in_(ciks))),
            activity=records(InstitutionalActivityEvent,select(InstitutionalActivityEvent).where(
                func.ltrim(InstitutionalActivityEvent.cik,'0').in_(ciks))),
            events=records(Event,select(Event).where(Event.member_bioguide_id.in_(cik_variants))),
            holders=records(InstitutionalHolder,select(InstitutionalHolder).where(func.ltrim(InstitutionalHolder.cik,'0').in_(ciks)),5000),
            summaries=records(InstitutionalSymbolSummary,select(InstitutionalSymbolSummary).where(InstitutionalSymbolSummary.symbol.in_(symbols))))
    data=dumps(payload).encode()
    print('FORM4_EXPORT_BASE64='+base64.b64encode(zlib.compress(data)).decode(),flush=True)
    print(dumps(dict(captured_at=payload['captured_at'],source_bytes=total,export_bytes=len(data),sha256=hashlib.sha256(data).hexdigest(),
        counts={k:len(v) for k,v in payload.items() if isinstance(v,list)},public_writes=0)),flush=True)


if __name__=='__main__':
    main()
