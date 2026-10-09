"""Bounded public Form 4 cohort and complete candidate populations, read-only."""
import base64
from datetime import datetime, timezone
import hashlib
import json
import zlib

from sqlalchemy import select, text, or_
from sqlalchemy.orm import Session
from app.db import engine
from app.models import Event, InsiderTransaction, InsiderTransactionNormalized, SecForm4Filing
from app.services.direct_feed_store import DirectFeedDocument, DirectFeedRevision, dumps


def main():
    assert engine.dialect.name == 'postgresql'
    with engine.connect() as conn, conn.begin():
        conn.execute(text('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY'))
        conn.execute(text("SET LOCAL statement_timeout = '30s'"))
        assert conn.scalar(text('SHOW transaction_read_only')) == 'on'
        with Session(bind=conn) as db:
            documents = list(db.scalars(select(DirectFeedDocument).where(
                DirectFeedDocument.feed == 'sec_form4').order_by(DirectFeedDocument.id)))
            documents = [row for row in documents if json.loads(row.metadata_json)['filing_date'] == '2026-10-07']
            assert 0 < len(documents) <= 500
            sources, symbols, accessions = [], set(), set()
            for row in documents:
                revision = db.scalar(select(DirectFeedRevision).where(
                    DirectFeedRevision.document_id == row.id,
                    DirectFeedRevision.content_hash == row.content_hash))
                assert revision is not None and revision.source_bytes is not None
                assert hashlib.sha256(revision.source_bytes).hexdigest() == row.content_hash
                parsed = json.loads(revision.parsed_json)
                symbols.update(item['ticker_normalized'] for item in parsed.get('transactions', []) if item.get('ticker_normalized'))
                accessions.add(row.source_key)
                sources.append(dict(id=row.id, metadata=json.loads(row.metadata_json), content_hash=row.content_hash,
                    raw_base64=base64.b64encode(revision.source_bytes).decode(), status=row.status,
                    error=row.error, first_seen_at=row.first_seen_at))
            def export(model, where, maximum=100000):
                rows = list(db.scalars(select(model).where(where).order_by(model.id).limit(maximum+1)))
                assert len(rows) <= maximum, f'{model.__tablename__} exceeds reviewed bound'
                return [{c.name:getattr(row,c.name) for c in model.__table__.columns} for row in rows]
            # Full symbol populations deliberately include other filing dates and
            # accessions so a partial export cannot make an old event look new.
            payload = dict(captured_at=datetime.now(timezone.utc), transaction_read_only=True,
                filing_date='2026-10-07', documents=sources,
                insiders=export(InsiderTransactionNormalized, or_(InsiderTransactionNormalized.ticker_normalized.in_(symbols),
                    InsiderTransactionNormalized.accession_number.in_(accessions))),
                raw_insiders=export(InsiderTransaction, InsiderTransaction.symbol.in_(symbols)),
                events=export(Event, (Event.event_type=='insider_trade') & Event.symbol.in_(symbols)),
                form4_filings=export(SecForm4Filing, SecForm4Filing.accession_number.in_(accessions), 500))
    print('FORM4_EXPORT_BASE64='+base64.b64encode(zlib.compress(dumps(payload).encode())).decode(), flush=True)
    print(dumps(dict(captured_at=payload['captured_at'], counts={key:len(payload[key]) for key in
        ['documents','insiders','raw_insiders','events','form4_filings']}, public_writes=0)), flush=True)


if __name__ == '__main__':
    main()
