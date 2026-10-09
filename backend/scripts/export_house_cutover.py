"""Database-enforced read-only public House cutover inventory, no customer rows."""
import base64
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import zlib

from sqlalchemy import select, text
from sqlalchemy.orm import Session
from app.db import engine
from app.models import Member, Security, Filing, Transaction, Event, TradeOutcome
from app.services.direct_feed_store import DirectFeedDocument, DirectFeedRevision, dumps
from app.services.direct_congress_reconciliation import resolve_direct_member
from app.services.direct_congress_worker import _context
from app.services.direct_congress_repair import _record
from app.services.feed_source_control import FeedSourceControl


def main():
    directory_bytes=Path('/app/config/congress_members_2026_10_08.json').read_bytes()
    assert hashlib.sha256(directory_bytes).hexdigest()=='348aefcf00ec37b3dbb2c01ce7cdc5ada8965b567594cec5275b423b123c407b'
    directory=json.loads(directory_bytes)
    populations={name:{} for name in ('members','securities','filings','transactions','events','outcomes')}
    documents=[]
    assert engine.dialect.name=='postgresql'
    with engine.connect() as conn,conn.begin():
        conn.execute(text('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY'))
        conn.execute(text("SET LOCAL statement_timeout = '15s'"))
        assert conn.scalar(text('SHOW transaction_read_only'))=='on'
        with Session(bind=conn) as db:
            staged=list(db.scalars(select(DirectFeedDocument).where(DirectFeedDocument.feed=='house_ptr').order_by(DirectFeedDocument.id).limit(101)))
            assert len(staged)<=100
            for doc in staged:
                metadata=json.loads(doc.metadata_json)
                if not '2026-10-01'<=metadata['filing_date']<='2026-10-08':continue
                revision=db.scalar(select(DirectFeedRevision).where(DirectFeedRevision.document_id==doc.id,DirectFeedRevision.content_hash==doc.content_hash))
                raw=revision.source_bytes if revision else None
                if raw is not None:assert hashlib.sha256(raw).hexdigest()==doc.content_hash
                documents.append(dict(document=_record(doc),raw_base64=base64.b64encode(raw).decode() if raw else None))
                resolved=resolve_direct_member(metadata,'house',directory)
                assert resolved['status']=='resolved',resolved
                state,stored=_context(db,metadata['url'],resolved['member'])
                for key in populations:
                    for row in state[key]:populations[key][row['id']]=row
                for row in state.get('member_trades',[]):populations['transactions'][row['id']]=row
                populations['securities'].update(state.get('member_securities',{}))
                if stored:populations['members'][stored.id]=_record(stored)
            assert all(len(rows)<=20000 for rows in populations.values())
            control=db.get(FeedSourceControl,'house_ptr')
            result=dict(captured_at=datetime.now(timezone.utc),transaction_read_only=True,customer_rows_exported=0,
                provider=control.provider if control else 'fmp',generation=control.generation if control else 0,
                documents=documents,directory=directory,
                **{key:sorted(rows.values(),key=lambda row:row['id']) for key,rows in populations.items()})
    print('HOUSE_CUTOVER_BASE64='+base64.b64encode(zlib.compress(dumps(result).encode())).decode(),flush=True)


if __name__=='__main__':main()
