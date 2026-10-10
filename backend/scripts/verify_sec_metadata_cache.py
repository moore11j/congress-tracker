"""Enforced read-only export of public prepared SEC identities and legacy rows."""
from datetime import datetime,timezone
import json,os
from sqlalchemy import select,text
from sqlalchemy.orm import Session
from app.db import engine
from app.models import InsightsSnapshot,TickerMeta,CikMeta
assert engine.dialect.name=='postgresql'
with engine.connect() as conn,conn.begin():
    conn.execute(text('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY'))
    conn.execute(text("SET LOCAL statement_timeout='20s'"))
    assert conn.scalar(text('SHOW transaction_read_only'))=='on'
    with Session(bind=conn) as db:
        rows=list(db.scalars(select(InsightsSnapshot).where(
            (InsightsSnapshot.kind=='sec-directory:exchange:v1') | InsightsSnapshot.kind.like('sec-company:%:v1')
        ).order_by(InsightsSnapshot.kind).limit(201)))
        assert 0<len(rows)<=200,'Review broader identity cache scope'
        state=db.get(InsightsSnapshot,'sec-research:warming:v1')
        state=json.loads(state.payload_json) if state else {}
        symbols=sorted(state.get('attempted_at',{}));assert 0<len(symbols)<=100
        ciks=[row.kind.split(':')[1] for row in rows if row.kind.startswith('sec-company:')]
        record=lambda row:{c.name:getattr(row,c.name) for c in row.__table__.columns}
        result=dict(observed_at=datetime.now(timezone.utc).isoformat(),transaction_read_only=True,database_writes=0,customer_rows_exported=0,
            metadata_provider=os.getenv('COMPANY_METADATA_PROVIDER','fmp'),symbols=symbols,
            public_metadata_caches=[dict(kind=row.kind,source=row.source,fetched_at=row.fetched_at,payload=json.loads(row.payload_json)) for row in rows],
            legacy_identity_rows=[record(row) for row in db.scalars(select(TickerMeta).where(TickerMeta.symbol.in_(symbols)))],
            legacy_cik_rows=[record(row) for row in db.scalars(select(CikMeta).where(CikMeta.cik.in_(ciks)))])
print('FINNHUB_RECEIPT='+json.dumps(result,default=str))
