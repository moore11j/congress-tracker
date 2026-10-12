"""Preserve institutional outputs while bounding repeated summary reads."""
from collections import Counter
from datetime import date, datetime, timezone
import json

import pytest
from sqlalchemy import select

from app.db import Base
from app.models import Event, InstitutionalFiling, InstitutionalSymbolSummary
from app.services import institutional_activity as service
from app.services.direct_feed_store import dumps
from test_institutional_activity import _engine, _session, _filing_row, upsert_positions_for_filing


class FrozenDatetime(datetime):
    @classmethod
    def now(cls, tz=None):
        value=datetime(2026,10,12,12,tzinfo=timezone.utc)
        return value.astimezone(tz) if tz else value.replace(tzinfo=None)


def seed(db, count=12):
    current=[]
    for holder_index,cik in enumerate(('0001234567','0007654321')):
        for quarter,day in ((2,date(2026,7,15)),(3,date(2026,10,7))):
            candidate=service.parse_latest_filing(_filing_row(cik=cik,filing_date=day,year=2026,quarter=quarter))
            service.upsert_institutional_holder(db,candidate)
            filing,_=service.upsert_institutional_filing(db,candidate);db.flush()
            rows=[dict(symbol=f'SYM{i}',cusip=f'{i:09d}',shares=100000*(i+1)*(quarter+holder_index),
                       marketValue=100000000*(i+1)*(quarter+holder_index),ownershipPercentage=0.2)
                  for i in range(count)]
            # An exit and an option row exercise the existing aggregation rules.
            if quarter==2:
                rows.append(dict(symbol='EXIT',cusip='999999999',shares=800000,marketValue=200000000))
            else:
                rows.append(dict(symbol='SYM0',cusip='000000000',shares=100,marketValue=1000,putCall='PUT'))
            upsert_positions_for_filing(db,filing=filing,rows=rows)
            if quarter==3:current.append(filing)
    # A superseded filing must never re-enter total holders/value through batching.
    old=InstitutionalFiling(cik='0005555555',accession_number='superseded-source',
        report_year=2026,report_quarter=3,filing_date=date(2026,10,1),superseded_by=current[0].id)
    db.add(old);db.flush()
    upsert_positions_for_filing(db,filing=old,rows=[dict(symbol='SYM0',shares=99999999,marketValue=999999999999)])
    db.commit()
    return current


def snapshot(db):
    # Engine/server timestamps are not controlled by the service clock.
    volatile={'created_at','updated_at','processed_at','generated_at'}
    return {table.name:sorted([{k:v for k,v in row._mapping.items() if k not in volatile}
                             for row in db.execute(select(table))],key=dumps)
            for table in Base.metadata.sorted_tables}


def test_filing_summary_does_not_repeat_active_quarter_lookup(monkeypatch):
    monkeypatch.setattr(service,'datetime',FrozenDatetime)
    engine=_engine()
    try:
        with _session(engine) as db:
            current=seed(db)
            calls=Counter();original=service._active_filing_ids_for_period
            def counted(db,**kwargs):
                calls[(kwargs['report_year'],kwargs['report_quarter'],kwargs.get('cik'))]+=1
                return original(db,**kwargs)
            monkeypatch.setattr(service,'_active_filing_ids_for_period',counted)
            result=service.process_filing_changes_and_events(db,current[0],holder_only=True)
            assert result['summaries']==13
            assert calls[(2026,3,None)]==1
            assert db.scalar(select(InstitutionalSymbolSummary.total_holders).where(
                InstitutionalSymbolSummary.normalized_symbol=='SYM0'))==2
    finally:engine.dispose()


@pytest.mark.parametrize('overflow',[None,1,8])
@pytest.mark.parametrize('symbol_batch',[False,True])
def test_batched_and_individual_reads_preserve_all_rows_and_events(monkeypatch,overflow,symbol_batch):
    monkeypatch.setattr(service,'datetime',FrozenDatetime)
    original=service._refresh_filing_symbol_summaries
    if overflow is not None:monkeypatch.setattr(service,'SUMMARY_ROW_LIMIT',overflow)
    def individual(db,symbols,report_year,report_quarter):
        for symbol in sorted(symbols):
            yield service.refresh_symbol_summary(db,symbol,report_year,report_quarter)
    states=[];results=[]
    for reader in (individual,original):
        monkeypatch.setattr(service,'_refresh_filing_symbol_summaries',reader)
        engine=_engine()
        try:
            with _session(engine) as db:
                filings=seed(db,4)
                outputs=[]
                for filing in filings:
                    if symbol_batch:
                        cursor=None
                        while True:
                            result=service.process_filing_changes_and_events_symbol_batch(db,filing,after_symbol=cursor,symbol_limit=2)
                            outputs.append(result);db.commit()
                            if result['complete']:break
                            cursor=result['next_symbol_cursor']
                    else:
                        outputs.append(service.process_filing_changes_and_events(db,filing,holder_only=True));db.commit()
                assert list(db.scalars(select(Event))), 'Exercise real feed-event materialization'
                results.append(outputs);states.append(snapshot(db))
        finally:engine.dispose()
    assert results[0]==results[1]
    assert states[0]==states[1]


def test_summary_reader_keeps_existing_empty_and_noncanonical_symbol_behavior():
    engine=_engine()
    try:
        with _session(engine) as db:
            assert list(service._refresh_filing_symbol_summaries(db,set(),2026,3))==[]
            assert list(service._refresh_filing_symbol_summaries(db,{'','sym0'},2026,3))==[None,None]
    finally:engine.dispose()
