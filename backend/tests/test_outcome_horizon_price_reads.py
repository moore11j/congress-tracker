"""Keep all outcome horizons while avoiding unrelated daily-history reads."""
from datetime import date,datetime,timedelta,timezone
from sqlalchemy import select,event as sa_event
from sqlalchemy.orm import Session
from app.models import OutcomeEntry,OutcomeHorizonObservation,PriceCache
from app.services.outcome_integrity import OUTCOME_HORIZONS,materialize_outcome_horizons,materialize_cached_outcome_horizons,materialize_outcome_entry
from test_outcome_integrity import _engine,_snapshot,_bar

# Frozen pre-repair implementation is the independent complete-result oracle.
def _reference_cached(db: Session, *, as_of: date) -> dict[str, int]:
    """Process every due verified entry from stored prices, without provider calls.

    The provider hydration budget must not cap work whose prices already exist.
    Existing observations remain immutable; the caller owns the transaction.
    """
    entries = db.execute(select(OutcomeEntry).where(
        OutcomeEntry.entry_session_date <= as_of - timedelta(days=min(OUTCOME_HORIZONS)),
    )).scalars().all()
    if not entries:
        return {"entries_checked": 0, "observations_created": 0}
    observations = db.execute(select(OutcomeHorizonObservation).where(
        OutcomeHorizonObservation.entry_id.in_([entry.id for entry in entries]),
    )).scalars().all()
    by_entry: dict[int, list[OutcomeHorizonObservation]] = {}
    for row in observations:
        by_entry.setdefault(row.entry_id, []).append(row)
    due_entries = [entry for entry in entries if any(
        entry.entry_session_date + timedelta(days=days) <= as_of
        and days not in {row.horizon_days for row in by_entry.get(entry.id, [])}
        for days in OUTCOME_HORIZONS
    )]
    if not due_entries:
        return {"entries_checked": 0, "observations_created": 0}
    symbols = {symbol for entry in due_entries for symbol in (entry.ticker_at_time, entry.benchmark_symbol)}
    start_day = min(entry.entry_session_date for entry in due_entries) + timedelta(days=min(OUTCOME_HORIZONS))
    prices = db.execute(select(PriceCache).where(
        PriceCache.symbol.in_(symbols),
        PriceCache.date >= start_day.isoformat(),
        PriceCache.date <= as_of.isoformat(),
    )).scalars().all()
    prices_by_key = {(row.symbol, row.date): row for row in prices}
    created = 0
    for entry in due_entries:
        existing = by_entry.get(entry.id, [])
        rows = materialize_outcome_horizons(
            db, entry, as_of=as_of, price_rows_by_key=prices_by_key, existing_observations=existing,
        )
        created += len(rows) - len(existing)
    return {"entries_checked": len(due_entries), "observations_created": created}

def seed_long_history(engine):
    start=date(2018,1,2);as_of=date(2026,10,15)
    with Session(engine) as db:
        snapshot=_snapshot(db,datetime(2018,1,2,13,tzinfo=timezone.utc))
        days=(as_of-start).days+1
        db.add_all(_bar(symbol,start+timedelta(days=i),value+i/100) for i in range(days) for symbol,value in [('CRM',100),('SPY',500)])
        db.flush()
        assert materialize_outcome_entry(db,snapshot) is not None
        db.commit()
    return as_of


def run_and_read(engine, function, as_of):
    loaded=[]
    with Session(engine) as db:
        def capture(session,instance):
            if isinstance(instance,PriceCache): loaded.append((instance.symbol,instance.date))
        sa_event.listen(db,'loaded_as_persistent',capture)
        result=function(db,as_of=as_of)
        db.commit()
        rows=db.scalars(select(OutcomeHorizonObservation).order_by(OutcomeHorizonObservation.horizon_days,OutcomeHorizonObservation.id)).all()
        projection=[{c.name:getattr(r,c.name) for c in r.__table__.columns if c.name!='created_at'} for r in rows]
        return result,projection,loaded


def test_only_required_horizon_prices_are_loaded_from_long_history():
    engine=_engine()
    try:
        as_of=seed_long_history(engine)
        result,rows,loaded=run_and_read(engine,materialize_cached_outcome_horizons,as_of)
        assert result=={'entries_checked':1,'observations_created':5}
        assert len(rows)==5
        assert len(loaded)==10, 'Only five target sessions for stock and benchmark are needed'
    finally: engine.dispose()


def test_all_observation_fields_match_pre_repair_and_repeat_is_immutable():
    original=_engine();candidate=_engine()
    try:
        as_of=seed_long_history(original);seed_long_history(candidate)
        expected,expected_rows,old_reads=run_and_read(original,_reference_cached,as_of)
        actual,actual_rows,new_reads=run_and_read(candidate,materialize_cached_outcome_horizons,as_of)
        assert actual==expected and actual_rows==expected_rows
        assert len(old_reads)>6000 and len(new_reads)==10
        repeated,repeated_rows,repeat_reads=run_and_read(candidate,materialize_cached_outcome_horizons,as_of)
        assert repeated=={'entries_checked':0,'observations_created':0}
        assert repeated_rows==actual_rows and repeat_reads==[]
    finally: original.dispose();candidate.dispose()


def test_query_chunks_do_not_truncate_more_than_400_required_keys():
    engine=_engine();start=date(2026,1,5);target=date(2026,1,12)
    try:
        with Session(engine) as db:
            db.add_all([_bar('SPY',start,500),_bar('SPY',target,510)])
            for i in range(405):
                symbol='T'+str(i)
                snapshot=_snapshot(db,datetime(2026,1,5,13,tzinfo=timezone.utc),ticker=symbol,security_id=i+1)
                db.add_all([_bar(symbol,start,100),_bar(symbol,target,110)])
                db.flush()
                assert materialize_outcome_entry(db,snapshot) is not None
            db.commit()
        result,rows,reads=run_and_read(engine,materialize_cached_outcome_horizons,target)
        assert result=={'entries_checked':405,'observations_created':405}
        assert len(rows)==405 and len(reads)==406
        assert len(set(reads))==406
    finally: engine.dispose()


import pytest


@pytest.mark.parametrize('scenario', ['holiday_pending', 'missing_target', 'invalid_basis'])
def test_exact_keys_preserve_held_outcomes_and_do_not_substitute_later_prices(scenario):
    engines=[_engine(),_engine()]
    try:
        for engine in engines:
            with Session(engine) as db:
                snapshot=_snapshot(db,datetime(2026,8,5,17,tzinfo=timezone.utc),ticker='TSM')
                prices=[_bar('TSM',date(2026,8,6),100),_bar('SPY',date(2026,8,6),500),
                        _bar('TSM',date(2026,9,7),110),_bar('SPY',date(2026,9,7),510),
                        _bar('TSM',date(2026,9,9),120),_bar('SPY',date(2026,9,9),520)]
                if scenario=='invalid_basis':
                    invalid=_bar('TSM',date(2026,9,8),115)
                    invalid.adjustment_status=None
                    prices.extend([invalid,_bar('SPY',date(2026,9,8),515)])
                db.add_all(prices);db.flush()
                assert materialize_outcome_entry(db,snapshot) is not None
                db.commit()
        as_of=date(2026,9,7) if scenario=='holiday_pending' else date(2026,9,11)
        expected,expected_rows,_=run_and_read(engines[0],_reference_cached,as_of)
        actual,actual_rows,_=run_and_read(engines[1],materialize_cached_outcome_horizons,as_of)
        assert actual==expected and actual_rows==expected_rows==[]
    finally:
        for engine in engines:engine.dispose()
