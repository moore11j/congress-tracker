"""Preserve complete institutional return inputs while bounding history reads."""
from datetime import date, datetime, timedelta, timezone
from typing import Any

import pytest
from sqlalchemy import create_engine, select, func, or_
from sqlalchemy.orm import Session
from app.db import Base
from app.models import InstitutionalPosition, PriceCache
from app.services import institutional_activity as service
from app.utils.symbols import normalize_symbol

_round_optional = service._round_optional

def reference_pairs(
    db: Session,
    *,
    cik: str,
    report_year: int,
    report_quarter: int,
    active_filing_ids: list[int],
    periods: list[dict[str, Any]],
    adjusted_only: bool = False,
) -> dict[str, dict[str, tuple[float, float]]]:
    if not periods:
        return {}
    price_column = PriceCache.adjusted_close if adjusted_only else PriceCache.close
    position_filters = [
        InstitutionalPosition.cik == cik,
        InstitutionalPosition.report_year == int(report_year),
        InstitutionalPosition.report_quarter == int(report_quarter),
        InstitutionalPosition.value_usd.is_not(None),
        InstitutionalPosition.value_usd > 0,
        InstitutionalPosition.normalized_symbol.is_not(None),
    ]
    if active_filing_ids:
        position_filters.append(InstitutionalPosition.filing_id.in_(active_filing_ids))
    position_symbols = (
        select(InstitutionalPosition.normalized_symbol.label("symbol"))
        .where(*position_filters)
        .distinct()
        .subquery()
    )
    results: dict[str, dict[str, tuple[float, float]]] = {}
    for period in periods:
        key = str(period["key"])
        start_date = period["start_date"]
        end_date = period["end_date"]
        bounds = (
            select(
                PriceCache.symbol.label("symbol"),
                func.min(PriceCache.date).label("start_day"),
                func.max(PriceCache.date).label("end_day"),
            )
            .join(position_symbols, position_symbols.c.symbol == PriceCache.symbol)
            .where(PriceCache.date >= start_date.isoformat())
            .where(PriceCache.date <= end_date.isoformat())
            .where(price_column.is_not(None))
            .where(price_column > 0)
            .group_by(PriceCache.symbol)
            .subquery()
        )
        rows = db.execute(
            select(PriceCache.symbol, PriceCache.date, price_column, bounds.c.start_day, bounds.c.end_day)
            .join(bounds, bounds.c.symbol == PriceCache.symbol)
            .where(or_(PriceCache.date == bounds.c.start_day, PriceCache.date == bounds.c.end_day))
            .where(price_column.is_not(None))
            .where(price_column > 0)
        ).all()
        grouped: dict[str, dict[str, float | str]] = {}
        for symbol, day, price, start_day, end_day in rows:
            normalized = normalize_symbol(symbol)
            parsed = _round_optional(price, 6)
            if not normalized or not day or parsed is None or parsed <= 0:
                continue
            item = grouped.setdefault(normalized, {"start_day": str(start_day), "end_day": str(end_day)})
            if str(day) == str(start_day):
                item["start_price"] = parsed
            if str(day) == str(end_day):
                item["end_price"] = parsed
        pairs: dict[str, tuple[float, float]] = {}
        for symbol, item in grouped.items():
            start_price = item.get("start_price")
            end_price = item.get("end_price")
            if isinstance(start_price, (int, float)) and isinstance(end_price, (int, float)):
                pairs[symbol] = (float(start_price), float(end_price))
        results[key] = pairs
    return results


def seed_history(engine, count=12, days=2200):
    Base.metadata.create_all(engine)
    with Session(engine) as db:
        positions=[]
        prices=[]
        for i in range(count):
            symbol=f"SYM{i:04d}"
            positions.append(dict(filing_id=1,cik="0001234567",normalized_symbol=symbol,
                report_year=2026,report_quarter=2,filing_date=date(2026,8,1),value_usd=1000+i))
            for day in range(days):
                value=100+i+day/100
                prices.append(dict(symbol=symbol,date=(date(2020,1,1)+timedelta(days=day)).isoformat(),close=value*2,adjusted_close=value))
        db.execute(InstitutionalPosition.__table__.insert(),positions)
        db.execute(PriceCache.__table__.insert(),prices)
        db.commit()
    periods=[dict(key="long",label="Long",start_date=date(2020,1,1),end_date=date(2026,10,15)),
        dict(key="short",label="Short",start_date=date(2025,1,1),end_date=date(2025,12,31)),
        dict(key="none",label="No prices",start_date=date(2030,1,1),end_date=date(2030,12,31))]
    return dict(cik="0001234567",report_year=2026,report_quarter=2,active_filing_ids=[1],periods=periods,adjusted_only=True)


def sqlite_work(db, function, params):
    raw=db.connection().connection.driver_connection
    units=0
    def tick():
        nonlocal units
        units+=1
        return 0
    raw.set_progress_handler(tick,100)
    try:
        result=function(db,**params)
    finally:
        raw.set_progress_handler(None,0)
    return result,units


def test_long_history_parity_uses_less_database_work():
    engine=create_engine("sqlite:///:memory:")
    try:
        params=seed_history(engine)
        with Session(engine) as db:
            old,old_work=sqlite_work(db,reference_pairs,params)
            new,new_work=sqlite_work(db,service._cached_price_pairs_for_holder_periods,params)
            assert old==new
            assert len(new['long'])==12 and len(new['short'])==12 and new['none']=={}
            assert new_work < old_work/2, (old_work,new_work)
    finally:
        engine.dispose()


@pytest.mark.parametrize('adjusted_only',[True,False])
@pytest.mark.parametrize('active_filing_ids',[[1],[]])
def test_sparse_invalid_bounds_and_filing_scope_parity(adjusted_only,active_filing_ids):
    engine=create_engine("sqlite:///:memory:")
    try:
        params=seed_history(engine,count=7,days=3)
        with Session(engine) as db:
            rows=list(db.scalars(select(PriceCache)))
            for row in rows:
                if row.symbol=='SYM0000':row.adjusted_close=None
                if row.symbol=='SYM0001' and row.date=='2020-01-01':row.adjusted_close=0;row.close=-5
                if row.symbol=='SYM0002' and row.date=='2020-01-03':row.adjusted_close=-1;row.close=0
                if row.symbol=='SYM0003' and row.date=='2020-01-01':row.adjusted_close=0.00000001;row.close=0.00000001
                if row.symbol=='SYM0004' and row.date=='2020-01-03':row.adjusted_close=float('inf');row.close=float('inf')
                if row.symbol=='SYM0005':row.adjusted_close=-1;row.close=0
            # Same symbol in another filing must not alter symbol completeness or duplicate results.
            db.add(InstitutionalPosition(filing_id=2,cik=params['cik'],normalized_symbol='EXTRA',report_year=2026,report_quarter=2,filing_date=date(2026,8,2),value_usd=100))
            db.add(PriceCache(symbol='EXTRA',date='2020-01-02',close=4,adjusted_close=2))
            db.commit()
            params.update(adjusted_only=adjusted_only,active_filing_ids=active_filing_ids)
            old=reference_pairs(db,**params)
            new=service._cached_price_pairs_for_holder_periods(db,**params)
            assert new==old
            assert ('EXTRA' in new['long']) == (not active_filing_ids)
            assert 'SYM0003' not in new['long'] and 'SYM0004' not in new['long']
            positions=[('SYM0006',700),('SYM0000',300)]
            for period in params['periods']:
                assert service._weighted_snapshot_return_from_pairs(period,positions,total_value=1000,price_pairs=new[period['key']]) == service._weighted_snapshot_return_from_pairs(period,positions,total_value=1000,price_pairs=old[period['key']])
    finally:
        engine.dispose()


def test_complete_large_holder_not_limited_to_preview():
    engine=create_engine("sqlite:///:memory:")
    try:
        params=seed_history(engine,count=1203,days=2)
        with Session(engine) as db:
            old=reference_pairs(db,**params)
            new=service._cached_price_pairs_for_holder_periods(db,**params)
            assert new==old and len(new['long'])==1203
            assert service._cached_price_pairs_for_holder_periods(db,**{**params,'periods':[]})=={}
    finally:
        engine.dispose()


def test_full_summary_and_persisted_metrics_match_reference(monkeypatch):
    from app.models import InstitutionalHolder, InstitutionalFiling, InstitutionalHolderPerformanceMetric
    class FrozenDatetime(datetime):
        @classmethod
        def now(cls,tz=None):
            value=datetime(2026,10,15,12,tzinfo=timezone.utc)
            return value if tz else value.replace(tzinfo=None)
    monkeypatch.setattr(service,'datetime',FrozenDatetime)
    engine=create_engine("sqlite:///:memory:")
    try:
        params=seed_history(engine,count=3,days=2400)
        with Session(engine) as db:
            db.add(InstitutionalHolder(cik=params['cik'],holder_name='Synthetic full holder',latest_report_year=2026,latest_report_quarter=2,latest_filing_date=date(2026,8,1)))
            db.add(InstitutionalFiling(id=1,cik=params['cik'],accession_number='boundary-fixture',filing_date=date(2026,8,1),report_year=2026,report_quarter=2,report_period_end=date(2026,6,30),form_type='13F-HR'))
            db.commit()
            candidate=service._cached_price_pairs_for_holder_periods
            monkeypatch.setattr(service,'_cached_price_pairs_for_holder_periods',reference_pairs)
            expected=service.holder_performance_summary(db,params['cik'])
            assert expected['status']=='ok' and len(expected['items'])==4
            service.cache_holder_performance_summary(db,expected);db.commit()
            def snapshot():
                return [{c.name:getattr(row,c.name) for c in InstitutionalHolderPerformanceMetric.__table__.columns if c.name not in {'created_at','updated_at'}} for row in db.scalars(select(InstitutionalHolderPerformanceMetric).order_by(InstitutionalHolderPerformanceMetric.id))]
            before=snapshot()
            monkeypatch.setattr(service,'_cached_price_pairs_for_holder_periods',candidate)
            actual=service.holder_performance_summary(db,params['cik'])
            assert actual==expected
            result=service.cache_holder_performance_summary(db,actual);db.commit()
            assert result=={'updated':4,'skipped':0}
            assert snapshot()==before
            assert service.holder_performance_summary(db,params['cik'])==expected
    finally:
        engine.dispose()
