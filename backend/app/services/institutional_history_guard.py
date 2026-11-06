"""Persist historical repair boundaries across later ordinary refreshes."""
from datetime import date
from sqlalchemy import Date, Text, delete, select, tuple_
from sqlalchemy.orm import Mapped, mapped_column
from app.db import Base


class InstitutionalHistoricalScope(Base):
    __tablename__ = 'institutional_historical_scopes'
    normalized_symbol: Mapped[str] = mapped_column(Text, primary_key=True)
    report_year: Mapped[int] = mapped_column(primary_key=True)
    report_quarter: Mapped[int] = mapped_column(primary_key=True)
    publish_since: Mapped[date] = mapped_column(Date)
    repair_plan_hash: Mapped[str] = mapped_column(Text, index=True)


def install_scopes(db, bundle):
    """Internal: called only inside the receipt-verified canonical transaction."""
    scopes = {tuple(scope) for scope in bundle['scopes']}
    boundary = date.fromisoformat(bundle['publish_since'])
    rows = list(db.scalars(select(InstitutionalHistoricalScope).where(tuple_(
        InstitutionalHistoricalScope.normalized_symbol, InstitutionalHistoricalScope.report_year,
        InstitutionalHistoricalScope.report_quarter).in_(sorted(scopes)))))
    if any(row.publish_since != boundary or row.repair_plan_hash != bundle['sha256'] for row in rows):
        raise ValueError('Historical scope has a different reviewed repair owner')
    present = {(r.normalized_symbol, r.report_year, r.report_quarter) for r in rows}
    for symbol, year, quarter in sorted(scopes - present):
        db.add(InstitutionalHistoricalScope(normalized_symbol=symbol, report_year=year,
            report_quarter=quarter, publish_since=boundary, repair_plan_hash=bundle['sha256']))
    db.flush()
    return len(scopes - present)


def remove_scopes(db, plan_hash):
    return db.execute(delete(InstitutionalHistoricalScope).where(
        InstitutionalHistoricalScope.repair_plan_hash == plan_hash)).rowcount
