from __future__ import annotations

import os
from datetime import date, datetime, timezone
from pathlib import Path

from fastapi import FastAPI, Depends, Query, HTTPException
from sqlalchemy import create_engine, select, func, and_, or_
from sqlalchemy.orm import sessionmaker, Session, DeclarativeBase, Mapped, mapped_column

# --- Database ---------------------------------------------------------------

# Always use the SAME effective DATABASE_URL across the app.
# Default is Fly persistent volume at /data/app.db
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:////data/app.db")

# Ensure /data exists when using the Fly volume path
if DATABASE_URL.startswith("sqlite:////data/"):
    Path("/data").mkdir(parents=True, exist_ok=True)

# SQLite needs check_same_thread=False for typical web usage
connect_args = {"check_same_thread": False} if DATABASE_URL.startswith("sqlite") else {}

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    connect_args=connect_args,
)

SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)


class Base(DeclarativeBase):
    pass


# --- Models -----------------------------------------------------------------

class Member(Base):
    __tablename__ = "members"
    id: Mapped[int] = mapped_column(primary_key=True)
    bioguide_id: Mapped[str] = mapped_column(unique=True, index=True)
    first_name: Mapped[str | None]
    last_name: Mapped[str | None]
    chamber: Mapped[str]
    party: Mapped[str | None]
    state: Mapped[str | None]


class Security(Base):
    __tablename__ = "securities"
    id: Mapped[int] = mapped_column(primary_key=True)
    symbol: Mapped[str | None] = mapped_column(unique=True, index=True)
    name: Mapped[str]
    asset_class: Mapped[str]
    sector: Mapped[str | None]


class Filing(Base):
    __tablename__ = "filings"
    id: Mapped[int] = mapped_column(primary_key=True)
    member_id: Mapped[int]
    source: Mapped[str]
    filing_date: Mapped[date | None]
    document_url: Mapped[str | None]
    document_hash: Mapped[str | None]


class Transaction(Base):
    __tablename__ = "transactions"
    id: Mapped[int] = mapped_column(primary_key=True)
    filing_id: Mapped[int]
    member_id: Mapped[int]
    security_id: Mapped[int | None]

    owner_type: Mapped[str]
    transaction_type: Mapped[str]
    trade_date: Mapped[date | None]
    report_date: Mapped[date | None]
    amount_range_min: Mapped[float | None]
    amount_range_max: Mapped[float | None]
    description: Mapped[str | None]


# --- App --------------------------------------------------------------------

app = FastAPI(title="Congress Tracker API", version="0.1.0")


@app.on_event("startup")
def _startup_create_tables():
    # Creates tables if missing. Does NOT delete or overwrite data.
    Base.metadata.create_all(engine)


def _sqlite_path_from_database_url(database_url: str) -> str | None:
    """
    Supports:
      sqlite:////absolute/path.db
      sqlite:///relative-or-absolute/path.db
      sqlite:relative.db
    Returns an absolute-ish path string to the sqlite file, or None if not sqlite.
    """
    if not database_url or not database_url.startswith("sqlite:"):
        return None

    rest = database_url[len("sqlite:"):]

    # sqlite:////data/db.sqlite  -> /data/db.sqlite
    if rest.startswith("////"):
        return rest[3:]  # keep one leading slash

    # sqlite:///app/db.sqlite -> /app/db.sqlite (absolute)
    if rest.startswith("///"):
        return rest[2:]  # keep one leading slash

    # sqlite://relative.db -> relative.db
    if rest.startswith("//"):
        return rest[2:]

    # sqlite:relative.db -> relative.db
    return rest


def _utc_iso_from_mtime(path: str) -> str | None:
    try:
        mtime = os.path.getmtime(path)
    except Exception:
        return None
    dt = datetime.fromtimestamp(mtime, tz=timezone.utc)
    return dt.isoformat().replace("+00:00", "Z")


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/admin/seed-demo")
def seed_demo(db: Session = Depends(get_db)):
    existing = db.execute(select(Member).where(Member.bioguide_id == "DEMO0001")).scalar_one_or_none()
    if existing:
        return {"status": "ok", "message": "Demo data already seeded."}

    m = Member(
        bioguide_id="DEMO0001",
        first_name="Demo",
        last_name="Member",
        chamber="house",
        party="I",
        state="CA",
    )
    s = Security(
        symbol="NVDA",
        name="NVIDIA Corporation",
        asset_class="stock",
        sector="Technology",
    )
    db.add_all([m, s])
    db.flush()

    f = Filing(
        member_id=m.id,
        source="house",
        filing_date=date(2026, 1, 9),
        document_url="https://example.com",
        document_hash="demo-1",
    )
    db.add(f)
    db.flush()

    tx = Transaction(
        filing_id=f.id,
        member_id=m.id,
        security_id=s.id,
        owner_type="self",
        transaction_type="buy",
        trade_date=date(2025, 12, 1),
        report_date=date(2026, 1, 9),
        amount_range_min=15000,
        amount_range_max=50000,
        description="Purchase - Demo",
    )
    db.add(tx)
    db.commit()

    return {"status": "ok", "message": "Seeded demo member + NVDA trade."}


@app.get("/api/feed")
def feed(
    db: Session = Depends(get_db),
    limit: int = Query(50, ge=1, le=200),
    cursor: str | None = None,

    # Filters
    symbol: str | None = None,
    member: str | None = None,           # matches first/last name substring
    chamber: str | None = None,          # "house" or "senate"
    transaction_type: str | None = None, # "purchase", "sale", etc.
    min_amount: float | None = None,     # compares against amount_range_max
):
    q = (
        select(Transaction, Member, Security)
        .join(Member, Transaction.member_id == Member.id)
        .join(Security, Transaction.security_id == Security.id)
    )

    # ---- Filters ----
    if symbol:
        q = q.where(Security.symbol == symbol.strip().upper())

    if chamber:
        q = q.where(Member.chamber == chamber.strip().lower())

    if transaction_type:
        q = q.where(Transaction.transaction_type == transaction_type.strip().lower())

    if min_amount is not None:
        q = q.where(
            or_(
                Transaction.amount_range_max >= min_amount,
                and_(Transaction.amount_range_max.is_(None), Transaction.amount_range_min >= min_amount),
            )
        )

    if member:
        term = f"%{member.strip().lower()}%"
        q = q.where(
            or_(
                Member.first_name.ilike(term),
                Member.last_name.ilike(term),
                (Member.first_name + " " + Member.last_name).ilike(term),
            )
        )

    # ---- Cursor pagination (report_date DESC, id DESC) ----
    if cursor:
        try:
            cursor_date_str, cursor_id_str = cursor.split("|", 1)
            cursor_id = int(cursor_id_str)
            cursor_date = date.fromisoformat(cursor_date_str)
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid cursor format. Expected YYYY-MM-DD|id")

        q = q.where(
            or_(
                Transaction.report_date < cursor_date,
                and_(
                    Transaction.report_date == cursor_date,
                    Transaction.id < cursor_id,
                ),
            )
        )

    q = q.order_by(Transaction.report_date.desc(), Transaction.id.desc()).limit(limit + 1)

    rows = db.execute(q).all()

    items = []
    for tx, m, s in rows[:limit]:
        items.append(
            {
                "id": tx.id,
                "member": {
                    "bioguide_id": m.bioguide_id,
                    "name": f"{m.first_name or ''} {m.last_name or ''}".strip(),
                    "chamber": m.chamber,
                    "party": m.party,
                    "state": m.state,
                },
                "security": {
                    "symbol": s.symbol,
                    "name": s.name,
                    "asset_class": s.asset_class,
                    "sector": s.sector,
                },
                "transaction_type": tx.transaction_type,
                "owner_type": tx.owner_type,
                "trade_date": tx.trade_date.isoformat() if tx.trade_date else None,
                "report_date": tx.report_date.isoformat() if tx.report_date else None,
                "amount_range_min": tx.amount_range_min,
                "amount_range_max": tx.amount_range_max,
            }
        )

    next_cursor = None
    if len(rows) > limit:
        tx_last = rows[limit - 1][0]
        if tx_last.report_date:
            next_cursor = f"{tx_last.report_date.isoformat()}|{tx_last.id}"

    return {"items": items, "next_cursor": next_cursor}


@app.get("/api/meta")
def meta():
    # IMPORTANT: use the same resolved DATABASE_URL the app uses (not env-only),
    # so meta works even when DATABASE_URL isn't explicitly set.
    db_file = _sqlite_path_from_database_url(DATABASE_URL)

    last_updated_utc = None
    if db_file:
        if not db_file.startswith("/"):
            db_file = os.path.abspath(db_file)
        last_updated_utc = _utc_iso_from_mtime(db_file)

    # Fallback if not sqlite OR file missing:
    if last_updated_utc is None:
        db = SessionLocal()
        try:
            latest = db.execute(select(func.max(Filing.filing_date))).scalar_one_or_none()
            if latest:
                dt = datetime(latest.year, latest.month, latest.day, tzinfo=timezone.utc)
                last_updated_utc = dt.isoformat().replace("+00:00", "Z")
        finally:
            db.close()

    return {"last_updated_utc": last_updated_utc}
