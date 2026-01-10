from datetime import date
from fastapi import FastAPI, Depends
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker, Session, DeclarativeBase, Mapped, mapped_column

import os

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+psycopg://app:app@localhost:5432/congress")

engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)

class Base(DeclarativeBase):
    pass

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

# Create tables if not exist (for demo). Later we’ll switch to migrations.
Base.metadata.create_all(engine)

app = FastAPI(title="Congress Tracker API", version="0.1.0")

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
    # Idempotent seed: if demo member exists, do nothing
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
def feed(db: Session = Depends(get_db)):
    rows = db.execute(
        select(Transaction, Member, Security)
        .join(Member, Transaction.member_id == Member.id)
        .join(Security, Transaction.security_id == Security.id)
        .order_by(Transaction.report_date.desc(), Transaction.id.desc())
        .limit(50)
    ).all()

    items = []
    for tx, m, s in rows:
        items.append({
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
            "trade_date": tx.trade_date,
            "report_date": tx.report_date,
            "amount_range_min": tx.amount_range_min,
            "amount_range_max": tx.amount_range_max,
        })

    return {"items": items, "next_cursor": None}
