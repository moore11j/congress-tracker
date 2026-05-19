from __future__ import annotations

import json
from datetime import date

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

import app.ingest_house as house_module
from app.backfill_events_from_trades import insert_missing_congress_events_from_transactions
from app.db import Base
from app.models import Event, Filing, Member, Security, Transaction


class _NoopCongressMetadata:
    def resolve(self, **_kwargs):
        return None


def _session_factory():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(
        bind=engine,
        tables=[
            Member.__table__,
            Security.__table__,
            Filing.__table__,
            Transaction.__table__,
            Event.__table__,
        ],
    )
    return sessionmaker(bind=engine, autoflush=False, autocommit=False)


def _evans_rows() -> list[dict]:
    base = {
        "firstName": "Dwight",
        "lastName": "Evans",
        "office": "Dwight Evans",
        "district": "PA03",
        "party": "D",
        "disclosureDate": "2026-05-15",
        "link": "https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/2026/20003798223.pdf",
        "type": "sale",
        "owner": "self",
        "amount": "$1,001 - $15,000",
    }
    return [
        {**base, "symbol": "AMT", "assetDescription": "American Tower Corp", "transactionDate": "2026-05-11"},
        {**base, "symbol": "CVS", "assetDescription": "CVS Health Corp", "transactionDate": "2025-11-21"},
        {**base, "symbol": "INTC", "assetDescription": "Intel Corp", "transactionDate": "2026-05-07"},
        {**base, "symbol": "PWR", "assetDescription": "Quanta Services Inc", "transactionDate": "2026-05-07"},
        {**base, "symbol": "TMO", "assetDescription": "Thermo Fisher Scientific Inc", "transactionDate": "2026-05-07"},
    ]


def _patch_house_source(monkeypatch, session_factory, rows: list[dict]) -> None:
    monkeypatch.setattr(house_module, "SessionLocal", session_factory)
    monkeypatch.setattr(house_module, "get_congress_metadata_resolver", lambda: _NoopCongressMetadata())
    monkeypatch.setattr(house_module, "_fetch_page", lambda page, limit: rows if page == 0 else [])


def test_house_ingest_recovers_every_transaction_row_from_existing_multi_trade_filing(monkeypatch):
    Session = _session_factory()
    rows = _evans_rows()

    _patch_house_source(monkeypatch, Session, rows[:1])
    first_result = house_module.ingest_house(pages=1, limit=100, sleep_s=0)
    assert first_result["inserted"] == 1

    db = Session()
    try:
        assert db.query(Filing).count() == 1
        assert db.query(Transaction).count() == 1
    finally:
        db.close()

    _patch_house_source(monkeypatch, Session, rows)
    dry_run = house_module.ingest_house(pages=1, limit=100, sleep_s=0, dry_run=True)
    assert dry_run["inserted"] == 4

    db = Session()
    try:
        assert db.query(Transaction).count() == 1
    finally:
        db.close()

    apply_result = house_module.ingest_house(pages=1, limit=100, sleep_s=0)
    assert apply_result["inserted"] == 4

    db = Session()
    try:
        transactions = (
            db.execute(select(Transaction, Security).join(Security, Security.id == Transaction.security_id))
            .all()
        )
        assert len(transactions) == 5
        assert {security.symbol for _tx, security in transactions} == {"AMT", "CVS", "INTC", "PWR", "TMO"}
        assert {tx.report_date for tx, _security in transactions} == {date(2026, 5, 15)}
        assert {tx.trade_date for tx, security in transactions if security.symbol == "CVS"} == {date(2025, 11, 21)}
    finally:
        db.close()


def test_congress_event_projection_is_transaction_level_and_idempotent(monkeypatch):
    Session = _session_factory()
    rows = _evans_rows()
    _patch_house_source(monkeypatch, Session, rows)
    house_module.ingest_house(pages=1, limit=100, sleep_s=0)

    db = Session()
    try:
        inserted = insert_missing_congress_events_from_transactions(db)
        db.commit()
        assert inserted == 5
        assert db.query(Event).filter(Event.event_type == "congress_trade").count() == 5

        inserted_again = insert_missing_congress_events_from_transactions(db)
        db.commit()
        assert inserted_again == 0
        assert db.query(Event).filter(Event.event_type == "congress_trade").count() == 5

        events = db.execute(select(Event).where(Event.event_type == "congress_trade")).scalars().all()
        assert {event.symbol for event in events} == {"AMT", "CVS", "INTC", "PWR", "TMO"}
        cvs = next(event for event in events if event.symbol == "CVS")
        payload = json.loads(cvs.payload_json)
        assert payload["trade_date"] == "2025-11-21"
        assert payload["report_date"] == "2026-05-15"
        assert payload["filing_date"] == "2026-05-15"
        assert cvs.event_date.date() == date(2026, 5, 15)
    finally:
        db.close()
