from __future__ import annotations

import sys
from datetime import date

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from app.backfill_events_from_trades import _congress_event_from_transaction
from app.db import Base
from app.ingest_house import upsert_house_transaction_from_row
from app.models import Event, Filing, Member, Security, Transaction
from scripts.ops import backfill_missing_congress_multi_trade_events as ops


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


def _evans_rows(document: str = "20034556") -> list[dict]:
    base = {
        "firstName": "Dwight",
        "lastName": "Evans",
        "office": "Dwight Evans",
        "district": "PA03",
        "party": "D",
        "disclosureDate": "2026-05-18",
        "link": f"https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/2026/{document}.pdf",
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


def _patch_ops(monkeypatch, session_factory, rows: list[dict]) -> None:
    monkeypatch.setattr(ops, "SessionLocal", session_factory)
    monkeypatch.setattr(ops, "get_congress_metadata_resolver", lambda: _NoopCongressMetadata())
    monkeypatch.setattr(ops, "fetch_house_page", lambda page, limit: rows if page == 0 else [])
    monkeypatch.setattr(ops, "fetch_senate_page", lambda page, limit: [])


def _seed_existing_amt(session_factory, row: dict) -> None:
    db = session_factory()
    try:
        outcome = upsert_house_transaction_from_row(
            db,
            row,
            metadata=_NoopCongressMetadata(),
            seen_transaction_keys=set(),
        )
        db.add(
            _congress_event_from_transaction(
                outcome["transaction"],
                outcome["filing"],
                outcome["member"],
                outcome["security"],
            )
        )
        db.commit()
    finally:
        db.close()


def test_targeted_document_mode_recovers_missing_evans_rows_without_duplicate_amt(monkeypatch):
    Session = _session_factory()
    rows = _evans_rows()
    _patch_ops(monkeypatch, Session, rows)
    _seed_existing_amt(Session, rows[0])

    dry_run = ops.run_document_repair(
        document="20034556.pdf",
        apply=False,
        member="Dwight Evans",
        symbols={"AMT", "CVS", "INTC", "PWR", "TMO"},
        pages=1,
        limit=100,
    )
    assert dry_run["parsed_transaction_count"] == 5
    assert dry_run["totals"]["transactions_to_insert"] == 4
    assert dry_run["totals"]["events_to_insert"] == 4
    by_symbol = {row["symbol"]: row for row in dry_run["rows"]}
    assert by_symbol["AMT"]["existing_transaction"] is True
    assert by_symbol["AMT"]["existing_event"] is True
    assert by_symbol["CVS"]["trade_date"] == "2025-11-21"

    db = Session()
    try:
        assert db.query(Transaction).count() == 1
        assert db.query(Event).filter(Event.event_type == "congress_trade").count() == 1
    finally:
        db.close()

    applied = ops.run_document_repair(
        document="20034556.pdf",
        apply=True,
        member="Dwight Evans",
        symbols={"AMT", "CVS", "INTC", "PWR", "TMO"},
        pages=1,
        limit=100,
    )
    assert applied["totals"]["transactions_to_insert"] == 4
    assert applied["totals"]["events_inserted"] == 4

    db = Session()
    try:
        tx_rows = db.execute(select(Transaction, Security).join(Security, Security.id == Transaction.security_id)).all()
        assert {security.symbol for _tx, security in tx_rows} == {"AMT", "CVS", "INTC", "PWR", "TMO"}
        assert db.query(Transaction).count() == 5
        assert db.query(Event).filter(Event.event_type == "congress_trade").count() == 5
        cvs = next(tx for tx, security in tx_rows if security.symbol == "CVS")
        assert cvs.trade_date == date(2025, 11, 21)
        assert cvs.report_date == date(2026, 5, 18)
    finally:
        db.close()

    rerun = ops.run_document_repair(
        document="20034556.pdf",
        apply=False,
        member="Dwight Evans",
        symbols={"AMT", "CVS", "INTC", "PWR", "TMO"},
        pages=1,
        limit=100,
    )
    assert rerun["totals"]["transactions_to_insert"] == 0
    assert rerun["totals"]["events_to_insert"] == 0


def test_targeted_document_mode_only_touches_requested_document(monkeypatch):
    Session = _session_factory()
    requested_rows = _evans_rows("20034556")
    other_rows = _evans_rows("20039999")
    other_rows[0] = {**other_rows[0], "symbol": "MSFT", "assetDescription": "Microsoft Corp"}
    _patch_ops(monkeypatch, Session, requested_rows + other_rows)

    result = ops.run_document_repair(
        document="20034556.pdf",
        apply=True,
        member="Dwight Evans",
        symbols={"AMT", "CVS", "INTC", "PWR", "TMO"},
        pages=1,
        limit=100,
    )
    assert result["parsed_transaction_count"] == 5

    db = Session()
    try:
        symbols = {
            symbol
            for (symbol,) in db.execute(select(Security.symbol).order_by(Security.symbol))
        }
        assert symbols == {"AMT", "CVS", "INTC", "PWR", "TMO"}
    finally:
        db.close()


def test_apply_without_document_or_explicit_broad_flags_refuses(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["backfill_missing_congress_multi_trade_events.py", "--apply"])
    with pytest.raises(SystemExit):
        ops.main()
