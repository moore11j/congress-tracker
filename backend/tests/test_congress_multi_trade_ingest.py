from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

import app.ingest_house as house_module
from app.backfill_events_from_trades import insert_missing_congress_events_from_transactions
from app.db import Base
from app.models import Event, Filing, GovernmentContractAction, Member, Security, Transaction
from app.routers.events import list_events, list_ticker_events
from app.services.congress_assets import parse_treasury_details
from scripts.ops import reprocess_recent_non_equity_disclosures as reprocess_non_equity


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
            GovernmentContractAction.__table__,
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
        assert payload["symbol"] == "CVS"
        assert payload["ticker"] == "CVS"
        assert payload["company_name"] == "CVS Health Corp"
        assert payload["issuer_name"] == "CVS Health Corp"
        assert payload["security_description"] == "CVS Health Corp"
        assert payload["instrument_type"] == "equity"
        assert payload["symbol"] != payload["event_type"]
        assert payload["company_name"] != payload["event_type"]
        assert cvs.event_date.date() == date(2026, 5, 15)
    finally:
        db.close()


def test_house_recent_ingest_filters_by_report_date_not_trade_date(monkeypatch):
    Session = _session_factory()
    today = datetime.now(timezone.utc).date()
    stale_report = today - timedelta(days=30)
    rows = [
        {
            "firstName": "Bill",
            "lastName": "Fresh",
            "office": "Bill Fresh",
            "district": "MA09",
            "party": "D",
            "disclosureDate": today.isoformat(),
            "link": "https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/2026/20034417.pdf",
            "type": "purchase",
            "owner": "self",
            "amount": "$1,001 - $15,000",
            "symbol": "JPM",
            "assetDescription": "JPMorgan Chase & Co",
            "transactionDate": (today - timedelta(days=90)).isoformat(),
        },
        {
            "firstName": "Bill",
            "lastName": "Fresh",
            "office": "Bill Fresh",
            "district": "MA09",
            "party": "D",
            "disclosureDate": stale_report.isoformat(),
            "link": "https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/2026/20030000.pdf",
            "type": "purchase",
            "owner": "self",
            "amount": "$1,001 - $15,000",
            "symbol": "NOC",
            "assetDescription": "Northrop Grumman Corp",
            "transactionDate": today.isoformat(),
        },
    ]
    _patch_house_source(monkeypatch, Session, rows)

    result = house_module.ingest_house(pages=1, limit=100, sleep_s=0, recent_days=7)

    assert result["inserted"] == 1
    assert result["skipped_old"] == 1
    db = Session()
    try:
        transactions = (
            db.execute(select(Transaction, Security).join(Security, Security.id == Transaction.security_id))
            .all()
        )
        assert [(tx.report_date, tx.trade_date, security.symbol) for tx, security in transactions] == [
            (today, today - timedelta(days=90), "JPM")
        ]
    finally:
        db.close()


def test_keating_style_multi_row_filing_projects_each_public_equity_identity(monkeypatch):
    Session = _session_factory()
    symbols = {
        "JPM": "JPMorgan Chase & Co",
        "NOC": "Northrop Grumman Corp",
        "OTIS": "Otis Worldwide Corp",
        "SPG": "Simon Property Group Inc",
        "TMUS": "T-Mobile US Inc",
        "CPB": "Campbell Soup Co",
        "VMW": "VMware Inc",
    }
    rows = [
        {
            "firstName": "William",
            "lastName": "Keating",
            "office": "William Keating",
            "district": "MA09",
            "party": "D",
            "disclosureDate": "2026-05-19",
            "link": "https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/2026/20034417.pdf",
            "type": "sale",
            "owner": "self",
            "amount": "$1,001 - $15,000",
            "symbol": symbol,
            "assetDescription": company,
            "transactionDate": "2026-05-01",
        }
        for symbol, company in symbols.items()
    ]
    _patch_house_source(monkeypatch, Session, rows)
    house_module.ingest_house(pages=1, limit=100, sleep_s=0)

    db = Session()
    try:
        inserted = insert_missing_congress_events_from_transactions(db, since_report_date=date(2026, 5, 14))
        db.commit()
        assert inserted == len(symbols)
        events = db.execute(select(Event).where(Event.event_type == "congress_trade")).scalars().all()
        assert {event.symbol for event in events} == set(symbols)
        for event in events:
            payload = json.loads(event.payload_json)
            assert payload["symbol"] == event.symbol
            assert payload["ticker"] == event.symbol
            assert payload["company_name"] == symbols[event.symbol]
            assert payload["security_name"] == symbols[event.symbol]
            assert payload["event_type"] == "congress_trade"
    finally:
        db.close()


def test_non_equity_rows_do_not_create_fake_ticker_events(monkeypatch):
    Session = _session_factory()
    today = datetime.now(timezone.utc).date()
    rows = [
        {
            "firstName": "Bill",
            "lastName": "Fresh",
            "office": "Bill Fresh",
            "district": "MA09",
            "party": "D",
            "disclosureDate": today.isoformat(),
            "link": "https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/2026/20034418.pdf",
            "type": "purchase",
            "owner": "self",
            "amount": "$1,001 - $15,000",
            "symbol": "UST",
            "assetDescription": "US Treasury Bills",
            "assetType": "Government Security",
            "transactionDate": today.isoformat(),
        }
    ]
    _patch_house_source(monkeypatch, Session, rows)
    result = house_module.ingest_house(pages=1, limit=100, sleep_s=0, recent_days=7)
    assert result["inserted"] == 1
    assert result["non_equity_symbol_skipped"] == 1

    db = Session()
    try:
        inserted = insert_missing_congress_events_from_transactions(db, recent_days=7)
        db.commit()
        assert inserted == 1
        tx = db.execute(select(Transaction)).scalar_one()
        event = db.execute(select(Event)).scalar_one()
        assert tx.security_id is None
        assert event.symbol is None
        assert event.event_type == "congress_treasury_trade"
        payload = json.loads(event.payload_json)
        assert payload["asset_class"] == "treasury"
        assert payload["issuer_name"] == "U.S. Treasury"
        assert payload["ticker"] is None
        assert "Treasury" in payload["description"]
        assert db.query(Security).count() == 0
    finally:
        db.close()


def test_treasury_duration_extraction_patterns():
    assert parse_treasury_details("4 Week Treasury Bill")["duration_label"] == "4W"
    assert parse_treasury_details("13 Week Treasury Bill")["duration_label"] == "13W"
    assert parse_treasury_details("26 Week Treasury Bill")["duration_days"] == 182
    assert parse_treasury_details("Treasury Bill due 07/15/2026")["maturity_date"] == "2026-07-15"
    unknown = parse_treasury_details("U.S. Treasury Bills")
    assert unknown["instrument_type"] == "treasury_bill"
    assert unknown["duration_label"] is None
    assert unknown["maturity_date"] is None


def test_treasury_event_appears_in_congress_feed_but_not_ticker_events(monkeypatch):
    Session = _session_factory()
    today = datetime.now(timezone.utc).date()
    rows = [
        {
            "firstName": "Scott",
            "lastName": "Peters",
            "office": "Scott Peters",
            "district": "CA50",
            "party": "D",
            "disclosureDate": today.isoformat(),
            "link": "https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/2026/20034419.pdf",
            "type": "sale",
            "owner": "self",
            "amount": "$50,001 - $100,000",
            "symbol": "UST",
            "assetDescription": "13 Week Treasury Bill",
            "assetType": "Government Security",
            "transactionDate": today.isoformat(),
        }
    ]
    _patch_house_source(monkeypatch, Session, rows)
    house_module.ingest_house(pages=1, limit=100, sleep_s=0, recent_days=7)

    db = Session()
    try:
        inserted = insert_missing_congress_events_from_transactions(db, recent_days=7)
        db.commit()
        assert inserted == 1

        congress_page = list_events(db=db, tape="congress", limit=10)
        assert len(congress_page.items) == 1
        item = congress_page.items[0]
        assert item.event_type == "congress_treasury_trade"
        assert item.symbol is None
        assert item.payload["duration_label"] == "13W"
        assert item.payload["asset_class"] == "treasury"

        treasury_page = list_events(db=db, tape="congress", asset_class="treasury", limit=10)
        assert [event.event_type for event in treasury_page.items] == ["congress_treasury_trade"]

        ticker_page = list_ticker_events(symbol="UST", db=db, limit=10)
        assert ticker_page.items == []
    finally:
        db.close()


def test_direct_crypto_creates_non_ticker_disclosure_but_crypto_etf_stays_linked(monkeypatch):
    Session = _session_factory()
    today = datetime.now(timezone.utc).date()
    base = {
        "firstName": "Crypto",
        "lastName": "Holder",
        "office": "Crypto Holder",
        "district": "NY01",
        "party": "R",
        "disclosureDate": today.isoformat(),
        "link": "https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/2026/20034420.pdf",
        "type": "purchase",
        "owner": "self",
        "amount": "$1,001 - $15,000",
        "transactionDate": today.isoformat(),
    }
    rows = [
        {**base, "symbol": "BTC", "assetDescription": "Bitcoin", "assetType": "Cryptocurrency"},
        {**base, "symbol": "ETH", "assetDescription": "Ethereum", "assetType": "Cryptocurrency"},
        {**base, "symbol": "IBIT", "assetDescription": "iShares Bitcoin Trust ETF", "assetType": "ETF"},
    ]
    _patch_house_source(monkeypatch, Session, rows)
    house_module.ingest_house(pages=1, limit=100, sleep_s=0, recent_days=7)

    db = Session()
    try:
        inserted = insert_missing_congress_events_from_transactions(db, recent_days=7)
        db.commit()
        assert inserted == 3

        crypto_events = db.execute(
            select(Event).where(Event.event_type == "congress_crypto_trade").order_by(Event.id)
        ).scalars().all()
        assert len(crypto_events) == 2
        assert {event.symbol for event in crypto_events} == {None}
        assert {json.loads(event.payload_json)["symbol"] for event in crypto_events} == {"BTC", "ETH"}

        ibit_event = db.execute(select(Event).where(Event.symbol == "IBIT")).scalar_one()
        assert ibit_event.event_type == "congress_trade"
        assert db.execute(select(Security.symbol)).scalars().all() == ["IBIT"]

        crypto_page = list_events(db=db, tape="congress", asset_class="crypto", limit=10)
        assert len(crypto_page.items) == 2
        assert {item.payload["symbol"] for item in crypto_page.items} == {"BTC", "ETH"}
        assert list_ticker_events(symbol="BTC", db=db, limit=10).items == []
    finally:
        db.close()


def test_recent_non_equity_reprocess_inserts_treasury_event_idempotently(monkeypatch):
    Session = _session_factory()
    today = date(2026, 5, 15)
    db = Session()
    try:
        member = Member(
            bioguide_id="P000608",
            first_name="Scott",
            last_name="Peters",
            chamber="house",
            party="D",
            state="CA",
        )
        db.add(member)
        db.flush()
        filing = Filing(
            member_id=member.id,
            filing_date=today,
            document_url="https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/2026/20034419.pdf",
            source="house_fmp",
        )
        db.add(filing)
        db.flush()
        tx = Transaction(
            filing_id=filing.id,
            member_id=member.id,
            security_id=None,
            owner_type="self",
            transaction_type="purchase",
            trade_date=date(2026, 5, 10),
            report_date=today,
            amount_range_min=1001,
            amount_range_max=15000,
            description="13 Week U.S. Treasury Bills",
        )
        db.add(tx)
        db.commit()
    finally:
        db.close()

    monkeypatch.setattr(reprocess_non_equity, "SessionLocal", Session)

    dry = reprocess_non_equity.run(since_report_date=date(2026, 5, 1), apply=False)
    assert dry["insertable"] == 1
    apply = reprocess_non_equity.run(since_report_date=date(2026, 5, 1), apply=True)
    assert apply["inserted"] == 1
    dry_again = reprocess_non_equity.run(since_report_date=date(2026, 5, 1), apply=False)
    assert dry_again["insertable"] == 0

    db = Session()
    try:
        event = db.execute(select(Event)).scalar_one()
        assert event.event_type == "congress_treasury_trade"
        assert event.symbol is None
        assert json.loads(event.payload_json)["asset_class"] == "treasury"
    finally:
        db.close()
