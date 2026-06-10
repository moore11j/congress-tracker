from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db import Base
from app.models import InsiderTransaction, Member, Security, TickerMeta, Watchlist, WatchlistItem
from app.services.search_suggest import search_suggestions


def _db():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    return Session()


def test_search_suggest_exact_symbol_ranks_first_and_respects_limit():
    db = _db()
    try:
        db.add_all(
            [
                Security(symbol="AAPL", name="Apple Inc.", asset_class="stock", sector="Technology"),
                Security(symbol="AAP", name="Advance Auto Parts", asset_class="stock", sector="Consumer"),
                TickerMeta(symbol="AAPL", company_name="Apple Inc.", exchange="NASDAQ"),
            ]
        )
        db.commit()

        payload = search_suggestions(db, "aapl", limit=1)

        assert len(payload["items"]) == 1
        assert payload["items"][0]["kind"] == "ticker"
        assert payload["items"][0]["symbol"] == "AAPL"
        assert payload["items"][0]["href"] == "/ticker/AAPL"
    finally:
        db.close()


def test_search_suggest_symbol_prefix_beats_name_contains():
    db = _db()
    try:
        db.add_all(
            [
                Security(symbol="TSM", name="Taiwan Semiconductor Manufacturing", asset_class="stock", sector="Technology"),
                Security(symbol="ABC", name="TSM Holdings", asset_class="stock", sector="Industrial"),
            ]
        )
        db.commit()

        items = search_suggestions(db, "ts", limit=5)["items"]

        assert items[0]["symbol"] == "TSM"
    finally:
        db.close()


def test_search_suggest_includes_members_after_tickers():
    db = _db()
    try:
        db.add_all(
            [
                Security(symbol="PELO", name="Peloton Adjacent", asset_class="stock", sector=None),
                Member(bioguide_id="P000197", first_name="Nancy", last_name="Pelosi", chamber="house", party="D", state="CA"),
                InsiderTransaction(
                    source="test",
                    external_id="insider-1",
                    filing_date=datetime.now(timezone.utc).date(),
                    transaction_date=datetime.now(timezone.utc).date(),
                    payload_json="{}",
                    insider_name="Jane Pelosi",
                    reporting_cik="0001234567",
                    symbol="AAPL",
                ),
            ]
        )
        db.commit()

        items = search_suggestions(db, "pel", limit=8)["items"]
        kinds = [item["kind"] for item in items]

        assert "ticker" in kinds
        assert "member" in kinds
    finally:
        db.close()


def test_search_suggest_finds_company_name_typo():
    db = _db()
    try:
        db.add_all(
            [
                Security(symbol="MSFT", name="Microsoft Corporation", asset_class="stock", sector="Technology"),
                Security(symbol="MSTR", name="MicroStrategy Incorporated", asset_class="stock", sector="Technology"),
                TickerMeta(symbol="MSFT", company_name="Microsoft Corporation", exchange="NASDAQ"),
            ]
        )
        db.commit()

        items = search_suggestions(db, "microsft", limit=5)["items"]

        assert items[0]["kind"] == "ticker"
        assert items[0]["symbol"] == "MSFT"
    finally:
        db.close()


def test_search_suggest_finds_member_last_name_typo():
    db = _db()
    try:
        db.add_all(
            [
                Member(bioguide_id="P000197", first_name="Nancy", last_name="Pelosi", chamber="house", party="D", state="CA"),
                Member(bioguide_id="P000608", first_name="Gary", last_name="Palmer", chamber="house", party="R", state="AL"),
            ]
        )
        db.commit()

        items = search_suggestions(db, "pelsoi", limit=5)["items"]

        assert items[0]["kind"] == "member"
        assert items[0]["label"] == "Nancy Pelosi"
    finally:
        db.close()


def test_search_suggest_personalizes_watchlist_symbols():
    db = _db()
    try:
        tesla = Security(symbol="TSLA", name="Tesla Inc.", asset_class="stock", sector="Consumer")
        test = Security(symbol="TEST", name="Test Industries", asset_class="stock", sector="Industrial")
        db.add_all([tesla, test])
        db.flush()
        watchlist = Watchlist(id=101, name="My AI basket", owner_user_id=42)
        db.add(watchlist)
        db.flush()
        db.add(WatchlistItem(watchlist_id=watchlist.id, security_id=tesla.id))
        db.commit()

        anonymous_items = search_suggestions(db, "tes", limit=5)["items"]
        personalized_items = search_suggestions(db, "tes", limit=5, user_id=42)["items"]

        assert anonymous_items[0]["symbol"] == "TEST"
        assert personalized_items[0]["symbol"] == "TSLA"
    finally:
        db.close()
