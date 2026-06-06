from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db import Base
from app.models import InsiderTransaction, Member, Security, TickerMeta
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
