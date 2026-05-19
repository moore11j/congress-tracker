from __future__ import annotations

import json
from datetime import datetime, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from app.db import Base
from app.models import Event
from app.routers.events import global_search, insider_summary, insider_trades


def _db() -> Session:
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)
    return Session(engine)


def test_global_search_returns_insider_result_and_route():
    db = _db()
    try:
        now = datetime(2026, 5, 19, tzinfo=timezone.utc)
        db.add(
            Event(
                id=1,
                event_type="insider_trade",
                ts=now,
                event_date=now,
                symbol="AAPL",
                source="test",
                member_name=None,
                payload_json=json.dumps(
                    {
                        "insider_name": "Tim Cook",
                        "reporting_cik": "0001214156",
                        "company_name": "Apple Inc.",
                        "officerTitle": "Chief Executive Officer",
                    }
                ),
            )
        )
        db.commit()

        payload = global_search(db=db, q="Tim Cook", limit=8)

        insider = next(item for item in payload["results"] if item["type"] == "insider")
        assert insider["label"] == "Tim Cook"
        assert insider["route"] == "/insider/tim-cook-0001214156?issuer=AAPL"
        assert "Apple Inc." in insider["subtitle"]
        assert "AAPL" in insider["subtitle"]
    finally:
        db.close()


def test_global_search_matches_company_role_and_ticker_context_for_insiders():
    db = _db()
    try:
        now = datetime(2026, 5, 19, tzinfo=timezone.utc)
        db.add(
            Event(
                id=2,
                event_type="insider_trade",
                ts=now,
                event_date=now,
                symbol="AAPL",
                source="test",
                member_name="Timothy D Cook",
                payload_json=json.dumps(
                    {
                        "reporting_cik": "0001214156",
                        "company_name": "Apple Inc.",
                        "role": "CEO",
                    }
                ),
            )
        )
        db.commit()

        apple_payload = global_search(db=db, q="Apple CEO", limit=8)
        ticker_payload = global_search(db=db, q="AAPL Cook", limit=8)

        assert any(item["type"] == "insider" and item["id"] == "0001214156:AAPL" for item in apple_payload["results"])
        assert any(item["type"] == "insider" and item["id"] == "0001214156:AAPL" for item in ticker_payload["results"])
    finally:
        db.close()


def test_global_search_returns_company_scoped_results_for_same_insider():
    db = _db()
    try:
        now = datetime(2026, 5, 19, tzinfo=timezone.utc)
        db.add_all(
            [
                Event(
                    id=10,
                    event_type="insider_trade",
                    ts=now,
                    event_date=now,
                    symbol="AAPL",
                    source="test",
                    member_name="Tim Cook",
                    payload_json=json.dumps({"reporting_cik": "0001214156", "company_name": "Apple Inc.", "role": "CEO"}),
                ),
                Event(
                    id=11,
                    event_type="insider_trade",
                    ts=now,
                    event_date=now,
                    symbol="NKE",
                    source="test",
                    member_name="Tim Cook",
                    payload_json=json.dumps({"reporting_cik": "0001214156", "company_name": "Nike Inc.", "role": "Director"}),
                ),
            ]
        )
        db.commit()

        payload = global_search(db=db, q="Tim Cook", limit=8)
        insider_routes = [item["route"] for item in payload["results"] if item["type"] == "insider"]

        assert "/insider/tim-cook-0001214156?issuer=AAPL" in insider_routes
        assert "/insider/tim-cook-0001214156?issuer=NKE" in insider_routes
    finally:
        db.close()


def test_company_scoped_insider_endpoints_filter_trades_by_issuer():
    db = _db()
    try:
        now = datetime.now(timezone.utc)
        db.add_all(
            [
                Event(
                    id=20,
                    event_type="insider_trade",
                    ts=now,
                    event_date=now,
                    symbol="AAPL",
                    source="test",
                    member_name="Tim Cook",
                    trade_type="purchase",
                    amount_max=1000,
                    payload_json=json.dumps({"reporting_cik": "0001214156", "company_name": "Apple Inc.", "role": "CEO"}),
                ),
                Event(
                    id=21,
                    event_type="insider_trade",
                    ts=now,
                    event_date=now,
                    symbol="NKE",
                    source="test",
                    member_name="Tim Cook",
                    trade_type="sale",
                    amount_max=2000,
                    payload_json=json.dumps({"reporting_cik": "0001214156", "company_name": "Nike Inc.", "role": "Director"}),
                ),
            ]
        )
        db.commit()

        summary = insider_summary(db=db, reporting_cik="0001214156", lookback_days=90, issuer="AAPL")
        trades = insider_trades(db=db, reporting_cik="0001214156", lookback_days=90, issuer="AAPL", limit=10)

        assert summary["primary_symbol"] == "AAPL"
        assert summary["primary_company_name"] == "Apple Inc."
        assert [item["symbol"] for item in trades["items"]] == ["AAPL"]
    finally:
        db.close()
