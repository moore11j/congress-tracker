from __future__ import annotations

import json
from datetime import datetime, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from starlette.requests import Request

from app.db import Base
from app.models import Event, InsiderTransactionNormalized, TickerMeta
import app.routers.events as events_router
from app.routers.events import global_search, insider_summary, insider_trades, suggest_member_insider


def _db() -> Session:
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)
    return Session(engine)


def _request(path: str = "/", headers: dict[str, str] | None = None) -> Request:
    raw_headers = [
        (key.lower().encode("latin-1"), value.encode("latin-1"))
        for key, value in (headers or {}).items()
    ]
    return Request({"type": "http", "method": "GET", "path": path, "headers": raw_headers})


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


def test_global_search_finds_normalized_form4_insider_without_event():
    db = _db()
    try:
        db.add(
            InsiderTransactionNormalized(
                accession_number="0000320193-26-000001",
                issuer_name="Apple Inc.",
                ticker_normalized="AAPL",
                reporting_owner_cik="0001214156",
                reporting_owner_name="Tim Cook",
                officer_title="Chief Executive Officer",
                transaction_date=datetime(2026, 4, 1, tzinfo=timezone.utc).date(),
                filing_date=datetime(2026, 4, 2, tzinfo=timezone.utc).date(),
                normalized_hash="tim-cook-aapl-global",
            )
        )
        db.commit()

        payload = global_search(db=db, q="Tim Cook", limit=8)

        insider = next(item for item in payload["results"] if item["type"] == "insider")
        assert insider["label"] == "Tim Cook"
        assert insider["symbol"] == "AAPL"
        assert insider["route"] == "/insider/tim-cook-0001214156?issuer=AAPL"
    finally:
        db.close()


def test_member_insider_suggest_finds_normalized_form4_insider_without_event():
    db = _db()
    try:
        db.add(
            InsiderTransactionNormalized(
                accession_number="0000320193-26-000003",
                issuer_name="Apple Inc.",
                ticker_normalized="AAPL",
                reporting_owner_cik="0001214156",
                reporting_owner_name="Tim Cook",
                officer_title="Chief Executive Officer",
                transaction_date=datetime(2026, 4, 1, tzinfo=timezone.utc).date(),
                filing_date=datetime(2026, 4, 2, tzinfo=timezone.utc).date(),
                normalized_hash="tim-cook-aapl-member-insider",
            )
        )
        db.commit()

        payload = suggest_member_insider(db=db, q="1214156", limit=8)

        insider = next(item for item in payload["items"] if item["category"] == "insider")
        assert insider["value"] == "Tim Cook"
        assert insider["reporting_cik"] == "0001214156"
        assert insider["symbol"] == "AAPL"
        assert insider["company_name"] == "Apple Inc."
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


def test_insider_summary_uses_normalized_form4_identity_without_event():
    db = _db()
    try:
        events_router._INSIDER_SUMMARY_CACHE.clear()
        db.add(TickerMeta(symbol="AAPL", company_name="Apple Inc.", exchange="NASDAQ"))
        db.add(
            InsiderTransactionNormalized(
                accession_number="0000320193-26-000002",
                issuer_name="APPLE INC.",
                ticker_normalized="AAPL",
                reporting_owner_cik="0001214156",
                reporting_owner_name="Tim Cook",
                officer_title="Chief Executive Officer",
                transaction_date=datetime(2026, 4, 1, tzinfo=timezone.utc).date(),
                filing_date=datetime(2026, 4, 2, tzinfo=timezone.utc).date(),
                normalized_hash="tim-cook-aapl-summary",
            )
        )
        db.commit()

        summary = insider_summary(request=_request(), db=db, reporting_cik="0001214156", lookback_days=90)

        assert summary["status"] == "identity_only"
        assert summary["insider_name"] == "Tim Cook"
        assert summary["primary_symbol"] == "AAPL"
        assert summary["primary_company_name"] == "Apple Inc."
        assert summary["primary_role"] == "Chief Executive Officer"
        assert summary["total_trades"] == 0
        assert summary["role_contexts"] == [
            {
                "symbol": "AAPL",
                "issuer_cik": None,
                "company_name": "Apple Inc.",
                "role": "Chief Executive Officer",
                "filings": 1,
                "latest_filing_date": "2026-04-02",
                "latest_transaction_date": "2026-04-01",
            }
        ]
    finally:
        events_router._INSIDER_SUMMARY_CACHE.clear()
        db.close()


def test_insider_summary_prefers_executive_issuer_for_normalized_identity():
    db = _db()
    try:
        events_router._INSIDER_SUMMARY_CACHE.clear()
        db.add_all(
            [
                TickerMeta(symbol="AAPL", company_name="Apple Inc.", exchange="NASDAQ"),
                TickerMeta(symbol="NKE", company_name="Nike Inc.", exchange="NYSE"),
                InsiderTransactionNormalized(
                    accession_number="0000320193-26-000004",
                    issuer_name="Apple Inc.",
                    ticker_normalized="AAPL",
                    reporting_owner_cik="0001214156",
                    reporting_owner_name="Tim Cook",
                    officer_title="Chief Executive Officer",
                    transaction_date=datetime(2026, 3, 1, tzinfo=timezone.utc).date(),
                    filing_date=datetime(2026, 3, 2, tzinfo=timezone.utc).date(),
                    normalized_hash="tim-cook-aapl-ceo-summary",
                ),
                InsiderTransactionNormalized(
                    accession_number="0000320187-26-000004",
                    issuer_name="Nike Inc.",
                    ticker_normalized="NKE",
                    reporting_owner_cik="0001214156",
                    reporting_owner_name="Tim Cook",
                    is_director=True,
                    transaction_date=datetime(2026, 4, 1, tzinfo=timezone.utc).date(),
                    filing_date=datetime(2026, 4, 2, tzinfo=timezone.utc).date(),
                    normalized_hash="tim-cook-nke-director-summary",
                ),
            ]
        )
        db.commit()

        summary = insider_summary(request=_request(), db=db, reporting_cik="0001214156", lookback_days=90)

        assert summary["primary_symbol"] == "AAPL"
        assert summary["primary_company_name"] == "Apple Inc."
        assert summary["primary_role"] == "Chief Executive Officer"
        contexts_by_symbol = {context["symbol"]: context for context in summary["role_contexts"]}
        assert contexts_by_symbol["AAPL"]["role"] == "Chief Executive Officer"
        assert contexts_by_symbol["NKE"]["role"] == "Director"
        assert contexts_by_symbol["NKE"]["company_name"] == "Nike Inc."
    finally:
        events_router._INSIDER_SUMMARY_CACHE.clear()
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

        request = _request()
        summary = insider_summary(request=request, db=db, reporting_cik="0001214156", lookback_days=90, issuer="AAPL")
        trades = insider_trades(request=request, db=db, reporting_cik="0001214156", lookback_days=90, issuer="AAPL", limit=10)

        assert summary["primary_symbol"] == "AAPL"
        assert summary["primary_company_name"] == "Apple Inc."
        assert [item["symbol"] for item in trades["items"]] == ["AAPL"]
    finally:
        db.close()


def test_insider_summary_guest_ssr_preserves_identity_fields():
    db = _db()
    try:
        now = datetime.now(timezone.utc)
        db.add(
            Event(
                id=30,
                event_type="insider_trade",
                ts=now,
                event_date=now,
                symbol="NKE",
                source="test",
                member_name="Tim Cook",
                trade_type="purchase",
                amount_max=1060750,
                payload_json=json.dumps(
                    {
                        "reporting_cik": "0001214156",
                        "company_name": "Nike Inc.",
                        "role": "Director",
                        "transaction_date": now.date().isoformat(),
                    }
                ),
            )
        )
        db.commit()

        summary = insider_summary(
            request=_request(
                "/api/insiders/0001214156/summary",
                {"x-walnut-request-source": "ssr"},
            ),
            db=db,
            reporting_cik="0001214156",
            lookback_days=90,
        )

        assert summary["status"] == "identity_only"
        assert summary["insider_name"] == "Tim Cook"
        assert summary["primary_symbol"] == "NKE"
        assert summary["primary_company_name"] == "Nike Inc."
        assert summary["primary_role"] == "Director"
        assert summary["total_trades"] == 1
    finally:
        db.close()
