from __future__ import annotations

import json
from datetime import datetime, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from app.db import Base
from app.models import Event
from app.routers.events import global_search


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
        assert insider["route"] == "/insider/tim-cook-0001214156"
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

        assert any(item["type"] == "insider" and item["id"] == "0001214156" for item in apple_payload["results"])
        assert any(item["type"] == "insider" and item["id"] == "0001214156" for item in ticker_payload["results"])
    finally:
        db.close()
