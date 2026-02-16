from __future__ import annotations

import json
from datetime import datetime, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db import Base
from app.main import feed
from app.routers.events import list_events
from app.models import Event, InsiderTransaction


def main() -> None:
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)

    with Session() as db:
        raw = InsiderTransaction(
            source="fmp",
            external_id="test-insider-1",
            symbol="NVDA",
            reporting_cik="0001234567",
            insider_name="Test Insider",
            transaction_type="P-Purchase",
            role="CEO",
            ownership="Direct",
            payload_json=json.dumps({"symbol": "NVDA"}),
        )
        db.add(raw)

        event = Event(
            event_type="insider_trade",
            ts=datetime.now(timezone.utc),
            event_date=datetime.now(timezone.utc),
            symbol="NVDA",
            source="fmp",
            transaction_type="P-Purchase",
            trade_type="p-purchase",
            payload_json=json.dumps(
                {
                    "external_id": raw.external_id,
                    "insider_name": raw.insider_name,
                    "transaction_date": "2026-01-01",
                    "filing_date": "2026-01-02",
                    "role": "CEO",
                    "ownership": "Direct",
                }
            ),
            impact_score=0.0,
        )
        db.add(event)
        db.commit()

        insider_events = list_events(
            db=db,
            tape="insider",
            role="ceo",
            ownership="direct",
            limit=10,
            min_amount=None,
            max_amount=None,
            whale=None,
            recent_days=None,
            offset=0,
            include_total=False,
            cursor=None,
            debug=None,
        )
        assert insider_events.items, "Expected insider /api/events rows with role+ownership filter"

        insider_feed = feed(db=db, tape="insider", limit=10)
        assert insider_feed["items"], "Expected insider feed items"
        assert insider_feed["items"][0]["event_type"] == "insider_trade"

        congress_feed = feed(db=db, tape="congress", limit=10)
        assert congress_feed["items"] == [], "Expected no congress items in this test"

        all_feed = feed(db=db, tape="all", limit=10)
        types = {item["event_type"] for item in all_feed["items"]}
        assert "insider_trade" in types

    print("Insider feed smoke checks passed.")


if __name__ == "__main__":
    main()
