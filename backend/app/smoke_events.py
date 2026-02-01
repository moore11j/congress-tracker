from __future__ import annotations

import json
from datetime import datetime, timezone

from app.db import SessionLocal
from app.models import Event


"""
Quick smoke helper:
  python3 -m app.smoke_events

Then try:
  curl "http://localhost:8000/api/events?limit=10"
  curl "http://localhost:8000/api/tickers/NVDA/events?limit=10"
  curl "http://localhost:8000/api/watchlists/1/events?limit=10"
"""


def seed_events() -> None:
    db = SessionLocal()
    try:
        now = datetime.now(timezone.utc)
        sample_events = [
            Event(
                event_type="congress_trade",
                ts=now,
                event_date=now,
                ticker="NVDA",
                symbol="NVDA",
                source="house",
                headline="Demo trade",
                summary="Representative purchased shares.",
                url="https://example.com/trade",
                impact_score=0.25,
                payload_json=json.dumps({"trade_id": "demo-1"}),
            ),
            Event(
                event_type="news",
                ts=now,
                event_date=now,
                ticker="AAPL",
                symbol="AAPL",
                source="news_vendor",
                headline="Demo news",
                summary="Sample headline for testing.",
                url="https://example.com/news",
                impact_score=0.1,
                payload_json=json.dumps({"story_id": "demo-2"}),
            ),
        ]
        db.add_all(sample_events)
        db.commit()
    finally:
        db.close()


if __name__ == "__main__":
    seed_events()
    print("Seeded demo events.")
