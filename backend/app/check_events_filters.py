from __future__ import annotations

from datetime import datetime, timedelta, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db import Base
from app.models import Event
from app.routers.events import list_events


def _seed_events(db) -> None:
    now = datetime.now(timezone.utc)
    old = now - timedelta(days=10)
    db.add_all(
        [
            Event(
                event_type="congress_trade",
                ts=now,
                event_date=now,
                symbol="AAPL",
                source="house",
                impact_score=0.0,
                payload_json='{"symbol":"AAPL"}',
                amount_max=5000,
            ),
            Event(
                event_type="congress_trade",
                ts=old,
                event_date=old,
                symbol="MSFT",
                source="house",
                impact_score=0.0,
                payload_json='{"symbol":"MSFT"}',
                amount_max=7500,
            ),
        ]
    )
    db.commit()


def main() -> None:
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)

    with Session() as db:
        _seed_events(db)

        recent_page = list_events(
            db=db, recent_days=1, limit=50, min_amount=None, whale=None
        )
        wide_page = list_events(
            db=db, recent_days=30, limit=50, min_amount=None, whale=None
        )

        assert len(recent_page.items) <= len(wide_page.items), (
            "recent_days=1 should return fewer or equal rows than recent_days=30"
        )

        empty_symbol = list_events(
            db=db,
            symbol="ZZZZZZ",
            limit=50,
            min_amount=None,
            whale=None,
            recent_days=None,
        )
        assert (
            len(empty_symbol.items) == 0
        ), "symbol=ZZZZZZ should return zero results"

        empty_amount = list_events(
            db=db,
            min_amount=999_999_999,
            limit=50,
            whale=None,
            recent_days=None,
        )
        assert (
            len(empty_amount.items) == 0
        ), "min_amount=999999999 should return zero results"

    print("Event filter checks passed.")


if __name__ == "__main__":
    main()
