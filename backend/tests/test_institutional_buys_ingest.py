from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker

from app.db import Base
from app.ingest_institutional_buys import ingest_institutional_buys
from app.models import Event, InstitutionalTransaction


def test_ingest_institutional_buys_creates_raw_and_event(monkeypatch):
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)

    TestSession = sessionmaker(bind=engine, autoflush=False, autocommit=False)

    sample_rows = [
        {
            "symbol": "msft",
            "holder": "Big Fund LP",
            "institutionCik": "0001234567",
            "filingDate": "2026-03-20",
            "reportDate": "2026-03-15",
            "shares": 10000,
            "marketValue": 4200000,
            "avgPricePaid": 420,
            "changeInShares": 2500,
            "changePercent": 33.3,
        }
    ]

    monkeypatch.setattr("app.ingest_institutional_buys.SessionLocal", TestSession)
    monkeypatch.setattr(
        "app.ingest_institutional_buys.fetch_institutional_buys",
        lambda *args, **kwargs: sample_rows if kwargs.get("page", 0) == 0 else [],
    )

    result = ingest_institutional_buys(days=60, page_limit=2, per_page=50)

    assert result["inserted_raw"] == 1
    assert result["inserted_events"] == 1

    with Session(engine) as db:
        raw = db.execute(select(InstitutionalTransaction)).scalar_one()
        event = db.execute(select(Event).where(Event.event_type == "institutional_buy")).scalar_one()

    assert raw.symbol == "MSFT"
    assert raw.institution_name == "Big Fund LP"
    assert event.symbol == "MSFT"
    assert event.trade_type == "purchase"
    assert event.transaction_type == "Holding Increase"
    assert event.amount_max == 4200000
