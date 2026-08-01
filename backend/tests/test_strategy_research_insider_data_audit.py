from __future__ import annotations

import json
from datetime import date, datetime, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db import Base
from app.models import Event, InsiderTransaction, PriceCache
from app.strategy_research.insider_data_audit import run_audit


def _session():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    Base.metadata.create_all(bind=engine)
    return SessionLocal()


def _legacy_insider(
    db,
    *,
    row_id: int,
    symbol: str = "AAPL",
    transaction_type: str = "purchase",
    filing_date: date | None = date(2024, 1, 5),
    transaction_date: date | None = date(2024, 1, 2),
) -> None:
    db.add(
        InsiderTransaction(
            id=row_id,
            source="legacy",
            external_id=f"legacy-{row_id}",
            symbol=symbol,
            reporting_cik="0001234567",
            insider_name="Example Insider",
            transaction_type=transaction_type,
            role="CEO",
            ownership="direct",
            transaction_date=transaction_date,
            filing_date=filing_date,
            shares=10.0,
            price=100.0,
            payload_json=json.dumps({"symbol": symbol, "filing_date": filing_date.isoformat() if filing_date else None}),
        )
    )


def _event(db, *, row_id: int, symbol: str = "AAPL", filing_date: str = "2024-01-05") -> None:
    db.add(
        Event(
            id=row_id,
            event_type="insider_trade",
            ts=datetime(2024, 1, 5, tzinfo=timezone.utc),
            event_date=datetime(2024, 1, 5, tzinfo=timezone.utc),
            symbol=symbol,
            source="legacy",
            trade_type="purchase",
            payload_json=json.dumps({"symbol": symbol, "filing_date": filing_date, "reporting_cik": "0001234567"}),
        )
    )


def _price(db, symbol: str, day: str, adjusted_close: float) -> None:
    db.add(
        PriceCache(
            symbol=symbol,
            date=day,
            close=adjusted_close,
            adjusted_close=adjusted_close,
            raw_close=adjusted_close,
        )
    )


def test_insider_data_audit_counts_usable_legacy_purchases_and_future_rows():
    db = _session()
    try:
        _legacy_insider(db, row_id=1)
        _legacy_insider(db, row_id=2, transaction_type="sale")
        _legacy_insider(db, row_id=3, symbol="MSFT", filing_date=date(2030, 1, 1), transaction_date=date(2030, 1, 1))
        _event(db, row_id=10)
        _price(db, "AAPL", "2024-01-08", 100.0)
        _price(db, "SPY", "2024-01-08", 100.0)
        db.commit()

        result = run_audit(
            db,
            as_of=date(2026, 7, 31),
            focus_universe=("AAPL", "MSFT"),
            top_n=5,
        )

        assert result["table_counts"]["insider_transactions"] == 3
        assert result["legacy_quality"]["purchase_rows"] == 2
        assert result["legacy_quality"]["usable_purchase_rows"] == 1
        assert result["legacy_quality"]["future_dated_rows"] == 1
        assert result["focus_universe_counts"][0]["symbol"] == "AAPL"
        assert result["focus_universe_counts"][0]["usable_purchase_rows"] == 1
        assert result["future_dated_examples"][0]["symbol"] == "MSFT"
    finally:
        db.close()
