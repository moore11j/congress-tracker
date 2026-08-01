from __future__ import annotations

from datetime import date

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db import Base
from app.models import InsiderTransactionNormalized, PriceCache
from app.strategy_research.insider_price_universe import (
    load_insider_purchase_price_coverage,
    run,
    rows_needing_adjusted_backfill,
)


def _session():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    Base.metadata.create_all(bind=engine)
    return SessionLocal()


def _insider_row(
    db,
    *,
    row_id: int,
    symbol: str,
    transaction_type: str = "open_market_purchase",
    filing_date: date = date(2024, 1, 5),
    transaction_date: date = date(2024, 1, 2),
    is_duplicate: bool = False,
) -> None:
    db.add(
        InsiderTransactionNormalized(
            id=row_id,
            accession_number=f"0000000000-{row_id}",
            ticker_normalized=symbol,
            reporting_owner_cik="0001234567",
            reporting_owner_name="Example Insider",
            owner_relationship_json="{}",
            is_director=True,
            is_officer=False,
            is_ten_percent_owner=False,
            transaction_date=transaction_date,
            filing_date=filing_date,
            transaction_code="P",
            transaction_type_normalized=transaction_type,
            shares=10.0,
            price=100.0,
            value=1000.0,
            is_derivative=False,
            footnotes_json="[]",
            ten_b5_1_flag=False,
            normalized_hash=f"hash-{row_id}",
            is_duplicate=is_duplicate,
            parser_confidence=0.95,
        )
    )


def _price(db, *, symbol: str, day: str, adjusted_close: float | None) -> None:
    db.add(
        PriceCache(
            symbol=symbol,
            date=day,
            close=adjusted_close or 100.0,
            adjusted_close=adjusted_close,
            raw_close=100.0,
        )
    )


def test_coverage_uses_normalized_nonduplicate_open_market_purchases_only():
    db = _session()
    _insider_row(db, row_id=1, symbol="AAPL")
    _insider_row(db, row_id=2, symbol="AAPL")
    _insider_row(db, row_id=3, symbol="MSFT", transaction_type="open_market_sale")
    _insider_row(db, row_id=4, symbol="NVDA", is_duplicate=True)
    _insider_row(db, row_id=5, symbol="TSLA", transaction_date=date(2027, 1, 1))
    _price(db, symbol="AAPL", day="2024-01-05", adjusted_close=100.0)
    _price(db, symbol="AAPL", day="2024-01-08", adjusted_close=None)
    db.commit()

    rows = load_insider_purchase_price_coverage(
        db,
        start_date=date(2024, 1, 1),
        end_date=date(2026, 7, 31),
    )

    assert [row.symbol for row in rows] == ["AAPL"]
    assert rows[0].purchase_count == 2
    assert rows[0].price_rows == 2
    assert rows[0].adjusted_rows == 1
    assert rows[0].reconstructed_rows == 0
    assert rows_needing_adjusted_backfill(rows, min_adjusted_rows=2) == rows
    assert rows_needing_adjusted_backfill(rows, min_adjusted_rows=1) == rows


def test_run_can_exclude_known_provider_misses(monkeypatch):
    db = _session()
    _insider_row(db, row_id=1, symbol="AXIA3")
    _insider_row(db, row_id=2, symbol="AAPL")
    db.commit()
    monkeypatch.setattr("app.strategy_research.insider_price_universe.SessionLocal", lambda: db)

    result = run(
        start_date=date(2024, 1, 1),
        end_date=date(2026, 7, 31),
        apply=False,
        min_purchase_count=1,
        min_adjusted_rows=1,
        max_symbols=None,
        symbols_per_batch=10,
        sleep_seconds=0,
        exclude_symbols=("AXIA3",),
    )

    assert result["symbols_needing_backfill"] == 1
    assert result["exclude_symbols"] == ["AXIA3"]
    assert result["selected_preview"][0]["symbol"] == "AAPL"
