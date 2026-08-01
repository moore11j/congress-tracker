from __future__ import annotations

from datetime import date, datetime, timezone

from sqlalchemy import create_engine, inspect
from sqlalchemy.orm import sessionmaker

from app.db import Base, ensure_fundamentals_snapshot_schema
from app.models import FundamentalsCache, FundamentalsSnapshot
from app.services.fundamentals_snapshots import (
    latest_fundamentals_snapshot_on_or_before,
    snapshot_current_fundamentals,
    snapshot_symbol_rejection_reason,
)


def _session():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    Base.metadata.create_all(bind=engine)
    ensure_fundamentals_snapshot_schema(engine)
    return SessionLocal, engine


def _fundamentals(symbol: str, *, fetched_at: datetime, revenue_growth: float, roe: float) -> FundamentalsCache:
    return FundamentalsCache(
        symbol=symbol,
        provider="fmp",
        fetched_at=fetched_at,
        period_date=fetched_at.date(),
        status="ok",
        company_name=f"{symbol} Inc.",
        sector="Technology",
        market_cap=1_000_000_000.0,
        revenue_growth=revenue_growth,
        roe=roe,
        trailing_pe=20.0,
    )


def test_fundamentals_snapshot_schema_creates_table_and_indexes():
    _, engine = _session()

    inspector = inspect(engine)
    columns = {column["name"] for column in inspector.get_columns("fundamentals_snapshots")}

    assert "fundamentals_snapshots" in inspector.get_table_names()
    assert any(index["name"] == "ix_fundamentals_snapshots_symbol_date" for index in inspector.get_indexes("fundamentals_snapshots"))
    assert {"source_kind", "availability_basis", "data_quality_confidence"} <= columns


def test_snapshot_current_fundamentals_is_idempotent_per_symbol_provider_day():
    SessionLocal, _ = _session()
    db = SessionLocal()
    try:
        db.add(
            _fundamentals(
                "AAPL",
                fetched_at=datetime(2026, 8, 1, 12, 0, tzinfo=timezone.utc),
                revenue_growth=12.0,
                roe=30.0,
            )
        )
        db.commit()

        observed_at = datetime(2026, 8, 1, 16, 0, tzinfo=timezone.utc)
        first = snapshot_current_fundamentals(db, observed_at=observed_at)
        second = snapshot_current_fundamentals(db, observed_at=observed_at)
        db.commit()

        count = db.query(FundamentalsSnapshot).count()
        snapshot = db.query(FundamentalsSnapshot).one()
        assert first["snapshots_written"] == 1
        assert second["snapshots_written"] == 1
        assert count == 1
        assert snapshot.snapshot_date == date(2026, 8, 1)
        assert snapshot.revenue_growth == 12.0
        assert snapshot.roe == 30.0
        assert snapshot.source_kind == "fundamentals_cache_current_snapshot"
        assert snapshot.availability_basis == "current fundamentals cache row observed at snapshot time"
        assert snapshot.data_quality_confidence == "high"
    finally:
        db.close()


def test_latest_snapshot_on_or_before_returns_historical_state_not_current_cache():
    SessionLocal, _ = _session()
    db = SessionLocal()
    try:
        row = _fundamentals(
            "MSFT",
            fetched_at=datetime(2026, 8, 1, 12, 0, tzinfo=timezone.utc),
            revenue_growth=10.0,
            roe=25.0,
        )
        db.add(row)
        db.commit()
        snapshot_current_fundamentals(db, observed_at=datetime(2026, 8, 1, 18, 0, tzinfo=timezone.utc))
        row.revenue_growth = -5.0
        row.roe = 8.0
        row.fetched_at = datetime(2026, 8, 2, 12, 0, tzinfo=timezone.utc)
        row.period_date = date(2026, 8, 2)
        snapshot_current_fundamentals(db, observed_at=datetime(2026, 8, 2, 18, 0, tzinfo=timezone.utc))
        db.commit()

        older = latest_fundamentals_snapshot_on_or_before(db, "MSFT", as_of=date(2026, 8, 1))
        newer = latest_fundamentals_snapshot_on_or_before(db, "MSFT", as_of=date(2026, 8, 2))

        assert older is not None
        assert older.revenue_growth == 10.0
        assert newer is not None
        assert newer.revenue_growth == -5.0
    finally:
        db.close()


def test_snapshot_current_fundamentals_skips_invalid_symbols():
    SessionLocal, _ = _session()
    db = SessionLocal()
    try:
        db.add(
            _fundamentals(
                "AAPL",
                fetched_at=datetime(2026, 8, 1, 12, 0, tzinfo=timezone.utc),
                revenue_growth=12.0,
                roe=30.0,
            )
        )
        db.add(
            _fundamentals(
                "META))",
                fetched_at=datetime(2026, 8, 1, 12, 0, tzinfo=timezone.utc),
                revenue_growth=12.0,
                roe=30.0,
            )
        )
        db.add(
            _fundamentals(
                "TXNRESEARCH",
                fetched_at=datetime(2026, 8, 1, 12, 0, tzinfo=timezone.utc),
                revenue_growth=12.0,
                roe=30.0,
            )
        )
        db.add(
            _fundamentals(
                "ABALX",
                fetched_at=datetime(2026, 8, 1, 12, 0, tzinfo=timezone.utc),
                revenue_growth=12.0,
                roe=30.0,
            )
        )
        db.commit()

        result = snapshot_current_fundamentals(db, observed_at=datetime(2026, 8, 1, 18, 0, tzinfo=timezone.utc))
        db.commit()

        assert result["rows_seen"] == 4
        assert result["snapshots_written"] == 1
        assert result["skipped_invalid_symbols"] == {
            "likely_mutual_fund": 1,
            "symbol_contains_non_ticker_artifact": 1,
            "unsupported_symbol": 1,
        }
        assert [row.symbol for row in db.query(FundamentalsSnapshot).all()] == ["AAPL"]
    finally:
        db.close()


def test_snapshot_symbol_hygiene_allows_class_share_aliases_and_rejects_artifacts():
    assert snapshot_symbol_rejection_reason("BRK.B") == ("BRK.B", None)
    assert snapshot_symbol_rejection_reason("BRK-B") == ("BRK-B", None)
    assert snapshot_symbol_rejection_reason("ABALX") == ("ABALX", "likely_mutual_fund")
    assert snapshot_symbol_rejection_reason("[SYMBOL]")[1] is not None
    assert snapshot_symbol_rejection_reason("AAPLU0026CHART=STOCK") == (
        "AAPLU0026CHART=STOCK",
        "symbol_contains_non_ticker_artifact",
    )
