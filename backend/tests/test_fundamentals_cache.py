from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker

from app.db import Base, ensure_fundamentals_cache_schema
from app.models import FundamentalsCache, PriceCache
from app.services.fundamentals_cache import FundamentalsFetchResult, fundamentals_source_diagnostics, fundamentals_summary_from_cache_row, normalize_fundamentals_payload, upsert_fundamentals_cache
from app.services.screener import ScreenerParams, build_screener_response
import app.populate_fundamentals_cache as populate_module


def _engine():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)
    ensure_fundamentals_cache_schema(engine)
    return engine


def _session_factory(engine):
    return sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)


def _values(symbol: str, **overrides):
    values = normalize_fundamentals_payload(symbol=symbol, screener_row={"symbol": symbol, "companyName": f"{symbol} Corp"})
    values.update(overrides)
    return values


def _patch_cli_db(monkeypatch):
    engine = _engine()
    SessionLocal = _session_factory(engine)
    monkeypatch.setattr(populate_module, "engine", engine)
    monkeypatch.setattr(populate_module, "SessionLocal", SessionLocal)
    return engine, SessionLocal


def _seed_cache(db: Session, symbol: str, **values) -> FundamentalsCache:
    row = FundamentalsCache(
        symbol=symbol,
        provider="fmp",
        fetched_at=values.pop("fetched_at", datetime.now(timezone.utc)),
        status=values.pop("status", "ok"),
        company_name=values.pop("company_name", f"{symbol} Corp"),
        sector=values.pop("sector", "Technology"),
        industry=values.pop("industry", "Software"),
        country=values.pop("country", "US"),
        exchange=values.pop("exchange", "NASDAQ"),
        market_cap=values.pop("market_cap", 10_000_000_000),
        price=values.pop("price", 100),
        volume=values.pop("volume", 1_000_000),
        avg_volume=values.pop("avg_volume", 1_000_000),
        **values,
    )
    db.add(row)
    return row


def _add_price_history(db: Session, symbol: str, closes: list[float]) -> None:
    start = (datetime.now(timezone.utc) - timedelta(days=len(closes))).date()
    for index, close in enumerate(closes):
        db.add(PriceCache(symbol=symbol, date=(start + timedelta(days=index)).isoformat(), close=float(close)))


def test_fundamentals_cli_dry_run_does_not_write(monkeypatch):
    engine, _SessionLocal = _patch_cli_db(monkeypatch)
    monkeypatch.setattr(
        populate_module,
        "fetch_fundamentals_for_symbol",
        lambda symbol: FundamentalsFetchResult(symbol=symbol, values=_values(symbol, trailing_pe=12)),
    )

    report = populate_module.populate_fundamentals_cache(symbols=["AAPL"], dry_run=True)

    with Session(engine) as db:
        assert db.execute(select(FundamentalsCache)).scalars().all() == []
    assert report["dry_run"] is True
    assert report["fetched"] == 1
    assert report["updated"] == 0


def test_fundamentals_cli_apply_upserts(monkeypatch):
    engine, _SessionLocal = _patch_cli_db(monkeypatch)
    monkeypatch.setattr(
        populate_module,
        "fetch_fundamentals_for_symbol",
        lambda symbol: FundamentalsFetchResult(symbol=symbol, values=_values(symbol, trailing_pe=12, price_to_sales=4)),
    )

    report = populate_module.populate_fundamentals_cache(symbols=["AAPL"], dry_run=False)

    with Session(engine) as db:
        row = db.execute(select(FundamentalsCache).where(FundamentalsCache.symbol == "AAPL")).scalar_one()
    assert report["updated"] == 1
    assert row.trailing_pe == 12
    assert row.price_to_sales == 4


def test_fundamentals_cli_refresh_updates_existing_row(monkeypatch):
    engine, _SessionLocal = _patch_cli_db(monkeypatch)
    with Session(engine) as db:
        _seed_cache(db, "AAPL", trailing_pe=30, price_to_sales=9)
        db.commit()
    monkeypatch.setattr(
        populate_module,
        "fetch_fundamentals_for_symbol",
        lambda symbol: FundamentalsFetchResult(symbol=symbol, values=_values(symbol, trailing_pe=18, price_to_sales=3)),
    )

    populate_module.populate_fundamentals_cache(symbols=["AAPL"], dry_run=False)

    with Session(engine) as db:
        row = db.execute(select(FundamentalsCache).where(FundamentalsCache.symbol == "AAPL")).scalar_one()
    assert row.trailing_pe == 18
    assert row.price_to_sales == 3


def test_fundamentals_cli_missing_provider_fields_stay_null(monkeypatch):
    engine, _SessionLocal = _patch_cli_db(monkeypatch)
    monkeypatch.setattr(
        populate_module,
        "fetch_fundamentals_for_symbol",
        lambda symbol: FundamentalsFetchResult(symbol=symbol, values=_values(symbol)),
    )

    populate_module.populate_fundamentals_cache(symbols=["AAPL"], dry_run=False)

    with Session(engine) as db:
        row = db.execute(select(FundamentalsCache).where(FundamentalsCache.symbol == "AAPL")).scalar_one()
    assert row.trailing_pe is None
    assert row.gross_margin is None


def test_fundamentals_summary_normalizes_payload_and_composite_status():
    engine = _engine()
    with Session(engine) as db:
        row = _seed_cache(
            db,
            "NVDA",
            revenue_growth=12.4,
            roe=34.1,
            ev_to_ebitda=38.2,
            operating_margin_expansion=-1.6,
            net_debt_to_ebitda=0.7,
        )
        db.commit()

        summary = fundamentals_summary_from_cache_row(row)

    assert summary["status"] == "bullish"
    assert summary["headline"] == "Fundamental strength"
    assert summary["metrics"]["revenue_growth"]["display"] == "12.4%"
    assert summary["metrics"]["revenue_growth"]["value"] == 0.124
    assert summary["metrics"]["ev_to_ebitda"]["display"] == "38.2x"
    assert summary["metrics"]["operating_margin_expansion"]["display"] == "-1.6 pts"
    assert summary["metrics"]["net_debt_to_ebitda"]["display"] == "0.7x"
    assert "fcf_yield" not in summary["metrics"]
    assert summary["data_quality"]["scored_metric_count"] == 5


def test_fundamentals_summary_missing_metrics_render_dash_and_do_not_score():
    engine = _engine()
    with Session(engine) as db:
        row = _seed_cache(db, "MISS", revenue_growth=None, roe=None, fcf_yield=None, ev_to_ebitda=None)
        db.commit()

        summary = fundamentals_summary_from_cache_row(row)

    assert summary["metrics"]["revenue_growth"]["display"] == "\u2014"
    assert summary["metrics"]["return_on_equity"]["state"] == "unavailable"
    assert summary["data_quality"]["scored_metric_count"] == 0
    assert summary["status"] == "unavailable"


def test_fundamentals_summary_requires_three_available_metrics():
    engine = _engine()
    with Session(engine) as db:
        row = _seed_cache(db, "THIN", revenue_growth=12, roe=18, ev_to_ebitda=None, operating_margin_expansion=None, net_debt_to_ebitda=None)
        db.commit()

        summary = fundamentals_summary_from_cache_row(row)

    assert summary["data_quality"]["scored_metric_count"] == 2
    assert summary["data_quality"]["available"] is False
    assert summary["status"] == "unavailable"


def test_fundamentals_normalization_accepts_defensive_aliases_and_margin_fallback():
    values = normalize_fundamentals_payload(
        symbol="NVDA",
        ratios_row={"operatingProfitMarginTTM": 0.342},
        metrics_row={
            "symbol": "NVDA",
            "returnOnEquity": 0.34,
            "evToEBITDA": 38.2,
            "netDebtToEBITDA": 0.7,
        },
        growth_row={"growth_revenue": 0.124},
        ratios_history_rows=[
            {"date": "2025-01-31", "operatingProfitMargin": 0.318},
        ],
        income_statement_rows=[
            {"date": "2026-01-31", "revenue": 100, "operatingIncome": 20},
            {"date": "2025-01-31", "revenue": 100, "operatingIncome": 19},
        ],
    )

    assert values["revenue_growth"] == 12.4
    assert values["roe"] == 34.0
    assert values["ev_to_ebitda"] == 38.2
    assert values["net_debt_to_ebitda"] == 0.7
    assert values["operating_margin_expansion"] == pytest.approx(2.4)


def test_fundamentals_normalization_treats_large_roe_ratio_as_percent():
    values = normalize_fundamentals_payload(
        symbol="AAPL",
        metrics_row={"symbol": "AAPL", "returnOnEquityTTM": 1.4668924498270723},
    )

    assert values["roe"] == pytest.approx(146.68924498270723)


def test_fundamentals_normalization_ignores_net_debt_ratio_when_ebitda_non_positive():
    values = normalize_fundamentals_payload(
        symbol="DEBT",
        metrics_row={"symbol": "DEBT", "netDebtToEBITDATTM": 3.2, "ebitdaTTM": 0},
    )

    assert values["net_debt_to_ebitda"] is None


def test_fundamentals_diagnostics_report_missing_api_key_without_secret(monkeypatch):
    monkeypatch.delenv("FMP_API_KEY", raising=False)

    diagnostics = fundamentals_source_diagnostics("NVDA")

    assert diagnostics["symbol"] == "NVDA"
    assert diagnostics["status"] == "missing_api_key"
    assert diagnostics["api_key_present"] is False
    assert "api_key" not in diagnostics


def test_fundamentals_update_does_not_clear_existing_identity_fields():
    engine = _engine()
    with Session(engine) as db:
        _seed_cache(
            db,
            "MSTR",
            company_name="Strategy Inc",
            sector="Technology",
            industry="Software - Application",
        )
        db.commit()

        values = _values("MSTR", company_name=None, sector=None, industry=None)
        values["country"] = None
        values["exchange"] = None
        assert upsert_fundamentals_cache(db, values)
        db.commit()

        row = db.execute(select(FundamentalsCache).where(FundamentalsCache.symbol == "MSTR")).scalar_one()

    assert row.company_name == "Strategy Inc"
    assert row.sector == "Technology"
    assert row.industry == "Software - Application"


def test_screener_reads_cached_fundamentals_without_provider_call(monkeypatch):
    monkeypatch.delenv("SCREENER_PROVIDER_FALLBACK", raising=False)
    monkeypatch.setattr(
        "app.services.screener.fetch_company_screener",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("provider read path call")),
    )
    engine = _engine()
    with Session(engine) as db:
        _seed_cache(db, "AAPL", trailing_pe=None, price_to_sales=None)
        db.commit()

        response = build_screener_response(db, ScreenerParams(page_size=10))

    assert [item["symbol"] for item in response["items"]] == ["AAPL"]
    assert response["items"][0]["trailing_pe"] is None


def test_cached_screener_trailing_pe_max_excludes_nulls_and_values_above_max(monkeypatch):
    monkeypatch.delenv("SCREENER_PROVIDER_FALLBACK", raising=False)
    engine = _engine()
    with Session(engine) as db:
        _seed_cache(db, "PASS", trailing_pe=8)
        _seed_cache(db, "HIGH", trailing_pe=12)
        _seed_cache(db, "NULL", trailing_pe=None)
        db.commit()

        response = build_screener_response(db, ScreenerParams(page_size=10, trailing_pe_max=10))

    assert [item["symbol"] for item in response["items"]] == ["PASS"]


def test_cached_screener_price_to_sales_max_excludes_nulls_and_values_above_max(monkeypatch):
    monkeypatch.delenv("SCREENER_PROVIDER_FALLBACK", raising=False)
    engine = _engine()
    with Session(engine) as db:
        _seed_cache(db, "PASS", price_to_sales=1.5)
        _seed_cache(db, "HIGH", price_to_sales=2.5)
        _seed_cache(db, "NULL", price_to_sales=None)
        db.commit()

        response = build_screener_response(db, ScreenerParams(page_size=10, price_sales_max=2))

    assert [item["symbol"] for item in response["items"]] == ["PASS"]


def test_cached_screener_gross_margin_min_excludes_nulls_and_values_below_min(monkeypatch):
    monkeypatch.delenv("SCREENER_PROVIDER_FALLBACK", raising=False)
    engine = _engine()
    with Session(engine) as db:
        _seed_cache(db, "PASS", gross_margin=55)
        _seed_cache(db, "LOW", gross_margin=45)
        _seed_cache(db, "NULL", gross_margin=None)
        db.commit()

        response = build_screener_response(db, ScreenerParams(page_size=10, gross_margin_min=50))

    assert [item["symbol"] for item in response["items"]] == ["PASS"]


def test_cached_screener_combines_fundamental_and_rsi_filters_with_and_logic(monkeypatch):
    monkeypatch.delenv("SCREENER_PROVIDER_FALLBACK", raising=False)
    engine = _engine()
    with Session(engine) as db:
        _seed_cache(db, "PASS", trailing_pe=15)
        _seed_cache(db, "HIGHPE", trailing_pe=25)
        _seed_cache(db, "LOWRSI", trailing_pe=12)
        _add_price_history(db, "PASS", [100 + index for index in range(30)])
        _add_price_history(db, "HIGHPE", [100 + index for index in range(30)])
        _add_price_history(db, "LOWRSI", [130 - index for index in range(30)])
        db.commit()

        response = build_screener_response(db, ScreenerParams(page_size=10, trailing_pe_max=20, rsi_min=40))

    assert [item["symbol"] for item in response["items"]] == ["PASS"]
