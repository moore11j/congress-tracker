from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from app.db import Base
from app.entitlements import ENTITLEMENTS
from app.models import FundamentalsCache, IndexMembership, MarketPressureSnapshot, PriceCache, QuoteCache, Security, TickerMeta, UserAccount, Watchlist, WatchlistItem
from app.services.confirmation_score import confirmation_score_bundle_from_source_payloads
from app.services.market_pressure import build_market_pressure_response
import app.services.market_pressure as market_pressure
import app.services.market_pressure_ingest as market_pressure_ingest
from app.services.market_pressure_ingest import refresh_market_pressure_snapshots


def _session_factory():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(
        bind=engine,
        tables=[
            UserAccount.__table__,
            Watchlist.__table__,
            WatchlistItem.__table__,
            Security.__table__,
            IndexMembership.__table__,
            MarketPressureSnapshot.__table__,
            PriceCache.__table__,
            QuoteCache.__table__,
            TickerMeta.__table__,
            FundamentalsCache.__table__,
        ],
    )
    return sessionmaker(bind=engine, autoflush=False, autocommit=False)


def _bundle(symbol: str, direction: str = "bullish") -> dict:
    return confirmation_score_bundle_from_source_payloads(
        symbol,
        sources_payload={
            "price_volume": {
                "present": True,
                "direction": direction,
                "strength": 90,
                "quality": 90,
                "freshness_days": 1,
            },
            "signals": {
                "present": True,
                "direction": direction,
                "strength": 88,
                "quality": 88,
                "freshness_days": 1,
            },
        },
        lookback_days=30,
    )


def test_market_pressure_snapshot_job_reuses_cache_fetches_missing_and_serves_page(monkeypatch):
    Session = _session_factory()
    db = Session()
    try:
        generated_at = datetime.now(timezone.utc)
        previous_day = (generated_at - timedelta(days=1)).date().isoformat()
        user = UserAccount(email="owner@example.com")
        db.add(user)
        db.flush()
        watchlist = Watchlist(name="default", owner_user_id=user.id)
        db.add(watchlist)
        db.add_all(
            [
                Security(symbol="SPY", name="SPDR S&P 500 ETF Trust", asset_class="etf_fund", sector="US Large Cap"),
                Security(symbol="QQQ", name="Invesco QQQ Trust", asset_class="etf", sector="US Large Cap"),
                TickerMeta(symbol="SPY", company_name="SPDR S&P 500 ETF Trust", exchange="NYSEARCA", sector="US Large Cap"),
                TickerMeta(symbol="QQQ", company_name="Invesco QQQ Trust", exchange="NASDAQ", sector="US Large Cap"),
                FundamentalsCache(symbol="SPY", provider="fixture", fetched_at=generated_at, status="ok", market_cap=500_000_000_000, sector="US Large Cap"),
                FundamentalsCache(symbol="QQQ", provider="fixture", fetched_at=generated_at, status="ok", market_cap=300_000_000_000, sector="US Large Cap"),
                PriceCache(symbol="SPY", date=previous_day, close=100.0),
                PriceCache(symbol="QQQ", date=previous_day, close=200.0),
                QuoteCache(symbol="SPY", price=101.0, asof_ts=(generated_at - timedelta(minutes=5)).replace(tzinfo=None), market_cap=500_000_000_000),
            ]
        )
        db.commit()

        requested: list[str] = []

        def fixture_fetch(symbol: str):
            requested.append(symbol)
            assert symbol == "QQQ"
            return {"date": generated_at.isoformat(), "close": 210.0}

        monkeypatch.setattr(
            market_pressure_ingest,
            "get_confirmation_score_bundles_for_tickers",
            lambda _db, symbols, lookback_days: {symbol: _bundle(symbol) for symbol in symbols},
        )

        results = refresh_market_pressure_snapshots(
            db,
            universes=["etf"],
            force=True,
            market_hours_only=False,
            calls_per_minute=500,
            fetcher=fixture_fetch,
            sleep_fn=lambda _seconds: None,
            now=generated_at,
        )

        assert requested == ["QQQ"]
        assert results[0].status == "ok"
        assert results[0].snapshot_count == 2
        assert results[0].fetched_count == 1
        assert results[0].cache_hit_count == 1

        snapshots = {row.symbol: row for row in db.execute(select(MarketPressureSnapshot)).scalars().all()}
        assert snapshots["SPY"].price == 101.0
        assert snapshots["SPY"].price_change_pct == 1.0
        assert snapshots["SPY"].market_cap == 500_000_000_000
        assert snapshots["SPY"].confirmation_direction == "bullish"
        assert snapshots["QQQ"].price == 210.0
        assert snapshots["QQQ"].price_change_pct == 5.0
        assert json.loads(snapshots["QQQ"].tile_json)["symbol"] == "QQQ"

        monkeypatch.setattr(market_pressure, "get_current_prices_meta_db", lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("live fetch should not run")))
        response = build_market_pressure_response(
            db,
            universe="etf",
            period="1d",
            view="market_pressure",
            entitlements=ENTITLEMENTS["pro"],
            user=user,
            confirmation_loader=lambda _db, symbols: (_ for _ in ()).throw(AssertionError("confirmation should come from snapshot")),
        )

        tiles = {tile["symbol"]: tile for sector in response["sectors"] for tile in sector["tiles"]}
        assert response["summary"]["symbolCount"] == 2
        assert tiles["SPY"]["priceChangePct"] == 1.0
        assert tiles["QQQ"]["marketCap"] == 300_000_000_000
    finally:
        db.close()
