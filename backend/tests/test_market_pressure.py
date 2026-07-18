from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone

import pytest
from fastapi import HTTPException
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db import Base
from app.entitlements import ENTITLEMENTS
from app.models import FundamentalsCache, IndexMembership, MarketPressureSnapshot, PriceCache, QuoteCache, Security, TickerMeta, UserAccount, Watchlist, WatchlistItem
import app.services.market_pressure as market_pressure
from app.services.confirmation_score import confirmation_score_bundle_from_source_payloads
from app.services.index_memberships import refresh_index_memberships
from app.services.market_pressure import (
    build_market_pressure_capabilities_response,
    build_market_pressure_response,
    resolve_market_pressure_params,
)


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


@pytest.fixture
def db():
    Session = _session_factory()
    session = Session()
    try:
        yield session
    finally:
        session.close()


def _source(direction: str, *, freshness_days: int = 3, strength: int = 92, quality: int = 92) -> dict:
    return {
        "present": True,
        "direction": direction,
        "strength": strength,
        "quality": quality,
        "freshness_days": freshness_days,
        "label": f"{direction} fixture",
        "score_contribution": 10,
    }


def _bundle(symbol: str, sources: dict[str, dict]) -> dict:
    return confirmation_score_bundle_from_source_payloads(symbol, sources_payload=sources, lookback_days=30)


def _seed_watchlist(db, *, user_email: str = "owner@example.com", symbols: list[str]) -> UserAccount:
    user = UserAccount(email=user_email)
    db.add(user)
    db.flush()
    watchlist = Watchlist(name=f"{user_email}-default", owner_user_id=user.id)
    db.add(watchlist)
    db.flush()
    for symbol in symbols:
        security = Security(symbol=symbol, name=f"{symbol} Inc", asset_class="equity", sector="Technology" if symbol != "JPM" else "Financials")
        db.add(security)
        db.flush()
        db.add(WatchlistItem(watchlist_id=watchlist.id, security_id=security.id))
        db.add(TickerMeta(symbol=symbol, company_name=f"{symbol} Corp", exchange="NASDAQ", sector=security.sector))
    db.commit()
    return user


def _seed_price(db, symbol: str, *, start_close: float = 100.0, end_close: float = 105.0, days_back: int = 7) -> None:
    today = datetime.now(timezone.utc).date()
    start = today - timedelta(days=days_back)
    previous = today - timedelta(days=1)
    db.add_all(
        [
            PriceCache(symbol=symbol, date=start.isoformat(), close=start_close),
            PriceCache(symbol=symbol, date=previous.isoformat(), close=start_close + 1),
            PriceCache(symbol=symbol, date=today.isoformat(), close=end_close),
        ]
    )
    db.commit()


def _seed_index_membership(db, index_code: str, *, count: int) -> list[str]:
    prefix = "SP" if index_code == "sp500" else "NQ"
    symbols = [f"{prefix}{idx:03d}" for idx in range(1, count + 1)]
    result = refresh_index_memberships(
        db,
        index_code=index_code,
        rows=[{"symbol": symbol} for symbol in symbols],
        source="fixture:index-memberships",
        source_as_of=date(2026, 7, 14),
        refreshed_at=datetime(2026, 7, 14, tzinfo=timezone.utc),
    )
    assert result.status == "ok"
    return symbols


def _tiles_by_symbol(response: dict) -> dict[str, dict]:
    return {tile["symbol"]: tile for sector in response["sectors"] for tile in sector["tiles"]}


def _minimal_market_pressure_tile(symbol: str, *, market_cap: float | None = 100_000_000_000) -> dict:
    return {
        "symbol": symbol,
        "companyName": f"{symbol} Corp",
        "sector": "Technology" if symbol not in {"JPM", "XOM"} else "Financial Services",
        "exchange": "NASDAQ",
        "marketCap": market_cap,
        "priceChangePct": -1.0,
        "priceStartAt": None,
        "priceEndAt": None,
        "confirmationScore": None,
        "confirmationDirection": "unavailable",
        "confirmationStrength": None,
        "divergence": "unavailable",
        "dataState": "unavailable",
        "layers": {},
        "confirmationAsOf": None,
    }


def test_market_pressure_uses_market_cap_and_live_quote_fallback_for_missing_one_day_prices(db, monkeypatch):
    user = _seed_watchlist(db, symbols=["MEGA", "MID", "SMALL"])
    fetched_at = datetime(2026, 7, 15, tzinfo=timezone.utc)
    db.add_all(
        [
            FundamentalsCache(
                symbol="MEGA",
                provider="fixture",
                fetched_at=fetched_at,
                status="ok",
                company_name="Mega Cap Corp",
                sector="Technology",
                exchange="NASDAQ",
                market_cap=3_000_000_000_000,
            ),
            FundamentalsCache(
                symbol="MID",
                provider="fixture",
                fetched_at=fetched_at,
                status="ok",
                company_name="Mid Cap Corp",
                sector="Technology",
                exchange="NYSE",
                market_cap=50_000_000_000,
            ),
            FundamentalsCache(
                symbol="SMALL",
                provider="fixture",
                fetched_at=fetched_at,
                status="ok",
                company_name="Small Cap Corp",
                sector="Technology",
                exchange="NASDAQ",
                market_cap=500_000_000,
            ),
        ]
    )
    db.commit()
    monkeypatch.setenv("MARKET_PRESSURE_LIVE_PRICE_LIMIT", "2")
    requested_symbols: list[str] = []

    def fixture_quotes(_db, symbols, **kwargs):
        requested_symbols.extend(symbols)
        assert kwargs["lane"] == "market_pressure_quote"
        assert kwargs["allow_live_user_fetch"] is True
        assert kwargs["force_quote_endpoint"] is True
        assert kwargs["max_network_fetch"] == 2
        return {
            "MEGA": {"change_percent": 2.5, "asof_ts": fetched_at},
            "MID": {"change_percent": -1.25, "asof_ts": fetched_at},
        }

    monkeypatch.setattr(market_pressure, "get_current_prices_meta_db", fixture_quotes)

    response = build_market_pressure_response(
        db,
        universe="watchlist",
        period="1d",
        view="market_pressure",
        entitlements=ENTITLEMENTS["pro"],
        user=user,
        confirmation_loader=lambda _db, symbols: {},
    )

    tiles = _tiles_by_symbol(response)
    assert requested_symbols == ["MEGA", "MID"]
    assert tiles["MEGA"]["marketCap"] == 3_000_000_000_000
    assert tiles["MID"]["marketCap"] == 50_000_000_000
    assert tiles["SMALL"]["marketCap"] == 500_000_000
    assert tiles["MEGA"]["priceChangePct"] == 2.5
    assert tiles["MID"]["priceChangePct"] == -1.25
    assert tiles["SMALL"]["priceChangePct"] is None
    assert response["priceAsOf"].startswith("2026-07-15")


def test_market_pressure_fetches_live_market_cap_for_complete_one_day_price(db, monkeypatch):
    user = _seed_watchlist(db, symbols=["MSFT"])
    _seed_price(db, "MSFT", start_close=100, end_close=105)
    fetched_at = datetime(2026, 7, 15, tzinfo=timezone.utc)
    monkeypatch.setenv("MARKET_PRESSURE_LIVE_PRICE_LIMIT", "1")
    requested_symbols: list[str] = []

    def fixture_quotes(_db, symbols, **kwargs):
        requested_symbols.extend(symbols)
        assert kwargs["lane"] == "market_pressure_quote"
        assert kwargs["force_quote_endpoint"] is True
        assert kwargs["max_network_fetch"] == 1
        return {
            "MSFT": {
                "change_percent": 9.9,
                "market_cap": 3_600_000_000_000,
                "asof_ts": fetched_at,
            }
        }

    monkeypatch.setattr(market_pressure, "get_current_prices_meta_db", fixture_quotes)

    response = build_market_pressure_response(
        db,
        universe="watchlist",
        period="1d",
        view="market_pressure",
        entitlements=ENTITLEMENTS["pro"],
        user=user,
        confirmation_loader=lambda _db, symbols: {},
    )

    tiles = _tiles_by_symbol(response)
    assert requested_symbols == ["MSFT"]
    assert tiles["MSFT"]["marketCap"] == 3_600_000_000_000
    assert tiles["MSFT"]["priceChangePct"] == 3.9604


def test_market_pressure_repairs_implausibly_low_important_market_cap(db, monkeypatch):
    user = _seed_watchlist(db, symbols=["NVDA"])
    _seed_price(db, "NVDA", start_close=100, end_close=105)
    fetched_at = datetime(2026, 7, 15, tzinfo=timezone.utc)
    db.add(
        FundamentalsCache(
            symbol="NVDA",
            provider="fixture",
            fetched_at=fetched_at,
            status="ok",
            company_name="Nvidia Corp",
            sector="Technology",
            exchange="NASDAQ",
            market_cap=4_000_000,
        )
    )
    db.commit()
    monkeypatch.setenv("MARKET_PRESSURE_LIVE_PRICE_LIMIT", "1")
    requested_symbols: list[str] = []

    def fixture_quotes(_db, symbols, **kwargs):
        requested_symbols.extend(symbols)
        assert kwargs["lane"] == "market_pressure_quote"
        return {
            "NVDA": {
                "change_percent": -2.2,
                "market_cap": 4_200_000_000_000,
                "asof_ts": fetched_at,
            }
        }

    monkeypatch.setattr(market_pressure, "get_current_prices_meta_db", fixture_quotes)

    response = build_market_pressure_response(
        db,
        universe="watchlist",
        period="1d",
        view="market_pressure",
        entitlements=ENTITLEMENTS["pro"],
        user=user,
        confirmation_loader=lambda _db, symbols: {},
    )

    tiles = _tiles_by_symbol(response)
    assert requested_symbols == ["NVDA"]
    assert tiles["NVDA"]["marketCap"] == 4_200_000_000_000
    assert response["audit"]["status"] == "ok"


def test_market_pressure_classifies_complete_canonical_confirmation_and_divergence(db):
    user = _seed_watchlist(db, symbols=["HA", "FW", "AB", "AD", "MIX", "NEU", "MISS", "OLD"])
    _seed_price(db, "HA", start_close=100, end_close=98)
    _seed_price(db, "FW", start_close=100, end_close=106)
    _seed_price(db, "AB", start_close=100, end_close=105)
    _seed_price(db, "AD", start_close=100, end_close=95)
    _seed_price(db, "MIX", start_close=100, end_close=101)
    _seed_price(db, "NEU", start_close=100, end_close=100)
    _seed_price(db, "MISS", start_close=100, end_close=100)
    _seed_price(db, "OLD", start_close=100, end_close=97)

    bundles = {
        "HA": _bundle("HA", {"price_volume": _source("bullish"), "signals": _source("bullish")}),
        "FW": _bundle("FW", {"price_volume": _source("bearish"), "signals": _source("bearish")}),
        "AB": _bundle("AB", {"price_volume": _source("bullish"), "signals": _source("bullish")}),
        "AD": _bundle("AD", {"price_volume": _source("bearish"), "signals": _source("bearish")}),
        "MIX": _bundle("MIX", {"price_volume": _source("bullish"), "signals": _source("bearish")}),
        "NEU": _bundle("NEU", {"price_volume": _source("neutral"), "fundamentals": _source("neutral")}),
        "MISS": _bundle("MISS", {}),
        "OLD": _bundle("OLD", {"price_volume": _source("bullish", freshness_days=45), "signals": _source("bullish", freshness_days=45)}),
    }

    response = build_market_pressure_response(
        db,
        universe="watchlist",
        period="5d",
        view="market_pressure",
        entitlements=ENTITLEMENTS["pro"],
        user=user,
        confirmation_loader=lambda _db, symbols: {symbol: bundles[symbol] for symbol in symbols},
    )

    tiles = _tiles_by_symbol(response)
    assert tiles["HA"]["confirmationDirection"] == "bullish"
    assert tiles["HA"]["divergence"] == "hidden_accumulation"
    assert tiles["FW"]["confirmationDirection"] == "bearish"
    assert tiles["FW"]["divergence"] == "fragile_winner"
    assert tiles["AB"]["divergence"] == "aligned_bullish"
    assert tiles["AD"]["divergence"] == "aligned_bearish"
    assert tiles["MIX"]["confirmationDirection"] == "conflicted"
    assert tiles["NEU"]["confirmationDirection"] == "neutral"
    assert tiles["MISS"]["confirmationDirection"] == "unavailable"
    assert tiles["OLD"]["dataState"] == "stale"
    assert tiles["OLD"]["divergence"] == "unavailable"
    assert response["confirmationFreshnessWindowDays"] == 30
    assert response["period"] == "5d"
    assert response["summary"]["hiddenAccumulationCount"] == 1
    assert response["summary"]["fragileWinnerCount"] == 1
    assert response["sectors"][0]["summary"]["averagePriceChangePct"] is not None
    assert tiles["HA"]["confirmationScore"] == bundles["HA"]["score"]


def test_market_pressure_filters_views_without_frontend_scoring(db):
    user = _seed_watchlist(db, symbols=["HA", "FW", "AB"])
    _seed_price(db, "HA", start_close=100, end_close=98)
    _seed_price(db, "FW", start_close=100, end_close=106)
    _seed_price(db, "AB", start_close=100, end_close=105)
    bundles = {
        "HA": _bundle("HA", {"price_volume": _source("bullish"), "signals": _source("bullish")}),
        "FW": _bundle("FW", {"price_volume": _source("bearish"), "signals": _source("bearish")}),
        "AB": _bundle("AB", {"price_volume": _source("bullish"), "signals": _source("bullish")}),
    }
    hidden = build_market_pressure_response(
        db,
        universe="watchlist",
        period="1d",
        view="hidden_accumulation",
        entitlements=ENTITLEMENTS["admin"],
        user=user,
        confirmation_loader=lambda _db, symbols: {symbol: bundles[symbol] for symbol in symbols},
    )
    fragile = build_market_pressure_response(
        db,
        universe="watchlist",
        period="1d",
        view="fragile_winners",
        entitlements=ENTITLEMENTS["admin"],
        user=user,
        confirmation_loader=lambda _db, symbols: {symbol: bundles[symbol] for symbol in symbols},
    )
    assert set(_tiles_by_symbol(hidden)) == {"HA"}
    assert set(_tiles_by_symbol(fragile)) == {"FW"}


def test_market_pressure_pro_and_admin_receive_complete_canonical_score(db):
    user = _seed_watchlist(db, symbols=["LOCK"])
    _seed_price(db, "LOCK", start_close=100, end_close=105)
    bundle = _bundle(
        "LOCK",
        {
            "congress": _source("bullish"),
            "institutional_activity": _source("bearish"),
            "options_flow": _source("bearish"),
            "macro_positioning": _source("bearish"),
            "signals": _source("bullish"),
        },
    )

    pro = build_market_pressure_response(
        db,
        universe="watchlist",
        period="1d",
        view="market_pressure",
        entitlements=ENTITLEMENTS["pro"],
        user=user,
        confirmation_loader=lambda _db, symbols: {symbol: bundle for symbol in symbols},
    )
    admin = build_market_pressure_response(
        db,
        universe="watchlist",
        period="1d",
        view="market_pressure",
        entitlements=ENTITLEMENTS["admin"],
        user=user,
        confirmation_loader=lambda _db, symbols: {symbol: bundle for symbol in symbols},
    )

    for response in (pro, admin):
        tile = _tiles_by_symbol(response)["LOCK"]
        assert tile["confirmationScore"] == bundle["score"]
        assert tile["confirmationDirection"] == ("conflicted" if bundle["direction"] == "mixed" else bundle["direction"])
        assert tile["layers"]["institutions"]["status"] == "available"
        assert tile["layers"]["optionsFlow"]["status"] == "available"
        assert tile["layers"]["macroPositioning"]["status"] == "available"
        assert response["entitlement"]["lockedLayers"] == []


def test_market_pressure_rejects_unauthorized_before_protected_batch_work(db, monkeypatch):
    user = _seed_watchlist(db, symbols=["LOCK"])
    suspended = _seed_watchlist(db, user_email="suspended@example.com", symbols=["SUSP"])
    suspended.is_suspended = True
    db.commit()
    calls = {"membership": 0, "price": 0, "confirmation": 0}

    def forbidden_membership(*_args, **_kwargs):
        calls["membership"] += 1
        raise AssertionError("membership should not be loaded for unauthorized requests")

    def forbidden_price(*_args, **_kwargs):
        calls["price"] += 1
        raise AssertionError("price history should not be loaded for unauthorized requests")

    def forbidden_confirmation(*_args, **_kwargs):
        calls["confirmation"] += 1
        raise AssertionError("confirmation should not be loaded for unauthorized requests")

    import app.services.market_pressure as market_pressure

    monkeypatch.setattr(market_pressure, "index_universe_capabilities", forbidden_membership)
    monkeypatch.setattr(market_pressure, "active_index_membership_snapshot", forbidden_membership)
    monkeypatch.setattr(market_pressure, "_load_price_performance", forbidden_price)

    unauthorized_cases = [
        (ENTITLEMENTS["free"], user, 403),
        (ENTITLEMENTS["premium"], user, 403),
        (ENTITLEMENTS["pro"], suspended, 403),
        (ENTITLEMENTS["free"], None, 401),
    ]
    for entitlements, case_user, expected_status in unauthorized_cases:
        with pytest.raises(HTTPException) as exc:
            build_market_pressure_response(
                db,
                universe="watchlist",
                period="1d",
                view="market_pressure",
                entitlements=entitlements,
                user=case_user,
                confirmation_loader=forbidden_confirmation,
            )
        assert exc.value.status_code == expected_status

    assert calls == {"membership": 0, "price": 0, "confirmation": 0}


def test_market_pressure_capabilities_are_gated_and_do_not_load_request_work(db, monkeypatch):
    user = _seed_watchlist(db, symbols=["LOCK"])
    calls = {"price": 0, "confirmation": 0}

    def forbidden_price(*_args, **_kwargs):
        calls["price"] += 1
        raise AssertionError("price history should not be loaded for capabilities")

    def forbidden_confirmation(*_args, **_kwargs):
        calls["confirmation"] += 1
        raise AssertionError("confirmation should not be loaded for capabilities")

    import app.services.market_pressure as market_pressure

    monkeypatch.setattr(market_pressure, "_load_price_performance", forbidden_price)

    capabilities = build_market_pressure_capabilities_response(
        db,
        entitlements=ENTITLEMENTS["pro"],
        user=user,
    )
    assert capabilities["universes"]["watchlist"] is True
    assert capabilities["universes"]["sp500"] is False

    with pytest.raises(HTTPException) as exc:
        build_market_pressure_capabilities_response(db, entitlements=ENTITLEMENTS["free"], user=user)
    assert exc.value.status_code == 403
    assert calls == {"price": 0, "confirmation": 0}


def test_market_pressure_pro_and_admin_can_access_supported_index_universes(db):
    user = _seed_watchlist(db, symbols=["WATCH"])
    sp500_symbols = _seed_index_membership(db, "sp500", count=503)
    nasdaq_symbols = _seed_index_membership(db, "nasdaq100", count=101)
    for symbol in [sp500_symbols[0], nasdaq_symbols[0]]:
        _seed_price(db, symbol, start_close=100, end_close=104)
        db.add(TickerMeta(symbol=symbol, company_name=f"{symbol} Corp", exchange="NASDAQ", sector="Technology"))
    db.commit()

    def fixture_confirmation(_db, symbols):
        return {
            symbol: _bundle(symbol, {"price_volume": _source("bullish"), "signals": _source("bullish")})
            for symbol in symbols[:1]
        }

    pro_sp500 = build_market_pressure_response(
        db,
        universe="sp500",
        period="1d",
        view="market_pressure",
        entitlements=ENTITLEMENTS["pro"],
        user=user,
        confirmation_loader=fixture_confirmation,
    )
    admin_nasdaq = build_market_pressure_response(
        db,
        universe="nasdaq100",
        period="1d",
        view="market_pressure",
        entitlements=ENTITLEMENTS["admin"],
        user=user,
        confirmation_loader=fixture_confirmation,
    )

    assert pro_sp500["capabilities"]["universes"]["sp500"] is True
    assert pro_sp500["capabilities"]["universeDetails"]["sp500"]["membershipCount"] == 503
    assert pro_sp500["summary"]["symbolCount"] == 503
    assert pro_sp500["summary"]["unavailableCount"] == 502
    assert "metadata" not in pro_sp500
    assert admin_nasdaq["capabilities"]["universes"]["nasdaq100"] is True
    assert admin_nasdaq["capabilities"]["universeDetails"]["nasdaq100"]["membershipCount"] == 101
    assert admin_nasdaq["summary"]["symbolCount"] == 101
    assert admin_nasdaq["capabilities"]["universes"]["all_us"] is False
    assert admin_nasdaq["capabilities"]["universeDetails"]["all_us"]["reason"] == "complete_us_equity_universe_not_available"


def test_market_pressure_sp500_excludes_duplicate_alphabet_share_class(db):
    user = _seed_watchlist(db, symbols=["WATCH"])
    symbols = [f"SP{idx:03d}" for idx in range(1, 502)] + ["GOOG", "GOOGL"]
    result = refresh_index_memberships(
        db,
        index_code="sp500",
        rows=[{"symbol": symbol} for symbol in symbols],
        source="fixture:index-memberships",
        source_as_of=date(2026, 7, 14),
        refreshed_at=datetime(2026, 7, 14, tzinfo=timezone.utc),
    )
    assert result.status == "ok"
    db.add(TickerMeta(symbol="GOOGL", company_name="Alphabet Inc", exchange="NASDAQ", sector="Communication Services"))
    db.add(FundamentalsCache(symbol="GOOGL", provider="fixture", fetched_at=datetime.now(timezone.utc), status="ok", market_cap=2_000_000_000_000, sector="Communication Services"))
    db.commit()

    response = build_market_pressure_response(
        db,
        universe="sp500",
        period="1d",
        view="market_pressure",
        entitlements=ENTITLEMENTS["pro"],
        user=user,
        confirmation_loader=lambda _db, symbols: {},
    )

    tiles = _tiles_by_symbol(response)
    assert response["capabilities"]["universeDetails"]["sp500"]["membershipCount"] == 503
    assert response["summary"]["symbolCount"] == 502
    assert "GOOG" not in tiles
    assert "GOOGL" in tiles


def test_market_pressure_audit_flags_cached_sp500_missing_important_symbols(db):
    user = _seed_watchlist(db, symbols=["WATCH"])
    symbols = ["NVDA", "AAPL", "MSFT", "AMZN", "TSLA", "JPM"] + [f"SP{idx:03d}" for idx in range(1, 498)]
    result = refresh_index_memberships(
        db,
        index_code="sp500",
        rows=[{"symbol": symbol} for symbol in symbols],
        source="fixture:index-memberships",
        source_as_of=date(2026, 7, 14),
        refreshed_at=datetime(2026, 7, 14, tzinfo=timezone.utc),
    )
    assert result.status == "ok"
    generated_at = datetime.now(timezone.utc)
    for symbol in symbols:
        if symbol == "NVDA":
            continue
        tile = _minimal_market_pressure_tile(symbol, market_cap=500_000_000_000 if symbol in {"AAPL", "MSFT", "AMZN"} else 100_000_000_000)
        db.add(
            MarketPressureSnapshot(
                universe="sp500",
                period="1d",
                symbol=symbol,
                company_name=tile["companyName"],
                sector=tile["sector"],
                exchange=tile["exchange"],
                price_change_pct=tile["priceChangePct"],
                market_cap=tile["marketCap"],
                confirmation_direction=tile["confirmationDirection"],
                data_state=tile["dataState"],
                generated_at=generated_at,
                tile_json=json.dumps(tile),
            )
        )
    db.commit()

    response = build_market_pressure_response(
        db,
        universe="sp500",
        period="1d",
        view="market_pressure",
        entitlements=ENTITLEMENTS["pro"],
        user=user,
        confirmation_loader=lambda _db, symbols: {},
    )

    tiles = _tiles_by_symbol(response)
    assert "market_pressure_snapshot_rejected_by_audit" in response["warnings"]
    assert "NVDA" in tiles
    assert response["audit"]["status"] == "fail"
    assert "NVDA" in response["audit"]["importantMissingMarketCapSymbols"]
    assert any(warning.startswith("market_pressure_audit:important_market_cap_missing:") for warning in response["warnings"])


def test_market_pressure_audit_flags_important_symbols_without_market_cap(db, monkeypatch):
    user = _seed_watchlist(db, symbols=["WATCH"])
    symbols = ["NVDA", "AAPL", "MSFT", "AMZN", "TSLA", "JPM"] + [f"SP{idx:03d}" for idx in range(1, 498)]
    result = refresh_index_memberships(
        db,
        index_code="sp500",
        rows=[{"symbol": symbol} for symbol in symbols],
        source="fixture:index-memberships",
        source_as_of=date(2026, 7, 14),
        refreshed_at=datetime(2026, 7, 14, tzinfo=timezone.utc),
    )
    assert result.status == "ok"
    monkeypatch.setattr(market_pressure, "_load_live_one_day_price_fallback", lambda *_args, **_kwargs: {})
    for symbol in symbols:
        db.add(TickerMeta(symbol=symbol, company_name=f"{symbol} Corp", exchange="NASDAQ", sector="Technology"))
    db.commit()

    response = build_market_pressure_response(
        db,
        universe="sp500",
        period="1d",
        view="market_pressure",
        entitlements=ENTITLEMENTS["pro"],
        user=user,
        confirmation_loader=lambda _db, symbols: {},
    )

    assert response["summary"]["symbolCount"] == 503
    assert response["audit"]["status"] == "fail"
    assert {"NVDA", "AAPL", "AMZN", "TSLA", "JPM"}.issubset(set(response["audit"]["importantMissingMarketCapSymbols"]))
    assert any(warning.startswith("market_pressure_audit:important_market_cap_missing:") for warning in response["warnings"])


def test_market_pressure_etf_universe_uses_security_master_asset_classes(db):
    user = _seed_watchlist(db, symbols=["WATCH"])
    db.add_all(
        [
            Security(symbol="SPY", name="SPDR S&P 500 ETF Trust", asset_class="etf_fund", sector="US Large Cap"),
            Security(symbol="QQQ", name="Invesco QQQ Trust ETF", asset_class="ETF", sector="US Large Cap"),
            Security(symbol="TLT", name="iShares 20+ Year Treasury Bond ETF", asset_class="fund", sector="Fixed Income"),
            Security(symbol="AAPL", name="Apple Inc.", asset_class="equity", sector="Technology"),
        ]
    )
    db.add_all(
        [
            TickerMeta(symbol="SPY", company_name="SPDR S&P 500 ETF Trust", exchange="NYSEARCA", sector="US Large Cap"),
            TickerMeta(symbol="QQQ", company_name="Invesco QQQ Trust ETF", exchange="NASDAQ", sector="US Large Cap"),
            TickerMeta(symbol="TLT", company_name="iShares 20+ Year Treasury Bond ETF", exchange="NASDAQ", sector="Fixed Income"),
        ]
    )
    db.commit()
    for symbol in ["SPY", "QQQ", "TLT", "AAPL"]:
        _seed_price(db, symbol, start_close=100, end_close=103)

    response = build_market_pressure_response(
        db,
        universe="etfs",
        period="1d",
        view="market_pressure",
        entitlements=ENTITLEMENTS["pro"],
        user=user,
        confirmation_loader=lambda _db, symbols: {
            symbol: _bundle(symbol, {"price_volume": _source("bullish"), "signals": _source("bullish")})
            for symbol in symbols
        },
    )

    assert response["universe"] == "etf"
    assert response["capabilities"]["universes"]["etf"] is True
    assert response["capabilities"]["universeDetails"]["etf"]["membershipCount"] == 3
    assert response["capabilities"]["universeDetails"]["etf"]["sourceLabel"] == "Walnut ETF securities"
    assert response["summary"]["symbolCount"] == 3
    assert set(_tiles_by_symbol(response)) == {"SPY", "QQQ", "TLT"}
    assert "AAPL" not in _tiles_by_symbol(response)


def test_market_pressure_unsupported_invalid_and_auth_states(db):
    assert resolve_market_pressure_params(universe="etfs", period="1d", view="market-pressure").universe == "etf"
    params = resolve_market_pressure_params(universe="all-us", period="bad", view="crowded-trades")
    assert params.universe == "all_us"
    assert params.period == "1d"
    assert params.view == "crowded_trades"
    assert "invalid_period:bad" in params.warnings

    user = _seed_watchlist(db, symbols=[])
    unsupported = build_market_pressure_response(
        db,
        universe="all_us",
        period="1d",
        view="market_pressure",
        entitlements=ENTITLEMENTS["pro"],
        user=user,
        confirmation_loader=lambda _db, _symbols: {},
    )
    assert unsupported["warnings"] == ["unsupported_universe:all_us"]
    assert unsupported["capabilities"]["universes"]["all_us"] is False

    crowded = build_market_pressure_response(
        db,
        universe="watchlist",
        period="1d",
        view="crowded_trades",
        entitlements=ENTITLEMENTS["pro"],
        user=user,
        confirmation_loader=lambda _db, _symbols: {},
    )
    assert crowded["warnings"] == ["unsupported_view:crowded_trades"]


def test_index_membership_refresh_rejects_empty_or_malformed_without_wiping_existing(db):
    existing_symbols = _seed_index_membership(db, "sp500", count=503)

    empty = refresh_index_memberships(
        db,
        index_code="sp500",
        rows=[],
        source="fixture:index-memberships",
        source_as_of=date(2026, 7, 15),
        refreshed_at=datetime(2026, 7, 15, tzinfo=timezone.utc),
    )
    malformed = refresh_index_memberships(
        db,
        index_code="sp500",
        rows=[{"not_symbol": "AAPL"}],
        source="fixture:index-memberships",
        source_as_of=date(2026, 7, 15),
        refreshed_at=datetime(2026, 7, 15, tzinfo=timezone.utc),
    )

    active = db.query(IndexMembership).filter_by(index_code="sp500", is_active=True).all()
    assert empty.status == "rejected"
    assert malformed.status == "rejected"
    assert sorted(row.symbol for row in active) == existing_symbols


def test_index_membership_refresh_end_dates_removed_members_without_deleting(db):
    existing_symbols = _seed_index_membership(db, "nasdaq100", count=101)
    refreshed_symbols = existing_symbols[:-1]
    refreshed_symbols.append("NQ999")

    result = refresh_index_memberships(
        db,
        index_code="nasdaq100",
        rows=refreshed_symbols,
        source="fixture:index-memberships",
        source_as_of=date(2026, 7, 15),
        refreshed_at=datetime(2026, 7, 15, tzinfo=timezone.utc),
    )

    removed = db.query(IndexMembership).filter_by(index_code="nasdaq100", symbol=existing_symbols[-1]).one()
    added = db.query(IndexMembership).filter_by(index_code="nasdaq100", symbol="NQ999").one()
    assert result.status == "ok"
    assert result.end_dated_count == 1
    assert removed.is_active is False
    assert removed.effective_to == date(2026, 7, 15)
    assert added.is_active is True


def test_market_pressure_watchlist_is_user_scoped(db):
    user_a = _seed_watchlist(db, user_email="a@example.com", symbols=["AAA"])
    user_b = _seed_watchlist(db, user_email="b@example.com", symbols=["BBB"])
    _seed_price(db, "AAA")
    _seed_price(db, "BBB")
    bundles = {
        "AAA": _bundle("AAA", {"price_volume": _source("bullish"), "signals": _source("bullish")}),
        "BBB": _bundle("BBB", {"price_volume": _source("bearish"), "signals": _source("bearish")}),
    }

    response_a = build_market_pressure_response(
        db,
        universe="watchlist",
        period="1d",
        view="market_pressure",
        entitlements=ENTITLEMENTS["admin"],
        user=user_a,
        confirmation_loader=lambda _db, symbols: {symbol: bundles[symbol] for symbol in symbols},
    )
    response_b = build_market_pressure_response(
        db,
        universe="watchlist",
        period="1d",
        view="market_pressure",
        entitlements=ENTITLEMENTS["admin"],
        user=user_b,
        confirmation_loader=lambda _db, symbols: {symbol: bundles[symbol] for symbol in symbols},
    )

    assert set(_tiles_by_symbol(response_a)) == {"AAA"}
    assert set(_tiles_by_symbol(response_b)) == {"BBB"}
