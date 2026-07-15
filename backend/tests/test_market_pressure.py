from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from fastapi import HTTPException
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db import Base
from app.entitlements import ENTITLEMENTS
from app.models import FundamentalsCache, PriceCache, Security, TickerMeta, UserAccount, Watchlist, WatchlistItem
from app.services.confirmation_score import confirmation_score_bundle_from_source_payloads
from app.services.market_pressure import build_market_pressure_response, resolve_market_pressure_params


def _session_factory():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(
        bind=engine,
        tables=[
            UserAccount.__table__,
            Watchlist.__table__,
            WatchlistItem.__table__,
            Security.__table__,
            PriceCache.__table__,
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


def _tiles_by_symbol(response: dict) -> dict[str, dict]:
    return {tile["symbol"]: tile for sector in response["sectors"] for tile in sector["tiles"]}


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
    calls = {"price": 0, "confirmation": 0}

    def forbidden_price(*_args, **_kwargs):
        calls["price"] += 1
        raise AssertionError("price history should not be loaded for unauthorized requests")

    def forbidden_confirmation(*_args, **_kwargs):
        calls["confirmation"] += 1
        raise AssertionError("confirmation should not be loaded for unauthorized requests")

    import app.services.market_pressure as market_pressure

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

    assert calls == {"price": 0, "confirmation": 0}


def test_market_pressure_unsupported_invalid_and_auth_states(db):
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
