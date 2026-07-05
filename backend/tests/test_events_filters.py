from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from fastapi import Request
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

import app.routers.events as events_module
from app.db import Base
from app.models import Event, GovernmentContractAction, InstitutionalActivityEvent, InstitutionalHolder, InstitutionalPositionChange, Security, TradeOutcome
from app.routers.events import list_events


def _db() -> Session:
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)
    return Session(engine)


def _stub_enrichment(monkeypatch) -> None:
    monkeypatch.setattr("app.routers.events.get_current_prices_meta_db", lambda *_args, **_kwargs: {})
    monkeypatch.setattr("app.routers.events.get_eod_close", lambda *_args, **_kwargs: None)
    monkeypatch.setattr("app.routers.events.get_confirmation_metrics_for_symbols", lambda *_args, **_kwargs: {})
    monkeypatch.setattr("app.routers.events._ticker_meta_with_security_names", lambda *_args, **_kwargs: {})
    monkeypatch.setattr("app.routers.events.get_cik_meta", lambda *_args, **_kwargs: {})


def _clear_events_response_cache() -> None:
    events_module._EVENTS_RESPONSE_CACHE.clear()
    events_module._EVENTS_RESPONSE_INFLIGHT.clear()


def _request(headers: dict[str, str]) -> Request:
    return Request(
        {
            "type": "http",
            "method": "GET",
            "path": "/api/events",
            "headers": [(key.lower().encode("latin-1"), value.encode("latin-1")) for key, value in headers.items()],
            "query_string": b"",
            "server": ("testserver", 80),
            "scheme": "http",
            "client": ("127.0.0.1", 12345),
        }
    )


def _event(event_id: int, event_type: str, **kwargs) -> Event:
    now = kwargs.pop("ts", datetime(2026, 5, 19, tzinfo=timezone.utc))
    return Event(
        id=event_id,
        event_type=event_type,
        ts=now,
        event_date=kwargs.pop("event_date", now),
        source=kwargs.pop("source", "test"),
        payload_json=json.dumps(kwargs.pop("payload", {})),
        **kwargs,
    )


def _institutional_event(event_id: int, event_type: str, **kwargs) -> InstitutionalActivityEvent:
    return InstitutionalActivityEvent(
        id=event_id,
        symbol=kwargs.pop("symbol", "AAPL"),
        normalized_symbol=kwargs.pop("normalized_symbol", "AAPL"),
        cik=kwargs.pop("cik", "0001067983"),
        holder_name=kwargs.pop("holder_name", "Berkshire Hathaway Inc."),
        event_type=event_type,
        direction=kwargs.pop("direction", "bullish"),
        title=kwargs.pop("title", "Institutional Activity"),
        summary=kwargs.pop("summary", "Reported 13F filing activity."),
        filing_date=kwargs.pop("filing_date", date(2026, 6, 30)),
        report_year=kwargs.pop("report_year", 2026),
        report_quarter=kwargs.pop("report_quarter", 2),
        reported_value_usd=kwargs.pop("reported_value_usd", 125_000_000.0),
        value_delta_usd=kwargs.pop("value_delta_usd", 25_000_000.0),
        ownership_pct=kwargs.pop("ownership_pct", 1.2),
        holder_breadth=kwargs.pop("holder_breadth", 1),
        materiality_score=kwargs.pop("materiality_score", 85.0),
        confirmation_score=kwargs.pop("confirmation_score", 8.0),
        freshness_status=kwargs.pop("freshness_status", "active"),
        **kwargs,
    )


def test_department_filter_in_all_mode_returns_only_matching_contract_events(monkeypatch):
    db = _db()
    try:
        _stub_enrichment(monkeypatch)
        contract = _event(
            1,
            "government_contract",
            symbol="LMT",
            member_name="Department of Defense",
            payload={
                "awarding_agency": "Department of Defense",
                "recipient_name": "Lockheed Martin",
                "event_subtype": "funding_action",
            },
            amount_min=1_000_000,
            amount_max=1_000_000,
        )
        db.add_all(
            [
                contract,
                GovernmentContractAction(
                    parent_award_id="A-1",
                    dedupe_key="A-1-P1",
                    event_id=1,
                    symbol="LMT",
                    awarding_agency="Department of Defense",
                    recipient_name="Lockheed Martin",
                    action_date=datetime(2026, 5, 19, tzinfo=timezone.utc).date(),
                    obligated_amount=1_000_000,
                ),
                _event(2, "insider_trade", symbol="AAPL", member_name="Tim Cook", payload={"reporting_cik": "0001214156"}),
                _event(3, "congress_trade", symbol="MSFT", member_name="Nancy Pelosi", member_bioguide_id="P000197"),
            ]
        )
        db.commit()

        page = list_events(db=db, mode="all", department="Department of Defense", limit=10, enrich_prices=False)

        assert [item.id for item in page.items] == [1]
        assert {item.event_type for item in page.items} == {"government_contract"}
    finally:
        db.close()


def test_symbol_scoped_events_return_base_rows_when_price_enrichment_unavailable(monkeypatch):
    db = _db()
    try:
        monkeypatch.setattr("app.routers.events.get_eod_close", lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("busy")))
        monkeypatch.setattr("app.routers.events.get_current_prices_meta_db", lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("busy")))
        monkeypatch.setattr("app.routers.events.get_confirmation_metrics_for_symbols", lambda *_args, **_kwargs: {})
        monkeypatch.setattr("app.routers.events._ticker_meta_with_security_names", lambda *_args, **_kwargs: {})
        monkeypatch.setattr("app.routers.events.get_cik_meta", lambda *_args, **_kwargs: {})
        monkeypatch.setattr("app.routers.events._enqueue_missing_trade_outcomes", lambda *_args, **_kwargs: None)
        db.add(
            _event(
                101,
                "congress_trade",
                symbol="NBIS",
                member_name="Member",
                member_bioguide_id="M1",
                trade_type="purchase",
                amount_min=1_000,
                amount_max=15_000,
                payload={"trade_date": "2026-05-01", "report_date": "2026-05-02"},
            )
        )
        db.commit()

        page = list_events(db=db, symbol="NBIS", recent_days=365, limit=100, enrich_prices=True)

        assert [item.id for item in page.items] == [101]
        assert page.items[0].price is None
        assert page.items[0].pnl_pct is None
    finally:
        db.close()


def test_logged_out_feed_ssr_returns_public_events(monkeypatch):
    db = _db()
    try:
        _clear_events_response_cache()
        _stub_enrichment(monkeypatch)
        db.add(
            _event(
                151,
                "congress_trade",
                symbol="T",
                member_name="Gary Peters",
                member_bioguide_id="P000595",
                trade_type="purchase",
                payload={"trade_date": "2026-06-29", "report_date": "2026-07-02"},
            )
        )
        db.commit()

        request = _request(
            {
                "x-walnut-request-source": "ssr",
                "x-walnut-route-family": "feed",
                "x-walnut-component": "Feed",
                "x-walnut-panel": "Feed",
            }
        )
        page = list_events(request=request, db=db, mode="all", limit=10, enrich_prices=False)

        assert [item.id for item in page.items] == [151]
        assert page.items[0].event_type == "congress_trade"
    finally:
        db.close()


def test_events_feed_uses_one_shared_quote_lookup_for_visible_pnl(monkeypatch):
    db = _db()
    try:
        _clear_events_response_cache()
        monkeypatch.setattr("app.routers.events.get_eod_close", lambda *_args, **_kwargs: None)
        monkeypatch.setattr("app.routers.events.get_confirmation_metrics_for_symbols", lambda *_args, **_kwargs: {})
        monkeypatch.setattr("app.routers.events._ticker_meta_with_security_names", lambda *_args, **_kwargs: {})
        monkeypatch.setattr("app.routers.events.get_cik_meta", lambda *_args, **_kwargs: {})
        monkeypatch.setattr("app.routers.events._enqueue_missing_trade_outcomes", lambda *_args, **_kwargs: None)
        calls: list[tuple[list[str], dict]] = []

        def fake_quotes(_db, symbols, **kwargs):
            calls.append((list(symbols), kwargs))
            return {
                "AAPL": {
                    "symbol": "AAPL",
                    "price": 110.0,
                    "asof_ts": datetime(2026, 7, 1, tzinfo=timezone.utc),
                    "is_stale": False,
                    "source": "cache",
                }
            }

        monkeypatch.setattr("app.routers.events.get_current_prices_meta_db", fake_quotes)
        db.add_all(
            [
                _event(
                    201,
                    "congress_trade",
                    symbol="AAPL",
                    member_name="Member One",
                    member_bioguide_id="M1",
                    trade_type="purchase",
                    payload={"trade_date": "2026-06-01", "report_date": "2026-06-02"},
                ),
                _event(
                    202,
                    "insider_trade",
                    symbol="AAPL",
                    member_name="Insider One",
                    trade_type="purchase",
                    payload={"trade_date": "2026-06-01", "report_date": "2026-06-02", "reporting_cik": "0001", "shares": 10},
                ),
                TradeOutcome(
                    event_id=201,
                    member_id="M1",
                    member_name="Member One",
                    symbol="AAPL",
                    trade_type="purchase",
                    trade_date=date(2026, 6, 1),
                    entry_price=100.0,
                    current_price=None,
                    return_pct=None,
                    benchmark_symbol="^GSPC",
                    scoring_status="no_current_price",
                    methodology_version="feed_pnl_cache_v1",
                ),
                TradeOutcome(
                    event_id=202,
                    member_id="I1",
                    member_name="Insider One",
                    symbol="AAPL",
                    trade_type="purchase",
                    trade_date=date(2026, 6, 1),
                    entry_price=100.0,
                    current_price=None,
                    return_pct=None,
                    benchmark_symbol="^GSPC",
                    scoring_status="no_current_price",
                    methodology_version="insider_v1",
                ),
            ]
        )
        db.commit()

        request = _request(
            {
                "user-agent": "Mozilla/5.0",
                "accept": "application/json",
                "x-walnut-request-source": "client",
                "x-walnut-active-user": "browser",
                "x-walnut-route-family": "feed",
            }
        )

        page = list_events(request=request, db=db, mode="all", limit=10, enrich_prices=True)

        assert calls == [
                (
                    ["AAPL"],
                    {
                        "allow_cache_write": False,
                        "release_connection_before_fetch": False,
                        "lane": "feed_quote",
                        "allow_live_user_fetch": True,
                        "stale_while_revalidate": True,
                        "cache_only": False,
                        "force_quote_endpoint": True,
                        "skip_db_sanity": True,
                    },
                )
            ]
        by_id = {item.id: item for item in page.items}
        assert by_id[201].current_price == 110.0
        assert round(by_id[201].pnl_pct or 0, 6) == 10.0
        assert round(by_id[201].gain_loss_percent or 0, 6) == 10.0
        assert by_id[201].gain_loss_status == "ok"
        assert by_id[201].gain_loss_amount is None
        assert by_id[201].pnl_source == "quote_cache"
        assert by_id[202].current_price == 110.0
        assert round(by_id[202].pnl_pct or 0, 6) == 10.0
        assert round(by_id[202].gain_loss_amount or 0, 6) == 100.0
        assert by_id[202].gain_loss_status == "ok"
        assert by_id[202].pnl_source == "quote_cache"
    finally:
        db.close()


def test_events_feed_direct_logged_out_request_uses_cache_only_for_visible_pnl(monkeypatch):
    db = _db()
    try:
        _clear_events_response_cache()
        monkeypatch.setattr("app.routers.events.get_eod_close", lambda *_args, **_kwargs: None)
        monkeypatch.setattr("app.routers.events.get_confirmation_metrics_for_symbols", lambda *_args, **_kwargs: {})
        monkeypatch.setattr("app.routers.events._ticker_meta_with_security_names", lambda *_args, **_kwargs: {})
        monkeypatch.setattr("app.routers.events.get_cik_meta", lambda *_args, **_kwargs: {})
        monkeypatch.setattr("app.routers.events._enqueue_missing_trade_outcomes", lambda *_args, **_kwargs: None)
        calls: list[tuple[list[str], dict]] = []

        def fake_quotes(_db, symbols, **kwargs):
            calls.append((list(symbols), kwargs))
            assert kwargs["cache_only"] is True
            assert kwargs["allow_live_user_fetch"] is False
            return {}

        monkeypatch.setattr("app.routers.events.get_current_prices_meta_db", fake_quotes)
        db.add_all(
            [
                _event(
                    211,
                    "congress_trade",
                    symbol="AAPL",
                    member_name="Member One",
                    member_bioguide_id="M1",
                    trade_type="purchase",
                    payload={"trade_date": "2026-06-01", "report_date": "2026-06-02"},
                ),
                TradeOutcome(
                    event_id=211,
                    member_id="M1",
                    member_name="Member One",
                    symbol="AAPL",
                    trade_type="purchase",
                    trade_date=date(2026, 6, 1),
                    entry_price=100.0,
                    current_price=None,
                    return_pct=None,
                    benchmark_symbol="^GSPC",
                    scoring_status="no_current_price",
                    methodology_version="feed_pnl_cache_v1",
                ),
            ]
        )
        db.commit()

        request = _request({"user-agent": "curl/8.0", "accept": "application/json"})
        page = list_events(request=request, db=db, mode="all", limit=10, enrich_prices=True)

        assert calls == [
            (
                ["AAPL"],
                {
                    "allow_cache_write": False,
                    "release_connection_before_fetch": False,
                    "lane": "feed_quote",
                        "allow_live_user_fetch": False,
                        "stale_while_revalidate": False,
                        "cache_only": True,
                        "force_quote_endpoint": False,
                        "skip_db_sanity": False,
                    },
                )
            ]
        assert page.items[0].current_price is None
        assert page.items[0].pnl_pct is None
        assert page.items[0].gain_loss_status == "missing_current_price"
    finally:
        db.close()


def test_symbol_scoped_feed_uses_payload_trade_price_for_visible_pnl(monkeypatch):
    db = _db()
    try:
        _clear_events_response_cache()
        monkeypatch.setattr("app.routers.events.get_eod_close", lambda *_args, **_kwargs: None)
        monkeypatch.setattr("app.routers.events.get_confirmation_metrics_for_symbols", lambda *_args, **_kwargs: {})
        monkeypatch.setattr("app.routers.events._ticker_meta_with_security_names", lambda *_args, **_kwargs: {})
        monkeypatch.setattr("app.routers.events.get_cik_meta", lambda *_args, **_kwargs: {})
        enqueue_calls: list[int] = []
        monkeypatch.setattr("app.routers.events._enqueue_missing_trade_outcomes", lambda *_args, **_kwargs: enqueue_calls.append(1))
        calls: list[tuple[list[str], dict]] = []

        def fake_quotes(_db, symbols, **kwargs):
            calls.append((list(symbols), kwargs))
            return {
                "TSM": {
                    "symbol": "TSM",
                    "price": 80.0,
                    "asof_ts": datetime(2026, 7, 1, tzinfo=timezone.utc),
                    "is_stale": False,
                    "source": "live_quote",
                }
            }

        monkeypatch.setattr("app.routers.events.get_current_prices_meta_db", fake_quotes)
        db.add(
            _event(
                221,
                "insider_trade",
                symbol="TSM",
                member_name="Tien Bor-Zen",
                trade_type="purchase",
                amount_min=75_700,
                amount_max=75_700,
                payload={
                    "transaction_date": "2026-06-30",
                    "filing_date": "2026-06-30",
                    "price": 75.7,
                    "shares": 1000,
                    "raw": {"price": 75.7, "securitiesTransacted": 1000},
                },
            )
        )
        db.commit()

        request = _request(
            {
                "user-agent": "Mozilla/5.0",
                "accept": "application/json",
                "x-walnut-request-source": "client",
                "x-walnut-active-user": "browser",
                "x-walnut-route-family": "feed",
            }
        )
        page = list_events(request=request, db=db, symbol="TSM", limit=10, enrich_prices=True)

        assert calls == [
                (
                    ["TSM"],
                    {
                        "allow_cache_write": False,
                        "release_connection_before_fetch": False,
                        "lane": "feed_quote",
                        "allow_live_user_fetch": True,
                        "stale_while_revalidate": True,
                        "cache_only": False,
                        "force_quote_endpoint": True,
                        "skip_db_sanity": True,
                    },
                )
            ]
        assert enqueue_calls == []
        item = page.items[0]
        assert item.trade_price == 75.7
        assert item.current_price == 80.0
        assert round(item.gain_loss_percent or 0, 6) == round(((80.0 - 75.7) / 75.7) * 100, 6)
        assert round(item.gain_loss_amount or 0, 6) == 4300.0
        assert item.gain_loss_status == "ok"
    finally:
        db.close()


def test_feed_gain_loss_recomputes_from_visible_prices_when_persisted_return_is_stale(monkeypatch):
    db = _db()
    try:
        _clear_events_response_cache()
        monkeypatch.setattr("app.routers.events.get_eod_close", lambda *_args, **_kwargs: None)
        monkeypatch.setattr("app.routers.events.get_confirmation_metrics_for_symbols", lambda *_args, **_kwargs: {})
        monkeypatch.setattr("app.routers.events._ticker_meta_with_security_names", lambda *_args, **_kwargs: {})
        monkeypatch.setattr("app.routers.events.get_cik_meta", lambda *_args, **_kwargs: {})
        calls: list[tuple[list[str], dict]] = []

        def fake_quotes(_db, symbols, **kwargs):
            calls.append((list(symbols), kwargs))
            return {
                "IBP": {"symbol": "IBP", "price": 228.0, "asof_ts": datetime(2026, 7, 5, tzinfo=timezone.utc), "is_stale": False},
                "SEPN": {"symbol": "SEPN", "price": 34.0, "asof_ts": datetime(2026, 7, 5, tzinfo=timezone.utc), "is_stale": False},
                "MCB": {"symbol": "MCB", "price": 98.53, "asof_ts": datetime(2026, 7, 5, tzinfo=timezone.utc), "is_stale": False},
            }

        monkeypatch.setattr("app.routers.events.get_current_prices_meta_db", fake_quotes)
        db.add_all(
            [
                _event(
                    231,
                    "congress_trade",
                    symbol="IBP",
                    member_name="David Taylor",
                    member_bioguide_id="TAYLOR",
                    trade_type="purchase",
                    payload={"trade_date": "2026-07-01", "report_date": "2026-07-02"},
                ),
                _event(
                    232,
                    "congress_trade",
                    symbol="SEPN",
                    member_name="David Long",
                    member_bioguide_id="LONG",
                    trade_type="sale",
                    payload={"trade_date": "2026-07-01", "report_date": "2026-07-02"},
                ),
                _event(
                    233,
                    "congress_trade",
                    symbol="MCB",
                    member_name="Nick Rosenberg",
                    member_bioguide_id="ROSENBERG",
                    trade_type="sale",
                    payload={"trade_date": "2026-07-01", "report_date": "2026-07-02"},
                ),
                TradeOutcome(
                    event_id=231,
                    member_id="TAYLOR",
                    member_name="David Taylor",
                    symbol="IBP",
                    trade_type="purchase",
                    trade_date=date(2026, 7, 1),
                    entry_price=209.0,
                    current_price=117.65,
                    return_pct=-43.0,
                    benchmark_symbol="^GSPC",
                    benchmark_return_pct=0.0,
                    scoring_status="ok",
                    methodology_version="feed_pnl_cache_v1",
                ),
                TradeOutcome(
                    event_id=232,
                    member_id="LONG",
                    member_name="David Long",
                    symbol="SEPN",
                    trade_type="sale",
                    trade_date=date(2026, 7, 1),
                    entry_price=35.0,
                    current_price=21.4,
                    return_pct=38.0,
                    benchmark_symbol="^GSPC",
                    benchmark_return_pct=0.0,
                    scoring_status="ok",
                    methodology_version="feed_pnl_cache_v1",
                ),
                TradeOutcome(
                    event_id=233,
                    member_id="ROSENBERG",
                    member_name="Nick Rosenberg",
                    symbol="MCB",
                    trade_type="sale",
                    trade_date=date(2026, 7, 1),
                    entry_price=100.0,
                    current_price=98.53,
                    return_pct=40.0,
                    benchmark_symbol="^GSPC",
                    benchmark_return_pct=0.0,
                    scoring_status="ok",
                    methodology_version="feed_pnl_cache_v1",
                ),
            ]
        )
        db.commit()

        page = list_events(db=db, mode="all", limit=10, enrich_prices=True)
        by_id = {item.id: item for item in page.items}

        assert calls and calls[0][0] == ["MCB", "SEPN", "IBP"]
        assert calls[0][1]["cache_only"] is False
        assert by_id[231].trade_price == 209.0
        assert by_id[231].current_price == 228.0
        assert by_id[231].gain_loss_percent == by_id[231].pnl_pct
        assert round(by_id[231].gain_loss_percent or 0, 6) == round(((228.0 - 209.0) / 209.0) * 100, 6)

        assert by_id[232].trade_price == 35.0
        assert by_id[232].current_price == 34.0
        assert round(by_id[232].gain_loss_percent or 0, 6) == round(((35.0 - 34.0) / 35.0) * 100, 6)

        assert by_id[233].trade_price == 100.0
        assert by_id[233].current_price == 98.53
        assert round(by_id[233].gain_loss_percent or 0, 6) == round(((100.0 - 98.53) / 100.0) * 100, 6)
    finally:
        db.close()


def test_insider_name_and_role_filters_work_in_all_and_insider_modes(monkeypatch):
    db = _db()
    try:
        _stub_enrichment(monkeypatch)
        db.add_all(
            [
                _event(
                    10,
                    "insider_trade",
                    symbol="AAPL",
                    member_name=None,
                    trade_type="purchase",
                    amount_max=5_000_000,
                    payload={
                        "insider_name": "Tim Cook",
                        "reporting_cik": "0001214156",
                        "company_name": "Apple Inc.",
                        "officerTitle": "Chief Executive Officer",
                    },
                ),
                _event(
                    11,
                    "insider_trade",
                    symbol="AAPL",
                    member_name="Arthur Levinson",
                    trade_type="sale",
                    amount_max=1_000_000,
                    payload={"reporting_cik": "0001111111", "role": "Director"},
                ),
            ]
        )
        db.commit()

        insider_page = list_events(db=db, mode="insider", member="Tim Cook", limit=10, enrich_prices=False)
        all_page = list_events(db=db, mode="all", member="Tim Cook", limit=10, enrich_prices=False)
        role_page = list_events(db=db, mode="all", role="CEO", limit=10, enrich_prices=False)

        assert [item.id for item in insider_page.items] == [10]
        assert [item.id for item in all_page.items] == [10]
        assert [item.id for item in role_page.items] == [10]
    finally:
        db.close()


def test_asset_class_filters_cover_public_equity_treasury_crypto_and_other(monkeypatch):
    db = _db()
    try:
        _stub_enrichment(monkeypatch)
        db.add(Security(symbol="JPM", name="JPMorgan Chase & Co", asset_class="stock", sector="Financials"))
        db.add_all(
            [
                _event(20, "congress_trade", symbol="MSFT", member_name="Member", member_bioguide_id="M1", trade_type="purchase", payload={"asset_class": "equity"}),
                _event(21, "insider_trade", symbol="AAPL", member_name="Tim Cook", trade_type="purchase", payload={"reporting_cik": "0001214156"}),
                _event(22, "congress_treasury_trade", symbol=None, member_name="Member", member_bioguide_id="M1", payload={"asset_class": "treasury"}),
                _event(23, "congress_crypto_trade", symbol=None, member_name="Member", member_bioguide_id="M1", payload={"asset_class": "crypto"}),
                _event(24, "congress_trade", symbol=None, member_name="Member", member_bioguide_id="M1", payload={"asset_class": "other"}),
                _event(25, "congress_trade", symbol="IBIT", member_name="Member", member_bioguide_id="M1", payload={"asset_class": "etf", "security_name": "iShares Bitcoin Trust ETF"}),
                _event(26, "congress_trade", symbol=None, member_name="Member", member_bioguide_id="M1", payload={"asset_class": "etf", "security_name": "iShares 3-7 Year Treasury Bond ETF"}),
                _event(27, "congress_trade", symbol="JPM", member_name="Member", member_bioguide_id="M1", payload={"asset_class": "other", "security_name": "JPMorgan Chase & Co"}),
                _event(28, "congress_trade", symbol=None, member_name="Member", member_bioguide_id="M1", payload={"symbol": "JPM", "asset_class": "other", "security_name": "JPMorgan Chase & Co"}),
                _event(29, "congress_trade", symbol=None, member_name="Member", member_bioguide_id="M1", payload={"asset_class": "other", "security_name": "JPMorgan Chase & Co"}),
            ]
        )
        db.commit()

        equities = list_events(db=db, mode="all", asset_class="equity", limit=10, enrich_prices=False)
        treasuries = list_events(db=db, mode="congress", asset_class="treasury", limit=10, enrich_prices=False)
        crypto = list_events(db=db, mode="congress", asset_class="crypto", limit=10, enrich_prices=False)
        other = list_events(db=db, mode="congress", asset_class="other", limit=10, enrich_prices=False)
        etf_fund = list_events(db=db, mode="congress", asset_class="etf_fund", limit=10, enrich_prices=False)

        assert [item.id for item in equities.items] == [27, 21, 20]
        assert [item.id for item in treasuries.items] == [22]
        assert [item.id for item in crypto.items] == [23]
        assert [item.id for item in other.items] == [24]
        assert [item.id for item in etf_fund.items] == [26, 25]
    finally:
        db.close()


def test_filed_after_pnl_and_signal_filters(monkeypatch):
    db = _db()
    try:
        _stub_enrichment(monkeypatch)
        db.add_all(
            [
                _event(
                    30,
                    "congress_trade",
                    symbol="MSFT",
                    member_name="Member",
                    member_bioguide_id="M1",
                    trade_type="purchase",
                    payload={
                        "trade_date": "2026-05-01",
                        "report_date": "2026-05-10",
                        "smart_score": 82,
                    },
                ),
                _event(
                    31,
                    "congress_trade",
                    symbol="TSLA",
                    member_name="Member",
                    member_bioguide_id="M1",
                    trade_type="purchase",
                    payload={
                        "trade_date": "2026-01-01",
                        "report_date": "2026-05-10",
                        "smart_score": 45,
                    },
                ),
                TradeOutcome(
                    event_id=30,
                    member_id="M1",
                    member_name="Member",
                    symbol="MSFT",
                    trade_type="purchase",
                    trade_date=datetime(2026, 5, 1, tzinfo=timezone.utc).date(),
                    entry_price=100.0,
                    current_price=112.5,
                    return_pct=12.5,
                    benchmark_symbol="^GSPC",
                    scoring_status="ok",
                    methodology_version="congress_v1",
                ),
                TradeOutcome(
                    event_id=31,
                    member_id="M1",
                    member_name="Member",
                    symbol="TSLA",
                    trade_type="purchase",
                    trade_date=datetime(2026, 1, 1, tzinfo=timezone.utc).date(),
                    entry_price=100.0,
                    current_price=97.0,
                    return_pct=-3.0,
                    benchmark_symbol="^GSPC",
                    scoring_status="ok",
                    methodology_version="congress_v1",
                ),
            ]
        )
        db.commit()

        filed = list_events(db=db, mode="congress", filed_after_max=30, limit=10, enrich_prices=False)
        pnl = list_events(db=db, mode="congress", pnl_min=10, limit=10, enrich_prices=True)
        pnl_max = list_events(db=db, mode="congress", pnl_max=0, limit=10, enrich_prices=True)
        signal = list_events(db=db, mode="congress", signal_min=70, limit=10, enrich_prices=False)

        assert [item.id for item in filed.items] == [30]
        assert [item.id for item in pnl.items] == [30]
        assert [item.id for item in pnl_max.items] == [31]
        assert [item.id for item in signal.items] == [30]
    finally:
        db.close()


def test_feed_events_expose_actor_and_ticker_net_30d(monkeypatch):
    db = _db()
    try:
        _stub_enrichment(monkeypatch)
        now = datetime.now(timezone.utc)
        db.add_all(
            [
                _event(40, "congress_trade", ts=now, symbol="MSFT", member_name="Member", member_bioguide_id="M1", trade_type="purchase", amount_max=20_000),
                _event(41, "congress_trade", ts=now - timedelta(days=1), symbol="MSFT", member_name="Other", member_bioguide_id="M2", trade_type="sale", amount_max=5_000),
                _event(42, "insider_trade", ts=now, symbol="AAPL", member_name="Tim Cook", trade_type="purchase", amount_max=10_000, payload={"reporting_cik": "0001214156"}),
                _event(43, "insider_trade", ts=now - timedelta(days=1), symbol="AAPL", member_name="Other Insider", trade_type="sale", amount_max=3_000, payload={"reporting_cik": "0009999999"}),
            ]
        )
        db.commit()

        page = list_events(db=db, mode="all", limit=10, enrich_prices=False)
        by_id = {item.id: item for item in page.items}

        assert by_id[40].member_net_30d == 20_000
        assert by_id[40].symbol_net_30d == 15_000
        assert by_id[42].member_net_30d == 10_000
        assert by_id[42].symbol_net_30d == 7_000
    finally:
        db.close()


def test_list_events_caches_production_http_read_path(monkeypatch):
    _clear_events_response_cache()
    db = _db()
    try:
        _stub_enrichment(monkeypatch)
        first_ts = datetime(2026, 5, 19, tzinfo=timezone.utc)
        db.add(
            _event(
                1,
                "congress_trade",
                ts=first_ts,
                event_date=first_ts,
                symbol="AAPL",
                member_name="Nancy Pelosi",
                member_bioguide_id="P000197",
            )
        )
        db.commit()

        request = object()
        first_page = list_events(request=request, db=db, symbol="AAPL", limit=10, enrich_prices=True)
        assert [item.id for item in first_page.items] == [1]

        second_ts = datetime(2026, 5, 20, tzinfo=timezone.utc)
        db.add(
            _event(
                2,
                "congress_trade",
                ts=second_ts,
                event_date=second_ts,
                symbol="AAPL",
                member_name="Nancy Pelosi",
                member_bioguide_id="P000197",
            )
        )
        db.commit()

        cached_page = list_events(request=request, db=db, symbol="AAPL", limit=10, enrich_prices=True)
        assert [item.id for item in cached_page.items] == [1]

        _clear_events_response_cache()
        uncached_page = list_events(request=request, db=db, symbol="AAPL", limit=10, enrich_prices=True)
        assert [item.id for item in uncached_page.items] == [2, 1]
    finally:
        _clear_events_response_cache()
        db.close()


def test_list_events_caches_anonymous_http_requests_but_not_session_requests(monkeypatch):
    _clear_events_response_cache()
    db = _db()
    try:
        _stub_enrichment(monkeypatch)
        first_ts = datetime(2026, 5, 19, tzinfo=timezone.utc)
        db.add(
            _event(
                11,
                "congress_trade",
                ts=first_ts,
                event_date=first_ts,
                symbol="AAPL",
                member_name="Nancy Pelosi",
                member_bioguide_id="P000197",
            )
        )
        db.commit()

        anonymous_request = _request({"x-walnut-request-source": "load_test", "user-agent": "k6/0.49.0"})
        first_page = list_events(request=anonymous_request, db=db, symbol="AAPL", limit=10, enrich_prices=False)
        assert [item.id for item in first_page.items] == [11]

        second_ts = datetime(2026, 5, 20, tzinfo=timezone.utc)
        db.add(
            _event(
                12,
                "congress_trade",
                ts=second_ts,
                event_date=second_ts,
                symbol="AAPL",
                member_name="Nancy Pelosi",
                member_bioguide_id="P000197",
            )
        )
        db.commit()

        cached_page = list_events(request=anonymous_request, db=db, symbol="AAPL", limit=10, enrich_prices=False)
        assert [item.id for item in cached_page.items] == [11]

        _clear_events_response_cache()
        session_request = _request({"cookie": "ct_session=invalid", "user-agent": "Mozilla/5.0"})
        session_first_page = list_events(request=session_request, db=db, symbol="AAPL", limit=10, enrich_prices=False)
        assert [item.id for item in session_first_page.items] == [12, 11]

        third_ts = datetime(2026, 5, 21, tzinfo=timezone.utc)
        db.add(
            _event(
                13,
                "congress_trade",
                ts=third_ts,
                event_date=third_ts,
                symbol="AAPL",
                member_name="Nancy Pelosi",
                member_bioguide_id="P000197",
            )
        )
        db.commit()

        uncached_session_page = list_events(request=session_request, db=db, symbol="AAPL", limit=10, enrich_prices=False)
        assert [item.id for item in uncached_session_page.items] == [13, 12, 11]
    finally:
        _clear_events_response_cache()
        db.close()


def test_feed_mode_options_include_institutional_without_renaming_contracts():
    feed_modes = Path(__file__).resolve().parents[2] / "frontend" / "lib" / "feedModes.ts"
    text = feed_modes.read_text(encoding="utf-8")

    assert '["government_contracts", "Government Contracts"]' in text
    assert '["institutional", "Institutional"]' in text


def test_institutional_feed_mode_returns_activity_events_for_entitled_users(monkeypatch):
    db = _db()
    try:
        monkeypatch.setattr(events_module, "_can_view_institutional_events", lambda *_args, **_kwargs: True)
        db.add_all(
            [
                InstitutionalHolder(cik="1067983", holder_name="Berkshire Hathaway Inc."),
                _institutional_event(201, "institutional_accumulation", holder_name=None),
                _institutional_event(
                    202,
                    "major_holder_exit",
                    normalized_symbol="MSFT",
                    symbol="MSFT",
                    direction="bearish",
                    reported_value_usd=90_000_000.0,
                    value_delta_usd=-90_000_000.0,
                    materiality_score=90.0,
                ),
                _institutional_event(
                    203,
                    "smart_money_confirmation",
                    normalized_symbol="NVDA",
                    symbol="NVDA",
                    holder_name="Point72 Asset Management",
                    direction="neutral",
                    reported_value_usd=80_000_000.0,
                    value_delta_usd=-12_000_000.0,
                    materiality_score=82.0,
                ),
                _institutional_event(
                    205,
                    "major_holder_reduction",
                    normalized_symbol="GOOG",
                    symbol="GOOG",
                    cik="0001452208",
                    holder_name=None,
                    direction="bearish",
                    reported_value_usd=30_000_000.0,
                    value_delta_usd=-5_000_000.0,
                    materiality_score=75.0,
                ),
                InstitutionalPositionChange(
                    cik="1452208",
                    holder_name="Example Capital Management",
                    symbol="GOOG",
                    normalized_symbol="GOOG",
                    report_year=2026,
                    report_quarter=2,
                    filing_date=date(2026, 6, 30),
                    change_type="decrease",
                    direction="bearish",
                    materiality_score=75.0,
                    is_material=True,
                ),
                _institutional_event(
                    204,
                    "cluster_accumulation",
                    normalized_symbol="TSLA",
                    symbol="TSLA",
                    cik=None,
                    holder_name=None,
                    direction="bullish",
                    reported_value_usd=300_000_000.0,
                    value_delta_usd=75_000_000.0,
                    materiality_score=95.0,
                ),
            ]
        )
        db.commit()

        page = list_events(db=db, tape="institutional", limit=10, enrich_prices=False)
        by_type = {item.event_type: item for item in page.items}

        assert set(by_type) == {"institutional_accumulation", "major_holder_exit", "smart_money_confirmation", "major_holder_reduction"}
        assert by_type["institutional_accumulation"].source == "Institutional Activity"
        assert by_type["institutional_accumulation"].member_name == "Berkshire Hathaway Inc."
        assert by_type["institutional_accumulation"].trade_type == "Reported Increase"
        assert by_type["major_holder_exit"].trade_type == "Reported Exit"
        assert by_type["smart_money_confirmation"].member_name == "Point72 Asset Management"
        assert by_type["smart_money_confirmation"].trade_type == "Reported Reduction"
        assert by_type["major_holder_reduction"].member_name == "Example Capital Management"
        assert by_type["major_holder_reduction"].trade_type == "Reported Reduction"
        assert by_type["institutional_accumulation"].payload["report_period"] == "Q2 2026"
        assert by_type["institutional_accumulation"].ts.date() == date(2026, 6, 30)
        assert not any("buy" in (item.trade_type or "").lower() or "sell" in (item.trade_type or "").lower() for item in page.items)
        assert not any((item.trade_type or "").lower() == "13f filing" for item in page.items)
        assert not any((item.member_name or "") == "Multiple institutions" for item in page.items)
        assert not any((item.member_name or "").startswith("CIK ") for item in page.items)
    finally:
        db.close()


def test_institutional_feed_mode_resolves_holder_name_from_cik_meta(monkeypatch):
    db = _db()
    try:
        monkeypatch.setattr(events_module, "_can_view_institutional_events", lambda *_args, **_kwargs: True)
        monkeypatch.setattr(events_module, "get_cik_meta", lambda *_args, **_kwargs: {"0001067983": "Berkshire Hathaway Inc."})
        db.add(_institutional_event(207, "major_holder_exit", holder_name=None, cik="0001067983"))
        db.commit()

        page = list_events(db=db, tape="institutional", limit=10, enrich_prices=False)

        assert len(page.items) == 1
        assert page.items[0].member_name == "Berkshire Hathaway Inc."
        assert page.items[0].payload["holder_name"] == "Berkshire Hathaway Inc."
        assert not page.items[0].member_name.startswith("CIK ")
    finally:
        db.close()


def test_institutional_feed_mode_filters_by_holder_name(monkeypatch):
    db = _db()
    try:
        monkeypatch.setattr(events_module, "_can_view_institutional_events", lambda *_args, **_kwargs: True)
        db.add_all(
            [
                InstitutionalHolder(cik="0001067983", holder_name="Berkshire Hathaway Inc."),
                _institutional_event(208, "institutional_accumulation", holder_name=None, cik="0001067983", normalized_symbol="AAPL"),
                _institutional_event(209, "institutional_accumulation", holder_name="Example Capital Management", cik="0009999999", normalized_symbol="MSFT"),
            ]
        )
        db.commit()

        page = list_events(db=db, tape="institutional", member="Berkshire", limit=10, enrich_prices=False)

        assert len(page.items) == 1
        assert page.items[0].member_name == "Berkshire Hathaway Inc."
        assert page.items[0].symbol == "AAPL"
    finally:
        db.close()


def test_institutional_feed_mode_returns_no_detail_for_unentitled_users():
    db = _db()
    try:
        db.add(_institutional_event(211, "institutional_accumulation", holder_name="Detailed Holder LLC", reported_value_usd=500_000_000.0))
        db.commit()

        page = list_events(db=db, tape="institutional", limit=10, enrich_prices=False)

        assert page.items == []
        assert page.limit == 10
    finally:
        db.close()


def test_all_mode_keeps_only_selective_institutional_feed_events(monkeypatch):
    db = _db()
    try:
        _stub_enrichment(monkeypatch)
        monkeypatch.setattr(events_module, "_can_view_institutional_events", lambda *_args, **_kwargs: True)
        now = datetime(2026, 6, 30, tzinfo=timezone.utc)
        db.add_all(
            [
                _event(220, "congress_trade", ts=now, event_date=now, symbol="AAPL", member_name="Member", member_bioguide_id="M1"),
                _event(221, "institutional_accumulation", ts=now, event_date=now, symbol="AAPL", member_name="Holder"),
                _event(222, "smart_money_confirmation", ts=now, event_date=now, symbol="AAPL", member_name="Holder", amount_min=150_000_000, amount_max=150_000_000, impact_score=95),
                _event(224, "new_institutional_position", ts=now, event_date=now, symbol="MSFT", member_name="Holder", amount_min=25_000_000, amount_max=25_000_000, impact_score=100),
                _event(
                    223,
                    "government_contract",
                    ts=now,
                    event_date=now,
                    symbol="LMT",
                    member_name="Department of Defense",
                    payload={"event_subtype": "funding_action"},
                ),
                GovernmentContractAction(
                    parent_award_id="B-1",
                    dedupe_key="B-1-P1",
                    event_id=223,
                    symbol="LMT",
                    awarding_agency="Department of Defense",
                    recipient_name="Lockheed Martin",
                    action_date=now.date(),
                    obligated_amount=1_000_000,
                ),
            ]
        )
        db.commit()

        page = list_events(db=db, mode="all", limit=10, enrich_prices=False)

        event_types = {item.event_type for item in page.items}
        assert "congress_trade" in event_types
        assert "government_contract" in event_types
        assert "smart_money_confirmation" in event_types
        assert "institutional_accumulation" not in event_types
        assert "new_institutional_position" not in event_types
    finally:
        db.close()


def test_institutional_mode_can_include_broader_material_activity_than_all(monkeypatch):
    db = _db()
    try:
        monkeypatch.setattr(events_module, "_can_view_institutional_events", lambda *_args, **_kwargs: True)
        db.add(_institutional_event(231, "institutional_distribution", direction="bearish", materiality_score=70.0))
        db.commit()

        page = list_events(db=db, mode="institutional", limit=10, enrich_prices=False)

        assert [item.event_type for item in page.items] == ["institutional_distribution"]
        assert page.items[0].trade_type == "Reported Reduction"
    finally:
        db.close()


def test_government_contract_feed_mode_is_unchanged_by_institutional_mode(monkeypatch):
    db = _db()
    try:
        _stub_enrichment(monkeypatch)
        monkeypatch.setattr(events_module, "_can_view_institutional_events", lambda *_args, **_kwargs: True)
        now = datetime(2026, 6, 30, tzinfo=timezone.utc)
        db.add_all(
            [
                _event(240, "institutional_accumulation", ts=now, event_date=now, symbol="AAPL"),
                _event(
                    241,
                    "government_contract",
                    ts=now,
                    event_date=now,
                    symbol="RTX",
                    member_name="Department of Defense",
                    payload={"event_subtype": "funding_action"},
                ),
                GovernmentContractAction(
                    parent_award_id="C-1",
                    dedupe_key="C-1-P1",
                    event_id=241,
                    symbol="RTX",
                    awarding_agency="Department of Defense",
                    recipient_name="RTX Corporation",
                    action_date=now.date(),
                    obligated_amount=2_000_000,
                ),
            ]
        )
        db.commit()

        page = list_events(db=db, tape="government_contracts", limit=10, enrich_prices=False)

        assert [item.event_type for item in page.items] == ["government_contract"]
    finally:
        db.close()


def test_institutional_feed_copy_avoids_data_source_wording():
    repo_root = Path(__file__).resolve().parents[2]
    paths = [
        repo_root / "frontend" / "app" / "page.tsx",
        repo_root / "frontend" / "components" / "feed" / "FeedCard.tsx",
        repo_root / "frontend" / "components" / "feed" / "FeedFiltersServer.tsx",
        repo_root / "frontend" / "lib" / "feedModes.ts",
    ]
    institutional_lines = "\n".join(
        line
        for path in paths
        for line in path.read_text(encoding="utf-8").splitlines()
        if "Institutional" in line or "institutional" in line or "13F" in line
    ).lower()

    for forbidden in ("fmp", "provider", "vendor", "cache"):
        assert forbidden not in institutional_lines
