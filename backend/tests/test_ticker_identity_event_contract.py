import json
from datetime import datetime, timedelta, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from app.db import Base
from app.main import _build_ticker_chart_bundle, _build_ticker_profile, _event_security_fields_for_symbol
from app.models import Event, Security
from app.routers.events import list_ticker_events
from app.services.ticker_identity import resolve_ticker_identity


def _engine():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)
    return engine


def _insider_event(
    *,
    event_id: int,
    symbol: str = "INFQ",
    trade_type: str | None,
    days_ago: int = 1,
    payload: dict | None = None,
) -> Event:
    ts = datetime.now(timezone.utc) - timedelta(days=days_ago)
    return Event(
        id=event_id,
        event_type="insider_trade",
        ts=ts,
        event_date=ts,
        symbol=symbol,
        source="fmp",
        trade_type=trade_type,
        amount_min=10_000,
        amount_max=25_000,
        payload_json=json.dumps(
            payload
            or {
                "symbol": symbol,
                "transaction_date": ts.date().isoformat(),
                "insider_name": "Example Insider",
                "raw": {
                    "issuerName": "Infleqtion Inc",
                    "securityName": "Stock Option (Right to Buy)",
                    "transactionType": trade_type,
                },
            }
        ),
    )


def test_resolve_ticker_identity_rejects_filing_instrument_titles():
    assert (
        resolve_ticker_identity(
            "INFQ",
            canonical_profile_name="Stock Option (Right to Buy)",
            issuer_company_names=["Infleqtion Inc"],
            metadata_name="INFQ",
        )
        == "Infleqtion Inc"
    )
    assert resolve_ticker_identity("INFQ", canonical_profile_name="Stock Option (Right to Buy)") == "INFQ"


def test_ticker_profile_uses_issuer_name_when_security_row_is_instrument_label(monkeypatch):
    engine = _engine()
    monkeypatch.setattr(
        "app.main._ticker_confirmation_score_bundle",
        lambda db, sym, options_flow_summary=None: {"ticker": sym, "lookback_days": 30, "score": 0, "sources": {}},
    )
    monkeypatch.setattr("app.main._company_profile_snapshot_from_fmp", lambda symbol: {})

    with Session(engine) as db:
        db.add(Security(symbol="INFQ", name="Stock Option (Right to Buy)", asset_class="stock", sector=None))
        db.add(_insider_event(event_id=1, trade_type="a-award"))
        db.commit()

        profile = _build_ticker_profile("INFQ", db)

    assert profile["ticker"]["name"] == "Infleqtion Inc"


def test_ticker_profile_falls_back_to_symbol_when_only_instrument_labels_exist(monkeypatch):
    engine = _engine()
    monkeypatch.setattr(
        "app.main._ticker_confirmation_score_bundle",
        lambda db, sym, options_flow_summary=None: {"ticker": sym, "lookback_days": 30, "score": 0, "sources": {}},
    )
    monkeypatch.setattr("app.main._company_profile_snapshot_from_fmp", lambda symbol: {})

    with Session(engine) as db:
        db.add(Security(symbol="INFQ", name="Stock Option (Right to Buy)", asset_class="stock", sector=None))
        db.add(
            _insider_event(
                event_id=1,
                trade_type="a-award",
                payload={
                    "symbol": "INFQ",
                    "company_name": "Stock Option (Right to Buy)",
                    "raw": {"securityName": "Stock Option (Right to Buy)"},
                },
            )
        )
        db.commit()

        profile = _build_ticker_profile("INFQ", db)

    assert profile["ticker"]["name"] == "INFQ"


def test_ticker_profile_includes_company_metadata_from_profile_snapshot(monkeypatch):
    engine = _engine()
    monkeypatch.setattr(
        "app.main._ticker_confirmation_score_bundle",
        lambda db, sym, options_flow_summary=None: {"ticker": sym, "lookback_days": 30, "score": 0, "sources": {}},
    )
    monkeypatch.setattr(
        "app.main._company_profile_snapshot_from_fmp",
        lambda symbol: {
            "sector": "Technology",
            "industry": "Semiconductors",
            "country": "US",
            "exchangeShortName": "NASDAQ",
        },
    )

    with Session(engine) as db:
        db.add(Security(symbol="NVDA", name="NVIDIA Corporation", asset_class="stock", sector=None))
        db.commit()

        profile = _build_ticker_profile("NVDA", db)

    assert profile["ticker"]["sector"] == "Technology"
    assert profile["ticker"]["industry"] == "Semiconductors"
    assert profile["ticker"]["country"] == "US"
    assert profile["ticker"]["exchange"] == "NASDAQ"


def test_watchlist_security_resolution_uses_safe_issuer_not_instrument_label():
    engine = _engine()

    with Session(engine) as db:
        db.add(_insider_event(event_id=1, trade_type="a-award"))
        db.commit()

        name, _ = _event_security_fields_for_symbol(db, "INFQ")

    assert name == "Infleqtion Inc"


def test_ticker_chart_and_ticker_events_share_visible_insider_contract(monkeypatch):
    engine = _engine()
    today = datetime.now(timezone.utc).date()
    monkeypatch.setattr(
        "app.main.get_daily_close_series_with_fallback",
        lambda db, symbol, start_key, end_key: {today.isoformat(): 10.0},
    )
    monkeypatch.setattr("app.main._quote_snapshot_from_fmp", lambda symbol: {})
    monkeypatch.setattr("app.main._ratios_ttm_from_fmp", lambda symbol: {})
    monkeypatch.setattr("app.main._company_profile_snapshot_from_fmp", lambda symbol: {})
    monkeypatch.setattr("app.main.get_daily_volume_series_from_provider", lambda symbol, start_key, end_key: {})
    monkeypatch.setattr("app.main.get_current_prices_db", lambda db, symbols: {})
    monkeypatch.setattr("app.main._query_unified_signals", lambda **kwargs: [])

    with Session(engine) as db:
        db.add(_insider_event(event_id=1, trade_type="a-award"))
        db.add(_insider_event(event_id=2, trade_type="purchase"))
        db.commit()

        ticker_events = list_ticker_events(symbol="INFQ", db=db, limit=10).items
        bundle = _build_ticker_chart_bundle("INFQ", 30, db)

    assert [event.id for event in ticker_events] == [2]
    assert [marker["event_id"] for marker in bundle["markers"]] == [2]
    assert bundle["markers"][0]["side"] == "buy"


def test_ticker_chart_has_no_insider_markers_when_visible_insider_activity_is_zero(monkeypatch):
    engine = _engine()
    today = datetime.now(timezone.utc).date()
    monkeypatch.setattr(
        "app.main.get_daily_close_series_with_fallback",
        lambda db, symbol, start_key, end_key: {today.isoformat(): 10.0},
    )
    monkeypatch.setattr("app.main._quote_snapshot_from_fmp", lambda symbol: {})
    monkeypatch.setattr("app.main._ratios_ttm_from_fmp", lambda symbol: {})
    monkeypatch.setattr("app.main._company_profile_snapshot_from_fmp", lambda symbol: {})
    monkeypatch.setattr("app.main.get_daily_volume_series_from_provider", lambda symbol, start_key, end_key: {})
    monkeypatch.setattr("app.main.get_current_prices_db", lambda db, symbols: {})
    monkeypatch.setattr("app.main._query_unified_signals", lambda **kwargs: [])

    with Session(engine) as db:
        db.add(_insider_event(event_id=1, trade_type="a-award"))
        db.commit()

        ticker_events = list_ticker_events(symbol="INFQ", db=db, limit=10).items
        bundle = _build_ticker_chart_bundle("INFQ", 30, db)

    assert ticker_events == []
    assert bundle["markers"] == []


def test_ticker_chart_marker_window_uses_same_canonical_event_date_as_activity(monkeypatch):
    engine = _engine()
    today = datetime.now(timezone.utc).date()
    stale_payload_day = (today - timedelta(days=400)).isoformat()
    monkeypatch.setattr(
        "app.main.get_daily_close_series_with_fallback",
        lambda db, symbol, start_key, end_key: {today.isoformat(): 10.0},
    )
    monkeypatch.setattr("app.main._quote_snapshot_from_fmp", lambda symbol: {})
    monkeypatch.setattr("app.main._ratios_ttm_from_fmp", lambda symbol: {})
    monkeypatch.setattr("app.main._company_profile_snapshot_from_fmp", lambda symbol: {})
    monkeypatch.setattr("app.main.get_daily_volume_series_from_provider", lambda symbol, start_key, end_key: {})
    monkeypatch.setattr("app.main.get_current_prices_db", lambda db, symbols: {})
    monkeypatch.setattr("app.main._query_unified_signals", lambda **kwargs: [])

    with Session(engine) as db:
        db.add(
            _insider_event(
                event_id=1,
                trade_type="purchase",
                days_ago=0,
                payload={
                    "symbol": "INFQ",
                    "transaction_date": stale_payload_day,
                    "insider_name": "Example Insider",
                    "raw": {
                        "issuerName": "Infleqtion Inc",
                        "transactionDate": stale_payload_day,
                        "transactionType": "purchase",
                    },
                },
            )
        )
        db.commit()

        ticker_events = list_ticker_events(symbol="INFQ", db=db, limit=10).items
        bundle = _build_ticker_chart_bundle("INFQ", 30, db)

    assert [event.id for event in ticker_events] == [1]
    assert [marker["event_id"] for marker in bundle["markers"]] == [1]
    assert bundle["markers"][0]["date"] == today.isoformat()
