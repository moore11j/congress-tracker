from datetime import datetime, timedelta, timezone
import json

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from app.db import Base
from app.models import Event, QuoteCache, Security, TradeOutcome
from app.routers import events as events_router


def test_insider_trades_does_not_trigger_sync_outcome_backfill():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)

    ts = datetime.now(timezone.utc)
    trade_date = ts.date().isoformat()
    with Session(engine) as db:
        db.add(
            Event(
                id=1,
                event_type="insider_trade",
                ts=ts,
                event_date=ts,
                symbol="JPM",
                source="fmp",
                trade_type="sale",
                payload_json=json.dumps(
                    {
                        "symbol": "JPM",
                        "transaction_date": trade_date,
                        "reporting_cik": "0000019617",
                        "insider_name": "Test Insider",
                    }
                ),
                amount_min=1000,
                amount_max=5000,
            )
        )
        db.commit()

        payload = events_router.insider_trades(
            reporting_cik="0000019617",
            db=db,
            lookback_days=90,
            limit=50,
        )

    assert payload["reporting_cik"] == "0000019617"
    assert payload["total"] == 1
    assert payload["page"] == 0
    assert payload["limit"] == 50
    assert payload["has_next"] is False
    assert len(payload["items"]) == 1
    assert payload["items"][0]["symbol"] == "JPM"


def test_insider_trades_returns_public_rows_with_pagination_metadata():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)

    ts = datetime.now(timezone.utc)
    with Session(engine) as db:
        db.add_all(
            [
                Event(
                    id=idx,
                    event_type="insider_trade",
                    ts=event_ts,
                    event_date=event_ts,
                    symbol="PLTR",
                    source="fmp",
                    trade_type="sale",
                    payload_json=json.dumps(
                        {
                            "symbol": "PLTR",
                            "transaction_date": event_ts.date().isoformat(),
                            "reporting_cik": "0001824159",
                            "insider_name": "Sankar Shyam",
                            "company_name": "Palantir Technologies Inc",
                        }
                    ),
                    amount_min=1000 * idx,
                    amount_max=5000 * idx,
                )
                for idx in range(1, 4)
                for event_ts in [ts - timedelta(days=idx)]
            ]
        )
        db.commit()

        first_page = events_router.insider_trades(
            reporting_cik="0001824159",
            db=db,
            lookback_days=90,
            limit=2,
            page=0,
        )
        second_page = events_router.insider_trades(
            reporting_cik="0001824159",
            db=db,
            lookback_days=90,
            limit=2,
            page=1,
        )

    assert first_page["total"] == 3
    assert first_page["page"] == 0
    assert first_page["limit"] == 2
    assert first_page["has_next"] is True
    assert [item["event_id"] for item in first_page["items"]] == [1, 2]
    assert first_page["items"][0]["symbol"] == "PLTR"
    assert first_page["items"][0]["company_name"] == "Palantir Technologies Inc"

    assert second_page["total"] == 3
    assert second_page["page"] == 1
    assert second_page["limit"] == 2
    assert second_page["has_next"] is False
    assert [item["event_id"] for item in second_page["items"]] == [3]


def test_insider_trades_empty_insider_returns_zero_total():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)

    with Session(engine) as db:
        payload = events_router.insider_trades(
            reporting_cik="0001824159",
            db=db,
            lookback_days=90,
            limit=20,
            page=0,
        )

    assert payload["reporting_cik"] == "0001824159"
    assert payload["total"] == 0
    assert payload["page"] == 0
    assert payload["limit"] == 20
    assert payload["has_next"] is False
    assert payload["items"] == []


def test_events_can_skip_price_enrichment_for_read_only_ticker_pages(monkeypatch):
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)

    ts = datetime.now(timezone.utc)
    with Session(engine) as db:
        db.add(
            Event(
                id=1,
                event_type="congress_trade",
                ts=ts,
                event_date=ts,
                symbol="JPM",
                source="congress",
                trade_type="purchase",
                member_name="Test Member",
                member_bioguide_id="T000001",
                payload_json=json.dumps(
                    {
                        "symbol": "JPM",
                        "trade_date": "2026-03-18",
                    }
                ),
                amount_min=1000,
                amount_max=5000,
            )
        )
        db.commit()

        def _boom(*args, **kwargs):
            raise AssertionError("read-only ticker event fetch should not enrich prices")

        monkeypatch.setattr(events_router, "get_eod_close", _boom)
        monkeypatch.setattr(events_router, "get_current_prices_meta_db", _boom)

        payload = events_router.list_events(
            db=db,
            symbol="JPM",
            recent_days=365,
            limit=100,
            enrich_prices=False,
        )

    assert len(payload.items) == 1
    assert payload.items[0].symbol == "JPM"
    assert payload.items[0].estimated_price is None
    assert payload.items[0].current_price is None


def test_insider_alpha_summary_skips_sync_backfill_for_high_volume_insider():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)

    ts = datetime(2026, 3, 20, tzinfo=timezone.utc)
    with Session(engine) as db:
        bulk = [
            Event(
                id=idx,
                event_type="insider_trade",
                ts=ts,
                event_date=ts,
                symbol="JPM",
                source="fmp",
                trade_type="sale",
                payload_json=json.dumps(
                    {
                        "symbol": "JPM",
                        "transaction_date": "2026-03-18",
                        "reporting_cik": "0000019617",
                        "insider_name": "Test Insider",
                    }
                ),
                amount_min=1000,
                amount_max=5000,
            )
            for idx in range(1, 206)
        ]
        db.add_all(bulk)
        db.commit()

        payload = events_router.insider_alpha_summary(
            reporting_cik="0000019617",
            db=db,
            lookback_days=90,
        )

    assert payload["reporting_cik"] == "0000019617"
    assert payload["trades_analyzed"] == 0


def test_insider_alpha_summary_does_not_sync_backfill_for_small_missing_set():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)

    ts = datetime(2026, 3, 20, tzinfo=timezone.utc)
    with Session(engine) as db:
        db.add(
            Event(
                id=1,
                event_type="insider_trade",
                ts=ts,
                event_date=ts,
                symbol="JPM",
                source="fmp",
                trade_type="sale",
                payload_json=json.dumps(
                    {
                        "symbol": "JPM",
                        "transaction_date": "2026-03-18",
                        "reporting_cik": "0000019617",
                        "insider_name": "Test Insider",
                    }
                ),
                amount_min=1000,
                amount_max=5000,
            )
        )
        db.commit()

        payload = events_router.insider_alpha_summary(
            reporting_cik="0000019617",
            db=db,
            lookback_days=90,
        )

    assert payload["reporting_cik"] == "0000019617"
    assert payload["trades_analyzed"] == 0


def test_insider_alpha_summary_includes_recent_activity_with_cached_quote_without_persisting():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)

    ts = datetime.now(timezone.utc)
    trade_date = ts.date().isoformat()
    with Session(engine) as db:
        db.add(
            Event(
                id=11,
                event_type="insider_trade",
                ts=ts,
                event_date=ts,
                symbol="JPM",
                source="fmp",
                trade_type="sale",
                payload_json=json.dumps(
                    {
                        "symbol": "JPM",
                        "transaction_date": trade_date,
                        "reporting_cik": "0000019617",
                        "insider_name": "Test Insider",
                        "price": 100.0,
                        "is_market_trade": True,
                    }
                ),
                amount_min=1000,
                amount_max=5000,
            )
        )
        db.add(QuoteCache(symbol="JPM", price=90.0, asof_ts=ts.replace(tzinfo=None)))
        db.commit()

        payload = events_router.insider_alpha_summary(
            reporting_cik="0000019617",
            db=db,
            lookback_days=90,
        )

        assert db.query(TradeOutcome).count() == 0

    assert payload["reporting_cik"] == "0000019617"
    assert payload["trades_analyzed"] == 1
    assert round(payload["avg_return_pct"], 6) == 10.0
    assert payload["best_trades"][0]["event_id"] == 11
    assert payload["worst_trades"] == []
    assert round(payload["member_series"][-1]["cumulative_return_pct"], 6) == 10.0


def test_insider_top_tickers_uses_analytics_cache_for_repeated_requests(monkeypatch):
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)
    ts = datetime.now(timezone.utc)
    calls = 0

    with Session(engine) as db:
        db.add(
            Event(
                id=901,
                event_type="insider_trade",
                ts=ts,
                event_date=ts,
                symbol="JPM",
                source="fmp",
                trade_type="sale",
                payload_json=json.dumps(
                    {
                        "symbol": "JPM",
                        "transaction_date": ts.date().isoformat(),
                        "reporting_cik": "0000099999",
                        "insider_name": "Cache Test",
                    }
                ),
                amount_min=1000,
                amount_max=5000,
            )
        )
        db.commit()

        original = events_router._load_insider_events_for_cik

        def counted_loader(*args, **kwargs):
            nonlocal calls
            calls += 1
            return original(*args, **kwargs)

        monkeypatch.setattr(events_router, "_load_insider_events_for_cik", counted_loader)

        first = events_router.insider_top_tickers("0000099999", db=db, lookback_days=90, limit=10)
        second = events_router.insider_top_tickers("0000099999", db=db, lookback_days=90, limit=10)

    assert calls == 1
    assert first == second
    assert first["items"][0]["symbol"] == "JPM"


def test_insider_trades_uses_persisted_payload_detail_fallbacks():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)

    ts = datetime.now(timezone.utc)
    trade_date = ts.date().isoformat()
    with Session(engine) as db:
        db.add(
            Event(
                id=1,
                event_type="insider_trade",
                ts=ts,
                event_date=ts,
                symbol="JPM",
                source="fmp",
                trade_type="purchase",
                payload_json=json.dumps(
                    {
                        "symbol": "JPM",
                        "security_name": "ASE Technology Holding Co., Ltd.",
                        "transaction_date": trade_date,
                        "reporting_cik": "0000019617",
                        "insider_name": "Jeffrey Chen",
                        "price": 12.34,
                        "trade_value": 123400,
                        "pnl_pct": 4.2,
                        "smart_score": 72,
                        "smart_band": "strong",
                    }
                ),
                amount_min=100000,
                amount_max=250000,
            )
        )
        db.commit()

        payload = events_router.insider_trades(
            reporting_cik="0000019617",
            db=db,
            lookback_days=90,
            limit=50,
        )

    row = payload["items"][0]
    assert row["symbol"] == "JPM"
    assert row["company_name"] == "ASE Technology Holding Co., Ltd."
    assert row["trade_date"] == trade_date
    assert row["price"] == 12.34
    assert row["trade_value"] == 123400
    assert row["pnl_pct"] == 4.2
    assert row["pnl_source"] == "persisted_payload"
    assert row["smart_score"] == 72
    assert row["smart_band"] == "strong"


def test_insider_trades_uses_security_name_and_quote_cache_without_outcome():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)

    ts = datetime.now(timezone.utc)
    trade_date = ts.date().isoformat()
    with Session(engine) as db:
        db.add(
            Event(
                id=1,
                event_type="insider_trade",
                ts=ts,
                event_date=ts,
                symbol="ASX",
                source="fmp",
                trade_type="sale",
                payload_json=json.dumps(
                    {
                        "symbol": "ASX",
                        "transaction_date": trade_date,
                        "reporting_cik": "0002111679",
                        "insider_name": "Chen Jeffrey",
                        "price": 387.0,
                        "raw": {
                            "securityName": "Ordinary Shares",
                            "transactionType": "S-Sale",
                            "reportingCik": "0002111679",
                        },
                    }
                ),
                amount_min=3483000,
                amount_max=3483000,
            )
        )
        db.add(Security(symbol="ASX", name="Ase Technology Holdings Co", asset_class="Equity", sector=None))
        db.add(QuoteCache(symbol="ASX", price=24.84, asof_ts=ts.replace(tzinfo=None)))
        db.commit()

        payload = events_router.insider_trades(
            reporting_cik="0002111679",
            db=db,
            lookback_days=30,
            limit=50,
        )

    row = payload["items"][0]
    assert row["symbol"] == "ASX"
    assert row["company_name"] == "Ase Technology Holdings Co"
    assert row["security_name"] == "Ordinary Shares"
    assert round(row["pnl_pct"], 6) == round(((row["price"] - 24.84) / row["price"]) * 100, 6)
    assert row["pnl_source"] == "normalized_filing"
