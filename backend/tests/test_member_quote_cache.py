from datetime import date, datetime, timedelta, timezone
import json

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from app.db import Base
from app.models import Event, Member, QuoteCache, PriceCache, TradeOutcome
from app import main
from app.services import quote_lookup


@pytest.mark.parametrize("mode", ["fresh", "stale", "sanity", "missing"])
def test_member_quotes_preserve_trades_and_reuse_validated_cache(monkeypatch, mode):
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    db = sessionmaker(bind=engine)()
    now = datetime.now(timezone.utc)
    quote_lookup._QUOTE_CACHE.clear()
    quote_lookup._QUOTE_META_CACHE.clear()
    quote_lookup._MISS_CACHE.clear()
    quote_lookup._QUOTE_CALL_TIMESTAMPS.clear()
    quote_lookup._quotes_disabled_until = None
    monkeypatch.setenv("FMP_PERSIST_USAGE_EVENTS", "0")
    monkeypatch.setenv("FMP_API_KEY", "test-only")
    monkeypatch.setattr(main, "_resolve_member_analytics_aliases", lambda *a: (None, ["TEST"]))
    monkeypatch.setattr(main, "_congress_baseline_map_for_symbols", lambda *a: {})
    monkeypatch.setattr(main, "get_confirmation_metrics_for_symbols", lambda *a: {})
    monkeypatch.setattr(main, "_member_trade_sector_map", lambda *a: {"AAPL": "Technology"})
    queued = []
    monkeypatch.setattr(quote_lookup, "_enqueue_quote_refreshes", lambda symbols, **kw: queued.extend(symbols))
    monkeypatch.setattr(quote_lookup, "ensure_fmp_live_allowed", lambda **kw: None)
    monkeypatch.setattr(quote_lookup, "fmp_endpoint_requests_for_domain", lambda *a, **kw: [])
    calls = []

    class Response:
        status_code = 200
        def json(self):
            return [{"symbol": "AAPL", "price": 202.0, "date": now.date().isoformat()}]
        def raise_for_status(self):
            pass

    def fetch(*args, **kwargs):
        calls.append(args[0])
        assert mode == "missing", "A populated profile must not force a new provider request"
        return Response()

    monkeypatch.setattr(quote_lookup.requests, "get", fetch)
    try:
        member = Member(bioguide_id="TEST", first_name="Test", last_name="Member", chamber="house")
        db.add(member); db.flush(); member_id = member.id
        if mode != "missing":
            db.add(QuoteCache(symbol="AAPL", price=1000.0 if mode == "sanity" else 200.0,
                asof_ts=now - timedelta(days=2) if mode == "stale" else now))
        db.add(PriceCache(symbol="AAPL", date=now.date().isoformat(), close=199.0))
        for index in range(12):
            event = Event(event_type="congress_trade", ts=now - timedelta(minutes=index),
                event_date=now - timedelta(minutes=index), source="official_house", symbol="AAPL",
                member_bioguide_id="TEST", member_name="Test Member", transaction_type="purchase",
                amount_min=1001, amount_max=15000, payload_json=json.dumps({"company_name":"Apple", "trade_date":"2026-10-01", "report_date":"2026-10-09"}))
            db.add(event); db.flush()
            db.add(TradeOutcome(event_id=event.id, member_id="TEST", member_name="Test Member", symbol="AAPL",
                trade_type="purchase", source="congress", trade_date=date(2026,10,1), entry_price=100,
                current_price=180, benchmark_symbol="SPY", return_pct=80, alpha_pct=75, scoring_status="ok",
                methodology_version="congress_v1", computed_at=now))
        db.commit()
        expected_ids = list(db.scalars(select(Event.id).order_by(Event.event_date.desc(),Event.id.desc())))
        result = main._member_recent_trades(db, member_id, lookback_days=None, limit=200)
        assert [r["event_id"] for r in result] == expected_ids
        expected_price = 199.0 if mode == "sanity" else 202.0 if mode == "missing" else 200.0
        assert all(r["current_price"] == expected_price for r in result)
        assert all(r["trade_price"] == 100 and r["pnl_pct"] == expected_price-100 for r in result)
        assert all(r["security_name"] == "Apple" and r["sector"] == "Technology" for r in result)
        assert all(r["trade_date"] == "2026-10-01" and r["report_date"] == "2026-10-09" for r in result)
        assert all(r["current_price_as_of"] is not None for r in result)
        if mode == "stale":
            assert queued == ["AAPL"]
            assert all(r["current_price_is_stale"] is True for r in result)
        elif mode != "missing":
            assert not queued
        assert len(calls) == (1 if mode == "missing" else 0)
        if mode == "sanity":
            assert all(r["current_price_source"] == "eod_sanity_fallback" for r in result)
    finally:
        db.close(); engine.dispose()
        quote_lookup._QUOTE_CACHE.clear(); quote_lookup._QUOTE_META_CACHE.clear(); quote_lookup._MISS_CACHE.clear()
