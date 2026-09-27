import json
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from app.db import Base
from app.models import Event, PriceCache, TickerMeta
from app.services.public_activity import public_activity
from app.services.seo_snapshots import _ticker_batch_candidates, refresh_ticker_seo_snapshot


@pytest.fixture
def db():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        yield session


def event(id, **overrides):
    now = datetime.now(timezone.utc)
    values = dict(id=id, event_type="insider_trade", ts=now, event_date=now,
                  source="sec", symbol="NVDA", trade_type="sale", member_name="Public Officer",
                  payload_json=json.dumps({"filing_date": now.date().isoformat(), "role": "CFO",
                                           "private_analysis": "must not escape", "smart_score": 99,
                                           "filing_url": "https://www.sec.gov/Archives/example.xml"}))
    values.update(overrides)
    return Event(**values)


def test_preview_returns_public_rows_without_enrichment_and_excludes_other_activity(db):
    db.add_all([event(1), event(2, trade_type="grant"), event(3, symbol="AAPL"),
                event(4, event_type="institutional_holding"),
                event(5, ts=datetime.now(timezone.utc)-timedelta(days=366))])
    db.commit()
    result = public_activity(db, tape="insider", symbol="nvda")
    assert [row["id"] for row in result["items"]] == [1]
    row = result["items"][0]
    assert row["payload"]["role"] == "CFO"
    assert row["url"].startswith("https://www.sec.gov/")
    assert "private_analysis" not in row["payload"]
    assert "smart_score" not in row["payload"]
    assert "smart_score" not in row and "pnl_pct" not in row


def test_preview_bounded_pagination_and_empty_are_truthful(db):
    db.add_all([event(i) for i in range(1, 25)])
    db.commit()
    first = public_activity(db, tape="insider", limit=100)
    assert len(first["items"]) == 21 and first["has_more"]
    last = public_activity(db, tape="insider", offset=21)
    assert len(last["items"]) == 3 and not last["has_more"]
    assert public_activity(db, tape="congress")["status"] == "empty"
    with pytest.raises(ValueError):
        public_activity(db, tape="institutional")


def test_priced_named_ticker_without_events_or_index_membership_is_discoverable(db):
    db.add_all([TickerMeta(symbol="CART", company_name="Maplebear Inc."),
                PriceCache(symbol="CART", date="2026-09-25", close=40)])
    db.commit()
    assert _ticker_batch_candidates(db, 1, include_existing=False) == ["CART"]
    snapshot = refresh_ticker_seo_snapshot(db, "CART")
    assert snapshot["indexable"]
    assert _ticker_batch_candidates(db, 1, include_existing=False) == []


def test_existing_snapshots_do_not_starve_next_missing_candidate(db):
    for i in range(6):
        symbol = f"AAA{i}"
        db.add_all([TickerMeta(symbol=symbol, company_name=f"Example {i}"),
                    PriceCache(symbol=symbol, date="2026-09-25", close=10)])
        db.flush()
        refresh_ticker_seo_snapshot(db, symbol)
    db.add_all([TickerMeta(symbol="ZZZ", company_name="Last example"),
                PriceCache(symbol="ZZZ", date="2026-09-25", close=20)])
    db.commit()
    assert _ticker_batch_candidates(db, 1, include_existing=False) == ["ZZZ"]
