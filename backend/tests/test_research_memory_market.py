import json
from datetime import datetime, timedelta, timezone

import pytest

from app.models import ConfirmationScoreSnapshot, PriceCache, QuoteCache, ResearchThesis, ResearchThesisMarketBaseline
from app.services import research_memory as memory
from app.services.research_memory_market import capture_baseline, market_context, thesis_market
from test_research_memory import db, _seed_user_and_security


CREATED = datetime(2026, 9, 26, 18, tzinfo=timezone.utc)


def snapshot(db, security, when, score=60, method=1, calculation_type="live", recorded=None):
    row = ConfirmationScoreSnapshot(security_id=security.id, ticker_at_time=security.symbol,
        calculated_at=when, created_at=recorded or when, market_date=when.date(), score=score,
        direction="bullish", strength="moderate", input_hash=f"{when}-{score}-{method}",
        methodology_version_id=method, calculation_type=calculation_type,
        reference_price=100, reference_price_at=when)
    db.add(row)
    db.flush()
    return row


def setup(db):
    owner, _, security = _seed_user_and_security(db)
    thesis = ResearchThesis(id="baseline-test", user_id=owner.id, security_id=security.id,
        ticker_at_creation=security.symbol, title="Growth", summary="Growth", orientation="bullish",
        status="draft", source_type="custom", created_at=CREATED)
    db.add(thesis)
    db.flush()
    return owner, security, thesis


def test_baseline_immutable_through_edit_and_activation_and_returns_use_creation(db):
    owner, security, thesis = setup(db)
    db.add(QuoteCache(symbol=security.symbol, price=100, asof_ts=CREATED - timedelta(hours=1)))
    snapshot(db, security, CREATED - timedelta(hours=1))
    db.flush()
    saved = capture_baseline(db, thesis)
    original = saved.payload_json
    db.commit()
    db.get(QuoteCache, security.symbol).price = 110
    db.get(QuoteCache, security.symbol).asof_ts = CREATED + timedelta(hours=1)
    snapshot(db, security, CREATED + timedelta(hours=1), score=65)
    db.commit()
    memory.update_draft(db, user=owner, thesis_id=thesis.id, structure=memory.template_draft("revenue_growth", symbol=security.symbol))
    memory.activate(db, user=owner, thesis_id=thesis.id)
    assert db.get(ResearchThesisMarketBaseline, thesis.id).payload_json == original
    current = market_context(db, thesis, CREATED + timedelta(hours=2))
    result = thesis_market(db, thesis, current)
    assert result["price_change"] == 10
    assert result["price_change_percent"] == pytest.approx(10)
    assert result["score_change"] == 5
    assert capture_baseline(db, thesis, historical=True).payload_json == original


def test_historical_baseline_excludes_future_quotes_late_backfills_and_backtests(db):
    _, security, thesis = setup(db)
    snapshot(db, security, CREATED - timedelta(hours=3), score=42)
    snapshot(db, security, CREATED + timedelta(hours=1), score=99)
    snapshot(db, security, CREATED - timedelta(hours=1), score=95, recorded=CREATED + timedelta(hours=1))
    snapshot(db, security, CREATED - timedelta(hours=2), score=98, calculation_type="backtest")
    db.add(QuoteCache(symbol=security.symbol, price=500, asof_ts=CREATED + timedelta(hours=1)))
    db.flush()
    baseline = json.loads(capture_baseline(db, thesis, historical=True).payload_json)
    assert baseline["score"] == 42
    assert baseline["price"] == 100
    assert baseline["price_source"] == "historical_snapshot"
    assert baseline["kind"] == "historical"


def test_weekend_daily_change_compares_friday_with_thursday(db):
    _, security, thesis = setup(db)
    db.add_all([PriceCache(symbol=security.symbol, date="2026-09-24", close=50, raw_close=100),
                PriceCache(symbol=security.symbol, date="2026-09-25", close=52, raw_close=104),
                QuoteCache(symbol=security.symbol, price=104, asof_ts=CREATED - timedelta(hours=1))])
    db.flush()
    result = market_context(db, thesis, CREATED)
    assert result["day_change"] == 4
    assert result["day_change_percent"] == 4


def test_missing_baseline_stays_missing_and_never_uses_current_as_start(db):
    _, security, thesis = setup(db)
    capture_baseline(db, thesis)
    db.flush()
    db.add(QuoteCache(symbol=security.symbol, price=120, asof_ts=CREATED + timedelta(hours=1)))
    db.flush()
    result = thesis_market(db, thesis, market_context(db, thesis, CREATED + timedelta(hours=2)))
    assert result["baseline"]["price"] is None
    assert result["price_change"] is None
    assert result["price_change_unavailable_reason"] == "baseline_unavailable"


def test_methodology_changes_and_splits_are_not_reported_as_gains(db):
    _, security, thesis = setup(db)
    db.add(QuoteCache(symbol=security.symbol, price=100, asof_ts=CREATED - timedelta(days=1)))
    snapshot(db, security, CREATED - timedelta(hours=1))
    db.flush()
    capture_baseline(db, thesis)
    db.flush()
    db.add(PriceCache(symbol=security.symbol, date="2026-09-28", close=50, raw_close=50, split_coefficient=2))
    snapshot(db, security, CREATED + timedelta(days=3), score=75, method=2)
    quote = db.get(QuoteCache, security.symbol)
    quote.price, quote.asof_ts = 50, CREATED + timedelta(days=3)
    db.flush()
    result = thesis_market(db, thesis, market_context(db, thesis, CREATED + timedelta(days=3, hours=1)))
    assert result["price_change"] is None
    assert result["price_change_unavailable_reason"] == "split_review_required"
    assert result["score_change"] is None
    assert result["score_methodology_changed"] is True


def test_same_day_close_is_not_used_before_market_close(db):
    _, security, thesis = setup(db)
    thesis.created_at = datetime(2026, 9, 25, 14, tzinfo=timezone.utc)
    db.add_all([PriceCache(symbol=security.symbol, date="2026-09-24", close=100),
                PriceCache(symbol=security.symbol, date="2026-09-25", close=150)])
    db.flush()
    baseline = json.loads(capture_baseline(db, thesis, historical=True).payload_json)
    assert baseline["price"] == 100


def test_new_drafts_capture_baselines_without_waiting_for_activation(db):
    owner, _, security = _seed_user_and_security(db)
    now = datetime.now(timezone.utc)
    db.add(QuoteCache(symbol=security.symbol, price=85, asof_ts=now - timedelta(minutes=1)))
    snapshot(db, security, now - timedelta(minutes=1), score=0)
    db.commit()
    result = memory.create_draft(db, user=owner, security=security, structure=memory.template_draft("revenue_growth", symbol=security.symbol))
    assert result["status"] == "draft"
    assert result["market"]["baseline"]["kind"] == "captured"
    assert result["market"]["baseline"]["price"] == 85
    assert result["market"]["baseline"]["score"] == 0


def test_split_day_daily_change_adjusts_previous_raw_close(db):
    _, security, thesis = setup(db)
    db.add_all([PriceCache(symbol=security.symbol, date="2026-09-24", close=100, raw_close=100),
                PriceCache(symbol=security.symbol, date="2026-09-25", close=51, raw_close=51, split_coefficient=2)])
    db.flush()
    current = market_context(db, thesis, CREATED)
    assert current["day_change"] == 1
    assert current["day_change_percent"] == 2


def test_baseline_table_is_added_by_idempotent_schema_setup(db):
    from app.db import ensure_research_memory_schema
    from sqlalchemy import inspect
    db.rollback()
    ensure_research_memory_schema(db.get_bind())
    ensure_research_memory_schema(db.get_bind())
    assert "research_thesis_market_baselines" in inspect(db.get_bind()).get_table_names()
