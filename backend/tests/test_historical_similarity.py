import json
from datetime import datetime, timedelta, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from app.db import Base, ensure_outcome_ledger_schema
from app.models import ConfirmationScoreSnapshot, PriceCache, Security
from app.services.historical_similarity import build_similar_historical_setups, public_similar_historical_setups
from app.services.outcome_ledger import current_confirmation_methodology


def _source(direction: str, contribution: int) -> dict:
    return {
        "present": True,
        "direction": direction,
        "strength": 72,
        "quality": 72,
        "score_contribution": contribution,
        "label": "test",
    }


def _sources() -> dict:
    return {"fundamentals": _source("bullish", 14), "price_volume": _source("bullish", 11)}


def test_similar_setups_uses_only_live_point_in_time_source_snapshots():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)
    ensure_outcome_ledger_schema(engine)
    now = datetime.now(timezone.utc)
    market_day = now.date() - timedelta(days=120)
    with Session(engine) as db:
        methodology = current_confirmation_methodology(db)
        db.add_all(
            [
                PriceCache(symbol="SPY", date=market_day.isoformat(), close=100.0),
                PriceCache(symbol="SPY", date=(market_day + timedelta(days=30)).isoformat(), close=101.0),
                PriceCache(symbol="SPY", date=(market_day + timedelta(days=90)).isoformat(), close=102.0),
            ]
        )
        for index in range(6):
            symbol = f"T{index}"
            security = Security(symbol=symbol, name=symbol, asset_class="equity", sector="Technology")
            db.add(security)
            db.flush()
            db.add_all(
                [
                    PriceCache(symbol=symbol, date=(market_day + timedelta(days=30)).isoformat(), close=106.0),
                    PriceCache(symbol=symbol, date=(market_day + timedelta(days=90)).isoformat(), close=112.0),
                ]
            )
            db.add(
                ConfirmationScoreSnapshot(
                    security_id=security.id,
                    ticker_at_time=symbol,
                    calculated_at=datetime.combine(market_day, datetime.min.time(), tzinfo=timezone.utc),
                    market_date=market_day,
                    score=72,
                    direction="bullish",
                    strength="strong",
                    reference_price=100.0,
                    reference_price_at=datetime.combine(market_day, datetime.min.time(), tzinfo=timezone.utc),
                    active_source_count=2,
                    active_sources_json=json.dumps(["fundamentals", "price_volume"]),
                    source_contributions_json=json.dumps(_sources()),
                    source_freshness_json=json.dumps({"fundamentals": {"freshness_days": 4}, "price_volume": {"freshness_days": 2}}),
                    input_hash=f"test-{index}",
                    methodology_version_id=methodology.id,
                    calculation_type="live" if index < 5 else "historical_reconstruction",
                )
            )
        db.commit()

        current = {
            "score": 72,
            "direction": "bullish",
            "sources": _sources(),
        }
        payload = build_similar_historical_setups(db, symbol="T0", confirmation_bundle=current, sector="Technology")

    assert payload["cohort_type"] == "live_prospective_only"
    assert payload["match_count"] == 5
    assert payload["horizons"]["30D"]["status"] == "limited"
    assert payload["horizons"]["30D"]["sample_size"] == 5
    assert payload["horizons"]["90D"]["sample_size"] == 5
    guest = public_similar_historical_setups(payload, include_details=False)
    assert guest["top_matches"] == []
    assert guest["access"]["locked"] is True
