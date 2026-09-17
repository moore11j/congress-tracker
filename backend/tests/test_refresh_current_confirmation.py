import json
from datetime import datetime, timezone
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from app.models import MarketPressureSnapshot, TickerContextBundleCache
from app.jobs import refresh_current_confirmation as job
from app.services.ticker_decision_layer import _confirmation_label


def test_directional_lean_labels():
    assert _confirmation_label('inactive', 'bullish', 16) == 'Weak bullish lean'
    assert _confirmation_label('inactive', 'bearish', 16) == 'Weak bearish lean'
    assert _confirmation_label('inactive', 'neutral', 0) == 'Inactive'
    assert _confirmation_label('inactive', 'mixed', 0) == 'Conflicted confirmation'
    assert _confirmation_label('strong', 'bullish', 67) == 'Strong Bullish'


def test_current_refresh_preserves_history_and_price_freshness(monkeypatch):
    from app.main import _TICKER_CONTEXT_BUNDLE_VERSION
    engine = create_engine('sqlite:///:memory:')
    for model in (MarketPressureSnapshot, TickerContextBundleCache):
        model.__table__.create(engine)
    before = datetime(2026, 9, 15, 15, tzinfo=timezone.utc)
    history = [{'date': '2026-09-14', 'score': 100}]
    bundle = {'score': 16, 'band': 'inactive', 'direction': 'bullish', 'sources': {}, 'scoring_version': job.CONFIRMATION_SCORING_VERSION}
    monkeypatch.setattr(job, 'build_ticker_confirmation_context', lambda db, symbols: {'bundles': {s: bundle for s in symbols}})
    monkeypatch.setattr(job, 'refresh_top_stocks_leaderboard', lambda db: {'returned': 10, 'generated_at': before.isoformat()})
    with Session(engine) as db:
        cache = TickerContextBundleCache(cache_key=f'ticker-context-bundle:v{_TICKER_CONTEXT_BUNDLE_VERSION}:BA:30:all:3:canonical', symbol='BA', user_segment='canonical', payload_json=json.dumps({'generated_at': before.isoformat(), 'confirmation_score_bundle': {'score':100, 'history':history}, 'source_cards':{'price_volume':{'price':209.65}}}), generated_at=before, stale_after=before, expires_at=before)
        tile = MarketPressureSnapshot(universe='sp500', period='1d', symbol='BA', price=209.65, price_change_pct=-0.3, price_as_of=before, generated_at=before, tile_json=json.dumps({'priceStartAt':before.isoformat(), 'priceEndAt':before.isoformat()}))
        db.add_all([cache,tile]); db.commit()
        original_times = (cache.generated_at, cache.stale_after, cache.expires_at, tile.price_as_of, tile.generated_at)
        result = job.refresh_current_confirmation(db)
        payload = json.loads(cache.payload_json)
        assert result['ticker_caches'] == result['market_tiles'] == 1
        assert payload['confirmation_score_bundle']['score'] == 16
        assert payload['confirmation_score_bundle']['history'] == history
        assert payload['decision_layer']['confirmation']['label'] == 'Weak bullish lean'
        assert payload['source_cards']['price_volume']['price'] == tile.price == 209.65
        assert original_times == (cache.generated_at, cache.stale_after, cache.expires_at, tile.price_as_of, tile.generated_at)
