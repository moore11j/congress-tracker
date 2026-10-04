from datetime import datetime, timedelta, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from app.models import FundamentalsCache, PriceCache
from app.services import leaderboard_market_data as market
from app.services.top_stocks import _ranked_payload

NOW = datetime(2026, 10, 4, tzinfo=timezone.utc)


def session():
    engine = create_engine('sqlite:///:memory:')
    for model in (FundamentalsCache, PriceCache):
        model.__table__.create(engine)
    return Session(engine)


def candidate(symbol):
    return {'symbol': symbol, 'confirmation': {'score': 69, 'direction': 'bullish', 'source_count': 3}}


def test_repaired_caps_break_ties_and_preserve_statement_freshness(monkeypatch):
    monkeypatch.setattr(market, 'fetch_batch_market_capitalization', lambda **kwargs: [
        {'symbol': 'AEO', 'date': '2026-10-02', 'marketCap': 3e9},
        {'symbol': 'PLTR', 'date': '2026-10-02', 'marketCap': 400e9}])
    monkeypatch.setattr(market, 'fetch_company_profile', lambda **kwargs: (_ for _ in ()).throw(AssertionError('Already complete')))
    with session() as db:
        observed = NOW - timedelta(days=20)
        for symbol in ('AEO', 'PLTR'):
            db.add(FundamentalsCache(symbol=symbol, provider='fmp', fetched_at=observed, status='ok', roe=25))
            for offset in range(1, 22):
                db.add(PriceCache(symbol=symbol, date=(NOW - timedelta(days=offset)).date().isoformat(), close=100,
                                  volume=None, day_volume=offset * 1000))
            # Future bars must not leak into the average.
            db.add(PriceCache(symbol=symbol, date='2026-10-05', close=100, volume=1e12))
        db.commit()
        rows = [candidate('AEO'), candidate('PLTR')]
        market.enrich_leaderboard_market_data(db, rows, now=NOW)
        assert [r['symbol'] for r in _ranked_payload(rows, generated_at=NOW.isoformat())['items']] == ['PLTR', 'AEO']
        assert rows[0]['avg_volume'] == 10500
        assert rows[0]['avg_volume_observations'] == 20
        cached = db.query(FundamentalsCache).filter_by(symbol='PLTR').one()
        assert cached.market_cap == 400e9
        assert cached.fetched_at.date() == observed.date()
        assert cached.roe == 25


def test_rejects_stale_future_mismatched_and_invalid_market_caps(monkeypatch):
    monkeypatch.setattr(market, 'fetch_batch_market_capitalization', lambda **kwargs: [
        {'symbol': 'STALE', 'date': '2026-09-01', 'marketCap': 1e12},
        {'symbol': 'FUTURE', 'date': '2026-10-05', 'marketCap': 1e12},
        {'symbol': 'BAD', 'date': '2026-10-02', 'marketCap': float('nan')},
        {'symbol': 'OTHER', 'date': '2026-10-02', 'marketCap': 1e12}])
    monkeypatch.setattr(market, 'fetch_company_profile', lambda **kwargs: [{'symbol': 'WRONG', 'marketCap': 1e12, 'averageVolume': 1e8}])
    with session() as db:
        rows = [candidate(symbol) for symbol in ('STALE', 'FUTURE', 'BAD')]
        market.enrich_leaderboard_market_data(db, rows, now=NOW)
        assert all(r.get('market_cap') is None and r.get('avg_volume') is None for r in rows)


def test_sparse_or_old_volume_uses_provider_average_not_single_day(monkeypatch):
    monkeypatch.setattr(market, 'fetch_batch_market_capitalization', lambda **kwargs: [])
    monkeypatch.setattr(market, 'fetch_company_profile', lambda symbol: [{'symbol': symbol, 'marketCap': 20e9, 'averageVolume': 4e6}])
    with session() as db:
        db.add(PriceCache(symbol='THIN', date='2026-10-02', close=100, volume=1e9))
        for offset in range(10, 30):
            db.add(PriceCache(symbol='OLD', date=(NOW - timedelta(days=offset)).date().isoformat(), close=100, volume=1e9))
        db.commit()
        rows = [candidate('THIN'), candidate('OLD')]
        market.enrich_leaderboard_market_data(db, rows, now=NOW)
        assert all(r['avg_volume'] == 4e6 and r['avg_volume_source'] == 'fmp_profile' for r in rows)


def test_provider_failure_does_not_erase_existing_market_data(monkeypatch):
    def failed(**kwargs):
        raise RuntimeError('Unavailable')
    monkeypatch.setattr(market, 'fetch_batch_market_capitalization', failed)
    monkeypatch.setattr(market, 'fetch_company_profile', failed)
    with session() as db:
        rows = [{**candidate('TEST'), 'market_cap': 1e9, 'avg_volume': 1e6}]
        market.enrich_leaderboard_market_data(db, rows, now=NOW)
        assert rows[0]['market_cap'] == 1e9 and rows[0]['avg_volume'] == 1e6
