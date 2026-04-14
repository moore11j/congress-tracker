from datetime import datetime, timedelta, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db import Base
from app.main import _build_ticker_chart_bundle
from app.models import Event, PriceCache


def _session():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)
    TestSession = sessionmaker(bind=engine, future=True)
    return TestSession()


class _FakeResponse:
    def __init__(self, status_code: int, payload):
        self.status_code = status_code
        self._payload = payload

    def json(self):
        return self._payload


def _dense_provider_rows(base_close: float):
    end = datetime.now(timezone.utc).date()
    start = end - timedelta(days=29)
    days: list = []
    cursor = start
    while cursor <= end:
        if cursor.weekday() < 5:
            days.append(cursor)
        cursor += timedelta(days=1)
    return [
        {"date": day.isoformat(), "close": round(base_close + idx, 2)}
        for idx, day in enumerate(days)
    ]


def test_ticker_chart_bundle_uses_daily_prices_sp500_and_normalized_markers(monkeypatch):
    db = _session()
    monkeypatch.delenv("FMP_API_KEY", raising=False)
    for symbol, rows in {
        "AAPL": [
            ("2026-04-09", 190.0),
            ("2026-04-10", 195.0),
        ],
        "^GSPC": [
            ("2026-04-09", 5100.0),
            ("2026-04-10", 5150.0),
        ],
    }.items():
        for day, close in rows:
            db.add(PriceCache(symbol=symbol, date=day, close=close))

    db.add(
        Event(
            event_type="congress_trade",
            ts=datetime(2026, 4, 10, tzinfo=timezone.utc),
            event_date=datetime(2026, 4, 10, tzinfo=timezone.utc),
            symbol="AAPL",
            source="house",
            impact_score=1.0,
            payload_json='{"trade_date":"2026-04-10"}',
            member_name="Example Member",
            member_bioguide_id="E000001",
            chamber="House",
            party="D",
            trade_type="Purchase",
            amount_min=1000,
            amount_max=15000,
        )
    )
    db.commit()

    monkeypatch.setattr(
        "app.main._quote_snapshot_from_fmp",
        lambda symbol: {
            "price": 196.0,
            "previousClose": 195.0,
            "marketCap": 3_000_000_000,
            "avgVolume": 50_000_000,
            "pe": 28.5,
            "beta": 1.2,
        },
    )
    monkeypatch.setattr("app.main._query_unified_signals", lambda **kwargs: [])

    bundle = _build_ticker_chart_bundle("aapl", 30, db)

    assert bundle["resolution"] == "daily"
    assert bundle["benchmark"]["symbol"] == "^GSPC"
    assert bundle["benchmark"]["label"] == "S&P 500"
    assert bundle["prices"][-1] == {"date": "2026-04-10", "close": 195.0}
    assert bundle["benchmark"]["points"][-1]["close"] == 5150.0
    assert bundle["markers"][0]["kind"] == "congress"
    assert bundle["markers"][0]["date"] == "2026-04-10"
    assert bundle["quote"]["current_price"] == 196.0
    assert bundle["quote"]["day_change"] == 1.0
    assert bundle["quote"]["market_cap"] == 3_000_000_000


def test_ticker_chart_bundle_hydrates_sparse_cache_from_daily_history(monkeypatch):
    db = _session()
    monkeypatch.setenv("FMP_API_KEY", "test-key")
    monkeypatch.setattr("app.main._quote_snapshot_from_fmp", lambda symbol: {})
    monkeypatch.setattr("app.main.get_current_prices_db", lambda db, symbols: {})
    monkeypatch.setattr("app.main._query_unified_signals", lambda **kwargs: [])

    for day, close in [
        ("2026-02-02", 96.18),
        ("2026-03-03", 94.07),
    ]:
        db.add(PriceCache(symbol="ROKU", date=day, close=close))
    db.commit()

    provider_rows = _dense_provider_rows(90.0)

    def fake_fetch(url, params, retries=2):
        if params["symbol"] == "ROKU":
            return _FakeResponse(200, provider_rows)
        return _FakeResponse(200, [])

    monkeypatch.setattr("app.services.price_lookup._fetch_with_backoff", fake_fetch)

    bundle = _build_ticker_chart_bundle("ROKU", 30, db)

    assert len(bundle["prices"]) == len(provider_rows)
    assert bundle["prices"][0] == {"date": provider_rows[0]["date"], "close": provider_rows[0]["close"]}
    assert bundle["prices"][-1] == {"date": provider_rows[-1]["date"], "close": provider_rows[-1]["close"]}


def test_ticker_chart_bundle_hydrates_missing_adr_history(monkeypatch):
    db = _session()
    monkeypatch.setenv("FMP_API_KEY", "test-key")
    monkeypatch.setattr("app.main._quote_snapshot_from_fmp", lambda symbol: {"price": 2.59})
    monkeypatch.setattr("app.main._query_unified_signals", lambda **kwargs: [])

    provider_rows = _dense_provider_rows(2.0)

    def fake_fetch(url, params, retries=2):
        if params["symbol"] == "BZUN":
            return _FakeResponse(200, provider_rows)
        return _FakeResponse(200, [])

    monkeypatch.setattr("app.services.price_lookup._fetch_with_backoff", fake_fetch)

    bundle = _build_ticker_chart_bundle("BZUN", 30, db)

    assert bundle["prices"][0] == {"date": provider_rows[0]["date"], "close": provider_rows[0]["close"]}
    assert bundle["prices"][-1] == {"date": provider_rows[-1]["date"], "close": provider_rows[-1]["close"]}
    assert bundle["quote"]["current_price"] == 2.59


def test_ticker_chart_bundle_keeps_ticker_when_benchmark_history_missing(monkeypatch):
    db = _session()
    monkeypatch.setenv("FMP_API_KEY", "test-key")
    monkeypatch.setattr("app.main._quote_snapshot_from_fmp", lambda symbol: {})
    monkeypatch.setattr("app.main.get_current_prices_db", lambda db, symbols: {})
    monkeypatch.setattr("app.main._query_unified_signals", lambda **kwargs: [])

    provider_rows = _dense_provider_rows(100.0)

    def fake_fetch(url, params, retries=2):
        if params["symbol"] == "AAPL":
            return _FakeResponse(200, provider_rows)
        return _FakeResponse(429, [])

    monkeypatch.setattr("app.services.price_lookup._fetch_with_backoff", fake_fetch)

    bundle = _build_ticker_chart_bundle("AAPL", 30, db)

    assert len(bundle["prices"]) == len(provider_rows)
    assert bundle["benchmark"]["points"] == []
