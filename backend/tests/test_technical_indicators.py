from datetime import datetime, timedelta, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db import Base
from app.models import PriceCache
from app.services.technical_indicators import build_ticker_technical_indicators


class _FakeResponse:
    def __init__(self, status_code: int, payload):
        self.status_code = status_code
        self._payload = payload

    def json(self):
        return self._payload


def _session():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)
    Session = sessionmaker(bind=engine, future=True)
    return Session()


def _dense_provider_rows(base_close: float, days: int = 70):
    end = datetime.now(timezone.utc).date()
    start = end - timedelta(days=days - 1)
    rows = []
    close = base_close
    current = start
    while current <= end:
        if current.weekday() < 5:
            rows.append({"date": current.isoformat(), "close": round(close, 2), "volume": 1_000_000})
            close += 0.8
        current += timedelta(days=1)
    return rows


def test_rsi_fallback_computes_from_daily_close_history(monkeypatch):
    db = _session()
    provider_rows = _dense_provider_rows(100.0)

    def fake_fetch(url, params, retries=2):
        if params["symbol"] == "AAPL":
            return _FakeResponse(200, provider_rows)
        return _FakeResponse(200, [])

    monkeypatch.setenv("FMP_API_KEY", "test-key")
    monkeypatch.setattr("app.services.price_lookup._fetch_with_backoff", fake_fetch)

    technicals = build_ticker_technical_indicators(db, "AAPL", lookback_days=90)

    assert technicals["price_points"] == len(provider_rows)
    assert technicals["rsi"]["status"] == "ok"
    assert technicals["rsi"]["value"] is not None
    assert technicals["rsi"]["message"] in {"RSI above neutral", "RSI near neutral", "RSI below neutral"}


def test_rsi_reports_insufficient_history_when_price_window_is_too_short():
    db = _session()
    end = datetime.now(timezone.utc).date()
    current = end - timedelta(days=13)
    close = 20.0
    while current <= end:
        if current.weekday() < 5:
            db.add(PriceCache(symbol="NEW", date=current.isoformat(), close=close))
            close += 0.5
        current += timedelta(days=1)
    db.commit()

    technicals = build_ticker_technical_indicators(db, "NEW", lookback_days=14)

    assert technicals["rsi"]["status"] == "unavailable"
    assert technicals["rsi"]["reason"] == "insufficient_price_history"
    assert technicals["rsi"]["message"] == "RSI unavailable - insufficient price history"
