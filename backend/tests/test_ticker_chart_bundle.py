from datetime import datetime, timedelta, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db import Base
from app.main import _build_insider_stock_chart_bundle, _build_ticker_chart_bundle, _build_ticker_chart_quote
from app.models import Event, PriceCache
from app.request_priority import reset_request_context, set_request_context
from app.services.price_lookup import get_daily_close_series_with_fallback


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
        {"date": day.isoformat(), "close": round(base_close + idx, 2), "volume": 1_000_000 + idx}
        for idx, day in enumerate(days)
    ]


def _disable_chart_metric_fetches(monkeypatch):
    monkeypatch.setattr("app.main._ratios_ttm_from_fmp", lambda symbol: {})
    monkeypatch.setattr("app.main._company_profile_snapshot_from_fmp", lambda symbol: {})
    monkeypatch.setattr("app.main.get_daily_volume_series_from_provider", lambda symbol, start_date, end_date: {})


def test_ticker_chart_bundle_uses_daily_prices_sp500_and_normalized_markers(monkeypatch):
    db = _session()
    monkeypatch.delenv("FMP_API_KEY", raising=False)
    today = datetime.now(timezone.utc).date()
    prior = today - timedelta(days=1)
    for symbol, rows in {
        "AAPL": [
            (prior.isoformat(), 190.0),
            (today.isoformat(), 195.0),
        ],
        "SPY": [
            (prior.isoformat(), 5100.0),
            (today.isoformat(), 5150.0),
        ],
    }.items():
        for day, close in rows:
            db.add(PriceCache(symbol=symbol, date=day, close=close))

    db.add(
        Event(
            event_type="congress_trade",
            ts=datetime.combine(today, datetime.min.time(), tzinfo=timezone.utc),
            event_date=datetime.combine(today, datetime.min.time(), tzinfo=timezone.utc),
            symbol="AAPL",
            source="house",
            impact_score=1.0,
            payload_json='{"trade_date":"%s"}' % today.isoformat(),
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
            "volume": 18_400_000,
            "avgVolume": 50_000_000,
            "avgVolume30D": 51_000_000,
            "pe": 11.0,
            "beta": 9.9,
        },
    )
    monkeypatch.setattr(
        "app.main._ratios_ttm_from_fmp",
        lambda symbol: {"priceToEarningsRatioTTM": 32.889608822880916},
    )
    monkeypatch.setattr("app.main._company_profile_snapshot_from_fmp", lambda symbol: {"beta": 1.2})
    monkeypatch.setattr("app.main._query_unified_signals", lambda **kwargs: [])

    bundle = _build_ticker_chart_bundle("aapl", 30, db)

    assert bundle["resolution"] == "daily"
    assert bundle["benchmark"]["symbol"] == "SPY"
    assert bundle["benchmark"]["label"] == "S&P 500"
    assert bundle["prices"][-1] == {"date": today.isoformat(), "close": 195.0}
    assert bundle["benchmark"]["points"][-1]["close"] == 5150.0
    assert bundle["markers"][0]["kind"] == "congress"
    assert bundle["markers"][0]["date"] == today.isoformat()
    assert bundle["quote"]["current_price"] == 195.0
    assert bundle["quote"]["latest_close"] == 195.0
    assert bundle["quote"]["previous_close"] == 190.0
    assert bundle["quote"]["day_change"] == 5.0
    assert bundle["quote"]["market_cap"] == 3_000_000_000
    assert bundle["quote"]["day_volume"] == 18_400_000
    assert bundle["quote"]["average_volume"] == 51_000_000
    assert bundle["quote"]["trailing_pe"] == 32.889608822880916
    assert bundle["quote"]["beta"] == 1.2


def test_insider_stock_chart_scopes_markers_to_reporting_cik_and_symbol(monkeypatch):
    db = _session()
    monkeypatch.delenv("FMP_API_KEY", raising=False)
    _disable_chart_metric_fetches(monkeypatch)
    monkeypatch.setattr("app.main._quote_snapshot_from_fmp", lambda symbol: {})
    monkeypatch.setattr("app.main.get_current_prices_db", lambda db, symbols: {})

    today = datetime.now(timezone.utc).date()
    for symbol in ("AAPL", "SPY"):
        db.add(PriceCache(symbol=symbol, date=today.isoformat(), close=100.0))

    db.add(
        Event(
            id=1,
            event_type="insider_trade",
            ts=datetime.now(timezone.utc),
            event_date=datetime.now(timezone.utc),
            symbol="AAPL",
            source="fmp",
            trade_type="Purchase",
            amount_min=10_000,
            amount_max=10_000,
            payload_json='{"reporting_cik":"0001234567","symbol":"AAPL","transaction_date":"%s","filing_date":"%s","insider_name":"Scoped Insider","shares":50,"price":20}'
            % (today.isoformat(), today.isoformat()),
        )
    )
    db.add(
        Event(
            id=2,
            event_type="insider_trade",
            ts=datetime.now(timezone.utc),
            event_date=datetime.now(timezone.utc),
            symbol="AAPL",
            source="fmp",
            trade_type="Sale",
            amount_min=20_000,
            amount_max=20_000,
            payload_json='{"reporting_cik":"0009999999","symbol":"AAPL","transaction_date":"%s","insider_name":"Other Insider"}'
            % today.isoformat(),
        )
    )
    db.add(
        Event(
            id=3,
            event_type="congress_trade",
            ts=datetime.now(timezone.utc),
            event_date=datetime.now(timezone.utc),
            symbol="AAPL",
            source="house",
            trade_type="Purchase",
            amount_min=1_000,
            amount_max=15_000,
            payload_json='{"trade_date":"%s"}' % today.isoformat(),
        )
    )
    db.commit()

    bundle = _build_insider_stock_chart_bundle("0001234567", days=30, symbol="AAPL", db=db)

    assert bundle["symbol"] == "AAPL"
    assert [marker["event_id"] for marker in bundle["markers"]] == [1]
    assert bundle["markers"][0]["kind"] == "insider"
    assert bundle["markers"][0]["side"] == "buy"
    assert bundle["markers"][0]["meta"]["filing_date"] == today.isoformat()
    assert bundle["markers"][0]["meta"]["shares"] == 50.0
    assert bundle["markers"][0]["meta"]["price"] == 20.0


def test_insider_stock_chart_empty_without_symbol(monkeypatch):
    db = _session()
    monkeypatch.delenv("FMP_API_KEY", raising=False)

    bundle = _build_insider_stock_chart_bundle("0001234567", days=30, symbol=None, db=db)

    assert bundle["symbol"] is None
    assert bundle["prices"] == []
    assert bundle["markers"] == []


def test_ticker_chart_bundle_hydrates_sparse_cache_from_daily_history(monkeypatch):
    db = _session()
    monkeypatch.setenv("FMP_API_KEY", "test-key")
    _disable_chart_metric_fetches(monkeypatch)
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


def test_daily_close_series_refreshes_dense_cache_with_stale_tail(monkeypatch):
    db = _session()
    monkeypatch.setenv("FMP_API_KEY", "test-key")
    end = datetime.now(timezone.utc).date()
    start = end - timedelta(days=44)
    weekdays = []
    cursor = start
    while cursor <= end:
        if cursor.weekday() < 5:
            weekdays.append(cursor)
        cursor += timedelta(days=1)
    stale_rows = weekdays[:-5]
    tail_rows = weekdays[-5:]

    for index, day in enumerate(stale_rows):
        db.add(PriceCache(symbol="MU", date=day.isoformat(), close=100.0 + index))
    db.commit()

    def fake_fetch(url, params, retries=2):
        requested_from = datetime.fromisoformat(params["from"]).date()
        rows = tail_rows if requested_from > stale_rows[-1] else stale_rows
        return _FakeResponse(
            200,
            [
                {"date": day.isoformat(), "close": 100.0 + index}
                for index, day in enumerate(rows, start=1 if rows is tail_rows else 0)
            ],
        )

    monkeypatch.setattr("app.services.price_lookup._fetch_with_backoff", fake_fetch)

    series = get_daily_close_series_with_fallback(db, "MU", start.isoformat(), end.isoformat())

    assert max(series) == tail_rows[-1].isoformat()
    assert len([day for day in tail_rows if day.isoformat() in series]) == len(tail_rows)


def test_ticker_chart_bundle_hydrates_missing_adr_history(monkeypatch):
    db = _session()
    monkeypatch.setenv("FMP_API_KEY", "test-key")
    _disable_chart_metric_fetches(monkeypatch)
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
    assert bundle["quote"]["current_price"] == provider_rows[-1]["close"]


def test_ticker_chart_bundle_keeps_ticker_when_benchmark_history_missing(monkeypatch):
    db = _session()
    monkeypatch.setenv("FMP_API_KEY", "test-key")
    _disable_chart_metric_fetches(monkeypatch)
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


def test_ticker_chart_bundle_computes_average_volume_from_daily_history(monkeypatch):
    db = _session()
    monkeypatch.setenv("FMP_API_KEY", "test-key")
    monkeypatch.setattr("app.main._quote_snapshot_from_fmp", lambda symbol: {})
    monkeypatch.setattr("app.main._ratios_ttm_from_fmp", lambda symbol: {})
    monkeypatch.setattr("app.main._company_profile_snapshot_from_fmp", lambda symbol: {})
    monkeypatch.setattr("app.main.get_current_prices_db", lambda db, symbols: {})
    monkeypatch.setattr("app.main._query_unified_signals", lambda **kwargs: [])

    end = datetime.now(timezone.utc).date()
    rows = []
    cursor = end - timedelta(days=70)
    idx = 0
    while cursor <= end:
        if cursor.weekday() < 5:
            rows.append(
                {
                    "date": cursor.isoformat(),
                    "close": 100.0 + idx,
                    "volume": 1_000_000 + (idx * 10_000),
                }
            )
            idx += 1
        cursor += timedelta(days=1)

    full_calls = 0

    def fake_fetch(url, params, retries=2):
        nonlocal full_calls
        if params["symbol"] == "AAPL" and url.endswith("/historical-price-eod/full"):
            full_calls += 1
            return _FakeResponse(200, rows)
        return _FakeResponse(200, [])

    monkeypatch.setattr("app.services.price_lookup._fetch_with_backoff", fake_fetch)

    bundle = _build_ticker_chart_bundle("AAPL", 60, db)

    expected = sum(row["volume"] for row in rows[-30:]) / 30
    assert bundle["quote"]["average_volume"] == expected
    assert full_calls == 1


def test_ticker_chart_quote_uses_chart_series_as_canonical_daily_price(monkeypatch):
    db = _session()
    _disable_chart_metric_fetches(monkeypatch)
    monkeypatch.setattr(
        "app.main._quote_snapshot_from_fmp",
        lambda symbol: {"price": 195.55, "change": -1.38, "changesPercentage": -0.7, "volume": 123},
    )
    monkeypatch.setattr("app.main.get_current_prices_meta_db", lambda *args, **kwargs: {args[1][0]: {"price": 195.55}})
    monkeypatch.setattr("app.main._cached_average_volume", lambda db, symbol: 55_000_000)
    db.add(PriceCache(symbol="NVDA", date="2026-07-08", close=204.12, volume=60_000_000))
    db.commit()

    quote = _build_ticker_chart_quote(
        db,
        "NVDA",
        [
            {"date": "2026-07-07", "close": 196.94},
            {"date": "2026-07-08", "close": 204.12},
        ],
    )

    assert quote["current_price"] == 204.12
    assert quote["latest_close"] == 204.12
    assert quote["previous_close"] == 196.94
    assert quote["day_change"] == 204.12 - 196.94
    assert quote["day_change_pct"] == ((204.12 - 196.94) / 196.94) * 100
    assert quote["day_volume"] == 60_000_000
    assert quote["average_volume"] == 55_000_000
    assert quote["source_freshness"]["price_source"] == "daily_series"


def test_ticker_chart_request_time_refreshes_stale_recent_history(monkeypatch):
    db = _session()
    monkeypatch.setenv("FMP_API_KEY", "test-key")
    expected = datetime(2026, 6, 23, tzinfo=timezone.utc).date()
    stale_day = expected - timedelta(days=6)
    _disable_chart_metric_fetches(monkeypatch)
    monkeypatch.setattr("app.main.get_expected_latest_market_date", lambda: expected)
    monkeypatch.setattr("app.main._quote_snapshot_from_fmp", lambda symbol: {})
    monkeypatch.setattr("app.main.get_current_prices_db", lambda db, symbols: {})
    monkeypatch.setattr("app.main._query_unified_signals", lambda **kwargs: [])
    db.add(PriceCache(symbol="AAPL", date=stale_day.isoformat(), close=190.0))
    db.add(PriceCache(symbol="SPY", date=expected.isoformat(), close=5200.0))
    db.commit()

    provider_rows = [
        {"date": (expected - timedelta(days=offset)).isoformat(), "close": 200.0 - offset, "volume": 1_000_000}
        for offset in range(4, -1, -1)
    ]
    calls = []

    def fake_fetch(url, params, retries=2):
        calls.append(params["symbol"])
        if params["symbol"] == "AAPL":
            return _FakeResponse(200, provider_rows)
        return _FakeResponse(200, [])

    monkeypatch.setattr("app.services.price_lookup._fetch_with_backoff", fake_fetch)
    token = set_request_context({"path": "/api/tickers/AAPL/chart-bundle", "priority": "heavy"})
    try:
        bundle = _build_ticker_chart_bundle("AAPL", 30, db)
    finally:
        reset_request_context(token)

    assert "AAPL" in calls
    assert bundle["prices"][-1] == {"date": expected.isoformat(), "close": 200.0}
    assert bundle["freshness"]["status"] == "ok"
    assert bundle["freshness"]["is_stale"] is False


def test_ticker_chart_request_time_uses_alternate_source_when_dense_history_tail_is_stale(monkeypatch):
    db = _session()
    monkeypatch.setenv("FMP_API_KEY", "test-key")
    monkeypatch.setenv("MASSIVE_API_KEY", "massive-test-key")
    expected = datetime(2026, 6, 23, tzinfo=timezone.utc).date()
    stale_day = expected - timedelta(days=6)
    _disable_chart_metric_fetches(monkeypatch)
    monkeypatch.setattr("app.main.get_expected_latest_market_date", lambda: expected)
    monkeypatch.setattr("app.main._quote_snapshot_from_fmp", lambda symbol: {})
    monkeypatch.setattr("app.main.get_current_prices_db", lambda db, symbols: {})
    monkeypatch.setattr("app.main._query_unified_signals", lambda **kwargs: [])
    db.add(PriceCache(symbol="NVDA", date=stale_day.isoformat(), close=180.0))
    db.add(PriceCache(symbol="SPY", date=expected.isoformat(), close=5200.0))
    db.commit()

    fmp_rows = []
    cursor = expected - timedelta(days=20)
    idx = 0
    while cursor <= stale_day:
        if cursor.weekday() < 5:
            fmp_rows.append({"date": cursor.isoformat(), "close": 180.0 + idx, "volume": 1_000_000 + idx})
            idx += 1
        cursor += timedelta(days=1)

    massive_days = [
        expected - timedelta(days=7),
        expected - timedelta(days=6),
        expected - timedelta(days=5),
        expected - timedelta(days=1),
        expected,
    ]
    massive_rows = [
        {
            "t": int(datetime.combine(day, datetime.min.time(), tzinfo=timezone.utc).timestamp() * 1000),
            "c": 220.0 + idx,
            "v": 2_000_000 + idx,
        }
        for idx, day in enumerate(massive_days)
    ]
    calls = []

    def fake_fetch(url, params, retries=2):
        calls.append(url)
        if "historical-price-eod" in url and params.get("symbol") == "NVDA":
            return _FakeResponse(200, fmp_rows)
        if "/v2/aggs/ticker/NVDA/" in url:
            return _FakeResponse(200, {"results": massive_rows})
        return _FakeResponse(200, [])

    monkeypatch.setattr("app.services.price_lookup._fetch_with_backoff", fake_fetch)
    token = set_request_context({"path": "/api/tickers/NVDA/chart-bundle", "priority": "heavy"})
    try:
        bundle = _build_ticker_chart_bundle("NVDA", 30, db)
    finally:
        reset_request_context(token)

    assert any("/v2/aggs/ticker/NVDA/" in url for url in calls)
    assert bundle["prices"][-1] == {"date": expected.isoformat(), "close": 224.0}
    assert bundle["status"] == "ok"
    assert bundle["freshness"]["is_stale"] is False


def test_ticker_chart_marks_stale_when_request_time_refresh_has_no_recent_data(monkeypatch):
    db = _session()
    monkeypatch.setenv("FMP_API_KEY", "test-key")
    expected = datetime(2026, 6, 23, tzinfo=timezone.utc).date()
    stale_day = expected - timedelta(days=6)
    _disable_chart_metric_fetches(monkeypatch)
    monkeypatch.setattr("app.main.get_expected_latest_market_date", lambda: expected)
    monkeypatch.setattr("app.main._quote_snapshot_from_fmp", lambda symbol: {})
    monkeypatch.setattr("app.main.get_current_prices_db", lambda db, symbols: {})
    monkeypatch.setattr("app.main._query_unified_signals", lambda **kwargs: [])
    db.add(PriceCache(symbol="SNDK", date=stale_day.isoformat(), close=42.0))
    db.add(PriceCache(symbol="SPY", date=expected.isoformat(), close=5200.0))
    db.commit()

    monkeypatch.setattr("app.services.price_lookup._fetch_with_backoff", lambda *args, **kwargs: _FakeResponse(200, []))
    token = set_request_context({"path": "/api/tickers/SNDK/chart-bundle", "priority": "heavy"})
    try:
        bundle = _build_ticker_chart_bundle("SNDK", 30, db)
    finally:
        reset_request_context(token)

    assert bundle["prices"][-1] == {"date": stale_day.isoformat(), "close": 42.0}
    assert bundle["status"] == "stale"
    assert bundle["freshness"]["is_stale"] is True
    assert bundle["freshness"]["latest_date"] == stale_day.isoformat()


def test_daily_close_series_background_context_refreshes_instead_of_requeueing(monkeypatch):
    db = _session()
    monkeypatch.setenv("FMP_API_KEY", "test-key")
    end = datetime(2026, 6, 23, tzinfo=timezone.utc).date()
    start = end - timedelta(days=20)
    provider_rows = [
        {"date": (end - timedelta(days=offset)).isoformat(), "close": 120.0 - offset}
        for offset in range(4, -1, -1)
    ]
    enqueued = []

    def fake_fetch(url, params, retries=2):
        if params["symbol"] == "MSFT":
            return _FakeResponse(200, provider_rows)
        return _FakeResponse(200, [])

    monkeypatch.setattr("app.services.price_lookup._fetch_with_backoff", fake_fetch)
    monkeypatch.setattr("app.services.price_lookup.enqueue_data_enrichment_job", lambda **kwargs: enqueued.append(kwargs))
    token = set_request_context({"path": "background", "priority": "normal", "job_type": "price_series"})
    try:
        series = get_daily_close_series_with_fallback(db, "MSFT", start.isoformat(), end.isoformat())
    finally:
        reset_request_context(token)

    assert series[max(series)] == 120.0
    assert enqueued == []
