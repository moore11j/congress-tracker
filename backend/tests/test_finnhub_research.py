from datetime import datetime, timedelta, timezone
import json

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from app.db import Base
from app.models import InsightsSnapshot
from app.request_priority import reset_request_context, set_request_context
from app.services import finnhub_research as adapter, replacement_news as cache
from app.services import fmp_news, insights_snapshots as insights

NOW = datetime.now(timezone.utc)


def news_row(**kwargs):
    return {"id": 1, "headline": "Treasury yields rise", "url": "https://example.com/article",
            "source": "Publisher", "datetime": int(NOW.timestamp()), "related": "AAPL", **kwargs}


def rating_row(**kwargs):
    return {"symbol": "AAPL", "period": NOW.date().replace(day=1).isoformat(),
            "strongBuy": 2, "buy": 3, "hold": 4, "sell": 1, "strongSell": 0, **kwargs}


def forbidden(*args, **kwargs):
    pytest.fail("Unexpected live provider, model, or legacy cache call")


@pytest.fixture
def sessions(monkeypatch):
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine, tables=[InsightsSnapshot.__table__])
    factory = sessionmaker(bind=engine)
    monkeypatch.setattr(cache, "SessionLocal", factory)
    monkeypatch.setenv("NEWS_PROVIDER", "finnhub")
    monkeypatch.setenv("FMP_PROVIDER_DISABLED", "1")
    monkeypatch.setattr(fmp_news, "_request_rows", forbidden)
    monkeypatch.setattr(fmp_news, "_cache_get", forbidden)
    monkeypatch.setattr(fmp_news, "db_ticker_content_cache_get", forbidden)
    yield factory
    engine.dispose()


def test_news_deduplicates_urls_preserves_time_and_does_not_republish_text():
    rows = [news_row(summary="Not republished"), news_row(id=2, url="https://example.com/article?utm_source=x#top")]
    result = adapter.normalize_news(rows, observed_at=NOW, symbol="AAPL")
    assert len(result["items"]) == 1
    item = result["items"][0]
    assert item["source"] == "finnhub" and item["site"] == "Publisher"
    assert item["published_at"] == datetime.fromtimestamp(rows[0]["datetime"], timezone.utc).isoformat()
    assert item["summary"] is None and item["observed_at"] == NOW.isoformat()


@pytest.mark.parametrize("change", [{"url": "javascript:alert(1)"}, {"url": "http://127.0.0.1/x"},
    {"url": "https://user:password@example.com/x"}, {"source": ""}, {"datetime": True},
    {"datetime": int((NOW + timedelta(days=1)).timestamp())}, {"related": "MSFT"},
    {"datetime": int((NOW - timedelta(days=8)).timestamp())}])
def test_invalid_news_cannot_be_reported_as_empty_coverage(change):
    with pytest.raises(adapter.FinnhubUnavailable, match="no_valid_recent_rows"):
        adapter.normalize_news([news_row(**change)], observed_at=NOW, symbol="AAPL")


def test_conflicting_provider_id_is_held():
    with pytest.raises(adapter.FinnhubUnavailable, match="conflicting_news_identity"):
        adapter.normalize_news([news_row(), news_row(url="https://example.com/other")], observed_at=NOW)


def test_recommendations_keep_source_period_and_explicit_scope():
    result = adapter.normalize_recommendations([rating_row(), rating_row()], "AAPL", observed_at=NOW)
    assert len(result["items"]) == 1 and result["current"]["total"] == 10
    assert result["current"]["period"] == NOW.date().replace(day=1).isoformat()
    assert result["observed_at"] == NOW.isoformat()
    assert not result["earnings_estimates_available"] and not result["price_targets_available"]
    assert not result["publication_eligible"]
    old = rating_row(period=(NOW.date() - timedelta(days=46)).isoformat())
    assert adapter.normalize_recommendations([old], "AAPL", observed_at=NOW)["status"] == "stale"


@pytest.mark.parametrize("change", [{"symbol": "MSFT"}, {"buy": -1}, {"buy": 2.5}, {"buy": True},
    {"period": "unknown"}, {"period": (NOW.date() + timedelta(days=1)).isoformat()}])
def test_bad_recommendations_are_held(change):
    with pytest.raises(adapter.FinnhubUnavailable):
        adapter.normalize_recommendations([rating_row(**change)], "AAPL", observed_at=NOW)


def test_conflicting_recommendation_period_is_not_arbitrarily_picked():
    with pytest.raises(adapter.FinnhubUnavailable, match="conflicting_recommendation_period"):
        adapter.normalize_recommendations([rating_row(), rating_row(buy=9)], "AAPL", observed_at=NOW)


def test_no_key_no_http_and_no_premium_endpoints(monkeypatch):
    monkeypatch.delenv("FINNHUB_API_KEY", raising=False)
    monkeypatch.setattr(adapter.requests, "get", forbidden)
    with pytest.raises(adapter.FinnhubUnavailable, match="missing_api_key"):
        adapter.fetch_recommendations("AAPL")
    with pytest.raises(ValueError, match="Unsupported research endpoint"):
        adapter.request_rows("stock/price-target", {})


@pytest.mark.parametrize("code,reason", [(401, "authentication_failed"), (403, "access_denied"), (429, "rate_limited"), (302, "provider_unavailable")])
def test_access_failures_no_retry_no_secrets(monkeypatch, code, reason):
    calls = []
    class Response:
        status_code = code
        headers = {}
        def __enter__(self): return self
        def __exit__(self, *args): pass
    def request(url, **kwargs):
        calls.append((url, kwargs))
        return Response()
    monkeypatch.setenv("FINNHUB_API_KEY", "private-test-key")
    monkeypatch.setattr(adapter.requests, "get", request)
    with pytest.raises(adapter.FinnhubUnavailable, match=reason):
        adapter.fetch_recommendations("AAPL")
    assert len(calls) == 1
    assert "private-test-key" not in calls[0][0] and "token" not in calls[0][1]["params"]
    assert calls[0][1]["allow_redirects"] is False


def test_shared_cooldown_prevents_next_http_call(monkeypatch):
    from app.services import finnhub_budget as budget
    engine = create_engine('sqlite:///:memory:')
    Base.metadata.create_all(engine, tables=[InsightsSnapshot.__table__])
    monkeypatch.setattr(budget, 'SessionLocal', sessionmaker(bind=engine))
    monkeypatch.setenv('FINNHUB_SHARED_LIMITER_ENABLED', '1')
    monkeypatch.setenv('FINNHUB_API_KEY', 'private-test-key')
    calls = []
    class Response:
        status_code = 429
        headers = {'Retry-After': '120'}
        def __enter__(self): return self
        def __exit__(self, *args): pass
    def request(*args, **kwargs):
        calls.append(True)
        return Response()
    monkeypatch.setattr(adapter.requests, 'get', request)
    try:
        with pytest.raises(adapter.FinnhubUnavailable, match='rate_limited'):
            adapter.request_json('news', {})
        with pytest.raises(adapter.FinnhubUnavailable, match='provider_cooldown'):
            adapter.request_json('stock/metric', {'symbol': 'AAPL'})
        assert len(calls) == 1
    finally:
        engine.dispose()


def test_shared_budget_failure_blocks_http_without_leaking_error(monkeypatch):
    from app.services import finnhub_budget as budget
    monkeypatch.setenv('FINNHUB_SHARED_LIMITER_ENABLED', '1')
    monkeypatch.setenv('FINNHUB_API_KEY', 'private-test-key')
    def fail():
        raise RuntimeError('private database connection details')
    monkeypatch.setattr(budget, 'SessionLocal', fail)
    monkeypatch.setattr(adapter.requests, 'get', forbidden)
    with pytest.raises(adapter.FinnhubUnavailable) as error:
        adapter.request_json('news', {})
    assert str(error.value) == 'request_budget_unavailable'


def test_persistent_news_reused_across_pages_categories_and_no_fmp(sessions, monkeypatch):
    calls = []
    def fetch(**kwargs):
        calls.append(kwargs)
        return adapter.normalize_news([news_row(), news_row(id=2, url="https://example.com/other", headline="Gold rises")], observed_at=NOW)
    monkeypatch.setattr(cache, "fetch_news", fetch)
    first = fmp_news.get_general_news(limit=1)
    second = fmp_news.get_general_news(page=1, limit=1)
    treasury = fmp_news.get_insights_category_news("us-treasury")
    assert first["has_next"] and not second["has_next"]
    assert first["items"][0]["url"] != second["items"][0]["url"]
    assert treasury["items"][0]["title"] == "Treasury yields rise"
    assert len(calls) == 1
    with sessions() as db:
        assert len(db.scalars(select(InsightsSnapshot)).all()) == 1


def test_public_ticker_force_refresh_only_enqueues(sessions, monkeypatch):
    queued = []
    monkeypatch.setattr(cache, "fetch_news", forbidden)
    monkeypatch.setattr(cache, "_enqueue", lambda *args: queued.append(args))
    token = set_request_context({"path": "/api/tickers/AAPL/news", "panel": "TickerNewsPanel", "request_source": "client", "route_family": "ticker"})
    try:
        result = fmp_news.get_stock_news(symbol="AAPL", force_refresh=True)
    finally:
        reset_request_context(token)
    assert result["status"] == "warming" and queued == [("AAPL", "general")]


def test_failing_refresh_preserves_stale_cache_and_expires_it(sessions, monkeypatch):
    payload = adapter.normalize_news([news_row()], observed_at=NOW)
    with sessions() as db:
        db.add(InsightsSnapshot(kind="finnhub-news:market:general", source="finnhub", payload_json=json.dumps(payload), fetched_at=NOW - timedelta(hours=1)))
        db.commit()
    def fail(**kwargs): raise adapter.FinnhubUnavailable("rate_limited")
    monkeypatch.setattr(cache, "fetch_news", fail)
    result = cache.prepared_news(public=False)
    assert result["stale"] and result["reason"] == "rate_limited" and len(result["items"]) == 1
    with sessions() as db:
        row = db.get(InsightsSnapshot, "finnhub-news:market:general")
        assert json.loads(row.payload_json) == payload
        row.fetched_at = NOW - timedelta(hours=25)
        db.commit()
    assert cache.prepared_news(public=False)["status"] == "unavailable"


def test_insights_never_revives_fmp_cache_and_preserves_replacement_provenance(sessions, monkeypatch):
    monkeypatch.setattr(insights, "enrich_walnut_takes", lambda db, items, **kwargs: items)
    with sessions() as db:
        db.add(InsightsSnapshot(kind=insights.INSIGHTS_HEADLINES_KIND, source="fmp", fetched_at=NOW,
            payload_json=json.dumps({"items": [{"title": "Old vendor"}]})))
        db.commit()
        assert insights.get_insights_headlines(db)["items"] == []
        monkeypatch.setattr(insights, "get_general_news", lambda **kwargs: {"items": []})
        assert insights.refresh_insights_headlines(db)["items"] == []
        prepared = adapter.normalize_news([news_row()], observed_at=NOW)
        monkeypatch.setattr(insights, "get_general_news", lambda **kwargs: prepared)
        result = insights.refresh_insights_headlines(db)
        assert result["source"] == "finnhub" and result["provider_observed_at"] == NOW.isoformat()
        assert insights.get_insights_headlines(db)["items"][0]["site"] == "Publisher"


def test_selected_news_jobs_survive_fmp_shutdown(monkeypatch):
    from app.services.data_enrichment_queue import _disabled_fmp_content_job
    monkeypatch.setenv("FMP_PROVIDER_DISABLED", "1")
    monkeypatch.setenv("NEWS_PROVIDER", "finnhub")
    assert not _disabled_fmp_content_job("news_general") and not _disabled_fmp_content_job("news_stock")
    monkeypatch.setenv("NEWS_PROVIDER", "fmp")
    assert _disabled_fmp_content_job("news_general")


@pytest.mark.parametrize("body,reason", [(b'{"error":"secret"}', "invalid_response"),
    (b'not-json', "invalid_response"), (b'[{"symbol":"AAPL"},null]', "invalid_response"),
    (b'x' * (adapter.MAX_BYTES + 1), "response_too_large")], ids=["error-object", "invalid-json", "invalid-row", "oversized"])
def test_provider_body_validation_and_size_limits(monkeypatch, body, reason):
    class Response:
        status_code = 200
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def iter_content(self, size): yield body
    monkeypatch.setenv("FINNHUB_API_KEY", "private-test-key")
    monkeypatch.setattr(adapter.requests, "get", lambda *args, **kwargs: Response())
    with pytest.raises(adapter.FinnhubUnavailable, match=reason):
        adapter.fetch_news()


def test_successful_transport_contract(monkeypatch):
    class Response:
        status_code = 200
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def iter_content(self, size): yield json.dumps([rating_row()]).encode()
    monkeypatch.setenv("FINNHUB_API_KEY", "private-test-key")
    monkeypatch.setattr(adapter.requests, "get", lambda *args, **kwargs: Response())
    assert adapter.fetch_recommendations("AAPL")["current"]["total"] == 10


def test_empty_ticker_never_fetches_general_feed(sessions, monkeypatch):
    monkeypatch.setattr(cache, "fetch_news", forbidden)
    assert fmp_news.get_stock_news(symbol="")["reason"] == "invalid_symbol"


def test_category_jobs_do_not_collide(monkeypatch):
    from app.services import data_enrichment_queue as queue
    queued = []
    monkeypatch.setattr(queue, "enqueue_data_enrichment_job", lambda **kwargs: queued.append(kwargs))
    cache._enqueue(None, "crypto")
    cache._enqueue(None, "currencies")
    cache._enqueue(None, "us-macro")
    assert [item["window_key"] for item in queued] == ["finnhub:crypto", "finnhub:forex", "finnhub:general"]


def test_digest_selected_news_is_cache_only_and_respects_preferences(sessions, monkeypatch):
    from app.services import email_digests as digests
    from app.models import NotificationSubscription, Watchlist
    monkeypatch.setattr(cache, "fetch_news", forbidden)
    monkeypatch.setattr(cache, "_enqueue", forbidden)
    monkeypatch.setattr(digests, "_watchlist_symbols", lambda *args: ["AAPL", "MSFT"])
    monkeypatch.setattr(digests, "get_stock_news", forbidden)
    monkeypatch.setattr(digests, "get_press_releases", lambda **kwargs: {"items": []})
    monkeypatch.setenv("PRESS_RELEASE_PROVIDER", "fmp")
    subscription = NotificationSubscription(active=True, source_payload_json=json.dumps({"watchlist_news_enabled": True}))
    watchlist = Watchlist(id=1)
    with sessions() as db:
        for symbol in ["AAPL", "MSFT"]:
            payload = adapter.normalize_news([news_row(related=symbol)], observed_at=NOW, symbol=symbol)
            db.add(InsightsSnapshot(kind=f"finnhub-news:company:{symbol}", source="finnhub", fetched_at=NOW, payload_json=json.dumps(payload)))
        db.commit()
        result = digests._watchlist_market_news_items(db, watchlist, since=NOW - timedelta(days=1), subscription=subscription)
        assert len(result) == 1 and result[0]["site"] == "Publisher"
        subscription.active = False
        assert digests._watchlist_market_news_items(db, watchlist, since=NOW - timedelta(days=1), subscription=subscription) == []


def test_generic_insights_reader_cannot_bypass_source_filter(sessions):
    with sessions() as db:
        db.add(InsightsSnapshot(kind=insights.INSIGHTS_HEADLINES_KIND, source="fmp", fetched_at=NOW,
            payload_json=json.dumps({"items": [{"title": "Old vendor"}]})))
        db.commit()
        assert insights.get_insights_snapshot(db, kind=insights.INSIGHTS_HEADLINES_KIND)["items"] == []
