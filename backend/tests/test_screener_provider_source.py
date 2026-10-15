from datetime import datetime, timezone
from unittest.mock import Mock

import pytest
import requests
from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import Session
from starlette.requests import Request

from app.db import Base
from app.entitlements import ENTITLEMENTS
from app.models import Event, FundamentalsCache, QuoteCache, TickerMeta
from app.services import screener
from app.routers import screener as router


@pytest.fixture(autouse=True)
def isolate(monkeypatch):
    monkeypatch.setenv("FMP_PERSIST_USAGE_EVENTS", "false")
    monkeypatch.setenv("SCREENER_PROVIDER_FALLBACK", "1")
    monkeypatch.setattr(screener, "enqueue_data_enrichment_job", Mock(return_value=None))
    def forbidden(*args, **kwargs):
        pytest.fail("Source test attempted HTTP")
    monkeypatch.setattr(requests.sessions.Session, "request", forbidden)


@pytest.mark.parametrize("provider", ["fmp", "sec_edgar", "free_direct"])
@pytest.mark.parametrize("tier", [None, "free", "premium"])
def test_cached_source_and_selected_values_are_consistent(monkeypatch, provider, tier):
    monkeypatch.setenv("FUNDAMENTALS_PROVIDER", provider)
    monkeypatch.setattr(screener, "fetch_company_screener", Mock(side_effect=AssertionError("Unexpected FMP fallback")))
    engine=create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    now=datetime.now(timezone.utc)
    with Session(engine) as db:
        db.add_all([TickerMeta(symbol="AAA", company_name="Alpha", sector="Technology"),
                    TickerMeta(symbol="BBB", company_name="Beta", sector="Technology"),
                    QuoteCache(symbol="AAA", price=50, asof_ts=now),
                    QuoteCache(symbol="BBB", price=60, asof_ts=now)])
        for name, pe in [("fmp", 10), ("sec_edgar", 20), ("free_direct", 30)]:
            db.add(FundamentalsCache(symbol="AAA", provider=name, fetched_at=now,
                status="ok", company_name="Alpha", price=50, sector="Technology",
                market_cap=1_000_000, trailing_pe=pe))
        db.commit()
        params=screener.ScreenerParams(page_size=10, price_min=1, price_max=100, sector="Technology")
        def read():
            if tier is None:
                return screener.build_screener_response(db, params)
            return screener.build_screener_response_for_entitlements(db, params, entitlements=ENTITLEMENTS[tier])
        result=read()
        repeated=read()
        assert result["source"] == repeated["source"] == "walnut_cached_screener"
        assert result["fundamentals_provider"] == repeated["fundamentals_provider"] == provider
        rows={row["symbol"]:row for row in result["items"]}
        assert set(rows)=={"AAA", "BBB"}
        assert result["returned"]==len(rows)==2
        assert rows["AAA"]["trailing_pe"] == {"fmp":10,"sec_edgar":20,"free_direct":30}[provider]
        assert rows["BBB"]["trailing_pe"] is None
        assert rows["BBB"]["price"]==60
        assert [(r["symbol"],r["price"],r["trailing_pe"]) for r in repeated["items"]] == [(r["symbol"],r["price"],r["trailing_pe"]) for r in result["items"]]
        assert db.scalar(select(func.count()).select_from(Event))==0
        if tier:
            assert result["access"]["tier"]==tier


@pytest.mark.parametrize("provider", ["fmp", "free_direct"])
def test_only_actual_fmp_fallback_is_labeled_fmp(monkeypatch, provider):
    monkeypatch.setenv("FUNDAMENTALS_PROVIDER", provider)
    fetch=Mock(return_value=[{"symbol":"AAA","companyName":"Alpha","price":10}])
    monkeypatch.setattr(screener,"fetch_company_screener",fetch)
    engine=create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine) as db:
        result=screener.build_screener_response(db,screener.ScreenerParams())
    assert result["fundamentals_provider"]==provider
    assert result["source"]==("fmp_company_screener" if provider=="fmp" else "walnut_cached_screener")
    assert fetch.call_count==(1 if provider=="fmp" else 0)
    assert result["returned"]==(1 if provider=="fmp" else 0)


def test_response_cache_cannot_cross_fundamentals_provider(monkeypatch):
    monkeypatch.setenv("SCREENER_RESPONSE_CACHE_TTL_SECONDS","30")
    request=Request({"type":"http","method":"GET","path":"/api/screener","query_string":b"page=1","headers":[]})
    keys=[]
    for provider in ("fmp","sec_edgar","free_direct"):
        monkeypatch.setenv("FUNDAMENTALS_PROVIDER",provider)
        keys.append(router._screener_response_cache_key(request,ENTITLEMENTS["free"]))
    assert len(set(keys))==3
    router._SCREENER_RESPONSE_CACHE.clear()
    router._SCREENER_RESPONSE_CACHE[keys[0]]=(__import__('time').time()+30,{"source":"old_fmp_response"})
    try:
        assert router._screener_response_cache_get(keys[1]) is None
        assert router._screener_response_cache_get(keys[2]) is None
        assert router._screener_response_cache_get(keys[0])=={"source":"old_fmp_response"}
    finally:
        router._SCREENER_RESPONSE_CACHE.clear()
