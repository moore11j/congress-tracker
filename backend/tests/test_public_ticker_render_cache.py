from __future__ import annotations

import pytest
from fastapi import HTTPException
from starlette.requests import Request
import app.main as main


def request(query=b''):
    return Request({'type': 'http', 'method': 'GET', 'path': '/api/tickers/NVDA/context-bundle',
                    'query_string': query, 'headers': [], 'scheme': 'https', 'server': ('test', 443)})


def test_public_cache_miss_never_starts_research_build(monkeypatch):
    monkeypatch.setattr(main, '_ticker_context_bundle_cached_for_segment', lambda *a, **k: None)
    monkeypatch.setattr(main, '_build_ticker_context_bundle', lambda **k: pytest.fail('public cache read must not build research'))
    with pytest.raises(HTTPException) as raised:
        main.ticker_context_bundle(request(), 'NVDA', side='all', limit=3, lookback_days=365, cached_only=True, db=object())
    assert raised.value.status_code == 503
    assert raised.value.detail == 'public_context_cache_miss'
    assert raised.value.headers['Retry-After'] == '60'


def test_public_cache_hit_is_projected_for_anonymous_entitlements(monkeypatch):
    monkeypatch.setattr(main, '_ticker_context_bundle_cached_for_segment', lambda *a, **k: {'private': 'must be projected'})
    projected = {'ticker': {'symbol': 'NVDA'}, 'public': True}
    monkeypatch.setattr(main, '_project_ticker_context_bundle_for_entitlements', lambda *a, **k: projected)
    monkeypatch.setattr(main, '_build_ticker_context_bundle', lambda **k: pytest.fail('cache hit must not build'))
    assert main.ticker_context_bundle(request(), 'NVDA', side='all', limit=3, lookback_days=365, cached_only=True, db=object()) == projected


def test_active_request_preserves_existing_build_path(monkeypatch):
    monkeypatch.setattr(main, '_is_inactive_logged_out_api_request', lambda r: False)
    monkeypatch.setattr(main, '_is_direct_context_bundle_cached_only_request', lambda r: False)
    monkeypatch.setattr(main, '_build_ticker_context_bundle', lambda **k: {'live': True})
    assert main.ticker_context_bundle(request(), 'NVDA', side='all', limit=3, lookback_days=365, cached_only=False, db=object()) == {'live': True}


def test_public_and_active_requests_do_not_share_response_cache_key():
    public = main._normalized_ticker_context_bundle_public_query(request(b'cached_only=true'))
    active = main._normalized_ticker_context_bundle_public_query(request())
    assert public != active
    assert dict(public)['cached_only'] == '1'
