from __future__ import annotations

from fastapi import Request

import app.main as main_module


def _request() -> Request:
    return Request(
        {
            "type": "http",
            "method": "GET",
            "path": "/api/events",
            "headers": [(b"user-agent", b"Mozilla/5.0")],
            "query_string": b"limit=25&page_size=25&enrich_prices=1",
            "server": ("testserver", 80),
            "scheme": "http",
            "client": ("127.0.0.1", 12345),
        }
    )


def test_public_events_cache_key_changes_with_feed_epoch(monkeypatch) -> None:
    monkeypatch.setattr(main_module, "current_feed_events_epoch", lambda: "1")
    first = main_module._public_get_cache_key(_request())

    monkeypatch.setattr(main_module, "current_feed_events_epoch", lambda: "2")
    second = main_module._public_get_cache_key(_request())

    assert first is not None
    assert second is not None
    assert first != second


def test_explicit_feed_sort_cannot_bypass_entitlement_check_via_public_cache():
    for query in (b"sort_by=amount", b"sort_dir=asc"):
        request = _request()
        request.scope["query_string"] = query
        assert main_module._public_get_cache_key(request) is None
