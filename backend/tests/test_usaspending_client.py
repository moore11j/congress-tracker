from __future__ import annotations

from datetime import date

import requests

from app.clients import usaspending


class _FakeResponse:
    status_code = 200

    def __init__(self, payload):
        self._payload = payload
        self.text = ""

    def json(self):
        return self._payload


def test_award_detail_fetch_uses_supported_sort_field(monkeypatch) -> None:
    captured_payload: dict[str, object] = {}

    def _fake_post(url: str, json: dict[str, object], timeout: int):
        captured_payload.update(json)
        return _FakeResponse({"results": [], "page_metadata": {"hasNext": False}})

    monkeypatch.setattr(usaspending.requests, "post", _fake_post)

    usaspending.fetch_recipient_contract_award_details(
        start_date=date(2026, 1, 1),
        end_date=date(2026, 3, 31),
        recipient_name="Palantir Technologies Inc",
    )

    assert captured_payload.get("sort") == "Award Amount"
    assert captured_payload.get("sort") != "Action Date"


def test_spending_fetch_retries_transient_connection_errors(monkeypatch) -> None:
    attempts = {"count": 0}
    sleeps: list[float] = []

    def _fake_post(url: str, json: dict[str, object], timeout: int):
        attempts["count"] += 1
        if attempts["count"] < 3:
            raise requests.ConnectionError("Remote end closed connection without response")
        return _FakeResponse({"results": [], "page_metadata": {"hasNext": False}})

    monkeypatch.setattr(usaspending.requests, "post", _fake_post)
    monkeypatch.setattr(usaspending.time, "sleep", lambda value: sleeps.append(value))
    monkeypatch.setattr(usaspending.random, "uniform", lambda a, b: 0.0)

    payload = usaspending.fetch_recipient_contract_spending(
        start_date=date(2026, 1, 1),
        end_date=date(2026, 3, 31),
        retry_attempts=4,
        retry_backoff_base_s=0.01,
        retry_jitter_s=0.0,
    )

    assert payload["results"] == []
    assert attempts["count"] == 3
    assert len(sleeps) == 2


def test_award_detail_fetch_retries_429_then_succeeds(monkeypatch) -> None:
    attempts = {"count": 0}

    class _RateLimitedResponse:
        status_code = 429
        text = "too many requests"

        def json(self):
            return {}

    def _fake_post(url: str, json: dict[str, object], timeout: int):
        attempts["count"] += 1
        if attempts["count"] == 1:
            return _RateLimitedResponse()
        return _FakeResponse({"results": [], "page_metadata": {"hasNext": False}})

    monkeypatch.setattr(usaspending.requests, "post", _fake_post)
    monkeypatch.setattr(usaspending.time, "sleep", lambda value: None)
    monkeypatch.setattr(usaspending.random, "uniform", lambda a, b: 0.0)

    payload = usaspending.fetch_recipient_contract_award_details(
        start_date=date(2026, 1, 1),
        end_date=date(2026, 3, 31),
        recipient_name="Palantir Technologies Inc",
        retry_attempts=3,
    )

    assert payload["results"] == []
    assert attempts["count"] == 2
