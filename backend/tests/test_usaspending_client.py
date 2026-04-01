from __future__ import annotations

from datetime import date

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

