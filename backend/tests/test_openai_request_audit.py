from __future__ import annotations

import pytest

from app.services import openai_request_audit


class _Response:
    status_code = 200

    @staticmethod
    def json():
        return {
            "id": "resp_123",
            "usage": {"input_tokens": 12, "output_tokens": 7},
        }


def test_audit_records_response_metadata_without_prompt(monkeypatch: pytest.MonkeyPatch):
    events: list[dict[str, object]] = []
    monkeypatch.setattr(openai_request_audit, "_persist", events.append)

    response = openai_request_audit.audited_openai_request(
        feature="research_briefs",
        operation="brief_generation",
        method="POST",
        endpoint="https://api.openai.com/v1/responses",
        payload={"model": "gpt-5.6-luna", "input": "private prompt", "max_output_tokens": 6000, "tools": [{"type": "web_search"}]},
        send=_Response,
    )

    assert response.status_code == 200
    assert len(events) == 1
    event = events[0]
    assert event["model"] == "gpt-5.6-luna"
    assert event["response_id"] == "resp_123"
    assert '"input_tokens":12' in str(event["usage_json"])
    assert "private prompt" not in str(event)


def test_audit_records_request_exception(monkeypatch: pytest.MonkeyPatch):
    events: list[dict[str, object]] = []
    monkeypatch.setattr(openai_request_audit, "_persist", events.append)

    with pytest.raises(RuntimeError, match="network unavailable"):
        openai_request_audit.audited_openai_request(
            feature="ai_growth",
            operation="web_search",
            method="POST",
            endpoint="https://api.openai.com/v1/responses",
            payload={"model": "gpt-5.6-terra", "input": "private prompt"},
            send=lambda: (_ for _ in ()).throw(RuntimeError("network unavailable")),
        )

    assert len(events) == 1
    assert events[0]["succeeded"] is False
    assert "network unavailable" in str(events[0]["error"])
