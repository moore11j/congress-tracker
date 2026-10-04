import json

import pytest
import requests

from app.services import growth_research_direction as direction, growth_daily_video as daily


def test_selection_is_bounded_and_audited(monkeypatch):
    captured = {}
    monkeypatch.setattr(direction, "resolved_setting_value", lambda *_: "test-key")
    monkeypatch.setattr(direction, "_record_openai_usage_cost", lambda *a, **kw: None)
    class Response:
        def raise_for_status(self): pass
        def json(self):
            return {"id": "test-response", "status": "completed", "output": [{"content": [{"type": "output_text",
                "text": json.dumps({"excerpt_index": 1, "hook_style": "finding"})}]}]}
    def request(**kw):
        captured.update(kw["payload"])
        return Response()
    monkeypatch.setattr(direction, "audited_openai_request", request)
    plan, audit = direction.select_direction(None, {"article": {"title": "A supported question"}}, ["first", "second"])
    assert plan["excerpt_index"] == 1
    assert audit["response_id"] == "test-response" and audit["status"] == "completed"
    assert captured["model"] == "gpt-6.1-sol" and captured["reasoning"] == {"effort": "low"}
    assert captured["max_output_tokens"] == 900 and captured["store"] is False


@pytest.mark.parametrize("plan", [{"excerpt_index": 99, "hook_style": "finding"},
    {"excerpt_index": True, "hook_style": "title"}, {"excerpt_index": 0, "hook_style": "BUY NOW"},
    {"excerpt_index": 0, "hook_style": "title", "claim": "made up"}])
def test_model_cannot_inject_new_claims(plan):
    with pytest.raises(ValueError): direction.checked_plan(plan, ["published excerpt"])


def test_provider_failure_is_safe_and_does_not_retry(monkeypatch):
    monkeypatch.setattr(direction, "resolved_setting_value", lambda *_: "test-key")
    calls = []
    def request(**kw):
        calls.append(kw)
        raise requests.Timeout("private provider content")
    monkeypatch.setattr(direction, "audited_openai_request", request)
    plan, audit = direction.select_direction(None, {"article": {}}, ["published excerpt"])
    assert len(calls) == 1 and plan == {"excerpt_index": 0, "hook_style": "title"}
    assert audit["status"] == "fallback" and "private" not in json.dumps(audit)


def test_creative_remains_bound_to_selected_published_text():
    source = {"id": "test", "status": "published", "primary_ticker": "NVDA", "article": {
        "title": "What changed in NVIDIA's business?", "slug": "test",
        "sections": [{"body_markdown": "Reported ownership changes describe quarter-end holdings rather than live buying.\n\nThe filing record cannot establish why a manager changed its position."}]}}
    plan = {"excerpt_index": 1, "hook_style": "question"}
    board = daily.creative(source, 3, direction=plan)
    assert board["source_excerpt"] == daily.excerpt_candidates(source)[1]
    assert board["scenes"][0]["shot"] == "daily_takeaway"
    assert board["source_excerpt"] in board["scenes"][0]["narration"]
    item = {"payload": {"creative": board, "research_source": source,
        "research_source_hash": daily.source_fingerprint(source), "campaign_hash": daily.digest(board)}}
    assert daily.validate(item) == board
    board["scenes"][0]["narration"] = "Invented financial claim"
    item["payload"]["campaign_hash"] = daily.digest(board)
    with pytest.raises(ValueError, match="Daily creative changed"): daily.validate(item)
