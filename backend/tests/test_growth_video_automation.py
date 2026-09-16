import copy
import json
from datetime import datetime, timedelta, timezone

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient
from sqlalchemy import text

from test_growth_video import db, seed
from app.db import get_db
from app.models import AiMarketingOpportunity
from app.routers import growth_video as api
from app.services import growth_buffer as buffer, growth_daily_video as daily
from app.services import growth_video_automation as automation, growth_video_store as store
from app.services import growth_video_pipeline as pipeline, research_briefs


def source(db, monkeypatch):
    _, item = seed(db, monkeypatch)
    item["article"]["key_points"] = ["NVIDIA's reported ownership changes describe quarter-end holdings rather than live buying."]
    item["article"]["sections"] = [{"heading": "Ownership context", "body_markdown": item["article"]["key_points"][0]}]
    item["published_at"] = store.now()
    research_briefs._upsert_db_draft(db, item)
    db.commit()
    automation.ensure_schema(db)
    return item


def test_new_walkthrough_uses_ticker_research_and_legacy_stays_valid(db, monkeypatch):
    original = source(db, monkeypatch)
    item = daily.create_job(db, original, 1, feedback="Open the ticker Research tab")
    board = daily.validate(item, db)
    assert board["walkthrough_version"] == 2
    assert board["storyboard"][1]["shot"] == "daily_research"
    assert board["storyboard"][1]["walnut_url"].endswith("/ticker/NVDA")
    assert "Click Research" in board["narration"] and "Insights" not in board["narration"]
    assert "NVDA ticker → Research" in board["caption"]
    legacy = daily.creative(original, 1)
    assert "walkthrough_version" not in legacy
    item["payload"].update(creative=legacy, campaign_hash=store.digest(legacy))
    assert daily.validate(item, db) == legacy


def test_navigation_timeout_retries_only_failed_scene_with_backoff_and_bound(db, monkeypatch):
    item = daily.create_job(db, source(db, monkeypatch), 1)
    item["payload"]["captures"]["daily_search"] = {"id": "retained"}
    store.save_job(db, item)
    from test_growth_video import Storage
    calls = []
    def timeout(shot):
        calls.append(shot)
        raise TimeoutError("private provider URL must not be persisted")
    storage = Storage()
    for attempt in range(1, 4):
        expected = "CAPTURE_PENDING" if attempt < 3 else "FAILED"
        assert pipeline.advance(db, item["id"], storage=storage, capture=timeout) == expected
        current = store.job(db, item["id"])
        assert current["payload"]["captures"] == {"daily_search": {"id": "retained"}}
        assert not current["payload"]["audio"] and not current["lease_token"]
        assert current["payload"]["capture_timeout_attempts"]["daily_research"] == attempt
        assert "private provider" not in json.dumps(current)
        if attempt < 3:
            assert pipeline.advance(db, item["id"], storage=storage, capture=timeout) == expected
            assert len(calls) == attempt  # Backoff prevents hot-loop capture.
            current["payload"].pop("capture_retry_at")
            store.save_job(db, current)
    assert calls == ["daily_research"] * 3
    assert not storage.calls  # No paid provider calls or replacement assets.


def test_success_after_capture_timeout_clears_active_error(db, monkeypatch):
    item = daily.create_job(db, source(db, monkeypatch), 1)
    from test_growth_video import Storage
    def timeout(shot):
        raise TimeoutError()
    assert pipeline.advance(db, item["id"], storage=Storage(), capture=timeout) == "CAPTURE_PENDING"
    item = store.job(db, item["id"])
    item["payload"].pop("capture_retry_at")
    store.save_job(db, item)
    assert pipeline.advance(db, item["id"], storage=Storage(), capture=lambda shot: (b"clip", b"png", {})) == "CAPTURE_PENDING"
    saved = store.job(db, item["id"])["payload"]
    assert "failure_context" not in saved and "capture_retry_at" not in saved
    assert saved["capture_retry_history"]


def ready(db, monkeypatch):
    item = daily.create_job(db, source(db, monkeypatch), 1)
    item["status"] = "READY_FOR_REVIEW"
    item["payload"]["video"] = {"width": 1080, "height": 1920, "sha256": "a" * 64,
        "object_key": f"ai-growth/{item['id']}/render.mp4", "bytes": 100, "content_type": "video/mp4"}
    store.save_job(db, item)
    return item


class Buffer:
    def __init__(self, uncertain=False):
        self.calls = []
        self.uncertain = uncertain

    def channel(self, platform):
        return {"id": buffer.CHANNELS[platform], "service": platform}

    def create(self, platform, caption, url):
        self.calls.append((platform, caption, url))
        if self.uncertain:
            raise TimeoutError("request may have succeeded")
        return {"id": platform + "-post", "status": "buffer", "channelId": buffer.CHANNELS[platform], "schedulingType": "automatic"}

    def post(self, post_id):
        return {"id": post_id, "status": "sent", "externalLink": "https://www.instagram.com/p/test/", "schedulingType": "automatic"}


def test_daily_requires_new_published_source_and_is_once_per_day(db, monkeypatch):
    # Publishing after reconciliation must have a later timestamp even on
    # coarse Windows clocks; this test is about event semantics, not timing.
    import itertools
    ticks = itertools.count()
    start = datetime.now(timezone.utc)
    monkeypatch.setattr(store, "now", lambda: (start + timedelta(seconds=next(ticks))).isoformat())
    original = source(db, monkeypatch)
    assert automation.create_daily(db)["status"] == "disabled"
    automation.configure(db, 1, True)
    assert automation.create_daily(db)["status"] == "waiting_for_published_brief"
    original["published_at"] = store.now()
    research_briefs._upsert_db_draft(db, original)
    db.commit()
    result = automation.create_daily(db)
    assert result["status"] == "created"
    assert store.job(db, result["job_id"])["status"] == "CAPTURE_PENDING"
    assert automation.create_daily(db)["status"] == "already_created"
    assert db.execute(text("SELECT COUNT(*) FROM growth_video_jobs")).scalar() == 1


def test_source_change_stops_render_and_publish(db, monkeypatch):
    item = ready(db, monkeypatch)
    source = copy.deepcopy(item["payload"]["research_source"])
    source["article"]["key_points"][0] = "Different financial conclusion based on newly received research and additional source data."
    research_briefs._upsert_db_draft(db, source)
    db.commit()
    with pytest.raises(ValueError, match="changed"):
        pipeline.validate_job(db, item)
    client = Buffer()
    with pytest.raises(ValueError, match="changed"):
        buffer.approve_publish(db, item, 1, ["instagram"], "caption", client=client)
    assert not client.calls


def test_no_implicit_publishing_and_repeated_approval_is_idempotent(db, monkeypatch):
    item = ready(db, monkeypatch)
    client = Buffer()
    assert not buffer.run_pending(db, client=client)
    buffer.approve_publish(db, item, 1, ["instagram", "tiktok"], "Reviewed caption", client=client)
    assert db.get(AiMarketingOpportunity, item["draft_id"]).status == "approved"
    item = store.job(db, item["id"])
    buffer.approve_publish(db, item, 1, ["instagram", "tiktok"], "Reviewed caption", client=client)
    assert not client.calls  # Approval commits an outbox; no HTTP mutation here.
    buffer.run_pending(db, client=client)
    buffer.run_pending(db, client=client)
    assert len(client.calls) == 2
    assert all(call[2].startswith("https://congress-tracker-api.fly.dev/api/growth-video-media/") for call in client.calls)
    assert len(buffer.publications(db, item["id"])) == 2
    assert all("token" not in row for row in buffer.publications(db))
    assert all(p["status"] == "SUBMITTED" for p in buffer.publications(db))


def test_unknown_provider_outcome_is_not_replayed(db, monkeypatch):
    item = ready(db, monkeypatch)
    client = Buffer(uncertain=True)
    buffer.approve_publish(db, item, 1, ["tiktok"], "caption", client=client)
    buffer.run_pending(db, client=client)
    buffer.run_pending(db, client=client)
    assert len(client.calls) == 1
    assert buffer.publications(db)[0]["status"] == "UNCERTAIN"


def test_low_resolution_and_unapproved_assets_are_rejected(db, monkeypatch):
    item = ready(db, monkeypatch)
    item["payload"]["video"]["width"] = 270
    with pytest.raises(ValueError, match="1080"):
        buffer.approve_publish(db, item, 1, ["instagram"], "caption", client=Buffer())
    with pytest.raises(ValueError, match="unavailable"):
        buffer.media_asset(db, "unknown")


def test_ready_email_is_once_and_failed_delivery_is_not_marked_sent(db, monkeypatch):
    item = ready(db, monkeypatch)
    calls = []
    def sender(db, **kwargs):
        calls.append(kwargs)
        return {"status": "sent"}
    automation.notify_ready(db, sender=lambda db, **kwargs: {"status": "failed"})
    assert not store.job(db, item["id"])["payload"].get("notifications")
    automation.notify_ready(db, sender=sender)
    automation.notify_ready(db, sender=sender)
    assert len(calls) == 1
    assert calls[0]["idempotency_key"].endswith(":ready")
    assert "growth_tab=drafts" in calls[0]["context"]["activity_url"]


def test_media_supports_ranges_and_revocation(db, monkeypatch):
    item = ready(db, monkeypatch)
    buffer.approve_publish(db, item, 1, ["instagram"], "caption", client=Buffer())
    token = db.execute(text("SELECT token FROM growth_video_publications")).scalar()
    class Body:
        def iter_chunks(self, chunk_size): yield b"1234567890"
        def close(self): pass
    class Storage:
        bucket = "private"
        @property
        def client(self): return self
        def get_object(self, **kwargs):
            assert kwargs["Range"] == "bytes=10-19"
            return {"Body": Body()}
    monkeypatch.setattr(api, "AssetStore", Storage)
    app = FastAPI()
    app.include_router(api.public_router)
    app.dependency_overrides[get_db] = lambda: db
    client = TestClient(app)
    url = "/growth-video-media/" + token
    assert client.get("/growth-video-media/invalid").status_code == 404
    response = client.head(url)
    assert response.status_code == 200 and response.headers["content-length"] == "100"
    response = client.get(url, headers={"Range": "bytes=10-19"})
    assert response.status_code == 206 and response.headers["content-range"] == "bytes 10-19/100"
    assert response.content == b"1234567890"
    assert client.get(url, headers={"Range": "bytes=200-"}).status_code == 416
    item = store.job(db, item["id"])
    item["status"] = "REJECTED"
    store.save_job(db, item)
    assert client.head(url).status_code == 404


def test_buffer_payload_discloses_ai_and_never_adds_paid_comment(monkeypatch):
    monkeypatch.setenv("BUFFER_API_KEY", "test")
    client = buffer.BufferClient()
    calls = []
    def query(query, variables):
        calls.append(variables["input"])
        return {"createPost": {"post": {"id": "post"}}}
    monkeypatch.setattr(client, "query", query)
    client.create("instagram", "caption", "https://api.walnutmarkets.com/test.mp4")
    payload = calls[0]
    assert payload["metadata"]["instagram"] == {"type": "reel", "isAiGenerated": True, "shouldShareToFeed": True}
    assert payload["mode"] == "shareNow" and payload["schedulingType"] == "automatic"


def test_unrendered_key_points_and_known_conflicts_are_not_narrated(db, monkeypatch):
    item = source(db, monkeypatch)
    item["article"]["sections"] = []
    with pytest.raises(ValueError, match="takeaway"):
        daily.creative(item)
    item["id"] = "rb_1789151207553_89bc04"
    with pytest.raises(ValueError, match="reconciliation"):
        daily.creative(item)


def test_api_budget_stops_before_a_network_request(db, monkeypatch):
    automation.ensure_schema(db)
    window = datetime.now(timezone.utc).strftime("month:%Y-%m")
    db.execute(text("INSERT INTO growth_buffer_api_budget VALUES (:key,2800)"), {"key": window})
    db.commit()
    monkeypatch.setenv("BUFFER_API_KEY", "test")
    monkeypatch.setattr(buffer.requests, "post", lambda *a, **k: pytest.fail("Must not send an over-budget request"))
    with pytest.raises(buffer.BufferError, match="budget"):
        buffer.BufferClient(db).channel("instagram")


def test_retry_requires_explicit_no_post_confirmation(db, monkeypatch):
    item = ready(db, monkeypatch)
    client = Buffer(uncertain=True)
    buffer.approve_publish(db, item, 1, ["instagram"], "caption", client=client)
    buffer.run_pending(db, client=client)
    with pytest.raises(HTTPException):
        api.retry_publish(item["id"], api.RetryPublish(platform="instagram"), db)
    assert buffer.publications(db)[0]["status"] == "UNCERTAIN"
    api.retry_publish(item["id"], api.RetryPublish(platform="instagram", confirmed_no_buffer_post=True), db)
    assert buffer.publications(db)[0]["status"] == "QUEUED"


def test_review_email_renders_without_sending(db):
    from app.services.email_delivery import _get_template, _render_template
    template = _get_template(db, "growth.video_review")
    result = _render_template(template, {"video_title": "NVDA <research>", "video_status": "Ready",
        "video_message": "Review before publishing.", "activity_url": "https://app.walnutmarkets.com/admin/ai-marketing?growth_tab=drafts"})
    assert "NVDA &lt;research&gt;" in result["body_html"]
    assert "growth_tab=drafts" in result["body_text"]


def test_takeaway_has_context_and_hook_uses_research_question(db, monkeypatch):
    item = source(db, monkeypatch)
    item["article"]["title"] = "Which companies are winning NASA contracts?"
    item["article"]["key_points"] = []
    item["article"]["sections"] = [{"body_markdown": "Together they represent about 94% of the linked NASA value.\n\nBoeing and Lockheed Martin account for most of the tracked NASA contract value."}]
    board = daily.creative(item)
    assert board["source_excerpt"].startswith("Boeing and Lockheed")
    assert board["narration"].startswith(item["article"]["title"])


def test_render_budget_waits_without_capture_and_resumes_when_due(db, monkeypatch):
    item = daily.create_job(db, source(db, monkeypatch), 1)
    store.consume_budget(db, "renders", 1)
    db.execute(text("UPDATE growth_video_budget SET renders=999"))
    db.commit()
    assert pipeline.advance(db, item["id"], storage=object(),
        capture=lambda *_: pytest.fail("Over-budget capture must not run")) == "BUDGET_WAITING"
    waiting = store.job(db, item["id"])
    assert waiting["payload"]["resume_stage"] == "CAPTURE_PENDING"
    assert not waiting["lease_token"]
    assert not waiting["payload"].get("render_budget_reserved")
    calls = []
    monkeypatch.setattr(pipeline, "advance", lambda db, job_id: calls.append(job_id) or "CAPTURE_PENDING")
    assert pipeline.run_pending(db) == []
    waiting["payload"]["retry_at"] = "2000-01-01T00:00:00+00:00"
    store.save_job(db, waiting)
    pipeline.run_pending(db)
    resumed = store.job(db, item["id"])
    assert calls == [item["id"]]
    assert resumed["status"] == "CAPTURE_PENDING"
    assert "budget_message" not in resumed["payload"]


def test_buffer_quota_before_submission_stays_queued(db, monkeypatch):
    item = ready(db, monkeypatch)
    client = Buffer()
    buffer.approve_publish(db, item, 1, ["instagram"], "caption", client=client)
    def quota(*args):
        raise buffer.BufferQuota("API budget reached before HTTP request")
    monkeypatch.setattr(client, "create", quota)
    buffer.run_pending(db, client=client)
    assert buffer.publications(db)[0]["status"] == "QUEUED"
    assert not client.calls


def test_human_source_date_is_not_truncated(db, monkeypatch):
    item = source(db, monkeypatch)
    item["data_as_of"] = "August 31, 2026"
    assert "August 31, 2026" in json.dumps(daily.creative(item)["storyboard"])
