import copy
import json
from datetime import datetime, timedelta, timezone

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient
from sqlalchemy import text

from test_growth_video import db, seed
from app.db import get_db
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
    item = store.job(db, item["id"])
    buffer.approve_publish(db, item, 1, ["instagram", "tiktok"], "Reviewed caption", client=client)
    assert not client.calls  # Approval commits an outbox; no HTTP mutation here.
    buffer.run_pending(db, client=client)
    buffer.run_pending(db, client=client)
    assert len(client.calls) == 2
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
