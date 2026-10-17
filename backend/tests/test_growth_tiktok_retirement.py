import pytest
from pydantic import ValidationError
from sqlalchemy import text

from test_growth_video import db
from test_growth_video_automation import ready, Buffer
from app.routers import growth_video as api
from app.services import growth_buffer as buffer


@pytest.mark.parametrize("model,values", [
    (api.Publish, {"platforms": ["tiktok"], "caption": "caption"}),
    (api.Publish, {"platforms": ["instagram", "tiktok"], "caption": "caption"}),
    (api.RetryPublish, {"platform": "tiktok", "confirmed_no_buffer_post": True}),
    (api.Reconcile, {"platform": "tiktok", "post_id": "old-post"}),
    (api.Generate, {"platform": "tiktok", "opportunity_id": "old-draft"}),
    (api.ProductAd, {"platform": "tiktok"}),
])
def test_retired_destination_rejected_by_api(model, values):
    with pytest.raises(ValidationError):
        model(**values)


def test_direct_publisher_rejects_tiktok_before_any_transport(db, monkeypatch):
    item = ready(db, monkeypatch)
    client = Buffer()
    with pytest.raises(ValueError, match="TikTok posting is disabled"):
        buffer.approve_publish(db, item, 1, ["instagram", "tiktok"], "caption", client=client)
    assert buffer.publications(db) == [] and client.calls == []
    monkeypatch.setenv("BUFFER_API_KEY", "test")
    client = buffer.BufferClient()
    monkeypatch.setattr(client, "query", lambda *a, **k: pytest.fail("Unexpected Buffer request"))
    with pytest.raises(buffer.BufferError, match="disabled"):
        client.channel("tiktok")
    with pytest.raises(buffer.BufferError, match="disabled"):
        client.create("tiktok", "caption", "https://example.com/video.mp4")


@pytest.mark.parametrize("status", ["QUEUED", "SUBMITTING", "SUBMITTED", "SCHEDULED", "FAILED", "UNCERTAIN", "PUBLISHED"])
def test_old_tiktok_rows_never_send_poll_or_starve_instagram(db, monkeypatch, status):
    item = ready(db, monkeypatch)
    client = Buffer()
    buffer.approve_publish(db, item, 1, ["instagram"], "caption", client=client)
    db.execute(text("""INSERT INTO growth_video_publications
        (job_id,platform,status,token,asset_hash,caption,approved_by,created_at,updated_at,post_id)
        SELECT job_id,'tiktok',:status,'old-tiktok-token',asset_hash,caption,approved_by,
        '2000-01-01','2000-01-01','old-tiktok-post' FROM growth_video_publications"""), {"status": status})
    db.commit()
    monkeypatch.setattr(client, "post", lambda *a: pytest.fail("Retired post must not be polled"))
    result = buffer.run_pending(db, client=client, limit=1)
    assert [row["platform"] for row in result] == ["instagram"]
    assert len(client.calls) == 1 and client.calls[0][0] == "instagram"
    old = next(row for row in buffer.publications(db) if row["platform"] == "tiktok")
    assert old["status"] == status and old["post_id"] == "old-tiktok-post"
    with pytest.raises(ValueError, match="Media unavailable"):
        buffer.media_asset(db, "old-tiktok-token")
