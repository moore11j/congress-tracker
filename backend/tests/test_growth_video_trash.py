from copy import deepcopy
from types import SimpleNamespace

import pytest
from fastapi import HTTPException
from sqlalchemy import text

from test_growth_video import db
from test_growth_video_automation import ready, Buffer
from app.routers import growth_video as api
from app.services import growth_buffer as buffer, growth_video_store as store, growth_daily_video as daily


def test_delete_hides_only_selected_version_and_restore_preserves_asset(db, monkeypatch):
    item = ready(db, monkeypatch)
    other = daily.create_job(db, item["payload"]["research_source"], 1, parent=item)
    original = deepcopy(item["payload"]["video"])
    user = SimpleNamespace(id=1)
    api.delete_video(item["id"], user=user, db=db)
    api.delete_video(item["id"], user=user, db=db)  # Repeated delete is harmless.
    state = api.state(db=db)
    assert item["id"] not in {j["id"] for j in state["jobs"]}
    assert other["id"] in {j["id"] for j in state["jobs"]}
    assert {j["id"] for j in state["deleted_jobs"]} == {item["id"]}
    assert "lease_token" not in state["deleted_jobs"][0]
    assert store.job(db, item["id"])["payload"]["video"] == original
    api.restore_video(item["id"], user=user, db=db)
    api.restore_video(item["id"], user=user, db=db)
    restored = store.job(db, item["id"])
    assert restored["status"] == "READY_FOR_REVIEW"
    assert restored["payload"]["video"] == original
    assert not api.state(db=db)["deleted_jobs"]
    assert not buffer.publications(db)


@pytest.mark.parametrize("status,lease", [("CAPTURE_PENDING", None), ("RENDERING", None), ("BUDGET_WAITING", None), ("FAILED", "worker")])
def test_delete_does_not_interrupt_worker(db, monkeypatch, status, lease):
    item = ready(db, monkeypatch)
    db.execute(text("UPDATE growth_video_jobs SET status=:status,lease_token=:lease WHERE id=:id"),
               {"status": status, "lease": lease, "id": item["id"]})
    db.commit()
    with pytest.raises(HTTPException, match="processing"):
        store.trash_job(db, item["id"], 1)
    assert store.job(db, item["id"])["status"] == status


@pytest.mark.parametrize("status", ["QUEUED", "SUBMITTING", "SUBMITTED", "SCHEDULED", "PUBLISHED", "FAILED"])
def test_publication_history_and_delivery_asset_cannot_be_deleted(db, monkeypatch, status):
    item = ready(db, monkeypatch)
    buffer.approve_publish(db, item, 1, ["instagram"], "caption", client=Buffer())
    db.execute(text("UPDATE growth_video_publications SET status=:status"), {"status": status})
    db.commit()
    with pytest.raises(HTTPException, match="Publishing has started"):
        store.trash_job(db, item["id"], 1)
    assert store.job(db, item["id"])["status"] == "APPROVED"
    assert buffer.publications(db, item["id"])[0]["status"] == status


def test_deleted_draft_rejects_publish_and_revision_actions(db, monkeypatch):
    item = ready(db, monkeypatch)
    store.trash_job(db, item["id"], 1)
    with pytest.raises(ValueError, match="completed video"):
        buffer.approve_publish(db, store.job(db, item["id"]), 1, ["instagram"], "caption", client=Buffer())
    with pytest.raises(HTTPException, match="Restore"):
        api.decision(item["id"], api.Decision(action="regenerate"), user=SimpleNamespace(id=1), db=db)
    assert not buffer.publications(db)


def test_publish_cannot_revive_stale_draft_after_delete(db, monkeypatch):
    item = ready(db, monkeypatch)
    store.trash_job(db, item["id"], 1)
    with pytest.raises(ValueError, match="changed during review"):
        buffer.approve_publish(db, item, 1, ["instagram"], "caption", client=Buffer())
    assert store.job(db, item["id"])["status"] == "DELETED"
    assert not buffer.publications(db)


def test_stale_delete_cannot_overwrite_publishing_approval(db, monkeypatch):
    item = ready(db, monkeypatch)
    publications = buffer.publications
    def approve_before_delete(db, job_id):
        monkeypatch.setattr(buffer, "publications", publications)
        buffer.approve_publish(db, deepcopy(item), 1, ["instagram"], "caption", client=Buffer())
        return []  # Simulate a read immediately before concurrent approval.
    monkeypatch.setattr(buffer, "publications", approve_before_delete)
    with pytest.raises(HTTPException, match="publishing started"):
        store.trash_job(db, item["id"], 1)
    assert store.job(db, item["id"])["status"] == "APPROVED"
    assert len(buffer.publications(db)) == 1
