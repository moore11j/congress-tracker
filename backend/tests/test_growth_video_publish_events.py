from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from app.services import growth_video_automation as automation


def _event_db():
    engine = create_engine("sqlite:///:memory:")
    db = Session(engine)
    db.execute(text("""CREATE TABLE research_brief_drafts (
        id TEXT PRIMARY KEY,
        status TEXT NOT NULL,
        published_at TEXT
    )"""))
    db.execute(text("""CREATE TABLE growth_video_brief_events (
        brief_id TEXT PRIMARY KEY,
        status TEXT NOT NULL,
        trigger_source TEXT NOT NULL,
        published_at TEXT,
        job_id TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        attempts INTEGER NOT NULL DEFAULT 0,
        error TEXT
    )"""))
    db.commit()
    return db


def test_publish_transition_enqueues_exactly_one_video_event():
    db = _event_db()
    try:
        assert automation._install_publish_trigger(db) == "database_trigger"
        db.execute(text("INSERT INTO research_brief_drafts(id,status) VALUES ('rb_nvda','draft')"))
        db.commit()
        assert db.execute(text("SELECT COUNT(*) FROM growth_video_brief_events")).scalar() == 0

        db.execute(text("""UPDATE research_brief_drafts
            SET status='published',published_at='2026-09-14T18:00:00+00:00'
            WHERE id='rb_nvda'"""))
        db.commit()
        row = db.execute(text("SELECT brief_id,status,trigger_source FROM growth_video_brief_events")).first()
        assert row == ("rb_nvda", "PENDING", "research_brief_publish")

        # Ordinary edits to an already-published brief must not enqueue another video.
        db.execute(text("""UPDATE research_brief_drafts
            SET published_at='2026-09-14T18:01:00+00:00' WHERE id='rb_nvda'"""))
        db.commit()
        assert db.execute(text("SELECT COUNT(*) FROM growth_video_brief_events")).scalar() == 1
    finally:
        db.close()


def test_video_review_uses_research_brief_review_recipient(monkeypatch):
    monkeypatch.setenv("RESEARCH_BRIEF_REVIEW_EMAIL", "editor@example.com")
    assert automation._review_recipient() == "editor@example.com"


def test_video_review_recipient_default(monkeypatch):
    monkeypatch.delenv("RESEARCH_BRIEF_REVIEW_EMAIL", raising=False)
    assert automation._review_recipient() == "jarod@walnutmarkets.com"
