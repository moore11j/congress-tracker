import pytest
from sqlalchemy import text
from test_growth_video import db
from app.services import growth_video_store as store


def test_development_drafts_are_counted_and_normal_limit_resumes_at_expiry(db, monkeypatch):
    monkeypatch.setattr(store, "now", lambda: "2026-09-13T06:59:59+00:00")
    store.set_setting(db, "GROWTH_VIDEO_DEVELOPMENT_OVERRIDE", {"expires_at": "2026-09-13T00:00:00-07:00"})
    db.commit()
    store.consume_budget(db, "creatives", 1)
    store.consume_budget(db, "creatives", 1)
    assert db.execute(text("SELECT creatives FROM growth_video_budget WHERE day='2026-09-13'")).scalar() == 2
    monkeypatch.setattr(store, "now", lambda: "2026-09-13T07:00:00+00:00")
    assert store.development_draft_override(db) is None
    with pytest.raises(store.BudgetExceeded):
        store.consume_budget(db, "creatives", 1)


@pytest.mark.parametrize("kind", ["renders", "opportunities"])
def test_draft_override_does_not_lift_other_limits(db, monkeypatch, kind):
    monkeypatch.setattr(store, "now", lambda: "2026-09-13T06:00:00+00:00")
    store.set_setting(db, "GROWTH_VIDEO_DEVELOPMENT_OVERRIDE", {"expires_at": "2026-09-13T07:00:00+00:00"})
    db.commit()
    store.consume_budget(db, kind, 1)
    with pytest.raises(store.BudgetExceeded):
        store.consume_budget(db, kind, 1)


@pytest.mark.parametrize("value", [{}, None, {"expires_at": "invalid"}, {"expires_at": "2099-01-01T00:00:00"}])
def test_invalid_override_preserves_the_limit(db, value):
    store.set_setting(db, "GROWTH_VIDEO_DEVELOPMENT_OVERRIDE", value)
    db.commit()
    assert store.development_draft_override(db) is None
    store.consume_budget(db, "creatives", 1)
    with pytest.raises(store.BudgetExceeded):
        store.consume_budget(db, "creatives", 1)
