import json
import pytest
from app.jobs.correct_institutional_briefs import corrected_payload, article_digest, PATCHES


def fixture():
    article = {"slug": "ownership", "title": "Prior", "required_plan": "free"}
    original = {"id": "test", "status": "published", "published_at": "2026-09-01", "article": article}
    correction = {"id": "test", "slug": "ownership", "expected_article_sha256": article_digest(article), "patch": {"title": "Corrected"}}
    return original, correction


def test_preserves_identity_access_and_date_and_is_idempotent():
    original, patch = fixture()
    new = corrected_payload(original, patch, "2026-10-06")
    assert original["article"]["title"] == "Prior"
    assert new["published_at"] == original["published_at"]
    assert new["article"]["required_plan"] == "free"
    assert corrected_payload(new, patch, "later") == new


def test_concurrent_edit_fails_closed():
    original, patch = fixture()
    original["article"]["title"] = "Owner edited"
    with pytest.raises(ValueError, match="changed since review"):
        corrected_payload(original, patch, "now")


def test_twelve_scoped_corrections_preserve_already_reviewed_nvda():
    patches = json.loads(PATCHES.read_text(encoding="utf-8"))
    assert len(patches) == 12
    assert len({p["id"] for p in patches}) == 12
    assert not {"rb_1790949687931_fd6b96", "rb_1789221682029_a265ac", "rb_1789151207553_89bc04"}.intersection(p["id"] for p in patches)
    for item in patches:
        assert not {"slug", "required_plan", "premium_required", "published_at"}.intersection(item["patch"])
        assert item["patch"]["source_links"]
