import pytest

from app.jobs.correct_nvda_ownership_brief import corrected_payload, DRAFT_ID, SLUG


def test_correction_preserves_access_url_and_publication_state():
    original = {"id": DRAFT_ID, "status": "published", "published_at": "2026-09-13",
                "article": {"slug": SLUG, "premium_required": False, "required_plan": None}}
    result = corrected_payload(original, "2026-09-27")
    assert result["status"] == "published" and result["published_at"] == original["published_at"]
    assert result["article"]["slug"] == SLUG
    assert result["article"]["premium_required"] is False
    assert result["article"]["required_plan"] is None
    assert "32,198,580" in result["article"]["summary"]
    assert len(result["article"]["source_links"]) == 9
    assert "updated_at" not in original


def test_correction_cannot_modify_a_different_brief():
    with pytest.raises(ValueError):
        corrected_payload({"id": "other", "status": "published", "article": {"slug": SLUG}}, "2026-09-27")
