from copy import deepcopy

import pytest

from app.jobs.correct_boeing_contract_brief import DRAFT_ID, SLUG, corrected_payload


def test_correction_preserves_publication_and_access_and_repairs_date_semantics():
    original = {"id": DRAFT_ID, "status": "published", "published_at": "2026-09-20", "article": {
        "slug": SLUG, "title": "Old title", "premium_required": False,
        "schema": {"datePublished": "2026-09-20"}}, "research_context": {"primary": {
            "government_contracts": {"recent_count": 1, "recent_award_amount": 100,
                "items": [{"source_url": "official", "award_date": "2028-12-29", "award_amount": 100}]}}}}
    saved = deepcopy(original)
    records = {"official": {"date_signed": "2021-12-30", "period_of_performance": {
        "start_date": "2028-12-29", "end_date": "2030-04-15"}}}
    updated = corrected_payload(original, {"title": "Corrected"}, records, "2026-10-04")
    assert original == saved
    assert updated["published_at"] == original["published_at"]
    assert updated["article"]["premium_required"] is False
    assert updated["article"]["schema"]["datePublished"] == "2026-09-20"
    contracts = updated["research_context"]["primary"]["government_contracts"]
    assert "recent_award_amount" not in contracts
    assert contracts["historical_snapshot_amount"] == 100
    assert contracts["items"][0]["award_date"] == "2021-12-30"
    assert contracts["items"][0]["original_stored_date"] == "2028-12-29"
    assert corrected_payload(updated, {"title": "Changed again"}, records, "later") == updated
    with pytest.raises(ValueError, match="Unverified"):
        corrected_payload(original, {"title": "Corrected"}, {}, "2026-10-04")
    with pytest.raises(ValueError, match="URL or access"):
        corrected_payload(original, {"slug": "new"}, records, "2026-10-04")
    original["id"] = "another-brief"
    with pytest.raises(ValueError, match="restricted"):
        corrected_payload(original, {"title": "Corrected"}, records, "2026-10-04")
