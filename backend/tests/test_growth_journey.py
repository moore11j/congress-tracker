import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace as NS

from app.growth_journey import acquisition_journey

NOW = datetime(2026, 9, 30, tzinfo=timezone.utc)


def event(i, minutes, path, user=None, session="one", source="reddit"):
    return NS(id=i, created_at=NOW + timedelta(minutes=minutes), path=path,
        normalized_path=path, user_id=user, session_id_hash=session,
        metadata_json=json.dumps({"properties": {"acquisition_source": source}}))


def test_ordered_signup_and_separate_retention_payment_outcomes():
    rows = [event(1, -5, "/"), event(2, -2, "/ticker/NVDA"),
        event(3, 1, "/events/signup_completed", 1),
        event(4, 2, "/events/ticker_added_to_watchlist", 1),
        event(5, 1500, "/ticker/NVDA", 1, "return", "google")]
    report = acquisition_journey(rows, [NS(id=1, created_at=NOW)],
        [NS(user_id=1, charged_at=NOW + timedelta(minutes=10))], NOW - timedelta(days=1))
    row = next(row for row in report["sources"] if row["source"] == "reddit")
    assert row["new_accounts"] == row["stock_before_signup"] == row["saved_accounts"] == row["returned_accounts"] == row["paid_accounts"] == 1
    assert row["checkout_accounts"] == 0  # payment does not fabricate browser telemetry
    assert report["unattributed_new_accounts"] == 0


def test_shared_browser_and_post_signup_only_sessions_cannot_claim_acquisition():
    rows = [event(1, -5, "/", 2), event(2, 1, "/events/signup_completed", 1),
        event(3, 2, "/ticker/NVDA", 3, "late")]
    users = [NS(id=i, created_at=NOW) for i in [1, 3]]
    report = acquisition_journey(rows, users, [], NOW - timedelta(days=1), truncated=True)
    assert report["attributed_new_accounts"] == 0
    assert report["unattributed_new_accounts"] == 2
    assert report["truncated"] is True


def test_missing_session_and_unsafe_source_are_not_identity_or_source():
    rows = [event(1, -1, "/", session=None), event(2, -1, "/", source="person@example.com"), event(3, 1, "/", 1, source="person@example.com")]
    result = acquisition_journey(rows, [NS(id=1, created_at=NOW)], [], NOW - timedelta(days=1))
    assert result["sources"][0]["source"] == "unattributed"
    assert result["sources"][0]["sessions"] == 1
