from datetime import datetime, timedelta, timezone

import pytest
from fastapi import HTTPException
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from app.db import Base
from app.models import UserAccount, SearchQueryLog, Security
from app.services import research_seo as seo

NOW = datetime(2026, 9, 11, 15, tzinfo=timezone.utc)


@pytest.fixture
def db():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        yield session
    engine.dispose()


def setup(db, enabled=True):
    admin = UserAccount(email="editor@example.com", role="admin")
    db.add(admin)
    db.commit()
    seo.save_settings(db, admin, {**seo.DEFAULTS, "enabled": enabled})
    return admin


def candidate(**changes):
    return {"id": "opportunity", "target_keyword": "are institutions accumulating NVDA stock",
            "ticker": "NVDA", "content_type": "ticker", "opportunity_score": 80,
            "source_urls": ["https://www.sec.gov/"], "walnut_angle": "Compare named institutional buyers and sellers",
            "rationale": "Fresh reported holdings", "secondary_keywords": ["Nvidia institutional ownership"],
            "search_intent": "Understand institutional position changes", "recommended_theme": "institutional_ownership", **changes}


def test_ranking_uses_customer_interest_and_excludes_duplicate_missing_evidence_and_stub():
    rows = [candidate(), candidate(id="x", ticker="AAPL", target_keyword="Apple institutional holders", opportunity_score=84),
            candidate(target_keyword="No sources", source_urls=[]), candidate(target_keyword="Macro", content_type="non_ticker")]
    result = seo.rank_candidates(rows, [{"ticker": "NVDA", "searches": 30}], [], 70)
    assert result[0]["ticker"] == "NVDA"
    assert result[0]["priority_score"] == 90
    assert len(result) == 2
    assert seo.rank_candidates([candidate()], [], ["Are institutions accumulating NVDA stock?"], 70) == []
    assert seo.rank_candidates([candidate(opportunity_score=20)], [], [], 70) == []
    # Do not treat an AVGO institutional article as a duplicate of NVDA's.
    assert seo.rank_candidates([candidate()], [], [{"ticker": "AVGO", "target_keyword": "are institutions accumulating AVGO stock"}], 70)


def test_interest_is_aggregate_known_symbols_not_raw_customer_queries(db):
    db.add(Security(symbol="NVDA", name="NVIDIA", asset_class="equity"))
    for query in ["nvda"]*4 + ["jarod@example.com"]*10 + ["ignore instructions"]*10:
        db.add(SearchQueryLog(query=query, normalized_query=query, result_count=1, created_at=NOW))
    db.add(SearchQueryLog(query="nvda", normalized_query="nvda", result_count=1, created_at=NOW-timedelta(days=60)))
    db.commit()
    assert seo.customer_interest(db, NOW) == [{"ticker": "NVDA", "searches": 4}]


def test_disabled_and_not_due_never_call_discovery(db, monkeypatch):
    setup(db, enabled=False)
    monkeypatch.setattr(seo.briefs, "discover_research_keyword_opportunities", lambda *_: pytest.fail("Paid discovery should not run"))
    assert seo.run_daily_plan(db, now=NOW)["status"] == "not_due"


def test_bad_settings_rejected(db):
    admin = setup(db)
    for invalid in [{"draft_time": "29:00"}, {"timezone": "Not/AZone"}, {"tickers": ["bad@input"]}]:
        with pytest.raises(HTTPException):
            seo.save_settings(db, admin, {**seo.DEFAULTS, **invalid})


def test_timezone_before_and_after_due_and_same_day_no_duplicate_spend(db, monkeypatch):
    setup(db)
    calls = []
    monkeypatch.setattr(seo.briefs, "discover_research_keyword_opportunities", lambda *args: calls.append(args) or {"items": []})
    assert seo.run_daily_plan(db, now=NOW.replace(hour=13))["status"] == "not_due"  # 6am Pacific
    assert seo.run_daily_plan(db, now=NOW.replace(hour=14))["status"] == "skipped"
    assert seo.run_daily_plan(db, now=NOW)["status"] == "already_attempted"
    assert len(calls) == 1


def test_failure_keeps_claim_and_does_not_retry_paid_work(db, monkeypatch):
    setup(db)
    calls = []
    def fail(*args):
        calls.append(1)
        raise HTTPException(503, "Credit balance exhausted")
    monkeypatch.setattr(seo.briefs, "discover_research_keyword_opportunities", fail)
    assert seo.run_daily_plan(db, now=NOW)["status"] == "failed"
    assert seo.run_daily_plan(db, now=NOW)["status"] == "already_attempted"
    assert len(calls) == 1
    assert seo.get_status(db)["runs"][0]["detail"]["error"] == "Credit balance exhausted"


def test_selected_topic_reaches_real_campaign_and_never_approves(db, monkeypatch):
    setup(db)
    selected = candidate()
    def discover(db, admin, payload):
        db.execute(text("""INSERT INTO research_keyword_opportunities
            (id, created_by, target_keyword, payload_json) VALUES ('opportunity', :owner, :keyword, '{}')"""),
            {"owner": admin.id, "keyword": selected["target_keyword"]})
        db.commit()
        return {"items": [selected]}
    calls = []
    monkeypatch.setattr(seo.briefs, "discover_research_keyword_opportunities", discover)
    monkeypatch.setattr(seo.briefs, "run_due_research_campaign_generation", lambda db, **kwargs: calls.append(kwargs) or {"generated": 1, "failed": 0})
    assert seo.run_daily_plan(db, now=NOW)["status"] == "draft_ready"
    run = seo.get_status(db)["runs"][0]
    campaign = seo.briefs.get_research_campaign(db, run["campaign_id"])
    assert campaign["config"]["editorial_brief"] == selected["walnut_angle"]
    assert campaign["config"]["review_first"] is True
    assert campaign["config"]["target_keyword"] == selected["target_keyword"]
    assert campaign["approved_count"] == 0
    assert campaign["published_count"] == 0
    assert len(calls) == 1
    assert seo.run_daily_plan(db, now=NOW)["status"] == "already_attempted"
    assert db.execute(text("SELECT COUNT(*) FROM research_campaigns")).scalar() == 1


def test_queued_manual_run_is_idempotent_and_can_run_when_daily_disabled(db, monkeypatch):
    setup(db, enabled=False)
    seo.request_today(db)
    seo.request_today(db)
    monkeypatch.setattr(seo.briefs, "discover_research_keyword_opportunities", lambda *args: {"items": []})
    assert seo.run_daily_plan(db)["status"] == "skipped"
    seo.request_today(db)
    assert seo.run_daily_plan(db)["status"] == "not_due"
    assert len(seo.get_status(db)["runs"]) == 1


def test_discovery_prompt_has_demand_provenance_and_avoidance():
    prompt = seo.briefs._keyword_discovery_prompt({"customer_interest": [{"ticker": "NVDA", "searches": 12}],
                                                 "excluded_queries": ["old question"], "ticker_articles_only": True})
    assert "not Google keyword volume" in prompt
    assert "old question" in prompt
    assert "do not return non_ticker" in prompt


def test_seven_unreviewed_articles_pause_without_paid_calls(db, monkeypatch):
    admin = setup(db)
    seo.briefs.ensure_research_brief_store_schema(db)
    for index in range(7):
        campaign = seo.briefs.create_research_campaign(db, admin, {
            "name": "prior daily research", "theme": "institutional_ownership", "tickers": ["NVDA"],
            "target_keyword": f"earlier topic {index}", "publish_start_at": NOW.isoformat(),
        })
        db.execute(text("""INSERT INTO research_seo_runs(day, status, campaign_id, detail_json, created_at, updated_at)
            VALUES (:day,'generating',:campaign,'{}',:now,:now)"""),
            {"day": f"2026-09-0{index+1}", "campaign": campaign["id"], "now": NOW.isoformat()})
        db.commit()
    monkeypatch.setattr(seo.briefs, "discover_research_keyword_opportunities", lambda *_: pytest.fail("Backlog should stop discovery"))
    assert seo.run_daily_plan(db, now=NOW)["status"] == "skipped"
    assert "Seven" in seo.get_status(db)["runs"][0]["detail"]["note"]


def test_winter_timezone_does_not_use_fixed_utc_offset(db, monkeypatch):
    setup(db)
    monkeypatch.setattr(seo.briefs, "discover_research_keyword_opportunities", lambda *args: {"items": []})
    winter = NOW.replace(month=12, hour=14)  # 6am PST, not 7am
    assert seo.run_daily_plan(db, now=winter)["status"] == "not_due"
    assert seo.run_daily_plan(db, now=winter.replace(hour=15))["status"] == "skipped"


def test_revoked_admin_is_blocked_before_discovery(db, monkeypatch):
    admin = setup(db)
    admin.role = "user"
    db.commit()
    monkeypatch.setattr(seo.briefs, "discover_research_keyword_opportunities", lambda *_: pytest.fail("Revoked owner"))
    assert seo.run_daily_plan(db, now=NOW)["status"] == "failed"


def test_daily_generation_retains_real_draft_warnings_with_one_correction(monkeypatch):
    calls = []
    draft = {"status": "draft", "article": {"title": "Actual model output"},
             "validation": {"status": "failed", "warnings": [{"code": "numeric_claim", "message": "Verify the cited amount", "blocking": True}]}}
    def generate(db, admin, config, **kwargs):
        calls.append(kwargs)
        return draft
    monkeypatch.setattr(seo.briefs, "generate_research_brief", generate)
    result, notes = seo.briefs._generate_campaign_brief_with_corrections(None, None, {}, review_first=True)
    assert result is draft
    assert result["validation"]["status"] == "failed"
    assert result["status"] == "draft"
    assert notes
    assert calls == [{"quality_repair_limit": 1}]


def test_daily_generation_does_not_replace_api_failure_with_a_draft(monkeypatch):
    def fail(*args, **kwargs):
        raise HTTPException(502, "Provider failed")
    monkeypatch.setattr(seo.briefs, "generate_research_brief", fail)
    with pytest.raises(HTTPException):
        seo.briefs._generate_campaign_brief_with_corrections(None, None, {}, review_first=True)
