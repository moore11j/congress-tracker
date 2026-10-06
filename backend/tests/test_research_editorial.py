from copy import deepcopy
from datetime import date
import json

from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from app.db import Base
from app.models import InstitutionalPositionChange, InstitutionalSymbolSummary, UserAccount

from app.services import research_briefs as briefs
from app.services.research_editorial import record_edits, editing_examples, topic_family


def ownership_context():
    return {"primary": {"identity": {"symbol": "NVDA", "company_name": "NVIDIA"}, "institutional_ownership_detail": {
        "verification": "sec_matched_share_pairs_v1", "comparisons": [{"holder_name": "fixture"}],
        "reporting_period": "Q2 2026", "accumulator_ranking_basis": "shares added",
        "top_accumulators": [{"holder_name": name, "shares_delta": shares, "change_type": "increase", "filing_date": "2026-08-14"}
            for name, shares in [("T. Rowe Price Associates Inc /MD/", 3000), ("BlackRock, Inc.", 2000), ("Vanguard Group Inc", 1000)]]}},
        "research_question": "Who is buying NVDA stock in the latest SEC filings?"}


def test_edit_examples_capture_changes_late_in_sections_and_ignore_metadata():
    draft = {"status": "draft"}
    before = {"title": "Old title", "source_links": [{"url": "https://example.com/old"}],
              "sections": [{"key": "buyers", "body_markdown": "intro " * 500 + "Price t Rowe added shares."}]}
    after = deepcopy(before)
    after["sections"][0]["body_markdown"] = "intro " * 500 + "T. Rowe Price added 3,000 shares."
    after["source_links"] = [{"url": "https://example.com/new"}]
    record_edits(draft, before, after, "2026-09-30")
    assert any("T. Rowe Price" in e["after"] for e in draft["editorial_edits"])
    assert all(e["field"].startswith("section:") for e in draft["editorial_edits"])
    saved = deepcopy(draft)
    record_edits(draft, after, after, "2026-10-01")
    assert draft == saved
    assert editing_examples([{**draft, "status": "rejected"}]) == []


def test_generic_buyer_answer_fails_and_named_top_three_pass():
    context = ownership_context()
    article = {"title": context["research_question"], "sections": [{"body_markdown": "A broad mix of managers increased their positions in Q2 2026."}]}
    assert briefs._ownership_answer_warnings(article, context)[0]["code"] == "named_buyers_missing"
    article["sections"][0]["body_markdown"] = "T. Rowe Price added 3,000 shares.\nBlackRock added 2,000 shares.\nVanguard Group added 1,000 shares."
    assert briefs._ownership_answer_warnings(article, context) == []
    article["sections"][0]["body_markdown"] = "T. Rowe Price added shares.\nBlackRock added shares.\nVanguard Group added shares."
    assert briefs._ownership_answer_warnings(article, context)[0]["code"] == "buyer_figures_missing"


def test_fewer_buyers_does_not_require_fabricating_a_third():
    context = ownership_context()
    context["primary"]["institutional_ownership_detail"]["top_accumulators"] = context["primary"]["institutional_ownership_detail"]["top_accumulators"][:1]
    article = {"title": context["research_question"], "sections": [{"body_markdown": "T. Rowe Price added 3,000 shares."}]}
    assert briefs._ownership_answer_warnings(article, context) == []


def test_buyer_evidence_and_edit_examples_survive_large_context_and_revision():
    context = ownership_context()
    context["external_research"] = {"irrelevant": "x" * 30000}
    context["editorial_edit_examples"] = [{"before": "Managers bought", "after": "Name the buyers"}]
    config = {"ticker": "NVDA", "research_question": context["research_question"]}
    for prompt in (briefs._prompt(config, context), briefs._revision_prompt(config, {}, "Name buyers", context)):
        assert '"shares_delta": 3000' in prompt
        assert "T. Rowe Price" in prompt and "Name the buyers" in prompt
        assert "not current factual evidence" in prompt
    assert "institutional_ownership_detail" in briefs._revision_fact_packet(context)["research_packet"]


def test_families_do_not_treat_congress_or_insiders_as_institutional_buyers():
    assert topic_family("Who is buying NVDA stock in latest SEC filings?") == "institutions"
    assert topic_family("Which insiders are buying NVDA stock?") == "insiders"
    assert topic_family("Which Congress members bought NVDA?") == "congress"


def test_unverified_change_rows_and_cached_ranking_are_not_editorial_evidence():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine) as db:
        db.add(InstitutionalSymbolSummary(symbol="NVDA", normalized_symbol="NVDA", report_year=2026, report_quarter=2,
            top_accumulators_json=json.dumps([{"holder_name": "Price T Rowe", "value_delta_usd": 999999}]),
            top_reducers_json="[]"))
        for cik, holder, shares, value in [("1", "PRICE T ROWE", 100, 999999), ("2", "BLACKROCK", 300, 100), ("3", "VANGUARD", 200, 200)]:
            db.add(InstitutionalPositionChange(cik=cik, holder_name=holder, normalized_symbol="NVDA",
                report_year=2026, report_quarter=2, filing_date=date(2026, 8, 14), change_type="increase",
                shares_delta=shares, value_delta_usd=value))
        db.commit()
        detail = briefs._institutional_ownership_detail(db, "NVDA")
        assert detail["top_accumulators"] == []
        assert detail["comparisons"] == []
        assert detail["verified_holder_count"] == 0
    engine.dispose()


def test_saved_manual_edit_reaches_future_prompt_without_reusing_article_facts(monkeypatch):
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine) as db:
        admin = UserAccount(email="editor@example.com", role="admin")
        db.add(admin)
        db.commit()
        context = ownership_context()
        draft = {"id": "edited", "status": "draft", "primary_ticker": "NVDA", "created_by": admin.id,
                 "updated_at": "2026-09-30", "config": {"ticker": "NVDA", "research_question": context["research_question"]}, "research_context": context,
                 "article": {"title": "Who is buying NVDA?", "summary": "A mix of managers bought NVDA.", "sections": []}}
        briefs._upsert_db_draft(db, draft)
        monkeypatch.setattr(briefs, "validate_article", lambda *a, **kw: {"status": "passed"})
        briefs.update_draft(admin, "edited", {"summary": "Name each buyer and quantify its position change."}, db=db)
        context["editorial_edit_examples"] = briefs._recent_editorial_examples(db)
        assert context["editorial_edit_examples"]
        assert "Name each buyer" in briefs._prompt({"ticker": "NVDA"}, context)
        assert briefs._db_draft(db, "edited")["editorial_edits"]
    engine.dispose()


def test_edit_history_is_never_in_public_article_payload():
    context = ownership_context()
    context["editorial_edit_examples"] = [{"before": "Private draft copy", "after": "Edited copy"}]
    draft = {"article": {"title": "Published", "sections": []}, "research_context": context,
             "editorial_edits": [{"before": "Private draft copy", "after": "Edited copy"}]}
    public = briefs._research_payload_for_entitlements(draft, None)
    assert "editorial_edits" not in public
    assert "editorial_edit_examples" not in (public.get("research_context") or {})
    assert draft["editorial_edits"]


def test_editorial_contract_and_real_navigation_survive_revision(monkeypatch):
    monkeypatch.setattr(briefs, "_db_drafts", lambda *a, **kw: [])
    context = ownership_context()
    context["walnut_site_context"] = briefs.retrieve_walnut_site_context(
        None, symbol="NVDA", target_keyword="nvidia institutional ownership", search_intent="Who holds shares?")
    config = {"ticker": "NVDA", "target_keyword": "nvidia institutional ownership"}
    first = briefs._prompt(config, context)
    revision = briefs._revision_prompt(config, {"title": "Who owns NVIDIA?"}, "Improve the opening", context)
    for prompt in (first, revision):
        assert "EDITORIAL STORY CONTRACT" in prompt
        assert "Distinguish owning shares from adding shares" in prompt
        assert "https://app.walnutmarkets.com/ticker/NVDA#ownership" in prompt
        assert "https://app.walnutmarkets.com/ticker/NVDA#research" in prompt
        assert "T. Rowe Price" in prompt and "3000" in prompt
        assert "Never withhold the answer for a click" in prompt


def test_sol_cost_estimate_uses_current_standard_rates(monkeypatch):
    from app.services import ai_marketing
    for key in (ai_marketing.OPENAI_INPUT_USD_PER_1M, ai_marketing.OPENAI_CACHED_INPUT_USD_PER_1M,
                ai_marketing.OPENAI_OUTPUT_USD_PER_1M):
        monkeypatch.delenv(key, raising=False)
    estimate = ai_marketing._estimate_openai_response_cost("gpt-6.1-sol", {
        "usage": {"input_tokens": 12000, "output_tokens": 3000}})
    assert estimate["cost_usd"] == 0.054
