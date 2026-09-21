from datetime import datetime, timedelta, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy.pool import StaticPool

from app.db import Base
from app.models import ResearchEvidenceEvent, ResearchSourceDocument, ResearchSourceCoverage, ResearchThesis, Security, UserAccount, Watchlist, WatchlistItem
from app.services import operational_intelligence
from app.services.operational_intelligence import candidate_securities, ticker_operational_intelligence
from app.services.research_evidence import validate_event


def make_db():
    engine = create_engine("sqlite+pysqlite:///:memory:", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    Base.metadata.create_all(engine, tables=[UserAccount.__table__, Security.__table__, Watchlist.__table__, WatchlistItem.__table__, ResearchThesis.__table__, ResearchSourceDocument.__table__, ResearchEvidenceEvent.__table__, ResearchSourceCoverage.__table__])
    return Session(engine), engine


def event(db, security, *, event_type, headline, watch_item=None, direction=None):
    row = ResearchEvidenceEvent(
        id=f"event-{db.query(ResearchEvidenceEvent).count()}", security_id=security.id, event_type=event_type,
        category="management_guidance", subject=security.name, metric="guidance", direction=direction or ("negative" if event_type == "guidance_lowered" else "positive"),
        source_type="press_release", source_provider="fmp", source_id=f"source-{db.query(ResearchEvidenceEvent).count()}",
        headline=headline, summary=f"{headline} summary", watch_item=watch_item, confidence="high", materiality="high",
        extraction_method="semantic", schema_version="evidence-v2", processing_version="operational-v1",
        content_hash=f"hash-{db.query(ResearchEvidenceEvent).count()}", published_at=datetime.now(timezone.utc),
    )
    db.add(row)


def test_operational_taxonomy_and_watch_item_are_validated():
    value = validate_event({
        "security_id": 1, "event_type": "guidance_lowered", "category": "management_guidance", "subject": "Company",
        "metric": "guidance", "direction": "negative", "source_type": "earnings_transcript", "source_provider": "fmp",
        "source_id": "call-1", "headline": "Guidance lowered", "summary": "Management lowered guidance.",
        "source_document_id": "doc-1", "evidence_excerpt": "Management lowered guidance.", "watch_item": "Next quarterly guidance",
        "confidence": "high", "materiality": "high", "extraction_method": "semantic",
    })
    assert value["event_type"] == "guidance_lowered"
    assert value["watch_item"] == "Next quarterly guidance"


def test_rotation_moves_attempted_thesis_behind_unchecked_watchlist():
    db, engine = make_db()
    try:
        first = Security(symbol="MU", name="Micron", asset_class="Equity")
        second = Security(symbol="NVDA", name="Nvidia", asset_class="Equity")
        user = UserAccount(email="rotation@example.test")
        watchlist = Watchlist(name="Research")
        db.add_all([first, second, user, watchlist]); db.commit()
        db.add(ResearchThesis(id="rotation-thesis", user_id=user.id, security_id=first.id, ticker_at_creation="MU", title="Thesis", summary="Summary", orientation="bullish", status="active", source_type="custom"))
        db.add(WatchlistItem(watchlist_id=watchlist.id, security_id=second.id, target_type="ticker", target_value="NVDA", target_label="NVDA"))
        db.commit()
        assert candidate_securities(db, limit=1)[0].id == first.id
        db.add(ResearchSourceCoverage(security_id=first.id, source_type="news_article", status="unavailable", last_attempt_at=datetime.now(timezone.utc)))
        db.commit()
        assert candidate_securities(db, limit=1)[0].id == second.id
    finally:
        db.close(); engine.dispose()


def test_coverage_distinguishes_unchecked_stale_and_disabled_and_hides_superseded(monkeypatch):
    monkeypatch.setenv("RESEARCH_TRANSCRIPT_ANALYSIS_ENABLED", "false")
    db, engine = make_db()
    try:
        security = Security(symbol="MU", name="Micron", asset_class="Equity")
        db.add(security); db.commit()
        event(db, security, event_type="product_launch", headline="Old launch", watch_item="Ramp next quarter")
        row = db.query(ResearchEvidenceEvent).one(); row.superseded_at = datetime.now(timezone.utc)
        db.add(ResearchSourceCoverage(security_id=security.id, source_type="news_article", status="ready", last_success_at=datetime.now(timezone.utc)-timedelta(days=2)))
        db.commit()
        result = ticker_operational_intelligence(db, security=security)
        assert [x["status"] for x in result["coverage"]] == ["stale", "not_checked", "disabled"]
        assert result["opportunities"] == result["catalysts"] == []
    finally:
        db.close(); engine.dispose()


def test_prepared_ticker_signal_summary_is_source_linked_and_bucketed():
    db, engine = make_db()
    try:
        security = Security(symbol="MU", name="Micron", asset_class="Equity", sector="Technology")
        db.add(security); db.commit()
        event(db, security, event_type="product_launch", headline="New product launched", watch_item="Commercial ramp")
        event(db, security, event_type="guidance_lowered", headline="Guidance lowered")
        db.commit()
        result = ticker_operational_intelligence(db, security=security)
        assert result["status"] == "ok"
        assert result["catalysts"][0]["title"] == "New product launched"
        assert result["risks"][0]["title"] == "Guidance lowered"
        assert result["watch_next"][0]["title"] == "Commercial ramp"
        assert "source_url" in result["catalysts"][0]
    finally:
        db.close(); engine.dispose()


def test_candidate_security_selection_prefers_active_theses_then_watchlists():
    db, engine = make_db()
    try:
        user = UserAccount(email="owner@example.test")
        thesis_security = Security(symbol="MU", name="Micron", asset_class="Equity", sector="Technology")
        watch_security = Security(symbol="NVDA", name="Nvidia", asset_class="Equity", sector="Technology")
        watchlist = Watchlist(name="Primary", owner_user_id=None)
        db.add_all([user, thesis_security, watch_security, watchlist]); db.commit()
        db.add(ResearchThesis(id="thesis-1", user_id=user.id, security_id=thesis_security.id, ticker_at_creation="MU", title="Thesis", summary="Summary", orientation="bullish", status="active", source_type="custom"))
        db.add(WatchlistItem(watchlist_id=watchlist.id, security_id=watch_security.id, target_type="ticker", target_value="NVDA", target_label="NVDA"))
        db.add(WatchlistItem(watchlist_id=watchlist.id, security_id=thesis_security.id, target_type="ticker", target_value="MU", target_label="MU"))
        db.commit()
        assert [row.symbol for row in candidate_securities(db, limit=2)] == ["MU", "NVDA"]
    finally:
        db.close(); engine.dispose()


def test_supply_changes_follow_source_supported_company_effect():
    db, engine = make_db()
    try:
        security = Security(symbol="MU", name="Micron", asset_class="Equity", sector="Technology")
        db.add(security); db.commit()
        event(db, security, event_type="supply_constraint", headline="Parts shortage delays production", direction="negative")
        event(db, security, event_type="supply_relief", headline="Parts available for production", direction="positive")
        event(db, security, event_type="pricing_increased", headline="Industry prices increased", direction="unknown")
        db.commit()
        result = ticker_operational_intelligence(db, security=security)
        assert result["risks"][0]["title"] == "Parts shortage delays production"
        assert result["catalysts"][0]["title"] == "Parts available for production"
        assert result["watch_next"] == []  # A neutral observation is not a future milestone.
    finally:
        db.close(); engine.dispose()


def test_source_identity_is_security_scoped_and_stable_when_title_changes():
    first = {"url": "https://example.test/news/1", "title": "Initial title"}
    amended = {**first, "title": "Corrected title"}
    assert operational_intelligence._source_id("news_article", first, security_id=1) == operational_intelligence._source_id("news_article", amended, security_id=1)
    assert operational_intelligence._source_id("news_article", first, security_id=1) != operational_intelligence._source_id("news_article", first, security_id=2)
    assert operational_intelligence._source_url("javascript:alert(1)") is None
    assert operational_intelligence._source_url("https://name:password@example.test") is None


def test_source_budget_reserves_calls_for_other_sources_and_respects_run_cap():
    total = operational_intelligence._ExtractionBudget(remaining=3)
    news = operational_intelligence._SourceBudget(parent=total, remaining=1)
    releases = operational_intelligence._SourceBudget(parent=total, remaining=5)
    assert news.consume()
    assert not news.consume()
    assert total.remaining == 2
    assert releases.consume() and releases.consume()
    assert not releases.consume()
    assert total.attempts == 3
    assert total.deferred == 2


def test_transcript_only_refresh_preserves_other_source_coverage(monkeypatch):
    db, engine = make_db()
    try:
        security = Security(symbol="MU", name="Micron", asset_class="Equity")
        db.add(security); db.commit()
        db.add(ResearchSourceCoverage(security_id=security.id, source_type="news_article", status="ready", documents_seen=12)); db.commit()
        monkeypatch.setenv("RESEARCH_TRANSCRIPT_ANALYSIS_ENABLED", "true")
        monkeypatch.setenv("RESEARCH_OPERATIONAL_INTELLIGENCE_ENABLED", "true")
        monkeypatch.setattr(operational_intelligence, "get_stock_news", lambda **_: (_ for _ in ()).throw(AssertionError("must not fetch news")))
        monkeypatch.setattr(operational_intelligence, "_ingest_latest_transcript", lambda *_, **__: {"documents": 1, "events": 0, "matches": 0, "skipped": 0})
        result = operational_intelligence.refresh_operational_intelligence(db, security_id=security.id, source_types={"earnings_transcript"})
        assert result["documents"] == 1
        news = db.get(ResearchSourceCoverage, (security.id, "news_article"))
        assert news.status == "ready" and news.documents_seen == 12
        assert db.get(ResearchSourceCoverage, (security.id, "earnings_transcript")).status == "ready"
    finally:
        db.close(); engine.dispose()


def test_processed_sources_retry_matching_without_extracting_again(monkeypatch):
    db, engine = make_db()
    try:
        security = Security(symbol="MU", name="Micron", asset_class="Equity", sector="Technology")
        db.add(security); db.commit()
        def extract_once(_db, *, document, **_kwargs):
            document.processing_status = "processed"
            _db.commit()
            return {"status": "processed", "events_written": 0}
        attempts = []
        monkeypatch.setattr(operational_intelligence, "extract_document_events", extract_once)
        monkeypatch.setattr(operational_intelligence, "_match_document_events", lambda *_args: (attempts.append(1) or len(attempts) - 1))
        item = {"title": "Micron launches a product", "summary": "The company announced a new product.", "url": "https://example.test/release"}
        operational_intelligence._ingest_article(db, security=security, item=item, document_type="press_release")
        monkeypatch.setattr(operational_intelligence, "extract_document_events", lambda **_kwargs: (_ for _ in ()).throw(AssertionError("must reuse extraction")))
        assert operational_intelligence._ingest_article(db, security=security, item=item, document_type="press_release")["matches"] == 1
    finally:
        db.close(); engine.dispose()


def test_duplicate_transcript_quarters_and_invalid_length_config_are_safe(monkeypatch):
    db, engine = make_db()
    try:
        security = Security(symbol="MU", name="Micron", asset_class="Equity", sector="Technology")
        db.add(security); db.commit()
        monkeypatch.setenv("RESEARCH_TRANSCRIPT_ANALYSIS_ENABLED", "true")
        monkeypatch.setenv("RESEARCH_TRANSCRIPT_MAX_CHARS", "invalid")
        monkeypatch.setattr(operational_intelligence, "_fmp_rows", lambda endpoint, params: [{"year": 2026, "quarter": 3, "date": "2026-09-01"}, {"year": 2026, "quarter": 3, "date": "2026-09-02"}] if endpoint.endswith("dates") else [{"content": "Source transcript. " * 20}])
        monkeypatch.setattr(operational_intelligence, "extract_document_events", lambda *_args, **_kwargs: {"status": "processed", "events_written": 0})
        monkeypatch.setattr(operational_intelligence, "_match_document_events", lambda *_args: 0)
        assert operational_intelligence._ingest_latest_transcript(db, security=security)["documents"] == 1
    finally:
        db.close(); engine.dispose()


def test_refresh_bounds_new_extractions_and_counts_failures_against_budget(monkeypatch):
    db, engine = make_db()
    try:
        security = Security(symbol="MU", name="Micron", asset_class="Equity", sector="Technology")
        db.add(security); db.commit()
        monkeypatch.setenv("RESEARCH_OPERATIONAL_MAX_EXTRACTIONS_PER_RUN", "1")
        monkeypatch.setenv("RESEARCH_TRANSCRIPT_ANALYSIS_ENABLED", "false")
        monkeypatch.setenv("RESEARCH_OPERATIONAL_INTELLIGENCE_ENABLED", "true")
        monkeypatch.setattr(operational_intelligence, "get_stock_news", lambda **_kwargs: {"status": "ok", "items": [{"title": f"Company operational development {i}", "url": f"https://example.test/{i}"} for i in range(3)]})
        monkeypatch.setattr(operational_intelligence, "get_press_releases", lambda **_kwargs: {"status": "empty", "items": []})
        calls = []
        def failed_extraction(*_args, **_kwargs):
            if not _kwargs["consume_call"]():
                return {"status": "deferred", "events_written": 0}
            calls.append(1)
            raise RuntimeError("provider error")
        monkeypatch.setattr(operational_intelligence, "extract_document_events", failed_extraction)
        result = operational_intelligence.refresh_operational_intelligence(db, security_id=security.id)
        assert result["extraction_attempts"] == 1
        assert result["extractions_deferred"] == 2
        assert len(calls) == 1
    finally:
        db.close(); engine.dispose()


def test_changed_operational_source_is_extracted_once_not_per_ticker_view(monkeypatch):
    db, engine = make_db()
    try:
        security = Security(symbol="MU", name="Micron", asset_class="Equity", sector="Technology")
        db.add(security); db.commit()
        calls = []
        def extract_once(_db, *, document, **_kwargs):
            calls.append(1)
            document.processing_status = "processed"
            _db.commit()
            return {"status": "processed", "events_written": 1}
        monkeypatch.setattr(operational_intelligence, "extract_document_events", extract_once)
        monkeypatch.setattr(operational_intelligence, "_match_document_events", lambda *_args, **_kwargs: 0)
        item = {"title": "Micron launches a product", "summary": "The company announced a new product.", "url": "https://example.test/release", "published_at": "2026-09-20T12:00:00Z"}
        first = operational_intelligence._ingest_article(db, security=security, item=item, document_type="press_release")
        second = operational_intelligence._ingest_article(db, security=security, item=item, document_type="press_release")
        assert first["documents"] == 1 and first["events"] == 1
        assert second["skipped"] == 1 and calls == [1]
    finally:
        db.close(); engine.dispose()
