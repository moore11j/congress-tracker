from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db import Base
from app.models import InsiderTransactionNormalized, InstitutionalHolder, Member, Security, TickerMeta
import app.services.search_suggest as search_suggest_module
from app.services.search_suggest import search_suggestions
from app.services.universal_search import rebuild_search_entities, search_coverage_audit, search_entities


def _db():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    return Session()


def _seed_universal_search_fixture(db):
    db.add_all(
        [
            Security(symbol="NVDA", name="NVIDIA Corporation", asset_class="stock", sector="Technology"),
            Security(symbol="AAPL", name="Apple Inc.", asset_class="stock", sector="Technology"),
            TickerMeta(symbol="NVDA", company_name="NVIDIA Corporation", exchange="NASDAQ"),
            TickerMeta(symbol="AAPL", company_name="Apple Inc.", exchange="NASDAQ"),
            Member(bioguide_id="P000197", first_name="Nancy", last_name="Pelosi", chamber="house", party="D", state="CA"),
            InstitutionalHolder(
                cik="0001067983",
                holder_name="Berkshire Hathaway Inc.",
                normalized_holder_name="berkshire hathaway inc",
                holder_type="investment_manager",
                latest_report_year=2026,
                latest_report_quarter=1,
                quality_score=95,
            ),
            InsiderTransactionNormalized(
                accession_number="0000320193-26-000001",
                issuer_name="Apple Inc.",
                ticker_normalized="AAPL",
                reporting_owner_cik="0001214156",
                reporting_owner_name="COOK TIMOTHY D",
                officer_title="Chief Executive Officer",
                is_officer=True,
                transaction_date=datetime(2026, 4, 1, tzinfo=timezone.utc).date(),
                filing_date=datetime(2026, 4, 2, tzinfo=timezone.utc).date(),
                normalized_hash="tim-cook-aapl-1",
            ),
            InsiderTransactionNormalized(
                accession_number="0001045810-26-000001",
                issuer_name="NVIDIA Corporation",
                ticker_normalized="NVDA",
                reporting_owner_cik="0001249969",
                reporting_owner_name="HUANG JEN HSUN",
                officer_title="President and Chief Executive Officer",
                is_officer=True,
                transaction_date=datetime(2026, 5, 1, tzinfo=timezone.utc).date(),
                filing_date=datetime(2026, 5, 2, tzinfo=timezone.utc).date(),
                normalized_hash="jensen-huang-nvda-1",
            ),
        ]
    )
    db.commit()
    rebuild_search_entities(db)
    db.commit()
    search_suggest_module._anonymous_suggestion_cache.clear()


def test_universal_search_acceptance_queries_rank_expected_entities():
    db = _db()
    try:
        _seed_universal_search_fixture(db)

        expectations = {
            "Jensen Huang": ("insider", "Jensen Huang", "/insider/jensen-huang-0001249969?issuer=NVDA"),
            "Jen-Hsun Huang": ("insider", "Jensen Huang", "/insider/jensen-huang-0001249969?issuer=NVDA"),
            "Tim Cook": ("insider", "Tim Cook", "/insider/tim-cook-0001214156?issuer=AAPL"),
            "Timothy Cook": ("insider", "Tim Cook", "/insider/tim-cook-0001214156?issuer=AAPL"),
            "Timothy D. Cook": ("insider", "Tim Cook", "/insider/tim-cook-0001214156?issuer=AAPL"),
            "Pelosi": ("member", "Nancy Pelosi", "/member/NANCY_PELOSI"),
            "DoD": ("agency", "Department of Defense", "/departments/department-of-defense"),
        }

        for query, (kind, label, href) in expectations.items():
            items = search_suggestions(db, query, limit=8)["items"]
            assert items, query
            assert items[0]["kind"] == kind
            assert items[0]["label"] == label
            assert items[0]["href"] == href

        nvda_items = search_suggestions(db, "NVDA", limit=8)["items"]
        assert nvda_items[0]["kind"] == "ticker"
        assert nvda_items[0]["symbol"] == "NVDA"
        assert any(item["kind"] == "insider" and item["label"] == "Jensen Huang" for item in nvda_items)
    finally:
        search_suggest_module._anonymous_suggestion_cache.clear()
        db.close()


def test_universal_search_typo_tolerance_and_no_duplicates():
    db = _db()
    try:
        _seed_universal_search_fixture(db)

        typo_expectations = {
            "Tim Cok": ("insider", "Tim Cook"),
            "Jenson Huang": ("insider", "Jensen Huang"),
            "NVIDA": ("ticker", "NVIDIA Corporation"),
        }
        for query, (kind, label) in typo_expectations.items():
            items = search_suggestions(db, query, limit=8)["items"]
            assert items[0]["kind"] == kind
            assert items[0]["label"] == label
            keys = [f"{item['kind']}:{item['id']}" for item in items]
            assert len(keys) == len(set(keys))
    finally:
        search_suggest_module._anonymous_suggestion_cache.clear()
        db.close()


def test_search_coverage_audit_reports_index_counts_and_valid_urls():
    db = _db()
    try:
        _seed_universal_search_fixture(db)

        audit = search_coverage_audit(db)

        assert audit["indexed_entities"] >= 7
        assert audit["indexed_by_type"]["stock"] >= 2
        assert audit["indexed_by_type"]["member"] == 1
        assert audit["indexed_by_type"]["insider"] == 2
        assert audit["indexed_by_type"]["department"] >= 1
        assert audit["invalid_urls"] == 0
    finally:
        db.close()


def test_universal_search_direct_provider_context_queries():
    db = _db()
    try:
        _seed_universal_search_fixture(db)

        nvidia_ceo = search_entities(db, "NVIDIA CEO", limit=5)
        apple_ceo = search_entities(db, "Apple CEO", limit=5)
        berkshire = search_entities(db, "Berkshire", limit=5)

        assert nvidia_ceo[0]["kind"] == "insider"
        assert nvidia_ceo[0]["label"] == "Jensen Huang"
        assert apple_ceo[0]["kind"] == "insider"
        assert apple_ceo[0]["label"] == "Tim Cook"
        assert any(item["kind"] == "institution" and item["label"] == "Berkshire Hathaway Inc." for item in berkshire)
    finally:
        db.close()
