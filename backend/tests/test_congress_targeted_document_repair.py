from __future__ import annotations

import sys
import json
from datetime import date

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from app.backfill_events_from_trades import (
    _build_backfill_id,
    _congress_event_from_transaction,
    _congress_event_payload,
)
from app.db import Base
from app.ingest_house import upsert_house_transaction_from_row
from app.models import Event, Filing, Member, Security, Transaction
from scripts.ops import backfill_missing_congress_multi_trade_events as ops


class _NoopCongressMetadata:
    def resolve(self, **_kwargs):
        return None


def _session_factory():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(
        bind=engine,
        tables=[
            Member.__table__,
            Security.__table__,
            Filing.__table__,
            Transaction.__table__,
            Event.__table__,
        ],
    )
    return sessionmaker(bind=engine, autoflush=False, autocommit=False)


def _evans_rows(document: str = "20034556") -> list[dict]:
    base = {
        "firstName": "Dwight",
        "lastName": "Evans",
        "office": "Dwight Evans",
        "district": "PA03",
        "party": "D",
        "disclosureDate": "2026-05-18",
        "link": f"https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/2026/{document}.pdf",
        "type": "sale",
        "owner": "self",
        "amount": "$1,001 - $15,000",
    }
    return [
        {**base, "symbol": "AMT", "assetDescription": "American Tower Corp", "transactionDate": "2026-05-11"},
        {**base, "symbol": "CVS", "assetDescription": "CVS Health Corp", "transactionDate": "2025-11-21"},
        {**base, "symbol": "INTC", "assetDescription": "Intel Corp", "transactionDate": "2026-05-07"},
        {**base, "symbol": "PWR", "assetDescription": "Quanta Services Inc", "transactionDate": "2026-05-07"},
        {**base, "symbol": "TMO", "assetDescription": "Thermo Fisher Scientific Inc", "transactionDate": "2026-05-07"},
    ]


def _patch_ops(monkeypatch, session_factory, rows: list[dict]) -> None:
    monkeypatch.setattr(ops, "SessionLocal", session_factory)
    monkeypatch.setattr(ops, "get_congress_metadata_resolver", lambda: _NoopCongressMetadata())
    monkeypatch.setattr(ops, "fetch_house_page", lambda page, limit: rows if page == 0 else [])
    monkeypatch.setattr(ops, "fetch_senate_page", lambda page, limit: [])


def _seed_existing_amt(session_factory, row: dict) -> None:
    db = session_factory()
    try:
        outcome = upsert_house_transaction_from_row(
            db,
            row,
            metadata=_NoopCongressMetadata(),
            seen_transaction_keys=set(),
        )
        db.add(
            _congress_event_from_transaction(
                outcome["transaction"],
                outcome["filing"],
                outcome["member"],
                outcome["security"],
            )
        )
        db.commit()
    finally:
        db.close()


def test_targeted_document_mode_recovers_missing_evans_rows_without_duplicate_amt(monkeypatch):
    Session = _session_factory()
    rows = _evans_rows()
    _patch_ops(monkeypatch, Session, rows)
    _seed_existing_amt(Session, rows[0])

    dry_run = ops.run_document_repair(
        document="20034556.pdf",
        apply=False,
        member="Dwight Evans",
        symbols={"AMT", "CVS", "INTC", "PWR", "TMO"},
        pages=1,
        limit=100,
    )
    assert dry_run["parsed_transaction_count"] == 5
    assert dry_run["totals"]["transactions_to_insert"] == 4
    assert dry_run["totals"]["events_to_insert"] == 4
    by_symbol = {row["symbol"]: row for row in dry_run["rows"]}
    assert by_symbol["AMT"]["existing_transaction"] is True
    assert by_symbol["AMT"]["existing_event"] is True
    assert by_symbol["CVS"]["trade_date"] == "2025-11-21"

    db = Session()
    try:
        assert db.query(Transaction).count() == 1
        assert db.query(Event).filter(Event.event_type == "congress_trade").count() == 1
    finally:
        db.close()

    applied = ops.run_document_repair(
        document="20034556.pdf",
        apply=True,
        member="Dwight Evans",
        symbols={"AMT", "CVS", "INTC", "PWR", "TMO"},
        pages=1,
        limit=100,
    )
    assert applied["totals"]["transactions_to_insert"] == 4
    assert applied["totals"]["events_inserted"] == 4

    db = Session()
    try:
        tx_rows = db.execute(select(Transaction, Security).join(Security, Security.id == Transaction.security_id)).all()
        assert {security.symbol for _tx, security in tx_rows} == {"AMT", "CVS", "INTC", "PWR", "TMO"}
        assert db.query(Transaction).count() == 5
        assert db.query(Event).filter(Event.event_type == "congress_trade").count() == 5
        cvs = next(tx for tx, security in tx_rows if security.symbol == "CVS")
        assert cvs.trade_date == date(2025, 11, 21)
        assert cvs.report_date == date(2026, 5, 18)
    finally:
        db.close()

    rerun = ops.run_document_repair(
        document="20034556.pdf",
        apply=False,
        member="Dwight Evans",
        symbols={"AMT", "CVS", "INTC", "PWR", "TMO"},
        pages=1,
        limit=100,
    )
    assert rerun["totals"]["transactions_to_insert"] == 0
    assert rerun["totals"]["events_to_insert"] == 0


def test_targeted_document_mode_only_touches_requested_document(monkeypatch):
    Session = _session_factory()
    requested_rows = _evans_rows("20034556")
    other_rows = _evans_rows("20039999")
    other_rows[0] = {**other_rows[0], "symbol": "MSFT", "assetDescription": "Microsoft Corp"}
    _patch_ops(monkeypatch, Session, requested_rows + other_rows)

    result = ops.run_document_repair(
        document="20034556.pdf",
        apply=True,
        member="Dwight Evans",
        symbols={"AMT", "CVS", "INTC", "PWR", "TMO"},
        pages=1,
        limit=100,
    )
    assert result["parsed_transaction_count"] == 5

    db = Session()
    try:
        symbols = {
            symbol
            for (symbol,) in db.execute(select(Security.symbol).order_by(Security.symbol))
        }
        assert symbols == {"AMT", "CVS", "INTC", "PWR", "TMO"}
    finally:
        db.close()


def test_apply_without_document_or_explicit_broad_flags_refuses(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["backfill_missing_congress_multi_trade_events.py", "--apply"])
    with pytest.raises(SystemExit):
        ops.main()


def test_candidate_audit_classifies_low_risk_and_batch_apply_is_idempotent(monkeypatch):
    Session = _session_factory()
    monkeypatch.setattr(ops, "SessionLocal", Session)

    db = Session()
    try:
        outcome = upsert_house_transaction_from_row(
            db,
            _evans_rows()[0],
            metadata=_NoopCongressMetadata(),
            seen_transaction_keys=set(),
        )
        db.commit()
        transaction_id = outcome["transaction"].id
    finally:
        db.close()

    audit = ops.run_candidate_audit()
    assert audit["summary"]["total_candidate_events"] == 1
    assert audit["summary"]["by_risk"] == [("low", 1)]
    assert audit["sample_candidates"][0]["transaction_id"] == transaction_id

    dry_run = ops.run_candidate_batch(
        apply=False,
        risk="low",
        since_report_date=None,
        until_report_date=None,
        member=None,
        source=None,
        limit=100,
    )
    assert dry_run["selected_count"] == 1
    assert dry_run["events_to_insert"] == 1

    applied = ops.run_candidate_batch(
        apply=True,
        risk="low",
        since_report_date=None,
        until_report_date=None,
        member=None,
        source=None,
        limit=100,
    )
    assert applied["events_inserted"] == 1

    rerun = ops.run_candidate_batch(
        apply=False,
        risk="low",
        since_report_date=None,
        until_report_date=None,
        member=None,
        source=None,
        limit=100,
    )
    assert rerun["selected_count"] == 0
    assert rerun["events_to_insert"] == 0


def test_candidate_batch_refuses_high_risk_apply(monkeypatch):
    Session = _session_factory()
    monkeypatch.setattr(ops, "SessionLocal", Session)

    db = Session()
    try:
        member = Member(
            bioguide_id="E000296",
            first_name="Dwight",
            last_name="Evans",
            chamber="house",
            party="Democrat",
            state="PA",
        )
        db.add(member)
        db.flush()
        filing = Filing(
            member_id=member.id,
            source="house_fmp",
            filing_date=date(2026, 5, 18),
            document_url="https://example.test/no-symbol.pdf",
            document_hash="fmp:house:no-symbol",
        )
        db.add(filing)
        db.flush()
        db.add(
            Transaction(
                filing_id=filing.id,
                member_id=member.id,
                security_id=None,
                owner_type="self",
                transaction_type="sale",
                trade_date=date(2026, 5, 1),
                report_date=date(2026, 5, 18),
                amount_range_min=1001,
                amount_range_max=15000,
                description=None,
            )
        )
        db.commit()
    finally:
        db.close()

    audit = ops.run_candidate_audit()
    assert audit["summary"]["by_risk"] == [("high", 1)]

    with pytest.raises(RuntimeError, match="high-risk"):
        ops.run_candidate_batch(
            apply=True,
            risk="high",
            since_report_date=None,
            until_report_date=None,
            member=None,
            source=None,
            limit=100,
        )


def _seed_no_security_candidate(session_factory, *, description: str | None) -> None:
    db = session_factory()
    try:
        member = Member(
            bioguide_id="T000001",
            first_name="Test",
            last_name="Member",
            chamber="house",
            party="Democrat",
            state="CA",
        )
        db.add(member)
        db.flush()
        filing = Filing(
            member_id=member.id,
            source="house_fmp",
            filing_date=date(2026, 1, 10),
            document_url="https://example.test/test.pdf",
            document_hash="fmp:house:test",
        )
        db.add(filing)
        db.flush()
        db.add(
            Transaction(
                filing_id=filing.id,
                member_id=member.id,
                security_id=None,
                owner_type="self",
                transaction_type="purchase",
                trade_date=date(2026, 1, 2),
                report_date=date(2026, 1, 10),
                amount_range_min=1001,
                amount_range_max=15000,
                description=description,
            )
        )
        db.commit()
    finally:
        db.close()


def test_resolution_rejects_generic_security_title(monkeypatch):
    Session = _session_factory()
    monkeypatch.setattr(ops, "SessionLocal", Session)
    _seed_no_security_candidate(Session, description="Common Stock")

    candidate = ops.run_candidate_audit()["sample_candidates"][0]
    assert candidate["resolution_confidence"] == "unresolved"
    assert candidate["resolution_source"] == "no_usable_issuer"
    assert candidate["risk"] == "high"


def test_resolution_exact_issuer_to_ticker_match(monkeypatch):
    Session = _session_factory()
    monkeypatch.setattr(ops, "SessionLocal", Session)
    db = Session()
    try:
        db.add(Security(symbol="AAPL", name="Apple Inc", asset_class="stock", sector="Technology"))
        db.commit()
    finally:
        db.close()
    _seed_no_security_candidate(Session, description="Apple Inc")

    candidate = ops.run_candidate_audit()["sample_candidates"][0]
    assert candidate["resolved_symbol"] == "AAPL"
    assert candidate["resolution_confidence"] == "exact"
    assert candidate["risk"] == "low"


def test_resolution_historical_issuer_to_ticker_match(monkeypatch):
    Session = _session_factory()
    monkeypatch.setattr(ops, "SessionLocal", Session)
    db = Session()
    try:
        db.add(
            Event(
                event_type="congress_trade",
                ts=date(2026, 1, 1),
                event_date=None,
                symbol="MSFT",
                source="house_fmp",
                impact_score=0,
                payload_json=json.dumps({"security_name": "Microsoft Corporation"}),
            )
        )
        db.commit()
    finally:
        db.close()
    _seed_no_security_candidate(Session, description="Microsoft Corporation")

    candidate = ops.run_candidate_audit()["sample_candidates"][0]
    assert candidate["resolved_symbol"] == "MSFT"
    assert candidate["resolution_confidence"] == "historical_exact"
    assert candidate["risk"] == "low"


def test_resolution_reviewed_alias_match(monkeypatch):
    Session = _session_factory()
    monkeypatch.setattr(ops, "SessionLocal", Session)
    _seed_no_security_candidate(Session, description="CVS Health Corporation")

    candidate = ops.run_candidate_audit()["sample_candidates"][0]
    assert candidate["resolved_symbol"] == "CVS"
    assert candidate["resolution_confidence"] == "alias_reviewed"
    assert candidate["risk"] == "low"


def test_collision_detection_keeps_candidate_high_risk(monkeypatch):
    Session = _session_factory()
    monkeypatch.setattr(ops, "SessionLocal", Session)
    db = Session()
    try:
        member = Member(
            bioguide_id="T000001",
            first_name="Test",
            last_name="Member",
            chamber="house",
            party="Democrat",
            state="CA",
        )
        security = Security(symbol="AAPL", name="Apple Inc", asset_class="stock", sector="Technology")
        db.add_all([member, security])
        db.flush()
        filing = Filing(
            member_id=member.id,
            source="house_fmp",
            filing_date=date(2026, 1, 10),
            document_url="https://example.test/test.pdf",
            document_hash="fmp:house:test",
        )
        db.add(filing)
        db.flush()
        tx = Transaction(
            filing_id=filing.id,
            member_id=member.id,
            security_id=security.id,
            owner_type="self",
            transaction_type="purchase",
            trade_date=date(2026, 1, 2),
            report_date=date(2026, 1, 10),
            amount_range_min=1001,
            amount_range_max=15000,
            description=None,
        )
        db.add(tx)
        db.flush()
        payload = _congress_event_payload(tx, filing, member, security)
        payload["transaction_id"] = 999999
        payload["external_id"] = "congress_tx:999999"
        payload["amount_range_min"] = 1001.0
        payload["amount_range_max"] = 15000.0
        payload["backfill_id"] = _build_backfill_id(payload)
        db.add(
            Event(
                event_type="congress_trade",
                ts=date(2026, 1, 1),
                event_date=None,
                symbol="AAPL",
                source="house_fmp",
                impact_score=0,
                payload_json=json.dumps(payload),
            )
        )
        db.commit()
    finally:
        db.close()

    candidate = ops.run_candidate_audit()["sample_candidates"][0]
    assert candidate["backfill_collision"] is True
    assert candidate["collision_reason"] == "same_document_or_filing_duplicate"
    assert candidate["risk"] == "high"


def _seed_unresolved_candidate(
    session_factory,
    *,
    trade_date: date,
    amount_min: int = 1001,
    amount_max: int = 15000,
    tx_type: str = "purchase",
    document: str = "source-test.pdf",
) -> None:
    db = session_factory()
    try:
        member = db.execute(select(Member).where(Member.bioguide_id == "T000001")).scalar_one_or_none()
        if member is None:
            member = Member(
                bioguide_id="T000001",
                first_name="Test",
                last_name="Member",
                chamber="house",
                party="Democrat",
                state="CA",
            )
            db.add(member)
            db.flush()
        filing = Filing(
            member_id=member.id,
            source="house_fmp",
            filing_date=date(2026, 1, 10),
            document_url=f"https://example.test/{document}",
            document_hash=f"fmp:house:{document}",
        )
        db.add(filing)
        db.flush()
        db.add(
            Transaction(
                filing_id=filing.id,
                member_id=member.id,
                security_id=None,
                owner_type="self",
                transaction_type=tx_type,
                trade_date=trade_date,
                report_date=date(2026, 1, 10),
                amount_range_min=amount_min,
                amount_range_max=amount_max,
                description=None,
            )
        )
        db.commit()
    finally:
        db.close()


def test_enrich_unresolved_source_rows_recovers_identity_without_events(monkeypatch, tmp_path):
    Session = _session_factory()
    monkeypatch.setattr(ops, "SessionLocal", Session)
    _seed_unresolved_candidate(Session, trade_date=date(2026, 1, 2), amount_min=1001, amount_max=15000)
    _seed_unresolved_candidate(Session, trade_date=date(2026, 1, 3), amount_min=15001, amount_max=50000)
    rows = [
        {
            "firstName": "Test",
            "lastName": "Member",
            "office": "Test Member",
            "district": "CA01",
            "disclosureDate": "2026-01-10",
            "link": "https://example.test/source-test.pdf",
            "type": "purchase",
            "owner": "self",
            "amount": "$1,001 - $15,000",
            "transactionDate": "2026-01-02",
            "symbol": "AAPL",
            "assetDescription": "Apple Inc",
        },
        {
            "firstName": "Test",
            "lastName": "Member",
            "office": "Test Member",
            "district": "CA01",
            "disclosureDate": "2026-01-10",
            "link": "https://example.test/source-test.pdf",
            "type": "purchase",
            "owner": "self",
            "amount": "$15,001 - $50,000",
            "transactionDate": "2026-01-03",
            "symbol": "MSFT",
            "assetDescription": "Microsoft Corporation",
        },
    ]
    monkeypatch.setattr(ops, "fetch_house_page", lambda page, limit: rows if page == 0 else [])
    monkeypatch.setattr(ops, "fetch_senate_page", lambda page, limit: [])

    result = ops.run_enrich_unresolved(
        artifact_dir=str(tmp_path),
        limit=100,
        since_report_date=None,
        until_report_date=None,
        source=None,
        document=None,
        pages=1,
        page_limit=100,
    )

    assert result["summary"]["selected_candidates"] == 2
    assert result["summary"]["ticker_recovered_count"] == 2
    assert result["summary"]["issuer_recovered_count"] == 2
    assert result["summary"]["risk_after_enrichment"] == [("low", 2)]
    db = Session()
    try:
        assert db.query(Event).filter(Event.event_type == "congress_trade").count() == 0
        assert db.query(Transaction).count() == 2
    finally:
        db.close()


def test_enrichment_artifact_moves_safe_source_ticker_to_low_risk(monkeypatch, tmp_path):
    Session = _session_factory()
    monkeypatch.setattr(ops, "SessionLocal", Session)
    _seed_unresolved_candidate(Session, trade_date=date(2026, 1, 2))
    rows = [
        {
            "firstName": "Test",
            "lastName": "Member",
            "office": "Test Member",
            "district": "CA01",
            "disclosureDate": "2026-01-10",
            "link": "https://example.test/source-test.pdf",
            "type": "purchase",
            "owner": "self",
            "amount": "$1,001 - $15,000",
            "transactionDate": "2026-01-02",
            "symbol": "AAPL",
            "assetDescription": "Apple Inc",
        }
    ]
    monkeypatch.setattr(ops, "fetch_house_page", lambda page, limit: rows if page == 0 else [])
    monkeypatch.setattr(ops, "fetch_senate_page", lambda page, limit: [])

    before = ops.run_candidate_audit(artifact_dir=str(tmp_path))
    assert before["summary"]["by_risk"] == [("high", 1)]

    ops.run_enrich_unresolved(
        artifact_dir=str(tmp_path),
        limit=100,
        since_report_date=None,
        until_report_date=None,
        source=None,
        document=None,
        pages=1,
        page_limit=100,
    )

    after = ops.run_candidate_audit(artifact_dir=str(tmp_path))
    assert after["enrichment_rows_loaded"] == 1
    assert after["summary"]["by_risk"] == [("low", 1)]
    assert after["sample_candidates"][0]["resolved_symbol"] == "AAPL"
    assert after["sample_candidates"][0]["resolution_confidence"] == "source_exact"


def test_enrichment_generic_source_title_remains_high(monkeypatch, tmp_path):
    Session = _session_factory()
    monkeypatch.setattr(ops, "SessionLocal", Session)
    _seed_unresolved_candidate(Session, trade_date=date(2026, 1, 2))
    rows = [
        {
            "firstName": "Test",
            "lastName": "Member",
            "office": "Test Member",
            "district": "CA01",
            "disclosureDate": "2026-01-10",
            "link": "https://example.test/source-test.pdf",
            "type": "purchase",
            "owner": "self",
            "amount": "$1,001 - $15,000",
            "transactionDate": "2026-01-02",
            "assetDescription": "Common Stock",
        }
    ]
    monkeypatch.setattr(ops, "fetch_house_page", lambda page, limit: rows if page == 0 else [])
    monkeypatch.setattr(ops, "fetch_senate_page", lambda page, limit: [])
    monkeypatch.setattr(ops, "_fetch_pdf_enrichments", lambda candidates: {})

    result = ops.run_enrich_unresolved(
        artifact_dir=str(tmp_path),
        limit=100,
        since_report_date=None,
        until_report_date=None,
        source=None,
        document=None,
        pages=1,
        page_limit=100,
    )
    assert result["summary"]["ticker_recovered_count"] == 0

    audit = ops.run_candidate_audit(artifact_dir=str(tmp_path))
    assert audit["summary"]["by_risk"] == [("high", 1)]


def test_pdf_enrichment_rejects_form_chrome():
    item = {
        "trade_date": "2026-02-19",
        "amount_min": 15001,
        "amount_max": 50000,
        "transaction_type": "purchase",
    }
    text = """
    Periodic Transaction Report
    State/District: VA08
    Owner
    Purchase
    02/19/2026
    $15,001 - $50,000
    Gains >
    """
    assert ops._enrichment_from_pdf_text(text, item) is None
