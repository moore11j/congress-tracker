from __future__ import annotations

from datetime import datetime, timedelta, timezone

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker

from app.db import Base
from app.ingest.government_contracts import (
    ensure_government_contracts_schema,
    ingest_government_contracts,
    load_ticker_aliases,
    match_recipient_to_symbol,
    normalize_recipient_name,
    normalize_usaspending_award,
)
from app.models import GovernmentContract
from app.services.government_contracts import get_government_contracts_signal, get_government_contracts_summaries_for_symbols


def _engine():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)
    ensure_government_contracts_schema(engine)
    return engine


def _row(
    *,
    award_id: str = "AWD-1",
    recipient_name: str = "LOCKHEED MARTIN CORPORATION",
    amount: float = 5_500_000,
    description: str = "Mission systems support",
) -> dict:
    return {
        "Award ID": award_id,
        "Recipient Name": recipient_name,
        "Award Amount": amount,
        "Start Date": "2026-04-15",
        "End Date": "2027-04-14",
        "Awarding Agency": "Department of Defense",
        "Awarding Sub Agency": "Department of the Navy",
        "Funding Agency": "Department of Defense",
        "Funding Sub Agency": "Department of the Navy",
        "Description": description,
        "Contract Award Type": "DEFINITIVE CONTRACT",
        "Award Type": None,
        "generated_internal_id": f"CONT_AWD_{award_id}",
    }


def test_recipient_normalization_removes_suffixes_and_punctuation():
    assert normalize_recipient_name("The Boeing Company, Inc.") == "BOEING"


def test_alias_exact_matching_returns_symbol():
    aliases = load_ticker_aliases()
    matched = match_recipient_to_symbol("LOCKHEED MARTIN CORPORATION", aliases)
    assert matched is not None
    assert matched["symbol"] == "LMT"
    assert matched["mapping_method"] == "alias_exact"


def test_alias_safe_contains_matching_returns_symbol():
    aliases = load_ticker_aliases()
    matched = match_recipient_to_symbol("LOCKHEED MARTIN ROTARY AND MISSION SYSTEMS", aliases)
    assert matched is not None
    assert matched["symbol"] == "LMT"
    assert matched["mapping_method"] == "alias_contains"


def test_unmapped_recipient_returns_none():
    aliases = load_ticker_aliases()
    assert match_recipient_to_symbol("SMALL BUSINESS INNOVATION PARTNER", aliases) is None


def test_usaspending_response_normalization_maps_expected_fields():
    normalized = normalize_usaspending_award(_row(), load_ticker_aliases())
    assert normalized is not None
    assert normalized["symbol"] == "LMT"
    assert normalized["award_id"] == "AWD-1"
    assert normalized["award_amount"] == 5_500_000
    assert normalized["award_date"].isoformat() == "2026-04-15"
    assert normalized["source_url"].endswith("CONT_AWD_AWD-1")


def test_upsert_dedupes_same_award_id(monkeypatch):
    engine = _engine()
    testing_session = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    monkeypatch.setattr("app.ingest.government_contracts.SessionLocal", testing_session)

    rows = [_row(description="Mission systems support")]

    def fake_fetch(**_kwargs):
        return list(rows)

    monkeypatch.setattr("app.ingest.government_contracts.fetch_spending_by_award", fake_fetch)

    first = ingest_government_contracts(
        lookback_days=30,
        min_award_amount=1_000_000,
        limit=100,
        max_pages=1,
        symbols=["LMT"],
        dry_run=False,
        verbose=False,
        enforce_guardrail=False,
    )
    rows[0]["Description"] = "Updated mission systems support"
    second = ingest_government_contracts(
        lookback_days=30,
        min_award_amount=1_000_000,
        limit=100,
        max_pages=1,
        symbols=["LMT"],
        dry_run=False,
        verbose=False,
        enforce_guardrail=False,
    )

    with Session(engine) as db:
        contracts = db.execute(select(GovernmentContract)).scalars().all()

    assert first["inserted_count"] == 1
    assert second["updated_count"] == 1
    assert len(contracts) == 1
    assert contracts[0].description == "Updated mission systems support"


def test_targeted_symbol_mode_uses_aliases(monkeypatch):
    captured_terms: list[str | None] = []

    def fake_fetch(*, recipient_search_text=None, **_kwargs):
        captured_terms.append(recipient_search_text)
        return []

    monkeypatch.setattr("app.ingest.government_contracts.fetch_spending_by_award", fake_fetch)

    result = ingest_government_contracts(
        lookback_days=30,
        min_award_amount=1_000_000,
        limit=100,
        max_pages=1,
        symbols=["LMT", "RTX"],
        dry_run=True,
        verbose=False,
        enforce_guardrail=False,
    )

    assert result["mode"] == "targeted"
    assert any(term and "LOCKHEED MARTIN" in term.upper() for term in captured_terms)
    assert any(term and "RTX" in term.upper() for term in captured_terms)


def test_aggregate_summary_sees_ingested_rows(monkeypatch):
    engine = _engine()
    testing_session = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    monkeypatch.setattr("app.ingest.government_contracts.SessionLocal", testing_session)
    monkeypatch.setattr(
        "app.ingest.government_contracts.fetch_spending_by_award",
        lambda **_kwargs: [_row(award_id="AWD-LMT", recipient_name="LOCKHEED MARTIN CORPORATION", amount=12_000_000)],
    )

    ingest_government_contracts(
        lookback_days=30,
        min_award_amount=1_000_000,
        limit=100,
        max_pages=1,
        symbols=["LMT"],
        dry_run=False,
        verbose=False,
        enforce_guardrail=False,
    )

    with Session(engine) as db:
        summaries = get_government_contracts_summaries_for_symbols(
            db,
            ["LMT", "RTX"],
            lookback_days=365,
            min_amount=1_000_000,
        )

    assert summaries["LMT"]["active"] is True
    assert summaries["LMT"]["contract_count"] == 1
    assert summaries["LMT"]["total_award_amount"] == 12_000_000
    assert summaries["RTX"]["active"] is False


def test_government_contracts_signal_is_neutral_without_contracts():
    engine = _engine()

    with Session(engine) as db:
        db.add(
            GovernmentContract(
                id=190,
                award_id="AWD-190",
                dedupe_key="dedupe-190",
                symbol="LMT",
                recipient_name="Lockheed Martin",
                raw_recipient_name="Lockheed Martin",
                award_date=(datetime.now(timezone.utc) - timedelta(days=20)).date(),
                award_amount=8_000_000,
                awarding_agency="Department of Defense",
                source="usaspending",
                mapping_method="alias_exact",
                mapping_confidence=1.0,
                payload_json="{}",
            )
        )
        db.commit()
        signal = get_government_contracts_signal(db, "NONE", lookback_days=365, min_amount=1_000_000)

    assert signal["active"] is False
    assert signal["direction"] == "neutral"
    assert signal["score_contribution"] == 0


def test_government_contracts_signal_uses_amount_tiers_without_recency_boost():
    engine = _engine()
    scenarios = [
        ("ONE", 1_000_000, 5),
        ("TEN", 10_000_000, 10),
        ("FIFTY", 50_000_000, 15),
        ("BIG", 250_000_000, 20),
    ]

    with Session(engine) as db:
        for index, (symbol, amount, _) in enumerate(scenarios, start=1):
            db.add(
                GovernmentContract(
                    id=index,
                    award_id=f"AWD-{symbol}",
                    dedupe_key=f"dedupe-{symbol}",
                    symbol=symbol,
                    recipient_name=f"{symbol} Recipient",
                    raw_recipient_name=f"{symbol} Recipient",
                    award_date=(datetime.now(timezone.utc) - timedelta(days=45)).date(),
                    award_amount=amount,
                    awarding_agency="Department of Defense",
                    source="usaspending",
                    mapping_method="alias_exact",
                    mapping_confidence=1.0,
                    payload_json="{}",
                )
            )
        db.commit()

        for symbol, _amount, expected_score in scenarios:
            signal = get_government_contracts_signal(db, symbol, lookback_days=365, min_amount=1_000_000)
            assert signal["active"] is True
            assert signal["direction"] == "bullish"
            assert signal["score_contribution"] == expected_score


def test_recent_large_government_contract_caps_score_at_twenty():
    engine = _engine()

    with Session(engine) as db:
        db.add(
            GovernmentContract(
                id=300,
                award_id="AWD-CAP",
                dedupe_key="dedupe-cap",
                symbol="CAP",
                recipient_name="CAP Recipient",
                raw_recipient_name="CAP Recipient",
                award_date=(datetime.now(timezone.utc) - timedelta(days=3)).date(),
                award_amount=300_000_000,
                awarding_agency="Department of Defense",
                source="usaspending",
                mapping_method="alias_exact",
                mapping_confidence=1.0,
                payload_json="{}",
            )
        )
        db.commit()

        signal = get_government_contracts_signal(db, "CAP", lookback_days=365, min_amount=1_000_000)

    assert signal["active"] is True
    assert signal["direction"] == "bullish"
    assert signal["score_contribution"] == 20


def test_guardrail_blocks_second_contract_run_within_twelve_hours(monkeypatch):
    engine = _engine()
    testing_session = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    monkeypatch.setattr("app.ingest.government_contracts.SessionLocal", testing_session)
    monkeypatch.setattr(
        "app.ingest.government_contracts.fetch_spending_by_award",
        lambda **_kwargs: [_row(award_id="AWD-GUARD")],
    )

    first = ingest_government_contracts(
        lookback_days=30,
        min_award_amount=1_000_000,
        limit=100,
        max_pages=1,
        symbols=["LMT"],
        dry_run=False,
        verbose=False,
        enforce_guardrail=True,
    )
    second = ingest_government_contracts(
        lookback_days=30,
        min_award_amount=1_000_000,
        limit=100,
        max_pages=1,
        symbols=["LMT"],
        dry_run=False,
        verbose=False,
        enforce_guardrail=True,
    )

    assert first["status"] == "ok"
    assert second["status"] == "guarded_skip"
