from __future__ import annotations

import json
from datetime import date, datetime, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from app.db import Base
from app.models import Event, GovernmentContract, Member, Security
from app.services.profile_overviews import congress_overview, departments_overview, profiles_summary


def _db() -> Session:
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)
    return Session(engine)


def test_profiles_summary_uses_real_aggregate_counts():
    db = _db()
    db.add(Member(bioguide_id="P000197", first_name="Nancy", last_name="Pelosi", chamber="house", party="D", state="CA"))
    db.add(Security(symbol="NVDA", name="NVIDIA Corp", asset_class="stock", sector="Technology"))
    db.add(
        Event(
            id=1,
            event_type="congress_trade",
            ts=datetime(2026, 8, 1, tzinfo=timezone.utc),
            event_date=datetime(2026, 8, 1, tzinfo=timezone.utc),
            symbol="NVDA",
            source="test",
            payload_json=json.dumps({"company_name": "NVIDIA Corp"}),
            member_name="Nancy Pelosi",
            member_bioguide_id="P000197",
            chamber="house",
            party="D",
            trade_type="purchase",
            amount_min=50_000,
            amount_max=100_000,
        )
    )
    db.add(
        GovernmentContract(
            award_id="A1",
            dedupe_key="A1",
            symbol="NVDA",
            recipient_name="NVIDIA Corp",
            award_date=date(2026, 8, 2),
            award_amount=1_000_000,
            awarding_agency="Department of Defense",
            source="test",
        )
    )
    db.commit()

    payload = profiles_summary(db, include_institutions=False)

    congress = next(card for card in payload["cards"] if card["kind"] == "congress")
    departments = next(card for card in payload["cards"] if card["kind"] == "departments")
    assert congress["metrics"][0]["value"] == 1
    assert departments["metrics"][1]["value"] == 1_000_000
    assert payload["activity"][0]["profile_href"] == "/member/nancy-pelosi"


def test_congress_overview_returns_page_ready_sections():
    db = _db()
    db.add(Security(symbol="NVDA", name="NVIDIA Corp", asset_class="stock", sector="Technology"))
    db.add(
        Event(
            id=2,
            event_type="congress_trade",
            ts=datetime(2026, 8, 1, tzinfo=timezone.utc),
            event_date=datetime(2026, 8, 1, tzinfo=timezone.utc),
            symbol="NVDA",
            source="test",
            payload_json="{}",
            member_name="Nancy Pelosi",
            member_bioguide_id="P000197",
            chamber="house",
            party="D",
            trade_type="purchase",
            amount_min=50_000,
            amount_max=100_000,
        )
    )
    db.commit()

    payload = congress_overview(db, chamber="house", period_days=365)

    assert payload["summary"][0]["label"] == "Total Trades"
    assert payload["summary"][0]["value"] == 1
    assert payload["top_members"][0]["href"] == "/member/nancy-pelosi"
    assert payload["most_traded_stocks"][0]["symbol"] == "NVDA"
    assert payload["sector_exposure"][0]["segments"][0]["label"] == "Technology"


def test_departments_overview_uses_contract_language():
    db = _db()
    db.add(
        GovernmentContract(
            award_id="A2",
            dedupe_key="A2",
            symbol="LMT",
            recipient_name="Lockheed Martin",
            award_date=date(2026, 7, 12),
            award_amount=2_500_000,
            awarding_agency="Department of Defense",
            source="test",
        )
    )
    db.commit()

    payload = departments_overview(db)

    assert payload["summary"][0]["label"] == "Total Contracts"
    assert payload["summary"][1]["label"] == "Total Contract Value"
    assert payload["top_departments"][0]["name"] == "Department of Defense"
    assert payload["top_vendors"][0]["symbol"] == "LMT"
