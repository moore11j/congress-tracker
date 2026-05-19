from __future__ import annotations

from datetime import date

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from app.db import Base
from app.ingest.government_contracts import ensure_government_contracts_schema
from app.models import GovernmentContract, GovernmentContractAction, Security
from app.routers.events import global_search
from app.services.government_departments import (
    canonical_department_name,
    department_slug,
    department_suggestions,
    get_department_profile,
)


def _db() -> Session:
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)
    ensure_government_contracts_schema(engine)
    return Session(engine)


def test_department_aliases_are_canonical_and_slugged():
    assert canonical_department_name("DoD") == "Department of Defense"
    assert canonical_department_name("Defense Department") == "Department of Defense"
    assert canonical_department_name("National Aeronautics and Space Administration") == "NASA"
    assert department_slug("NASA") == "nasa"
    assert department_slug("Department of Defense") == "department-of-defense"


def test_department_profile_aggregates_tickers_and_actions():
    db = _db()
    db.add(Security(symbol="LMT", name="Lockheed Martin Corporation", asset_class="stock", sector="Industrials"))
    db.add(
        GovernmentContract(
            award_id="AWD-LMT-1",
            dedupe_key="AWD-LMT-1",
            symbol="LMT",
            recipient_name="LOCKHEED MARTIN CORPORATION",
            raw_recipient_name="LOCKHEED MARTIN CORPORATION",
            award_date=date(2026, 4, 15),
            award_amount=12_000_000,
            awarding_agency="Department of Defense",
            awarding_sub_agency="Department of the Air Force",
            funding_agency="Department of Defense",
            period_start=date(2026, 4, 15),
            period_end=date(2027, 4, 14),
            description="Mission systems support",
            source="local",
        )
    )
    db.add(
        GovernmentContractAction(
            parent_award_id="AWD-LMT-1",
            modification_number="P00001",
            dedupe_key="AWD-LMT-1-P00001",
            symbol="LMT",
            recipient_name="LOCKHEED MARTIN CORPORATION",
            company_name="Lockheed Martin Corporation",
            awarding_agency="Department of Defense",
            awarding_sub_agency="Department of the Air Force",
            action_date=date(2026, 4, 20),
            obligated_amount=3_000_000,
            description="Incremental mission systems funding",
            source="local",
        )
    )
    db.commit()

    profile = get_department_profile(db, "department-of-defense")

    assert profile is not None
    assert profile["summary"]["totalAwarded"] == 12_000_000
    assert profile["summary"]["contractCount"] == 1
    assert profile["summary"]["linkedTickerCount"] == 1
    assert profile["summary"]["topTicker"] == "LMT"
    assert profile["tickers"][0]["companyName"] == "Lockheed Martin Corporation"
    assert profile["recentContracts"][0]["amount"] == 3_000_000
    assert profile["recentContracts"][0]["department"] == "Department of Defense"


def test_department_suggestions_include_department_routes():
    db = _db()

    results = department_suggestions(db, "dod", limit=5)

    assert results[0]["type"] == "government_agency"
    assert results[0]["label"] == "Department of Defense"
    assert results[0]["route"] == "/departments/department-of-defense"


def test_global_search_includes_department_results():
    db = _db()

    payload = global_search(db=db, q="defense", limit=5)

    assert payload["results"][0]["type"] == "government_agency"
    assert payload["results"][0]["route"] == "/departments/department-of-defense"
