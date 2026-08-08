from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from app.db import Base
from app.models import Event, GovernmentContract, GovernmentContractAction, InsiderTransactionNormalized, InstitutionalPosition, InstitutionalPositionChange, Member, Security, TickerMeta
from app.services.profile_overviews import congress_overview, departments_overview, insiders_overview, institutions_overview, profiles_summary


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
    db.add(
        InsiderTransactionNormalized(
            accession_number="0001",
            issuer_name="Exponent, Inc.",
            ticker_raw="EXPO",
            ticker_normalized="EXPO",
            reporting_owner_cik="1234567890",
            reporting_owner_name="Jane Insider",
            transaction_date=date(2026, 7, 30),
            filing_date=date(2026, 8, 1),
            transaction_code="P",
            transaction_type_normalized="purchase",
            value=125_000,
            normalized_hash="insider-1",
        )
    )
    db.commit()

    payload = profiles_summary(db, include_institutions=False)

    congress = next(card for card in payload["cards"] if card["kind"] == "congress")
    insiders = next(card for card in payload["cards"] if card["kind"] == "insiders")
    departments = next(card for card in payload["cards"] if card["kind"] == "departments")
    assert congress["metrics"][0]["value"] == 1
    assert insiders["metrics"][1]["value"] == 1
    assert departments["metrics"][1]["value"] == 1_000_000
    assert payload["directories"][0]["primary_title"] == "Most Active Members"
    assert payload["activity"][0]["profile_href"] == "/member/nancy-pelosi"


def test_congress_overview_returns_page_ready_sections():
    db = _db()
    db.add(Security(symbol="NVDA", name="NVIDIA Corp", asset_class="stock", sector="Technology"))
    db.add(TickerMeta(symbol="AAPL", company_name="Apple Inc.", sector="Consumer Electronics"))
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
    db.add(
        Event(
            id=3,
            event_type="congress_trade",
            ts=datetime(2026, 8, 2, tzinfo=timezone.utc),
            event_date=datetime(2026, 8, 2, tzinfo=timezone.utc),
            symbol="AAPL",
            source="test",
            payload_json="{}",
            member_name="Nancy Pelosi",
            member_bioguide_id=None,
            chamber="house",
            party="D",
            trade_type="purchase",
            amount_min=10_000,
            amount_max=15_000,
        )
    )
    db.commit()

    payload = congress_overview(db, chamber="house", period_days=365)

    assert payload["summary"][0]["label"] == "Total Trades"
    assert payload["summary"][0]["value"] == 2
    assert payload["top_members"][0]["href"] == "/member/nancy-pelosi"
    assert payload["most_traded_stocks"][0]["symbol"] == "NVDA"
    sector_labels = {segment["label"] for segment in payload["sector_exposure"][0]["segments"]}
    assert sector_labels == {"Technology"}
    assert len([row for row in payload["top_buyers"] if row["name"] == "Nancy Pelosi"]) == 1
    assert payload["top_buyers"][0]["trades"] == 2


def test_insiders_overview_uses_normalized_open_market_transactions():
    db = _db()
    today = date.today()
    now = datetime.combine(today, datetime.min.time(), tzinfo=timezone.utc)
    db.add(TickerMeta(symbol="EXPO", company_name="Exponent, Inc.", sector="Industrials"))
    db.add(
        Event(
            id=4,
            event_type="insider_trade",
            ts=now,
            event_date=now,
            symbol="EXPO",
            source="test",
            payload_json="{}",
            member_name=None,
            member_bioguide_id=None,
            trade_type="purchase",
            amount_min=4_000_000_000_000_000,
            amount_max=4_000_000_000_000_000,
        )
    )
    db.add_all(
        [
            InsiderTransactionNormalized(
                accession_number="insider-a",
                issuer_name="Exponent, Inc.",
                ticker_raw="EXPO",
                ticker_normalized="EXPO",
                reporting_owner_cik="1111",
                reporting_owner_name="Jane Buyer",
                officer_title="Chief Financial Officer",
                is_officer=True,
                transaction_date=today,
                filing_date=today,
                transaction_code="P",
                transaction_type_normalized="open_market_purchase",
                value=100_000,
                normalized_hash="insider-a-1",
            ),
            InsiderTransactionNormalized(
                accession_number="insider-b",
                issuer_name="Exponent, Inc.",
                ticker_raw="EXPO",
                ticker_normalized="EXPO",
                reporting_owner_cik="2222",
                reporting_owner_name="John Buyer",
                is_director=True,
                transaction_date=today,
                filing_date=today,
                transaction_code="P",
                transaction_type_normalized="open_market_purchase",
                value=250_000,
                normalized_hash="insider-b-1",
            ),
            InsiderTransactionNormalized(
                accession_number="insider-c",
                issuer_name="Exponent, Inc.",
                ticker_raw="EXPO",
                ticker_normalized="EXPO",
                reporting_owner_cik="1111",
                reporting_owner_name="Jane Buyer",
                transaction_date=today,
                filing_date=today,
                transaction_code="S",
                transaction_type_normalized="open_market_sale",
                value=50_000,
                normalized_hash="insider-c-1",
            ),
        ]
    )
    db.commit()

    payload = insiders_overview(db, period_days=365)

    assert payload["summary"][0]["label"] == "Open-Market Trades"
    assert payload["summary"][0]["value"] == 3
    assert payload["summary"][1]["value"] == 350_000
    assert payload["summary"][2]["value"] == 50_000
    assert payload["summary"][3]["value"] == 2
    assert payload["summary"][4]["value"] == 133333.33333333334
    assert payload["top_insiders"][0]["name"] == "John Buyer"
    assert payload["top_insiders"][0]["href"] == "/insider/john-buyer-0000002222"
    assert payload["recent_purchases"][0]["profile"] != "Profile unavailable"
    assert payload["recent_purchases"][0]["profile_href"]
    assert payload["most_traded_stocks"][0]["actor_count"] == 2
    assert payload["sector_activity"][0]["segments"][0]["label"] == "Industrials"
    assert payload["cluster_buying"][0]["symbol"] == "EXPO"
    assert payload["cluster_buying"][0]["unique_insiders"] == 2


def test_institutions_overview_compares_previous_quarter_and_classifies_mega_cap_tech():
    db = _db()
    filing_date = date(2026, 8, 1)
    previous_date = date(2026, 5, 1)
    db.add(TickerMeta(symbol="PFE", company_name="Pfizer Inc.", sector="Healthcare"))
    symbols = ["AAPL", "NVDA", "MSFT", "GOOGL", "AMZN", "META"]
    for index, symbol in enumerate(symbols, start=1):
        db.add(
            InstitutionalPosition(
                filing_id=index,
                cik=f"100{index}",
                symbol=symbol,
                normalized_symbol=symbol,
                issuer_name=symbol,
                shares=100,
                value_usd=1_000_000,
                report_year=2026,
                report_quarter=2,
                filing_date=filing_date,
            )
        )
        db.add(
            InstitutionalPosition(
                filing_id=index + 100,
                cik=f"100{index}",
                symbol=symbol,
                normalized_symbol=symbol,
                issuer_name=symbol,
                shares=50,
                value_usd=500_000,
                report_year=2026,
                report_quarter=1,
                filing_date=previous_date,
            )
        )
    db.add(
        InstitutionalPosition(
            filing_id=200,
            cik="2000",
            symbol="PFE",
            normalized_symbol="PFE",
            issuer_name="Pfizer Inc.",
            shares=100,
            value_usd=500_000,
            report_year=2026,
            report_quarter=2,
            filing_date=filing_date,
        )
    )
    db.add_all(
        [
            InstitutionalPositionChange(
                cik="1001",
                holder_name="Alpha Fund",
                symbol="AAPL",
                normalized_symbol="AAPL",
                report_year=2026,
                report_quarter=2,
                filing_date=filing_date,
                shares_delta=10,
                value_delta_usd=100_000,
                change_type="increase",
            ),
            InstitutionalPositionChange(
                cik="1002",
                holder_name="Beta Fund",
                symbol="NVDA",
                normalized_symbol="NVDA",
                report_year=2026,
                report_quarter=2,
                filing_date=filing_date,
                shares_delta=-5,
                value_delta_usd=-50_000,
                change_type="decrease",
            ),
            InstitutionalPositionChange(
                cik="1001",
                holder_name="Alpha Fund",
                symbol="AAPL",
                normalized_symbol="AAPL",
                report_year=2026,
                report_quarter=1,
                filing_date=previous_date,
                shares_delta=5,
                value_delta_usd=25_000,
                change_type="increase",
            ),
        ]
    )
    db.commit()

    payload = institutions_overview(db, include_details=True)

    assert payload["report_year"] == 2026
    assert payload["report_quarter"] == 2
    assert payload["previous_report_year"] == 2026
    assert payload["previous_report_quarter"] == 1
    assert payload["summary"][0]["value"] == 7
    assert payload["summary"][0]["previous_value"] == 6
    assert payload["summary"][1]["value"] == 6_500_000
    assert payload["summary"][1]["previous_value"] == 3_000_000
    assert payload["summary"][2]["value"] == 1
    assert payload["summary"][2]["previous_value"] == 1
    assert payload["summary"][3]["value"] == 1
    assert payload["summary"][3]["previous_value"] == 0
    assert payload["summary"][4]["value"] == 50_000
    assert payload["summary"][4]["previous_value"] == 25_000
    latest = payload["sector_exposure"][-1]["segments"]
    assert latest[0]["label"] == "Technology"
    assert latest[0]["percent"] > 90
    assert all(segment["label"] != "Other" for segment in latest)


def test_departments_overview_uses_contract_language():
    db = _db()
    today = date.today()
    db.add_all(
        [
            TickerMeta(symbol="LMT", company_name="Lockheed Martin Corporation", sector="Industrials"),
            TickerMeta(symbol="NVDA", company_name="NVIDIA Corp", sector="Technology"),
            GovernmentContract(
                award_id="A2",
                dedupe_key="A2",
                symbol="LMT",
                recipient_name="Lockheed Martin",
                award_date=today - timedelta(days=20),
                award_amount=2_500_000,
                awarding_agency="Department of Defense",
                source="test",
            ),
            GovernmentContractAction(
                parent_award_id="A2",
                modification_number="P0001",
                dedupe_key="A2-P0001",
                symbol="LMT",
                recipient_name="Lockheed Martin",
                company_name="Lockheed Martin Corporation",
                action_date=today - timedelta(days=10),
                obligated_amount=100_000,
                awarding_agency="Department of Defense",
                source="test",
            ),
            GovernmentContract(
                award_id="A3",
                dedupe_key="A3",
                symbol="NVDA",
                recipient_name="NVIDIA Corp",
                award_date=today - timedelta(days=30),
                award_amount=500_000,
                awarding_agency="Department of Health and Human Services",
                source="test",
            ),
            GovernmentContract(
                award_id="A4",
                dedupe_key="A4",
                symbol="LMT",
                recipient_name="Lockheed Martin",
                award_date=today - timedelta(days=400),
                award_amount=1_000_000,
                awarding_agency="Department of Defense",
                source="test",
            ),
            GovernmentContract(
                award_id="A5",
                dedupe_key="A5",
                symbol="LMT",
                recipient_name="Lockheed Martin",
                award_date=today + timedelta(days=30),
                award_amount=9_000_000,
                awarding_agency="Department of Defense",
                source="test",
            ),
        ]
    )
    db.commit()

    payload = departments_overview(db, period_days=365)

    assert payload["summary"][0]["label"] == "Total Contracts"
    assert payload["summary"][0]["value"] == 3
    assert payload["summary"][0]["previous_value"] == 1
    assert payload["summary"][1]["label"] == "Total Contract Value"
    assert payload["summary"][1]["value"] == 3_100_000
    assert payload["summary"][1]["previous_value"] == 1_000_000
    assert payload["summary"][2]["value"] == 2
    assert payload["summary"][2]["previous_value"] == 1
    assert payload["summary"][4]["value"] == 1
    assert payload["top_departments"][0]["name"] == "Department of Defense"
    assert payload["top_departments"][0]["previous_value"] == 1_000_000
    assert payload["top_departments"][0]["change_pct"] == 160
    assert payload["top_departments"][0]["top_vendor"] == "Lockheed Martin Corporation"
    assert payload["top_vendors"][0]["symbol"] == "LMT"
    assert payload["top_vendors"][0]["contract_value"] == 2_500_000
    assert payload["largest_recent_awards"][0]["value"] != 9_000_000
    sector_labels = {segment["label"] for row in payload["contract_value_over_time"] for segment in row["segments"]}
    assert {"Industrials", "Technology"}.issubset(sector_labels)
    assert "Other" not in sector_labels
