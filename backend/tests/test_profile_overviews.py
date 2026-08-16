from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from app.db import Base
from app.main import _profile_overview_database_cache_get, _profile_overview_persistent_key, _run_profile_overview_prewarm
from app.models import Event, GovernmentContract, GovernmentContractAction, InsiderTransactionNormalized, InstitutionalHolder, InstitutionalPosition, InstitutionalPositionChange, Member, Security, TickerContextBundleCache, TickerMeta
from app.services.profile_overviews import congress_overview, departments_overview, insiders_overview, institutions_overview, profile_activity, profiles_summary


def _db() -> Session:
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)
    return Session(engine)


def _add_months(value: date, months: int) -> date:
    month_index = (value.year * 12 + value.month - 1) + months
    return date(month_index // 12, month_index % 12 + 1, 1)


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
    for index in range(25):
        cik = f"99999999{index:02d}"
        db.add(
            InstitutionalHolder(
                cik=cik,
                holder_name=f"Long View Capital {index}",
                latest_filing_date=date(2026, 8, 1),
                latest_report_year=2026,
                latest_report_quarter=2,
            )
        )
        db.add(
            InstitutionalPosition(
                filing_id=300 + index,
                cik=cik,
                symbol="NVDA",
                normalized_symbol="NVDA",
                issuer_name="NVIDIA Corp",
                shares=100,
                value_usd=1_000_000,
                report_year=2026,
                report_quarter=2,
                filing_date=date(2026, 8, 1),
            )
        )
    db.add_all(
        [
            InstitutionalPositionChange(
                cik="9999999999",
                holder_name="Long View Capital",
                symbol="NVDA",
                normalized_symbol="NVDA",
                report_year=2026,
                report_quarter=2,
                filing_date=date(2026, 8, 1),
                shares_delta=20,
                value_delta_usd=200_000,
                change_type="increase",
            ),
            InstitutionalPositionChange(
                cik="9999999999",
                holder_name="Long View Capital",
                symbol="AAPL",
                normalized_symbol="AAPL",
                report_year=2026,
                report_quarter=2,
                filing_date=date(2026, 8, 1),
                shares_delta=-10,
                value_delta_usd=-50_000,
                change_type="decrease",
            ),
        ]
    )
    db.commit()

    payload = profiles_summary(db, include_institutions=False, include_activity=True)
    activity_mix = {item["type"]: item["value"] for item in payload["activity_mix"]}

    congress = next(card for card in payload["cards"] if card["kind"] == "congress")
    insiders = next(card for card in payload["cards"] if card["kind"] == "insiders")
    departments = next(card for card in payload["cards"] if card["kind"] == "departments")
    assert congress["metrics"][0]["value"] == 1
    assert insiders["metrics"][1]["value"] == 1
    assert departments["metrics"][1]["value"] == 1_000_000
    assert payload["directories"][0]["primary_title"] == "Top Congress by Trading Value"
    assert payload["activity"][0]["profile_href"] == "/member/nancy-pelosi"
    assert activity_mix == {"Congress": 1, "Insider": 1, "Institution": 2, "Department": 1}


def test_profile_activity_per_type_limit_preserves_each_activity_tab():
    db = _db()
    now = datetime.now(timezone.utc)
    event_types = (
        ("congress_trade", "Congress"),
        ("insider_trade", "Insider"),
        ("government_contract", "Department"),
    )
    event_id = 1
    for event_type, profile_name in event_types:
        for offset in range(7):
            db.add(
                Event(
                    id=event_id,
                    event_type=event_type,
                    ts=now - timedelta(minutes=event_id),
                    event_date=now - timedelta(minutes=event_id),
                    source="test",
                    payload_json=json.dumps({"insider_name": profile_name, "department": profile_name}),
                    member_name=profile_name if event_type == "congress_trade" else None,
                )
            )
            event_id += 1
    db.commit()

    activity = profile_activity(db, per_type_limit=5, include_institutions=False)
    counts = {activity_type: sum(item["type"] == activity_type for item in activity) for activity_type in ("Congress", "Insider", "Department")}

    assert len(activity) == 15
    assert counts == {"Congress": 5, "Insider": 5, "Department": 5}


def test_profile_activity_labels_unnamed_insider_filings_and_keeps_them_visible():
    db = _db()
    now = datetime.now(timezone.utc)
    for event_id in range(1, 8):
        db.add(
            Event(
                id=event_id,
                event_type="insider_trade",
                ts=now - timedelta(minutes=event_id),
                symbol="WALN",
                source="test",
                payload_json=json.dumps({"issuer_name": "Walnut Markets"}),
            )
        )
    db.commit()

    activity = profile_activity(db, per_type_limit=5, include_institutions=False)

    assert len(activity) == 5
    assert all(item["profile"] == "Walnut Markets insider filing" for item in activity)
    assert all(item["profile_href"] is None for item in activity)


def test_profiles_summary_cache_ignores_payload_without_activity_mix():
    db = _db()
    now = datetime.now(timezone.utc)
    key = ("profiles_summary", "all", 100, True, True)
    db.add(
        TickerContextBundleCache(
            cache_key=_profile_overview_persistent_key(key),
            symbol="PROFILE_OVERVIEW",
            user_segment="shared",
            payload_json=json.dumps({"activity": []}),
            generated_at=now,
            stale_after=now + timedelta(hours=1),
            expires_at=now + timedelta(days=1),
        )
    )
    db.commit()

    assert _profile_overview_persistent_key(key).startswith("profile-overview:v19:")
    assert _profile_overview_database_cache_get(db, key, now=now) is None


def test_profiles_summary_cache_serves_stale_payload_before_expiry():
    db = _db()
    now = datetime.now(timezone.utc)
    key = ("profiles_summary", "all", 100, False, True)
    payload = {
        "status": "ok",
        "activity": [{"id": "cached"}],
        "activity_mix": [{"type": "Congress", "value": 1}],
    }
    db.add(
        TickerContextBundleCache(
            cache_key=_profile_overview_persistent_key(key),
            symbol="PROFILE_OVERVIEW",
            user_segment="shared",
            payload_json=json.dumps(payload),
            generated_at=now - timedelta(hours=2),
            stale_after=now - timedelta(hours=1),
            expires_at=now + timedelta(hours=22),
        )
    )
    db.commit()

    assert _profile_overview_database_cache_get(db, key, now=now) == payload


def test_profile_subpage_overview_caches_serve_stale_payloads_before_expiry():
    db = _db()
    now = datetime.now(timezone.utc)
    keys = [
        ("profiles_congress_overview", "all", 365),
        ("profiles_insiders_overview", "", 365),
        ("profiles_institutions_overview", None, None, True),
        ("profiles_departments_overview", None, 365),
    ]

    for index, key in enumerate(keys):
        payload = {"status": "ok", "marker": index}
        db.add(
            TickerContextBundleCache(
                cache_key=_profile_overview_persistent_key(key),
                symbol="PROFILE_OVERVIEW",
                user_segment="shared",
                payload_json=json.dumps(payload),
                generated_at=now - timedelta(hours=2),
                stale_after=now - timedelta(hours=1),
                expires_at=now + timedelta(hours=22),
            )
        )
    db.commit()

    for index, key in enumerate(keys):
        assert _profile_overview_database_cache_get(db, key, now=now) == {"status": "ok", "marker": index}


def test_profile_overview_prewarm_warms_public_and_entitled_summaries(monkeypatch):
    from app import main

    seen_keys = []

    class FakeSession:
        closed = False

        def close(self):
            self.closed = True

    session = FakeSession()
    monkeypatch.setattr(main, "SessionLocal", lambda: session)
    monkeypatch.setattr(
        main,
        "_cached_profile_overview_response",
        lambda db, key, builder: seen_keys.append((db, key)),
    )

    _run_profile_overview_prewarm()

    assert [key for _, key in seen_keys] == [
        ("profiles_summary", "all", 25, False, True, 5),
        ("profiles_summary", "all", 25, True, True, 5),
        ("profiles_institutions_overview", None, None, False),
        ("profiles_institutions_overview", None, None, True),
    ]
    assert session.closed


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
    prior_period = datetime.now(timezone.utc) - timedelta(days=500)
    db.add_all(
        [
            Event(
                id=30,
                event_type="congress_trade",
                ts=prior_period,
                event_date=prior_period,
                symbol="NVDA",
                source="test",
                payload_json="{}",
                member_name="Nancy Pelosi",
                member_bioguide_id="P000197",
                chamber="house",
                party="D",
                trade_type="purchase",
                amount_min=100_000,
                amount_max=100_000,
            ),
            Event(
                id=31,
                event_type="congress_trade",
                ts=prior_period + timedelta(days=1),
                event_date=prior_period + timedelta(days=1),
                symbol="NVDA",
                source="test",
                payload_json="{}",
                member_name="Nancy Pelosi",
                member_bioguide_id="P000197",
                chamber="house",
                party="D",
                trade_type="sale",
                amount_min=95_000,
                amount_max=95_000,
            ),
        ]
    )
    db.commit()

    payload = congress_overview(db, chamber="house", period_days=365)

    assert payload["summary"][0]["label"] == "Total Trades"
    assert payload["summary"][0]["value"] == 2
    assert len(payload["monthly_activity"]) == 12
    assert payload["top_members"][0]["href"] == "/member/nancy-pelosi"
    assert payload["most_traded_stocks"][0]["symbol"] == "NVDA"
    assert payload["most_traded_stocks"][0]["trades"] == 1
    sector_labels = {segment["label"] for segment in payload["sector_exposure"][0]["segments"]}
    assert sector_labels == {"Technology"}
    assert len([row for row in payload["top_buyers"] if row["name"] == "Nancy Pelosi"]) == 1
    assert payload["top_buyers"][0]["trades"] == 2
    assert payload["snapshot"]["most_traded_ticker"]["net_value"] == 100_000
    assert payload["snapshot"]["top_buyer"]["value"] == 115_000
    assert payload["snapshot"]["most_active_sector"]["sector"] == "Technology"
    assert payload["snapshot"]["most_active_sector"]["trades"] == 2
    assert payload["snapshot"]["most_active_sector"]["trade_percent"] == 100.0
    assert payload["top_moving_sectors"][0]["current_activity_value"] == 115_000
    assert payload["top_moving_sectors"][0]["previous_activity_value"] == 195_000
    assert payload["top_moving_sectors"][0]["change_pct"] == -41.02564102564102


def test_congress_snapshot_most_active_member_uses_trade_count_not_value():
    db = _db()
    now = datetime.now(timezone.utc) - timedelta(days=2)
    db.add(Security(symbol="AAPL", name="Apple Inc.", asset_class="stock", sector="Technology"))
    db.add(Security(symbol="NVDA", name="NVIDIA Corp", asset_class="stock", sector="Technology"))
    db.add(
        Event(
            id=1001,
            event_type="congress_trade",
            ts=now,
            event_date=now,
            symbol="AAPL",
            source="test",
            payload_json="{}",
            member_name="Nancy Pelosi",
            member_bioguide_id="P000197",
            chamber="house",
            party="D",
            trade_type="purchase",
            amount_min=1_000_000,
            amount_max=1_000_000,
        )
    )
    db.add_all(
        [
            Event(
                id=1010 + index,
                event_type="congress_trade",
                ts=now + timedelta(minutes=index + 1),
                event_date=now + timedelta(minutes=index + 1),
                symbol="NVDA",
                source="test",
                payload_json="{}",
                member_name="Ro Khanna",
                member_bioguide_id="K000389",
                chamber="house",
                party="D",
                trade_type="purchase",
                amount_min=1_000,
                amount_max=1_000,
            )
            for index in range(3)
        ]
    )
    db.commit()

    payload = congress_overview(db, chamber="house", period_days=365)

    assert payload["top_members"][0]["name"] == "Nancy Pelosi"
    assert payload["snapshot"]["top_member"]["name"] == "Ro Khanna"
    assert payload["snapshot"]["top_member"]["trades"] == 3


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
            InsiderTransactionNormalized(
                accession_number="insider-prior",
                issuer_name="Exponent, Inc.",
                ticker_raw="EXPO",
                ticker_normalized="EXPO",
                reporting_owner_cik="3333",
                reporting_owner_name="Prior Seller",
                transaction_date=today - timedelta(days=500),
                filing_date=today - timedelta(days=499),
                transaction_code="S",
                transaction_type_normalized="open_market_sale",
                value=100_000,
                normalized_hash="insider-prior-1",
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
    assert len(payload["monthly_activity"]) == 12
    assert payload["monthly_activity"][-1]["period"] == today.strftime("%b %y")
    assert payload["monthly_activity"][-1]["net_value"] == 300_000
    assert payload["monthly_activity"][-1]["trades"] == 3
    assert payload["monthly_activity"][-1]["active_insiders"] == 2
    assert payload["monthly_activity"][-1]["average_trade_size"] == 133333.33333333334
    assert payload["top_insiders"][0]["name"] == "John Buyer"
    assert payload["top_insiders"][0]["href"] == "/insider/john-buyer-0000002222"
    assert payload["recent_purchases"][0]["profile"] != "Profile unavailable"
    assert payload["recent_purchases"][0]["profile_href"]
    assert payload["most_traded_stocks"][0]["actor_count"] == 2
    assert payload["sector_activity"][0]["segments"][0]["label"] == "Industrials"
    assert payload["sector_net_activity"][0]["sector"] == "Industrials"
    assert payload["sector_net_activity"][0]["current_value"] == 300_000
    assert payload["sector_net_activity"][0]["buy_value"] == 350_000
    assert payload["sector_net_activity"][0]["sell_value"] == 50_000
    role_mix = {row["label"]: row for row in payload["role_mix"]}
    assert role_mix["Directors"]["value"] == 1
    assert role_mix["Officers"]["value"] == 1
    assert role_mix["Other"]["value"] == 1
    assert payload["top_moving_sectors"][0]["sector"] == "Industrials"
    assert payload["top_moving_sectors"][0]["current_value"] == 300_000
    assert payload["top_moving_sectors"][0]["current_activity_value"] == 400_000
    assert payload["top_moving_sectors"][0]["previous_activity_value"] == 100_000
    assert payload["top_moving_sectors"][0]["change_pct"] == 300.0
    assert payload["recent_notable_trades"][0]["activity"] == "Open-Market Sale"
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
                cik=f"000000100{index}" if index == 1 else f"100{index}",
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
    alpha_row = next(row for row in payload["top_institutions"] if row["cik"] == "0000001001")
    assert alpha_row["previous_value"] == 500_000
    assert alpha_row["qoq_change"] == 100.0
    latest = payload["sector_exposure"][-1]["segments"]
    assert latest[0]["label"] == "Technology"
    assert latest[0]["percent"] > 90
    assert all(segment["label"] != "Other" for segment in latest)
    activity_by_period = {row["period"]: row for row in payload["institutional_activity_over_time"]}
    assert activity_by_period["Q1 2026"]["position_increase_value"] == 25_000
    assert activity_by_period["Q1 2026"]["position_decrease_value"] == 0
    assert activity_by_period["Q1 2026"]["total_positions"] == 6
    assert activity_by_period["Q2 2026"]["position_increase_value"] == 100_000
    assert activity_by_period["Q2 2026"]["position_decrease_value"] == -50_000
    assert activity_by_period["Q2 2026"]["total_positions"] == 7


def test_institutions_overview_skips_sparse_newer_period():
    db = _db()
    db.add(TickerMeta(symbol="AAPL", company_name="Apple Inc.", sector="Technology"))
    db.add(TickerMeta(symbol="MSFT", company_name="Microsoft Corp.", sector="Technology"))
    for index in range(30):
        db.add(
            InstitutionalPosition(
                filing_id=10_000 + index,
                cik=f"q1-{index}",
                symbol="AAPL",
                normalized_symbol="AAPL",
                issuer_name="Apple Inc.",
                shares=100,
                value_usd=1_000_000,
                report_year=2026,
                report_quarter=1,
                filing_date=date(2026, 5, 15),
            )
        )
    for index in range(2):
        db.add(
            InstitutionalPosition(
                filing_id=20_000 + index,
                cik=f"q2-{index}",
                symbol="MSFT",
                normalized_symbol="MSFT",
                issuer_name="Microsoft Corp.",
                shares=100,
                value_usd=1_000_000,
                report_year=2026,
                report_quarter=2,
                filing_date=date(2026, 8, 1),
            )
        )
    db.commit()

    payload = institutions_overview(db, include_details=True)

    assert payload["report_year"] == 2026
    assert payload["report_quarter"] == 1
    assert payload["previous_report_year"] is None
    assert payload["previous_report_quarter"] is None
    assert payload["summary"][0]["value"] == 30
    assert payload["summary"][0]["previous_value"] is None
    assert all(row["period"] != "Q2 2026" for row in payload["sector_exposure"])


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


def test_departments_overview_suppresses_deltas_when_recent_ingest_is_undercovered():
    db = _db()
    today = date.today()
    month_end = date(today.year, today.month, 1)
    current_month = _add_months(month_end, -2)
    previous_month = _add_months(current_month, -12)
    db.add(TickerMeta(symbol="LMT", company_name="Lockheed Martin Corporation", sector="Industrials"))
    for index in range(600):
        db.add(
            GovernmentContract(
                award_id=f"PREV-{index}",
                dedupe_key=f"PREV-{index}",
                symbol="LMT",
                recipient_name="Lockheed Martin",
                award_date=previous_month + timedelta(days=index % 20),
                award_amount=1_000_000,
                awarding_agency="Department of Defense",
                source="test",
            )
        )
    for index in range(10):
        db.add(
            GovernmentContract(
                award_id=f"CURR-{index}",
                dedupe_key=f"CURR-{index}",
                symbol="LMT",
                recipient_name="Lockheed Martin",
                award_date=current_month + timedelta(days=index % 20),
                award_amount=1_000_000,
                awarding_agency="Department of Defense",
                source="test",
            )
        )
    db.commit()

    payload = departments_overview(db, period_days=365)

    assert payload["comparison"]["status"] == "undercovered"
    assert payload["summary"][0]["value"] == 10
    assert payload["summary"][0]["previous_value"] is None
    assert payload["summary"][0]["change_pct"] is None
    assert payload["top_departments"][0]["previous_value"] is None
    assert payload["top_departments"][0]["change_pct"] is None
