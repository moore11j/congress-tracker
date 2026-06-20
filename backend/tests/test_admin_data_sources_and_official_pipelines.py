from __future__ import annotations

from datetime import datetime, timezone

import pytest
from fastapi import HTTPException
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
from starlette.requests import Request

from app.auth import sign_session_payload
from app.db import Base, ensure_provider_control_schema
from app.models import Event, ProviderSettingAuditLog, UserAccount
from app.routers.admin_data_sources import (
    ProviderSettingPatchPayload,
    admin_data_sources_status,
    admin_update_data_source_setting,
)
from app.services.official_congress import (
    congress_transaction_hash,
    normalize_congress_transaction,
    parse_house_disclosure,
    promote_congress_shadow_events,
    stage_congress_disclosure_shadow,
)
from app.services.provider_settings import get_provider_settings_by_domain
from app.services.sec_form4 import (
    insider_transaction_hash,
    parse_form4_xml,
    promote_form4_shadow_events,
    stage_form4_shadow,
)


def _session():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False}, future=True)
    Base.metadata.create_all(engine)
    ensure_provider_control_schema(engine)
    Session = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    return Session()


def _request_for_user(user: UserAccount) -> Request:
    token = sign_session_payload({"uid": user.id, "email": user.email})
    return Request({"type": "http", "method": "GET", "path": "/", "headers": [(b"authorization", f"Bearer {token}".encode())]})


def _user(db, email: str, *, role: str = "user") -> UserAccount:
    user = UserAccount(email=email, role=role, entitlement_tier="free")
    db.add(user)
    db.commit()
    db.refresh(user)
    return user


FORM4_SAMPLE = """<?xml version="1.0"?>
<ownershipDocument>
  <periodOfReport>2026-06-01</periodOfReport>
  <issuer>
    <issuerCik>0000320193</issuerCik>
    <issuerName>Apple Inc.</issuerName>
    <issuerTradingSymbol>AAPL</issuerTradingSymbol>
  </issuer>
  <reportingOwner>
    <reportingOwnerId>
      <rptOwnerCik>0001111111</rptOwnerCik>
      <rptOwnerName>Example Insider</rptOwnerName>
    </reportingOwnerId>
    <reportingOwnerRelationship>
      <isDirector>1</isDirector>
      <isOfficer>1</isOfficer>
      <isTenPercentOwner>0</isTenPercentOwner>
      <officerTitle>Chief Example Officer</officerTitle>
    </reportingOwnerRelationship>
  </reportingOwner>
  <nonDerivativeTable>
    <nonDerivativeTransaction>
      <securityTitle><value>Common Stock</value></securityTitle>
      <transactionDate><value>2026-05-31</value></transactionDate>
      <transactionCoding><transactionCode>P</transactionCode></transactionCoding>
      <transactionAmounts>
        <transactionShares><value>10</value></transactionShares>
        <transactionPricePerShare><value>100</value></transactionPricePerShare>
        <transactionAcquiredDisposedCode><value>A</value></transactionAcquiredDisposedCode>
      </transactionAmounts>
      <postTransactionAmounts><sharesOwnedFollowingTransaction><value>110</value></sharesOwnedFollowingTransaction></postTransactionAmounts>
      <ownershipNature><directOrIndirectOwnership><value>D</value></directOrIndirectOwnership></ownershipNature>
    </nonDerivativeTransaction>
    <nonDerivativeTransaction>
      <securityTitle><value>Restricted Stock Units</value></securityTitle>
      <transactionDate><value>2026-05-30</value></transactionDate>
      <transactionCoding><transactionCode>A</transactionCode></transactionCoding>
      <transactionAmounts>
        <transactionShares><value>5</value></transactionShares>
        <transactionPricePerShare><value>0</value></transactionPricePerShare>
        <transactionAcquiredDisposedCode><value>A</value></transactionAcquiredDisposedCode>
      </transactionAmounts>
      <ownershipNature><directOrIndirectOwnership><value>D</value></directOrIndirectOwnership></ownershipNature>
    </nonDerivativeTransaction>
  </nonDerivativeTable>
  <derivativeTable>
    <derivativeTransaction>
      <securityTitle><value>Stock Option</value></securityTitle>
      <transactionDate><value>2026-05-29</value></transactionDate>
      <transactionCoding><transactionCode>M</transactionCode></transactionCoding>
      <transactionAmounts>
        <transactionShares><value>2</value></transactionShares>
        <transactionPricePerShare><value>50</value></transactionPricePerShare>
        <transactionAcquiredDisposedCode><value>A</value></transactionAcquiredDisposedCode>
      </transactionAmounts>
      <ownershipNature><directOrIndirectOwnership><value>I</value></directOrIndirectOwnership></ownershipNature>
    </derivativeTransaction>
  </derivativeTable>
</ownershipDocument>
"""


def test_provider_settings_defaults_and_admin_crud():
    db = _session()
    try:
        settings = get_provider_settings_by_domain(db)
        assert settings["congress_trades"].active_provider == "walnut_official"
        assert settings["congress_trades"].fallback_provider == "fmp"
        assert settings["congress_trades"].mode == "shadow"
        assert settings["insider_trades"].active_provider == "sec_edgar"

        admin = _user(db, "admin@example.com", role="admin")
        updated = admin_update_data_source_setting(
            "congress_trades",
            ProviderSettingPatchPayload(active_provider="fmp", mode="disabled", is_enabled=False, reason="test switch"),
            _request_for_user(admin),
            db,
        )

        assert updated["active_provider"] == "fmp"
        assert updated["mode"] == "disabled"
        audit = db.execute(select(ProviderSettingAuditLog)).scalar_one()
        assert audit.domain_key == "congress_trades"
        assert audit.previous_provider == "walnut_official"
        assert audit.new_provider == "fmp"
    finally:
        db.close()


def test_data_sources_status_requires_admin():
    db = _session()
    try:
        user = _user(db, "reader@example.com")
        with pytest.raises(HTTPException) as exc:
            admin_data_sources_status(_request_for_user(user), db)
        assert exc.value.status_code == 403

        admin = _user(db, "admin@example.com", role="admin")
        payload = admin_data_sources_status(_request_for_user(admin), db)
        assert "congress_trades" in payload["current_data_source_map"]
        assert "provider_settings" in payload["tables"]["official_shadow"]
    finally:
        db.close()


def test_congress_normalization_symbol_resolution_and_stable_hash():
    raw = {
        "filing_id": "H-123",
        "member_name": "Rep Example",
        "owner": "Spouse",
        "transactionDate": "2026-06-01",
        "symbol": "BRK.B",
        "assetDescription": "Berkshire Hathaway Class B",
        "assetType": "Stock",
        "transactionType": "Purchase",
        "amount": "$1,001 - $15,000",
    }
    parsed = parse_house_disclosure(raw)[0]

    assert parsed["ticker_normalized"] == "BRK-B"
    assert parsed["owner_normalized"] == "spouse"
    assert parsed["transaction_type_normalized"] == "purchase"
    assert parsed["amount_low"] == 1001
    assert parsed["amount_high"] == 15000
    assert congress_transaction_hash(parsed) == parsed["normalized_hash"]

    unresolved = normalize_congress_transaction(
        {"issuerName": "Private Company LLC", "assetType": "Private Equity", "transactionDate": "2026-06-01"},
        chamber="house",
        source_provider="official_house",
    )
    assert unresolved["symbol_resolution_status"] in {"unresolved", "private"}
    assert unresolved["symbol_resolution_status"] != "inactive"


def test_form4_xml_parses_codes_without_misclassifying_awards():
    parsed = parse_form4_xml(FORM4_SAMPLE, accession_number="0000320193-26-000001")
    transactions = parsed["transactions"]

    assert parsed["filing"]["issuer_cik"] == "0000320193"
    assert parsed["filing"]["ticker_normalized"] == "AAPL"
    assert [row["transaction_type_normalized"] for row in transactions] == [
        "open_market_purchase",
        "grant_award",
        "option_exercise_conversion",
    ]
    assert transactions[0]["value"] == 1000
    assert transactions[1]["transaction_code_description"] == "Grant or award"
    assert transactions[2]["is_derivative"] is True
    assert insider_transaction_hash(transactions[0]) == transactions[0]["normalized_hash"]


def test_shadow_tables_do_not_affect_feed_until_explicit_promotion():
    db = _session()
    try:
        stage_congress_disclosure_shadow(
            db,
            source_provider="official_house",
            chamber="house",
            raw={
                "filing_id": "H-456",
                "member_name": "Rep Example",
                "transactionDate": "2026-06-01",
                "symbol": "AAPL",
                "assetDescription": "Apple Inc.",
                "transactionType": "Purchase",
                "amount": "$1,001 - $15,000",
            },
        )
        stage_form4_shadow(db, xml_text=FORM4_SAMPLE, accession_number="0000320193-26-000001")
        db.commit()

        assert db.execute(select(Event)).scalars().all() == []

        congress_report = promote_congress_shadow_events(db)
        insider_report = promote_form4_shadow_events(db)
        db.commit()

        assert congress_report["inserted"] == 1
        assert insider_report["inserted"] == 1
        events = db.execute(select(Event).order_by(Event.event_type.asc())).scalars().all()
        assert [event.event_type for event in events] == ["congress_trade", "insider_trade"]
        assert {event.source_provider for event in events} == {"official_house", "sec_edgar"}

        assert promote_congress_shadow_events(db)["inserted"] == 0
        assert promote_form4_shadow_events(db)["inserted"] == 0
    finally:
        db.close()
