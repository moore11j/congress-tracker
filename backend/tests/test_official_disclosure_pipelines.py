from __future__ import annotations

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from app.db import Base, ensure_provider_control_schema
from app.models import Event
from app.services.official_congress import (
    congress_transaction_hash,
    normalize_congress_transaction,
    parse_house_disclosure,
    promote_congress_shadow_events,
    stage_congress_disclosure_shadow,
)
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

    allstate_with_bad_symbol = normalize_congress_transaction(
        {
            "assetDescription": "ALLSTATE CORPORATION COMMON STOCK",
            "symbol": "SNDK",
            "assetType": "Stock",
            "transactionDate": "2026-06-01",
        },
        chamber="house",
        source_provider="official_house",
    )
    assert allstate_with_bad_symbol["ticker_normalized"] == "ALL"

    sandisk_without_symbol = normalize_congress_transaction(
        {
            "assetDescription": "SANDISK LLC CMN",
            "symbol": "",
            "assetType": "Stock",
            "transactionDate": "2026-01-29",
        },
        chamber="house",
        source_provider="official_house",
    )
    assert sandisk_without_symbol["ticker_normalized"] == "SNDK"
    assert sandisk_without_symbol["symbol_resolution_status"] == "resolved"

    sandisk_with_symbol = normalize_congress_transaction(
        {
            "assetDescription": "SANDISK CORPORATION - COMMON STOCK",
            "symbol": "SNDK",
            "assetType": "Stock",
            "transactionDate": "2026-01-29",
        },
        chamber="house",
        source_provider="official_house",
    )
    assert sandisk_with_symbol["ticker_normalized"] == "SNDK"

    western_digital_with_bad_sndk = normalize_congress_transaction(
        {
            "assetDescription": "WESTERN DIGITAL CORPORATION CMN",
            "symbol": "SNDK",
            "assetType": "Stock",
            "transactionDate": "2026-03-23",
        },
        chamber="house",
        source_provider="official_house",
    )
    assert western_digital_with_bad_sndk["ticker_normalized"] is None
    assert western_digital_with_bad_sndk["symbol_resolution_status"] == "issuer_symbol_conflict"


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
