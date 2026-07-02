from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest
from fastapi import HTTPException
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
from starlette.requests import Request

from app.auth import SESSION_COOKIE_NAME, sign_session_payload
from app.db import Base, ensure_provider_control_schema
from app.models import CongressTransactionNormalized, DataEnrichmentJob, Event, ProviderSettingAuditLog, ProviderUsageEvent, UserAccount
from app.routers.admin_data_sources import (
    DataSourceEndpointTestPayload,
    DataSourceRunPayload,
    ProviderSettingPatchPayload,
    admin_data_sources_status,
    admin_run_data_source,
    admin_test_data_source_endpoint,
    admin_update_data_source_setting,
)
from app.services.official_congress import (
    congress_transaction_hash,
    normalize_congress_transaction,
    parse_house_disclosure,
    promote_congress_shadow_events,
    stage_congress_disclosure_shadow,
)
from app.services.provider_settings import PROVIDER_ENDPOINT_VALIDATION_CLEANUP_REASON, PROVIDER_VALIDATION_CLEANUP_REASON, get_provider_settings_by_domain
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
    return Request({"type": "http", "method": "GET", "path": "/", "headers": [(b"cookie", f"{SESSION_COOKIE_NAME}={token}".encode())]})


def _user(db, email: str, *, role: str = "user") -> UserAccount:
    user = UserAccount(email=email, role=role, entitlement_tier="free")
    db.add(user)
    db.commit()
    db.refresh(user)
    return user


def _feed_event(event_type: str, *, source_provider: str, source_filing_id: str) -> Event:
    ts = datetime(2026, 6, 1, tzinfo=timezone.utc)
    return Event(
        event_type=event_type,
        ts=ts,
        event_date=ts,
        source=source_provider,
        payload_json=json.dumps({"source_provider": source_provider, "source_filing_id": source_filing_id}),
        source_provider=source_provider,
        source_filing_id=source_filing_id,
    )


class _FakeProviderResponse:
    def __init__(self, status_code: int = 200, payload=None):
        self.status_code = status_code
        self._payload = [] if payload is None else payload
        self.text = json.dumps(self._payload)

    def json(self):
        return self._payload


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
        assert settings["prices_intraday"].active_provider == "fmp"
        assert settings["prices_intraday"].fallback_provider == "fmp"
        assert settings["prices_intraday"].primary_endpoint_url.endswith("/stable/historical-chart/1min?symbol={symbol}")
        assert settings["prices_intraday"].fallback_endpoint_url.endswith("/stable/historical-price-eod/light?symbol={symbol}")
        assert '"price_field":"close"' in settings["prices_intraday"].primary_endpoint_contract_json
        assert '"date_format":"YYYY-MM-DD HH:MM:SS"' in settings["prices_intraday"].primary_endpoint_contract_json
        assert '"price_field":"price"' in settings["prices_intraday"].fallback_endpoint_contract_json
        assert settings["insider_trades"].active_provider == "sec_edgar"
        assert settings["pnl_enrichment"].active_provider == "internal_computed"
        assert settings["pnl_enrichment"].fallback_provider == "walnut_cache"
        assert settings["signal_inputs"].active_provider == "internal_computed"
        assert settings["signal_inputs"].fallback_provider == "walnut_cache"

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


def test_provider_endpoint_urls_are_saved_and_exposed():
    db = _session()
    try:
        admin = _user(db, "admin@example.com", role="admin")
        request = _request_for_user(admin)
        primary_url = "https://financialmodelingprep.com/stable/historical-chart/1min?symbol=AAPL"
        fallback_url = "https://financialmodelingprep.com/stable/historical-price-eod/light?symbol=AAPL"
        primary_contract = '{"response":{"price_field":"close","date_field":"date","date_format":"YYYY-MM-DD HH:MM:SS"}}'
        fallback_contract = '{"response":{"price_field":"price","date_field":"date","date_format":"YYYY-MM-DD"}}'

        updated = admin_update_data_source_setting(
            "prices_intraday",
            ProviderSettingPatchPayload(
                active_provider="fmp",
                fallback_provider="fmp",
                primary_endpoint_url=primary_url,
                fallback_endpoint_url=fallback_url,
                primary_endpoint_contract_json=primary_contract,
                fallback_endpoint_contract_json=fallback_contract,
                reason="endpoint switch",
            ),
            request,
            db,
        )
        assert updated["primary_endpoint_url"] == primary_url
        assert updated["fallback_endpoint_url"] == fallback_url
        assert json.loads(updated["primary_endpoint_contract_json"])["response"]["price_field"] == "close"
        assert json.loads(updated["fallback_endpoint_contract_json"])["response"]["date_format"] == "YYYY-MM-DD"

        payload = admin_data_sources_status(request, db)
        row = {item["domain_key"]: item for item in payload["domains"]}["prices_intraday"]
        assert row["endpoint_urls"]["primary"] == primary_url
        assert row["endpoint_urls"]["fallback"] == fallback_url
        assert json.loads(row["endpoint_contracts"]["primary"])["response"]["date_format"] == "YYYY-MM-DD HH:MM:SS"
        assert "historical-chart/1min?symbol=AAPL" in row["endpoint_names"]

        with pytest.raises(HTTPException) as exc:
            admin_update_data_source_setting(
                "prices_intraday",
                ProviderSettingPatchPayload(primary_endpoint_url=f"{primary_url}&apikey=secret", reason="bad secret"),
                request,
                db,
            )
        assert exc.value.status_code == 400
        assert "must not include an API key" in str(exc.value.detail)

        templated_url = "https://financialmodelingprep.com/stable/historical-price-eod/light?symbol=[symbol]"
        updated = admin_update_data_source_setting(
            "prices_intraday",
            ProviderSettingPatchPayload(primary_endpoint_url=templated_url, reason="alternate template syntax"),
            request,
            db,
        )
        assert updated["primary_endpoint_url"] == templated_url

        with pytest.raises(HTTPException) as exc:
            admin_update_data_source_setting(
                "prices_intraday",
                ProviderSettingPatchPayload(
                    primary_endpoint_url="https://financialmodelingprep.com/stable/historical-chart/1min?symbol=",
                    reason="bad blank symbol",
                ),
                request,
                db,
            )
        assert exc.value.status_code == 400
        assert "{symbol}/[symbol]" in str(exc.value.detail)
        assert "historical-chart/1min?symbol={symbol}" in str(exc.value.detail)

        with pytest.raises(HTTPException) as exc:
            admin_update_data_source_setting(
                "prices_intraday",
                ProviderSettingPatchPayload(primary_endpoint_contract_json='["not", "object"]', reason="bad contract"),
                request,
                db,
            )
        assert exc.value.status_code == 400
        assert "Endpoint contract JSON must be an object" in str(exc.value.detail)
    finally:
        db.close()


def test_data_source_status_repairs_blank_symbol_endpoint_to_default():
    db = _session()
    try:
        admin = _user(db, "admin@example.com", role="admin")
        request = _request_for_user(admin)
        settings = get_provider_settings_by_domain(db)
        bad_url = "https://financialmodelingprep.com/stable/historical-chart/1min?symbol="
        settings["prices_intraday"].primary_endpoint_url = bad_url
        db.commit()

        payload = admin_data_sources_status(request, db)
        row = {item["domain_key"]: item for item in payload["domains"]}["prices_intraday"]
        repaired_url = "https://financialmodelingprep.com/stable/historical-chart/1min?symbol={symbol}"
        assert row["endpoint_urls"]["primary"] == repaired_url
        assert row["endpoint_names"][0] == "historical-chart/1min?symbol=AAPL"
        assert json.loads(row["endpoint_contracts"]["primary"])["response"]["price_field"] == "close"

        refreshed = get_provider_settings_by_domain(db)["prices_intraday"]
        assert refreshed.primary_endpoint_url == repaired_url
        audit = db.execute(
            select(ProviderSettingAuditLog).where(
                ProviderSettingAuditLog.domain_key == "prices_intraday",
                ProviderSettingAuditLog.reason == PROVIDER_ENDPOINT_VALIDATION_CLEANUP_REASON,
            )
        ).scalar_one()
        assert audit.changed_by == "system"
    finally:
        db.close()


def test_data_source_status_migrates_legacy_intraday_defaults_to_chart_primary():
    db = _session()
    try:
        admin = _user(db, "admin@example.com", role="admin")
        request = _request_for_user(admin)
        settings = get_provider_settings_by_domain(db)
        settings["prices_intraday"].primary_endpoint_url = "https://financialmodelingprep.com/stable/historical-price-eod/light?symbol={symbol}"
        settings["prices_intraday"].fallback_endpoint_url = "https://financialmodelingprep.com/stable/quote-short?symbol={symbol}"
        db.commit()

        payload = admin_data_sources_status(request, db)
        row = {item["domain_key"]: item for item in payload["domains"]}["prices_intraday"]
        assert row["endpoint_urls"]["primary"].endswith("/historical-chart/1min?symbol={symbol}")
        assert row["endpoint_urls"]["fallback"].endswith("/historical-price-eod/light?symbol={symbol}")
        assert json.loads(row["endpoint_contracts"]["primary"])["response"]["price_field"] == "close"
        assert json.loads(row["endpoint_contracts"]["fallback"])["response"]["price_field"] == "price"
    finally:
        db.close()


def test_data_source_status_uses_endpoint_test_errors_not_provider_wide_errors():
    db = _session()
    try:
        admin = _user(db, "admin@example.com", role="admin")
        db.add(
            ProviderUsageEvent(
                provider="fmp",
                category="quote",
                endpoint="quote",
                success=False,
                error="provider_disabled",
            )
        )
        db.commit()

        payload = admin_data_sources_status(_request_for_user(admin), db)
        rows = {row["domain_key"]: row for row in payload["domains"]}

        assert rows["profiles"]["last_error"] is None
        assert rows["earnings"]["last_error"] is None
        assert rows["analyst_estimates"]["last_error"] is None
        assert rows["institutional_13f"]["last_error"] is None
    finally:
        db.close()


def test_admin_endpoint_test_records_domain_specific_health(monkeypatch):
    db = _session()
    try:
        admin = _user(db, "admin@example.com", role="admin")
        request = _request_for_user(admin)
        calls: list[tuple[str, dict]] = []

        monkeypatch.setenv("FMP_API_KEY", "secret-key")
        monkeypatch.setenv("FMP_PERSIST_USAGE_EVENTS", "0")

        def fake_get(url, params=None, timeout=10):
            calls.append((url, dict(params or {})))
            assert params["apikey"] == "secret-key"
            assert "secret-key" not in url
            return _FakeProviderResponse(200, [{"symbol": params.get("symbol", "AAPL"), "price": 190.0}])

        monkeypatch.setattr("app.services.provider_endpoints.requests.get", fake_get)

        result = admin_test_data_source_endpoint(
            "profiles",
            DataSourceEndpointTestPayload(symbol="AAPL", reason="health check"),
            request,
            db,
        )
        assert result["results"]["primary"]["status"] == "healthy"
        assert result["results"]["fallback"]["status"] == "skipped"
        assert calls[0][0].endswith("/stable/profile")
        assert calls[0][1]["symbol"] == "AAPL"

        payload = admin_data_sources_status(request, db)
        row = {item["domain_key"]: item for item in payload["domains"]}["profiles"]
        assert row["endpoint_tests"]["primary"]["status"] == "healthy"
        assert row["last_error"] is None
        assert "secret-key" not in json.dumps(payload)
    finally:
        db.close()


def test_provider_patch_is_config_only_and_preserves_feed_and_shadow_rows():
    db = _session()
    try:
        admin = _user(db, "admin@example.com", role="admin")
        db.add(_feed_event("congress_trade", source_provider="fmp", source_filing_id="legacy-fmp-event"))
        stage_congress_disclosure_shadow(
            db,
            source_provider="official_house",
            chamber="house",
            raw={
                "filing_id": "H-SAFE-SWITCH",
                "member_name": "Rep Example",
                "transactionDate": "2026-06-01",
                "symbol": "AAPL",
                "assetDescription": "Apple Inc.",
                "transactionType": "Purchase",
                "amount": "$1,001 - $15,000",
            },
        )
        db.commit()

        before_events = [(row.event_type, row.source_provider, row.source_filing_id) for row in db.execute(select(Event)).scalars().all()]
        before_shadow_hashes = [row[0] for row in db.execute(select(CongressTransactionNormalized.normalized_hash)).all()]

        updated = admin_update_data_source_setting(
            "congress_trades",
            ProviderSettingPatchPayload(active_provider="fmp", fallback_provider="none", mode="shadow", is_enabled=True, reason="switch future provider"),
            _request_for_user(admin),
            db,
        )

        after_events = [(row.event_type, row.source_provider, row.source_filing_id) for row in db.execute(select(Event)).scalars().all()]
        after_shadow_hashes = [row[0] for row in db.execute(select(CongressTransactionNormalized.normalized_hash)).all()]
        audits = db.execute(select(ProviderSettingAuditLog)).scalars().all()

        assert updated["active_provider"] == "fmp"
        assert before_events == after_events
        assert before_shadow_hashes == after_shadow_hashes
        assert len(audits) == 1
        assert audits[0].domain_key == "congress_trades"
    finally:
        db.close()


def test_shadow_provider_run_queues_dry_run_without_changing_public_feed_count():
    db = _session()
    try:
        admin = _user(db, "admin@example.com", role="admin")
        before_count = len(db.execute(select(Event)).scalars().all())

        result = admin_run_data_source(
            "insider_trades",
            DataSourceRunPayload(mode="shadow", reason="safe shadow refresh"),
            _request_for_user(admin),
            db,
        )

        after_count = len(db.execute(select(Event)).scalars().all())
        job = db.execute(select(DataEnrichmentJob)).scalar_one()
        payload = json.loads(job.payload_json)

        assert before_count == after_count == 0
        assert result["status"] == "queued"
        assert result["mode"] == "shadow"
        assert result["dry_run"] is True
        assert job.job_type == "sec_form4_ingest"
        assert payload["dry_run"] is True
        assert payload["source"] == "admin_data_sources"
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


def test_data_sources_status_exposes_switch_readiness_and_optional_history():
    db = _session()
    try:
        admin = _user(db, "admin@example.com", role="admin")
        stage_congress_disclosure_shadow(
            db,
            source_provider="official_house",
            chamber="house",
            raw={
                "filing_id": "H-READINESS",
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

        payload = admin_data_sources_status(_request_for_user(admin), db)
        congress = payload["diagnostics"]["congress"]
        insider = payload["diagnostics"]["insider"]

        assert congress["public_feed_impact"] == "none"
        assert congress["existing_data_preserved"] is True
        assert congress["duplicate_candidates"] == 0
        assert congress["would_insert_count"] == 1
        assert congress["would_skip_duplicate_count"] == 0
        assert congress["readiness_status"] == "ready_for_limited_forward_ingest"
        assert "comparison" in congress
        assert congress["comparison"]["missing_in_official"] == 0

        assert insider["public_feed_impact"] == "none"
        assert insider["existing_data_preserved"] is True
        assert insider["normalized_transactions"] == 3
        assert insider["duplicate_candidates"] == 0
        assert insider["readiness_status"] == "ready_for_limited_forward_ingest"
        assert "comparison" in insider
        assert "missing_in_sec" in insider["comparison"]
    finally:
        db.close()


def test_provider_settings_domain_aware_validation_matrix():
    db = _session()
    try:
        admin = _user(db, "admin@example.com", role="admin")
        request = _request_for_user(admin)
        get_provider_settings_by_domain(db)
        db.commit()

        def patch(domain_key: str, **payload):
            return admin_update_data_source_setting(
                domain_key,
                ProviderSettingPatchPayload(reason="matrix test", **payload),
                request,
                db,
            )

        def assert_bad(domain_key: str, expected: str, **payload):
            with pytest.raises(HTTPException) as exc:
                patch(domain_key, **payload)
            assert exc.value.status_code == 400
            assert expected in str(exc.value.detail)

        assert_bad("prices_eod", "FRED is not allowed for EOD equity prices", active_provider="fred")
        assert_bad("prices_eod", "SEC EDGAR is not allowed for EOD equity prices", active_provider="sec_edgar")
        assert_bad("insights_macro", "FMP is not allowed for Insights: US Macro", active_provider="fmp")
        assert_bad("congress_trades", "FRED is not allowed for Congress trades", active_provider="fred")
        assert_bad("insider_trades", "FRED is not allowed for insider trades / Form 4", active_provider="fred")
        assert_bad("insider_trades", "Official House Disclosures is not allowed for insider trades / Form 4", active_provider="official_house")
        assert_bad("house_disclosures", "Official Senate Disclosures is not allowed for House disclosures", active_provider="official_senate")
        assert_bad("prices_eod", "Shadow is not allowed for EOD equity prices", mode="shadow")

        with pytest.raises(HTTPException) as exc:
            patch("unknown_domain", active_provider="fmp")
        assert exc.value.status_code == 404

        assert patch("prices_eod", active_provider="fmp")["active_provider"] == "fmp"
        assert patch("prices_eod", fallback_provider="walnut_cache")["fallback_provider"] == "walnut_cache"
        congress = patch("congress_trades", active_provider="walnut_official", mode="shadow", is_enabled=True)
        assert congress["active_provider"] == "walnut_official"
        assert congress["mode"] == "shadow"
        insider = patch("insider_trades", active_provider="sec_edgar", mode="shadow", is_enabled=True)
        assert insider["active_provider"] == "sec_edgar"
        assert insider["mode"] == "shadow"
        macro = patch("insights_macro", active_provider="fred", mode="primary", is_enabled=True)
        assert macro["active_provider"] == "fred"
        pnl = patch("pnl_enrichment", active_provider="internal_computed", fallback_provider="walnut_cache", mode="primary", is_enabled=True)
        assert pnl["active_provider"] == "internal_computed"
        assert pnl["fallback_provider"] == "walnut_cache"
        signal = patch("signal_inputs", active_provider="internal_computed", fallback_provider="walnut_cache", mode="primary", is_enabled=True)
        assert signal["active_provider"] == "internal_computed"
        assert signal["fallback_provider"] == "walnut_cache"
        screener = patch("screener_fundamentals", active_provider="walnut_cache", mode="primary", is_enabled=True)
        assert screener["active_provider"] == "walnut_cache"

        disabled = patch("prices_historical", active_provider="disabled")
        assert disabled["active_provider"] == "disabled"
        assert disabled["mode"] == "disabled"
        assert disabled["is_enabled"] is False
    finally:
        db.close()


def test_data_sources_status_exposes_domain_options_and_cleans_legacy_disclosure_fallbacks():
    db = _session()
    try:
        admin = _user(db, "admin@example.com", role="admin")
        settings = get_provider_settings_by_domain(db)
        settings["house_disclosures"].fallback_provider = "fmp"
        settings["senate_disclosures"].fallback_provider = "fmp"
        db.commit()

        payload = admin_data_sources_status(_request_for_user(admin), db)
        rows = {row["domain_key"]: row for row in payload["domains"]}
        audits = db.execute(select(ProviderSettingAuditLog).where(ProviderSettingAuditLog.reason == PROVIDER_VALIDATION_CLEANUP_REASON)).scalars().all()

        assert rows["prices_eod"]["allowed_providers"] == ["fmp", "walnut_cache", "disabled"]
        assert "fred" not in rows["prices_eod"]["allowed_providers"]
        assert "sec_edgar" not in rows["prices_eod"]["allowed_providers"]
        assert rows["insights_macro"]["allowed_providers"] == ["fred", "walnut_cache", "disabled"]
        assert "fmp" not in rows["insights_macro"]["allowed_providers"]
        assert rows["congress_trades"]["allowed_providers"] == ["walnut_official", "fmp", "walnut_cache", "disabled"]
        assert "fred" not in rows["congress_trades"]["allowed_providers"]
        assert rows["insider_trades"]["allowed_providers"] == ["sec_edgar", "fmp", "walnut_cache", "disabled"]
        assert "official_house" not in rows["insider_trades"]["allowed_providers"]
        assert "official_senate" not in rows["insider_trades"]["allowed_providers"]
        assert rows["pnl_enrichment"]["active_provider"] == "internal_computed"
        assert rows["pnl_enrichment"]["fallback_provider"] == "walnut_cache"
        assert rows["pnl_enrichment"]["allowed_providers"] == ["internal_computed", "walnut_cache", "fmp", "disabled"]
        assert rows["pnl_enrichment"]["validation_warnings"] == []
        assert rows["signal_inputs"]["active_provider"] == "internal_computed"
        assert rows["signal_inputs"]["fallback_provider"] == "walnut_cache"
        assert rows["signal_inputs"]["allowed_providers"] == ["internal_computed", "walnut_cache", "disabled"]
        assert rows["signal_inputs"]["validation_warnings"] == []
        assert rows["house_disclosures"]["fallback_provider"] == "walnut_cache"
        assert rows["senate_disclosures"]["fallback_provider"] == "walnut_cache"
        assert rows["house_disclosures"]["can_save"] is True
        assert rows["senate_disclosures"]["can_save"] is True
        assert rows["house_disclosures"]["validation_warnings"] == []
        assert rows["senate_disclosures"]["validation_warnings"] == []
        assert {audit.domain_key for audit in audits} == {"house_disclosures", "senate_disclosures"}
        assert {audit.changed_by for audit in audits} == {"system"}
    finally:
        db.close()


def test_data_sources_status_keeps_field_specific_warnings_for_truly_invalid_saved_values():
    db = _session()
    try:
        admin = _user(db, "admin@example.com", role="admin")
        settings = get_provider_settings_by_domain(db)
        settings["prices_eod"].active_provider = "fred"
        db.commit()

        payload = admin_data_sources_status(_request_for_user(admin), db)
        rows = {row["domain_key"]: row for row in payload["domains"]}

        assert rows["prices_eod"]["can_save"] is False
        assert rows["prices_eod"]["validation_warnings"]
        assert "Invalid provider: FRED is not allowed for EOD equity prices" in rows["prices_eod"]["validation_warnings"][0]
        assert "Valid providers: FMP, Local Walnut Cache, Disabled" in rows["prices_eod"]["validation_warnings"][0]
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
