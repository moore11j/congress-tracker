from datetime import date
from pathlib import Path
import json

import pytest
from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import Session

from app.db import Base
from app.models import Event, InsiderTransactionNormalized, ResearchSourceDocument
from app.clients.direct_sources import DirectSourceClient, DirectSourceError, parse_house_pdf, parse_sec_index, parse_senate_html
from app.services.direct_feed_collection import collect_direct_feeds, parse_document
from app.services.direct_feed_store import (
    DirectFeedDocument, DirectFeedRevision, DirectFeedRun, discover, record_document,
    readiness_report, reconcile_insider,
)
from app.services.sec_form4 import parse_form4_xml
from test_official_disclosure_pipelines import FORM4_SAMPLE


@pytest.fixture
def db():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine, autoflush=False) as session:
        yield session
    engine.dispose()


def test_actual_filing_date_does_not_come_from_transaction_period():
    parsed = parse_form4_xml(FORM4_SAMPLE, accession_number="0000320193-26-000001")
    assert parsed["filing"]["filing_date"] is None
    assert parsed["transactions"][0]["filing_date"] is None
    parsed = parse_form4_xml(FORM4_SAMPLE, filing_date=date(2026, 6, 3))
    assert parsed["filing"]["filing_date"] == date(2026, 6, 3)
    assert parsed["transactions"][0]["transaction_date"] == date(2026, 5, 31)
    assert parsed["transactions"][0]["filing_date"] == date(2026, 6, 3)


def test_identical_sec_lots_remain_distinct_and_stable():
    start = FORM4_SAMPLE.index("<nonDerivativeTransaction>")
    end = FORM4_SAMPLE.index("</nonDerivativeTransaction>") + len("</nonDerivativeTransaction>")
    xml = FORM4_SAMPLE[:end] + FORM4_SAMPLE[start:end] + FORM4_SAMPLE[end:]
    parsed = parse_form4_xml(xml, accession_number="0000320193-26-000001")
    assert len(parsed["transactions"]) == 4
    assert len({row["normalized_hash"] for row in parsed["transactions"]}) == 4
    assert parsed == parse_form4_xml(xml, accession_number="0000320193-26-000001")


def test_rerun_and_changed_document_preserve_identity_and_revisions(db):
    metadata = {"key": "2026:1234", "url": "https://disclosures-clerk.house.gov/report.pdf"}
    document = discover(db, "house_ptr", metadata)
    record_document(db, document, b"first", "first text", {"transactions": []})
    db.commit()
    repeated = discover(db, "house_ptr", metadata)
    record_document(db, repeated, b"first", "first text", {"transactions": []})
    record_document(db, repeated, b"second", "corrected text", {"transactions": [1]}, reasons=["Amended source"])
    db.commit()
    assert repeated.id == document.id
    assert db.scalar(select(func.count()).select_from(DirectFeedDocument)) == 1
    assert db.scalar(select(func.count()).select_from(DirectFeedRevision)) == 2
    assert document.status == "quarantined"
    assert db.scalar(select(func.count()).select_from(Event)) == 0
    assert db.scalar(select(func.count()).select_from(InsiderTransactionNormalized)) == 0
    assert db.scalar(select(func.count()).select_from(ResearchSourceDocument)) == 0


def test_reconciliation_matches_fmp_record_without_inserting_or_collapsing_lots(db):
    parsed = parse_form4_xml(FORM4_SAMPLE, accession_number="0000320193-26-000001", filing_date=date(2026, 6, 3))
    item = parsed["transactions"][0]
    legacy = InsiderTransactionNormalized(**{**item, "normalized_hash": "legacy-fmp-id"})
    db.add(legacy)
    db.flush()
    parsed["transactions"] = [item, dict(item)]
    receipt = reconcile_insider(db, parsed)
    assert receipt == {"matched": [{"row": 1, "existing_id": legacy.id}], "unmatched": [], "ambiguous": [2], "existing_only_ids": []}
    parsed["transactions"][0] = {**item, "accession_number": "unproven-accession"}
    assert reconcile_insider(db, parsed)["ambiguous"] == [1]
    assert db.scalar(select(func.count()).select_from(InsiderTransactionNormalized)) == 1


def test_amendments_and_multiowner_filings_are_held():
    metadata = {"key": "0000320193-26-000001", "cik": "0000320193", "url": "https://www.sec.gov/example", "filing_date": "2026-06-03", "form": "4/A"}
    header = '<SEC-HEADER>\nACCESSION NUMBER: 0000320193-26-000001\nFILED AS OF DATE: 20260603\n</SEC-HEADER>\n'
    amended = FORM4_SAMPLE.replace('<ownershipDocument>', '<ownershipDocument><documentType>4/A</documentType>')
    _, _, reasons = parse_document("sec_form4", (header + amended).encode(), metadata)
    assert any("Amendment" in reason for reason in reasons)
    owner_start = FORM4_SAMPLE.index("<reportingOwner>")
    owner_end = FORM4_SAMPLE.index("</reportingOwner>") + len("</reportingOwner>")
    joint = FORM4_SAMPLE[:owner_end] + FORM4_SAMPLE[owner_start:owner_end] + FORM4_SAMPLE[owner_end:]
    joint = joint.replace('<ownershipDocument>', '<ownershipDocument><documentType>4</documentType>')
    _, _, reasons = parse_document("sec_form4", (header + joint).encode(), {**metadata, "form": "4"})
    assert any("Joint" in reason for reason in reasons)


@pytest.mark.parametrize('field,value', [
    ('price', None), ('shares', 999), ('security_title', 'Restricted Stock Units'),
    ('acquired_disposed', 'D'), ('direct_or_indirect', 'I'), ('is_derivative', True),
    ('issuer_cik', '0000000001'), ('filing_date', date(2026, 6, 4)),
])
def test_existing_filing_discrepancies_are_not_new_insider_trades(db, field, value):
    parsed = parse_form4_xml(FORM4_SAMPLE, accession_number='0000320193-26-000001', filing_date=date(2026, 6, 3))
    item = parsed['transactions'][0]
    if field == 'price':
        item = {**item, 'price': 0.0, 'value': 0.0}
    legacy = InsiderTransactionNormalized(**{**item, field: value, 'normalized_hash': 'legacy'})
    db.add(legacy)
    db.flush()
    parsed['transactions'] = [item]
    result = reconcile_insider(db, parsed)
    assert result['matched'] == result['unmatched'] == []
    assert result['ambiguous'] == [1]
    assert result['existing_only_ids'] == [legacy.id]
    assert db.scalar(select(func.count()).select_from(InsiderTransactionNormalized)) == 1


def test_house_rows_preserve_options_and_reject_partial_pdf(monkeypatch):
    import app.clients.direct_sources as sources
    source_text = """Filing ID #1
Name: Hon. Example
ID Owner Asset Transaction Type Date Notification Date Amount
$200?
SP Apple Inc. - Common Stock (AAPL) [ST]
P 06/01/2026 06/02/2026 $1,001 - $15,000
Filing Status: New
Meta Platforms, Inc. (META) [OP]
S (P) 06/01/2026 06/02/2026 $15,001 - $50,000
"""
    class Page:
        def extract_text(self, **kwargs):
            return source_text
    class Reader:
        pages = [Page()]
    monkeypatch.setattr(sources, "PdfReader", lambda raw: Reader())
    metadata = {"key": "2026:1", "filing_id": "1", "url": "https://disclosures-clerk.house.gov/1.pdf", "filing_date": "2026-06-03"}
    _, report = parse_house_pdf(b"%PDF fixture", metadata)
    assert len(report["transactions"]) == 2
    assert report["transactions"][0]["owner"] == "SP"
    assert report["transactions"][1]["asset_type"] == "option"
    assert report["transactions"][1]["transaction_type"] == "sale"
    _, normalized, _ = parse_document("house_ptr", b"%PDF fixture", metadata)
    assert normalized["transactions"][1]["asset_type_normalized"] == "option"
    source_text += "Missing values (AAPL) [ST]\n"
    with pytest.raises(DirectSourceError, match="coverage incomplete"):
        parse_house_pdf(b"%PDF fixture", metadata)


def test_sec_index_deduplicates_joint_filer_accessions_and_rejects_html():
    header = "CIK|Company Name|Form Type|Date Filed|Filename\n"
    row = "320193|Apple|4|2026-06-03|edgar/data/320193/0000320193-26-000001.txt\n"
    assert len(parse_sec_index((header + row + row).encode())) == 1
    with pytest.raises(DirectSourceError):
        parse_sec_index(b"<html>Request blocked</html>")


def test_transport_refuses_unreviewed_hosts_without_making_request():
    client = DirectSourceClient()
    for url in ("http://www.sec.gov/a", "https://127.0.0.1/a", "https://www.sec.gov.evil.example/a", "https://www.sec.gov:444/a"):
        with pytest.raises(DirectSourceError, match="approved HTTPS"):
            client.get(url)


def test_collection_retries_failed_sources_without_claiming_public_cutover(db):
    class Client:
        failing = True
        def get(self, url):
            if "company_tickers" in url:
                return b'{"0":{"cik_str":320193,"ticker":"AAPL","title":"Apple Inc."}}'
            if self.failing:
                raise DirectSourceError("Source HTTP 403")
            return b'{"cik":"320193","name":"Apple Inc.","tickers":["AAPL"],"sic":"3571"}'
    client = Client()
    kwargs = dict(sources=["sec_company"], start=date(2026, 6, 1), end=date(2026, 6, 3), symbols=["AAPL"])
    result = collect_direct_feeds(db, client, **kwargs)
    assert result["status"] == "partial"
    assert result["errors"]
    doc = db.scalar(select(DirectFeedDocument).where(DirectFeedDocument.feed == "sec_company"))
    doc.checked_at = None
    db.commit()
    client.failing = False
    result = collect_direct_feeds(db, client, **kwargs)
    assert result["processed"] == 1
    assert result["public_writes"] == 0
    assert not result["cutover_ready"]
    assert readiness_report(db)["fmp_disabled"] is False
    assert db.scalar(select(func.count()).select_from(DirectFeedRun)) == 2
    assert db.scalar(select(func.count()).select_from(DirectFeedDocument)) == 2


def test_senate_landing_page_is_not_an_empty_successful_report():
    with pytest.raises(DirectSourceError, match="unavailable"):
        parse_senate_html(b"<html>Please agree to conditions of access</html>", {})


SENATE_REAL_REPORT = Path(__file__).with_name("fixtures") / "senate_whitehouse_2026_10_01.html"


def test_senate_real_report_preserves_printed_rows_and_distinct_owners():
    _, report = parse_senate_html(SENATE_REAL_REPORT.read_bytes(), {"url": "https://efdsearch.senate.gov/report"})
    assert report["amendment_flag"] is False
    rows = report["transactions"]
    assert [row["source_line_ref"] for row in rows] == ["5", "4", "3", "2", "1"]
    assert [(row["symbol"], row["owner"]) for row in rows] == [
        ("JPM", "Spouse"), ("ADI", "Spouse"), ("V", "Spouse"), ("V", "Self"), ("JPM", "Self")]
    assert all(row["transaction_type"] == "Sale (Partial)" for row in rows)
    assert all(row["transaction_date"] == "2026-09-04" for row in rows)


def test_senate_explicit_amendment_is_not_confused_with_certification():
    raw = SENATE_REAL_REPORT.read_bytes()
    for payload, metadata in [
        (raw.replace(b"Report for", b"Report Amendment for"), {}),
        (raw, {"amendment_flag": True}),
    ]:
        _, report = parse_senate_html(payload, {"url": "https://efdsearch.senate.gov/report", **metadata})
        assert report["amendment_flag"] is True


def test_senate_repeated_or_absent_printed_identity_requires_review():
    raw = SENATE_REAL_REPORT.read_bytes()
    for bad in (raw.replace(b"<td>4</td>", b"<td>5</td>"), raw.replace(b"<td>4</td>", b"<td></td>")):
        with pytest.raises(DirectSourceError, match="row identity"):
            parse_senate_html(bad, {})
