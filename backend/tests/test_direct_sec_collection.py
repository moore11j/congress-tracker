"""SEC discovery, exact-source checks and isolated rerun regressions."""
from datetime import date
import hashlib
import json
from pathlib import Path

import pytest
from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import Session

from app.db import Base
from app.models import Event, InstitutionalFiling, InstitutionalPosition
from app.clients.direct_sources import DirectSourceError, parse_sec_index
from app.services.direct_feed_collection import collect_direct_feeds, parse_document
from app.services.direct_feed_store import DirectFeedDocument, DirectFeedRevision, reconcile_institutional


META = {"key": "0001096906-26-001512", "cik": "0001092903", "name": "Fixture manager",
        "form": "13F-HR", "filing_date": "2026-10-06",
        "url": "https://www.sec.gov/Archives/edgar/data/1092903/0001096906-26-001512.txt"}


def submission(*, form="13F-HR", amendment="", confidential="false"):
    # Synthetic compact submission follows the observed SEC cover/table shape.
    return f'''<SEC-HEADER>
ACCESSION NUMBER: 0001096906-26-001512
FILED AS OF DATE: 20261006
</SEC-HEADER>
<DOCUMENT><FILENAME>primary_doc.xml
<XML><edgarSubmission xmlns="http://www.sec.gov/edgar/thirteenffiler">
<headerData><submissionType>{form}</submissionType><filerInfo><filer><credentials>
<cik>0001092903</cik></credentials></filer></filerInfo></headerData>
<formData><coverPage><reportCalendarOrQuarter>09-30-2026</reportCalendarOrQuarter>
{amendment}</coverPage><summaryPage><tableEntryTotal>2</tableEntryTotal>
<tableValueTotal>300</tableValueTotal><isConfidentialOmitted>{confidential}</isConfidentialOmitted>
</summaryPage></formData></edgarSubmission></XML></DOCUMENT>
<DOCUMENT><FILENAME>holdings.xml
<XML><informationTable xmlns="http://www.sec.gov/edgar/document/thirteenf/informationtable">
<infoTable><nameOfIssuer>Example</nameOfIssuer><titleOfClass>COM</titleOfClass><cusip>000361105</cusip>
<value>100</value><shrsOrPrnAmt><sshPrnamt>10</sshPrnamt><sshPrnamtType>SH</sshPrnamtType></shrsOrPrnAmt></infoTable>
<infoTable><nameOfIssuer>Example</nameOfIssuer><titleOfClass>COM</titleOfClass><cusip>000361105</cusip>
<value>200</value><shrsOrPrnAmt><sshPrnamt>20</sshPrnamt><sshPrnamtType>SH</sshPrnamtType></shrsOrPrnAmt></infoTable>
</informationTable></XML></DOCUMENT>'''.encode()


@pytest.fixture
def db():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        yield session
    engine.dispose()


@pytest.mark.parametrize("header", ["File Name", "Filename"])
@pytest.mark.parametrize("filed", ["20261006", "2026-10-06"])
def test_real_sec_header_and_date_variants(header, filed):
    raw = f"CIK|Company Name|Form Type|Date Filed|{header}\n1092903|Example|13F-HR|{filed}|edgar/data/1092903/0001096906-26-001512.txt\n".encode()
    assert parse_sec_index(raw, forms=("13F-HR",))[0]["filing_date"] == "2026-10-06"


def test_13f_count_values_and_source_rows():
    _, result, reasons = parse_document("sec_13f", submission(), META)
    assert not reasons
    assert result["metadata"]["table_entry_total"] == 2
    assert result["metadata"]["table_value_total_usd"] == "300"
    assert result["metadata"]["report_period"] == "2026-09-30"
    assert [row["source_line_ref"] for row in result["positions"]] == ["1", "2"]
    assert [row["valueUsd"] for row in result["positions"]] == [100, 200]


def test_real_13f_share_type_whitespace_preserves_all_source_rows():
    raw = (Path(__file__).with_name('fixtures') / 'sec_13f_0001120048_26_000004.txt').read_bytes()
    assert hashlib.sha256(raw).hexdigest() == '78750fb0c08421730b4086c04c40de066bf19f3badce554f2b22cf49adbde860'
    metadata = {'key': '0001120048-26-000004', 'cik': '0001120048', 'name': 'STEGINSKY CAPITAL LLC',
        'form': '13F-HR', 'filing_date': '2026-10-07',
        'url': 'https://www.sec.gov/Archives/edgar/data/1120048/0001120048-26-000004.txt'}
    _, parsed, reasons = parse_document('sec_13f', raw, metadata)
    assert not reasons
    assert parsed['metadata']['table_entry_total'] == len(parsed['positions']) == 8
    assert all(row['shareType'] == 'SH' for row in parsed['positions'])
    assert parse_document('sec_13f', raw, metadata)[1] == parsed


@pytest.mark.parametrize('unit', ['SHARES', 'sh', '', 'NOT_SH'])
def test_invalid_13f_share_type_is_still_rejected(unit):
    with pytest.raises(DirectSourceError, match='share/principal'):
        parse_document('sec_13f', submission().replace(b'>SH<', ('> ' + unit + ' <').encode()), META)


@pytest.mark.parametrize("old,new", [
    (b"<tableEntryTotal>2", b"<tableEntryTotal>3"),
    (b"<tableValueTotal>300", b"<tableValueTotal>301"),
    (b"<value>200", b"<value>NaN"),
    (b"<cusip>000361105</cusip>", b"<cusip></cusip>"),
    (b"<cik>0001092903", b"<cik>0001092904"),
    (b"ACCESSION NUMBER: 0001096906-26-001512", b"ACCESSION NUMBER: 0001096906-26-001513"),
])
def test_incomplete_or_wrong_13f_is_rejected(old, new):
    with pytest.raises(DirectSourceError):
        parse_document("sec_13f", submission().replace(old, new), META)


def test_amendments_and_confidential_omissions_are_held():
    _, parsed, reasons = parse_document("sec_13f", submission(form="13F-HR/A", amendment="<amendmentType>NEW HOLDINGS</amendmentType>"), {**META, "form": "13F-HR/A"})
    assert parsed["metadata"]["amendment_type"] == "NEW HOLDINGS"
    assert any("amendment" in reason for reason in reasons)
    assert parse_document("sec_13f", submission(confidential="true"), META)[2]
    assert parse_document("sec_13f", submission(confidential="1"), META)[2]
    with pytest.raises(DirectSourceError, match="boolean"):
        parse_document("sec_13f", submission(confidential="unknown"), META)


def test_existing_sec_fetcher_reuses_identical_pure_parser(monkeypatch):
    import app.clients.sec_edgar as sec
    import re
    raw = re.findall(rb"<XML>(.*?)</XML>", submission(), re.S)[1]
    def request(url, *, expect_json):
        if expect_json:
            return {"directory": {"item": [{"name": "holdings.xml"}]}}
        return raw
    monkeypatch.setattr(sec, "_request", request)
    result = sec.fetch_13f_information_table(cik=META["cik"], accession_number=META["key"])
    assert len(result) == 2
    assert sum(row["valueUsd"] for row in result) == 300
    assert result[0]["sourceUrl"].endswith('/000109690626001512/holdings.xml')


def test_pre_2023_values_are_not_silently_treated_as_dollars():
    raw = submission().replace(b'20261006', b'20221006')
    with pytest.raises(DirectSourceError, match="Historical"):
        parse_document("sec_13f", raw, {**META, "filing_date": "2022-10-06"})


def test_reconcile_13f_preserves_source_rows_and_existing_aggregate(db):
    _, parsed, _ = parse_document("sec_13f", submission(), META)
    filing = InstitutionalFiling(cik=META["cik"], accession_number=META["key"], filing_date=date(2026,10,6), report_year=2026, report_quarter=3, report_period_end=date(2026,9,30))
    db.add(filing)
    db.flush()
    position = InstitutionalPosition(filing_id=filing.id, cik=filing.cik, cusip="000361105", normalized_symbol="EXM", shares=30, value_usd=300, report_year=2026, report_quarter=3, filing_date=filing.filing_date)
    db.add(position)
    db.flush()
    receipt = reconcile_institutional(db, parsed)
    assert len(receipt["matched"]) == 2
    assert {row["existing_id"] for row in receipt["matched"]} == {position.id}
    position.value_usd = 301
    assert reconcile_institutional(db, parsed)["ambiguous"] == ["1", "2"]
    assert db.scalar(select(func.count()).select_from(InstitutionalPosition)) == 1


def test_live_shape_discovery_and_rerun_have_no_duplicate_documents(db):
    class Client:
        def get(self, url):
            if url.endswith('.idx'):
                return b"CIK|Company Name|Form Type|Date Filed|File Name\n1092903|Example|13F-HR|20261006|edgar/data/1092903/0001096906-26-001512.txt\n"
            return submission()
    kwargs = dict(sources=["sec_13f"], start=date(2026,10,6), end=date(2026,10,6))
    first = collect_direct_feeds(db, Client(), **kwargs)
    assert first["processed"] == 1 and first["status"] == "collected"
    second = collect_direct_feeds(db, Client(), **kwargs)
    assert second["processed"] == 0
    assert db.scalar(select(func.count()).select_from(DirectFeedDocument)) == 1
    assert db.scalar(select(func.count()).select_from(DirectFeedRevision)) == 1
    assert db.scalar(select(func.count()).select_from(Event)) == 0
    assert db.scalar(select(func.count()).select_from(InstitutionalPosition)) == 0
    # An unresolved prior parse must stay visible even when not due for retry.
    doc = db.scalar(select(DirectFeedDocument))
    doc.status = "quarantined"
    db.commit()
    assert collect_direct_feeds(db, Client(), **kwargs)["status"] == "partial"


def test_existing_holdings_conflict_quarantines_collected_document(db):
    filing = InstitutionalFiling(cik=META['cik'], accession_number=META['key'], filing_date=date(2026,10,6),
                                report_year=2026, report_quarter=3, report_period_end=date(2026,9,30))
    db.add(filing)
    db.flush()
    db.add(InstitutionalPosition(filing_id=filing.id, cik=filing.cik, cusip='000361105',
                                shares=30, value_usd=301, report_year=2026, report_quarter=3,
                                filing_date=filing.filing_date))
    db.commit()
    class Client:
        def get(self, url):
            if url.endswith('.idx'):
                return b'CIK|Company Name|Form Type|Date Filed|File Name\n1092903|Example|13F-HR|20261006|edgar/data/1092903/0001096906-26-001512.txt\n'
            return submission()
    result = collect_direct_feeds(db, Client(), sources=['sec_13f'], start=date(2026,10,6), end=date(2026,10,6))
    assert result['status'] == 'partial'
    document = db.scalar(select(DirectFeedDocument))
    assert document.status == 'quarantined'
    assert 'Existing records differ' in document.error
    assert json.loads(document.reconciliation_json)['ambiguous'] == ['1', '2']


@pytest.mark.parametrize('field,value', [
    ('key', '0000320193-26-000002'), ('filing_date', '2026-06-04'),
    ('cik', '0000000001'), ('form', '4/A'),
])
def test_form4_payload_is_bound_to_index_identity(field, value):
    from test_official_disclosure_pipelines import FORM4_SAMPLE
    metadata = {'key': '0000320193-26-000001', 'filing_date': '2026-06-03',
                'cik': '0000320193', 'form': '4', 'url': 'https://www.sec.gov/example'}
    xml = FORM4_SAMPLE.replace('<ownershipDocument>', '<ownershipDocument><documentType>4</documentType>')
    raw = ('<SEC-HEADER>\nACCESSION NUMBER: 0000320193-26-000001\nFILED AS OF DATE: 20260603\n</SEC-HEADER>\n' + xml).encode()
    assert parse_document('sec_form4', raw, metadata)[1]['transactions']
    with pytest.raises(DirectSourceError):
        parse_document('sec_form4', raw, {**metadata, field: value})
