"""Reviewed closure days must not erase access failures or existing filings."""
from datetime import date
import json

import pytest
from sqlalchemy import func, select

from app.clients.direct_sources import DirectSourceError
from app.models import Event, InstitutionalPosition
from app.services import direct_feed_collection as collection
from app.services.direct_feed_store import DirectFeedDocument, DirectFeedRevision, DirectFeedRun, discover
from test_direct_sec_collection import db, META, submission

CALENDAR_URL = "https://www.sec.gov/submit-filings/filer-support-resources/edgar-calendar"
# SEC's published 2026 EDGAR calendar, reviewed October 14, 2026.
CLOSED = ["2026-01-01", "2026-01-19", "2026-02-16", "2026-05-25", "2026-06-19",
          "2026-07-03", "2026-09-07", "2026-10-12", "2026-11-11", "2026-11-26", "2026-12-25"]


def collect(db, client, start, end=None):
    return collection.collect_direct_feeds(db, client, sources=["sec_form4", "sec_13f"],
        start=date.fromisoformat(start), end=date.fromisoformat(end or start))


class Denied:
    def __init__(self): self.calls = []
    def get(self, url):
        self.calls.append(url)
        raise DirectSourceError("Source HTTP 403: " + url)


@pytest.mark.parametrize("day", CLOSED)
def test_confirmed_closure_does_not_request_missing_index_and_retains_proof(db, day):
    client = Denied()
    result = collect(db, client, day)
    assert client.calls == []
    assert result["errors"] == [] and result["status"] == "collected"
    assert result["sec_index_calendar"]["closed_days"] == [
        {"date": day, "reason": "published_edgar_closure", "source_url": CALENDAR_URL}]
    assert result["sec_index_calendar"]["unreviewed_years"] == []
    assert not result["cutover_ready"] and not result["fmp_disabled"]
    saved = json.loads(db.get(DirectFeedRun, result["run_id"]).report_json)
    assert saved["sec_index_calendar"] == result["sec_index_calendar"]
    assert result["public_writes"] == 0


@pytest.mark.parametrize("day", ["2026-10-13", "2026-04-03", "2027-01-01", "2025-01-01"])
def test_other_weekday_denials_are_not_treated_as_closures(db, day):
    client = Denied()
    result = collect(db, client, day)
    assert len(client.calls) == 1
    assert result["status"] == "partial"
    assert {e["feed"] for e in result["errors"]} == {"sec_form4", "sec_13f"}
    assert all("403" in e["error"] for e in result["errors"])
    assert not result["sec_index_calendar"]["closed_days"]
    assert result["sec_index_calendar"]["unreviewed_years"] == ([] if day.startswith("2026") else [int(day[:4])])


def test_long_weekend_preserves_both_open_days_and_repeat_identities(db):
    class Client:
        def __init__(self): self.calls = []
        def get(self, url):
            self.calls.append(url)
            day = "2026-10-09" if "20261009" in url else "2026-10-13"
            suffix = "000001" if day.endswith("09") else "000002"
            return ("CIK|Company Name|Form Type|Date Filed|Filename\n"
                    f"1092903|Example|13F-HR|{day}|edgar/data/1092903/0001096906-26-{suffix}.txt\n").encode()
    client = Client()
    kwargs = dict(sources=["sec_form4", "sec_13f"], start=date(2026,10,9), end=date(2026,10,13),
                  symbols=(), issuer_registry=(), senate_reports=())
    first = collection.discover_sources(db, client, **kwargs)
    before = [(d.feed,d.source_key,d.source_url,d.metadata_json) for d in db.scalars(select(DirectFeedDocument).order_by(DirectFeedDocument.id))]
    assert first == ({"sec_13f": 2}, [])
    assert len(before) == 2
    assert [url.rsplit("/",1)[1] for url in client.calls] == ["master.20261009.idx", "master.20261013.idx"]
    assert collection.discover_sources(db, client, **kwargs) == first
    assert before == [(d.feed,d.source_key,d.source_url,d.metadata_json) for d in db.scalars(select(DirectFeedDocument).order_by(DirectFeedDocument.id))]
    assert db.scalar(select(func.count()).select_from(Event)) == 0


def test_closure_does_not_skip_prior_pending_document_or_duplicate_on_repeat(db):
    discover(db, "sec_13f", META); db.commit()
    class Client:
        def get(self, url):
            assert url == META["url"]
            return submission()
    first = collect(db, Client(), "2026-10-12")
    assert first["processed"] == 1 and first["errors"] == []
    second = collect(db, Client(), "2026-10-12")
    assert second["processed"] == 0
    assert db.scalar(select(func.count()).select_from(DirectFeedDocument)) == 1
    assert db.scalar(select(func.count()).select_from(DirectFeedRevision)) == 1
    assert db.scalar(select(func.count()).select_from(InstitutionalPosition)) == 0
    assert db.scalar(select(func.count()).select_from(Event)) == 0


def test_wrong_day_index_remains_error_after_holiday(db):
    class Client:
        def get(self,url):
            return b"CIK|Company Name|Form Type|Date Filed|Filename\n1092903|Example|13F-HR|2026-10-09|edgar/data/1092903/0001096906-26-000001.txt\n"
    result = collect(db, Client(), "2026-10-13")
    assert result["status"] == "partial"
    assert all("unexpected filing date" in e["error"] for e in result["errors"])
    assert db.scalar(select(func.count()).select_from(DirectFeedDocument)) == 0
