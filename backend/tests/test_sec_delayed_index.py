from datetime import date
import json
import pytest
from sqlalchemy import select,func
from app.clients.direct_sources import DirectSourceError,parse_sec_index
from app.services.direct_feed_collection import discover_sources
from app.services.direct_feed_store import DirectFeedDocument
from app.models import Event
from test_direct_sec_collection import db

HEADER = "Description:           Daily Index of EDGAR Dissemination Feed\nLast Data Received:    Oct 14, 2026\n"
COLUMNS = "CIK|Company Name|Form Type|Date Filed|File Name\n"
ROWS = ("2079652|StoryOne LLC|13F-HR|20261013|edgar/data/2079652/0002079652-26-000005.txt\n"
        "320193|Example|4|20261014|edgar/data/320193/0000320193-26-000001.txt\n")


def test_delayed_entry_keeps_filing_date_with_exact_index_header():
    rows=parse_sec_index((HEADER+COLUMNS+ROWS).encode(),forms=('4','13F-HR'),index_date=date(2026,10,14))
    assert [r['filing_date'] for r in rows]==['2026-10-13','2026-10-14']
    assert rows[0]['key']=='0002079652-26-000005'


@pytest.mark.parametrize('header', ['',HEADER.replace('Oct 14','Oct 13'),HEADER.replace('Oct 14, 2026','invalid')])
def test_missing_wrong_or_invalid_header_cannot_authorize_delayed_entry(header):
    with pytest.raises(DirectSourceError):
        parse_sec_index((header+COLUMNS+ROWS).encode(),forms=('4','13F-HR'),index_date=date(2026,10,14))


def test_future_filing_is_rejected_even_with_matching_header():
    with pytest.raises(DirectSourceError,match='unexpected filing date'):
        parse_sec_index((HEADER+COLUMNS+ROWS.replace('20261013','20261015')).encode(),
                        forms=('4','13F-HR'),index_date=date(2026,10,14))


def test_wrong_index_header_rejected_even_when_rows_match_request():
    with pytest.raises(DirectSourceError,match='header does not match'):
        parse_sec_index((HEADER.replace('Oct 14','Oct 13')+COLUMNS+ROWS.replace('20261013','20261014')).encode(),
                        forms=('4','13F-HR'),index_date=date(2026,10,14))


def test_actual_delayed_index_shape_stages_both_dates_and_repeats_without_publication(db):
    class Client:
        def get(self,url):
            assert url.endswith('/2026/QTR4/master.20261014.idx')
            return (HEADER+COLUMNS+ROWS).encode()
    kwargs=dict(sources=['sec_form4','sec_13f'],start=date(2026,10,14),end=date(2026,10,14),
                symbols=(),issuer_registry=(),senate_reports=())
    result=discover_sources(db,Client(),**kwargs)
    assert result==({'sec_13f':1,'sec_form4':1},[])
    before=[(d.id,d.source_key,d.source_url,d.metadata_json,d.first_seen_at)
            for d in db.scalars(select(DirectFeedDocument).order_by(DirectFeedDocument.id))]
    assert len(before)==2
    assert [json.loads(row[3])['filing_date'] for row in before]==['2026-10-13','2026-10-14']
    assert discover_sources(db,Client(),**kwargs)==result
    assert before==[(d.id,d.source_key,d.source_url,d.metadata_json,d.first_seen_at)
                   for d in db.scalars(select(DirectFeedDocument).order_by(DirectFeedDocument.id))]
    assert db.scalar(select(func.count()).select_from(Event))==0
