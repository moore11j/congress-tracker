import json
from datetime import date

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from app.db import Base
from app.models import Event, InstitutionalActivityEvent, InstitutionalFiling, InstitutionalPosition, InstitutionalPositionChange
from app.services.institutional_sec_snapshot import resolve_snapshot, install_snapshot
from app.services.institutional_activity import process_filing_changes_and_events, upsert_positions_for_filing


class Client:
    kind = "NEW HOLDINGS"
    def fetch_13f_filing_metadata(self, **kw):
        return [{"accessionNumber": "original", "formType": "13F-HR", "filingDate": "2026-08-11"},
                {"accessionNumber": "supplement", "formType": "13F-HR/A", "filingDate": "2026-08-26"},
                {"accessionNumber": "future", "formType": "13F-HR/A", "filingDate": "2026-09-01"}]
    def fetch_13f_amendment_type(self, **kw):
        return self.kind
    def fetch_13f_information_table(self, *, accession_number, **kw):
        assert accession_number != "future", "must respect point-in-time cutoff"
        return [{"source": "sec_edgar", "accessionNumber": accession_number,
                 "sourceUrl": "https://www.sec.gov/Archives/" + accession_number,
                 "cusip": "NVIDIA" if accession_number == "original" else "OTHER",
                 "shares": 97 if accession_number == "original" else 5,
                 "valueUsd": 97_000_000 if accession_number == "original" else 5_000_000}]


def snapshot(client=None):
    return resolve_snapshot(cik="0001330387", year=2026, quarter=2, accession="supplement", client=client or Client())


def test_supplement_preserves_base_and_source_identity_at_cutoff():
    result = snapshot()
    assert [r["shares"] for r in result["rows"]] == [97, 5]
    assert [s["kind"] for s in result["sources"]] == ["ORIGINAL", "NEW HOLDINGS"]
    assert result["rows"][0]["accessionNumber"] == "original"


def test_restatement_replaces_not_adds():
    client = Client(); client.kind = "RESTATEMENT"
    assert [r["shares"] for r in snapshot(client)["rows"]] == [5]


def test_unknown_amendment_fails_closed():
    client = Client(); client.kind = None
    with pytest.raises(ValueError, match="Unknown SEC amendment"):
        snapshot(client)


def test_supplement_without_original_fails_closed():
    client = Client()
    client.fetch_13f_filing_metadata = lambda **kw: [{"accessionNumber": "supplement", "formType": "13F-HR/A"}]
    with pytest.raises(ValueError, match="without complete base"):
        snapshot(client)


def test_repeated_complete_table_is_deduplicated_but_conflicts_block():
    from app.services.institutional_sec_snapshot import merge_supplement
    base = Client().fetch_13f_information_table(accession_number="original")
    repeated = [{**base[0], "accessionNumber": "supplement", "sourceUrl": "https://www.sec.gov/Archives/supplement"}]
    assert merge_supplement(base, repeated) == base
    repeated[0]["shares"] += 1
    with pytest.raises(ValueError, match="Conflicting overlapping"):
        merge_supplement(base, repeated)


@pytest.mark.parametrize('cusip,symbols,year,quarter,expected', [
    ('74743L100', {'Q','Q-W'}, 2026, 2, 'Q'),
    ('74743L100', {'Q','Q-W'}, 2025, 4, 'Q'),
    ('74743L100', {'Q','Q-W'}, 2025, 3, None),
    ('OTHER', {'Q','Q-W'}, 2026, 2, None),
    ('74743L100', {'Q','Q-W','OTHER'}, 2026, 2, None),
    ('74743L100', {'Q-W'}, 2026, 2, 'Q-W'),
])
def test_qnity_common_share_alias_requires_confirmed_symbol_and_period(cusip,symbols,year,quarter,expected):
    from app.services.institutional_sec_snapshot import mapped_symbol
    assert mapped_symbol(cusip,symbols,year,quarter)==expected


@pytest.fixture
def db():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine, autoflush=False) as session:
        yield session


def filings(db):
    prior = InstitutionalFiling(cik="0001330387", report_year=2026, report_quarter=1,
        filing_date=date(2026, 5, 15), accession_number="prior", form_type="13F-HR")
    current = InstitutionalFiling(cik="0001330387", report_year=2026, report_quarter=2,
        filing_date=date(2026, 8, 26), accession_number="supplement", form_type="13F-HR/A", is_amendment=True)
    db.add_all([prior, current]); db.flush()
    db.add(InstitutionalPosition(filing_id=prior.id,cik=prior.cik,report_year=2026,report_quarter=1,
        filing_date=prior.filing_date,cusip="NVIDIA", normalized_symbol="NVDA",symbol="NVDA",shares=100,value_usd=100_000_000))
    db.flush()
    return current


def test_raw_amendment_is_never_a_complete_snapshot(db):
    current = filings(db)
    with pytest.raises(ValueError, match="complete reconciled"):
        upsert_positions_for_filing(db,filing=current,rows=Client().fetch_13f_information_table(accession_number="supplement"))
    with pytest.raises(ValueError, match="unreconciled"):
        process_filing_changes_and_events(db,current)


def test_repair_withdraws_false_exit_and_keeps_event_identity(db):
    current = filings(db)
    old = InstitutionalActivityEvent(symbol="NVDA",normalized_symbol="NVDA",cik=current.cik,
        event_type="major_holder_exit",filing_date=current.filing_date,report_year=2026,report_quarter=2,
        title="False exit",summary="False exit",freshness_status="stale",feed_visible=True)
    db.add(old); db.flush()
    old_id=old.id
    feed=Event(event_type=old.event_type,ts=current.filing_date,source="13F filing",source_provider="institutional_13f",
        source_filing_id=f"institutional:{old.id}:major_holder_exit:2026q2",payload_json='{"original_evidence": true}')
    db.add(feed); db.flush(); feed_id=feed.id
    install_snapshot(db,current,snapshot())
    process_filing_changes_and_events(db,current); db.flush()
    changes=db.scalars(select(InstitutionalPositionChange).where(InstitutionalPositionChange.normalized_symbol=="NVDA")).all()
    assert len(changes)==1
    assert (changes[0].change_type,changes[0].curr_shares,changes[0].shares_delta)==("decrease",97,-3)
    assert db.get(InstitutionalActivityEvent,old_id).freshness_status=="superseded"
    assert json.loads(db.get(Event,feed_id).payload_json)["original_evidence"] is True
    from app.routers.events import _not_superseded_institutional_clause
    assert db.scalar(select(Event.id).where(Event.id==feed_id,_not_superseded_institutional_clause(db))) is None
    with pytest.raises(ValueError, match="complete reconciled"):
        upsert_positions_for_filing(db,filing=current,rows=[{"symbol":"NVDA","shares":0}])


def test_snapshot_destination_and_checksum_are_guarded(db):
    current=filings(db); data=snapshot(); data["rows"][0]["shares"]=999
    with pytest.raises(ValueError,match="checksum"):
        install_snapshot(db,current,data)


def test_historical_repair_does_not_invent_new_positions_without_baseline(db):
    from app.jobs.reconcile_institutional_snapshots import rebuild
    current = filings(db)
    install_snapshot(db, current, snapshot())
    db.query(InstitutionalPosition).filter(InstitutionalPosition.report_quarter == 1).delete()
    db.flush()
    result = rebuild(db, current)
    assert result["changes"] == 0
    assert "baseline" in result["comparison_unavailable"]
    assert db.scalars(select(InstitutionalPositionChange)).all() == []


def test_sec_cusip_case_preserves_mapping_and_existing_position_id(db):
    from app.services.institutional_sec_snapshot import snapshot_digest
    current=filings(db)
    prior=InstitutionalPosition(filing_id=current.id,cik=current.cik,report_year=2026,report_quarter=2,
        filing_date=current.filing_date,cusip="NVIDIA",normalized_symbol="NVDA",symbol="NVDA",shares=1,value_usd=1)
    db.add(prior);db.flush();position_id=prior.id
    data=snapshot();data["rows"][0]["cusip"]="nvidia";data["sha256"]=snapshot_digest(data["rows"])
    install_snapshot(db,current,data)
    db.expire_all()
    updated=db.get(InstitutionalPosition,position_id)
    assert updated is not None and updated.normalized_symbol=="NVDA" and updated.shares==97


def test_withdrawal_uses_exact_indexed_source_ids():
    from types import SimpleNamespace
    from sqlalchemy.dialects import postgresql
    from app.services.institutional_activity import _archive_activity_rows
    class DB:
        def execute(self, query):
            sql=str(query.compile(dialect=postgresql.dialect()))
            assert 'source_filing_id IN' in sql
            assert 'split_part' not in sql and ' LIKE ' not in sql
            return self
        def scalars(self):
            return []
    row=SimpleNamespace(id=3,event_type="major_holder_exit",report_year=2026,report_quarter=2,
                        freshness_status="stale",feed_visible=True)
    _archive_activity_rows(DB(),[row])
    assert row.freshness_status=="superseded"


def test_scoped_repair_leaves_unrelated_manager_event_unchanged(db):
    from datetime import datetime, timezone
    from app.jobs.reconcile_institutional_snapshots import rebuild
    current=filings(db); original_time=datetime(2026,8,1,tzinfo=timezone.utc)
    unrelated=InstitutionalActivityEvent(symbol="NVDA",normalized_symbol="NVDA",cik="0009999999",
        event_type="major_holder_exit",filing_date=current.filing_date,report_year=2026,report_quarter=2,
        title="Unrelated event",summary="Unrelated summary",freshness_status="stale",feed_visible=True,updated_at=original_time)
    db.add(unrelated);db.flush(); id=unrelated.id
    install_snapshot(db,current,snapshot());rebuild(db,current);db.flush()
    row=db.get(InstitutionalActivityEvent,id)
    assert row.freshness_status=="stale" and row.feed_visible is True
    assert row.updated_at==original_time


def test_nebius_verified_ticker_change_is_period_and_cusip_scoped():
    from app.services.institutional_sec_snapshot import mapped_symbol
    assert mapped_symbol("N97284108", {"NBIS", "YNDX"}, 2026, 2) == "NBIS"
    assert mapped_symbol("N97284108", {"YNDX"}, 2024, 2) == "YNDX"
    assert mapped_symbol("N97284108", {"NBIS", "YNDX"}, 2024, 2) is None
    assert mapped_symbol("N97284108", {"NBIS", "OTHER"}, 2026, 2) is None
    assert mapped_symbol("DIFFERENT", {"NBIS", "YNDX"}, 2026, 2) is None


@pytest.mark.parametrize('cusip,old,current,first,previous', [
    ('03073E105','ABC','COR',(2023,3),(2023,2)),
    ('571748102','MMC','MRSH',(2026,1),(2025,4)),
    ('337738108','FI','FISV',(2025,4),(2025,3)),
])
def test_verified_current_alias_requires_cusip_period_and_available_new_symbol(cusip,old,current,first,previous):
    from app.services.institutional_sec_snapshot import mapped_symbol
    assert mapped_symbol(cusip,{old,current},*first)==current
    assert mapped_symbol(cusip,{old,current},*previous) is None
    assert mapped_symbol(cusip,{old},*first)==old
    assert mapped_symbol(cusip,{old,current,'UNRELATED'},*first) is None
    assert mapped_symbol('DIFFERENT',{old,current},*first) is None
    assert mapped_symbol(cusip,set(),*first) is None


def test_reviewed_issuer_aliases_require_independent_current_candidate_and_period():
    from app.services.institutional_sec_snapshot import mapped_symbol
    rules=[('165167735', 'CHK', 'EXE', 2024, 4), ('668771108', 'NLOK', 'GEN', 2022, 4), ('88023U101', 'TPX', 'SGI', 2025, 1), ('69121K104', 'ORCC', 'OBDC', 2023, 3), ('852234103', 'SQ', 'XYZ', 2025, 1), ('02156V109', 'ALCC', 'OKLO', 2024, 2), ('34964C106', 'FBHS', 'FBIN', 2022, 4), ('G3223R108', 'RE', 'EG', 2023, 3), ('714046109', 'PKI', 'RVTY', 2023, 2), ('75524B104', 'ROLL', 'RBC', 2022, 3), ('90984P303', 'UCBI', 'UCB', 2024, 3)]
    for cusip, old, current, year, quarter in rules:
        previous = (year, quarter-1) if quarter>1 else (year-1,4)
        assert mapped_symbol(cusip,{old,current},year,quarter)==current
        assert mapped_symbol(cusip,{old,current},*previous) is None
        assert mapped_symbol(cusip,{old},year,quarter)==old
        assert mapped_symbol(cusip,{old,current,'UNRELATED'},year,quarter) is None
        assert mapped_symbol('WRONGCUSIP',{old,current},year,quarter) is None


def test_additional_issuer_aliases_preserve_prior_and_unrelated_evidence():
    from app.services.institutional_sec_snapshot import mapped_symbol
    rules=[('000375204', 'ABB', 'ABBNY', 2023, 2), ('46137V282', 'RYT', 'RSPT', 2023, 2), ('42250P103', 'PEAK', 'DOC', 2024, 1), ('114340102', 'BRKS', 'AZTA', 2021, 4), ('649445400', 'NYCB', 'FLG', 2024, 4), ('224441105', 'CR', 'CXT', 2023, 2), ('228903100', 'CRY', 'AORT', 2022, 1), ('62886E108', 'NCR', 'VYX', 2023, 4), ('887399103', 'TMST', 'MTUS', 2024, 1)]
    for cusip, old, current, year, quarter in rules:
        previous = (year, quarter-1) if quarter>1 else (year-1,4)
        assert mapped_symbol(cusip,{old,current},year,quarter)==current
        assert mapped_symbol(cusip,{old,current},*previous) is None
        assert mapped_symbol(cusip,{old},year,quarter)==old
        assert mapped_symbol(cusip,{old,current,"UNRELATED"},year,quarter) is None
        assert mapped_symbol("WRONGCUSIP",{old,current},year,quarter) is None
        assert mapped_symbol(cusip,set(),year,quarter) is None
