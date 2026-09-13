import json
from datetime import date
import pytest
from sqlalchemy import select
from test_institutional_activity import _engine, _session
from app import ingest_institutional_activity as ingest
from app.models import InstitutionalFiling, InstitutionalPosition
from app.services import institutional_activity as s


def test_verified_sec_restatement_resists_stale_provider_and_wrong_accession(monkeypatch):
    with _session(_engine()) as db:
        original=s.parse_latest_filing({'cik':'0001081019','reportYear':2026,'reportQuarter':2,'filingDate':'2026-08-24','accessionNumber':'0001081019-26-000019','formType':'13F-HR'})
        amendment=s.parse_latest_filing({**original.raw,'accessionNumber':'0001081019-26-000020','formType':'13F-HR/A','source':'sec_edgar'})
        old,_=s.upsert_institutional_filing(db,original)
        amended,_=s.upsert_institutional_filing(db,amendment)
        row={'symbol':'NVDA','cusip':'67066G104','shares':35021490,'valueUsd':7007449934,'source':'sec_edgar','accessionNumber':amended.accession_number}
        s.upsert_positions_for_filing(db,filing=amended,rows=[row]);db.flush()
        assert old.superseded_by==amended.id
        s.upsert_institutional_filing(db,s.parse_latest_filing({**amendment.raw,'source':'fmp'}))
        assert json.loads(amended.raw_metadata_json)['_walnut_position_source']=='sec_edgar'
        with pytest.raises(ValueError,match='exact canonical accession'):
            s.upsert_positions_for_filing(db,filing=amended,rows=[{'symbol':'NVDA','shares':7007449934,'valueUsd':35021490}])
        with pytest.raises(ValueError,match='exact canonical accession'):
            s.upsert_positions_for_filing(db,filing=amended,rows=[{**row,'accessionNumber':old.accession_number}])
        calls=[]
        monkeypatch.setattr(ingest,'fetch_13f_information_table',lambda **kw:calls.append(kw) or [row])
        monkeypatch.setattr(ingest,'fetch_institutional_filing_extract',lambda **kw:pytest.fail('stale provider used'))
        assert ingest._fetch_positions_for_canonical_filing(amended)==[row]
        assert calls==[{'cik':'0001081019','accession_number':amended.accession_number}]
        s.upsert_positions_for_filing(db,filing=amended,rows=[{k:v for k,v in row.items() if k!='symbol'}]);db.flush()
        position=db.execute(select(InstitutionalPosition).where(InstitutionalPosition.filing_id==amended.id)).scalar_one()
        assert (position.normalized_symbol,position.shares,position.value_usd)==('NVDA',35021490,7007449934)


@pytest.mark.parametrize('shares,value,expected',[(35021490,7007449934,'decrease'),(40000000,6000000000,'increase'),(36688762,7007449934,'unchanged')])
def test_derived_activity_uses_share_changes(shares,value,expected):
    prior=InstitutionalPosition(id=1,normalized_symbol='NVDA',shares=36688762,value_usd=6398520093)
    current=InstitutionalPosition(id=2,normalized_symbol='NVDA',shares=shares,value_usd=value)
    filing=InstitutionalFiling(cik='0001081019',report_year=2026,report_quarter=2,filing_date=date(2026,8,24))
    payload=s._derived_activity_payload(current,prior,current_filing=filing,current_total=108615536242,prior_total=100000000000,current_prices={})
    assert payload['change_type']==expected
    assert payload['action']=={'decrease':'Reported Reduction','increase':'Reported Increase','unchanged':'Unchanged'}[expected]


def test_provider_rankings_do_not_reintroduce_superseded_sec_values():
    bad={'cik':'1081019','shares':7007449934,'value_usd':35021490}
    other={'cik':'0001364742','shares':1943474686,'value_usd':388900000000}
    corrected={'cik':'0001081019','shares':35021490,'value_usd':7007449934}
    assert s._prefer_verified_sec_holders([bad,other],{'0001081019':corrected},{'0001081019'})==[other,corrected]
    assert s._prefer_verified_sec_holders([bad,other],{}, {'0001081019'})==[other]
