"""Historical shell recovery must preserve identity and reject prior activity."""
from datetime import date, datetime, timezone
import json
import pytest
from sqlalchemy import select
from app.models import InstitutionalFiling, InstitutionalPosition, InstitutionalPositionChange, Event
from app.services.direct_feed_collection import parse_document
from app.services.direct_13f_publication import rehearse_new_13f
from app.services import institutional_activity as activity
from test_direct_13f_publication import db, document, mappings, publish, count, SINCE, CIK


def shell(db, doc):
    _, parsed, reasons = parse_document(doc['feed'], doc['raw'], doc['metadata'])
    assert not reasons
    m = parsed['metadata']
    filing = InstitutionalFiling(id=555, cik=CIK, accession_number=m['key'],
        filing_date=date.fromisoformat(m['filing_date']), report_year=m['report_year'], report_quarter=m['report_quarter'],
        report_period_end=date.fromisoformat(m['report_period']), form_type='13F-HR', is_amendment=False,
        filing_url='https://www.sec.gov/Archives/original-index.htm', raw_metadata_json='{"legacy":"retained"}')
    db.add(filing); db.commit()
    return filing


def snapshot(db):
    return {model.__tablename__: [{c.name:getattr(row,c.name) for c in model.__table__.columns}
        for row in db.scalars(select(model).order_by(model.id))]
        for model in (InstitutionalFiling, InstitutionalPosition, InstitutionalPositionChange, Event)}


def test_historical_shell_is_filled_once_without_replacing_id_or_creating_alerts(db):
    mappings(db); doc=document(quarter=2,serial=2); filing=shell(db,doc)
    original=(filing.id,filing.created_at,filing.filing_date,filing.filing_url)
    result=publish(db,doc)
    assert result['inserted_filings']==0 and result['inserted_positions']==1
    assert result['derived_state']=='historical_no_alerts' and result['feed_events']==result['changes']==0
    assert count(db,InstitutionalFiling)==1 and count(db,Event)==0
    assert (filing.id,filing.created_at,filing.filing_date,filing.filing_url)==original
    meta=json.loads(filing.raw_metadata_json)
    assert meta['legacy']=='retained' and meta['_walnut_direct_13f']['source_sha256']==doc['content_hash']
    position=db.scalar(select(InstitutionalPosition).where(InstitutionalPosition.filing_id==555))
    assert position.normalized_symbol=='AAA' and position.value_usd==100_000_000
    before=snapshot(db); repeated=publish(db,doc)
    assert repeated['inserted_positions']==0 and snapshot(db)==before


@pytest.mark.parametrize('invalid', ['processed','owned','amendment','superseded','date','period','current','boundary','duplicate_parent','partial','orphan_position','change','event'])
def test_unsafe_shell_remains_completely_unchanged(db,invalid):
    doc=document(quarter=3 if invalid=='current' else 2,serial=2,
        rows=[('000361105',10,100,''),('000361106',20,200,'')])
    filing=shell(db,doc); boundary=SINCE
    if invalid=='processed':filing.processed_at=datetime(2026,9,1,tzinfo=timezone.utc)
    elif invalid=='owned':filing.raw_metadata_json='{"_walnut_position_source":"sec_edgar"}'
    elif invalid=='amendment':filing.is_amendment=True
    elif invalid=='superseded':filing.superseded_by=999
    elif invalid=='date':filing.filing_date=date(2026,8,7)
    elif invalid=='period':filing.report_period_end=date(2026,3,31)
    elif invalid=='boundary':boundary=filing.filing_date
    elif invalid=='duplicate_parent':
        db.add(InstitutionalFiling(cik=CIK.lstrip('0'),accession_number='0001096906-26-009999',filing_date=filing.filing_date,
            report_year=2026,report_quarter=2,form_type='13F-HR'))
    elif invalid in {'partial','orphan_position'}:
        db.add(InstitutionalPosition(filing_id=555 if invalid=='partial' else 999,cik=CIK.lstrip('0'),cusip='000361105',
            symbol='AAA',normalized_symbol='AAA',shares=10,value_usd=100,report_year=2026,report_quarter=2,filing_date=filing.filing_date))
    elif invalid=='change':
        db.add(InstitutionalPositionChange(cik=CIK.lstrip('0'),normalized_symbol='AAA',cusip='000361105',
            report_year=2026,report_quarter=2,filing_date=filing.filing_date,change_type='new_position'))
    elif invalid=='event':
        db.add(Event(event_type='new_institutional_position',source='13F filing',ts=datetime(2026,8,6),
            source_provider=activity.INSTITUTIONAL_EVENT_SOURCE,member_bioguide_id=CIK.lstrip('0'),
            symbol='AAA',payload_json=json.dumps({'report_year':2026,'report_quarter':2})))
    db.commit(); before=snapshot(db)
    result=rehearse_new_13f(db,doc,publish_since=boundary);db.commit()
    assert result['status']=='held' and snapshot(db)==before


def test_verified_historical_shell_can_support_current_pair_only_once(db):
    mappings(db); prior=document(quarter=2,serial=2,rows=[('000361105',10_000_000,100_000_000,''),('000361106',10_000_000,100_000_000,'')]); shell(db,prior)
    publish(db,prior)
    current=document(rows=[('000361105',30_000_000,300_000_000,''),('000361107',20_000_000,200_000_000,'')])
    first=publish(db,current)
    assert first['derived_state']=='published' and first['feed_events']>0
    before=snapshot(db);again=publish(db,current)
    assert again['feed_events']==0 and snapshot(db)==before


def test_failed_historical_install_restores_entire_shell(db,monkeypatch):
    doc=document(quarter=2,serial=2);shell(db,doc);before=snapshot(db)
    original=activity.upsert_positions_for_filing
    def fail(*args,**kwargs):
        original(*args,**kwargs)
        raise ValueError('controlled installation failure')
    monkeypatch.setattr(activity,'upsert_positions_for_filing',fail)
    with pytest.raises(ValueError,match='controlled installation failure'):
        rehearse_new_13f(db,doc,publish_since=SINCE)
    db.expire_all()
    assert snapshot(db)==before
