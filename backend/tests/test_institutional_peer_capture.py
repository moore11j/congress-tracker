from copy import deepcopy
from datetime import date, datetime, timezone
import io
import json

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from app.db import Base
from app.models import Event, InstitutionalFiling, InstitutionalPosition
from app.services.direct_feed_store import DirectFeedDocument
from app.services.institutional_activity import INSTITUTIONAL_EVENT_SOURCE
from app.services import institutional_peer_capture as peer


@pytest.fixture
def case():
    engine = create_engine('sqlite:///:memory:')
    Base.metadata.create_all(engine)
    metadata = {'key':'0000000001-26-000001', 'cik':'0000000001'}
    scope = dict(version=1, parent_capture_id='a'*32, parent_baseline_sha256='b'*64,
        documents=[dict(id=1, content_hash='c'*64, metadata_sha256=peer.digest(metadata))],
        period_cusips={'2026-06-30':['111111111']})
    with Session(engine) as db:
        db.add(DirectFeedDocument(id=1,feed='sec_13f',source_key=metadata['key'],source_url='https://www.sec.gov/source',
            metadata_json=json.dumps(metadata),content_hash='c'*64,status='parsed'))
        identities={1:(1,2026,2),2:(2,2026,2),3:(3,2026,2),4:(1,2025,1),5:(2,2026,3),6:(2,2026,1)}
        for ident,(owner,year,quarter) in identities.items():
            cik=f'{owner:010d}'
            db.add(InstitutionalFiling(id=ident,cik=cik,accession_number=f'{cik}-26-{ident:06d}',
                filing_date=date(2026,10,31),report_year=year,report_quarter=quarter,
                report_period_end=date(year,quarter*3,31 if quarter in (1,4) else 30),form_type='13F-HR'))
        def position(ident, filing, symbol, cusip, year=2026, quarter=2):
            assert identities[filing][1:]==(year,quarter)
            return InstitutionalPosition(id=ident,filing_id=filing,cik=f'{identities[filing][0]:010d}',normalized_symbol=symbol,
                symbol=symbol,cusip=cusip,shares=10,value_usd=100,report_year=year,report_quarter=quarter,filing_date=date(2026,10,31))
        db.add_all([
            position(1,1,'AAA','111111111'),
            position(2,2,'AAA','222222222'),  # Different holder and CUSIP, same summary.
            position(3,3,'ZZZ','333333333'),  # Unrelated security remains outside selected positions.
            position(4,4,'OLD','111111111',2025,1), # Ambiguous/historical mapping expands capture only.
            position(5,2,'OLD','444444444'),
            position(6,5,'AAA','222222222',2026,3),
            position(7,6,'AAA','222222222',2026,1),
        ])
        for ident, provider in ((1,INSTITUTIONAL_EVENT_SOURCE),(2,'unrelated')):
            db.add(Event(id=ident,event_type='institutional_accumulation',symbol='ZZZ',
                ts=datetime(2026,8,1,tzinfo=timezone.utc),source='fixture',source_provider=provider,
                source_filing_id=f'preserve-orphan-{ident}',payload_json='{}'))
        db.commit()
        yield db, scope
    engine.dispose()


def capture(db, scope):
    out=io.BytesIO(); writer=peer.export_peers(db,out,scope)
    receipt=writer.finish(transaction_read_only=True,production_writes=0)
    return out.getvalue(),receipt


def test_complete_peers_ambiguity_following_period_and_orphan_identity(case):
    db,scope=case
    before=[tuple(r) for r in db.execute(select(InstitutionalPosition.id,InstitutionalPosition.shares,InstitutionalPosition.value_usd))]
    raw,receipt=capture(db,scope)
    frames=[json.loads(line) for line in raw.splitlines() if 'table' in json.loads(line)]
    assert {r['row']['id'] for r in frames if r['table']=='positions'}=={1,2,5,6}
    assert {r['row']['id'] for r in frames if r['table']=='filings'}==set(range(1,7))
    assert {r['row']['id'] for r in frames if r['table']=='events'}=={1}
    assert frames[0]['row']['symbol_periods']=={'2026Q2':['AAA','OLD'],'2026Q3':['AAA','OLD']}
    spool=io.BytesIO(); accepted=peer.receive_peers(io.BytesIO(raw),spool,scope)
    assert accepted['counts']==receipt['counts'] and not accepted['full_cutover_ready']
    assert not db.new and not db.dirty and not db.deleted
    assert [tuple(r) for r in db.execute(select(InstitutionalPosition.id,InstitutionalPosition.shares,InstitutionalPosition.value_usd))]==before


@pytest.mark.parametrize('field,value', [('content_hash','d'*64),('feed','sec_form4'),('status','quarantined'),('metadata_json','{}')])
def test_changed_source_is_rejected_before_any_output(case,field,value):
    db,scope=case; setattr(db.get(DirectFeedDocument,1),field,value);db.commit()
    out=io.BytesIO()
    with pytest.raises(ValueError,match='source_changed'):peer.export_peers(db,out,scope)
    assert out.getvalue()==b''


def test_missing_source_is_rejected(case):
    db,scope=case;db.delete(db.get(DirectFeedDocument,1));db.commit()
    with pytest.raises(ValueError,match='source_missing'):capture(db,scope)


@pytest.mark.parametrize('mutation',['truncate','append','row','scope','duplicate','table','period','private_event',
    'terminal_bool','count_bool','terminal_extra'])
def test_receiver_refuses_incomplete_changed_or_out_of_scope_records(case,mutation):
    db,scope=case;raw,_=capture(db,scope)
    if mutation=='truncate':raw=raw.rsplit(b'\n',2)[0]+b'\n'
    elif mutation=='append':raw+=b'{}\n'
    elif mutation=='scope':scope={**scope,'parent_baseline_sha256':'e'*64}
    else:
        frames=[json.loads(line) for line in raw.splitlines()]
        if mutation=='row':frames[3]['row']['cik']='0000000999'
        elif mutation=='duplicate':frames.insert(3,deepcopy(frames[2]))
        elif mutation=='table':frames[3]['table']='user_accounts'
        elif mutation=='period':frames[0]['row']['symbol_periods']['2027Q1']=[]
        elif mutation=='private_event':next(r for r in frames if r.get('table')=='events')['row']['source_provider']='unrelated'
        elif mutation=='terminal_bool':frames[-1]['complete']['production_writes']=False
        elif mutation=='count_bool':frames[-1]['complete']['counts']['metadata']=True
        elif mutation=='terminal_extra':frames[-1]['complete']['approved']=True
        raw=b''.join(peer.encode(f)+b'\n' for f in frames)
    with pytest.raises(ValueError):peer.receive_peers(io.BytesIO(raw),io.BytesIO(),scope)


def test_bounds_do_not_truncate_and_terminal_cannot_repeat(case,monkeypatch):
    db,scope=case
    monkeypatch.setitem(peer.LIMITS,'positions',1)
    with pytest.raises(ValueError,match='table_bound'):capture(db,scope)
    out=io.BytesIO();writer=peer.PeerStream(out);writer.write('metadata',{})
    writer.finish(transaction_read_only=True,production_writes=0)
    with pytest.raises(ValueError):writer.finish()
    with pytest.raises(ValueError):writer.write('filings',{})


def test_sparse_event_windows_preserve_complete_query(case):
    db,_=case
    expected=[dict(r) for r in db.execute(select(Event.__table__).where(Event.source_provider==INSTITUTIONAL_EVENT_SOURCE)).mappings()]
    actual=list(peer.bounded_pk_records(db,Event,select(Event.__table__).where(Event.source_provider==INSTITUTIONAL_EVENT_SOURCE),page_size=1))
    assert [dict(r) for r in actual]==expected
    with pytest.raises(ValueError):list(peer.bounded_pk_records(db,Event,select(Event.__table__),page_size=0))


def test_complete_measured_activity_capacity_and_first_excess_row():
    class Null:
        def write(self, data):pass
    writer=peer.PeerStream(Null())
    writer.write('metadata',{})
    # The measured full source scope contains268491 activities. Preserve all of
    # them and verify the real300000-row boundary without retaining test output.
    for ident in range(300000):writer.write('activity',{'id':ident+1})
    assert writer.counts['activity']==300000
    with pytest.raises(ValueError,match='peer_table_bound'):
        writer.write('activity',{'id':300001})


def test_canonical_scope_file_binds_exact_reviewed_payload(case,tmp_path):
    _,scope=case;path=tmp_path/'scope.json';raw=peer.encode(scope);path.write_bytes(raw)
    assert peer.read_scope(path,peer.digest(scope))==scope
    path.write_bytes(raw+b' ')
    with pytest.raises(ValueError):peer.read_scope(path,peer.digest(scope))


@pytest.mark.parametrize('change',['bool','duplicate_source','hash','quarter','duplicate_cusip','unknown_field'])
def test_scope_validation_rejects_ambiguous_authorization(case,change):
    _,original=case;scope=deepcopy(original)
    if change=='bool':scope['version']=True
    elif change=='duplicate_source':scope['documents']*=2
    elif change=='hash':scope['documents'][0]['content_hash']='secret'
    elif change=='quarter':scope['period_cusips']={'2026-06-29':['111111111']}
    elif change=='duplicate_cusip':scope['period_cusips']['2026-06-30']*=2
    elif change=='unknown_field':scope['customer_records']=True
    with pytest.raises(ValueError):peer.validate_scope(scope)
