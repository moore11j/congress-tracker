from datetime import date

import pytest
from sqlalchemy import create_engine,func,select
from sqlalchemy.orm import Session

from app.models import InstitutionalPosition as Position
from app.services.institutional_capture_export import position_records


@pytest.fixture
def database():
    engine=create_engine('sqlite://')
    Position.__table__.create(engine)
    with engine.begin() as c:
        c.execute(Position.__table__.insert(),[
            dict(id=i*3,filing_id=i,cik=('00000000123' if i%17==0 else '123' if i%19==0 else '456'),
                 report_year=2026,report_quarter=3,filing_date=date(2026,10,7),cusip=str(i))
            for i in range(1,385)])
    with Session(engine) as db:yield db
    engine.dispose()


@pytest.mark.parametrize('page_size',[1,17,64,10000])
@pytest.mark.parametrize('ciks',[['123'],['456'],['123','456'],['999'],[]])
def test_complete_sparse_range_and_legacy_identity_parity(database,page_size,ciks):
    query=select(*Position.__table__.columns).where(func.ltrim(Position.cik,'0').in_(ciks)).order_by(Position.id)
    expected=[dict(r) for r in database.execute(query).mappings()]
    actual=[dict(r) for r in position_records(database,query,page_size=page_size)]
    assert actual==expected
    assert len({r['id'] for r in actual})==len(actual)
    assert [dict(r) for r in position_records(database,query,page_size=page_size)]==actual


def test_empty_inventory(database):
    database.execute(Position.__table__.delete())
    assert list(position_records(database,select(*Position.__table__.columns)))==[]


@pytest.mark.parametrize('size',[0,-1,10001,True,1.5])
def test_invalid_scan_range_is_rejected(database,size):
    with pytest.raises(ValueError,match='invalid_position_page_size'):
        list(position_records(database,select(*Position.__table__.columns),page_size=size))
