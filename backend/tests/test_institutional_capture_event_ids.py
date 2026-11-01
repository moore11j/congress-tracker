from datetime import datetime, timezone

import pytest
from sqlalchemy import create_engine, event, select
from sqlalchemy.orm import Session

from app.models import Event
from app.services.institutional_capture_export import event_ids


@pytest.fixture
def database():
    engine=create_engine('sqlite://')
    Event.__table__.create(engine)
    with engine.begin() as c:
        c.execute(Event.__table__.insert(),[
            dict(id=i*3,member_bioguide_id=member,event_type='institutional',
                 ts=datetime(2026,10,8,tzinfo=timezone.utc),source='fixture',payload_json='{}')
            for i,member in enumerate(['123','0000000123','00000000123',None,'999','123']+
                                      [str(1000+j) for j in range(240)],1)])
    with Session(engine) as db:yield db
    engine.dispose()


@pytest.mark.parametrize('batch_size',[1,17,100])
@pytest.mark.parametrize('variants',[
    [],['123'],['0000000123'],['00000000123'],['missing'],['123','123','0000000123'],
    [str(1000+j) for j in range(240)]+['123','0000000123']])
def test_complete_identity_scope_order_and_repeat(database,batch_size,variants):
    query=select(*Event.__table__.columns).where(Event.member_bioguide_id.in_(variants)).order_by(Event.id)
    expected=[dict(row) for row in database.execute(query).mappings()]
    ids=event_ids(database,variants,batch_size=batch_size)
    actual=[dict(row) for row in database.execute(select(*Event.__table__.columns).where(
        Event.id.in_(ids)).order_by(Event.id)).mappings()]
    assert actual==expected
    assert ids==sorted(set(ids))
    assert event_ids(database,variants,batch_size=batch_size)==ids


@pytest.mark.parametrize('batch_size',[1,100])
def test_global_bound_across_batches_and_exact_boundary(database,batch_size):
    variants=['123','0000000123']
    assert len(event_ids(database,variants,batch_size=batch_size,maximum=3))==3
    with pytest.raises(ValueError,match='events exceeds reviewed row bound'):
        event_ids(database,variants,batch_size=batch_size,maximum=2)


def test_lookup_does_not_order_by_unrelated_primary_key(database):
    statements=[]
    def collect(_conn,_cur,statement,_params,_context,_many):statements.append(statement)
    event.listen(database.bind,'before_cursor_execute',collect)
    try:event_ids(database,['123','0000000123'],batch_size=1)
    finally:event.remove(database.bind,'before_cursor_execute',collect)
    assert len(statements)==2
    assert all('ORDER BY' not in sql and 'LIMIT' in sql for sql in statements)


@pytest.mark.parametrize('key,value',[
    ('batch_size',0),('batch_size',101),('batch_size',True),('batch_size',1.5),
    ('maximum',0),('maximum',100001),('maximum',True),('maximum',1.5)])
def test_invalid_bounds(database,key,value):
    with pytest.raises(ValueError,match='invalid_event_'):
        event_ids(database,['123'],**{key:value})
