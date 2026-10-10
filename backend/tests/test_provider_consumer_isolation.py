from datetime import datetime, timezone, timedelta
import pytest
from sqlalchemy import select
from app.models import SearchEntity,SearchEntityTerm,FundamentalsCache
from app.services.universal_search import refresh_stock_search_entities,_entity,_entity_terms
from test_sec_metadata_consumers import db,seed


def state(db,model):
    return sorted([str({c.name:getattr(row,c.name) for c in model.__table__.columns}) for row in db.scalars(select(model))])


def test_stock_refresh_preserves_other_entities_and_repeat_is_noop(db):
    SearchEntity.__table__.create(db.get_bind());SearchEntityTerm.__table__.create(db.get_bind())
    seed(db)
    other=_entity(entity_id='member:test',entity_type='member',source_table='members',source_id='test',
        display_name='Preserved Member',canonical_name='Preserved Member',aliases=['Preserved Member'],
        canonical_url='/members/test')
    db.add(other);db.add_all(_entity_terms(other));db.commit()
    before=[state(db,m) for m in [SearchEntity,SearchEntityTerm]]
    assert refresh_stock_search_entities(db)['changed']
    assert [state(db,m) for m in [SearchEntity,SearchEntityTerm]]==before
    first=refresh_stock_search_entities(db,apply=True);db.commit()
    assert first['changed'] and first['other_entity_writes']==0
    all_before=[state(db,m) for m in [SearchEntity,SearchEntityTerm]]
    assert not refresh_stock_search_entities(db,apply=True)['changed'];db.commit()
    assert [state(db,m) for m in [SearchEntity,SearchEntityTerm]]==all_before
    assert state(db,SearchEntity).count(before[0][0])==1
    assert all(value in state(db,SearchEntityTerm) for value in before[1])
    assert db.scalar(select(SearchEntity).where(SearchEntity.entity_id=='stock:ZBEX-A')).source_table=='sec_directory'
    assert db.scalar(select(SearchEntity).where(SearchEntity.entity_id=='stock:ZBEX.A')) is None


def test_missing_prepared_directory_cannot_empty_stock_index(db):
    SearchEntity.__table__.create(db.get_bind());SearchEntityTerm.__table__.create(db.get_bind())
    from app.clients.direct_sources import DirectSourceError
    with pytest.raises(DirectSourceError):refresh_stock_search_entities(db,apply=True)


def test_ticker_latest_fundamentals_cannot_mix_selected_provider(db,monkeypatch):
    import app.main as main
    FundamentalsCache.__table__.create(db.get_bind())
    now=datetime.now(timezone.utc)
    sec=FundamentalsCache(symbol='ZBEX',provider='sec_edgar',status='ok',fetched_at=now-timedelta(hours=1),gross_margin=22)
    old=FundamentalsCache(symbol='ZBEX',provider='fmp',status='ok',fetched_at=now,gross_margin=77)
    db.add_all([sec,old]);db.commit()
    monkeypatch.setenv('FUNDAMENTALS_PROVIDER','sec_edgar')
    assert main._latest_fundamentals_row(db,'ZBEX').provider=='sec_edgar'
    monkeypatch.setattr(main,'enqueue_data_enrichment_job',lambda **kwargs:None)
    monkeypatch.setattr(main,'record_cache_hit',lambda **kwargs:None)
    monkeypatch.setattr(main,'fetch_fundamentals_for_symbol',lambda *args:pytest.fail('Public SEC cache read fetched provider data'))
    assert main._cached_ticker_fundamentals_row(db,'ZBEX').provider=='sec_edgar'
    monkeypatch.setenv('FUNDAMENTALS_PROVIDER','fmp')
    assert main._latest_fundamentals_row(db,'ZBEX').provider=='fmp'


def test_stock_refresh_replaces_existing_stock_without_identity_collision(db):
    SearchEntity.__table__.create(db.get_bind());SearchEntityTerm.__table__.create(db.get_bind())
    seed(db)
    old=_entity(entity_id='stock:ZBEX',entity_type='stock',display_name='Legacy Name',source_table='ticker_meta',canonical_url='/ticker/ZBEX')
    db.add(old);db.add_all(_entity_terms(old));db.commit()
    assert refresh_stock_search_entities(db,apply=True)['changed'];db.commit()
    rows=list(db.scalars(select(SearchEntity).where(SearchEntity.entity_id=='stock:ZBEX')))
    assert len(rows)==1 and rows[0].display_name=='Zebra Example Inc' and rows[0].source_table=='sec_directory'
    assert not refresh_stock_search_entities(db,apply=True)['changed']
