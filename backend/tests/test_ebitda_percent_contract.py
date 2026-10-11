from datetime import datetime, timedelta, timezone
import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session
from app.db import Base
from app.models import FundamentalsCache, FundamentalsSnapshot
from app.services import fundamentals_cache as cache
from app.services.fundamentals_snapshots import upsert_fundamentals_snapshot
from app.services.screener import ScreenerParams, _cache_filters, _matches_fundamental_filters


@pytest.mark.parametrize('raw,expected',[(0,0),(.005,.5),(.6781638552538398,67.81638552538398),(1,100),(1.5,150),(-1,-100),(-7.101256185763228,-710.1256185763228)])
def test_fmp_annual_growth_fraction_has_explicit_units(raw,expected):
    result=cache.normalize_fundamentals_payload(symbol='TEST',growth_row={'growthEBITDA':raw})
    assert result['ebitda_growth']==pytest.approx(expected)


@pytest.mark.parametrize('raw',[None,True,'bad','7%',float('nan'),float('inf'),1e308,10**400])
def test_invalid_or_overflow_growth_does_not_enter_cache(raw):
    result=cache.normalize_fundamentals_payload(symbol='TEST',growth_row={'growthEBITDA':raw})
    assert result['ebitda_growth'] is None


def test_other_input_contracts_and_zero_priority_are_preserved():
    assert cache.normalize_fundamentals_payload(symbol='TEST',screener_row={'ebitdaGrowth':0},growth_row={'growthEBITDA':1.5})['ebitda_growth']==0
    assert cache.normalize_fundamentals_payload(symbol='TEST',growth_row={'ebitdaGrowth':15,'growthEBITDA':1.5})['ebitda_growth']==15


def test_sgrp_actual_statement_growth_drives_filters_without_changing_summary_or_history():
    # Captured annual FMP statements: EBITDA -16.028M versus 2.627M.
    expected=(-16028000-2627000)/abs(2627000)*100
    values=cache.normalize_fundamentals_payload(symbol='SGRP',growth_row={'date':'2025-12-31','growthEBITDA':-7.101256185763228})
    assert values['ebitda_growth']==pytest.approx(expected)
    now=datetime.now(timezone.utc)
    engine=create_engine('sqlite:///:memory:');Base.metadata.create_all(engine)
    with Session(engine) as db:
        values.update(fetched_at=now,revenue_growth=10,roe=20,ev_to_ebitda=12,operating_margin_expansion=2,net_debt_to_ebitda=.3)
        original={**values,'ebitda_growth':-7.101256185763228}
        cache.upsert_fundamentals_cache(db,original);db.commit()
        row=db.scalar(select(FundamentalsCache))
        summary=cache.fundamentals_summary_from_cache_row(row,now=now)
        historical=upsert_fundamentals_snapshot(db,row,observed_at=now-timedelta(days=1));db.commit()
        historical_value=historical.ebitda_growth;historical_hash=historical.source_payload_hash
        cache.upsert_fundamentals_cache(db,values);db.commit()
        assert cache.fundamentals_summary_from_cache_row(row,now=now)==summary
        output=cache.cache_row_to_screener_row(row)
        assert output['ebitda_growth']==pytest.approx(expected)
        excluded=ScreenerParams(ebitda_growth_min=-100)
        included=ScreenerParams(ebitda_growth_max=-100)
        assert not _matches_fundamental_filters(output,excluded)
        assert _matches_fundamental_filters(output,included)
        assert cache.cached_screener_rows(db,filters=_cache_filters(excluded))==[]
        assert [r['symbol'] for r in cache.cached_screener_rows(db,filters=_cache_filters(included))]==['SGRP']
        stamp=row.updated_at;cache.upsert_fundamentals_cache(db,values);db.commit()
        assert row.updated_at==stamp
        db.refresh(historical)
        assert (historical.ebitda_growth,historical.source_payload_hash)==(historical_value,historical_hash)
        assert len(db.scalars(select(FundamentalsSnapshot)).all())==1
    engine.dispose()
