from copy import deepcopy
from datetime import date
import json

import pytest

from app.services import sec_asset_turnover as turnover
from app.services import sec_financial_statements as statements
from app.services.ticker_financials import _normalize_health
from test_sec_fundamentals import NOW, sources


def source_pair():
    facts, company = sources()
    facts['facts']['us-gaap']['Assets'] = {'units': {'USD': [
        {'end':'2025-06-30','filed':'2025-08-01','val':200,'accn':'0000000001-25-000001','form':'10-Q'},
        {'end':'2026-06-30','filed':'2026-08-01','val':300,'accn':'0000000001-26-000001','form':'10-Q'},
    ]}}
    return facts,company


def project(facts=None,company=None):
    original,identity=source_pair()
    return statements.project(json.dumps(facts or original).encode(),json.dumps(company or identity).encode(),symbol='ABC',cik='1',observed_at=NOW)


def test_reported_ttm_revenue_over_ending_assets_with_full_provenance_and_repeat():
    result=project()
    assert result['summary']['assetRatio']==pytest.approx(130/300)
    assert result['summary']['assetRatioBasis']==turnover.BASIS
    assert result['health']['assetRatio']==result['summary']['assetRatio']
    assert result['subsections']['health']['data']==result['health']
    evidence=result['sourceEvidence']['assetTurnover']
    assert evidence['status']=='available'
    assert evidence['assets']['tag']=='Assets'
    assert evidence['assets']['value']==300
    assert evidence['revenue']['value']==130
    assert len(evidence['revenue']['inputs'])==3
    assert evidence['unit']=='multiple'
    assert result==project()


@pytest.mark.parametrize('change', ['absent','zero','negative','conflicting','newer_amendment','wrong_period','wrong_unit','future','bool'])
def test_unusable_assets_cannot_be_substituted(change):
    facts,company=source_pair()
    values=facts['facts']['us-gaap']['Assets']['units']['USD']
    point=values[-1]
    if change=='absent': values.clear()
    elif change=='zero': point['val']=0
    elif change=='negative': point['val']=-300
    elif change=='conflicting': values.append({**point,'val':301})
    elif change=='newer_amendment': point.update(filed='2026-08-02',accn='0000000001-26-000002',form='10-Q/A')
    elif change=='wrong_period': point['end']='2026-05-31'
    elif change=='wrong_unit': facts['facts']['us-gaap']['Assets']['units']={'EUR':values}
    elif change=='future': point['filed']='2026-10-09'
    elif change=='bool': point['val']=True
    result=project(facts,company)
    assert result['summary']['assetRatio'] is None
    assert result['summary']['assetRatioBasis'] is None
    assert result['sourceEvidence']['assetTurnover']['status']=='unavailable'
    assert result['summary']['revenueTtm']==130
    assert result['summary']['currentRatio']==2


@pytest.mark.parametrize('change', ['wrong_cik','currency','filing','period','negative_revenue','nonfinite','bool','duplicate_closing','no_accession'])
def test_calculation_rejects_incompatible_evidence(change):
    result=project()
    prepared=result['sourceEvidence']['assetTurnover']
    evidence={'current':{'revenue':deepcopy(prepared['revenue'])},'period_start':prepared['period_start'],
        'period_end':prepared['period_end'],'available_by':'2026-10-07','cik':'0000000001'}
    asset=deepcopy(prepared['assets'])
    if change=='wrong_cik': asset['source_cik']='2'
    elif change=='currency': asset['unit']='EUR'
    elif change=='filing': asset['accession']='other'
    elif change=='period': evidence['period_end']='2026-05-31'
    elif change=='negative_revenue': evidence['current']['revenue']['value']=-1
    elif change=='nonfinite': asset['value']=float('inf')
    elif change=='bool': asset['value']=True
    elif change=='duplicate_closing': evidence['current']['revenue']['inputs'].append(deepcopy(evidence['current']['revenue']['inputs'][1]))
    elif change=='no_accession': asset['accession']=''
    assert turnover.derive(evidence,{(None,date(2026,6,30)):asset})['status']=='unavailable'


def test_zero_revenue_is_not_missing_and_no_liability_or_average_assets_fallback():
    prepared=project()['sourceEvidence']['assetTurnover']
    revenue=deepcopy(prepared['revenue']); revenue['value']=0
    evidence={'current':{'revenue':revenue},'period_start':prepared['period_start'],
        'period_end':prepared['period_end'],'available_by':'2026-10-07','cik':'1'}
    result=turnover.derive(evidence,{(None,date(2026,6,30)):prepared['assets']})
    assert result['status']=='available' and result['value']==0
    health=_normalize_health([],[],[{'totalAssets':300,'totalLiabilities':150}])
    assert health['assetRatio'] is None and health['assetRatioBasis'] is None


def test_fmp_reported_asset_turnover_preserves_zero_and_explicit_basis():
    health=_normalize_health([{'assetTurnoverTTM':0}],[{'assetTurnoverTTM':4}],[{'totalAssets':300,'totalLiabilities':150}])
    assert health['assetRatio']==0
    assert health['assetRatioBasis']=='revenue_to_total_assets'
    assert _normalize_health([],[{'assetTurnoverTTM':.75}],[])['assetRatio']==.75


def test_prepared_turnover_survives_cache_and_public_reads_without_extra_fetches(monkeypatch):
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    from app.db import Base
    from app.models import InsightsSnapshot
    from app.services import sec_directory, ticker_financials, data_enrichment_queue
    from app.request_priority import set_request_context, reset_request_context
    engine = create_engine('sqlite:///:memory:')
    Base.metadata.create_all(engine)
    factory = sessionmaker(bind=engine)
    monkeypatch.setattr(statements, 'SessionLocal', factory)
    monkeypatch.setattr(sec_directory, 'directory', lambda: {'ABC': {'cik': '0000000001'}})
    monkeypatch.setenv('FINANCIAL_STATEMENTS_PROVIDER', 'sec_edgar')
    facts, company = source_pair()
    calls, queued = [], []
    def fetch(self, url):
        calls.append(url)
        return json.dumps(facts if 'companyfacts' in url else company).encode()
    monkeypatch.setattr(statements.DirectSourceClient, 'get', fetch)
    monkeypatch.setattr(data_enrichment_queue, 'enqueue_data_enrichment_job', lambda **kw: queued.append(kw))
    try:
        prepared = ticker_financials.get_ticker_financials('ABC')
        assert prepared['summary']['assetRatio'] == pytest.approx(130/300)
        assert len(calls) == 2
        token = set_request_context({'path': '/api/tickers/ABC/financials'})
        try:
            for _ in range(2):
                assert ticker_financials.get_ticker_financials('ABC') == prepared
        finally:
            reset_request_context(token)
        with factory() as db:
            rows = db.query(InsightsSnapshot).all()
            assert len(rows) == 1
            assert json.loads(rows[0].payload_json) == prepared
        assert len(calls) == 2 and not queued
    finally:
        engine.dispose()
