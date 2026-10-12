from copy import deepcopy
from datetime import datetime, timezone, date, timedelta
from pathlib import Path
import json
import pytest
from sqlalchemy import create_engine, select, func
from sqlalchemy.orm import sessionmaker
from app.db import Base
from app.models import Security, InsightsSnapshot, Event, FundamentalsCache, ResearchSourceDocument
from app.services.issuer_accenture_financials import URL, prepare_accenture_annual
from app.services.issuer_release_financials import IssuerFinancialError
from app.services.issuer_financial_registry import key_for, PROFILES
from app.services.issuer_financial_staging import prepare_staged_financials
from app.services.sec_earnings_store import stage_earnings_material
from app.services import issuer_financial_reader as reader, sec_press_releases as press

NOW=datetime(2026,10,12,tzinfo=timezone.utc)
FIXTURE=Path(__file__).parent/'fixtures/acn-annual-2026-tables.html'
def raw(): return FIXTURE.read_bytes()
def parse(source=None): return prepare_accenture_annual(source or raw(),source_url=URL,observed_at=NOW)


def test_reported_annual_parent_income_and_cash_flow_not_adjusted_or_quarter():
    value=parse()
    assert value['inputs']['revenue']['current']==74_183_445_000
    assert value['inputs']['net_income']['current']==8_367_056_000
    assert value['inputs']['consolidated_net_income']['current']==8_522_882_000
    assert value['values']['operating_margin']==pytest.approx(15.375344188989875)
    assert value['values']['eps_growth']==pytest.approx((13.66/12.29-1)*100)
    assert value['values']['free_cash_flow']==11_620_598_000
    assert value['values']['current_ratio']==pytest.approx(31_831_140/22_304_374)
    assert value['inputs']['free_cash_flow']['derivation']=='operating_cash_flow + capex_signed'
    assert value['inputs']['current_assets']['period_type']=='instant'
    assert value==parse() and not value['publication_eligible']
    assert not {'net_debt_to_ebitda','ebitda_growth','roe','forward_pe'} & value['values'].keys()


@pytest.mark.parametrize('before,after',[
    ('NYSE: ACN','NYSE: OTHER'),('October 1, 2026','October 2, 2026'),
    ('Year Ended','Quarter Ended'),('August 31,2026','August 31,2027'),
    ('thousands of U.S. dollars','millions of U.S. dollars'),
    ('Consolidated Income Statements','Adjusted Income Statements'),
    ('74,183,445','74,183,4460'),('8,367,056','8,367,0570'),
    ('(742,748)','742,748'),('Net income attributable to noncontrolling interests','Unknown interests'),
    ('EARNINGS PER SHARE:','UNKNOWN SHARES:'),('13.66','NaN'),
    ('Total current liabilities','Unknown current liabilities'),('12,831,471','12,831,4720'),
])
def test_changed_source_identity_columns_units_or_accounting_fail_closed(before,after):
    source=raw();assert before.encode() in source
    with pytest.raises(IssuerFinancialError):parse(source.replace(before.encode(),after.encode()))


def test_duplicate_statements_are_not_silently_selected():
    with pytest.raises(IssuerFinancialError,match='ambiguous'):parse(raw()+raw())


@pytest.mark.parametrize('observed',[NOW.replace(tzinfo=None),datetime(2026,10,1,23,tzinfo=timezone.utc),datetime(2026,10,2,2,tzinfo=timezone.utc)])
def test_source_cannot_be_used_before_publication_day_finishes(observed):
    with pytest.raises(IssuerFinancialError):prepare_accenture_annual(raw(),source_url=URL,observed_at=observed)


def originals():
    accession=PROFILES['ACN']['accession']
    company=json.dumps({'cik':1467373,'tickers':['ACN'],'filings':{'recent':{
        'accessionNumber':[accession],'filingDate':['2026-10-01'],'form':['8-K'],
        'primaryDocument':['report.htm'],'items':['2.02,9.01'],'acceptanceDateTime':['2026-10-01T12:00:00Z']}}}).encode()
    submission=f'''<SEC-HEADER>
<ACCEPTANCE-DATETIME>20261001080000
ACCESSION NUMBER: {accession}
CONFORMED SUBMISSION TYPE: 8-K
FILED AS OF DATE: 20261001
CENTRAL INDEX KEY: 0001467373
ITEM INFORMATION: Results of Operations and Financial Condition
</SEC-HEADER>
<DOCUMENT>
<TYPE>8-K
<FILENAME>report.htm
<TEXT><html><a href="q4fy26earnings8-kexhibit.htm">Earnings release</a></html></TEXT>
</DOCUMENT>
<DOCUMENT>
<TYPE>EX-99.1
<FILENAME>q4fy26earnings8-kexhibit.htm
<TEXT>{raw().decode()}</TEXT>
</DOCUMENT>'''.encode()
    index=f'''<html>CIK: 0001467373<a href="{accession}.txt">Complete submission</a>
<div class="infoHead">Filing Date</div><div>2026-10-01</div>
<div class="infoHead">Accepted</div><div>2026-10-01 08:00:00</div></html>'''.encode()
    return company,submission,index


@pytest.fixture
def dbs(monkeypatch):
    engine=create_engine('sqlite://');Base.metadata.create_all(engine);factory=sessionmaker(bind=engine)
    monkeypatch.setattr(reader,'SessionLocal',factory);monkeypatch.setattr(press,'SessionLocal',factory)
    with factory() as db:db.add(Security(id=1,symbol='ACN',name='Accenture',asset_class='stock'));db.commit()
    yield factory
    engine.dispose()


def base():
    return {'symbol':'ACN','provider':'sec_edgar','company_name':'Accenture','period_date':date(2026,5,31),
        'status':'ok','error':None,'roe':999,'free_cash_flow':1,'revenue_growth':1,
        'source_evidence_json':json.dumps({'cik':'0001467373','current':{},'prior':{},'instant':{},'calculations':{}})}


def stage(db,raws=None):
    company,submission,index=raws or originals()
    return stage_earnings_material(db,company_raw=company,submission_raw=submission,index_raw=index,
        symbol='ACN',cik='0001467373',accession=PROFILES['ACN']['accession'])


def test_staging_reader_instant_evidence_repeat_and_canonical_preservation(dbs):
    with dbs() as db:
        doc=stage(db);first=prepare_staged_financials(db,doc.id,security_id=1,observed_at=NOW);db.commit()
        assert first['status']=='prepared'
        assert prepare_staged_financials(db,doc.id,security_id=1,observed_at=NOW+timedelta(minutes=1))==first
    old=base();before=deepcopy(old)
    value=reader.supplement(old,originals()[0],observed_at=NOW)
    assert old==before and value['period_date']==date(2026,8,31) and 'roe' not in value
    assert value['free_cash_flow']==11_620_598_000
    evidence=json.loads(value['source_evidence_json'])
    assert evidence['issuer_release_preparation']['status']=='used'
    assert 'current_assets' not in evidence['current']
    assert 'start' not in evidence['instant']['current_assets']
    assert evidence['instant']['current_assets']['end']=='2026-08-31'
    assert evidence['current']['free_cash_flow']['inputs'][0]['derivation']=='operating_cash_flow + capex_signed'
    assert value==reader.supplement(old,originals()[0],observed_at=NOW)
    with dbs() as db:
        assert db.get(InsightsSnapshot,key_for('ACN')) is not None
        assert db.get(InsightsSnapshot,key_for('AZO')) is None
        for model in (Event,FundamentalsCache,ResearchSourceDocument):assert db.scalar(select(func.count()).select_from(model))==0


def test_existing_collector_populates_acn_without_extra_requests(dbs,monkeypatch):
    monkeypatch.setenv('SEC_EARNINGS_WARMING_ENABLED','1');sources=originals();calls=[]
    class Client:
        def get(self,url):
            calls.append(url)
            return sources[0] if url.endswith('.json') else sources[2] if url.endswith('-index.html') else sources[1]
    args={'client':Client(),'directory':{'ACN':{'cik':1467373}},'preparation_only':True}
    assert len(press.refresh_sec_releases('ACN',**args)['items'])==1 and len(calls)==3
    with dbs() as db:before=db.get(InsightsSnapshot,key_for('ACN')).payload_json
    assert len(press.refresh_sec_releases('ACN',**args)['items'])==1 and len(calls)==6
    with dbs() as db:assert db.get(InsightsSnapshot,key_for('ACN')).payload_json==before


from test_custom_alert_fundamental_basis import scenario as alert_scenario


def test_actual_acn_basis_change_rebaselines_alert_then_real_crossing_triggers(dbs,alert_scenario):
    from app.models import MonitoringAlert
    from app.services import free_fundamentals as free
    from app.services.custom_alert_rules import validate_conditions
    with dbs() as source_db:
        doc=stage(source_db);prepare_staged_financials(source_db,doc.id,security_id=1,observed_at=NOW);source_db.commit()
    db,rule,row,run=alert_scenario
    db.scalar(select(Security)).symbol='ACN';row.symbol='ACN'
    rule.conditions_json=json.dumps(validate_conditions([dict(metric='operating_margin',
        operator='gt',comparison_type='value',comparison_value=15.5)]))
    row.provider='fmp';row.operating_margin=15.75
    assert run(0)['initialized']==1
    new=reader.supplement(base(),originals()[0],observed_at=NOW)
    combined=free.combine(new,{'source':'finnhub','version':free.finnhub_metrics.VERSION,'symbol':'ACN','values':{},'fields':{}})
    row.provider='free_direct';row.source_evidence_json=combined['source_evidence_json'];row.operating_margin=combined['operating_margin']
    transition=run(5)
    assert transition['initialized']==1 and transition['triggered']==0
    assert run(10)['triggered']==0
    row.operating_margin=16;assert run(15)['triggered']==1
    assert run(20)['triggered']==0
    row.provider='fmp';row.operating_margin=15.75
    rollback=run(25);assert rollback['initialized']==1 and rollback['triggered']==0
    assert len(db.scalars(select(MonitoringAlert)).all())==1


def test_acn_dependency_and_version_invalidate_only_reviewed_cache(dbs,monkeypatch):
    from app.services import sec_fundamentals_preparation as prep
    monkeypatch.setattr(prep,'SessionLocal',dbs)
    with dbs() as db:
        doc=stage(db);prepare_staged_financials(db,doc.id,security_id=1,observed_at=NOW)
        payload={'version':prep.version_for_symbol('ACN'),'source':'sec_edgar','symbol':'ACN',
            'observed_at':NOW.isoformat(),'issuer_dependency':prep._issuer_dependency(db,'ACN'),'status':'partial'}
        db.add(InsightsSnapshot(kind='sec-current-fundamentals:ACN:v1',source='sec_edgar',fetched_at=NOW,payload_json=json.dumps(payload)));db.commit()
    assert prep._cached('ACN',NOW)==payload
    with dbs() as db:
        row=db.get(InsightsSnapshot,key_for('ACN'));row.payload_json='{}';db.commit()
    assert prep._cached('ACN',NOW) is None
    assert prep.version_for_symbol('ACN')!=prep.version_for_symbol('AAPL')
