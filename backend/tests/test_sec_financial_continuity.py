from copy import deepcopy
from datetime import date, datetime, timezone
import hashlib
import json
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import QueuePool
from app.db import Base
from app.models import InsightsSnapshot
from app.services import sec_financial_continuity as continuity, sec_financial_statements as statements
from app.services import sec_fundamentals as sec, sec_directory
from test_sec_fundamentals import sources

NOW=datetime(2026,10,10,23,tzinfo=timezone.utc)
REVENUE='RevenueFromContractWithCustomerExcludingAssessedTax'


def originals():
    old,company=sources()
    old.update(cik=34088,entityName='Exxon Mobil Corporation')
    current=deepcopy(old);current.update(cik=2115436,entityName='ExxonMobil Holdings Corp')
    for concept in current['facts']['us-gaap'].values():
        concept['units']['USD']=[r for r in concept['units']['USD'] if r['end']=='2026-06-30']
    company.update(cik='2115436',name='ExxonMobil Holdings Corp',tickers=['XOM'])
    return current,old,company


def merge(current,old,**kwargs):
    params=dict(symbol='XOM',cik='2115436',available_by=date(2026,10,9),observed_at=NOW,
                history_raw=json.dumps(old).encode())
    params.update(kwargs)
    return continuity.record_sets(current,**params)


def test_matching_accounting_history_restores_ttm_and_reported_rows_with_provenance():
    current,old,company=originals()
    args=dict(symbol='XOM',cik='2115436',observed_at=NOW)
    with pytest.raises(sec.SecFundamentalsCoverageError):
        sec.project_fundamentals(json.dumps(current).encode(),json.dumps(company).encode(),**args)
    r=sec.project_fundamentals(json.dumps(current).encode(),json.dumps(company).encode(),
                              history_raw=json.dumps(old).encode(),**args)
    assert r['free_cash_flow']==32.5
    e=json.loads(r['source_evidence_json'])
    inputs=e['current']['revenue']['inputs']
    assert {i['source_cik'] for i in inputs}=={continuity.CURRENT_CIK,continuity.PREDECESSOR_CIK}
    assert all(i['source_sha256']==hashlib.sha256(json.dumps(old).encode()).hexdigest()
               for i in inputs if i['source_cik']==continuity.PREDECESSOR_CIK)
    panel=statements.project(json.dumps(current).encode(),json.dumps(company).encode(),
                             history_raw=json.dumps(old).encode(),**args)
    assert len(panel['annual'])==2 and panel['summary']['freeCashFlowTtm']==32.5
    assert len({r['date'] for r in panel['annual']})==2
    assert panel['sourceEvidence']['financialContinuity']['proof_sha256']==continuity.PROOF_SHA
    assert panel==statements.project(json.dumps(current).encode(),json.dumps(company).encode(),
                                    history_raw=json.dumps(old).encode(),**args)


@pytest.mark.parametrize('change',['value','same_day_conflict','currency','concept','no_overlap'])
def test_mismatched_history_cannot_fill_metric(change):
    current,old,_=originals()
    concept=old['facts']['us-gaap'][REVENUE]
    if change=='value':concept['units']['USD'][-1]['val']=999
    if change=='same_day_conflict':concept['units']['USD'].append({**concept['units']['USD'][-1],'val':999})
    if change=='currency':concept['units']['EUR']=concept['units'].pop('USD')
    if change=='concept':old['facts']['us-gaap']['Revenues']=old['facts']['us-gaap'].pop(REVENUE)
    if change=='no_overlap':concept['units']['USD'].pop()
    result,proof=merge(current,old)
    assert result['revenue']==sec._records(current,'revenue',date(2026,10,9))
    assert proof['metrics']['revenue']['status']!='matched'


@pytest.mark.parametrize('change',['symbol','current_cik','payload_cik','prior_cik','name','early','naive','cutoff','proof'])
def test_unreviewed_relationship_rejected(change,monkeypatch,tmp_path):
    current,old,_=originals();params={}
    if change=='symbol':params['symbol']='OTHER'
    if change=='current_cik':params['cik']='1'
    if change=='payload_cik':current['cik']=1
    if change=='prior_cik':old['cik']=1
    if change=='name':old['entityName']='Unrelated Company'
    if change=='early':params['observed_at']=NOW.replace(hour=18)
    if change=='naive':params['observed_at']=NOW.replace(tzinfo=None)
    if change=='cutoff':params['available_by']=date(2026,7,1)
    if change=='proof':
        p=tmp_path/'proof';p.write_bytes(b'changed');monkeypatch.setattr(continuity,'PROOF_FILE',p)
    with pytest.raises(continuity.ContinuityError):merge(current,old,**params)


def test_predecessor_future_periods_excluded_and_successor_facts_win():
    current,old,_=originals()
    rows=old['facts']['us-gaap'][REVENUE]['units']['USD']
    rows.append(dict(start='2026-07-01',end='2026-09-30',filed='2026-10-01',val=999,form='10-Q',accn='old-future'))
    result,_=merge(current,old)
    assert all(end<=date(2026,6,30) for start,end in result['revenue'])
    shared=result['revenue'][date(2026,1,1),date(2026,6,30)]
    assert shared['source_cik']==continuity.CURRENT_CIK and shared['value']==70


def test_unrelated_or_pre_review_fetch_uses_no_transport():
    class Client:
        def get(self,url):pytest.fail('Unexpected source fetch')
    for symbol,cik,when in [('ABC','1',NOW),('XOM','34088',NOW),('XOM','2115436',NOW.replace(hour=18))]:
        assert continuity.fetch_history(symbol,cik,client=Client(),observed_at=when) is None


def test_statement_source_reads_release_pool_and_failure_preserves_existing_cache(monkeypatch):
    from app.clients.direct_sources import DirectSourceError
    engine=create_engine('sqlite:///:memory:',poolclass=QueuePool,pool_size=1,max_overflow=0,pool_timeout=.1)
    Base.metadata.create_all(engine,tables=[InsightsSnapshot.__table__]);factory=sessionmaker(bind=engine)
    monkeypatch.setattr(statements,'SessionLocal',factory)
    monkeypatch.setattr(sec_directory,'directory',lambda:{'ABC':{'cik':'0000000001'}})
    facts,company=sources();calls=[]
    def fetch(self,url):
        assert engine.pool.checkedout()==0
        calls.append(url)
        return json.dumps(facts if 'companyfacts' in url else company).encode()
    monkeypatch.setattr(statements.DirectSourceClient,'get',fetch)
    first=statements.prepared('ABC');assert len(calls)==2
    assert statements.prepared('ABC')==first and len(calls)==2
    with factory() as db:
        row=db.get(InsightsSnapshot,'sec-financials:ABC:v1');row.fetched_at=NOW.replace(year=2025);db.commit()
        before=row.payload_json
    def fail(self,url):
        assert engine.pool.checkedout()==0
        raise DirectSourceError('Source HTTP 429: '+url)
    monkeypatch.setattr(statements.DirectSourceClient,'get',fail)
    with pytest.raises(DirectSourceError):statements.prepared('ABC')
    with factory() as db:assert db.get(InsightsSnapshot,'sec-financials:ABC:v1').payload_json==before
    assert engine.pool.checkedout()==0
    engine.dispose()


def test_only_reviewed_statement_cache_requires_repreparation(monkeypatch):
    engine=create_engine('sqlite:///:memory:');Base.metadata.create_all(engine,tables=[InsightsSnapshot.__table__])
    factory=sessionmaker(bind=engine)
    with factory() as db:
        for symbol in ['ABC','XOM']:
            payload={'symbol':symbol,'source':'sec_edgar','status':'unavailable'}
            db.add(InsightsSnapshot(kind=f'sec-financials:{symbol}:v1',source='sec_edgar',fetched_at=NOW,payload_json=json.dumps(payload)))
        db.commit()
        assert statements.cached_payload(db,'ABC',now=NOW) is not None
        assert statements.cached_payload(db,'XOM',now=NOW) is None
        row=db.get(InsightsSnapshot,'sec-financials:XOM:v1')
        payload=json.loads(row.payload_json);payload['preparationVersion']='sec_statements_xom_continuity_v1'
        row.payload_json=json.dumps(payload);db.commit()
        assert statements.cached_payload(db,'XOM',now=NOW)==payload
    engine.dispose()
