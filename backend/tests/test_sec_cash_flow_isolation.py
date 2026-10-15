from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json

import pytest
from sqlalchemy import select, func
from app.clients.direct_sources import DirectSourceClient, DirectSourceError
from app.models import InsightsSnapshot, FundamentalsCache, Event
from app.services import sec_fundamentals as sec, sec_issuer_cash_flow as issuer
from app.services import sec_fundamentals_preparation as prep, sec_directory, data_enrichment_queue as queue
from app.jobs import warm_sec_fundamentals as job
from test_sec_issuer_cash_flow import fixture, NOW
from test_sec_fundamentals_warming import factory


@pytest.mark.parametrize('failed_index', [0,1,2])
def test_each_optional_failure_keeps_every_other_field_and_drops_all_partial_cash_flow(failed_index):
    facts,company,docs=fixture()
    raw,meta=json.dumps(facts).encode(),json.dumps(company).encode()
    values=sec.project_fundamentals(raw,meta,symbol='NVDA',cik=issuer.CIK,observed_at=NOW)
    # Prove a failed supplement cannot keep stale/partial cash metrics supplied
    # by an upstream caller. The caller's original object must remain untouched.
    for key in ('free_cash_flow','fcf_margin','fcf_growth'):
        values[key]=999
    original=deepcopy(values);calls=[]
    class Client:
        def get(self,url):
            index=len(calls);calls.append(url)
            if index==failed_index:raise DirectSourceError('Source HTTP 503: diagnostic text')
            return docs[url]
    result=issuer.supplement(values,raw,meta,client=Client())
    excluded={'source_evidence_json','free_cash_flow','fcf_margin','fcf_growth'}
    assert {k:v for k,v in result.items() if k not in excluded}=={k:v for k,v in values.items() if k not in excluded}
    assert all(key not in result for key in excluded-{'source_evidence_json'})
    assert values==original and len(calls)==failed_index+1
    proof=json.loads(result['source_evidence_json'])['issuer_cash_flow']
    assert proof['reason']=='cash_flow_source_unavailable' and proof['retryable'] is True
    assert len(proof['filings'])==failed_index and 'diagnostic text' not in result['source_evidence_json']


def test_selected_core_fetch_returns_verified_metrics_on_supplement_failure(monkeypatch):
    facts,company,docs=fixture()
    monkeypatch.setenv('STOCK_PRICE_PROVIDER','fmp')
    monkeypatch.setattr(sec,'company_directory',lambda day:{'NVDA':{'cik':issuer.CIK}})
    def fetch(self,url):
        if '/Archives/' in url:raise DirectSourceError('Source HTTP 403: '+url)
        return json.dumps(facts if 'companyfacts' in url else company).encode()
    monkeypatch.setattr(DirectSourceClient,'get',fetch)
    actual=sec.fetch_fundamentals('NVDA')
    expected=sec.project_fundamentals(json.dumps(facts).encode(),json.dumps(company).encode(),
        symbol='NVDA',cik=issuer.CIK,observed_at=datetime.now(timezone.utc))
    for key in ('revenue_growth','gross_margin','operating_margin','net_margin','roe','current_ratio'):
        assert actual[key]==expected[key]
    assert 'free_cash_flow' not in actual
    assert json.loads(actual['source_evidence_json'])['issuer_cash_flow']['status']=='unavailable'


def test_real_preparation_failure_cache_repeat_and_recovery(factory,monkeypatch):
    facts,company,docs=fixture();calls=[];down=[True];clock=[NOW]
    class Clock(datetime):
        @classmethod
        def now(cls,tz=None):return clock[0]
    monkeypatch.setattr(prep,'datetime',Clock)
    monkeypatch.setattr(sec_directory,'directory',lambda:{'NVDA':{'cik':issuer.CIK}})
    def fetch(self,url):
        calls.append(url)
        if '/Archives/' in url:
            if down[0]:raise DirectSourceError('Source cooldown: '+url)
            return docs[url]
        return json.dumps(facts if 'companyfacts' in url else company).encode()
    monkeypatch.setattr(DirectSourceClient,'get',fetch)
    first=prep.prepare('NVDA')
    assert first['status']=='partial' and len(first['values'])>=6
    assert first['calculation_evidence']['issuer_cash_flow']['retryable'] is True
    assert 'free_cash_flow' not in first['values'] and len(calls)==3
    with factory() as db:before=db.get(InsightsSnapshot,'sec-current-fundamentals:NVDA:v1').payload_json
    clock[0]+=timedelta(minutes=5)
    assert prep.prepare('NVDA')==first and len(calls)==3
    with factory() as db:assert db.get(InsightsSnapshot,'sec-current-fundamentals:NVDA:v1').payload_json==before
    down[0]=False;clock[0]=NOW+prep.CASH_FLOW_RETRY_TTL
    recovered=prep.prepare('NVDA')
    assert recovered['values']['free_cash_flow']==31.5 and len(calls)==8
    for key,value in first['values'].items():assert recovered['values'][key]==value
    assert recovered['calculation_evidence']['issuer_cash_flow']['status']=='available'
    assert prep.prepare('NVDA')==recovered and len(calls)==8
    with factory() as db:
        assert db.scalar(select(func.count()).select_from(FundamentalsCache))==0
        assert db.scalar(select(func.count()).select_from(Event))==0


def test_partial_cash_flow_stops_sec_batch_and_preserves_existing_backoff(factory,monkeypatch):
    monkeypatch.setenv('SEC_FUNDAMENTALS_UNIVERSE_WARMING_ENABLED','1')
    monkeypatch.setattr(queue,'DEFAULT_PREWARM_SYMBOLS',['NVDA','ZZZ'])
    calls=[]
    def prepare(symbol,**kwargs):
        calls.append(symbol)
        return dict(symbol=symbol,source='sec_edgar',status='partial',values={'roe':20},
            observed_at=NOW.isoformat(),calculation_evidence={'issuer_cash_flow':dict(
                status='unavailable',reason='cash_flow_source_unavailable',retryable=True)})
    monkeypatch.setattr(prep,'prepare',prepare)
    result=job.run()
    assert result['status']=='partial' and calls==['NVDA']
    assert result['sec_stop_reason']=='cash_flow_source_unavailable'
    assert result['completed_scopes']==1 and result['planned_scopes']==2
    assert result['results'][0]['metrics']==1 and result['results'][0]['status']=='partial'
    with factory() as db:
        state=json.loads(db.get(InsightsSnapshot,job.KEY).payload_json)
        assert 'lease' not in state and list(state['attempted_at'])==['NVDA']
        observed=datetime.fromisoformat(result['observed_at'])
        assert datetime.fromisoformat(state['scope_refresh_after']['NVDA'])==observed+timedelta(hours=20)
    assert prep.CASH_FLOW_RETRY_TTL==job.PRIORITY_REFRESH


def test_only_nvda_preparation_version_changes_and_keeps_statement_policy():
    assert prep.version_for_symbol('NVDA')==prep._base_version_for_symbol('NVDA')+'_'+prep.CASH_FLOW_RECOVERY_VERSION
    assert prep.version_for_symbol('AAPL')==prep._base_version_for_symbol('AAPL')


def test_stream_failure_is_sanitized_and_retains_independent_metrics():
    from requests import ConnectionError
    facts,company,docs=fixture()
    raw,meta=json.dumps(facts).encode(),json.dumps(company).encode()
    values=sec.project_fundamentals(raw,meta,symbol='NVDA',cik=issuer.CIK,observed_at=NOW)
    class Client:
        def get(self,url):raise ConnectionError('sensitive transport diagnostic')
    result=issuer.supplement(values,raw,meta,client=Client())
    assert result['revenue_growth']==values['revenue_growth'] and 'free_cash_flow' not in result
    proof=json.loads(result['source_evidence_json'])['issuer_cash_flow']
    assert proof['retryable'] is True and 'sensitive transport diagnostic' not in result['source_evidence_json']


def test_invalid_optional_html_is_held_without_discarding_core_metrics():
    facts,company,docs=fixture()
    raw,meta=json.dumps(facts).encode(),json.dumps(company).encode()
    values=sec.project_fundamentals(raw,meta,symbol='NVDA',cik=issuer.CIK,observed_at=NOW)
    class Client:
        def get(self,url):return b'   '
    result=issuer.supplement(values,raw,meta,client=Client())
    assert result['revenue_growth']==values['revenue_growth'] and 'free_cash_flow' not in result
    proof=json.loads(result['source_evidence_json'])['issuer_cash_flow']
    assert proof['reason']=='invalid_filing_html' and not proof.get('retryable')
