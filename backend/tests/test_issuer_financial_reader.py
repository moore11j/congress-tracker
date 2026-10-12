from copy import deepcopy
from datetime import date,timedelta
import json
import pytest
from sqlalchemy import select,func
from app.models import InsightsSnapshot,FundamentalsCache,Event
from app.services import issuer_financial_reader as reader,sec_fundamentals as sec
from app.services import sec_fundamentals_preparation as prep,free_fundamentals as free
from app.services.issuer_financial_staging import KEY,prepare_staged_financials
from test_issuer_financial_staging import scenario,originals,stage,NOW


def base(period='2026-05-09'):
    return {'symbol':'AZO','provider':'sec_edgar','company_name':'AutoZone',
        'fetched_at':NOW,'period_date':date.fromisoformat(period),'status':'ok','error':None,
        'revenue_growth':4.0,'operating_margin':19.0,'free_cash_flow':500.0,'roe':100.0,
        'current_ratio':2.0,'price_to_sales':4.0,'market_cap':1000.0,
        'source_evidence_json':json.dumps({'cik':'0000866787','methodology':sec.VERSION,
            'current':{'revenue':{'value':100}},'prior':{},'instant':{},'calculations':{},
            'facts':{'sha256':'old'},'company':{'sha256':'checked'},'available_by':'2026-10-11'})}


@pytest.fixture
def ready(scenario,monkeypatch):
    monkeypatch.setattr(reader,'SessionLocal',scenario)
    with scenario() as db:
        doc=stage(db)
        prepare_staged_financials(db,doc.id,security_id=1,observed_at=NOW)
        db.commit()
    return scenario


def apply(values=None,company=None,now=NOW):
    return reader.supplement(values or base(),company or originals()[0],observed_at=now)


def test_new_annual_period_replaces_only_verified_metrics_and_clears_older_values(ready):
    old=base();before=deepcopy(old)
    value=apply(old)
    assert old==before
    assert value['period_date']==date(2026,8,29)
    assert value['revenue_growth']==100
    assert 'eps_growth' in value
    for field in ('free_cash_flow','roe','current_ratio','price_to_sales','market_cap'):
        assert field not in value
    evidence=json.loads(value['source_evidence_json'])
    assert evidence['current']['revenue']['end']=='2026-08-29'
    assert evidence['current']['revenue']['value']==2000000
    assert evidence['instant']=={}
    assert evidence['issuer_release_preparation']['status']=='used'
    assert evidence['issuer_release_preparation']['rechecked_at']==NOW.isoformat()
    with ready() as db:
        for model in (FundamentalsCache,Event):
            assert db.scalar(select(func.count()).select_from(model))==0
    assert apply()==value


@pytest.mark.parametrize('period',['2026-08-29','2026-11-28'])
def test_equal_or_newer_sec_period_keeps_complete_sec_values(ready,period):
    old=base(period);value=apply(old)
    assert all(value[k]==v for k,v in old.items() if k!='source_evidence_json')
    assert json.loads(value['source_evidence_json'])['issuer_release_preparation']['status']=='not_needed'


@pytest.mark.parametrize('case',['missing','version','metric','future','latest_filing','identity'])
def test_missing_tampered_or_superseded_preparation_cannot_override_sec(ready,case):
    company=originals()[0];old=base();now=NOW
    with ready() as db:
        row=db.get(InsightsSnapshot,KEY);receipt=json.loads(row.payload_json)
        if case=='missing':db.delete(row)
        elif case=='version':receipt['staging_version']='obsolete'
        elif case=='metric':receipt['values']['revenue_growth']=999
        elif case=='future':now=NOW-timedelta(seconds=1)
        elif case=='identity':old['source_evidence_json']='{"cik":"0000000001"}'
        else:
            data=json.loads(company);recent=data['filings']['recent']
            recent['accessionNumber'][0]='0001171843-26-009999';recent['filingDate'][0]='2026-10-01'
            company=json.dumps(data).encode()
        if case!='missing':row.payload_json=json.dumps(receipt)
        db.commit()
    value=apply(old,company,now)
    assert value['period_date']==old['period_date'] and value['free_cash_flow']==500
    assert json.loads(value['source_evidence_json'])['issuer_release_preparation']['status']=='held'


def test_other_issuers_do_not_read_staging(monkeypatch):
    monkeypatch.setattr(reader,'SessionLocal',lambda:pytest.fail('unrelated issuer database call'))
    old=base();old['symbol']='AAPL'
    assert reader.supplement(old,b'invalid',observed_at=NOW) is old


def test_combination_cannot_backfill_old_ratios_into_newer_annual_period(ready):
    values=apply()
    metric={'symbol':'AZO','source':'finnhub','version':free.finnhub_metrics.VERSION,
        'values':{'current_ratio':2.0,'revenue_growth':50.0},
        'fields':{'current_ratio':{'period_date':'2026-05-09'},'revenue_growth':{'period_date':'2026-08-29'}}}
    result=free.combine(values,metric)
    assert result['revenue_growth']==100 and result.get('current_ratio') is None
    assert json.loads(result['source_evidence_json'])['supplement']['used_fields']==[]


def test_core_fetch_and_preparation_both_apply_reviewed_statement(ready,monkeypatch):
    from app.clients.direct_sources import DirectSourceClient
    from app.clients import massive_stocks
    from app.services import sec_financial_continuity,sec_inline_financials,sec_issuer_cash_flow,sec_directory
    company=originals()[0]
    metadata=json.loads(company);metadata.update(name='AUTOZONE INC',exchanges=['NYSE'])
    company=json.dumps(metadata).encode()
    calls=[]
    def get(self,url):calls.append(url);return b'{}' if 'companyfacts' in url else company
    monkeypatch.setattr(DirectSourceClient,'get',get)
    monkeypatch.setattr(sec,'company_directory',lambda *a:{'AZO':{'cik':'0000866787'}})
    monkeypatch.setattr(sec_directory,'directory',lambda:{'AZO':{'cik':'0000866787'}})
    monkeypatch.setattr(sec_financial_continuity,'fetch_history',lambda *a,**k:None)
    monkeypatch.setattr(sec_inline_financials,'acquire',lambda *a,**k:None)
    monkeypatch.setattr(sec,'project_fundamentals',lambda *a,**k:base())
    monkeypatch.setattr(sec_issuer_cash_flow,'supplement',lambda values,*a,**k:values)
    monkeypatch.setattr(massive_stocks,'selected',lambda:False)
    monkeypatch.setattr(prep,'SessionLocal',ready)
    monkeypatch.setenv('SEC_FUNDAMENTALS_WARMING_ENABLED','1')
    core=sec.fetch_fundamentals('AZO')
    candidate=prep.prepare('AZO')
    assert core['period_date']==date(2026,8,29)
    assert candidate['period_date']=='2026-08-29'
    assert core['eps_growth']==candidate['values']['eps_growth']
    assert len(calls)==4 and all('sec.gov' in call for call in calls)
    assert candidate['calculation_evidence']['issuer_release_preparation']['status']=='used'
    assert candidate['version']==prep.version_for_symbol('AZO')
    assert prep.prepare('AZO')==candidate and len(calls)==4


def test_reviewed_issuer_enters_existing_bounded_warmer(ready,monkeypatch):
    from contextlib import nullcontext
    from types import SimpleNamespace
    from app.jobs import warm_sec_earnings as job
    from app.services import data_enrichment_queue as queue,sec_press_releases as press
    monkeypatch.setattr(job,'SessionLocal',ready)
    monkeypatch.setattr(job,'collector_lock',lambda:nullcontext(True))
    monkeypatch.setattr(job,'check_background_job_guard',lambda *a:SimpleNamespace(proceed=True))
    monkeypatch.setattr(queue,'_recently_viewed_ticker_symbols',lambda *a,**k:[])
    monkeypatch.setattr(queue,'DEFAULT_PREWARM_SYMBOLS',[])
    calls=[]
    def prepare(symbol):
        calls.append(symbol)
        return {'status':'ok','items':[],'provider':'sec_edgar_earnings'}
    monkeypatch.setattr(press,'prepare_sec_releases',prepare)
    result=job.run()
    assert calls==['AZO'] and result['universe_size']==1 and result['planned_scopes']==1
    assert result['canonical_writes']==result['emails']==0


from test_custom_alert_fundamental_basis import scenario as alert_scenario


def test_actual_issuer_basis_change_rebaselines_alert_then_genuine_change_triggers(ready,alert_scenario):
    from app.models import Security,MonitoringAlert
    from app.services.custom_alert_rules import validate_conditions
    db,rule,row,run=alert_scenario
    db.scalar(select(Security)).symbol='AZO';row.symbol='AZO'
    rule.conditions_json=json.dumps(validate_conditions([dict(metric='operating_margin',
        operator='gt',comparison_type='value',comparison_value=25)]))
    row.provider='free_direct';row.operating_margin=19
    old=base();evidence=json.loads(old['source_evidence_json'])
    evidence['methodology']=free.VERSION
    evidence['calculations']={'operating_margin':'TTM operating_income / TTM revenue * 100'}
    row.source_evidence_json=json.dumps(evidence)
    assert run(0)['initialized']==1
    replacement=apply()
    no_metrics={'source':'finnhub','version':free.finnhub_metrics.VERSION,
        'symbol':'AZO','values':{},'fields':{}}
    combined=free.combine(replacement,no_metrics)
    row.source_evidence_json=combined['source_evidence_json'];row.operating_margin=combined['operating_margin']
    transition=run(5)
    assert transition['initialized']==1 and transition['triggered']==0
    assert run(10)['triggered']==0
    row.operating_margin=20;assert run(15)['triggered']==0
    row.operating_margin=30;assert run(20)['triggered']==1
    assert run(25)['triggered']==0
    assert len(db.scalars(select(MonitoringAlert)).all())==1


def test_new_receipt_invalidates_fresh_financial_preparation_immediately(ready,monkeypatch):
    from datetime import datetime,timezone
    from app.services.issuer_financial_staging import KEY
    now=datetime.now(timezone.utc)
    monkeypatch.setattr(prep,'SessionLocal',ready)
    with ready() as db:
        dependency=prep._issuer_dependency(db,'AZO')
        payload={'version':prep.version_for_symbol('AZO'),'source':'sec_edgar','symbol':'AZO',
            'observed_at':now.isoformat(),'issuer_dependency':dependency,'status':'partial'}
        db.add(InsightsSnapshot(kind='sec-current-fundamentals:AZO:v1',source='sec_edgar',
            fetched_at=now,payload_json=json.dumps(payload)))
        db.commit()
    assert prep._cached('AZO',now)==payload
    with ready() as db:
        row=db.get(InsightsSnapshot,KEY)
        value=json.loads(row.payload_json);value['version']='next-reviewed-version'
        row.payload_json=json.dumps(value);db.commit()
    assert prep._cached('AZO',now) is None
    with ready() as db:
        db.delete(db.get(InsightsSnapshot,KEY));db.commit()
    assert prep._cached('AZO',now) is None


def test_receipt_change_during_preparation_cannot_publish_mixed_snapshot(ready,monkeypatch):
    from datetime import datetime,timezone
    from app.services import sec_directory
    monkeypatch.setattr(prep,'SessionLocal',ready)
    monkeypatch.setenv('SEC_FUNDAMENTALS_WARMING_ENABLED','1')
    with ready() as db:
        db.add(InsightsSnapshot(kind='sec-current-fundamentals:AZO:v1',source='sec_edgar',
            fetched_at=datetime.now(timezone.utc),payload_json='{"version":"older-valid-state"}'))
        db.commit()
    def changed_directory():
        with ready() as db:
            row=db.get(InsightsSnapshot,KEY)
            value=json.loads(row.payload_json);value['version']='changed-during-collection'
            row.payload_json=json.dumps(value);db.commit()
        return {}
    monkeypatch.setattr(sec_directory,'directory',changed_directory)
    with pytest.raises(ValueError,match='source changed during preparation'):
        prep.prepare('AZO')
    with ready() as db:
        assert db.get(InsightsSnapshot,'sec-current-fundamentals:AZO:v1').payload_json=='{"version":"older-valid-state"}'

