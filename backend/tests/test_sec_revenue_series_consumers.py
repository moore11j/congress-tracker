from test_sec_revenue_series import ROOT,NOW,REVENUES,sources,series
import gzip,hashlib,json
from datetime import datetime
import pytest

@pytest.mark.parametrize('symbol,metric',[('KELYA','revenue_growth'),('KELYA','operating_margin'),('KGS','revenue_growth'),('KGS','operating_margin'),('WAT','operating_margin')])
def test_actual_revenue_series_source_recovery_and_alert_transitions(symbol,monkeypatch,metric):
    import socket,types
    from datetime import timedelta
    from unittest.mock import patch
    from sqlalchemy import create_engine,select,func
    from sqlalchemy.orm import sessionmaker
    from app.db import Base
    from app.models import (FundamentalsCache,Event,EmailDelivery,MonitoringAlert,
        Security,UserAccount,Watchlist,WatchlistItem,WatchlistAlertRule)
    from app.services import sec_fundamentals_preparation as prep,sec_inline_financials as inline,sec_fundamentals as sec
    from app.services.fundamentals_cache import fundamentals_summary_from_cache_row
    from app.services.confirmation_score import _fundamentals_context_source
    from app.services.custom_alert_rules import evaluate_watchlist_custom_alerts,validate_conditions
    monkeypatch.setattr(socket.socket,'connect',lambda *a,**k:pytest.fail('Unexpected network'))
    manifest=json.loads((ROOT/'sources.json').read_text());urls={}
    for name,meta in manifest.items():
        if name.startswith(symbol+'-'):
            raw=gzip.decompress((ROOT/(name+'.gz')).read_bytes())
            assert hashlib.sha256(raw).hexdigest()==meta['sha256'];urls[meta['url']]=raw
    calls=[]
    class Client:
        issuer_document_urls=frozenset()
        def get(self,url):calls.append(url);return urls[url]
    class Clock(datetime):
        @classmethod
        def now(cls,tz=None):return NOW
    facts=urls[manifest[symbol+'-facts.json']['url']];company=urls[manifest[symbol+'-company.json']['url']]
    supplement=inline.acquire(facts,company,symbol=symbol,cik=str(sources(symbol)[1]['cik']).zfill(10),observed_at=NOW,client=Client(),include_debt_precision=True)
    new=sec.project_fundamentals(facts,company,symbol=symbol,cik=str(sources(symbol)[1]['cik']).zfill(10),observed_at=NOW,filing_supplement=supplement)
    assert json.loads(new['source_evidence_json'])['current']['revenue']['value']==REVENUES[symbol]
    proof=json.loads(new['source_evidence_json'])
    assert proof['inline_filing_supplement']['revenue_series_selection']['methodology']==series.VERSION
    engine=create_engine('sqlite://');Base.metadata.create_all(engine);factory=sessionmaker(bind=engine)
    monkeypatch.setenv('SEC_FUNDAMENTALS_WARMING_ENABLED','1')
    with patch.object(prep,'SessionLocal',factory),patch.object(prep,'datetime',Clock),patch.object(prep,'DirectSourceClient',Client),patch.object(prep.sec_directory,'directory',return_value={symbol:{'cik':str(sources(symbol)[1]['cik']).zfill(10)}}):
        payload=prep.prepare(symbol)
        assert payload['status']=='partial' and payload['values']=={k:new[k] for k in prep.METRICS if k in new}
        request_count=len(calls)
        assert prep.prepare(symbol)==payload and len(calls)==request_count
        with factory() as db:
            assert db.scalar(select(func.count()).select_from(FundamentalsCache))==0
            assert db.scalar(select(func.count()).select_from(Event))==0
    summary=fundamentals_summary_from_cache_row(types.SimpleNamespace(**new),now=NOW)
    assert _fundamentals_context_source(summary)
    with factory() as db:
        user=UserAccount(email=symbol+'@example.test');security=Security(symbol=symbol,name=symbol,asset_class='equity')
        db.add_all([user,security]);db.flush()
        watch=Watchlist(name='Partial financials',owner_user_id=user.id);db.add(watch);db.flush()
        db.add(WatchlistItem(watchlist_id=watch.id,security_id=security.id,target_type='ticker'))
        threshold=new[metric]-1
        rule=WatchlistAlertRule(user_id=user.id,watchlist_id=watch.id,name='Current ratio',enabled=True,scope_type='any_watchlist_ticker',match_type='all',delivery='both',conditions_json=json.dumps(validate_conditions([dict(metric=metric,operator='gt',comparison_type='value',comparison_value=threshold)])))
        row=FundamentalsCache(symbol=symbol,provider='sec_edgar',status='ok',fetched_at=NOW,source_evidence_json=json.dumps({'financial_status':'unavailable','calculations':{}}))
        db.add_all([rule,row]);db.commit();monkeypatch.setenv('FUNDAMENTALS_PROVIDER','sec_edgar')
        def run(minutes):
            result=evaluate_watchlist_custom_alerts(db,user_id=user.id,watchlist_id=watch.id,now=NOW+timedelta(minutes=minutes));db.commit();return result
        assert run(0)['triggered']==0
        for key,value in payload['values'].items():setattr(row,key,value)
        row.source_evidence_json=new['source_evidence_json']
        assert run(5)['triggered']==0 and run(10)['triggered']==0
        setattr(row,metric,threshold-1);assert run(15)['triggered']==0
        setattr(row,metric,new[metric]);assert run(20)['triggered']==1 and run(25)['triggered']==0
        setattr(row,metric,None);row.source_evidence_json=json.dumps({'financial_status':'unavailable','calculations':{}})
        assert run(30)['triggered']==0
        assert db.scalar(select(func.count()).select_from(MonitoringAlert))==1
        assert db.scalar(select(func.count()).select_from(EmailDelivery))==0
    engine.dispose()



