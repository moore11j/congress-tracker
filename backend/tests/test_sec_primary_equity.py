from copy import deepcopy
from datetime import date,datetime,timezone
import gzip,hashlib,json
from pathlib import Path
from unittest.mock import patch
import pytest
from lxml import html
from app.services import sec_fundamentals as sec,sec_primary_equity as equity
from app.services.sec_inline_financials import nodes

ROOT=Path(__file__).parent/'fixtures/sec_primary_equity'
MANIFEST=json.loads((ROOT/'sources.json').read_bytes())
RAW={k:gzip.decompress((ROOT/v['file']).read_bytes()) for k,v in MANIFEST.items()}
COMPANY=json.loads(RAW['company']);PAYLOAD=json.loads(RAW['facts'])
CIK=str(COMPANY['cik']).zfill(10);NOW=datetime(2026,11,18,21,15,tzinfo=timezone.utc)
DAY=date(2026,11,17)
class Client:
    issuer_document_urls=frozenset()
    def __init__(self):self.calls=[]
    def get(self,url):
        self.calls.append(url)
        return next(RAW[k] for k,v in MANIFEST.items() if v['url']==url)

@pytest.mark.parametrize('name',MANIFEST)
def test_complete_original(name):
    assert len(RAW[name])==MANIFEST[name]['bytes']
    assert hashlib.sha256(RAW[name]).hexdigest()==MANIFEST[name]['sha256']

@pytest.mark.parametrize('name,expected',[('current',2160426000),('prior',2079374000)])
def test_complete_parent_equity(name,expected):
    row=equity.statement(RAW[name],MANIFEST[name]['filing'],issuer=COMPANY['name'])
    assert row['value']==expected

@pytest.mark.parametrize('name',['current','prior'])
@pytest.mark.parametrize('mutation',['treasury','year','extra_component','nil','hidden','unit','scale','minority','parent_nil','aoci'])
def test_rejects_unqualified_original(name,mutation):
    tree=html.fromstring(RAW[name],parser=html.HTMLParser(encoding='utf-8'))
    if mutation=='year':
        for n in tree.iter():
            if n.text and n.text.strip()==MANIFEST[name]['filing']['report_date'][:4]:n.text='2001'
    else:
        target=next(n for n in nodes(tree,'nonfraction') if n.get('name')=='us-gaap:TreasuryStockValue' and n.get('contextref')=='c-3')
        if mutation=='treasury':target.text='1'
        elif mutation=='nil':target.set('xmlns:xsi','http://www.w3.org/2001/XMLSchema-instance');target.set('xsi:nil','true')
        elif mutation=='hidden':next(target.iterancestors('td')).set('style','display:none')
        elif mutation=='unit':target.set('unitref','shares')
        elif mutation=='scale':target.set('scale','6')
        elif mutation=='extra_component':next(target.iterancestors('tr')).addnext(html.fromstring('<tr><td>Unmodeled equity</td><td>10</td></tr>'))
        else:
            node=deepcopy(target);node.text='1'
            tag={'minority':'MinorityInterest','parent_nil':'StockholdersEquity','aoci':'AccumulatedOtherComprehensiveIncomeLossNetOfTax'}[mutation]
            node.set('name','us-gaap:'+tag)
            if mutation=='parent_nil':node.set('xmlns:xsi','http://www.w3.org/2001/XMLSchema-instance');node.set('xsi:nil','true')
            next(target.iterancestors('table')).addnext(node)
    with pytest.raises(ValueError):equity.statement(html.tostring(tree,encoding='utf-8'),MANIFEST[name]['filing'],issuer=COMPANY['name'])

def test_full_reader_preserves_values_and_repeat():
    class Clock(datetime):
        @classmethod
        def now(cls,tz=None):return NOW
    client=Client()
    before=sec.project_fundamentals(RAW['facts'],RAW['company'],symbol='LGIH',cik=CIK,observed_at=NOW)
    with patch.object(sec,'datetime',Clock),patch.object(sec,'company_directory',lambda _: {'LGIH':{'cik':CIK}}),patch('app.clients.direct_sources.DirectSourceClient',lambda:client):
        after=sec.fetch_fundamentals('LGIH');calls=list(client.calls);client.calls.clear()
        assert sec.fetch_fundamentals('LGIH')==after and client.calls==calls
    assert all(after.get(k)==v for k,v in before.items() if k!='source_evidence_json')
    assert after['roe']==pytest.approx(3.3502051983584127)
    assert equity.cached_valid(json.loads(after['source_evidence_json']))
    assert len(calls)==5 and len(set(calls))==5

def test_reported_conflict_and_equity_not_overridden():
    payload=deepcopy(PAYLOAD)
    records=payload['facts']['us-gaap']['AdditionalPaidInCapital']['units']['USD']
    for p in records:
        if p.get('end')=='2026-09-30':p['val']+=1
    with pytest.raises(ValueError):equity.acquire(payload,COMPANY,cik=CIK,available_by=DAY,client=Client())
    payload=deepcopy(PAYLOAD)
    payload['facts']['us-gaap']['StockholdersEquity']={'units':{'USD':[dict(p) for p in payload['facts']['us-gaap'][equity.totals[0]]['units']['USD']]}}
    assert equity.plan(payload,COMPANY,cik=CIK,available_by=DAY) is None

def test_replays_retained_originals_and_rejects_tampered_proof():
    item=equity.acquire(PAYLOAD,COMPANY,cik=CIK,available_by=DAY,client=Client())
    supplement=dict(primary_equity=item,evidence=dict(primary_equity=deepcopy(item['evidence'])))
    records={'equity':{}}
    after=equity.apply(records,PAYLOAD,COMPANY,cik=CIK,available_by=DAY,supplement=supplement)
    assert len(after['equity'])==2 and records=={'equity':{}}
    item['evidence']['rows'][0]['value']+=1
    with pytest.raises(ValueError):equity.apply(records,PAYLOAD,COMPANY,cik=CIK,available_by=DAY,supplement=supplement)


@pytest.mark.parametrize('mutation',['identity','stale','missing_component','amended','foreign'])
def test_plan_keeps_unreviewed_scopes_unrequested(mutation):
    payload,company=deepcopy(PAYLOAD),deepcopy(COMPANY);day=DAY
    if mutation=='identity':
        with pytest.raises(ValueError):equity.plan(payload,company,cik='0000000001',available_by=day)
        return
    if mutation=='stale':day=date(2028,1,1)
    if mutation=='missing_component':del payload['facts']['us-gaap']['TreasuryStockValue']
    if mutation in ('amended','foreign'):
        recent=company['filings']['recent'];index=recent['reportDate'].index('2026-09-30')
        recent['form'][index]='10-Q/A' if mutation=='amended' else '20-F'
    assert equity.plan(payload,company,cik=CIK,available_by=day) is None


@pytest.mark.parametrize('key',['source_sha256','value','source_cik','filed','headers','column','rows','components'])
def test_cached_evidence_rejects_tampering(key):
    item=equity.acquire(PAYLOAD,COMPANY,cik=CIK,available_by=DAY,client=Client())
    supplement=dict(primary_equity=item,evidence=dict(primary_equity=deepcopy(item['evidence'])))
    records=equity.apply({'equity':{}},PAYLOAD,COMPANY,cik=CIK,available_by=DAY,supplement=supplement)
    evidence=dict(cik=CIK,period_end='2026-09-30',available_by=str(DAY),
        inline_filing_supplement=supplement['evidence'],current={'equity':list(records['equity'].values())})
    assert equity.cached_valid(evidence)
    changed=deepcopy(evidence);changed['inline_filing_supplement']['primary_equity']['rows'][0][key]=None
    assert not equity.cached_valid(changed)
    del evidence['inline_filing_supplement']['primary_equity']
    assert not equity.cached_valid(evidence)


def test_real_preparation_repeat_and_alert_rebaseline(monkeypatch):
    from datetime import timedelta
    from unittest.mock import patch
    from sqlalchemy import create_engine,select,func
    from sqlalchemy.orm import sessionmaker
    from app.db import Base
    from app.models import (FundamentalsCache,Event,EmailDelivery,MonitoringAlert,Security,
        UserAccount,Watchlist,WatchlistItem,WatchlistAlertRule)
    from app.services import sec_fundamentals_preparation as prep
    from app.services.custom_alert_rules import evaluate_watchlist_custom_alerts,validate_conditions
    sources,raw=MANIFEST,RAW;company=COMPANY;cik=CIK
    calls=[]
    class Client:
        issuer_document_urls=frozenset()
        def get(self,url):
            calls.append(url)
            return next(raw[k] for k,v in sources.items() if v['url']==url)
    class Clock(datetime):
        @classmethod
        def now(cls,tz=None):return NOW
    engine=create_engine('sqlite://');Base.metadata.create_all(engine);factory=sessionmaker(bind=engine)
    monkeypatch.setenv('SEC_FUNDAMENTALS_WARMING_ENABLED','1')
    with patch.object(prep,'SessionLocal',factory),patch.object(prep,'datetime',Clock),patch.object(prep,'DirectSourceClient',Client),patch.object(prep.sec_directory,'directory',return_value={'LGIH':{'cik':cik}}):
        payload=prep.prepare('LGIH');first=list(calls)
        assert payload['values']['roe']==pytest.approx(3.3502051983584127)
        assert prep.prepare('LGIH')==payload and calls==first
        assert set(first)=={sources[k]['url'] for k in ('facts','company','annual','prior','current')}
        with factory() as db:
            assert db.scalar(select(func.count()).select_from(FundamentalsCache))==0
            assert db.scalar(select(func.count()).select_from(Event))==0
    monkeypatch.setenv('FUNDAMENTALS_PROVIDER','sec_edgar')
    with factory() as db:
        user=UserAccount(email='zero-proof@example.test');security=Security(symbol='LGIH',name='Zero proof',asset_class='equity')
        db.add_all([user,security]);db.flush();watch=Watchlist(name='Proof',owner_user_id=user.id);db.add(watch);db.flush()
        db.add(WatchlistItem(watchlist_id=watch.id,security_id=security.id,target_type='ticker'))
        # ROE is the supported
        # custom-alert input recovered from the complete equity statements.
        metric='roe';threshold=payload['values'][metric]-1
        rule=WatchlistAlertRule(user_id=user.id,watchlist_id=watch.id,name='Margin',enabled=True,scope_type='any_watchlist_ticker',match_type='all',delivery='both',
            conditions_json=json.dumps(validate_conditions([dict(metric=metric,operator='gt',comparison_type='value',comparison_value=threshold)])))
        row=FundamentalsCache(symbol='LGIH',provider='sec_edgar',status='ok',fetched_at=NOW,
            source_evidence_json=json.dumps({'financial_status':'unavailable','calculations':{}}))
        db.add_all([rule,row]);db.commit()
        def run(minutes):
            result=evaluate_watchlist_custom_alerts(db,user_id=user.id,watchlist_id=watch.id,now=NOW+timedelta(minutes=minutes));db.commit();return result['triggered']
        assert run(0)==0
        for k,v in payload['values'].items():setattr(row,k,v)
        row.source_evidence_json=json.dumps(payload['calculation_evidence'])
        assert run(5)==run(10)==0
        setattr(row,metric,threshold-1);assert run(15)==0
        setattr(row,metric,payload['values'][metric]);assert run(20)==1 and run(25)==0
        assert db.scalar(select(func.count()).select_from(MonitoringAlert))==1
        assert db.scalar(select(func.count()).select_from(EmailDelivery))==0
        assert db.scalar(select(func.count()).select_from(Event))==0
    engine.dispose()
