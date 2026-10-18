from copy import deepcopy
from datetime import date,datetime,timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
import gzip,hashlib,json,socket
import pytest
from test_sec_tabular_financials import source
from app.services import sec_tabular_reporting as tables,sec_inline_financials as inline
from app.services import sec_fundamentals as core,sec_financial_statements as statements

NOW=datetime(2026,10,17,5,tzinfo=timezone.utc)
ROOT=Path(__file__).parent/'fixtures'


def originals(symbol):
    raw,args=source(symbol);urls={}
    for item in json.loads((ROOT/'sec_tabular_financials/sources.json').read_text()).values():
        for entry in item['files'].values():
            body=gzip.decompress((ROOT/'sec_tabular_financials'/entry['file']).read_bytes())
            assert hashlib.sha256(body).hexdigest()==entry['sha256'];urls[entry['url']]=body
    for url,entry in json.loads((ROOT/'sec_tabular_reporting/sources.json').read_text()).items():
        body=gzip.decompress((ROOT/'sec_tabular_reporting'/entry['file']).read_bytes())
        assert len(body)==entry['bytes'] and hashlib.sha256(body).hexdigest()==entry['sha256'];urls[url]=body
    facts=urls[f'https://data.sec.gov/api/xbrl/companyfacts/CIK{args["cik"]}.json']
    return raw,args,facts,urls


class Client:
    issuer_document_urls=frozenset()
    def __init__(self,urls):self.urls=urls;self.calls=[]
    def get(self,url):self.calls.append(url);return self.urls[url]


@pytest.mark.parametrize('symbol,currency,count',[('NTB','USD',10),('WB','USD',18),('YQ','CNY',22)])
def test_complete_original_normalization_and_bounded_acquisition(symbol,currency,count):
    raw,args,facts,urls=originals(symbol)
    with patch.object(socket.socket,'connect',side_effect=AssertionError('No network')):
        result=tables.normalize(raw,**args)
        assert result==tables.normalize(raw,**args)
        assert result['evidence']['currency']==currency and result['evidence']['native_observation_count']==count
        client=Client(urls)
        supplement=inline.acquire(facts,args['company_raw'],symbol=symbol,cik=args['cik'],observed_at=NOW,client=client)
        assert supplement['evidence']['table_reporting']['currency']==currency
        assert len(client.calls)==len(set(client.calls))<=6
        checked=tables.validate(supplement,company_raw=args['company_raw'],symbol=symbol,cik=args['cik'],available_by=args['available_by'])
        assert checked==result
    assert not result['records']['equity'] and not result['records']['net_income']
    for points in result['records'].values():
        for point in points.values():
            assert point['unit']==currency and point['source_kind']=='html_statement' and point['tag'].startswith('html:')


@pytest.mark.parametrize('symbol',['CGNT','NIVF','YI','ZEPP'])
def test_missing_explicit_accounting_basis_is_held(symbol):
    raw,args=source(symbol)
    with pytest.raises(tables.TabularReportingError,match='table_accounting_basis_unproved'):
        tables.normalize(raw,**args)


@pytest.mark.parametrize('symbol',['WB','YQ'])
def test_existing_core_and_public_panel_contracts_preserve_native_values(symbol):
    raw,args,facts,urls=originals(symbol)
    supplement=inline.acquire(facts,args['company_raw'],symbol=symbol,cik=args['cik'],observed_at=NOW,client=Client(urls))
    panel=statements.project(facts,args['company_raw'],symbol=symbol,cik=args['cik'],observed_at=NOW,filing_supplement=supplement)
    assert panel['status']=='partial' and panel['preparationVersion']==tables.VERSION
    assert not panel['annual'] and not panel['semiannual']
    assert len(panel['quarterly'])==2
    assert panel['quarterly'][-1]['revenue']==(453826000 if symbol=='WB' else 90092000)
    assert all(row['eps'] is None and row['netIncome'] is None and row['freeCashFlow'] is None for row in panel['quarterly'])
    assert panel['summary']['revenueTtm'] is None and panel['summary']['netIncomeTtm'] is None
    row=SimpleNamespace(source='sec_edgar',fetched_at=NOW,payload_json=json.dumps(panel))
    assert statements.cached_row_payload(row,symbol,now=NOW)==panel
    if symbol=='YQ':
        values=core.project_fundamentals(facts,args['company_raw'],symbol=symbol,cik=args['cik'],observed_at=NOW,filing_supplement=supplement)
        assert values['current_ratio']==559829000/316129000
        assert panel['summary']['currentRatio']==values['current_ratio']
        assert 'free_cash_flow' not in values and 'roe' not in values and 'net_margin' not in values
    else:
        with pytest.raises(core.SecFundamentalsCoverageError):
            core.project_fundamentals(facts,args['company_raw'],symbol=symbol,cik=args['cik'],observed_at=NOW,filing_supplement=supplement)


@pytest.mark.parametrize('replacement',[b'not prepared in accordance',b'never prepared in accordance',b'prepared inconsistently with'])
def test_negated_accounting_declaration_is_never_admitted(replacement):
    raw,args=source('WB');assert b'prepared in accordance' in raw
    with pytest.raises(tables.TabularReportingError,match='accounting_basis_unproved'):
        tables.normalize(raw.replace(b'prepared in accordance',replacement),**args)


def test_no_parent_income_or_equity_is_inferred_from_consolidated_totals():
    raw,args=source('NTB');result=tables.normalize(raw,**args)
    assert result['records']['consolidated_income'] and result['records']['consolidated_equity']
    assert not result['records']['net_income'] and not result['records']['equity']
    assert not result['records']['current_assets'] and not result['records']['current_liabilities']


def test_yq_convenience_usd_does_not_reach_native_records():
    raw,args=source('YQ');result=tables.normalize(raw,**args)
    assert result['evidence']['source_observation_count']==33 and result['evidence']['native_observation_count']==22
    assert all(p['unit']=='CNY' for rows in result['records'].values() for p in rows.values())
    assert b'82,509' in raw
    changed=tables.normalize(raw.replace(b'82,509',b'92,509'),**args)
    assert changed['records']['current_assets'][None,date(2026,6,30)]['value']==559829000


@pytest.mark.parametrize('defect',['currency','original','cik','company','period'])
def test_consumer_recomputes_complete_original_evidence(defect):
    raw,args,facts,urls=originals('YQ');client=Client(urls)
    supplement=inline.acquire(facts,args['company_raw'],symbol='YQ',cik=args['cik'],observed_at=NOW,client=client)
    if defect=='currency':supplement['evidence']['table_reporting']['currency']='USD'
    elif defect=='original':supplement['table_original']=supplement['table_original'].replace(b'559,829',b'659,829')
    elif defect=='period':supplement['evidence']['table_reporting']['current_period']='2026-09-30'
    elif defect=='company':args['company_raw']=args['company_raw']+b' '
    else:args['cik']='1'
    with pytest.raises(tables.TabularReportingError):
        tables.validate(supplement,company_raw=args['company_raw'],symbol='YQ',cik=args['cik'],available_by=args['available_by'])


@pytest.mark.parametrize('field,value',[('reportingCurrency','USD'),('preparationVersion','old')])
def test_public_cache_rejects_wrong_currency_or_version(field,value):
    raw,args,facts,urls=originals('YQ');supplement=inline.acquire(facts,args['company_raw'],symbol='YQ',cik=args['cik'],observed_at=NOW,client=Client(urls))
    panel=statements.project(facts,args['company_raw'],symbol='YQ',cik=args['cik'],observed_at=NOW,filing_supplement=supplement)
    panel[field]=value;row=SimpleNamespace(source='sec_edgar',fetched_at=NOW,payload_json=json.dumps(panel))
    assert statements.cached_row_payload(row,'YQ',now=NOW) is None


def test_yq_preparation_cache_and_public_repeat_do_not_refetch(monkeypatch):
    from sqlalchemy import create_engine,select,func
    from sqlalchemy.orm import sessionmaker
    from app.models import InsightsSnapshot
    from app.services import sec_fundamentals_preparation as prep,sec_directory
    raw,args,facts,urls=originals('YQ');client=Client(urls)
    monkeypatch.setattr(socket.socket,'connect',lambda *a,**kw:pytest.fail('Unexpected network'))
    monkeypatch.setenv('SEC_FUNDAMENTALS_WARMING_ENABLED','1')
    monkeypatch.setattr(sec_directory,'directory',lambda:{'YQ':{'cik':args['cik']}})
    class Clock(datetime):
        @classmethod
        def now(cls,tz=None):return NOW
    engine=create_engine('sqlite://');InsightsSnapshot.__table__.create(engine);factory=sessionmaker(bind=engine)
    with patch.object(prep,'SessionLocal',factory),patch.object(prep,'datetime',Clock),patch.object(prep,'DirectSourceClient',lambda:client):
        payload=prep.prepare('YQ');assert payload['status']=='partial'
        assert payload['values']=={'current_ratio':559829000/316129000}
        count=len(client.calls);assert prep.prepare('YQ')==payload and len(client.calls)==count
    with patch.object(statements,'SessionLocal',factory),patch.object(statements,'datetime',Clock),patch.object(statements,'DirectSourceClient',lambda:client):
        payload=statements.prepared('YQ');assert payload['status']=='partial'
        count=len(client.calls);assert statements.prepared('YQ')==payload and len(client.calls)==count
    with factory() as db:assert db.scalar(select(func.count()).select_from(InsightsSnapshot))==2
    engine.dispose()



def test_source_score_watchlist_recovery_repeat_crossing_and_rollback(monkeypatch):
    import test_sec_tagged_foreign_consumers as consumer
    raw,args,facts,urls=originals('YQ')
    monkeypatch.setattr(consumer,'originals',lambda requested:(facts,args['company_raw'],args['cik'],urls))
    monkeypatch.setattr(consumer,'EXPECTED',{'YQ':{'metrics':{'current_ratio':559829000/316129000}}})
    monkeypatch.setattr(consumer,'NOW',NOW)
    consumer.test_original_source_score_and_alert_recovery_repeat_crossing_rollback('YQ','current_ratio',monkeypatch)


@pytest.mark.parametrize('symbol',['WB','YQ'])
def test_forecast_overlay_is_withheld_for_unverified_listed_share_basis(symbol,monkeypatch):
    from app.services import finnhub_paid_readers as paid
    raw,args,facts,urls=originals(symbol);supplement=inline.acquire(facts,args['company_raw'],symbol=symbol,cik=args['cik'],observed_at=NOW,client=Client(urls))
    panel=statements.project(facts,args['company_raw'],symbol=symbol,cik=args['cik'],observed_at=NOW,filing_supplement=supplement)
    monkeypatch.setattr(paid,'enabled',lambda:True)
    monkeypatch.setattr(paid,'forecast',lambda *a,**kw:pytest.fail('Unqualified forecasts read'))
    assert paid.attach_financials(None,symbol,panel,now=NOW) is panel


def test_stripped_table_proof_is_rejected_by_both_cache_readers():
    from app.services.sec_tagged_foreign import cached_currency_valid
    assert not cached_currency_valid({'methodology':tables.VERSION,'reporting_currency':'CNY'})
    raw,args,facts,urls=originals('YQ');supplement=inline.acquire(facts,args['company_raw'],symbol='YQ',cik=args['cik'],observed_at=NOW,client=Client(urls))
    panel=statements.project(facts,args['company_raw'],symbol='YQ',cik=args['cik'],observed_at=NOW,filing_supplement=supplement)
    del panel['sourceEvidence']['tableReporting']
    row=SimpleNamespace(source='sec_edgar',fetched_at=NOW,payload_json=json.dumps(panel))
    assert statements.cached_row_payload(row,'YQ',now=NOW) is None


def test_cached_table_proof_cannot_be_reassigned_to_another_company():
    from app.services.sec_tagged_foreign import cached_currency_valid
    raw,args,facts,urls=originals('YQ')
    supplement=inline.acquire(facts,args['company_raw'],symbol='YQ',cik=args['cik'],observed_at=NOW,client=Client(urls))
    panel=statements.project(facts,args['company_raw'],symbol='YQ',cik=args['cik'],observed_at=NOW,filing_supplement=supplement)
    panel['symbol']='WB'
    row=SimpleNamespace(source='sec_edgar',fetched_at=NOW,payload_json=json.dumps(panel))
    assert statements.cached_row_payload(row,'WB',now=NOW) is None
    values=core.project_fundamentals(facts,args['company_raw'],symbol='YQ',cik=args['cik'],observed_at=NOW,filing_supplement=supplement)
    proof=json.loads(values['source_evidence_json'])
    assert cached_currency_valid(proof)
    proof['cik']='0000000001'
    assert not cached_currency_valid(proof)
    del proof['cik']
    assert not cached_currency_valid(proof)


def test_unqualified_table_hold_is_truthful_and_cached(monkeypatch):
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    from app.models import InsightsSnapshot
    from app.services import sec_directory
    raw,args,facts,urls=originals('YQ');client=Client(urls)
    monkeypatch.setattr(socket.socket,'connect',lambda *a,**kw:pytest.fail('Unexpected network'))
    monkeypatch.setattr(sec_directory,'directory',lambda:{'YQ':{'cik':args['cik']}})
    def held(*a,**kw):raise tables.TabularReportingError('table_accounting_basis_unproved')
    monkeypatch.setattr(inline,'acquire',held)
    class Clock(datetime):
        @classmethod
        def now(cls,tz=None):return NOW
    engine=create_engine('sqlite://');InsightsSnapshot.__table__.create(engine);factory=sessionmaker(bind=engine)
    with patch.object(statements,'SessionLocal',factory),patch.object(statements,'datetime',Clock),patch.object(statements,'DirectSourceClient',lambda:client):
        payload=statements.prepared('YQ')
        assert payload['status']=='unavailable' and 'accounting basis' in payload['message']
        assert payload['subsections']['income']['reason_code']=='unqualified_html_statement'
        count=len(client.calls);assert statements.prepared('YQ')==payload and len(client.calls)==count
    engine.dispose()
