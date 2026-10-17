"""Native statement acquisition, calculations, panels and consumer contracts."""
from copy import deepcopy
from datetime import datetime,timezone,date,timedelta
from pathlib import Path
from types import SimpleNamespace
import gzip,hashlib,json,socket
import pytest
from app.services import sec_statement_currency as native,sec_inline_financials as inline
from app.services import sec_fundamentals as sec,sec_financial_statements as statements
from test_sec_statement_currency import acquisition_inputs

NOW=datetime(2026,10,17,5,tzinfo=timezone.utc)
ROOT=Path(__file__).parent/'fixtures/sec_native_financials'


def originals(symbol):
    args,company,urls=acquisition_inputs(symbol)
    if symbol=='NAMI':
        from test_sec_document_set_cohort import originals as cohort
        facts=cohort(symbol)[0]
    else:
        manifest=json.loads((ROOT/'sources.json').read_text());name=symbol+'-facts.json'
        facts=gzip.decompress((ROOT/(name+'.gz')).read_bytes())
        assert hashlib.sha256(facts).hexdigest()==manifest[name]['sha256']
    cik=args['cik'];urls=dict(urls)
    urls[f'https://data.sec.gov/submissions/CIK{cik}.json']=company
    urls[f'https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json']=facts
    return facts,company,cik,urls


class Client:
    issuer_document_urls=frozenset()
    def __init__(self,urls):self.urls=urls;self.calls=[]
    def get(self,url):self.calls.append(url);return self.urls[url]


@pytest.mark.parametrize('symbol,currency',[('ADSE','EUR'),('ALC','USD'),('NAMI','CNY')])
def test_existing_acquisition_calculation_and_panel_contracts(symbol,currency,monkeypatch):
    monkeypatch.setattr(socket.socket,'connect',lambda *a,**k:pytest.fail('Unexpected network'))
    facts,company,cik,urls=originals(symbol);client=Client(urls)
    supplement=inline.acquire(facts,company,symbol=symbol,cik=cik,observed_at=NOW,client=client)
    assert len(client.calls)==len(set(client.calls))==2
    assert supplement['evidence']['native_reporting']['currency']==currency
    values=sec.project_fundamentals(facts,company,symbol=symbol,cik=cik,observed_at=NOW,filing_supplement=supplement)
    assert values==sec.project_fundamentals(facts,company,symbol=symbol,cik=cik,observed_at=NOW,filing_supplement=supplement)
    assert values['period_date']==date(2026,6,30) and values['current_ratio']>0
    if currency!='USD':assert 'free_cash_flow' not in values
    assert 'eps_growth' not in values and 'net_debt_to_ebitda' not in values
    panel=statements.project(facts,company,symbol=symbol,cik=cik,observed_at=NOW,filing_supplement=supplement)
    assert panel==statements.project(facts,company,symbol=symbol,cik=cik,observed_at=NOW,filing_supplement=supplement)
    assert panel['reportingCurrency']==currency and panel['status']=='partial'
    assert panel['annual']
    if symbol in {'ADSE','NAMI'}:
        assert not panel['quarterly'] and panel['semiannual'][-1]['date']=='2026-06-30'
        assert all('Half-year' in r['period'] and 160 <= (date.fromisoformat(r['date'])-date.fromisoformat(r['periodStart'])).days+1 <= 200 for r in panel['semiannual'])
    else:assert panel['quarterly'] and not panel['semiannual']
    assert all(row['currency']==currency and row['eps'] is None for row in panel['annual']+panel['quarterly'])
    row=SimpleNamespace(source='sec_edgar',fetched_at=NOW,payload_json=json.dumps(panel))
    assert statements.cached_row_payload(row,symbol,now=NOW)==panel
    bad=deepcopy(panel);bad['reportingCurrency']='OTHER';row.payload_json=json.dumps(bad)
    assert statements.cached_row_payload(row,symbol,now=NOW) is None


EXPECTED = {
 'ADSE': {'current_ratio':59837000/63909000},
 'ALC': {'current_ratio':6624000000/3167000000,'gross_margin':6174000000/10861000000*100,
         'net_margin':643000000/10861000000*100,'operating_margin':948000000/10861000000*100,
         'free_cash_flow':1740000000.0,'revenue_growth':8.339152119700756},
 'NAMI': {'current_ratio':121999000/81453000,'gross_margin':12.23742632357905,
          'net_margin':-23.883133256591567,'operating_margin':-27.363920407827656,
          'revenue_growth':-16.86904759049581},
}


@pytest.mark.parametrize('symbol',list(EXPECTED))
@pytest.mark.parametrize('existing_hold',[False,True])
def test_source_calculation_cache_preparation_and_public_repeat(symbol,existing_hold,monkeypatch):
    from unittest.mock import patch
    from sqlalchemy import create_engine,select,func
    from sqlalchemy.orm import sessionmaker
    from app.models import InsightsSnapshot
    from app.services import sec_fundamentals_preparation as prep,sec_directory
    from app.request_priority import set_request_context,reset_request_context
    facts,company,cik,urls=originals(symbol);client=Client(urls)
    monkeypatch.setattr(socket.socket,'connect',lambda *a,**kw:pytest.fail('Unexpected network'))
    class Clock(datetime):
        @classmethod
        def now(cls,tz=None):return NOW
    engine=create_engine('sqlite://');InsightsSnapshot.__table__.create(engine);factory=sessionmaker(bind=engine)
    monkeypatch.setenv('SEC_FUNDAMENTALS_WARMING_ENABLED','1')
    monkeypatch.setattr(sec_directory,'directory',lambda:{symbol:{'cik':cik}})
    if existing_hold:
        from app.services.ticker_financials import _unavailable
        old=NOW-timedelta(hours=25)
        core={'version':prep.version_for_symbol(symbol),'symbol':symbol,'source':'sec_edgar',
              'observed_at':old.isoformat(),'status':'unavailable','values':{},
              'reason':'unreviewed_foreign_statement_currency'}
        history=_unavailable(symbol,message='Reporting currency requires verification.',reason=core['reason'])
        history.update(source='sec_edgar',updatedAt=old.isoformat())
        with factory() as db:
            for key,payload in [(f'sec-current-fundamentals:{symbol}:v1',core),(f'sec-financials:{symbol}:v1',history)]:
                db.add(InsightsSnapshot(kind=key,source='sec_edgar',fetched_at=old,payload_json=json.dumps(payload)))
            db.commit()
        # Existing truthful holds remain usable until their ordinary TTL. No
        # whole-universe invalidation is needed to admit newly supported sources.
        with patch.object(prep,'SessionLocal',factory):
            assert prep._cached(symbol,old+timedelta(hours=23))==core
            assert prep._cached(symbol,NOW) is None
        with factory() as db:
            assert statements.cached_payload(db,symbol,now=old+timedelta(hours=23))==history
            assert statements.cached_payload(db,symbol,now=NOW) is None
    with patch.object(prep,'SessionLocal',factory),patch.object(prep,'datetime',Clock),patch.object(prep,'DirectSourceClient',lambda:client):
        prepared=prep.prepare(symbol)
        for key,value in EXPECTED[symbol].items():assert prepared['values'][key]==pytest.approx(value)
        count=len(client.calls);assert prep.prepare(symbol)==prepared and len(client.calls)==count
        assert prepared['calculation_evidence']['reporting_currency']==('EUR' if symbol=='ADSE' else 'CNY' if symbol=='NAMI' else 'USD')
        if symbol=='NAMI':assert 'free_cash_flow' not in prepared['values'] and 'fcf_margin' not in prepared['values']
    with patch.object(statements,'SessionLocal',factory),patch.object(statements,'datetime',Clock),patch.object(statements,'DirectSourceClient',lambda:client):
        token=set_request_context({})
        try:
            panel=statements.prepared(symbol)
            count=len(client.calls);assert statements.prepared(symbol)==panel and len(client.calls)==count
        finally:reset_request_context(token)
        token=set_request_context({'path':'/api/tickers/'+symbol+'/financials'})
        try:assert statements.prepared(symbol)==panel and len(client.calls)==count
        finally:reset_request_context(token)
    with factory() as db:assert db.scalar(select(func.count()).select_from(InsightsSnapshot))==2
    engine.dispose()


@pytest.mark.parametrize('symbol,metric',[('ADSE','current_ratio'),('ALC','current_ratio'),('ALC','revenue_growth'),('NAMI','current_ratio')])
def test_score_watchlist_recovery_repeat_crossing_and_rollback(symbol,metric,monkeypatch):
    import test_sec_tagged_foreign_consumers as consumer
    facts,company,cik,urls=originals(symbol)
    monkeypatch.setattr(consumer,'originals',lambda requested:(facts,company,cik,urls))
    monkeypatch.setattr(consumer,'EXPECTED',{symbol:{'metrics':EXPECTED[symbol]}})
    monkeypatch.setattr(consumer,'NOW',NOW)
    consumer.test_original_source_score_and_alert_recovery_repeat_crossing_rollback(symbol,metric,monkeypatch)


@pytest.mark.parametrize('defect',['currency','accession','cik','filed','directory','exhibit','declaration','policy'])
def test_tampered_native_proofs_never_reach_calculation(defect):
    facts,company,cik,urls=originals('NAMI');p=inline.acquire(facts,company,symbol='NAMI',cik=cik,observed_at=NOW,client=Client(urls))
    proof=p['evidence']
    if defect in {'currency','accession','cik','filed'}:proof[defect]='invalid'
    elif defect=='directory':proof['document_set_discovery']['accession']='0000000001-26-000001'
    elif defect=='exhibit':proof['cover_evidence']['exhibit_sha256']='0'*64
    elif defect=='declaration':proof['statement_currency_declaration']['currency']='USD'
    else:proof['native_reporting']['currency_policy']='old'
    with pytest.raises((native.StatementCurrencyError,inline.InlineFactsError)):
        sec.project_fundamentals(facts,company,symbol='NAMI',cik=cik,observed_at=NOW,filing_supplement=p)


@pytest.mark.parametrize('symbol',['ADSE','ALC','NAMI'])
def test_foreign_forecast_overlay_stays_withheld_before_any_estimate_read(symbol,monkeypatch):
    from app.services import finnhub_paid_readers as paid
    facts,company,cik,urls=originals(symbol);p=inline.acquire(facts,company,symbol=symbol,cik=cik,observed_at=NOW,client=Client(urls))
    panel=statements.project(facts,company,symbol=symbol,cik=cik,observed_at=NOW,filing_supplement=p)
    monkeypatch.setattr(paid,'enabled',lambda:True)
    monkeypatch.setattr(paid,'forecast',lambda *a,**kw:pytest.fail('Unverified foreign forecast read'))
    assert paid.attach_financials(None,symbol,panel,now=NOW) is panel


def test_native_facts_never_use_convenience_usd_amounts_or_price_inputs():
    facts,company,cik,urls=originals('NAMI');p=inline.acquire(facts,company,symbol='NAMI',cik=cik,observed_at=NOW,client=Client(urls))
    baseline=sec.project_fundamentals(facts,company,symbol='NAMI',cik=cik,observed_at=NOW,filing_supplement=p)
    payload=json.loads(facts)
    for tag in payload['facts']['us-gaap'].values():
        for row in tag.get('units',{}).get('USD',[]):row['val']=999999999999999
    modified=sec.project_fundamentals(json.dumps(payload).encode(),company,symbol='NAMI',cik=cik,observed_at=NOW,filing_supplement=p)
    for metric in EXPECTED['NAMI']:assert modified[metric]==baseline[metric]
    assert 'free_cash_flow' not in modified and 'net_debt_to_ebitda' not in modified and 'eps_growth' not in modified


@pytest.mark.parametrize('symbol',['ADSE','ALC','NAMI'])
def test_public_route_normalization_preserves_original_periods_and_currency(symbol):
    # Execute the production normalization functions without importing the
    # unrelated application startup and provider clients.
    import ast
    from typing import Any
    source=Path(__file__).parents[1]/'app/main.py'
    names={'_public_ticker_payload','_normalized_section_status',
           '_financial_sections_present','_normalize_ticker_financials_payload'}
    tree=ast.parse(source.read_text(encoding='utf-8'))
    functions=[node for node in tree.body if isinstance(node,ast.FunctionDef) and node.name in names]
    assert len(functions)==len(names)
    scope={'Any':Any,'_iso_utc_now':lambda:NOW.isoformat()}
    exec(compile(ast.Module(body=functions,type_ignores=[]),str(source),'exec'),scope)
    facts,company,cik,urls=originals(symbol)
    filing=inline.acquire(facts,company,symbol=symbol,cik=cik,observed_at=NOW,client=Client(urls))
    panel=statements.project(facts,company,symbol=symbol,cik=cik,observed_at=NOW,filing_supplement=filing)
    response=scope['_normalize_ticker_financials_payload'](deepcopy(panel))
    assert response['status']=='partial' and 'income' in response['sections_present']
    for period in ['annual','quarterly','semiannual']:
        assert response[period]==panel[period]
        assert response['sections']['income'][period]==panel[period]
    assert response['reportingCurrency']==panel['reportingCurrency']
    assert response['sourceEvidence']==panel['sourceEvidence']
