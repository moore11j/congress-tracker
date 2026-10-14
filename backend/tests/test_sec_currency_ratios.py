from copy import deepcopy
from datetime import datetime, timezone
import json
from pathlib import Path

import pytest
from app.services import sec_currency_ratios as subject, sec_fundamentals as sec

ROOT = Path(__file__).parent/'fixtures/sec_enb'
META = json.loads((ROOT/'observations.json').read_text(encoding='utf-8'))
NOW = datetime(2026,10,14,22,20,tzinfo=timezone.utc)
FIELDS = {'operating_margin','net_margin','revenue_growth','operating_margin_expansion',
          'current_ratio','roe','fcf_margin','fcf_growth'}


def project(facts=None, company=None, now=NOW, symbol='ENB', cik=subject.CIK, supplement=None):
    return sec.project_fundamentals(json.dumps(facts or META['facts']).encode(),
        json.dumps(company or META['company']).encode(), symbol=symbol,cik=cik,
        observed_at=now,filing_supplement=supplement)


def test_actual_cad_source_arithmetic_and_all_retained_inputs_match_originals():
    result=project();assert result==project()
    e=json.loads(result['source_evidence_json'])
    assert set(result)&FIELDS == FIELDS
    assert e['reporting_currency']=='CAD'
    assert e['current']['revenue']['value']==(65194+51675-33378)*1e6
    assert e['prior']['revenue']['value']==(53473+33378-22374)*1e6
    assert result['operating_margin']==pytest.approx(11132/83491*100)
    assert result['net_margin']==pytest.approx(6127/83491*100)
    assert result['roe']==pytest.approx(6127/((65448+65349)/2)*100)
    assert result['revenue_growth']==pytest.approx((83491/64477-1)*100)
    assert result['operating_margin_expansion']==pytest.approx((11132/83491-10626/64477)*100)
    assert result['current_ratio']==pytest.approx(17720/24577)
    assert result['fcf_margin']==pytest.approx(1668/83491*100)
    assert result['fcf_growth']==pytest.approx((1668/5125-1)*100)
    assert e['native_cash_flow']['current']==1668000000
    assert e['native_cash_flow']['canonical_usd_amount_available'] is False
    assert e['leverage_preparation']['candidates']==[]
    assert e['leverage_preparation']['ebitda']['unit']=='CAD'
    assert not set(result)&{'free_cash_flow','eps_ttm','net_debt_to_ebitda','ev_to_ebitda'}
    originals=META['original_inline']
    for section in ['current','prior','instant']:
        for point in e[section].values():
            if not point:continue
            for fact in point.get('inputs',[point]):
                assert fact['unit']=='CAD'
                matches=[row for doc in originals.values()
                    for row in doc['facts'].get(fact['tag'],{}).get('units',{}).get('CAD',[])
                    if row.get('start')==fact['start'] and row['end']==fact['end']
                    and row['accn']==fact['accession'] and row['filed']==fact['filed']]
                assert matches and {x['val'] for x in matches}=={fact['value']}


@pytest.mark.parametrize('change',['usd_only','missing_total','conflict','incomplete_new_period','stale','wrong_cik','wrong_ticker','naive'])
def test_unsupported_sources_hold(change):
    f=deepcopy(META['facts']);c=deepcopy(META['company']);now=NOW;cik=subject.CIK
    rows=f['facts']['us-gaap']['Revenues']['units']['CAD']
    if change=='usd_only':f['facts']['us-gaap']['Revenues']['units']={'USD':rows}
    elif change=='missing_total':f['facts']['us-gaap'].pop('Revenues')
    elif change=='conflict':
        row=next(r for r in rows if r.get('start')=='2026-01-01' and r['end']=='2026-06-30')
        rows.append({**row,'val':row['val']+1,'filed':'2026-10-13'})
        rows.append({**row,'filed':'2026-10-13'})
    elif change=='incomplete_new_period':rows.append(dict(start='2026-07-01',end='2026-09-30',val=100,filed='2026-10-13',accn='0000895728-26-999999',form='10-Q'))
    elif change=='stale':now=NOW.replace(year=2027)
    elif change=='wrong_cik':f['cik']=c['cik']=1;cik='1'
    elif change=='wrong_ticker':c['tickers']=['WRONG']
    elif change=='naive':now=NOW.replace(tzinfo=None)
    with pytest.raises(ValueError):project(f,c,now=now,cik=cik)


def test_wrong_unit_or_missing_optional_never_becomes_zero_or_cross_currency():
    f=deepcopy(META['facts']);tags=f['facts']['us-gaap'];original=project()
    tags['OperatingIncomeLoss']['units']['USD']=tags['OperatingIncomeLoss']['units'].pop('CAD')
    result=project(f)
    assert 'operating_margin' not in result and 'operating_margin_expansion' not in result
    assert result['revenue_growth']==original['revenue_growth']
    tags['NetCashProvidedByUsedInOperatingActivities']['units']['CAD']=[]
    result=project(f)
    assert 'fcf_margin' not in result and 'fcf_growth' not in result
    assert json.loads(result['source_evidence_json'])['native_cash_flow']['current'] is None


def test_completed_filing_day_and_conflicting_restated_parent_income():
    f=deepcopy(META['facts']);rows=f['facts']['us-gaap']['NetIncomeLoss']['units']['CAD']
    row=next(r for r in rows if r.get('start')=='2026-01-01' and r['end']=='2026-06-30')
    rows.append({**row,'val':row['val']+1000000,'filed':'2026-10-14','form':'10-Q/A'})
    assert project(f)['net_margin']==project()['net_margin']
    assert project(f,now=NOW.replace(day=15,hour=2))['net_margin']==project()['net_margin']
    assert project(f,now=NOW.replace(day=15,hour=5))['net_margin']!=project()['net_margin']
    rows.append({**row,'filed':'2026-10-14','form':'10-Q/A'})
    assert 'net_margin' not in project(f,now=NOW.replace(day=15,hour=5))


def test_unreviewed_other_issuer_does_not_acquire_cad_policy():
    f=deepcopy(META['facts']);c=deepcopy(META['company']);f['cik']=c['cik']=1;c['tickers']=['ABC']
    with pytest.raises(sec.SecFundamentalsCoverageError):project(f,c,symbol='ABC',cik='1')
    assert subject.basis('ABC','1') is None


def test_usd_market_inputs_do_not_consume_cad_money(monkeypatch):
    from app.clients.direct_sources import DirectSourceClient
    from app.clients.massive_stocks import MassiveStocksClient
    from app.services import sec_inline_financials as inline
    class Clock(datetime):
        @classmethod
        def now(cls,tz=None):return NOW
    monkeypatch.setattr(sec,'datetime',Clock)
    monkeypatch.setattr(sec,'company_directory',lambda day:{'ENB':{'cik':subject.CIK}})
    monkeypatch.setattr(DirectSourceClient,'get',lambda self,url:json.dumps(META['facts'] if 'companyfacts' in url else META['company']).encode())
    monkeypatch.setattr(inline,'acquire',lambda *a,**k:None)
    monkeypatch.setenv('STOCK_PRICE_PROVIDER','massive');monkeypatch.setenv('MASSIVE_API_KEY','test-only')
    monkeypatch.setattr(MassiveStocksClient,'details',lambda *a:{'cik':subject.CIK,'market_cap':10000000000})
    monkeypatch.setattr(MassiveStocksClient,'bars',lambda *a:[])
    result=sec.fetch_fundamentals('ENB')
    assert result['market_cap']==10000000000
    assert set(result)&FIELDS==FIELDS
    assert not set(result)&{'free_cash_flow','price_to_sales','trailing_pe','earnings_yield','fcf_yield','eps_ttm'}
    assert json.loads(result['source_evidence_json'])['market_income']['reason']=='common_income_currency_not_usd'


def test_inline_acquisition_uses_cad_and_selective_version(monkeypatch):
    from app.services import sec_inline_financials as inline,sec_fundamentals_preparation as prep
    f=deepcopy(META['facts']);c=META['company']
    for data in f['facts']['us-gaap'].values():
        for unit in data['units']:
            data['units'][unit]=[r for r in data['units'][unit] if r['end']<'2026-06-30']
    calls=[]
    class Client:
        def get(self,url):calls.append(url);return b'original'
    def parse(raw,**kw):
        assert raw==b'original' and kw['basis']==subject.basis('ENB',subject.CIK)
        assert kw['report_date']=='2026-06-30' and kw['cik']==subject.CIK
        return {'verified_arguments':True}
    monkeypatch.setattr(inline,'parse',parse)
    result=inline.acquire(json.dumps(f).encode(),json.dumps(c).encode(),symbol='ENB',cik=subject.CIK,observed_at=NOW,client=Client())
    assert result=={'verified_arguments':True} and calls==[subject.REVIEW['source_url']]
    assert prep.version_for_symbol('ENB')==prep.VERSION+'_'+subject.VERSION
    assert prep.version_for_symbol('AAPL')==prep.VERSION
