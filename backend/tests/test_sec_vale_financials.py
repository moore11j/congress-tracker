from copy import deepcopy
from datetime import datetime, timezone
import json
from pathlib import Path
import pytest
from app.services import sec_vale_financials as vale, sec_inline_financials as inline, sec_fundamentals as sec

ROOT = Path(__file__).parent/'fixtures/sec_vale'
META = json.loads((ROOT/'observations.json').read_text(encoding='utf-8'))
NOW = datetime(2026,10,15,21,tzinfo=timezone.utc)
FIELDS = {'gross_margin','operating_margin','net_margin','revenue_growth','operating_margin_expansion','current_ratio','roe'}


def parse(key='current', raw=None, **changes):
    kwargs={k:v for k,v in META['documents'][key].items() if k!='original_sha256'}
    kwargs.update(changes)
    return inline.parse((ROOT/(key+'.htm')).read_bytes() if raw is None else raw,cik=vale.CIK,basis=vale.basis(vale.CIK),**kwargs)


def project(facts=None,company=None,supplement=None,now=NOW):
    return sec.project_fundamentals(json.dumps(META['facts'] if facts is None else facts).encode(),
        json.dumps(META['company'] if company is None else company).encode(),symbol='VALE',cik=vale.CIK,
        observed_at=now,filing_supplement=parse() if supplement is None else (None if supplement is False else supplement))


class Client:
    def __init__(self):self.calls=[]
    def get(self,url):
        self.calls.append(url)
        for key,doc in META['documents'].items():
            if url==doc['url']:return (ROOT/(key+'.htm')).read_bytes()
        raise AssertionError('Unexpected request '+url)


def acquire(facts=None,company=None,now=NOW,client=None):
    return inline.acquire(json.dumps(META['facts'] if facts is None else facts).encode(),
        json.dumps(META['company'] if company is None else company).encode(),symbol='VALE',cik=vale.CIK,
        observed_at=now,client=client or Client())


def test_complete_original_inputs_arithmetic_and_repeat():
    result=project();assert result==project()
    assert set(result)&FIELDS==FIELDS
    revenue=38403+19756-16923;old_revenue=38056+16923-18379
    operating=5897+4530-3803;old_operating=10788+3803-6323
    income=2352+3268-3511
    assert result['revenue_growth']==pytest.approx((revenue/old_revenue-1)*100)
    assert result['operating_margin']==pytest.approx(operating/revenue*100)
    assert result['operating_margin_expansion']==pytest.approx((operating/revenue-old_operating/old_revenue)*100)
    assert result['gross_margin']==pytest.approx((13456+6289-5387)/revenue*100)
    assert result['net_margin']==pytest.approx(income/revenue*100)
    assert result['roe']==pytest.approx(income/((39264+38011)/2)*100)
    assert result['current_ratio']==pytest.approx(17659/14818)
    assert not set(result)&{'free_cash_flow','fcf_margin','fcf_growth','eps_ttm','eps_growth','net_debt_to_ebitda'}
    e=json.loads(result['source_evidence_json'])
    assert e['reporting_currency']=='USD'
    assert 'intangible' in e['reviewed_currency_basis']['cash_flow_basis']
    assert any('net operating revenue' in s for s in e['limitations'])
    assert not any('other revenue' in s for s in e['limitations'])
    originals={k:parse(k) for k in META['documents']};count=0
    for section in ('current','prior','instant'):
        for point in e[section].values():
            if not point:continue
            for f in point.get('inputs',[point]):
                found=[r for doc in originals.values() for r in doc['facts'].get(f['tag'],{}).get('units',{}).get(f['unit'],[]) if r.get('start')==f['start'] and r['end']==f['end'] and r['accn']==f['accession'] and r['filed']==f['filed']]
                assert found and {r['val'] for r in found}=={f['value']};count+=1
    assert count==28


def test_one_request_and_no_request_after_companyfacts_catches_up():
    client=Client();s=acquire(client=client)
    assert client.calls==[META['documents']['current']['url']]
    assert project(supplement=s)==project()
    merged=inline.merge(META['facts'],s,available_by=NOW.date())
    client=Client();assert acquire(facts=merged,client=client) is None and client.calls==[]


def test_changed_same_period_amendment_cannot_hide_behind_old_complete_facts():
    f=inline.merge(META['facts'],parse(),available_by=NOW.date());c=deepcopy(META['company'])
    r=c['filings']['recent'];i=r['accessionNumber'].index(META['documents']['current']['accession'])
    r['filingDate'][i]='2026-10-14';r['accessionNumber'][i]='0001292814-26-999999'
    client=Client()
    with pytest.raises(AssertionError,match='999999'):acquire(facts=f,company=c,client=client)
    assert len(client.calls)==1
    client=Client();assert acquire(facts=f,company=c,now=NOW.replace(day=14),client=client) is None
    assert client.calls==[]


@pytest.mark.parametrize('form,period,name,expected',[
    ('6-K/A','2026-06-30','valedfifrs2q26_6ka.htm',True),
    ('6-K','2026-09-30','valedfifrs3q26_6k.htm',True),
    ('20-F','2025-12-31','valeform20f_2025.htm',True),
    ('6-K','2026-09-30','valedfbrgaap3q26_6k.htm',False),
    ('6-K','2026-09-30','vale20261014_6k.htm',False),
    ('6-K','2026-09-30','valedfifrs2q26_6k.htm',False),
    ('6-K','2026-09-30','valedfifrs3q26_6ka.htm',False),
    ('6-K/A','2026-09-30','valedfifrs3q26_6k.htm',False),
    ('20-F','2025-06-30','valeform20f_2025.htm',False),
    ('20-F','2025-12-31','valeform20f_2024.htm',False),
    ('6-K','2026-08-30','valedfifrs3q26_6k.htm',False),
    ('10-Q','2026-06-30','valedfifrs2q26_6k.htm',False),
])
def test_exact_period_form_and_ifrs_filename_policy(form,period,name,expected):
    from app.services.sec_reporting_policy import periodic_document
    assert periodic_document('VALE',vale.CIK,form,period,name) is expected


@pytest.mark.parametrize('change',['wrong_cik','wrong_period','wrong_namespace','wrong_unit','segment','wrong_form'])
def test_invalid_inline_identity_or_units_fail_closed(change):
    raw=(ROOT/'current.htm').read_bytes();kwargs={}
    if change=='wrong_cik':raw=raw.replace(b'0000917851',b'0000917852')
    elif change=='wrong_period':kwargs['report_date']='2026-09-30'
    elif change=='wrong_namespace':raw=raw.replace(b'http://xbrl.ifrs.org/taxonomy/',b'https://example.com/taxonomy/').replace(b'https://xbrl.ifrs.org/taxonomy/',b'https://example.com/taxonomy/')
    elif change=='wrong_unit':raw=raw.replace(b'iso4217:USD',b'iso4217:BRL')
    elif change=='segment':raw=raw.replace(b'</xbrli:entity>',b'<xbrli:segment>dimension</xbrli:segment></xbrli:entity>')
    elif change=='wrong_form':kwargs['form']='6-K'
    with pytest.raises(inline.InlineFactsError):parse(raw=raw,**kwargs)


@pytest.mark.parametrize('change',['company_cik','ticker','directory_shape','naive'])
def test_acquisition_identity_rejected_before_transport(change):
    c=deepcopy(META['company']);now=NOW;client=Client()
    if change=='company_cik':c['cik']=1
    elif change=='ticker':c['tickers']=['OTHER']
    elif change=='directory_shape':c['filings']['recent']['form'].pop()
    elif change=='naive':now=NOW.replace(tzinfo=None)
    with pytest.raises(ValueError):acquire(company=c,now=now,client=client)
    assert client.calls==[]


def test_cash_tags_cannot_leak_into_ppe_only_contract_and_conflicts_hold_income():
    f=deepcopy(META['facts'])
    for tag in ('CashFlowsFromUsedInOperatingActivities','CashFlowsFromUsedInOperations','PurchaseOfPropertyPlantAndEquipmentClassifiedAsInvestingActivities'):
        f['facts']['ifrs-full'][tag]=deepcopy(f['facts']['ifrs-full']['Revenue'])
    result=project(facts=f)
    assert not set(result)&{'free_cash_flow','fcf_margin','fcf_growth'}
    assert result['revenue_growth']==project()['revenue_growth']
    f=inline.merge(META['facts'],parse(),available_by=NOW.date())
    rows=f['facts']['ifrs-full']['ProfitLossAttributableToOwnersOfParent']['units']['USD']
    row=next(r for r in rows if r.get('start')=='2026-01-01' and r['end']=='2026-06-30')
    rows.extend([{**row,'filed':'2026-10-14'},{**row,'filed':'2026-10-14','val':row['val']+1}])
    result=project(facts=f,supplement=False)
    assert 'net_margin' not in result and 'roe' not in result


def test_selective_version_and_stale_period():
    from app.services import sec_fundamentals_preparation as prep,sec_currency_ratios as currency
    assert prep.version_for_symbol('VALE')==prep.VERSION+'_'+vale.VERSION
    assert prep.version_for_symbol('AAPL')==prep.VERSION
    assert currency.basis('OTHER',1) is None
    with pytest.raises(ValueError):currency.basis('VALE',1)
    with pytest.raises(sec.SecFundamentalsCoverageError):project(now=NOW.replace(year=2027))


def test_prepared_cache_repeat_and_actual_confirmation_contract(monkeypatch):
    from types import SimpleNamespace
    from dataclasses import asdict
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    from app.models import InsightsSnapshot
    from app.services import sec_fundamentals_preparation as prep, sec_directory
    from app.services.fundamentals_cache import fundamentals_summary_from_cache_row
    from app.services.confirmation_score import _fundamentals_context_source
    class Clock(datetime):
        @classmethod
        def now(cls,tz=None):return NOW
    engine=create_engine('sqlite:///:memory:');InsightsSnapshot.__table__.create(engine)
    factory=sessionmaker(bind=engine);client=Client();source_get=client.get
    def get(url):
        if 'companyfacts' in url:client.calls.append(url);return json.dumps(META['facts']).encode()
        if '/submissions/' in url:client.calls.append(url);return json.dumps(META['company']).encode()
        return source_get(url)
    monkeypatch.setenv('SEC_FUNDAMENTALS_WARMING_ENABLED','1')
    monkeypatch.setenv('FUNDAMENTALS_PROVIDER','fmp')
    monkeypatch.setattr(client,'get',get)
    monkeypatch.setattr(prep,'DirectSourceClient',lambda:client)
    monkeypatch.setattr(prep,'SessionLocal',factory)
    monkeypatch.setattr(prep,'datetime',Clock)
    monkeypatch.setattr(sec_directory,'directory',lambda:{'VALE':{'cik':vale.CIK}})
    try:
        result=prep.prepare('VALE');assert len(client.calls)==3
        assert set(result['values'])==FIELDS and result['status']=='partial'
        assert prep.prepare('VALE')==result and len(client.calls)==3
        with factory() as db:
            assert json.loads(db.get(InsightsSnapshot,'sec-current-fundamentals:VALE:v1').payload_json)==result
        summary=fundamentals_summary_from_cache_row(SimpleNamespace(**project()),now=NOW)
        assert summary['data_quality']['available'] and summary['data_quality']['scored_metric_count']==3
        assert summary['context']['free_cash_flow'] is None
        assert asdict(_fundamentals_context_source(summary))['present']
    finally:engine.dispose()




def test_new_financial_report_without_usable_inline_facts_does_not_reuse_old_period():
    c=deepcopy(META['company']);recent=c['filings']['recent']
    new={'accessionNumber':'0001292814-26-999999','form':'6-K','filingDate':'2026-10-14','reportDate':'2026-09-30','primaryDocument':'valedfifrs3q26_6k.htm'}
    for k,v in new.items():recent[k].insert(0,v)
    class NewReport:
        calls=[]
        def get(self,url):
            self.calls.append(url)
            assert url.endswith('/000129281426999999/valedfifrs3q26_6k.htm')
            return b'<html><body>Financial report without usable inline facts</body></html>'
    client=NewReport()
    with pytest.raises(inline.InlineFactsError):acquire(company=c,client=client)
    assert len(client.calls)==1


def test_event_and_brgaap_filings_do_not_supplant_ifrs_reports():
    c=deepcopy(META['company']);recent=c['filings']['recent']
    for name in ('vale20261014_6k.htm','valedfbrgaap3q26_6k.htm'):
        new={'accessionNumber':'0001292814-26-999999','form':'6-K','filingDate':'2026-10-14','reportDate':'2026-09-30','primaryDocument':name}
        for k,v in new.items():recent[k].insert(0,v)
    client=Client();result=acquire(company=c,client=client)
    assert client.calls==[META['documents']['current']['url']]
    assert result['evidence']['report_date']=='2026-06-30'
