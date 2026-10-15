from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import pytest
from app.services import sec_nvs_financials as nvs, sec_inline_financials as inline, sec_fundamentals as sec

ROOT = Path(__file__).parent/'fixtures/sec_nvs'
META = json.loads((ROOT/'observations.json').read_text(encoding='utf-8'))
NOW = datetime(2026,10,15,18,tzinfo=timezone.utc)
FIELDS = {'gross_margin','operating_margin','net_margin','revenue_growth','operating_margin_expansion',
          'current_ratio','roe','free_cash_flow','fcf_margin','fcf_growth'}


def original(key='2026-06-30'):
    kwargs={k:v for k,v in META['documents'][key].items() if k!='original_sha256'}
    if key!='annual':kwargs['cover_raw']=(ROOT/(key+'-cover.htm')).read_bytes()
    return (ROOT/(key+'.htm')).read_bytes(),kwargs


def parse(key='2026-06-30', raw=None, **changes):
    source,kwargs=original(key);kwargs.update(changes)
    return inline.parse(source if raw is None else raw,cik=nvs.CIK,basis=nvs.basis(nvs.CIK),**kwargs)


def project(facts=None,company=None,supplement=None,now=NOW):
    return sec.project_fundamentals(json.dumps(META['facts'] if facts is None else facts).encode(),
        json.dumps(META['company'] if company is None else company).encode(),symbol='NVS',cik=nvs.CIK,
        observed_at=now,filing_supplement=parse() if supplement is None else (None if supplement is False else supplement))


class Client:
    def __init__(self):self.calls=[]
    def get(self,url):
        self.calls.append(url)
        for key,doc in META['documents'].items():
            if url==doc['url']:return original(key)[0]
            if url==doc.get('cover_url'):return original(key)[1]['cover_raw']
        raise AssertionError('Unexpected request '+url)


def acquire(facts=None,company=None,now=NOW,client=None):
    return inline.acquire(json.dumps(META['facts'] if facts is None else facts).encode(),
        json.dumps(META['company'] if company is None else company).encode(),symbol='NVS',cik=nvs.CIK,
        observed_at=now,client=client or Client())


def test_complete_actual_inputs_independent_arithmetic_and_exact_repeat():
    result=project();assert result==project()
    assert set(result)&FIELDS==FIELDS
    # Reported statement amounts in USD millions, independently reconciled with three complete originals.
    revenue=54532+27521-27287;old_revenue=50317+27287-24341
    operating=17644+8985-9527;old_operating=14544+9527-7387
    income=13984+6420-7647
    cash=19144+9558-10309;capex=1548+667-585
    old_cash=17619+10309-7140;old_capex=1366+585-487
    assert result['revenue_growth']==pytest.approx((revenue/old_revenue-1)*100)
    assert result['operating_margin']==pytest.approx(operating/revenue*100)
    assert result['operating_margin_expansion']==pytest.approx((operating/revenue-old_operating/old_revenue)*100)
    assert result['gross_margin']==pytest.approx((42975+21306-21907)/revenue*100)
    assert result['net_margin']==pytest.approx(income/revenue*100)
    assert result['roe']==pytest.approx(income/((41985+41542)/2)*100)
    assert result['current_ratio']==pytest.approx(27351/31748)
    assert result['free_cash_flow']==(cash-capex)*1e6
    assert result['fcf_margin']==pytest.approx((cash-capex)/revenue*100)
    assert result['fcf_growth']==pytest.approx(((cash-capex)/(old_cash-old_capex)-1)*100)
    e=json.loads(result['source_evidence_json'])
    assert e['reporting_currency']=='USD' and 'IFRS' in e['limitations'][0]
    assert not any('no FX conversion or USD cash amounts' in s for s in e['limitations'])
    assert 'discontinued' in e['reviewed_currency_basis']['cash_flow_basis']
    assert not set(result)&{'eps_ttm','net_debt_to_ebitda','ev_to_ebitda'}
    originals={k:parse(k) for k in META['documents']};count=0
    for section in ('current','prior','instant'):
        for point in e[section].values():
            if not point:continue
            for f in point.get('inputs',[point]):
                found=[r for doc in originals.values() for r in doc['facts'].get(f['tag'],{}).get('units',{}).get(f['unit'],[])
                    if r.get('start')==f['start'] and r['end']==f['end'] and r['accn']==f['accession'] and r['filed']==f['filed']]
                assert found and {r['val'] for r in found}=={f['value']};count+=1
    assert count==40


def test_two_bounded_requests_and_no_request_after_companyfacts_catches_up():
    client=Client();supplement=acquire(client=client)
    assert client.calls==[META['documents']['2026-06-30']['cover_url'],META['documents']['2026-06-30']['url']]
    assert project(supplement=supplement)==project()
    merged=inline.merge(META['facts'],supplement,available_by=NOW.date())
    client=Client();assert acquire(facts=merged,client=client) is None and client.calls==[]


def test_new_same_period_amendment_is_not_hidden_by_complete_old_facts():
    f=inline.merge(META['facts'],parse(),available_by=NOW.date());c=deepcopy(META['company'])
    recent=c['filings']['recent'];i=recent['reportDate'].index('2026-06-30')
    recent['filingDate'][i]='2026-10-14'
    recent['accessionNumber'][i]='0001114448-26-999999';recent['form'][i]='6-K/A'
    client=Client()
    with pytest.raises(AssertionError,match='999999'):acquire(facts=f,company=c,client=client)
    assert len(client.calls)==1


@pytest.mark.parametrize('change',['missing_cover','wrong_name','wrong_form','wrong_context','context_period','wrong_cik',
    'foreign_link','escape_link','query_link','duplicate_link','missing_link','namespace','rebind','foreign_cover','oversize','non_string_url'])
def test_untrusted_cover_exhibit_sets_fail_closed(change):
    raw,kwargs=original();cover=kwargs['cover_raw'];url=kwargs['cover_url']
    if change=='missing_cover':cover=b''
    elif change=='wrong_name':cover=cover.replace(b'Novartis AG',b'Other AG')
    elif change=='wrong_form':cover=cover.replace(b'>6-K<',b'>20-F<')
    elif change=='wrong_context':cover=cover.replace(b'Q_D-YTD-CurrentYear',b'absent')
    elif change=='context_period':raw=raw.replace(b'2026-01-01',b'2026-02-01')
    elif change=='wrong_cik':raw=raw.replace(b'0001114448',b'0001114449')
    elif change in ('foreign_link','escape_link','query_link'):
        target={'foreign_link':b'https://example.com/report.htm','escape_link':b'../report.htm','query_link':b'nvs-20260630-99_2.htm?x=1'}[change]
        cover=cover.replace(b'nvs-20260630-99_2.htm',target)
    elif change=='duplicate_link':cover=cover.replace(b'</body>',b'<a href="nvs-20260630-99_2.htm">99.2 Interim Financial Report</a></body>')
    elif change=='missing_link':cover=cover.replace(b'Interim Financial Report',b'Other Report')
    elif change=='namespace':cover=cover.replace(b'http://www.xbrl.org/2013/inlineXBRL',b'https://example.com/ix')
    elif change=='rebind':cover=cover.replace(b'<body',b'<body xmlns:dei="https://example.com/dei"',1)
    elif change=='foreign_cover':url=url.replace('/1114448/','/1114449/')
    elif change=='oversize':cover=b' '*200001
    elif change=='non_string_url':url=None
    with pytest.raises(inline.InlineFactsError):parse(raw=raw,cover_raw=cover,cover_url=url)


@pytest.mark.parametrize('change',['wrong_namespace','wrong_period','wrong_unit','segment','ambiguous_form'])
def test_exhibit_proof_does_not_override_invalid_financial_identity(change):
    raw,kwargs=original()
    if change=='wrong_namespace':raw=raw.replace(b'https://xbrl.ifrs.org/taxonomy/',b'https://example.com/taxonomy/')
    elif change=='wrong_period':kwargs['report_date']='2026-09-30'
    elif change=='wrong_unit':raw=raw.replace(b'iso4217:USD',b'iso4217:EUR')
    elif change=='segment':raw=raw.replace(b'</xbrli:entity>',b'<xbrli:segment>dimension</xbrli:segment></xbrli:entity>')
    elif change=='ambiguous_form':raw=raw.replace(b'</body>',b'<ix:nonNumeric name="dei:DocumentType" contextRef="Q_D-YTD-CurrentYear">20-F</ix:nonNumeric></body>')
    with pytest.raises(inline.InlineFactsError):inline.parse(raw,cik=nvs.CIK,basis=nvs.basis(nvs.CIK),**kwargs)


@pytest.mark.parametrize('change',['company_cik','ticker','directory_shape','naive'])
def test_acquisition_identity_rejected_before_transport(change):
    c=deepcopy(META['company']);now=NOW;client=Client()
    if change=='company_cik':c['cik']=1
    elif change=='ticker':c['tickers']=['OTHER']
    elif change=='directory_shape':c['filings']['recent']['form'].pop()
    elif change=='naive':now=NOW.replace(tzinfo=None)
    with pytest.raises(ValueError):acquire(company=c,now=now,client=client)
    assert client.calls==[]


def test_filing_day_not_available_until_completed_and_stale_period_is_held():
    client=Client();acquire(now=datetime(2026,7,21,18,tzinfo=timezone.utc),client=client)
    assert not any('000111444826000012' in u for u in client.calls)
    with pytest.raises(sec.SecFundamentalsCoverageError):project(now=NOW.replace(year=2027))


def test_missing_cash_is_not_zero_and_conflicting_fact_holds_affected_metric():
    supplement=parse();supplement['facts'].pop(nvs.TAGS['operating_cash_flow'][0])
    result=project(supplement=supplement)
    assert not set(result)&{'free_cash_flow','fcf_margin','fcf_growth'}
    assert result['revenue_growth']==project()['revenue_growth']
    f=inline.merge(META['facts'],parse(),available_by=NOW.date())
    rows=f['facts']['ifrs-full']['ProfitLossAttributableToOwnersOfParent']['units']['USD']
    row=next(r for r in rows if r.get('start')=='2026-01-01' and r['end']=='2026-06-30')
    rows.extend([{**row,'filed':'2026-10-14'},{**row,'filed':'2026-10-14','val':row['val']+1}])
    result=project(facts=f,supplement=False)
    assert 'net_margin' not in result and 'roe' not in result


def test_issuer_policy_is_scoped_and_versions_are_selective():
    from app.services import sec_currency_ratios as currency,sec_reporting_policy as policy,sec_fundamentals_preparation as prep
    assert currency.basis('OTHER','1') is None
    with pytest.raises(ValueError):currency.basis('NVS','1')
    assert prep.version_for_symbol('NVS')==prep.VERSION+'_'+nvs.VERSION
    assert prep.version_for_symbol('AAPL')==prep.VERSION
    assert prep.version_for_symbol('ENB')==prep.VERSION+'_'+currency.VERSION
    assert not policy.periodic_document('NVS',nvs.CIK,'6-K','2026-08-15','event.htm')
    assert not policy.periodic_document('NVS',nvs.CIK,'20-F','2026-06-30','annual.htm')
    assert policy.periodic_document('NVS',nvs.CIK,'6-K','2026-09-30','new.htm')


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
    monkeypatch.setattr(sec_directory,'directory',lambda:{'NVS':{'cik':nvs.CIK}})
    try:
        result=prep.prepare('NVS');assert len(client.calls)==4
        assert set(result['values'])==FIELDS and result['status']=='partial'
        assert prep.prepare('NVS')==result and len(client.calls)==4
        with factory() as db:
            assert json.loads(db.get(InsightsSnapshot,'sec-current-fundamentals:NVS:v1').payload_json)==result
        summary=fundamentals_summary_from_cache_row(SimpleNamespace(**project()),now=NOW)
        assert summary['data_quality']['available'] and summary['data_quality']['scored_metric_count']==3
        assert summary['context']['free_cash_flow']==16763000000
        assert asdict(_fundamentals_context_source(summary))['present']
    finally:engine.dispose()


def test_changed_amendment_uses_new_cover_and_no_old_period_fallback():
    f=inline.merge(META['facts'],parse(),available_by=NOW.date());c=deepcopy(META['company'])
    r=c['filings']['recent'];i=r['reportDate'].index('2026-06-30')
    r['filingDate'][i]='2026-10-14';r['accessionNumber'][i]='0001114448-26-999999';r['form'][i]='6-K/A'
    raw,kw=original();cover=kw['cover_raw'].replace(b'>6-K<',b'>6-K/A<')
    class Amended:
        calls=[]
        def get(self,url):
            self.calls.append(url)
            assert '/000111444826999999/' in url
            return raw if '99_2' in url else cover
    client=Amended();supplement=acquire(facts=f,company=c,client=client)
    assert len(client.calls)==2 and supplement['evidence']['form']=='6-K/A'
    assert supplement['evidence']['filed']=='2026-10-14'
    result=project(facts=f,company=c,supplement=supplement)
    assert result['free_cash_flow']==project()['free_cash_flow']
    inputs=json.loads(result['source_evidence_json'])['current']['revenue']['inputs']
    assert sum(p['accession']=='0001114448-26-999999' for p in inputs)==2
    client=Amended();client.calls=[]
    assert acquire(facts=f,company=c,now=NOW.replace(day=14),client=client) is None
    assert client.calls==[]
