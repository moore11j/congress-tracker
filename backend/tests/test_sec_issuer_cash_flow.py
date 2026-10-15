from datetime import datetime, timezone
import json
import pytest
from test_sec_fundamentals import sources
from app.services import sec_fundamentals as sec, sec_issuer_cash_flow as issuer
NOW=datetime(2026,10,10,22,tzinfo=timezone.utc)


def fixture():
    facts,company=sources();facts['cik']=company['cik']=1045810
    company.update(name='NVIDIA CORP',tickers=['NVDA'])
    gaap=facts['facts']['us-gaap'];gaap[issuer.PRODUCTIVE]=gaap.pop('PaymentsToAcquirePropertyPlantAndEquipment')
    accessions=['0001045810-26-000021','0001045810-25-000209','0001045810-26-000075']
    filed=['2026-02-01','2025-08-01','2026-08-01'];ends=['2025-12-31','2025-06-30','2026-06-30']
    company['filings']={'recent':{'accessionNumber':accessions,'filingDate':filed,'reportDate':ends,
        'form':['10-K','10-Q','10-Q'],'primaryDocument':['nvda-'+s.replace('-','')+'.htm' for s in ends]}}
    for v in gaap.values():
        for r in v['units']['USD']:
            i=0 if r['end'].endswith('12-31') else (1 if r['end']<'2025-01-01' else 2)
            r.update(accn=accessions[i],filed=filed[i])
    docs={}
    for i,accn in enumerate(accessions):
        rows=[r for r in gaap['NetCashProvidedByUsedInOperatingActivities']['units']['USD'] if r['accn']==accn]
        body=''
        for j,r in enumerate(rows):
            body+=f'<xbrli:context id="c{j}"><xbrli:entity><xbrli:identifier scheme="http://www.sec.gov/CIK">0001045810</xbrli:identifier></xbrli:entity><xbrli:period><xbrli:startDate>{r["start"]}</xbrli:startDate><xbrli:endDate>{r["end"]}</xbrli:endDate></xbrli:period></xbrli:context>'
            body+=f'<ix:nonFraction name="{issuer.PRINCIPAL}" contextRef="c{j}" unitRef="usd" format="ixt:num-dot-decimal" scale="0">1</ix:nonFraction>'
        raw=f'<html xmlns:nvda="http://www.nvidia.com/20260630"><body><ix:nonNumeric name="dei:EntityCentralIndexKey">0001045810</ix:nonNumeric><ix:nonNumeric name="dei:DocumentPeriodEndDate" format="ixt:date-monthname-day-year-en">{datetime.fromisoformat(ends[i]).strftime("%B %d, %Y")}</ix:nonNumeric><xbrli:unit id="usd"><xbrli:measure>iso4217:USD</xbrli:measure></xbrli:unit>'+body+'</body></html>'
        url=f'https://www.sec.gov/Archives/edgar/data/1045810/{accn.replace("-", "")}/nvda-{ends[i].replace("-", "")}.htm'
        docs[url]=raw.encode()
    return facts,company,docs


def run(facts,company,docs):
    raw,meta=json.dumps(facts).encode(),json.dumps(company).encode()
    values=sec.project_fundamentals(raw,meta,symbol='NVDA',cik='1045810',observed_at=NOW)
    calls=[]
    class Client:
        def get(self,url):calls.append(url);return docs[url]
    return issuer.supplement(values,raw,meta,client=Client()),calls


def test_filed_cash_flow_math_provenance_and_repeat():
    result,calls=run(*fixture())
    assert result['free_cash_flow']==31.5
    assert result['fcf_margin']==pytest.approx(31.5/130*100)
    assert result['fcf_growth']==pytest.approx((31.5/26.5-1)*100)
    proof=json.loads(result['source_evidence_json'])['issuer_cash_flow']
    assert proof['status']=='available' and len(calls)==3
    assert len(proof['current']['principal_payments']['inputs'])==3
    assert len(proof['filings'])==3 and all(len(f['sha256'])==64 for f in proof['filings'])
    assert proof['current']['productive_asset_purchases']['value']==6.5
    assert run(*fixture())[0]==result


@pytest.mark.parametrize('kind',['cik','namespace','namespace_shape','period','currency','dimension','negative','conflict','scale','missing','date_format'])
def test_bad_filed_evidence_never_produces_partial_cash_flow(kind):
    facts,company,docs=fixture();key=next(iter(docs));raw=docs[key]
    pairs={'cik':(b'0001045810',b'0000000001'),'namespace':(b'http://www.nvidia.com/',b'http://other.example/'),
        'namespace_shape':(b'http://www.nvidia.com/',b'http://wwwXnvidiaYcom/'),
        'period':(b'December 31, 2025',b'December 30, 2025'),'currency':(b'iso4217:USD',b'iso4217:EUR'),
        'dimension':(b'</xbrli:entity>',b'<xbrli:segment>Segment</xbrli:segment></xbrli:entity>'),
        'negative':(b'scale="0"',b'scale="0" sign="-"'),'scale':(b'scale="0"',b'scale="99"'),
        'missing':(issuer.PRINCIPAL.encode(),b'nvda:OtherPayment'),'date_format':(b'ixt:date-monthname-day-year-en',b'other:date')}
    if kind in pairs:raw=raw.replace(*pairs[kind])
    else:
        start=raw.index(b'<ix:nonFraction');end=raw.index(b'</ix:nonFraction>',start)+len(b'</ix:nonFraction>')
        raw=raw[:end]+raw[start:end].replace(b'>1<',b'>2<')+raw[end:]
    docs[key]=raw;result,_=run(facts,company,docs)
    assert all(k not in result for k in ['free_cash_flow','fcf_margin','fcf_growth'])
    assert 'revenue_growth' in result
    assert json.loads(result['source_evidence_json'])['issuer_cash_flow']['status']=='unavailable'


def test_future_directory_and_missing_period_hold_without_zero():
    facts,company,docs=fixture();company['filings']['recent']['filingDate'][0]='2026-10-10'
    result,calls=run(facts,company,docs)
    assert 'free_cash_flow' not in result and len(calls)<=1
    facts,company,docs=fixture();key=next(iter(docs));docs[key]=docs[key].replace(b'2024-01-01',b'2024-01-02')
    assert 'free_cash_flow' not in run(facts,company,docs)[0]


def test_unreviewed_issuer_never_fetches_filings():
    facts,company,_=fixture();company['tickers']=['OTHER']
    raw,meta=json.dumps(facts).encode(),json.dumps(company).encode()
    values=sec.project_fundamentals(raw,meta,symbol='OTHER',cik='1045810',observed_at=NOW)
    class Client:
        def get(self,url):pytest.fail('unreviewed issuer fetched')
    assert issuer.supplement(values,raw,meta,client=Client()) is values


def test_untransformed_scaled_decimal_matches_actual_filings():
    facts,company,docs=fixture()
    docs={k:v.replace(b' format="ixt:num-dot-decimal"',b'').replace(b'scale="0"',b'scale="6"') for k,v in docs.items()}
    result,_=run(facts,company,docs)
    assert result['free_cash_flow']==32.5-1000000
    key=next(iter(docs));docs[key]=docs[key].replace(b'>1</ix:nonFraction>',b'>1,000</ix:nonFraction>')
    assert 'free_cash_flow' not in run(facts,company,docs)[0]


def test_selected_core_fetch_uses_same_reviewed_evidence_without_market_provider(monkeypatch):
    facts,company,docs=fixture()
    monkeypatch.setenv('STOCK_PRICE_PROVIDER','fmp')
    monkeypatch.setattr(sec,'company_directory',lambda day:{'NVDA':{'cik':issuer.CIK}})
    from app.clients.direct_sources import DirectSourceClient
    mapping={**docs,f'https://data.sec.gov/submissions/CIK{issuer.CIK}.json':json.dumps(company).encode(),
        f'https://data.sec.gov/api/xbrl/companyfacts/CIK{issuer.CIK}.json':json.dumps(facts).encode()}
    calls=[]
    def fetch(self,url):calls.append(url);return mapping[url]
    monkeypatch.setattr(DirectSourceClient,'get',fetch)
    result=sec.fetch_fundamentals('NVDA')
    assert result['free_cash_flow']==31.5 and len(calls)==5
    assert 'market_cap' not in result
    assert json.loads(result['source_evidence_json'])['issuer_cash_flow']['status']=='available'


def test_source_budget_and_transport_error_preserve_other_financials():
    facts,company,docs=fixture()
    raw,meta=json.dumps(facts).encode(),json.dumps(company).encode()
    values=sec.project_fundamentals(raw,meta,symbol='NVDA',cik=issuer.CIK,observed_at=NOW)
    class Client:
        def get(self,url):raise issuer.DirectSourceError('source_unavailable')
    result=issuer.supplement(values,raw,meta,client=Client())
    assert {k:v for k,v in result.items() if k != 'source_evidence_json'} == {k:v for k,v in values.items() if k != 'source_evidence_json'}
    proof=json.loads(result['source_evidence_json'])['issuer_cash_flow']
    assert proof['reason']=='cash_flow_source_unavailable' and proof['retryable'] is True
    assert 'free_cash_flow' not in values
    evidence=json.loads(values['source_evidence_json'])
    for i,r in enumerate(evidence['current']['operating_cash_flow']['inputs']):r['accession']=f'0001045810-26-00010{i}'
    values['source_evidence_json']=json.dumps(evidence)
    class Forbidden:
        def get(self,url):pytest.fail('over-budget request')
    result=issuer.supplement(values,raw,meta,client=Forbidden())
    assert json.loads(result['source_evidence_json'])['issuer_cash_flow']['reason']=='filing_budget_exceeded'
