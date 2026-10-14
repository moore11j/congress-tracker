from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
import gzip
import hashlib
import json

import pytest
from app.services import sec_registration_history as subject, sec_fundamentals as sec
from app.services import sec_inline_financials as inline

ROOT = Path(__file__).parent/'fixtures/sec_spcx'
NOW = datetime(2026,10,14,22,45,tzinfo=timezone.utc)


def raw(name):
    return gzip.decompress((ROOT/(name+'.gz')).read_bytes())


def inputs():
    return json.loads(raw('SPCX-facts.json')), json.loads(raw('SPCX-company.json'))


@pytest.fixture(scope='module')
def original():
    return subject.parse(raw('SPCX-prospectus.html'))


def project(supplement, facts=None, company=None, now=NOW):
    f,c = inputs()
    return sec.project_fundamentals(json.dumps(f if facts is None else facts).encode(),
        json.dumps(c if company is None else company).encode(),symbol='SPCX',cik=subject.CIK,
        observed_at=now,filing_supplement=supplement)


def test_actual_complete_originals_current_inputs_and_six_metrics(original):
    assert hashlib.sha256(raw('SPCX-prospectus.html')).hexdigest()==subject.SHA256
    assert subject.projection_digest(original)==subject.PROJECTION_SHA256
    current=raw('SPCX-2026-06-30.html')
    assert hashlib.sha256(current).hexdigest()==subject.CURRENT_SHA256
    parsed=inline.parse(current,cik=subject.CIK,accession=subject.CURRENT_ACCESSION,
        filed=subject.CURRENT_FILED,report_date=subject.CURRENT_END,form='10-Q',url=subject.CURRENT_URL)
    result=project(original); assert result==project(original)
    e=json.loads(result['source_evidence_json'])
    assert e['current']['revenue']['value']==(18674+12508-8138)*1e6
    assert result['operating_margin']==pytest.approx(-3732/23044*100)
    assert result['net_margin']==pytest.approx(-8218/23044*100)
    assert result['roe']==pytest.approx(-8218/((5217+127224)/2)*100)
    assert result['current_ratio']==pytest.approx(108047/21122)
    assert result['free_cash_flow']==-32348e6
    assert result['fcf_margin']==pytest.approx(-32348/23044*100)
    assert not set(result)&{'revenue_growth','operating_margin_expansion','fcf_growth','eps_ttm','trailing_pe','net_debt_to_ebitda'}
    assert len(e['financial_continuity']['overlap_checks'])==11
    for section in ('current','instant'):
        for point in e[section].values():
            if not point:continue
            for p in point.get('inputs',[point]):
                if p['accession']==subject.ACCESSION:
                    assert p['source_form']=='424B4' and p['source_sha256']==subject.SHA256 and p['source_label']
                    continue
                rows=parsed['facts'][p['tag']]['units']['USD']
                matches=[r for r in rows if r.get('start')==p['start'] and r['end']==p['end']]
                assert matches and {r['val'] for r in matches}=={p['value']}


def test_missing_history_cannot_make_current_trailing_year():
    with pytest.raises(sec.SecFundamentalsCoverageError):project(None)


@pytest.mark.parametrize('change',['amount','unit','form','filed','accession','missing','conflict'])
def test_interim_bridge_conflicts_hold_entire_history(original,change):
    f,c=inputs();tag=subject.TAGS['revenue'];rows=f['facts']['us-gaap'][tag]['units']['USD']
    target=next(r for r in rows if r.get('start')=='2026-01-01' and r['end']=='2026-06-30')
    if change=='amount':target['val']+=1
    elif change=='unit':f['facts']['us-gaap'][tag]['units']={'CAD':rows}
    elif change=='form':target['form']='424B4'
    elif change=='filed':target['filed']='2026-08-03'
    elif change=='accession':target['accn']='0001628280-26-999999'
    elif change=='missing':rows.remove(target)
    elif change=='conflict':rows.append({**target,'val':target['val']+1})
    with pytest.raises(subject.RegistrationHistoryError):project(original,facts=f)


@pytest.mark.parametrize('change',['cik','ticker','name','duplicate_ticker','shape','missing_original','newer','amendment','naive','same_filing_day','stale'])
def test_company_period_and_completed_day_guards(original,change):
    f,c=inputs();now=NOW;r=c['filings']['recent']
    if change=='cik':c['cik']='1'
    elif change=='ticker':c['tickers']=['OLD_ETF']
    elif change=='name':c['name']='Unrelated issuer'
    elif change=='duplicate_ticker':c['tickers']*=2
    elif change=='shape':r['form'].pop()
    elif change=='missing_original':r['accessionNumber'][r['accessionNumber'].index(subject.ACCESSION)]='other'
    elif change in {'newer','amendment'}:
        i=r['accessionNumber'].index(subject.CURRENT_ACCESSION)
        r['accessionNumber'][i]='0001628280-26-999999'
        if change=='newer':r['reportDate'][i]='2026-09-30';r['filingDate'][i]='2026-10-13'
        else:r['form'][i]='10-Q/A'
    elif change=='naive':now=NOW.replace(tzinfo=None)
    elif change=='same_filing_day':now=datetime(2026,8,4,23,tzinfo=timezone.utc)
    elif change=='stale':now=NOW.replace(year=2027)
    with pytest.raises(ValueError):project(original,company=c,now=now)


@pytest.mark.parametrize('change',['annual_amount','bridge_amount','bridge_missing','provenance','hash','evidence'])
def test_prepared_history_cannot_be_modified_after_review(original,change):
    altered=deepcopy(original)
    if change=='annual_amount':altered['facts'][subject.TAGS['revenue']]['units']['USD'][0]['val']+=1
    elif change=='bridge_amount':altered['registration_bridge']['revenue'][0]+=1
    elif change=='bridge_missing':altered['registration_bridge']={}
    elif change=='provenance':next(iter(altered['provenance'].values()))['source_form']='10-K'
    elif change=='hash':altered['evidence']['sha256']='0'*64
    elif change=='evidence':altered['evidence']['current_review_url']='https://example.com'
    with pytest.raises(subject.RegistrationHistoryError,match='projection_changed'):project(altered)


def test_new_registration_facts_do_not_enter_periodic_reader(original):
    f,c=inputs();rows=f['facts']['us-gaap'][subject.TAGS['revenue']]['units']['USD']
    rows.append(dict(val=1e15,start='2024-01-01',end='2024-06-30',form='424B4',accn='unreviewed',filed='2026-08-01'))
    result=project(original,facts=f)
    assert 'revenue_growth' not in result
    assert result['operating_margin']==project(original)['operating_margin']


def test_explicit_periodic_annual_conflict_is_not_overwritten(original):
    f,c=inputs();rows=f['facts']['us-gaap'][subject.TAGS['revenue']]['units']['USD']
    row=dict(val=1e9,start='2025-01-01',end='2025-12-31',form='10-Q',accn=subject.CURRENT_ACCESSION,filed=subject.CURRENT_FILED)
    rows.extend([row,{**row,'val':2e9}])
    with pytest.raises(sec.SecFundamentalsCoverageError):project(original,facts=f)


def test_acquisition_exact_source_and_changed_original_hold():
    f,c=inputs();requests=[]
    class Client:
        def get(self,url):requests.append(url);return raw('SPCX-prospectus.html')
    result=inline.acquire(json.dumps(f).encode(),json.dumps(c).encode(),symbol='SPCX',cik=subject.CIK,observed_at=NOW,client=Client())
    assert requests==[subject.URL]
    assert project(result)['free_cash_flow']==-32348e6
    with pytest.raises(subject.RegistrationHistoryError,match='original_changed'):subject.parse(raw('SPCX-prospectus.html')+b' ')


def test_known_coverage_hold_is_explicit_in_preparation_path():
    f,c=inputs();r=c['filings']['recent'];i=r['accessionNumber'].index(subject.CURRENT_ACCESSION);r['form'][i]='10-Q/A'
    class Client:
        def get(self,url):raise AssertionError('Unavailable review must not download')
    result=inline.acquire(json.dumps(f).encode(),json.dumps(c).encode(),symbol='SPCX',cik=subject.CIK,observed_at=NOW,client=Client())
    assert 'registration_error' in result
    with pytest.raises(subject.RegistrationHistoryError):project(result)


def test_common_income_stays_distinct_from_parent_income(original):
    from app.services.sec_common_income import resolve
    f,c=inputs();fr,cr=json.dumps(f).encode(),json.dumps(c).encode();result=project(original);e=json.loads(result['source_evidence_json'])
    common=resolve(fr,cr,e,symbol='SPCX',cik=subject.CIK,observed_at=NOW,filing_supplement=original)
    assert common['status']=='available' and common['point']['value']==-8889e6
    assert e['current']['net_income']['value']==-8218e6


def test_version_scoped_to_reviewed_issuer():
    from app.services.sec_fundamentals_preparation import version_for_symbol,VERSION
    assert version_for_symbol('SPCX')==VERSION+'_'+subject.VERSION
    assert version_for_symbol('AAPL')==VERSION


@pytest.mark.parametrize('metric',['revenue','operating_income','net_income','common_income','operating_cash_flow','capex','equity','current_assets','current_liabilities'])
def test_single_revised_historical_amount_requires_accounting_review(original,metric):
    f,c=inputs();tag=subject.TAGS[metric];row=deepcopy(original['facts'][tag]['units']['USD'][0])
    row.update(val=row['val']+1,form='10-Q',accn=subject.CURRENT_ACCESSION,filed=subject.CURRENT_FILED)
    f['facts']['us-gaap'].setdefault(tag,{'units':{'USD':[]}})['units']['USD'].append(row)
    with pytest.raises(subject.RegistrationHistoryError):project(original,facts=f)
