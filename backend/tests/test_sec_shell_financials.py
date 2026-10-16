from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import pytest
from app.services import sec_shell_financials as nvs, sec_inline_financials as inline, sec_fundamentals as sec

ROOT = Path(__file__).parent/'fixtures/sec_shell'
META = json.loads((ROOT/'observations.json').read_text(encoding='utf-8'))
NOW = datetime(2026,10,15,18,tzinfo=timezone.utc)
FIELDS = {'net_margin','revenue_growth','current_ratio','roe'}


def original(key='current'):
    kwargs={k:v for k,v in META['documents'][key].items() if k!='original_sha256'}
    if key!='annual':kwargs['cover_raw']=(ROOT/(key+'-cover.htm')).read_bytes()
    return (ROOT/(key+'.htm')).read_bytes(),kwargs


def parse(key='current', raw=None, **changes):
    source,kwargs=original(key);kwargs.update(changes)
    return inline.parse(source if raw is None else raw,cik=nvs.CIK,basis=nvs.basis(nvs.CIK),**kwargs)


def project(facts=None,company=None,supplement=None,now=NOW):
    return sec.project_fundamentals(json.dumps(META['facts'] if facts is None else facts).encode(),
        json.dumps(META['company'] if company is None else company).encode(),symbol='SHEL',cik=nvs.CIK,
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
        json.dumps(META['company'] if company is None else company).encode(),symbol='SHEL',cik=nvs.CIK,
        observed_at=now,client=client or Client())


def test_complete_original_inputs_and_four_ratio_arithmetic():
    result=project();assert result==project()
    assert set(result)&FIELDS==FIELDS
    revenue=266886+164355-134640
    old_revenue=284312+134640-146942
    income=17837+16515-8381
    assert result['revenue_growth']==pytest.approx((revenue/old_revenue-1)*100)
    assert result['net_margin']==pytest.approx(income/revenue*100)
    assert result['roe']==pytest.approx(income/((181137+180786)/2)*100)
    assert result['current_ratio']==pytest.approx(122833/85614)
    assert not set(result)&{'gross_margin','operating_margin','operating_margin_expansion','free_cash_flow','fcf_margin','fcf_growth','net_debt_to_ebitda','eps_ttm','eps_growth'}
    evidence=json.loads(result['source_evidence_json'])
    assert 'investing cash flows' in evidence['reviewed_currency_basis']['cash_flow_basis']
    assert evidence['reporting_currency']=='USD'
    originals={k:parse(k) for k in META['documents']};count=0
    for section in ('current','prior','instant'):
        for point in evidence[section].values():
            if not point:continue
            for f in point.get('inputs',[point]):
                found=[r for doc in originals.values() for r in doc['facts'].get(f['tag'],{}).get('units',{}).get(f['unit'],[]) if r.get('start')==f['start'] and r['end']==f['end'] and r['accn']==f['accession'] and r['filed']==f['filed']]
                assert found and {r['val'] for r in found}=={f['value']}
                count+=1
    assert count==22


def test_current_split_cover_requests_and_no_request_after_facts_catch_up():
    client=Client();supplement=acquire(client=client)
    assert client.calls==[META['documents']['current']['cover_url'],META['documents']['current']['url']]
    assert project(supplement=supplement)==project()
    merged=inline.merge(META['facts'],supplement,available_by=NOW.date())
    client=Client();assert acquire(facts=merged,client=client) is None and client.calls==[]


@pytest.mark.parametrize('change',['missing_cover','wrong_name','wrong_form','wrong_context','context_period','wrong_cik',
    'foreign_link','escape_link','query_link','duplicate_link','missing_link','namespace','rebind','foreign_cover','oversize','non_string_url'])
def test_untrusted_cover_exhibit_sets_fail_closed(change):
    raw,kwargs=original();cover=kwargs['cover_raw'];url=kwargs['cover_url']
    if change=='missing_cover':cover=b''
    elif change=='wrong_name':cover=cover.replace(b'Shell plc',b'Other plc')
    elif change=='wrong_form':cover=cover.replace(b'>6-K<',b'>20-F<')
    elif change=='wrong_context':cover=cover.replace(b'c-4',b'absent')
    elif change=='context_period':raw=raw.replace(b'2026-01-01',b'2026-02-01')
    elif change=='wrong_cik':raw=raw.replace(b'0001306965',b'0001306966')
    elif change in ('foreign_link','escape_link','query_link'):
        target={'foreign_link':b'https://example.com/report.htm','escape_link':b'../report.htm','query_link':b'shel-20260630.htm?x=1'}[change]
        cover=cover.replace(b'href="shel-20260630.htm"',b'href="'+target+b'"')
    elif change=='duplicate_link':cover=cover.replace(b'</body>',b'<a href="shel-20260630.htm">99.2</a></body>')
    elif change=='missing_link':cover=cover.replace(b'>99.2<',b'>98.2<')
    elif change=='namespace':cover=cover.replace(b'http://www.xbrl.org/2013/inlineXBRL',b'https://example.com/ix')
    elif change=='rebind':cover=cover.replace(b'<body',b'<body xmlns:dei="https://example.com/dei"',1)
    elif change=='foreign_cover':url=url.replace('/1306965/','/1306966/')
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
    elif change=='ambiguous_form':raw=raw.replace(b'</body>',b'<ix:nonNumeric name="dei:DocumentType" contextRef="c-4">20-F</ix:nonNumeric></body>')
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


def test_same_period_amendment_is_not_hidden_and_today_remains_unavailable():
    f=inline.merge(META['facts'],parse(),available_by=NOW.date());c=deepcopy(META['company'])
    recent=c['filings']['recent'];i=recent['accessionNumber'].index(META['documents']['current']['accession'])
    recent['filingDate'][i]='2026-10-14';recent['accessionNumber'][i]='0001306965-26-999999';recent['form'][i]='6-K/A'
    client=Client()
    with pytest.raises(AssertionError,match='999999'):acquire(facts=f,company=c,client=client)
    assert len(client.calls)==1
    client=Client();assert acquire(facts=f,company=c,now=NOW.replace(day=14),client=client) is None and client.calls==[]


def test_stale_and_conflicting_income_do_not_create_false_metrics():
    with pytest.raises(sec.SecFundamentalsCoverageError):project(now=datetime(2027,3,1,tzinfo=timezone.utc))
    f=inline.merge(META['facts'],parse(),available_by=NOW.date())
    rows=f['facts']['ifrs-full']['ProfitLossAttributableToOwnersOfParent']['units']['USD']
    row=next(r for r in rows if r.get('start')=='2026-01-01' and r['end']=='2026-06-30')
    rows.extend([{**row,'filed':'2026-10-14'},{**row,'filed':'2026-10-14','val':row['val']+1}])
    result=project(facts=f,supplement=False)
    assert 'net_margin' not in result and 'roe' not in result


def test_policy_version_scope_and_no_fmp_mutation(monkeypatch):
    from app.services import sec_currency_ratios as currency,sec_reporting_policy as policy,sec_fundamentals_preparation as prep
    assert currency.basis('OTHER','1') is None
    with pytest.raises(ValueError):currency.basis('SHEL','1')
    assert prep.version_for_symbol('SHEL')==prep.VERSION+'_'+nvs.VERSION
    assert prep.version_for_symbol('AAPL')==prep.VERSION
    assert not policy.periodic_document('SHEL',nvs.CIK,'6-K','2026-10-07','outlook.htm')
    assert not policy.periodic_document('SHEL',nvs.CIK,'20-F','2026-06-30','annual.htm')
    assert policy.periodic_document('SHEL',nvs.CIK,'6-K','2026-09-30','new.htm')
    monkeypatch.setenv('FUNDAMENTALS_PROVIDER','fmp')
    import os
    project();assert os.environ['FUNDAMENTALS_PROVIDER']=='fmp'
