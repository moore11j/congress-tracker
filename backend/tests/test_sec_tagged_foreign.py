"""Shared filing qualification, complete originals and source-to-consumer checks."""
from copy import deepcopy
from datetime import date,datetime,timezone,timedelta
from pathlib import Path
import gzip,hashlib,json,socket
import pytest
from app.services import sec_tagged_foreign as tagged,sec_fundamentals as sec,sec_inline_financials as inline

ROOT=Path(__file__).parent/'fixtures/sec_tagged_foreign'
EXPECTED=json.loads((ROOT/'expected.json').read_text())
NOW=datetime(2026,10,17,2,20,tzinfo=timezone.utc)
AVAILABLE=date(2026,10,15)


def originals(symbol):
    manifest=json.loads((ROOT/'sources.json').read_text());urls={}
    for name,meta in manifest.items():
        if not name.startswith(symbol+'-'):continue
        raw=gzip.decompress((ROOT/(name+'.gz')).read_bytes())
        assert hashlib.sha256(raw).hexdigest()==meta['sha256'];urls[meta['url']]=raw
    facts=urls[manifest[symbol+'-facts.json']['url']];company=urls[manifest[symbol+'-company.json']['url']]
    return facts,company,str(json.loads(company)['cik']).zfill(10),urls


class Client:
    issuer_document_urls=frozenset()
    def __init__(self,urls):self.urls=urls;self.calls=[]
    def get(self,url):self.calls.append(url);return self.urls[url]


@pytest.mark.parametrize('symbol',list(EXPECTED))
def test_complete_original_acquisition_projection_panels_and_repeat(symbol,monkeypatch):
    from app.services import sec_financial_statements as statements
    monkeypatch.setattr(socket.socket,'connect',lambda *a,**k:pytest.fail('Unexpected network'))
    facts,company,cik,urls=originals(symbol);client=Client(urls)
    supplement=inline.acquire(facts,company,symbol=symbol,cik=cik,observed_at=NOW,client=client,include_debt_precision=True)
    assert len(client.calls)==1 and client.calls[0]==supplement['evidence']['url']
    projected=sec.project_fundamentals(facts,company,symbol=symbol,cik=cik,observed_at=NOW,filing_supplement=supplement)
    proof=json.loads(projected['source_evidence_json'])
    assert {k:projected[k] for k in proof['calculations']}==EXPECTED[symbol]['metrics']
    assert proof['inline_filing_supplement']['standard_foreign_reporting']['methodology']==tagged.VERSION
    panel=statements.project(facts,company,symbol=symbol,cik=cik,observed_at=NOW,filing_supplement=supplement)
    assert len(panel['annual'])==EXPECTED[symbol]['annual']
    assert len(panel['quarterly'])==EXPECTED[symbol]['quarterly']
    assert all(row['eps'] is None and row['currency']=='USD' for row in panel['annual']+panel['quarterly'])
    assert 'verified listed-share basis' in panel['reportingBasis']
    assert panel==statements.project(facts,company,symbol=symbol,cik=cik,observed_at=NOW,filing_supplement=supplement)
    # Source amounts for the current revenue calculation must appear in an
    # independently parsed original current or annual statement.
    parsed=[]
    manifest=json.loads((ROOT/'sources.json').read_text())
    for name,meta in manifest.items():
        if name.startswith(symbol+'-') and name.endswith('.htm'):
            parsed.append(inline.parse(urls[meta['url']],cik=cik,accession=meta['accession'],filed=meta['filed'],report_date=meta['end'],form=meta['form'],url=meta['url']))
    for point in proof['current']['revenue']['inputs']:
        matches=[row for original in parsed for row in original['facts'].get(point['tag'],{}).get('units',{}).get(point['unit'],[]) if row.get('start')==point['start'] and row['end']==point['end']]
        assert any(row['val']==point['value'] for row in matches)


@pytest.mark.parametrize('symbol',list(EXPECTED))
def test_actual_preparation_repeat_and_no_canonical_writes(symbol,monkeypatch):
    from unittest.mock import patch
    from sqlalchemy import create_engine,select,func
    from sqlalchemy.orm import sessionmaker
    from app.models import InsightsSnapshot
    from app.services import sec_fundamentals_preparation as prep
    facts,company,cik,urls=originals(symbol);client=Client(urls)
    class Clock(datetime):
        @classmethod
        def now(cls,tz=None):return NOW
    engine=create_engine('sqlite://');InsightsSnapshot.__table__.create(engine);factory=sessionmaker(bind=engine)
    monkeypatch.setenv('SEC_FUNDAMENTALS_WARMING_ENABLED','1')
    monkeypatch.setattr(socket.socket,'connect',lambda *a,**k:pytest.fail('Unexpected network'))
    with patch.object(prep,'SessionLocal',factory),patch.object(prep,'datetime',Clock),patch.object(prep,'DirectSourceClient',lambda:client),patch.object(prep.sec_directory,'directory',lambda:{symbol:{'cik':cik}}):
        first=prep.prepare(symbol)
        assert first['values']==EXPECTED[symbol]['metrics'] and first['status']=='partial'
        count=len(client.calls);assert prep.prepare(symbol)==first and len(client.calls)==count
        with factory() as db:assert db.scalar(select(func.count()).select_from(InsightsSnapshot))==1
    engine.dispose()


def synthetic():
    from test_sec_fundamentals import sources
    facts,company=sources();company['tickers']=['GENERIC'];directory={}
    for content in facts['facts']['us-gaap'].values():
        for row in content['units']['USD']:
            form='20-F' if row['form']=='10-K' else '6-K'
            key=(row['end'],row['filed'],form)
            if key not in directory:directory[key]=f'0000000001-26-{len(directory)+1:06d}'
            row.update(form=form,accn=directory[key])
    company['filings']={'recent':{k:[] for k in ['accessionNumber','form','filingDate','reportDate','primaryDocument']}}
    recent=company['filings']['recent']
    for (end,filed,form),acc in directory.items():
        for k,v in {'accessionNumber':acc,'form':form,'filingDate':filed,'reportDate':end,'primaryDocument':end+'.htm'}.items():recent[k].append(v)
    return facts,company


def plan(facts,company,available=AVAILABLE):
    return tagged.plan(facts,company,symbol=company['tickers'][0],cik='1',available_by=available)


def test_no_symbol_allowlist_and_no_input_mutation():
    facts,company=synthetic();before=deepcopy((facts,company));result=plan(facts,company)
    assert result['current']['report_date']=='2026-06-30'
    assert (facts,company)==before
    company['tickers']=['DIFFERENT'];assert plan(facts,company)==result


@pytest.mark.parametrize('defect',['wrong_cik','shape','ambiguous','newer_unqualified'])
def test_identity_ambiguity_and_new_unqualified_financial_facts_are_held(defect):
    facts,company=synthetic();recent=company['filings']['recent']
    current=plan(facts,company)['current']
    if defect=='wrong_cik':facts['cik']=2
    elif defect=='shape':recent['reportDate'].pop()
    elif defect=='newer_unqualified':
        tag='NetIncomeLoss';rows=facts['facts']['us-gaap'][tag]['units']['USD'];row=deepcopy(rows[-1]);row.update(accn='0000000001-26-999999',filed='2026-09-01');rows.append(row)
    else:
        i=recent['accessionNumber'].index(current['accession'])
        for key in recent:recent[key].append(recent[key][i])
        recent['accessionNumber'][-1]='0000000001-26-999999'
        for content in facts['facts']['us-gaap'].values():
            content['units']['USD'] += [{**x,'accn':'0000000001-26-999999'} for x in content['units']['USD'] if x['accn']==current['accession']]
    with pytest.raises(tagged.TaggedForeignError):plan(facts,company)


@pytest.mark.parametrize('defect',['currency','stale','no_balance','no_duration','event_only','unsafe_filename','same_day'])
def test_unqualified_sources_do_not_become_current_financials(defect):
    facts,company=synthetic();recent=company['filings']['recent'];available=AVAILABLE
    if defect=='currency':
        for content in facts['facts']['us-gaap'].values():content['units']['EUR']=content['units'].pop('USD')
    elif defect=='stale':available=date(2027,10,15)
    elif defect=='no_balance':
        for tag in tagged.BALANCE:facts['facts']['us-gaap'].pop(tag,None)
    elif defect=='no_duration':
        for tag in tagged.DURATION:facts['facts']['us-gaap'].pop(tag,None)
    elif defect=='event_only':facts['facts']['us-gaap']={}
    elif defect=='unsafe_filename':recent['primaryDocument']=['../evil.htm']*len(recent['form'])
    else:available=date(2026,7,31)
    assert plan(facts,company,available) is None


def test_new_event_only_filing_does_not_replace_current_statement():
    facts,company=synthetic();before=plan(facts,company);recent=company['filings']['recent']
    for k,v in {'accessionNumber':'0000000001-26-999999','form':'6-K','filingDate':'2026-10-01','reportDate':'2026-10-01','primaryDocument':'press.htm'}.items():recent[k].append(v)
    assert plan(facts,company)==before


def test_current_domestic_reporting_retains_existing_path():
    facts,company=synthetic();recent=company['filings']['recent']
    for k,v in {'accessionNumber':'0000000001-26-999999','form':'10-Q','filingDate':'2026-10-01','reportDate':'2026-06-30','primaryDocument':'domestic.htm'}.items():recent[k].append(v)
    assert plan(facts,company) is None


@pytest.mark.parametrize('defect',['absent','hash','accession','currency','policy'])
def test_source_proof_is_required_and_bound_to_current_selection(defect):
    facts,company,cik,urls=originals('FLNG');client=Client(urls)
    supplement=inline.acquire(facts,company,symbol='FLNG',cik=cik,observed_at=NOW,client=client)
    if defect=='absent':supplement=None
    elif defect=='hash':supplement['evidence']['sha256']='invalid'
    elif defect=='policy':supplement['evidence']['standard_foreign_reporting']['current']['report_date']='2026-03-31'
    else:supplement['evidence'][defect]='wrong'
    with pytest.raises(tagged.TaggedForeignError):
        sec.project_fundamentals(facts,company,symbol='FLNG',cik=cik,observed_at=NOW,filing_supplement=supplement)


@pytest.mark.parametrize('value',['June 30 2026','June 30, 2026','Jun. 30, 26','JUNE 30 2026'])
def test_standard_english_date_separators(value):
    from test_sec_inline_financials import document,parse
    raw=document().replace(b'name="dei:DocumentPeriodEndDate"',b'name="dei:DocumentPeriodEndDate" format="ixt:date-monthname-day-year-en"').replace(b'>2026-06-30</ix:nonNumeric>',('>'+value+'</ix:nonNumeric>').encode())
    assert parse(raw)['evidence']['report_date']=='2026-06-30'


@pytest.mark.parametrize('value',['June 31 2026','June 30','2026 June 30','Bogus 30 2026'])
def test_invalid_dates_are_typed_holds(value):
    from test_sec_inline_financials import document,parse
    raw=document().replace(b'name="dei:DocumentPeriodEndDate"',b'name="dei:DocumentPeriodEndDate" format="ixt:date-monthname-day-year-en"').replace(b'>2026-06-30</ix:nonNumeric>',('>'+value+'</ix:nonNumeric>').encode())
    with pytest.raises(inline.InlineFactsError):parse(raw)
