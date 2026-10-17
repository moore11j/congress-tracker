"""Every recovered source class: primary and linked financial originals."""
from pathlib import Path
from datetime import datetime,timezone
from copy import deepcopy
import gzip,hashlib,json,socket
from unittest.mock import patch
import pytest
from app.services import sec_inline_financials as inline,sec_fundamentals as sec,sec_financial_statements as statements

ROOT=Path(__file__).parent/'fixtures/sec_document_sets/cohort'
MANIFEST=json.loads((ROOT/'sources.json').read_text())
EXPECTED=json.loads((ROOT/'expected.json').read_text())
NOW=datetime(2026,10,17,2,20,tzinfo=timezone.utc)

def originals(symbol):
    urls={};raws={}
    for name,meta in MANIFEST.items():
        if meta['symbol']!=symbol:continue
        raw=gzip.decompress((ROOT/(name+'.gz')).read_bytes())
        assert hashlib.sha256(raw).hexdigest()==meta['sha256']
        urls[meta['url']]=raw;raws[name]=raw
    facts,company=raws[symbol+'-facts.json'],raws[symbol+'-company.json']
    return facts,company,str(json.loads(company)['cik']).zfill(10),urls

class Client:
    issuer_document_urls=frozenset()
    def __init__(self,urls):self.urls=urls;self.calls=[]
    def get(self,url):self.calls.append(url);return self.urls[url]

@pytest.mark.parametrize('symbol',list(EXPECTED))
def test_every_recovered_source_projection_cache_and_history(symbol,monkeypatch):
    from sqlalchemy import create_engine,select,func
    from sqlalchemy.orm import sessionmaker
    from app.models import InsightsSnapshot
    from app.services import sec_fundamentals_preparation as prep
    monkeypatch.setattr(socket.socket,'connect',lambda *a,**k:pytest.fail('Unexpected network'))
    facts,company,cik,urls=originals(symbol);client=Client(urls);wanted=EXPECTED[symbol]
    supplement=inline.acquire(facts,company,symbol=symbol,cik=cik,observed_at=NOW,client=client)
    assert bool(supplement['evidence'].get('unindexed_primary_discovery'))==wanted['primary']
    assert bool(supplement['evidence'].get('document_set_discovery'))==wanted['document_set']
    assert supplement['evidence']['document_set_discovery_limit']['statement_complete'] is False
    value=sec.project_fundamentals(facts,company,symbol=symbol,cik=cik,observed_at=NOW,filing_supplement=supplement)
    assert {k:value[k] for k in json.loads(value['source_evidence_json'])['calculations']}==wanted['metrics']
    assert value==sec.project_fundamentals(facts,company,symbol=symbol,cik=cik,observed_at=NOW,filing_supplement=supplement)
    panel=statements.project(facts,company,symbol=symbol,cik=cik,observed_at=NOW,filing_supplement=supplement)
    assert (len(panel['annual']),len(panel['quarterly']))==(wanted['annual'],wanted['quarterly'])
    assert all(row['eps'] is None for row in panel['annual']+panel['quarterly'])
    class Clock(datetime):
        @classmethod
        def now(cls,tz=None):return NOW
    engine=create_engine('sqlite://');InsightsSnapshot.__table__.create(engine);factory=sessionmaker(bind=engine)
    monkeypatch.setenv('SEC_FUNDAMENTALS_WARMING_ENABLED','1');client=Client(urls)
    with patch.object(prep,'SessionLocal',factory),patch.object(prep,'datetime',Clock),patch.object(prep,'DirectSourceClient',lambda:client),patch.object(prep.sec_directory,'directory',lambda:{symbol:{'cik':cik}}):
        first=prep.prepare(symbol);assert first['values']==wanted['metrics']
        count=len(client.calls);assert prep.prepare(symbol)==first and len(client.calls)==count
        with factory() as db:assert db.scalar(select(func.count()).select_from(InsightsSnapshot))==1
    engine.dispose()

@pytest.mark.parametrize('symbol',['SLGL','TSEM'])
def test_identity_in_exhibit_is_validated_after_cover_only_attempt(symbol):
    facts,company,cik,urls=originals(symbol)
    result=inline.acquire(facts,company,symbol=symbol,cik=cik,observed_at=NOW,client=Client(urls))
    proof=result['evidence'];cover=proof['document_set_discovery']['cover_url']
    with pytest.raises(inline.InlineFactsError,match='filing_identity'):
        inline.parse(urls[cover],cik=cik,accession=proof['accession'],filed=proof['filed'],report_date=proof['report_date'],form=proof['form'],url=cover)
    assert proof['cik']==cik and proof['reported_facts']>0

@pytest.mark.parametrize('field',['cover_url','cik','accession','filed','report_date','form','directory_sha256'])
def test_primary_discovery_proof_cannot_be_substituted(field):
    facts,company,cik,urls=originals('ASC')
    result=inline.acquire(facts,company,symbol='ASC',cik=cik,observed_at=NOW,client=Client(urls))
    result['evidence']['unindexed_primary_discovery'][field]='invalid'
    with pytest.raises(ValueError):sec.project_fundamentals(facts,company,symbol='ASC',cik=cik,observed_at=NOW,filing_supplement=result)

CONSUMERS=[(s,m) for s,w in EXPECTED.items() for m in ('current_ratio','roe','revenue_growth') if m in w['metrics']]
@pytest.mark.parametrize('symbol,metric',CONSUMERS)
def test_all_recovered_watchlist_consumers(symbol,metric,monkeypatch):
    import test_sec_tagged_foreign_consumers as consumer
    monkeypatch.setattr(consumer,'originals',originals)
    monkeypatch.setattr(consumer,'EXPECTED',EXPECTED)
    monkeypatch.setattr(consumer,'NOW',NOW)
    consumer.test_original_source_score_and_alert_recovery_repeat_crossing_rollback(symbol,metric,monkeypatch)

def test_older_foreign_candidate_never_displaces_completed_domestic_report():
    from datetime import date
    from app.services import sec_tagged_foreign as tagged
    facts,company,cik,urls=originals('AER');value=json.loads(company)
    recent=value['filings']['recent']
    # An issuer has transitioned to a completed domestic report after every
    # foreign candidate. Discovery must make no requests for old 6-Ks.
    recent['accessionNumber'].insert(0,'0001378789-26-999999')
    recent['form'].insert(0,'10-Q');recent['filingDate'].insert(0,'2026-10-15')
    recent['reportDate'].insert(0,'2026-10-15');recent['primaryDocument'].insert(0,'domestic.htm')
    class Denied:
        def get(self,url):pytest.fail('Old foreign filing displaced domestic report')
    assert tagged._document_set(json.loads(facts),value,json.dumps(value).encode(),symbol='AER',cik=cik,available_by=date(2026,10,15),client=Denied()) is None
