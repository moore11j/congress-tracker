"""Company-statement work must not consume slots for fund symbols."""
from datetime import datetime,timedelta,timezone
from pathlib import Path
import gzip,hashlib,json

import pytest
import requests
from sqlalchemy import select
from app.db import Base
from app.models import FundamentalsCache,InsightsSnapshot
from app.jobs import warm_sec_fundamentals as job
from app.services import sec_fundamentals_preparation as prep,data_enrichment_queue as queue,finnhub_metrics
from app.services.fundamentals_snapshots import snapshot_symbol_rejection_reason
from app.utils.symbols import classify_symbol
from test_sec_fundamentals_warming import factory


def original(name):
    root=Path(__file__).parent/'fixtures/sec_fund_queue_nov1'
    source=json.loads((root/'manifest.json').read_bytes())[name]
    raw=gzip.decompress((root/source['file']).read_bytes())
    assert len(raw)==source['bytes'] and hashlib.sha256(raw).hexdigest()==source['sha256']
    return json.loads(raw)


def test_existing_financial_rule_against_complete_official_directories():
    funds=original('funds');companies=original('equities')
    assert funds['fields']==['cik','seriesId','classId','symbol']
    assert companies['fields']==['cik','name','ticker','exchange']
    fund_symbols={row[3] for row in funds['data']}
    for symbol in ('ABALX','VFIAX','VMFXX'):
        assert symbol in fund_symbols
        assert snapshot_symbol_rejection_reason(symbol)==(symbol,'likely_mutual_fund')
        # The global pricing/event symbol behavior remains unchanged.
        assert classify_symbol(symbol)[0]=='eligible'
    # The company directory also contains unlisted investment-fund filers.
    # Their presence does not make a fund ticker an exchange-listed company.
    conflicts=[row[2] for row in companies['data'] if row[3] and job.SNAPSHOT_MUTUAL_FUND_LIKE_RE.fullmatch(row[2])]
    assert conflicts==[],conflicts
    assert all(not job.SNAPSHOT_MUTUAL_FUND_LIKE_RE.fullmatch(s) for s in ('NFLX','RELX','BRK-B','BNTX'))


def canonical_rows(db):
    return {table.name:[dict(row) for row in db.execute(select(table)).mappings()]
        for table in Base.metadata.sorted_tables if table.name!='insights_snapshots'}


@pytest.mark.parametrize('broad',[False,True])
def test_funds_release_sec_slots_preserve_universe_metrics_and_all_canonical_rows(factory,monkeypatch,broad):
    now=datetime(2026,11,1,23,40,tzinfo=timezone.utc)
    class Clock(datetime):
        @classmethod
        def now(cls,tz=None):return now
    monkeypatch.setattr(job,'datetime',Clock)
    monkeypatch.setenv('SEC_FUNDAMENTALS_UNIVERSE_WARMING_ENABLED','1' if broad else '0')
    monkeypatch.setenv('SEC_FUNDAMENTALS_WARMING_BATCH_SIZE','16')
    monkeypatch.setenv('FINNHUB_METRICS_WARMING_ENABLED','1')
    monkeypatch.setenv('FINNHUB_METRICS_BATCH_SIZE','10')
    monkeypatch.setattr(queue,'DEFAULT_PREWARM_SYMBOLS',['ABALX','NFLX'])
    monkeypatch.setattr(requests.sessions.Session,'request',lambda *a,**k:pytest.fail('Unexpected external request'))
    old=(now-timedelta(days=2)).isoformat()
    state={'attempted_at':{'ABALX':old},'attempted_versions':{'ABALX':'retained-version'},
           'scope_refresh_after':{'ABALX':old},'metric_attempted_at':{'ABALX':old}}
    with factory() as db:
        db.add_all([FundamentalsCache(symbol=s,provider='fmp',status='ok',fetched_at=now,
            company_name=s,price=123.45,roe=25,source_evidence_json='{"retained":"unchanged"}')
            for s in ('ABALX','VFIAX','VMFXX','AAPL','BRK-B','NFLX')])
        db.add(InsightsSnapshot(kind=job.KEY,source='free_direct',fetched_at=now,payload_json=json.dumps(state)))
        # Retain a previously prepared fund record as history, without refreshing it.
        db.add(InsightsSnapshot(kind='sec-current-fundamentals:ABALX:v1',source='sec_edgar',fetched_at=now-timedelta(days=2),payload_json='{"reason":"retained_history"}'))
        db.commit();before=canonical_rows(db)
        fund_before=db.get(InsightsSnapshot,'sec-current-fundamentals:ABALX:v1').payload_json
    sec_calls=[];metric_calls=[]
    def prepare(symbol,**kwargs):
        sec_calls.append(symbol)
        return dict(status='partial',source='sec_edgar',values={},observed_at=now.isoformat())
    def metric(symbol):
        metric_calls.append(symbol)
        return dict(status='unavailable',values={},observed_at=now.isoformat())
    monkeypatch.setattr(prep,'prepare',prepare);monkeypatch.setattr(finnhub_metrics,'prepare',metric)
    result=job.run()
    expected_funds=['ABALX','VFIAX','VMFXX'] if broad else ['ABALX']
    expected_stocks={'AAPL','BRK-B','NFLX'} if broad else {'NFLX'}
    assert set(sec_calls)==expected_stocks and len(sec_calls)==len(expected_stocks)
    assert set(metric_calls)==set(expected_funds)|expected_stocks
    assert result['excluded_sec_scopes']==expected_funds
    assert result['universe_size']==len(expected_funds)+len(expected_stocks)
    assert result['completed_scopes']==result['planned_scopes']==len(expected_stocks)
    assert result['canonical_writes']==result['emails']==0
    for _ in range(2):
        with factory() as db:
            current=json.loads(db.get(InsightsSnapshot,job.KEY).payload_json)
            for field in ('attempted_at','attempted_versions','scope_refresh_after'):
                assert current[field]['ABALX']==state[field]['ABALX']
            assert db.get(InsightsSnapshot,'sec-current-fundamentals:ABALX:v1').payload_json==fund_before
            assert canonical_rows(db)==before
        again=job.run()
        assert again['excluded_sec_scopes']==expected_funds
        assert again['universe_size']==result['universe_size']
    assert not set(sec_calls)&set(expected_funds)
    assert len(metric_calls)==len(expected_funds)+len(expected_stocks)
