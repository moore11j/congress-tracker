"""Full original/received documents, downstream projection and tamper boundaries."""
from copy import deepcopy
from datetime import datetime,timezone,timedelta
import gzip,hashlib,importlib,json,socket,types
from pathlib import Path
from unittest.mock import patch
import pytest
from app.services import sec_original_integrity as integrity,sec_inline_financials as inline,sec_fundamentals as sec
ROOT=Path(__file__).parent/'fixtures'
OBSERVED=json.loads((ROOT/'sec_delivery_additional_observations.json').read_text())['sources']
SYMBOLS=('asml','sap','tsm','cnq','mga','ul','spcx')
NOW=datetime(2026,10,15,22,45,tzinfo=timezone.utc)
def module(symbol):
    return importlib.import_module('app.services.sec_'+('registration_history' if symbol=='spcx' else symbol+'_financials'))
def original(row):
    return gzip.decompress((ROOT/'sec_delivery_additional'/f"{row['symbol']}-{row['key']}.htm.gz").read_bytes())
def received(row):
    raw=original(row);assert hashlib.sha256(raw).hexdigest()==row['sha256']
    match=integrity._TERMINAL.search(raw);a,b=match.span('path')
    result=raw[:a]+row['delivery_path'].encode()+raw[b:]
    assert len(result)==row['received_bytes'] and hashlib.sha256(result).hexdigest()==row['received_sha256']
    return result
def parse(row,raw):
    m=module(row['symbol'])
    if row['symbol']=='asml':return m.parse(raw,end=row['key'])
    if row['symbol']=='ul':return m.parse_html(raw,source_key=row['key'])
    if row['symbol']=='spcx':return m.parse(raw)
    return m.parse(raw,source_key=row['key'])
def source(row):
    m=module(row['symbol']);return m.ORIGINAL if row['symbol']=='spcx' else m.SOURCES[row['key']]
def numbers(value):
    if isinstance(value,dict):return {k:numbers(v) for k,v in value.items() if k not in ('source_sha256','sha256','_original')}
    if isinstance(value,list):return [numbers(v) for v in value]
    return value
@pytest.mark.parametrize('row',OBSERVED,ids=lambda r:r['symbol']+'_'+r['key'])
def test_complete_received_parser_parity_and_authenticated_receipt(row):
    raw=received(row);proof=integrity.verified_original(raw,source(row))
    assert proof['source_sha256']==row['received_sha256']
    assert proof['reviewed_source_sha256']==row['sha256']
    assert proof['document_sha256']==row['document_sha256']
    old,new=parse(row,original(row)),parse(row,raw)
    assert numbers(old)==numbers(new)
    assert new==parse(row,raw)
@pytest.mark.parametrize('row',OBSERVED,ids=lambda r:r['symbol']+'_'+r['key'])
@pytest.mark.parametrize('change',('financial_bytes','missing_script','inline_script','duplicate_script','new_attribute','absolute_path','trailing_content'))
def test_non_delivery_changes_rejected(row,change):
    raw=received(row);tag=integrity._TERMINAL.search(raw).group()
    if change=='financial_bytes':raw=raw.replace(b'2025',b'2024',1)
    elif change=='missing_script':raw=raw.replace(tag,b'')
    elif change=='inline_script':raw=raw.replace(tag,tag.replace(b'</script>',b'1</script>'))
    elif change=='duplicate_script':raw=raw.replace(tag,tag+tag)
    elif change=='new_attribute':raw=raw.replace(tag,tag.replace(b' src=',b' async src='))
    elif change=='absolute_path':raw=raw.replace(row['delivery_path'].encode(),b'https://example.test/script.js')
    elif change=='trailing_content':raw+=b' '
    with pytest.raises(ValueError):integrity.verified_original(raw,source(row))
    with pytest.raises(ValueError):parse(row,raw)
def inputs(symbol):
    return tuple(gzip.decompress((ROOT/'sec_delivery_additional'/f'{symbol}-{name}.json.gz').read_bytes()) for name in ('facts','company'))
def project(symbol,rotate):
    m=module(symbol);facts,company=inputs(symbol)
    urls={r['url']:(received(r) if rotate else original(r)) for r in OBSERVED if r['symbol']==symbol}
    class Client:
        issuer_document_urls=frozenset()
        def get(self,url):return urls[url]
    kwargs=dict(symbol=symbol.upper(),cik=m.CIK,observed_at=NOW)
    bundle=inline.acquire(facts,company,client=Client(),**kwargs)
    return sec.project_fundamentals(facts,company,filing_supplement=bundle,**kwargs)
@pytest.mark.parametrize('symbol',SYMBOLS)
def test_full_source_financial_projection_and_cache_replay(symbol,monkeypatch):
    from sqlalchemy import create_engine,select
    from sqlalchemy.orm import sessionmaker
    from app.db import Base
    from app.models import InsightsSnapshot,FundamentalsCache
    from app.services import sec_fundamentals_preparation as prep
    from app.services.fundamentals_cache import fundamentals_summary_from_cache_row
    from app.services.confirmation_score import _fundamentals_context_source
    monkeypatch.setattr(socket.socket,'connect',lambda *a,**k:pytest.fail('Unexpected network request'))
    old,new=project(symbol,False),project(symbol,True)
    assert {k:v for k,v in old.items() if k!='source_evidence_json'}=={k:v for k,v in new.items() if k!='source_evidence_json'}
    assert new==project(symbol,True)
    assert '"_original":' not in new['source_evidence_json']
    m=module(symbol);facts,company=inputs(symbol)
    urls={r['url']:received(r) for r in OBSERVED if r['symbol']==symbol}
    urls.update({f'https://data.sec.gov/submissions/CIK{m.CIK}.json':company,f'https://data.sec.gov/api/xbrl/companyfacts/CIK{m.CIK}.json':facts})
    class Client:
        issuer_document_urls=frozenset()
        def get(self,url):return urls[url]
    class Clock(datetime):
        @classmethod
        def now(cls,tz=None):return NOW
    engine=create_engine('sqlite://');Base.metadata.create_all(engine);factory=sessionmaker(bind=engine)
    monkeypatch.setenv('SEC_FUNDAMENTALS_WARMING_ENABLED','1')
    with patch.object(prep,'SessionLocal',factory),patch.object(prep,'datetime',Clock),patch.object(prep,'DirectSourceClient',Client),patch.object(prep.sec_directory,'directory',return_value={symbol.upper():{'cik':m.CIK}}):
        payload=prep.prepare(symbol.upper())
        assert payload['values']=={k:new[k] for k in prep.METRICS if k in new} and payload['values']
        with patch.object(prep,'DirectSourceClient',side_effect=AssertionError('Repeated cached fetch')):
            assert prep.prepare(symbol.upper())==payload
        with factory() as db:assert not list(db.scalars(select(FundamentalsCache)))
    summary=fundamentals_summary_from_cache_row(types.SimpleNamespace(**new),now=NOW)
    assert _fundamentals_context_source(summary)
    # Real source transitions, hash-only rotation, a later crossing and rollback.
    from app.models import MonitoringAlert,Security,UserAccount,Watchlist,WatchlistItem,WatchlistAlertRule
    from app.services.custom_alert_rules import evaluate_watchlist_custom_alerts,validate_conditions
    with factory() as db:
        user=UserAccount(email=symbol+'-delivery@example.test');security=Security(symbol=symbol.upper(),name=symbol,asset_class='equity')
        db.add_all([user,security]);db.flush()
        watch=Watchlist(name='Delivery integrity',owner_user_id=user.id);db.add(watch);db.flush()
        db.add(WatchlistItem(watchlist_id=watch.id,security_id=security.id,target_type='ticker'))
        threshold=new['current_ratio']-1
        rule=WatchlistAlertRule(user_id=user.id,watchlist_id=watch.id,name='Current ratio',enabled=True,scope_type='any_watchlist_ticker',match_type='all',delivery='both',conditions_json=json.dumps(validate_conditions([dict(metric='current_ratio',operator='gt',comparison_type='value',comparison_value=threshold)])))
        row=FundamentalsCache(symbol=symbol.upper(),provider='sec_edgar',status='ok',fetched_at=NOW,source_evidence_json=json.dumps({'methodology':'sec_fundamentals_v1','financial_status':'unavailable','calculations':{}}))
        db.add_all([rule,row]);db.commit();monkeypatch.setenv('FUNDAMENTALS_PROVIDER','sec_edgar')
        def run(minutes):
            result=evaluate_watchlist_custom_alerts(db,user_id=user.id,watchlist_id=watch.id,now=NOW+timedelta(minutes=minutes));db.commit();return result
        assert run(0)['triggered']==0
        for key,value in payload['values'].items():setattr(row,key,value)
        row.source_evidence_json=old['source_evidence_json'];assert run(5)['triggered']==0
        row.source_evidence_json=new['source_evidence_json'];assert run(10)['triggered']==0;assert run(11)['triggered']==0
        row.current_ratio=threshold-1;assert run(15)['triggered']==0
        row.current_ratio=new['current_ratio'];assert run(20)['triggered']==1;assert run(25)['triggered']==0
        row.current_ratio=None;row.source_evidence_json=json.dumps({'methodology':'sec_fundamentals_v1','financial_status':'unavailable','calculations':{}})
        assert run(30)['triggered']==0
        assert len(db.scalars(select(MonitoringAlert)).all())==1
    engine.dispose()
@pytest.mark.parametrize('change',('raw_missing','raw_financial','evidence_hash','provenance_hash','projection_value','projection_period','receipt_pin'))
def test_spcx_projection_cannot_hide_tampering_in_delivery_metadata(change):
    row=next(r for r in OBSERVED if r['symbol']=='spcx');m=module('spcx');bundle=parse(row,received(row))
    assert m.projection_digest(bundle)==m.PROJECTION_SHA256
    if change=='raw_missing':bundle.pop('_original')
    elif change=='raw_financial':bundle['_original']=bundle['_original'].replace(b'2025',b'2024',1)
    elif change=='evidence_hash':bundle['evidence']['sha256']='0'*64
    elif change=='provenance_hash':next(iter(bundle['provenance'].values()))['source_sha256']='0'*64
    elif change=='projection_value':bundle['facts'][m.TAGS['revenue']]['units']['USD'][0]['val']+=1
    elif change=='projection_period':bundle['facts'][m.TAGS['revenue']]['units']['USD'][0]['end']='2024-12-31'
    elif change=='receipt_pin':bundle['evidence']['document_sha256']='0'*64
    facts,_=inputs('spcx')
    with pytest.raises(m.RegistrationHistoryError):m.validate_bridge(json.loads(facts),bundle,available_by=NOW.date())
