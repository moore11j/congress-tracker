from datetime import datetime,timezone,timedelta
import json
import pytest
from sqlalchemy import create_engine,select,func
from sqlalchemy.orm import sessionmaker
from app.db import Base
from app.models import Security,InsightsSnapshot,Event,ResearchSourceDocument,FundamentalsCache
from app.services import sec_press_releases as press
from app.services.direct_feed_store import DirectFeedDocument,DirectFeedRevision
from app.services.sec_earnings_store import stage_earnings_material
from app.services.issuer_financial_staging import prepare_staged_financials,KEY,ACCESSION
from test_issuer_release_financials import release

NOW=datetime(2026,10,12,tzinfo=timezone.utc)


def originals():
    company=json.dumps({'cik':866787,'tickers':['AZO'],'filings':{'recent':{
        'accessionNumber':[ACCESSION],'filingDate':['2026-09-22'],'form':['8-K'],
        'primaryDocument':['report.htm'],'items':['2.02,9.01'],
        'acceptanceDateTime':['2026-09-22T14:55:14Z']}}}).encode()
    body=release().decode().replace('</p>', ' reported net sales for its fourth quarter ended August 29, 2026. EPS increased.</p>',1)
    body=body.replace('<table>', '<p>Condensed Consolidated Statements of Operations</p><table>',1)
    body += '<p>' + 'Additional company financial disclosures. '*20 + '</p>'
    submission=f'''<SEC-HEADER>
<ACCEPTANCE-DATETIME>20260922065514
ACCESSION NUMBER: {ACCESSION}
CONFORMED SUBMISSION TYPE: 8-K
FILED AS OF DATE: 20260922
CENTRAL INDEX KEY: 0000866787
ITEM INFORMATION: Results of Operations and Financial Condition
</SEC-HEADER>
<DOCUMENT>
<TYPE>8-K
<FILENAME>report.htm
<TEXT><html><a href="exh_991.htm">Press Release</a></html></TEXT>
</DOCUMENT>
<DOCUMENT>
<TYPE>EX-99.1
<FILENAME>exh_991.htm
<TEXT>{body}</TEXT>
</DOCUMENT>'''.encode()
    index=f'''<html>CIK: 0000866787<a href="{ACCESSION}.txt">Complete submission</a>
<div class="infoHead">Filing Date</div><div>2026-09-22</div>
<div class="infoHead">Accepted</div><div>2026-09-22 06:55:14</div></html>'''.encode()
    return company,submission,index


@pytest.fixture
def scenario(monkeypatch):
    engine=create_engine('sqlite://')
    Base.metadata.create_all(engine)
    factory=sessionmaker(bind=engine)
    monkeypatch.setattr(press,'SessionLocal',factory)
    monkeypatch.setenv('SEC_EARNINGS_WARMING_ENABLED','1')
    with factory() as db:
        db.add(Security(id=1,symbol='AZO',name='AutoZone',asset_class='stock'));db.commit()
    yield factory
    engine.dispose()


def stage(db, raws=None):
    company,submission,index=raws or originals()
    return stage_earnings_material(db,company_raw=company,submission_raw=submission,index_raw=index,
        symbol='AZO',cik='0000866787',accession=ACCESSION)


def test_existing_collector_prepares_once_with_no_extra_source_calls(scenario):
    raws=originals(); calls=[]
    class Client:
        def get(self,url):
            calls.append(url)
            return raws[0] if url.endswith('.json') else raws[2] if url.endswith('-index.html') else raws[1]
    def refresh(): return press.refresh_sec_releases('AZO',client=Client(),directory={'AZO':{'cik':866787}},preparation_only=True)
    assert len(refresh()['items'])==1
    with scenario() as db:
        snapshot=db.get(InsightsSnapshot,KEY)
        before=(snapshot.payload_json,snapshot.fetched_at)
        value=json.loads(snapshot.payload_json)
        assert value['values']['revenue_growth']==100
        assert value['source_bundle']['acceptance_evidence']['submissions_comparison']=='conflict'
        assert value['publication_eligible'] is False
    assert len(refresh()['items'])==1 and len(calls)==6
    with scenario() as db:
        snapshot=db.get(InsightsSnapshot,KEY)
        assert (snapshot.payload_json,snapshot.fetched_at)==before
        assert db.scalar(select(func.count()).select_from(DirectFeedDocument))==3
        assert db.scalar(select(func.count()).select_from(DirectFeedRevision))==3
        assert db.scalar(select(func.count()).select_from(InsightsSnapshot))==1
        for model in (Event,ResearchSourceDocument,FundamentalsCache):
            assert db.scalar(select(func.count()).select_from(model))==0


@pytest.mark.parametrize('tamper',['raw','parsed','metadata','security'])
def test_source_tampering_cannot_create_financial_receipt(scenario,tamper):
    with scenario() as db:
        doc=stage(db); db.flush()
        if tamper=='raw': db.scalar(select(DirectFeedRevision).where(DirectFeedRevision.document_id==doc.id)).source_bytes=b'changed'
        elif tamper=='parsed': doc.parsed_json='{}'
        elif tamper=='metadata':
            metadata=json.loads(doc.metadata_json);metadata['key']='wrong';doc.metadata_json=json.dumps(metadata)
        else: db.get(Security,1).symbol='OTHER'
        with pytest.raises(ValueError): prepare_staged_financials(db,doc.id,security_id=1,observed_at=NOW)
        assert db.get(InsightsSnapshot,KEY) is None


def test_observation_waits_for_later_conflicting_acceptance_clock(scenario):
    with scenario() as db:
        doc=stage(db)
        with pytest.raises(ValueError,match='precedes acceptance'):
            prepare_staged_financials(db,doc.id,security_id=1,observed_at=datetime(2026,9,22,12,tzinfo=timezone.utc))
        result=prepare_staged_financials(db,doc.id,security_id=1,observed_at=NOW)
        assert result['status']=='prepared'
        assert prepare_staged_financials(db,doc.id,security_id=1,observed_at=NOW+timedelta(minutes=5))==result


def test_financial_format_hold_preserves_release_evidence(scenario):
    with scenario() as db:
        company,submission,index=originals()
        doc=stage(db,(company,submission.replace(b'in thousands',b'in millions'),index))
        assert json.loads(doc.parsed_json)['status']=='prepared'
        result=prepare_staged_financials(db,doc.id,security_id=1,observed_at=NOW)
        assert result['status']=='held' and result['values']=={} and not result['publication_eligible']


@pytest.mark.parametrize('removed',[b'fourth quarter',b'Condensed Consolidated Statements of Operations'])
def test_sales_update_without_earnings_context_remains_held(scenario,removed):
    with scenario() as db:
        company,submission,index=originals()
        doc=stage(db,(company,submission.replace(removed,b'monthly update'),index))
        assert json.loads(doc.parsed_json)['status']=='held'
        assert prepare_staged_financials(db,doc.id,security_id=1,observed_at=NOW)['status']=='held'


def test_only_reviewed_issuer_cache_requires_refresh(scenario):
    now=datetime.now(timezone.utc)
    payload={'items':[], 'status':'ok', 'provider':press.PROVIDER, 'cache_version':press.CACHE_VERSION}
    with scenario() as db:
        for symbol in ('AZO','OTHER'):
            press._save_payload(db,symbol,payload,now)
        db.commit()
    assert press._cached('AZO') is None
    assert press._cached('OTHER')==payload
    with scenario() as db:
        press._save_payload(db,'AZO',{**payload,'cache_version':press._cache_version('AZO')},now)
        db.commit()
    assert press._cached('AZO') is not None


def test_existing_canonical_financials_and_research_are_unchanged(scenario):
    with scenario() as db:
        db.add(FundamentalsCache(symbol='AZO',provider='fmp',fetched_at=NOW,revenue_growth=7.2))
        db.add(ResearchSourceDocument(id='existing',security_id=1,document_type='press_release',
            source_provider='fmp',external_id='azo-existing',title='Existing release',
            content_hash='original',processing_version='v1',processing_status='processed'))
        db.commit()
        before={model.__tablename__:list(db.execute(select(model.__table__)).mappings())
                for model in (FundamentalsCache,ResearchSourceDocument)}
        doc=stage(db)
        assert prepare_staged_financials(db,doc.id,security_id=1,observed_at=NOW)['status']=='prepared'
        db.commit()
        for model in (FundamentalsCache,ResearchSourceDocument):
            assert list(db.execute(select(model.__table__)).mappings())==before[model.__tablename__]
        assert db.scalar(select(func.count()).select_from(Event))==0


def test_changed_invalid_revision_clears_previously_prepared_values(scenario):
    with scenario() as db:
        doc=stage(db)
        first=prepare_staged_financials(db,doc.id,security_id=1,observed_at=NOW)
        assert first['values']
        company,submission,index=originals()
        changed=stage(db,(company,submission.replace(b'in thousands',b'in millions'),index))
        assert changed.id==doc.id
        held=prepare_staged_financials(db,doc.id,security_id=1,observed_at=NOW+timedelta(hours=1))
        assert held['status']=='held' and held['values']=={}
        assert held['source_bundle']['submission_sha256']!=first['source_bundle']['submission_sha256']
        assert json.loads(db.get(InsightsSnapshot,KEY).payload_json)==held
        assert db.scalar(select(func.count()).select_from(InsightsSnapshot))==1
