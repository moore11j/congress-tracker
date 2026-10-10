from copy import deepcopy
import pytest
from app.services.institutional_reference import (capture_reference, reference_identity,
    resolve_reference_candidate, reference_query)

NOW = '2026-10-10T05:25:00+00:00'
CUSIP = '30609A109'
PERIOD = '2026-09-30'


def payload():
    return {'status':'OK','request_id':'test-id','results':[{'ticker':'OKE','name':'ONEOK',
        'market':'stocks','locale':'us','currency_name':'usd','active':True,'type':'CS',
        'cik':'0001039684','share_class_figi':'BBG024TZWVS6','composite_figi':'BBG024TZWVN1',
        'primary_exchange':'XNYS','last_updated_utc':'2026-10-01T06:11:53Z'}]}


class Client:
    def __init__(self, value): self.value,self.calls=value,[]
    def get(self, path, params):
        self.calls.append((path,params))
        return deepcopy(self.value)


def capture(value=None):
    client=Client(value if value is not None else payload())
    result=capture_reference(client,cusip=CUSIP,report_period=PERIOD,observed_at=NOW)
    assert client.calls == [('/v3/reference/tickers',reference_query(CUSIP,PERIOD))]
    return result


def identity(document, **kwargs):
    return reference_identity(document,cusip=CUSIP,report_period=PERIOD,observed_by=NOW,**kwargs)


def test_one_request_preserves_identity_and_explicit_observation_without_price_data():
    doc=capture();result=identity(doc)
    assert result['status']=='verified' and result['symbol']=='OKE'
    assert result['report_period']==PERIOD and result['observed_at']==NOW
    assert result['source_availability_basis']=='reference_observed_at'
    assert result['provider_updated_at']=='2026-10-01T06:11:53+00:00'
    assert 'price' not in doc['response']['results'][0]
    assert resolve_reference_candidate(result,set())=='OKE'
    assert resolve_reference_candidate(result,{'OLD','OKE'})=='OKE'
    assert resolve_reference_candidate(result,{'UNRELATED'}) is None


def test_date_query_does_not_backdate_actual_reference_availability():
    doc=capture()
    result=reference_identity(doc,cusip=CUSIP,report_period=PERIOD,observed_by='2026-10-08T23:59:59Z')
    assert result=={'status':'unavailable','reason':'not_yet_observed'}
    assert resolve_reference_candidate(result,{'OKE'}) is None


@pytest.mark.parametrize('mutation',['cusip','date','url','provider','checksum','naive_time'])
def test_wrong_query_provenance_or_modified_evidence_is_rejected(mutation):
    doc=capture()
    if mutation=='cusip':doc['query']['cusip']='000000001'
    if mutation=='date':doc['query']['date']='2026-06-30'
    if mutation=='url':doc['source_url']='https://example.test/identity'
    if mutation=='provider':doc['provider']='sec_edgar'
    if mutation=='checksum':doc['response']['results'][0]['ticker']='WRONG'
    if mutation=='naive_time':doc['observed_at']='2026-10-10T05:25:00'
    with pytest.raises(ValueError):identity(doc)


@pytest.mark.parametrize('kind,expected',[('empty','no_reference_match'),('two','ambiguous_reference_identity'),
    ('next','ambiguous_reference_identity'),('option','unsupported_reference_security'),
    ('foreign','unsupported_reference_security'),('currency','unsupported_reference_security'),
    ('inactive','unsupported_reference_security'),('figi','unsupported_reference_security')])
def test_empty_ambiguous_or_unsupported_results_never_create_a_mapping(kind,expected):
    raw=payload()
    if kind=='empty':raw['results']=[]
    if kind=='two':raw['results'].append(deepcopy(raw['results'][0]))
    if kind=='next':raw['next_url']='https://api.massive.com/next?apiKey=not-a-real-key'
    if kind=='option':raw['results'][0]['type']='WARRANT'
    if kind=='foreign':raw['results'][0]['locale']='global'
    if kind=='currency':raw['results'][0]['currency_name']='eur'
    if kind=='inactive':raw['results'][0]['active']=False
    if kind=='figi':raw['results'][0]['share_class_figi']=None
    doc=capture(raw);result=identity(doc)
    assert result['reason']==expected and resolve_reference_candidate(result,{'OKE'}) is None
    assert 'apiKey' not in str(doc)


def test_future_or_non_quarter_queries_and_oversized_results_are_rejected_before_use():
    client=Client(payload())
    for cusip,period in [('bad','2026-09-30'),(CUSIP,'2026-09-29'),(CUSIP,'2026-12-31')]:
        with pytest.raises(ValueError):capture_reference(client,cusip=cusip,report_period=period,observed_at=NOW)
    assert client.calls==[]
    raw=payload();raw['results']*=3
    with pytest.raises(ValueError):capture(raw)


def test_share_class_normalization_is_explicit():
    raw=payload();raw['results'][0]['ticker']='BRK.B'
    result=identity(capture(raw))
    assert result['symbol']=='BRK-B'
    assert resolve_reference_candidate(result,{'BRK/B'})=='BRK-B'
    assert resolve_reference_candidate(result,{'BRKB'}) is None


def test_durable_staging_repeat_retains_first_observation_and_preserves_revisions():
    import json
    from sqlalchemy import create_engine, select, func
    from sqlalchemy.orm import Session
    from app.db import Base
    from app.services.direct_feed_store import DirectFeedDocument, DirectFeedRevision
    from app.services.institutional_reference import stage_reference, load_reference
    engine=create_engine('sqlite:///:memory:')
    Base.metadata.create_all(engine,tables=[DirectFeedDocument.__table__,DirectFeedRevision.__table__])
    try:
        with Session(engine) as db:
            first=capture()
            assert stage_reference(db,first)['status']=='staged';db.commit()
            row=db.scalar(select(DirectFeedDocument));first_hash,first_checked=row.content_hash,row.checked_at
            repeated=deepcopy(first);repeated['observed_at']='2026-10-11T00:00:00Z'
            # A new request ID is transport bookkeeping, not a new identity fact.
            raw=payload();raw['request_id']='second-request'
            repeated=capture_reference(Client(raw),cusip=CUSIP,report_period=PERIOD,observed_at='2026-10-11T00:00:00Z')
            assert stage_reference(db,repeated)['status']=='reused';db.commit()
            assert row.content_hash==first_hash and row.checked_at==first_checked
            assert load_reference(db,cusip=CUSIP,report_period=PERIOD)['observed_at']==NOW
            assert db.scalar(select(func.count()).select_from(DirectFeedRevision))==1
            raw['results'][0]['ticker']='OTHER'
            changed=capture_reference(Client(raw),cusip=CUSIP,report_period=PERIOD,observed_at='2026-10-11T00:00:00Z')
            assert stage_reference(db,changed)['status']=='staged';db.commit()
            assert db.scalar(select(func.count()).select_from(DirectFeedRevision))==2
            assert load_reference(db,cusip=CUSIP,report_period=PERIOD)['response']['results'][0]['ticker']=='OTHER'
            with pytest.raises(ValueError,match='rewind'):stage_reference(db,first)
            db.rollback()
            latest=db.scalar(select(DirectFeedRevision).where(DirectFeedRevision.content_hash==row.content_hash))
            latest.source_bytes=b'tampered';db.commit()
            with pytest.raises(ValueError,match='checksum'):load_reference(db,cusip=CUSIP,report_period=PERIOD)
    finally:
        engine.dispose()
