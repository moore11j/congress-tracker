from datetime import datetime,timezone
import json
from urllib.parse import urlsplit
import pytest
from sqlalchemy import create_engine,select
from sqlalchemy.orm import Session
from app.db import Base
from app.models import InsightsSnapshot
from app.services import news_thumbnails as images
from app.services.finnhub_research import normalize_news


@pytest.mark.parametrize('url',['javascript:alert(1)','data:image/png;x','https://user:pass@example.com/i',
    'http://127.0.0.1/i','http://169.254.169.254/i','https://example.local/i','https://example.com:8443/i'])
def test_unsafe_image_rejected(url):
    assert images.image_url(url) is None


def test_provider_image_keeps_signed_query_and_article_identity():
    now=datetime.now(timezone.utc)
    row={'id':1,'headline':'Headline','url':'https://publisher.example/story','source':'Publisher','datetime':now.timestamp(),
         'image':'https://cdn.example/image.jpg?sig=x%2Fy&size=100'}
    first=normalize_news([row],observed_at=now)['items'][0]
    second=normalize_news([{**row,'image':None}],observed_at=now)['items'][0]
    assert first['image_url']==row['image']
    assert {k:v for k,v in first.items() if k!='image_url'}=={k:v for k,v in second.items() if k!='image_url'}


def test_metadata_priority_relative_entities_and_unsafe_fallback():
    raw=b'<head><meta name="twitter:image" content="/twitter.jpg"><meta property="og:image" content="http://127.0.0.1/x"><meta property="og:image" content="/photo.jpg?a=1&amp;b=2"></head>'
    assert images.parse_image(raw,article_url='https://publisher.example/story')=='https://publisher.example/photo.jpg?a=1&b=2'
    assert images.parse_image(b'<html>No image</html>',article_url='https://publisher.example') is None


def test_dns_validation_rejects_mixed_public_private_answers(monkeypatch):
    monkeypatch.setattr(images.socket,'getaddrinfo',lambda *a,**k:[(0,0,0,'',('8.8.8.8',443)),(0,0,0,'',('127.0.0.1',443))])
    with pytest.raises(ValueError):images._connection(urlsplit('https://publisher.example/story'),3)


def test_validated_address_is_pinned_without_re_resolving_hostname(monkeypatch):
    monkeypatch.setattr(images.socket,'getaddrinfo',lambda *a,**k:[(0,0,0,'',('8.8.8.8',443))])
    called=[]
    monkeypatch.setattr(images.socket,'create_connection',lambda *a:called.append(a))
    connection=images._connection(urlsplit('https://publisher.example/story'),3)
    connection._create_connection(('publisher.example',443),3)
    assert called[0][0]==('8.8.8.8',443)
    assert connection.host=='publisher.example'


def test_redirect_cannot_fetch_private_target(monkeypatch):
    class Response:
        status=302
        def getheader(self,key,default=None):return 'http://169.254.169.254/secret' if key=='Location' else default
    class Connection:
        def request(self,*a,**k):pass
        def getresponse(self):return Response()
        def close(self):pass
    calls=[]
    monkeypatch.setattr(images,'_connection',lambda parts,timeout:calls.append(parts.hostname) or Connection())
    assert images.extract_image('https://publisher.example/story') is None
    assert calls==['publisher.example']


def test_cached_fallback_preserves_timestamps_headlines_and_repeats_without_fetch(monkeypatch):
    from app.services.insights_snapshots import seed_finnhub_headlines
    engine=create_engine('sqlite:///:memory:')
    Base.metadata.create_all(engine,tables=[InsightsSnapshot.__table__])
    now=datetime.now(timezone.utc)
    rows=[{'id':i+1,'headline':f'Story {i}','url':f'https://publisher.example/{i}','source':'Publisher','datetime':now.timestamp()} for i in range(3)]
    payload=normalize_news(rows,observed_at=now)
    calls=[]
    with Session(engine) as db:
        db.add(InsightsSnapshot(kind=images.KINDS[0],source='finnhub',fetched_at=now,payload_json=json.dumps(payload)));db.commit()
        assert seed_finnhub_headlines(db)['status']=='ok';db.commit()
        def fetch(url,**kw):
            assert not db.in_transaction()
            calls.append(url)
            return 'https://cdn.example/photo.jpg' if url.endswith('/2') else None
        monkeypatch.setattr(images,'extract_image',fetch)
        first=images.prepare_news_thumbnails(db,limit=2)
        assert first['attempted']==2 and first['pending']==1 and first['found']==1
        assert seed_finnhub_headlines(db)['status']=='ok';db.commit()
        raw=db.get(InsightsSnapshot,images.KINDS[0]);saved=json.loads(raw.payload_json)
        for old,new in zip(payload['items'],saved['items']):
            assert {k:v for k,v in old.items() if k not in {'image_url','image_source'}}=={k:v for k,v in new.items() if k not in {'image_url','image_source'}}
        assert raw.fetched_at.replace(tzinfo=timezone.utc)==now
        headline=db.get(InsightsSnapshot,'market-headlines:finnhub')
        assert sum(bool(i['image_url']) for i in json.loads(headline.payload_json)['items'])==1
        images.prepare_news_thumbnails(db,limit=2)
        before=[(r.kind,r.payload_json,r.fetched_at) for r in db.scalars(select(InsightsSnapshot).order_by(InsightsSnapshot.kind))]
        third=images.prepare_news_thumbnails(db)
        after=[(r.kind,r.payload_json,r.fetched_at) for r in db.scalars(select(InsightsSnapshot).order_by(InsightsSnapshot.kind))]
        assert third['attempted']==0 and len(calls)==3 and before==after
    engine.dispose()
