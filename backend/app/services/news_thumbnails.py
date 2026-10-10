"""Bounded publisher image metadata, prepared offline from public page reads."""
from datetime import datetime, timedelta, timezone
from html.parser import HTMLParser
import hashlib
import http.client
import ipaddress
import json
import socket
import ssl
import time
from urllib.parse import urljoin, urlsplit, urlunsplit

MAX_BYTES = 512_000
KINDS = tuple('finnhub-news:market:'+feed for feed in ('general','crypto','forex'))


def image_url(value, *, base=None):
    if not isinstance(value,str) or not value.strip() or len(value)>4096:
        return None
    value=value.strip()
    if any(ord(c)<32 for c in value) or '\\' in value:
        return None
    try:
        parts=urlsplit(urljoin(base,value) if base else value)
        host=(parts.hostname or '').lower()
        if parts.scheme not in {'https','http'} or parts.username or parts.password or parts.port not in {None,80,443}:
            return None
        if not host or '.' not in host or host.endswith(('.localhost','.local','.internal')):
            return None
        try:
            if not ipaddress.ip_address(host).is_global:return None
        except ValueError:
            pass
        return urlunsplit((parts.scheme,parts.netloc,parts.path or '/',parts.query,''))
    except ValueError:
        return None


class ImageMetadata(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.candidates=[]
    def handle_starttag(self,tag,attrs):
        attrs=dict(attrs)
        if tag=='meta':
            key=(attrs.get('property') or attrs.get('name') or '').lower()
            if key in {'og:image','og:image:url','og:image:secure_url','twitter:image','twitter:image:src'}:
                self.candidates.append((0 if key.startswith('og:') else 1,attrs.get('content')))
        elif tag=='link' and (attrs.get('rel') or '').lower()=='image_src':
            self.candidates.append((2,attrs.get('href')))


def parse_image(raw, *, article_url):
    parser=ImageMetadata()
    parser.feed(raw.decode('utf-8',errors='replace'))
    for _,candidate in sorted(parser.candidates,key=lambda pair:pair[0]):
        if result:=image_url(candidate,base=article_url):return result
    return None


def _connection(parts, timeout):
    host=parts.hostname
    port=parts.port or (443 if parts.scheme=='https' else 80)
    addresses=socket.getaddrinfo(host,port,type=socket.SOCK_STREAM)
    ips=list(dict.fromkeys(row[4][0] for row in addresses))
    if not ips or any(not ipaddress.ip_address(ip).is_global for ip in ips):
        raise ValueError('Non-public publisher address')
    # Pin the validated destination. TLS still verifies the original hostname;
    # redirects resolve and validate again. No proxy, cookies or provider auth.
    connection=(http.client.HTTPSConnection(host,port,timeout=timeout,context=ssl.create_default_context())
                if parts.scheme=='https' else http.client.HTTPConnection(host,port,timeout=timeout))
    connection._create_connection=lambda address,timeout=timeout,source_address=None,**kw: socket.create_connection((ips[0],port),timeout,source_address)
    return connection


def extract_image(article_url, *, deadline=None):
    deadline=deadline or time.monotonic()+8
    url=image_url(article_url)
    try:
        for _ in range(4):
            remaining=deadline-time.monotonic()
            if not url or remaining<=0:return None
            parts=urlsplit(url)
            connection=_connection(parts,min(3,remaining))
            try:
                path=parts.path or '/'
                if parts.query:path+='?'+parts.query
                connection.request('GET',path,headers={'User-Agent':'WalnutMarkets/1.0 (article preview; https://walnutmarkets.com)',
                    'Accept':'text/html,application/xhtml+xml','Accept-Encoding':'identity'})
                response=connection.getresponse()
                if response.status in {301,302,303,307,308}:
                    url=image_url(response.getheader('Location'),base=url)
                    continue
                if response.status!=200 or response.getheader('Content-Type','').split(';')[0].strip().lower() not in {'text/html','application/xhtml+xml'}:
                    return None
                if response.getheader('Content-Encoding','identity').lower() not in {'','identity'}:return None
                raw=bytearray()
                while len(raw)<MAX_BYTES and time.monotonic()<deadline:
                    block=response.read1(min(16384,MAX_BYTES-len(raw)))
                    if not block:break
                    raw.extend(block)
                    if b'</head>' in raw.lower():break
                return parse_image(bytes(raw),article_url=url)
            finally:
                connection.close()
    except (OSError,ValueError,http.client.HTTPException):
        return None
    return None


def prepare_news_thumbnails(db, *, limit=8):
    from sqlalchemy import select
    from app.models import InsightsSnapshot
    now=datetime.now(timezone.utc)
    def aware(stamp):return stamp.replace(tzinfo=timezone.utc) if stamp.tzinfo is None else stamp
    snapshots=list(db.scalars(select(InsightsSnapshot).where(InsightsSnapshot.kind.in_(KINDS),InsightsSnapshot.source=='finnhub')))
    urls=[]
    for row in snapshots:
        if not timedelta(0)<=now-aware(row.fetched_at)<=timedelta(minutes=15):continue
        for item in json.loads(row.payload_json).get('items',[])[:50]:
            if not image_url(item.get('image_url')) and (url:=image_url(item.get('url'))) and url not in urls:
                urls.append(url)
    keys={url:'news-thumbnail:'+hashlib.sha256(url.encode()).hexdigest() for url in urls}
    cached={row.kind:row for row in db.scalars(select(InsightsSnapshot).where(InsightsSnapshot.kind.in_(list(keys.values()))))} if keys else {}
    known={}
    for url,key in keys.items():
        row=cached.get(key)
        if row:
            result=json.loads(row.payload_json)
            ttl=timedelta(days=7) if result.get('image_url') else timedelta(hours=12)
            if timedelta(0)<=now-aware(row.fetched_at)<=ttl:known[url]=result.get('image_url')
    db.commit()  # No database transaction/connection held during article HTTP.
    attempted={}
    deadline=time.monotonic()+20
    for url in urls:
        if url in known:continue
        if len(attempted)>=max(0,min(8,limit)) or time.monotonic()>=deadline:break
        attempted[url]=extract_image(url,deadline=min(deadline,time.monotonic()+8))
    for url,result in attempted.items():
        row=db.get(InsightsSnapshot,keys[url])
        if row is None:
            row=InsightsSnapshot(kind=keys[url],source='publisher_metadata',fetched_at=now,payload_json='{}');db.add(row)
        row.source,row.fetched_at,row.payload_json='publisher_metadata',now,json.dumps({'image_url':result,'article_url':url})
    known.update(attempted)
    updated=0
    # Reload under row locks: do not overwrite a concurrent news refresh or
    # advance its observation time just because an image was discovered.
    for kind in KINDS:
        row=db.get(InsightsSnapshot,kind,populate_existing=True,with_for_update=True)
        if row is None or row.source!='finnhub':continue
        payload=json.loads(row.payload_json)
        for item in payload.get('items',[])[:50]:
            url=image_url(item.get('url'))
            if not image_url(item.get('image_url')) and known.get(url):
                item['image_url']=known[url];item['image_source']='publisher_metadata';updated+=1
        revision=hashlib.sha256(json.dumps([(r.get('url'),r.get('image_url')) for r in payload.get('items',[])[:50]],sort_keys=True).encode()).hexdigest()
        if payload.get('thumbnail_revision')!=revision:
            payload['thumbnail_revision']=revision;row.payload_json=json.dumps(payload,sort_keys=True)
    db.commit()
    return {'attempted':len(attempted),'found':sum(bool(v) for v in attempted.values()),'updated':updated,
            'pending':len(set(urls)-set(known)),'canonical_writes':0,'emails':0}
