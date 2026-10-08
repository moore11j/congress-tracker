from datetime import date
import json
from pathlib import Path

import pytest
import requests
from sqlalchemy import func, select

from app.clients.senate_efd import SenateEfdClient, parse_search_row, HOME, SEARCH
from app.clients.direct_sources import DirectSourceError
from app.clients.direct_sources import parse_senate_html
from app.services.official_congress import classify_congress_asset_type
from app.services.direct_feed_collection import collect_direct_feeds, parse_document
from app.services.direct_feed_store import DirectFeedDocument, DirectFeedRevision
from test_direct_feeds import db, SENATE_REAL_REPORT

START, END = date(2026,10,1), date(2026,10,7)
REPORT = '6bf3b6f7-9e1b-499a-bd5a-990292ce2e72'


def row(key=REPORT, title='Periodic Transaction Report for 10/01/2026'):
    return ['Sheldon','Whitehouse','Senator',f'<a href="/search/view/ptr/{key}/">{title}</a>','10/01/2026']


class Response:
    def __init__(self, body=b'', status=200, headers=None):
        self.body, self.status_code, self.headers = body, status, headers or {}
    def __enter__(self): return self
    def __exit__(self, *a): pass
    def iter_content(self, n): yield self.body


class Session:
    def __init__(self, replies):
        self.headers, self.cookies, self.calls = {}, requests.cookies.RequestsCookieJar(), []
        self.replies = list(replies)
    def request(self, method, url, **kwargs):
        self.calls.append((method,url,kwargs))
        if method=='POST' and url==HOME:self.cookies.set('csrftoken','fixture-token')
        return self.replies.pop(0)


def client_for(monkeypatch, pages):
    monkeypatch.setattr('app.clients.senate_efd.time.sleep',lambda *a:None)
    session=Session([Response(b'<input name="csrfmiddlewaretoken" value="fixture-token">'),
        Response(status=302,headers={'Location':SEARCH}),*[Response(json.dumps(p).encode()) for p in pages]])
    return SenateEfdClient(accept_notice=True,session=session)


def test_notice_search_pagination_and_receipt(monkeypatch):
    client=client_for(monkeypatch,[{'recordsFiltered':2,'data':[row()]},
        {'recordsFiltered':2,'data':[row('0541be4f-4f96-4d84-8d6f-d349f218bb2a')]}])
    rows=client.discover(start=START,end=END,page_size=1)
    assert len(rows)==2 and client.search_receipt['complete']
    assert rows[0]['filing_id']==REPORT and rows[0]['member_name']=='Sheldon Whitehouse'
    assert len(client.search_receipt['page_sha256'])==2
    assert client.session.calls[1][2]['data']['prohibition_agreement']=='1'
    assert client.session.calls[3][2]['data']['start']=='1'


@pytest.mark.parametrize('pages',[
    [{'recordsFiltered':2,'data':[row()]},{'recordsFiltered':3,'data':[row()]}],
    [{'recordsFiltered':2,'data':[row()]},{'recordsFiltered':2,'data':[row()]}],
    [{'recordsFiltered':2,'data':[]}],
    [{'data':[]}],
])
def test_incomplete_changing_or_duplicated_search_never_passes(monkeypatch,pages):
    client=client_for(monkeypatch,pages)
    with pytest.raises(DirectSourceError):client.discover(start=START,end=END,page_size=1)
    assert client.search_receipt is None


def test_denial_is_not_retried_or_interpreted_as_empty(monkeypatch):
    monkeypatch.setattr('app.clients.senate_efd.time.sleep',lambda *a:None)
    session=Session([Response(b'Access denied',status=403)])
    client=SenateEfdClient(accept_notice=True,session=session)
    with pytest.raises(DirectSourceError,match='403'):client.discover(start=START,end=END)
    assert len(session.calls)==1 and not client.ready


def test_notice_must_be_configured_before_network():
    session=Session([])
    with pytest.raises(DirectSourceError,match='acknowledgement'):
        SenateEfdClient(session=session).discover(start=START,end=END)
    assert session.calls==[]


@pytest.mark.parametrize('mutation',['url','date','title'])
def test_search_identity_constraints(mutation):
    record=row()
    if mutation=='url':record[3]=record[3].replace('/search/', 'https://example.com/search/')
    elif mutation=='date':record[4]='09/30/2026'
    else:record[3]=record[3].replace('Periodic Transaction','Annual')
    with pytest.raises(DirectSourceError):parse_search_row(record,start=START,end=END)


def test_report_heading_does_not_overwrite_portal_filing_date():
    metadata=parse_search_row(row(title='Periodic Transaction Report for 10/02/2026'),start=START,end=END)
    assert metadata['report_date']=='2026-10-02' and metadata['filing_date']=='2026-10-01'


def test_staged_official_discovery_and_repeat_preserve_rows(db,monkeypatch):
    class Senate:
        search_receipt={'complete':True,'reported_total':1}
        def discover(self,**kwargs):return [parse_search_row(row(),start=START,end=END)]
        def get(self,url):return SENATE_REAL_REPORT.read_bytes()
    class Other:
        def get(self,url):pytest.fail('Used generic unauthenticated transport')
    senate=Senate()
    result=collect_direct_feeds(db,Other(),sources=['senate_ptr'],start=START,end=END,senate_client=senate)
    assert not result['errors'] and result['processed']==1
    staged=db.scalar(select(DirectFeedDocument))
    parsed=json.loads(staged.parsed_json)
    assert len(parsed['transactions'])==5 and all(t['filing_id']==REPORT for t in parsed['transactions'])
    staged.checked_at=None;db.commit()
    repeat=collect_direct_feeds(db,Other(),sources=['senate_ptr'],start=START,end=END,senate_client=senate)
    assert repeat['processed']==1
    assert db.scalar(select(func.count()).select_from(DirectFeedRevision))==1


@pytest.mark.parametrize('key,value',[('filing_date','2026-10-02'),('member_name','Another Senator'),
    ('report_title','Periodic Transaction Report for 10/02/2026')])
def test_report_body_must_match_discovery(key,value):
    metadata=parse_search_row(row(),start=START,end=END)
    metadata[key]=value
    with pytest.raises(DirectSourceError,match='differs from discovery'):
        parse_senate_html(SENATE_REAL_REPORT.read_bytes(),metadata)


def test_failed_rediscovery_cannot_reuse_prior_complete_receipt(monkeypatch):
    client=client_for(monkeypatch,[{'recordsFiltered':0,'data':[]},{'data':[]}])
    client.discover(start=START,end=END)
    assert client.search_receipt['complete']
    with pytest.raises(DirectSourceError):client.discover(start=START,end=END)
    assert client.search_receipt is None


@pytest.mark.parametrize('declared,symbol,expected',[('Corporate Bond','--','corporate_bond'),
    ('Municipal Bond','--','municipal_bond'),('Mutual Fund','VFIAX','mutual_fund'),('', '--','unresolved'),
    ('Non-Public Stock','--','private_stock'),('Stock','--','stock'),('Other','--','other')])
def test_explicit_non_equity_and_placeholder_tickers_are_not_stocks(declared,symbol,expected):
    assert classify_congress_asset_type(raw_symbol=symbol,security_name='Example asset',
        issuer_name=None,asset_type=declared)==expected


def test_real_fetterman_corporate_bonds_remain_non_equity():
    fixture=Path(__file__).with_name('fixtures')/'senate_fetterman_2026_10_07.html'
    metadata={'filing_id':'0541be4f-4f96-4d84-8d6f-d349f218bb2a','filing_date':'2026-10-07',
        'member_name':'John Fetterman','report_title':'Periodic Transaction Report for 10/07/2026',
        'url':'https://efdsearch.senate.gov/search/view/ptr/0541be4f-4f96-4d84-8d6f-d349f218bb2a/'}
    _,parsed,reasons=parse_document('senate_ptr',fixture.read_bytes(),metadata)
    assert not reasons and len(parsed['transactions'])==2
    assert all(t['asset_type_normalized']=='corporate_bond' and t['ticker_normalized'] is None
               and t['owner_normalized']=='dependent' for t in parsed['transactions'])


@pytest.mark.parametrize('code,expected',[('EF','etf'),('ET','etn'),('CS','corporate_debt'),
    ('GS','government_security'),('HN','private_fund'),('OT','other'),
    ('OL','business_interest'),('OI','investment_interest'),('AB','asset_backed_security')])
def test_house_official_asset_codes_preserve_classification(monkeypatch,code,expected):
    from app.clients import direct_sources as sources
    source_text=f'Filing ID #1\nName: Example\nSP Example Asset [{code}]\nP 10/01/2026 10/02/2026 $1,001 - $15,000\n'
    class Page:
        def extract_text(self, **kwargs):return source_text
    class Reader:pages=[Page()]
    monkeypatch.setattr(sources,'PdfReader',lambda raw:Reader())
    _,parsed,reasons=parse_document('house_ptr',b'%PDF fixture',{'filing_id':'1',
        'url':'https://disclosures-clerk.house.gov/1.pdf','filing_date':'2026-10-03'})
    assert not reasons and parsed['transactions'][0]['asset_type_normalized']==expected
