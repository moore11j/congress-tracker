"""Original scanned lots are complete and remain held until identities are reviewed."""
import copy
import hashlib
import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest
from sqlalchemy import select, func
from app.clients import house_ptr_preparation as prep
from app.clients.direct_sources import parse_house_pdf, DirectSourceError, HouseScanReviewRequired
from app.models import Event, Transaction, EmailDelivery
from app.services import direct_feed_collection as collection
from app.services.direct_feed_store import DirectFeedDocument, DirectFeedRevision, discover
from app.services.official_congress import congress_transaction_hash
from test_direct_feeds import db


@pytest.fixture
def source():
    raw = (Path(__file__).with_name('fixtures') / 'house_9116376.pdf').read_bytes()
    registry = json.loads(prep.REGISTRY.read_bytes())
    review = registry['preparations'][0]
    assert hashlib.sha256(raw).hexdigest() == review['source_sha256'] == '9112dbf52d805f97500c314cbfacf58b114c9448d0f9a81091bafdff63da1a95'
    return raw, review['metadata'], registry


def test_original_complete_population_and_economics_without_inferred_identity(source):
    raw, meta, _ = source
    text, parsed, holds = collection.parse_document('house_ptr', raw, meta)
    assert text.startswith('Reviewed economic fields; unverified asset descriptions')
    assert holds == [prep.HOLD]
    rows = parsed['transactions']
    assert len(rows) == len({r['normalized_hash'] for r in rows}) == 459
    assert rows[0]['source_line_ref'] == 'p2:r1'
    assert rows[-1]['source_line_ref'] == 'p31:r4'
    assert {r['ticker_raw'] for r in rows} == {None}
    assert {r['ticker_normalized'] for r in rows} == {None}
    assert {r['asset_type_normalized'] for r in rows} == {'unresolved'}
    assert {r['symbol_resolution_status'] for r in rows} == {'scan_security_review_required'}
    assert all(congress_transaction_hash(r) == r['normalized_hash'] for r in rows)
    report = parsed['source_report']
    assert report['preparation_review']['publication_eligible'] is False
    assert report['preparation_review']['page_row_counts'][0] == 0
    assert report['transactions'][263]['notification_date'] == '2026-10-01'
    assert report['transactions'][6]['assetDescription'].startswith('CALL/MSFT')
    assert report['transactions'][6]['symbol'] is None
    assert rows[267]['amount_low'] == 100001 and rows[267]['amount_high'] == 250000
    assert rows[268]['amount_low'] == 50001 and rows[268]['amount_high'] == 100000
    # Same economics in two different trusts are separate original rows.
    for left, right in zip(rows[447:451], rows[455:459], strict=True):
        for field in ('owner_normalized','transaction_date','issuer_name_raw','amount_low','amount_high'):
            assert left[field] == right[field]
        assert left['normalized_hash'] != right['normalized_hash']
    assert collection.parse_document('house_ptr', raw, meta)[1] == parsed


def test_blank_optional_owner_is_not_invented_as_self(source,tmp_path,monkeypatch):
    raw,meta,registry=source
    registry['preparations'][0]['rows'][0]['reviewed_economics']['ownerCodeRaw']=None
    path=tmp_path/'registry.json';path.write_text(json.dumps(registry),encoding='utf-8')
    monkeypatch.setattr(prep,'REGISTRY',path)
    _,parsed,holds=collection.parse_document('house_ptr',raw,meta)
    assert holds == [prep.HOLD]
    assert parsed['transactions'][0]['owner_normalized']=='unknown'
    assert parsed['source_report']['transactions'][0]['owner_disclosure_status']=='not_disclosed'


@pytest.mark.parametrize('field,value', [('filing_id','123'),('key','2026:123'),('member_name','Other Person'),
    ('district','CA01'),('form','PTR/A'),('filing_date','2026-10-08'),('url','https://example.com/scan.pdf')])
def test_changed_discovery_cannot_adopt_original_review(source, field, value):
    raw, meta, _ = source
    with pytest.raises(DirectSourceError, match='Invalid House economic preparation'):
        parse_house_pdf(raw, {**meta, field:value})


def test_changed_original_and_page_population_stay_held(source):
    raw, meta, _ = source
    with pytest.raises(HouseScanReviewRequired):
        parse_house_pdf(raw+b'\n% revised original',meta)
    with pytest.raises(ValueError):
        prep.prepared_house_scan(raw,meta,30)


@pytest.mark.parametrize('mutation', ['missing_row','repeated_row','wrong_page','wrong_position','cover_rows',
    'bad_owner','future_notification','reversed_dates','partial_purchase','bool_amount','bad_amount','bad_flag',
    'ticker_injection','wrong_scope','eligible','duplicate_review','amendment'])
def test_incomplete_or_enriched_candidate_cannot_be_used(source,tmp_path,monkeypatch,mutation):
    raw,meta,registry=source
    review=registry['preparations'][0]; row=review['rows'][0]; fields=row['reviewed_economics']
    if mutation=='missing_row': review['rows'].pop()
    elif mutation=='repeated_row': review['rows'][1]=copy.deepcopy(row)
    elif mutation=='wrong_page': row['page']=1
    elif mutation=='wrong_position': row['source_line_ref']='p2:r2'
    elif mutation=='cover_rows': review['page_row_counts'][0]=1
    elif mutation=='bad_owner': fields['ownerCodeRaw']='self'
    elif mutation=='future_notification': fields['notificationDate']='2026-10-08'
    elif mutation=='reversed_dates': fields['notificationDate']='2026-08-01'
    elif mutation=='partial_purchase': fields['partialSale']=True
    elif mutation=='bool_amount': fields['amountLow']=True
    elif mutation=='bad_amount': fields['amountHigh']=50000
    elif mutation=='bad_flag': fields['capGainsOver200']=None
    elif mutation=='ticker_injection': row['resolvedTicker']='MSFT'
    elif mutation=='wrong_scope': review['review_scope']='complete'
    elif mutation=='eligible': review['publication_eligible']=True
    elif mutation=='duplicate_review': registry['preparations'].append(copy.deepcopy(review))
    else: review['amendment_flag']=True
    path=tmp_path/'registry.json';path.write_text(json.dumps(registry),encoding='utf-8')
    monkeypatch.setattr(prep,'REGISTRY',path)
    with pytest.raises(DirectSourceError):parse_house_pdf(raw,meta)


def test_prepared_economics_are_retained_without_reconciliation_or_canonical_writes(db,source,monkeypatch):
    raw,meta,_=source
    doc=discover(db,'house_ptr',meta);db.commit()
    monkeypatch.setattr(collection,'discover_sources',lambda *a,**k:({},[]))
    def forbidden(*a,**k):raise AssertionError('Unverified descriptions reached reconciliation')
    monkeypatch.setattr(collection,'reconcile_congress',forbidden)
    class Source:
        calls=0
        def get(self,url):
            assert url==meta['url'];self.calls+=1;return raw
    client=Source()
    def collect():
        return collection.collect_direct_feeds(db,client,sources=['house_ptr'],start=date(2026,10,1),end=date(2026,10,12))
    assert collect()['results']=={'quarantined':1}
    assert doc.status=='quarantined'
    assert len(json.loads(doc.parsed_json)['transactions'])==459
    assert json.loads(doc.reconciliation_json)['status']=='held'
    before=doc.parsed_json
    doc.checked_at=datetime.now(timezone.utc)-timedelta(hours=2);db.commit()
    assert collect()['processed']==0 and client.calls==1
    doc.checked_at=datetime.now(timezone.utc)-timedelta(hours=25);db.commit()
    assert collect()['results']=={'quarantined':1} and client.calls==2
    assert doc.parsed_json==before
    revision=db.scalar(select(DirectFeedRevision))
    assert revision.source_bytes==raw
    assert db.scalar(select(func.count()).select_from(DirectFeedRevision))==1
    for model in (Event,Transaction,EmailDelivery):
        assert db.scalar(select(func.count()).select_from(model))==0


def test_retained_original_can_gain_preparation_without_rewriting_old_revision(db,source,monkeypatch):
    raw,meta,_=source
    doc=discover(db,'house_ptr',meta);db.commit()
    monkeypatch.setattr(collection,'discover_sources',lambda *a,**k:({},[]))
    class Source:
        def get(self,url):return raw
    def collect():
        return collection.collect_direct_feeds(db,Source(),sources=['house_ptr'],start=date(2026,10,1),end=date(2026,10,12))
    original=prep.prepared_house_scan
    monkeypatch.setattr(prep,'prepared_house_scan',lambda *a:None)
    assert collect()['results']=={'quarantined':1}
    revision=db.scalar(select(DirectFeedRevision));before=(revision.source_bytes,revision.parsed_json,revision.source_text)
    assert json.loads(doc.parsed_json)['transactions']==[]
    monkeypatch.setattr(prep,'prepared_house_scan',original)
    doc.checked_at=datetime.now(timezone.utc)-timedelta(hours=25);db.commit()
    assert collect()['results']=={'quarantined':1}
    assert len(json.loads(doc.parsed_json)['transactions'])==459
    db.refresh(revision)
    assert (revision.source_bytes,revision.parsed_json,revision.source_text)==before
    assert db.scalar(select(func.count()).select_from(DirectFeedRevision))==1
    # Even an erroneous parsed status cannot bypass revalidation on publication.
    from app.services.direct_congress_worker import _project
    doc.status='parsed'
    result=_project(db,doc,meta,raw,[],date(2026,10,1))
    assert result=={'status':'held','reasons':[prep.HOLD]}
    assert db.scalar(select(func.count()).select_from(Event))==0
    assert db.scalar(select(func.count()).select_from(Transaction))==0
