from copy import deepcopy
import io
from concurrent.futures import ThreadPoolExecutor

import pytest
from app.clients import direct_sources as sources, issuer_pdf_text as cache
from test_issuer_pdf import pdf, metadata, TEXT


@pytest.fixture(autouse=True)
def isolated_cache():
    cache.clear_cache()
    yield
    cache.clear_cache()


@pytest.mark.parametrize('field', ['company_pattern','publication_pattern','publisher_pattern','period_pattern','qa_pattern'])
def test_cached_text_cannot_bypass_changed_metadata(field):
    raw=pdf(TEXT);expected=sources.parse_issuer_material(raw,metadata())
    changed=metadata();changed[field]='Not present in this source'
    with pytest.raises(sources.DirectSourceError):sources.parse_issuer_material(raw,changed)
    assert sources.parse_issuer_material(raw,metadata())==expected


def test_text_reuse_does_not_reuse_mutable_parsed_metadata(monkeypatch):
    raw=pdf(TEXT);first=sources.parse_issuer_material(raw,metadata());saved=deepcopy(first)
    first[1]['text']='corrupted';first[1]['has_qa']=False
    monkeypatch.setattr(cache,'_extract',lambda *a:pytest.fail('Identical PDF extracted again'))
    assert sources.parse_issuer_material(raw,metadata())==saved
    altered=metadata();altered['transcript_publisher']='Different attribution'
    assert sources.parse_issuer_material(raw,altered)[1]['transcript_publisher']=='Different attribution'


def test_pdf_revision_and_page_policy_cannot_share_an_older_proof():
    raw=pdf(TEXT);old=sources.parse_issuer_material(raw,metadata())
    new=pdf(TEXT.replace('Microsoft','Changed issuer'))
    with pytest.raises(sources.DirectSourceError):sources.parse_issuer_material(new,metadata())
    many=pdf(TEXT,pages=41)
    broad={**metadata(),'symbol':'UL','discovery_kind':'unilever_results_v1'}
    sources.parse_issuer_material(many,broad)
    with pytest.raises(sources.DirectSourceError,match='page limit'):
        sources.parse_issuer_material(many,metadata())
    assert sources.parse_issuer_material(raw,metadata())==old


def test_warm_cache_does_not_accept_different_encryption_permissions():
    from pypdf import PdfReader,PdfWriter
    from pypdf.constants import UserAccessPermissions
    raw=pdf(TEXT);sources.parse_issuer_material(raw,metadata())
    writer=PdfWriter();writer.clone_document_from_reader(PdfReader(io.BytesIO(raw)))
    writer.encrypt(user_password='',owner_password='test-only',permissions_flag=UserAccessPermissions.PRINT)
    output=io.BytesIO();writer.write(output)
    with pytest.raises(sources.DirectSourceError,match='prohibits extraction'):
        sources.parse_issuer_material(output.getvalue(),metadata())


def test_budget_eviction_preserves_complete_text_and_reparses_evicted_source(monkeypatch):
    import sys
    one=pdf(TEXT);two=pdf(TEXT+' second document')
    expected=sources.parse_issuer_material(one,metadata());cache.clear_cache()
    monkeypatch.setattr(cache,'_LIMIT',sys.getsizeof(one)+sys.getsizeof(expected[0])+50)
    assert sources.parse_issuer_material(one,metadata())==expected
    sources.parse_issuer_material(two,metadata())
    assert cache._size<=cache._LIMIT
    assert sources.parse_issuer_material(one,metadata())==expected
    monkeypatch.setattr(cache,'_LIMIT',1);cache.clear_cache()
    assert sources.parse_issuer_material(one,metadata())==expected
    assert not cache._cache and cache._size==0


def test_concurrent_readers_preserve_text_and_retained_payload_accounting():
    raw=pdf(TEXT)
    with ThreadPoolExecutor(max_workers=4) as executor:
        results=list(executor.map(lambda _:sources.parse_issuer_material(raw,metadata()),range(8)))
    assert all(result==results[0] for result in results)
    assert len(cache._cache)==1 and cache._size==sum(weight for _,weight in cache._cache.values())
