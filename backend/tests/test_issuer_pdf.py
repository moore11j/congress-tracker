import io
import json
from datetime import date

import pytest
from pypdf import PdfWriter
from pypdf.generic import DictionaryObject, NameObject, DecodedStreamObject

from app.clients import direct_sources as sources
from app.services import issuer_transcripts as issuer
from app.services.direct_feed_store import record_document
from test_issuer_transcripts import setup, snapshot


def pdf(text, *, pages=1, encrypted=False, decoded_padding=0):
    writer = PdfWriter()
    for _ in range(pages):
        page = writer.add_blank_page(width=612, height=792)
        font = DictionaryObject({NameObject('/Type'): NameObject('/Font'), NameObject('/Subtype'): NameObject('/Type1'), NameObject('/BaseFont'): NameObject('/Helvetica')})
        page[NameObject('/Resources')] = DictionaryObject({NameObject('/Font'): DictionaryObject({NameObject('/F1'): writer._add_object(font)})})
        escaped = text.replace('\\', '\\\\').replace('(', '\\(').replace(')', '\\)')
        stream = DecodedStreamObject()
        stream.set_data(('BT /F1 8 Tf 20 770 Td ('+escaped+') Tj ET').encode()+b' '*decoded_padding)
        page[NameObject('/Contents')] = writer._add_object(stream.flate_encode())
    if encrypted:
        writer.encrypt('test-only')
    out = io.BytesIO(); writer.write(out)
    return out.getvalue()


def metadata():
    return {'symbol':'MSFT','document_type':'earnings_transcript','fiscal_year':2026,'fiscal_quarter':4,
            'period_pattern':'FY2026 Q4','company_pattern':'Microsoft','publication_pattern':'July 29, 2026',
            'source_format':'pdf','publisher_pattern':'Example Publisher','transcript_publisher':'Example Publisher'}


TEXT = 'Microsoft earnings transcript FY2026 Q4 July 29, 2026 Example Publisher QUESTION AND ANSWER SECTION '+('Example earnings evidence. '*60)


def test_real_pdf_extraction_preserves_identity_qa_and_publisher():
    raw=pdf(TEXT)
    text, parsed=sources.parse_issuer_material(raw,metadata())
    assert ' '.join(TEXT.split()) == text
    assert parsed['has_qa'] and parsed['transcript_publisher']=='Example Publisher'
    assert parsed['canonical_key']=='MSFT:earnings_transcript:2026:Q4'


@pytest.mark.parametrize('case', ['format','unreviewed','company','date','period','publisher','encrypted','pages','decoded','bytes'])
def test_pdf_rejects_unreviewed_mismatch_and_resource_limits(case):
    m=metadata();raw=pdf(TEXT)
    if case=='format':raw=b'<main>'+TEXT.encode()+b'</main>'
    if case=='unreviewed':m.pop('source_format')
    if case in {'company','date','period','publisher'}:
        m[{'company':'company_pattern','date':'publication_pattern','period':'period_pattern','publisher':'publisher_pattern'}[case]]='Not Present In Document'
    if case=='encrypted':raw=pdf(TEXT,encrypted=True)
    if case=='pages':raw=pdf(TEXT,pages=41)
    if case=='decoded':raw=pdf(TEXT,decoded_padding=5_000_001)
    if case=='bytes':raw=b'%PDF-'+b' '*2_000_000
    with pytest.raises(sources.DirectSourceError):sources.parse_issuer_material(raw,m)


def test_pdf_canonical_repeat_keeps_publisher_and_holds_publisher_change(setup):
    db,security_id,row_id,m,_=setup
    m.update(metadata())
    row=db.get(issuer.DirectFeedDocument,row_id);row.metadata_json=json.dumps(m)
    raw=pdf(TEXT);text,parsed=sources.parse_issuer_material(raw,m)
    record_document(db,row,raw,text,parsed);db.commit()
    first=issuer.prepare_research_document(db,security_id=security_id,publish_since=date(2026,7,29));db.commit()
    saved=snapshot(db)
    second=issuer.prepare_research_document(db,security_id=security_id,publish_since=date(2026,7,29));db.commit()
    assert first['created'] and not second['created'] and snapshot(db)==saved
    assert second['publication_evidence']['transcript_publisher']=='Example Publisher'
    m['transcript_publisher']='Different attribution'
    raw += b'\n'  # New transport revision, identical extracted transcript.
    row.metadata_json=json.dumps(m)
    text,parsed=sources.parse_issuer_material(raw,m);record_document(db,row,raw,text,parsed);db.commit()
    with pytest.raises(ValueError,match='identity or boundary'):
        issuer.prepare_research_document(db,security_id=security_id,publish_since=date(2026,7,29))


@pytest.mark.parametrize('case',['reviewed','other_path','other_host','direct_cdn'])
def test_only_exact_reviewed_cdn_redirect_is_accepted(monkeypatch,case):
    approved='https://cdn.example.test/company/transcript.pdf'
    destination={'reviewed':approved,'other_path':'https://cdn.example.test/other/transcript.pdf','other_host':'https://elsewhere.example.test/company/transcript.pdf','direct_cdn':approved}[case]
    calls=[]
    class Response:
        def __init__(self,redirect):
            self.status_code=302 if redirect else 200;self.is_redirect=redirect;self.headers={'Location':destination}
        def __enter__(self):return self
        def __exit__(self,*args):pass
        def iter_content(self,*args):yield b'approved bytes'
    class Session:
        headers={}
        def get(self,url,**kwargs):calls.append(url);return Response(len(calls)==1)
    monkeypatch.setattr(sources.time,'sleep',lambda _:None)
    client=sources.DirectSourceClient(issuer_hosts=['issuer.example.test'],issuer_redirect_urls=[approved],session=Session())
    url=approved if case=='direct_cdn' else 'https://issuer.example.test/transcript.pdf'
    if case=='reviewed':
        assert client.get(url)==b'approved bytes' and len(calls)==2
    else:
        with pytest.raises(sources.DirectSourceError,match='approved HTTPS'):client.get(url)
        assert len(calls)==(0 if case=='direct_cdn' else 1)
