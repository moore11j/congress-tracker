from copy import deepcopy
from datetime import datetime,timezone
import json
import pytest
from app.clients.direct_sources import DirectSourceError
from app.clients import issuer_pdf_text
from app.services import issuer_nvidia_discovery as n,issuer_discovery as d,issuer_transcripts as i

NOW=datetime(2026,11,25,7,21,tzinfo=timezone.utc)

def company():return deepcopy(next(c for c in json.loads(i.REGISTRY.read_bytes()) if c['symbol']=='NVDA'))

def report(year=2027,quarter=3,form='bloomberg_v1'):
    import calendar
    from datetime import date
    url=(n.CDN+f'content_files/NVDA-{quarter}Q{str(year)[2:]}-Earnings-Transcript.pdf' if form=='bloomberg_v1'
         else n.CDN+f'doc_financials/{year}/q{quarter}/NVDA-Q{quarter}-{year}-Earnings-Call-17-November-2026-5_00-PM-ET.pdf')
    return dict(ReportYear=year,ReportTitle=f'{n.QUARTERS[quarter-1]} {year}',ReportSubType=n.QUARTERS[quarter-1],
        ReportDate=date(year,quarter*3,calendar.monthrange(year,quarter*3)[1]).strftime('%m/%d/%Y 00:00:00'),LanguageId=1,
        Documents=[dict(DocumentTitle='Transcript',DocumentCategory='transcript',DocumentType='file',DocumentFileType='PDF',LanguageId=1,DocumentPath=url)])

def index(rows=None):return json.dumps(dict(GetFinancialReportListResult=rows or [report()])).encode()

def metadata(form='bloomberg_v1'):return n.parse_index(index([report(form=form)]),company(),2027,NOW)[0]

def transcript(form='bloomberg_v1'):
    if form=='factset_v1':
        header='Corrected Transcript 1-877-FACTSET www.callstreet.com Total Pages: 15 Copyright X 2001-2026 FactSet CallStreet, LLC 17-Nov-2026 NVIDIA Corp. (NVDA ) Q3 2027 Earnings Call '
        qa='QUESTION AND ANSWER SECTION Operator: Your first question comes from a synthetic analyst.'
    else:
        header='Participant roles Q3 2027 Earnings Call Company Participants Presentation Operator FINAL TRANSCRIPT 2026-11-17 NVIDIA Corp (NVDA US Equity) X COPYRIGHT 2026, BLOOMBERG LP. Printed on 11-18-2026 Page 1 of 15 '
        qa='Questions And Answers Operator (Question And Answer) Your first question comes from a synthetic analyst.'
    return header+'Synthetic earnings information for parser validation. '*30+qa

@pytest.mark.parametrize('form',['factset_v1','bloomberg_v1'])
def test_exact_fiscal_label_and_publication_are_distinct(monkeypatch,form):
    m=metadata(form);saved=deepcopy(m)
    monkeypatch.setattr(issuer_pdf_text,'extract_text',lambda *a,**k:transcript(form))
    first=n.parse_candidate(b'%PDF-synthetic',m,NOW)
    assert m==saved and m['index_fiscal_label']=='09/30/2027 00:00:00'
    assert 'period_end' not in m and 'published_at' not in m
    assert first[0]['published_at']=='2026-11-17' and first[2]['has_qa']
    assert first==n.parse_candidate(b'%PDF-synthetic',first[0],NOW)

@pytest.mark.parametrize('field,value',[('ReportYear',2026),('ReportTitle','Third Quarter 2026'),('ReportSubType','Other'),('ReportDate','11/17/2026 00:00:00'),('LanguageId',2)])
def test_index_identity_cannot_be_reinterpreted(field,value):
    r=report();r[field]=value
    with pytest.raises(DirectSourceError):n.parse_index(index([r]),company(),2027,NOW)

@pytest.mark.parametrize('field,value',[('DocumentCategory','release'),('DocumentFileType','HTML'),('DocumentType','link'),('LanguageId',2),
    ('DocumentPath',n.CDN+'content_files/NVDA-2Q27-Earnings-Transcript.pdf'),('DocumentPath','https://example.com/NVDA-3Q27-Earnings-Transcript.pdf')])
def test_unreviewed_source_or_kind_rejected(field,value):
    r=report();r['Documents'][0][field]=value
    with pytest.raises(DirectSourceError):n.parse_index(index([r]),company(),2027,NOW)

def test_duplicate_and_bound_checks():
    for rows in ([report(),report()],[report()]*5):
        with pytest.raises(DirectSourceError):n.parse_index(index(rows),company(),2027,NOW)
    r=report();r['Documents']*=2
    with pytest.raises(DirectSourceError):n.parse_index(index([r]),company(),2027,NOW)

@pytest.mark.parametrize('form',['factset_v1','bloomberg_v1'])
@pytest.mark.parametrize('before,after',[('NVDA','OTHER'),('NVIDIA Corp','Another Corp'),('Q3 2027','Q2 2027'),
    ('2026','2025')])
def test_wrong_document_does_not_pass_using_later_valid_body(monkeypatch,form,before,after):
    original=transcript(form)
    wrong=original.replace(before,after)
    # The exact correct title appended after a wrong first page is not permission.
    if before not in ('QUESTION AND ANSWER SECTION','Questions And Answers'):wrong+=' '+original
    monkeypatch.setattr(issuer_pdf_text,'extract_text',lambda *a,**k:wrong)
    with pytest.raises((DirectSourceError,ValueError)):n.parse_candidate(b'%PDF-synthetic',metadata(form),NOW)

@pytest.mark.parametrize('form,marker',[('factset_v1','QUESTION AND ANSWER SECTION'),('bloomberg_v1','Questions And Answers')])
def test_actual_qa_section_is_required(monkeypatch,form,marker):
    monkeypatch.setattr(issuer_pdf_text,'extract_text',lambda *a,**k:transcript(form).replace(marker,'Missing section'))
    with pytest.raises(DirectSourceError):n.parse_candidate(b'%PDF-synthetic',metadata(form),NOW)

@pytest.mark.parametrize('form',['factset_v1','bloomberg_v1'])
def test_future_call_does_not_become_available(monkeypatch,form):
    monkeypatch.setattr(issuer_pdf_text,'extract_text',lambda *a,**k:transcript(form))
    with pytest.raises(DirectSourceError):n.parse_candidate(b'%PDF-synthetic',metadata(form),NOW.replace(day=16))

def test_factset_call_date_must_match_source_filename(monkeypatch):
    monkeypatch.setattr(issuer_pdf_text,'extract_text',lambda *a,**k:transcript('factset_v1').replace('17-Nov-2026','18-Nov-2026'))
    with pytest.raises(DirectSourceError):n.parse_candidate(b'%PDF-synthetic',metadata('factset_v1'),NOW)

@pytest.mark.parametrize('form,publisher',[('factset_v1','FactSet CallStreet'),('bloomberg_v1','BLOOMBERG LP')])
def test_wrong_publisher_is_not_inherited(monkeypatch,form,publisher):
    monkeypatch.setattr(issuer_pdf_text,'extract_text',lambda *a,**k:transcript(form).replace(publisher,'Different Publisher'))
    with pytest.raises(DirectSourceError):n.parse_candidate(b'%PDF-synthetic',metadata(form),NOW)

def test_fiscal_indexes_cover_calendar_rollover_and_static_proof_is_preserved():
    assert n.index_years(NOW)==(2027,2026)
    assert n.index_years(NOW.replace(year=2027,month=1))==(2028,2027)
    assert d.enabled_company(company())
    assert d.index_metadata(company(),2027)['key']=='NVDA:reports:2027'
    assert i._reviewed_latest('NVDA')['fiscal_quarter']==2
