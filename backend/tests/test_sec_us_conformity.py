"""Explicit conformity wording, complete filed originals, and rejected assertions."""
from datetime import date
from pathlib import Path
import gzip,hashlib,json,socket
import pytest
from app.services import sec_tabular_reporting as reporting

ROOT=Path(__file__).parent/'fixtures/sec_us_conformity'
SENTENCE='The financial information is prepared in conformity with U.S. GAAP.'

@pytest.fixture(autouse=True)
def offline(monkeypatch):
    monkeypatch.setattr(socket.socket,'connect',lambda *a,**k:pytest.fail('Unexpected transport'))

@pytest.mark.parametrize('sentence',[
    SENTENCE,
    SENTENCE.replace('is prepared','has been prepared'),
    SENTENCE.replace('information is','statements are'),
    SENTENCE.replace('U.S. GAAP','accounting principles generally accepted in the United States of America'),
    SENTENCE.replace('information is','statements, and other financial information included in this report, have been'),
])
def test_explicit_conformity_preparation(sentence):
    proof=reporting.declaration(('<p>'+sentence+'</p>').encode(),{'facts':[{'currency':'USD'}]})
    assert proof['accounting_basis']=='US GAAP'
    assert len(proof['accounting_declarations'])==1

@pytest.mark.parametrize('sentence',[
    SENTENCE.replace('is prepared','is not prepared'),
    SENTENCE.replace('is prepared','will be prepared'),
    SENTENCE.replace('is prepared','would be prepared'),
    SENTENCE.replace('The financial','The hypothetical financial'),
    SENTENCE.replace('The financial','The adjusted financial'),
    SENTENCE.replace('The financial','The non-GAAP financial'),
    SENTENCE.replace('The financial','If financial'),
    SENTENCE.replace('U.S. GAAP','GAAP'),
    SENTENCE.replace('U.S. GAAP','Canadian GAAP'),
])
def test_non_declarations_stay_held(sentence):
    with pytest.raises(reporting.TabularReportingError,match='table_accounting_basis_unproved'):
        reporting.declaration(('<p>'+sentence+'</p>').encode(),{'facts':[{'currency':'USD'}]})

def originals():
    manifest=json.loads((ROOT/'sources.json').read_bytes())
    rows=[*manifest['sources'],manifest['company']];raw={}
    for item in rows:
        data=gzip.decompress((ROOT/item['file']).read_bytes())
        assert len(data)==item['bytes'] and hashlib.sha256(data).hexdigest()==item['sha256']
        raw[item['url']]=data
    exhibits=[m for m in manifest['sources'] if m['url']!=m['candidate']['cover_url']]
    def args(item):
        candidate=item['candidate']
        return dict(source_url=item['url'],cover_url=candidate['cover_url'],
            cover_raw=raw[candidate['cover_url']],company_raw=raw[manifest['company']['url']],
            symbol='YB',cik=candidate['cik'],available_by=date(2026,11,15))
    return exhibits,raw,args

def test_complete_current_filing_recovers_native_records_and_exact_repeat(monkeypatch):
    exhibits,raw,args=originals();item=exhibits[0]
    result=reporting.normalize(raw[item['url']],**args(item))
    assert result['evidence']['current_period']=='2026-06-30'
    assert result['evidence']['currency']=='CNY'
    assert result['evidence']['accounting_basis']=='US GAAP'
    assert len(result['records']['revenue'])==4
    assert result['records']['net_income']=={}  # No parent-holder income inferred.
    assert result==reporting.normalize(raw[item['url']],**args(item))
    from app.services import sec_statement_declarations
    monkeypatch.setattr(sec_statement_declarations,'us_conformity_declarations',lambda *a:[])
    with pytest.raises(reporting.TabularReportingError,match='table_accounting_basis_unproved'):
        reporting.normalize(raw[item['url']],**args(item))

def test_old_filing_stays_stale_and_negated_current_original_stays_held():
    exhibits,raw,args=originals()
    with pytest.raises(reporting.TabularReportingError,match='table_current_statement_stale'):
        reporting.normalize(raw[exhibits[1]['url']],**args(exhibits[1]))
    item=exhibits[0];source=raw[item['url']]
    assert source.count(b'prepared in conformity with')==1
    source=source.replace(b'prepared in conformity with',b'not prepared in conformity with')
    with pytest.raises(reporting.TabularReportingError,match='table_accounting_basis_unproved'):
        reporting.normalize(source,**args(item))

def test_complete_second_issuer_basis_does_not_override_period_validation(monkeypatch):
    from app.services import sec_tabular_financials
    # Isolate the declaration repair from the separate month-period reader.
    monkeypatch.setattr(sec_tabular_financials,'month_period_wording',lambda text:text)
    manifest=json.loads((ROOT/'sources.json').read_bytes())
    item=manifest['other_companies']['SB']
    company=gzip.decompress((ROOT/item['file']).read_bytes())
    assert hashlib.sha256(company).hexdigest()==item['sha256']
    changed=0
    for item in manifest['sources']:
        if item['candidate']['symbol']!='SB':continue
        raw=gzip.decompress((ROOT/item['file']).read_bytes())
        assert hashlib.sha256(raw).hexdigest()==item['sha256']
        if b'in conformity with' not in raw:continue
        proof=reporting.declaration(raw,{'facts':[{'currency':'USD'}]})
        assert proof['accounting_basis']=='US GAAP'
        candidate=item['candidate']
        with pytest.raises(reporting.TabularReportingError,match='table_current_statement_alignment'):
            reporting.normalize(raw,source_url=item['url'],cover_url=candidate['cover_url'],
                cover_raw=raw,company_raw=company,symbol='SB',cik=candidate['cik'],available_by=date(2026,11,15))
        changed+=1
    assert changed==2
