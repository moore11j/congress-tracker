"""Statement declarations must not lose conflicts to narrower wording."""
import gzip
import hashlib
import json
from pathlib import Path
import pytest
from app.services import sec_statement_currency as currency

URL='https://www.sec.gov/Archives/edgar/data/1905511/000149315226047701/form6-k.htm'

@pytest.mark.parametrize('phrase', [
    'The accompanying unaudited interim condensed consolidated financial statements are presented in the Singaporean dollars',
    'The financial statements are presented in Singapore dollars',
    'Our consolidated financial statements are presented in thousands of SGD',
])
def test_complete_statement_wording_exposes_currency_conflict(phrase):
    raw=f'<html>Our reporting currency is the US Dollar. {phrase}.</html>'.encode()
    proof=currency.declaration(raw,url=URL)
    assert proof['status']=='ambiguous' and proof['currency'] is None
    assert {r['currency'] for r in proof['declarations']}=={'SGD','USD'}
    with pytest.raises(currency.StatementCurrencyError,match='ambiguous'):
        currency.accounting_basis(raw,url=URL)

@pytest.mark.parametrize('phrase', [
    'The accompanying statements of our subsidiary are presented in SGD.',
    'The accompanying financial statements were presented in SGD.',
    'The accompanying financial statements are not presented in SGD.',
    'The accompanying financial statements will be presented in SGD.',
    'The accompanying financial statements are presented in dollars.',
])
def test_other_entities_history_and_ambiguous_units_do_not_select_currency(phrase):
    assert currency.declaration(phrase.encode(),url=URL)['status']=='unavailable'

def original():
    root=Path(__file__).parent/'fixtures/sec_currency_conflicts'
    meta=json.loads((root/'sources.json').read_bytes())['JCSE']
    raw=gzip.decompress((root/meta['file']).read_bytes())
    assert len(raw)==meta['bytes'] and hashlib.sha256(raw).hexdigest()==meta['sha256']
    return raw

def test_complete_jcse_original_holds_conflicting_reporting_currency():
    raw=original();proof=currency.declaration(raw,url=URL)
    assert proof['status']=='ambiguous'
    assert {r['currency'] for r in proof['declarations']}=={'SGD','USD'}
    with pytest.raises(currency.StatementCurrencyError,match='ambiguous'):
        currency.accounting_basis(raw,url=URL)

def test_previous_cached_proof_from_conflicting_original_is_rejected():
    digest=hashlib.sha256(original()).hexdigest()
    with pytest.raises(currency.StatementCurrencyError,match='ambiguous'):
        currency.bind({'evidence':{}},{'source_sha256':digest})
    assert not currency.cached_currency_valid({'inline_filing_supplement':{
        'native_reporting':{},'statement_currency_declaration':{'source_sha256':digest}}})

def test_complete_reader_returns_identity_only_for_conflicting_original(monkeypatch):
    from datetime import datetime, timezone
    import socket
    from app.services import sec_fundamentals as sec
    from app.services.independent_fundamentals import IndependentMetricsEligible
    root=Path(__file__).parent/'fixtures/sec_currency_conflicts'
    manifest=json.loads((root/'sources.json').read_bytes());sources={}
    for key,meta in manifest.items():
        raw=gzip.decompress((root/meta['file']).read_bytes())
        assert len(raw)==meta['bytes'] and hashlib.sha256(raw).hexdigest()==meta['sha256']
        url=meta.get('url') or ('https://data.sec.gov/submissions/CIK0001905511.json' if key=='company' else 'https://data.sec.gov/api/xbrl/companyfacts/CIK0001905511.json')
        sources[url]=raw
    class Clock(datetime):
        @classmethod
        def now(cls,tz=None):return datetime(2026,11,16,4,30,tzinfo=timezone.utc)
    calls=[]
    class Client:
        def get(self,url):calls.append(url);return sources[url]
    monkeypatch.setattr(socket.socket,'connect',lambda *a,**k:pytest.fail('Network forbidden'))
    monkeypatch.setattr(sec,'datetime',Clock)
    monkeypatch.setattr(sec,'company_directory',lambda _: {'JCSE':{'cik':'0001905511'}})
    monkeypatch.setattr('app.clients.direct_sources.DirectSourceClient',Client)
    with pytest.raises(IndependentMetricsEligible,match='statement_currency_ambiguous') as caught:
        sec.fetch_fundamentals('JCSE')
    value=caught.value.values;evidence=json.loads(value['source_evidence_json'])
    assert value['period_date'] is None and evidence['financial_status']=='unavailable'
    assert not any(key in value for key in ('roe','current_ratio','free_cash_flow','fcf_growth'))
    first=list(calls);calls.clear()
    with pytest.raises(IndependentMetricsEligible) as repeat:sec.fetch_fundamentals('JCSE')
    assert repeat.value.values==value and calls==first
