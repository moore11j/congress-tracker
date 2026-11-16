"""Explicit month-period captions, complete originals and retained ambiguity."""
from copy import deepcopy
from datetime import date,datetime,timezone
from pathlib import Path
import gzip,hashlib,json,socket
import pytest
from app.services import sec_tabular_financials as tables,sec_tabular_reporting as reporting

ROOT=Path(__file__).parent/'fixtures/sec_us_conformity'


@pytest.fixture(autouse=True)
def offline(monkeypatch):
    monkeypatch.setattr(socket.socket,'connect',lambda *a,**k:pytest.fail('Unexpected network'))


@pytest.mark.parametrize('word,months',[('Three',3),('Six',6),('Nine',9),('Twelve',12),('3',3),('6',6),('9',9),('12',12)])
@pytest.mark.parametrize('separator',['-',' '])
def test_explicit_period_has_exact_start_and_end(word,months,separator):
    header=f'{word}{separator}Month Period Ended December 31, 2026'
    assert tables.period_for(header,'income',date(2027,2,1))==(date(2026,13-months,1),date(2026,12,31),months)


@pytest.mark.parametrize('header',[
    'Two-Month Period Ended September 30, 2026',
    '13-Month Period Ended September 30, 2026',
    '-3-Month Period Ended September 30, 2026',
    'Three or Nine-Month Period Ended September 30, 2026',
    'Projected Three-Month Period Ended September 30, 2026',
    'Not Three-Month Period Ended September 30, 2026',
    'Three-Month Period Ending September 30, 2026',
    'Three-Month Period Ended September 29, 2026',
    'Three-Month Period Ended September 30, 2025 and 2026',
    'Three-Month Period Ended September 30, 2026 and Six months ended September 30, 2026',
    'Three-Month Period Ended September 30, 2027',
    'Three-Month Period Ended February 30, 2026',
])
def test_unsupported_ambiguous_projected_or_future_period_stays_held(header):
    with pytest.raises(tables.TabularFinancialError):
        tables.period_for(header,'income',date(2026,11,15))


def test_shared_caption_cannot_override_a_conflicting_column():
    header='Three-Month Period Ended September 30, 2025 and 2026'
    texts=['Three-Month Period Ended September 30,','2026']
    expected=(date(2026,7,1),date(2026,9,30),3)
    assert tables.column_period(header,texts,[header],'income',date(2026,11,15))==expected
    with pytest.raises(tables.TabularFinancialError):
        tables.column_period(header,texts,[header.replace('Three','Nine')],'income',date(2026,11,15))


def originals():
    manifest=json.loads((ROOT/'sources.json').read_bytes())
    company=manifest['other_companies']['SB']
    body=gzip.decompress((ROOT/company['file']).read_bytes())
    assert len(body)==company['bytes'] and hashlib.sha256(body).hexdigest()==company['sha256']
    result=[]
    for item in manifest['sources']:
        if item['candidate']['symbol']!='SB':continue
        raw=gzip.decompress((ROOT/item['file']).read_bytes())
        assert len(raw)==item['bytes'] and hashlib.sha256(raw).hexdigest()==item['sha256']
        if b'in conformity with' not in raw:continue
        c=item['candidate']
        kw=dict(source_url=item['url'],cover_url=c['cover_url'],cover_raw=raw,
                company_raw=body,symbol='SB',cik=c['cik'],available_by=date(2026,11,15))
        result.append((raw,kw))
    assert len(result)==2
    return result


@pytest.mark.parametrize('index,end,quarter_start,income,cash',[
    (0,date(2026,6,30),date(2026,4,1),40016000,77883000),
    (1,date(2026,9,30),date(2026,7,1),45868000,136196000),
])
def test_complete_sources_recover_periods_preserving_revenue_conflicts(monkeypatch,index,end,quarter_start,income,cash):
    raw,kw=originals()[index]
    result=reporting.normalize(raw,**kw)
    assert result==reporting.normalize(raw,**kw)
    assert result['evidence']['current_period']==str(end)
    assert result['evidence']['native_observation_count']==12
    assert result['evidence']['currency']=='USD'
    assert not result['evidence']['holds']
    assert result['records']['operating_income'][quarter_start,end]['value']==income
    assert result['records']['operating_cash_flow'][date(2026,1,1),end]['value']==cash
    # Consolidated earnings cannot become parent/common-holder income.
    assert not result['records']['net_income'] and not result['records']['equity']
    assert len(result['records']['consolidated_income'])==4
    # Both gross and net revenue are printed. Do not silently choose one.
    assert len(result['evidence']['conflicts'])==4
    assert len(result['records']['revenue'])==4 and all(v is None for v in result['records']['revenue'].values())
    assert reporting.cached_valid(result['evidence'],symbol='SB',cik='0001434754')
    for record in result['records']['operating_income'].values():
        assert any('Month Period Ended' in header for occurrence in record['source_occurrences'] for header in occurrence['header'])
    monkeypatch.setattr(tables,'month_period_wording',lambda text:text)
    with pytest.raises(reporting.TabularReportingError,match='table_current_statement_alignment'):
        reporting.normalize(raw,**kw)


def test_changed_column_date_and_wrong_issuer_stay_held():
    raw,kw=originals()[1]
    with pytest.raises(ValueError):
        reporting.normalize(raw,**{**kw,'symbol':'OTHER'})
    altered=raw.replace(b'September 30,',b'September 29,')
    assert altered!=raw
    with pytest.raises(ValueError):reporting.normalize(altered,**kw)


def test_legacy_alignment_hold_retries_once_and_other_receipts_stay_stable():
    from app.services import sec_financial_discovery_reader as reader
    from app.services.sec_statement_font_context import POLICY as font_policy
    from test_sec_financial_discovery_reader import inspect
    from test_sec_financial_discovery import complete,NOW
    receipt=inspect()['receipt']
    cover=next(r for r in complete()[0]['state']['receipts'].values() if r['candidate']==receipt['candidate'])
    assert reader.validate_receipt(receipt,cover,now=NOW)==receipt
    held=deepcopy(receipt)
    held.update(status='held',kind=None,current_period=None,statement_font_policy=font_policy,
                holds=[dict(accession=receipt['candidate']['accession'],reason='table_current_statement_alignment')])
    assert reader.validate_receipt(held,cover,now=NOW) is None
    held['period_wording_policy']=tables.PERIOD_WORDING_POLICY
    assert reader.validate_receipt(held,cover,now=NOW)==held
    held['holds'][0]['reason']='table_current_statement_stale'
    held.pop('period_wording_policy')
    assert reader.validate_receipt(held,cover,now=NOW)==held


def test_complete_current_filing_qualifies_in_actual_discovery_reader():
    from app.services import sec_financial_discovery as discovery,sec_financial_discovery_reader as reader
    raw,kw=originals()[1]
    root=Path(__file__).parent/'fixtures/sec_month_period_headers'
    meta=json.loads((root/'sources.json').read_bytes())['facts']
    facts=gzip.decompress((root/meta['file']).read_bytes())
    assert len(facts)==meta['bytes'] and hashlib.sha256(facts).hexdigest()==meta['sha256']
    now=datetime(2026,11,15,23,tzinfo=timezone.utc)
    common=dict(symbol='SB',cik='0001434754',available_by=kw['available_by'],observed_at=now)
    checkpoint=discovery.inspect(kw['company_raw'],**common)['state']
    from app.services.sec_document_sets import candidates
    candidate=next(c for c in candidates(kw['company_raw'],symbol='SB',cik=common['cik'],available_by=kw['available_by']) if c['cover_url']==kw['source_url'])
    identity={key:candidate[key] for key in discovery.IDENTITY_FIELDS}
    checkpoint['receipts'][discovery._fingerprint(identity)]=discovery._observe(raw,identity,now)
    class Client:
        def __init__(self):self.calls=[]
        def get(self,url):
            self.calls.append(url);assert url==kw['source_url'];return raw
    client=Client()
    result=reader.inspect_candidate(kw['company_raw'],facts,**common,checkpoint=checkpoint,
        accession=candidate['accession'],client=client)
    assert client.calls==[kw['source_url']]
    assert result['receipt']['status']=='qualified' and result['receipt']['current_period']=='2026-09-30'
    assert not result['receipt']['financial_publication_eligible']
    assert result['receipt']['originals'][0]['sha256']==hashlib.sha256(raw).hexdigest()
    assert result==reader.inspect_candidate(kw['company_raw'],facts,**common,checkpoint=checkpoint,
        accession=candidate['accession'],client=Client())
