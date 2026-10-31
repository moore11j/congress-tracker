"""Complete public filings and adversarial IFRS statement-scope validation."""
from copy import deepcopy
from datetime import date, datetime, timezone
from pathlib import Path
from types import SimpleNamespace
import gzip, hashlib, json, socket
import pytest
from lxml import html
from app.services import sec_statement_declarations as declarations
from app.services import sec_tabular_financials as tables, sec_tabular_reporting as reporting

ROOT = Path(__file__).parent / 'fixtures/sec_ifrs_declarations'
NOW = datetime(2026, 10, 31, 12, tzinfo=timezone.utc)
OWNER = 'Spotify Technology S.A.'
DECLARATION = ('The interim condensed consolidated financial statements of '+OWNER+
               ' for the three and nine months ended September 30, 2026 and 2025 '
               'have been prepared in accordance with IAS 34 Interim Financial Reporting.')


@pytest.fixture(autouse=True)
def offline(monkeypatch):
    monkeypatch.setattr(socket.socket, 'connect', lambda *a, **k: pytest.fail('Unexpected network'))


def sources(symbol='SPOT', index=0):
    manifest = json.loads((ROOT/'sources.json').read_text(encoding='utf-8'))['sources']
    selection = next(s for s in json.loads((ROOT/'selections.json').read_text(encoding='utf-8')) if s['symbol']==symbol)
    originals = {}
    for item in manifest:
        raw = gzip.decompress((ROOT/item['file']).read_bytes())
        assert len(raw)==item['bytes'] and hashlib.sha256(raw).hexdigest()==item['sha256']
        originals[item['url']] = raw
    target = next(m for m in manifest if m['file']==f'{symbol}-original-{index}.html.gz')
    company = originals[f'https://data.sec.gov/submissions/CIK{selection["cik"]}.json']
    args = dict(symbol=symbol, cik=selection['cik'], source_url=target['url'], company_raw=company,
                cover_raw=originals[selection['cover_url']], cover_url=selection['cover_url'],
                available_by=date(2026,10,30))
    return originals[target['url']], args, originals, selection


@pytest.mark.parametrize('standard', ['IAS 34 Interim Financial Reporting', 'International Accounting Standard 34',
                                    'IFRS Accounting Standards', 'International Financial Reporting Standards'])
def test_explicit_preparation_statement(standard):
    text=DECLARATION.replace('IAS 34 Interim Financial Reporting',standard)
    assert declarations.declarations(text)
    assert declarations.named_statement_scope(text,OWNER,tables.company_key)['issuer']==OWNER


@pytest.mark.parametrize('text', [
    DECLARATION.replace('prepared in accordance','not prepared in accordance'),
    DECLARATION.replace('have been prepared','would be prepared'),
    DECLARATION.replace('have been prepared','have never been prepared'),
    'Hypothetical '+DECLARATION,
    'Not '+DECLARATION,
    'Financial statements are important. Other results are prepared in accordance with IFRS.',
    'Our financial statements mention IAS 34.',
])
def test_non_declarations_cannot_establish_named_statement_scope(text):
    assert declarations.named_statement_scope(text,OWNER,tables.company_key) is None


@pytest.mark.parametrize('decoration', ['<blockquote>{}</blockquote>', '<script>{}</script>', '<template>{}</template>'])
def test_invisible_or_quoted_declaration_does_not_set_basis(decoration):
    raw=('<html><p>No accounting declaration.</p>'+decoration.format(DECLARATION)+'</html>').encode()
    with pytest.raises(reporting.TabularReportingError,match='accounting_basis_unproved'):
        reporting.declaration(raw,{'facts':[{'currency':'EUR'}]})


def test_different_explicit_owners_disable_document_scope():
    for extra in ['Other Holdings Consolidated Statements of Income',
                  DECLARATION.replace(OWNER,'Other Holdings'),
                  'Other Holdings Reports Third Quarter 2026 Financial Results']:
        tree=html.fromstring(f'<html><p>{DECLARATION}</p><p>{extra}</p></html>')
        assert tables.document_statement_scope(tree,OWNER) is None


def test_mixed_accounting_basis_is_held():
    raw=(DECLARATION+' Financial statements are prepared in accordance with US GAAP.').encode()
    with pytest.raises(reporting.TabularReportingError,match='accounting_basis_conflict'):
        reporting.declaration(raw,{'facts':[{'currency':'EUR'}]})


def test_real_spotify_original_normalization_and_public_contract_repeat():
    from app.services import sec_financial_statements as panels
    raw,args,originals,selection=sources()
    result=reporting.normalize(raw,**args)
    assert result==reporting.normalize(raw,**args)
    proof=result['evidence']
    assert proof['accounting_basis']=='IFRS' and proof['currency']=='EUR'
    assert proof['native_observation_count']==16 and proof['current_period']=='2026-09-30'
    assert proof['accounting_policy']==declarations.POLICY
    # Printed EUR millions, not dollar quotes, annualized YTD, or guessed income.
    assert result['records']['revenue'][date(2026,7,1),date(2026,9,30)]['value']==4969_000_000
    assert result['records']['revenue'][date(2025,7,1),date(2025,9,30)]['value']==4272_000_000
    assert result['records']['revenue'][date(2026,1,1),date(2026,9,30)]['value']==14279_000_000
    for key in ('net_income','operating_cash_flow','capex','eps','equity','current_assets','current_liabilities'):
        assert not result['records'][key]
    class Client:
        calls=[]
        def get(self,url):self.calls.append(url);return originals[url]
    client=Client()
    supplement=reporting.acquire(args['company_raw'],symbol='SPOT',cik=args['cik'],
        available_by=args['available_by'],client=client,candidate_accessions=(selection['accession'],))
    assert client.calls==[selection['cover_url']]
    # The HTML panel uses only CIK identity from companyfacts; this synthetic
    # identity-only document deliberately supplies no financial observations.
    facts=json.dumps({'cik':int(args['cik'])}).encode()
    kw=dict(symbol='SPOT',cik=args['cik'],observed_at=NOW,supplement=supplement)
    panel=reporting.panel(facts,args['company_raw'],**kw)
    assert panel==reporting.panel(facts,args['company_raw'],**kw)
    assert panel['status']=='partial' and len(panel['quarterly'])==2
    assert not panel['annual'] and not panel['semiannual']
    assert panel['quarterly'][-1]['revenue']==4969_000_000
    assert panel['summary']['revenueTtm'] is None and panel['summary']['currentRatio'] is None
    assert all(row['netIncome'] is None and row['freeCashFlow'] is None for row in panel['quarterly'])
    row=SimpleNamespace(source='sec_edgar',fetched_at=NOW,payload_json=json.dumps(panel))
    assert panels.cached_row_payload(row,'SPOT',now=NOW)==panel
    changed=deepcopy(proof);changed.pop('accounting_policy')
    assert not reporting.cached_valid(changed,symbol='SPOT',cik=args['cik'])


@pytest.mark.parametrize('symbol,index', [('CINT',0),('ONON',1),('FVRR',0)])
def test_other_complete_sources_retain_unproved_scope_or_basis_holds(symbol,index):
    raw,args,_,_=sources(symbol,index)
    with pytest.raises(reporting.TabularReportingError,match='table_(native_observations_unavailable|accounting_basis_unproved|reporting_currency_unproved)'):
        reporting.normalize(raw,**args)


def test_accounting_hold_retries_once_without_invalidating_qualified_receipts():
    from test_sec_financial_discovery_reader import inspect
    from test_sec_financial_discovery import complete, NOW as stamp
    from app.services import sec_financial_discovery_reader as reader
    result=inspect(); receipt=result['receipt']
    cover=next(r for r in complete()[0]['state']['receipts'].values() if r['candidate']==receipt['candidate'])
    assert reader.validate_receipt(receipt,cover,now=stamp)==receipt
    held=deepcopy(receipt)
    held.update(status='held',kind=None,current_period=None,
        holds=[{'accession':receipt['candidate']['accession'],'reason':'table_accounting_basis_unproved'}])
    assert reader.validate_receipt(held,cover,now=stamp) is None
    held['accounting_policy']=declarations.POLICY
    assert reader.validate_receipt(held,cover,now=stamp)==held
    held['holds'][0]['reason']='table_current_statement_stale'
    held.pop('accounting_policy')
    assert reader.validate_receipt(held,cover,now=stamp)==held


@pytest.mark.parametrize('text', [
    'Financial statements are prepared in accordance with IFRS 9.',
    'Financial statements are prepared in accordance with IFRS17.',
    'Financial statements will be prepared in accordance with IFRS.',
    'Financial statements would be prepared in accordance with IFRS.',
    'Financial statements are expected to be prepared in accordance with IFRS.',
    'Financial statements are prepared in accordance with IAS 1.',
])
def test_single_standard_or_future_intention_is_not_a_reporting_basis(text):
    assert not declarations.declarations(text)


@pytest.mark.parametrize('heading', [
    'Consolidated Statements of Income of Other Holdings',
    'Interim condensed consolidated statement of financial position for Other Holdings',
    'Consolidated Statements of Cash Flows excluding the reporting company',
])
def test_unnamed_statement_title_cannot_smuggle_a_different_owner(heading):
    tree=html.fromstring(f'<html><p>{DECLARATION}</p><p>{heading}</p></html>')
    assert tables.document_statement_scope(tree,OWNER) is None
