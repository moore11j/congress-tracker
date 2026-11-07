from pathlib import Path
from datetime import date,datetime,timezone
import gzip,hashlib,json
from copy import deepcopy
from lxml import html
import pytest
from app.services import sec_tabular_financials as tab,sec_tabular_reporting as reporting,sec_statement_layout as layout

ROOT=Path(__file__).parent/'fixtures/sec_caption_rows_nov7'
PROOF=json.loads((ROOT/'manifest.json').read_bytes())
def original(url):
    p=PROOF['originals'][url];raw=gzip.decompress((ROOT/p['file']).read_bytes())
    assert len(raw)==p['bytes'] and hashlib.sha256(raw).hexdigest()==p['sha256'];return raw
RAW=original(PROOF['statement_url']);COMPANY=original(PROOF['company_url'])
ARGS=dict(source_url=PROOF['statement_url'],cover_url=PROOF['cover_url'],cover_raw=original(PROOF['cover_url']),
          company_raw=COMPANY,symbol=PROOF['symbol'],cik=PROOF['cik'],available_by=date.fromisoformat(PROOF['available_by']))

def test_complete_original_statements_periods_units_and_repeat():
    obs=tab.extract(RAW,**ARGS)
    assert obs==tab.extract(RAW,**ARGS)
    assert len(obs['tables'])==3 and len(obs['facts'])==20 and not obs['holds'] and not obs['conflicts']
    assert {f['currency'] for f in obs['facts']}=={'USD'}
    values={(f['metric'],f['start'],f['end']):f['value'] for f in obs['facts']}
    assert values['revenue','2026-04-01','2026-06-30']==11534000
    assert values['revenue','2026-01-01','2026-06-30']==21970000
    assert values['consolidated_income','2026-04-01','2026-06-30']==-2744000
    assert values['operating_cash_flow','2026-04-01','2026-06-30']==674000
    assert values['operating_cash_flow','2026-01-01','2026-06-30']==289000
    assert values['current_assets',None,'2026-06-30']==34428000
    assert not {'parent_income','ebitda','eps','capex'} & {f['metric'] for f in obs['facts']}
    assert {o['table'] for f in obs['facts'] for o in f['occurrences']}=={6,8,12}
    assert obs['source']['statement_caption_rows_policy']==layout.CAPTION_ROWS_POLICY
    norm=reporting.normalize(RAW,**ARGS)
    assert norm==reporting.normalize(RAW,**ARGS)
    assert norm['evidence']['accounting_basis']=='US GAAP' and norm['evidence']['currency']=='USD'
    assert norm['evidence']['current_period']=='2026-06-30'
    assert norm['evidence']['accounting_declarations']==['The Company reports financial results in accordance with U.S. GAAP']

def caption_tree(owner='Example Ltd.',currency='U.S dollars in thousands',heading='Consolidated Balance Sheets',between='<p></p>'):
    return html.fromstring(f'<div><table><tr><td>{owner}</td></tr><tr><td>{heading}</td></tr><tr><td>{currency}</td></tr></table>{between}<table id="values"><tr><td>Total assets</td><td>10</td></tr></table></div>')

@pytest.mark.parametrize('owner',['Other Ltd.','Example Subsidiary Ltd.'])
def test_explicit_adjacent_owner_cannot_be_replaced(owner):
    tree=caption_tree(owner=owner)
    with pytest.raises(tab.TabularFinancialError,match='layout_statement_owner_unproved'):
        layout.context(tree.get_element_by_id('values'),'Example Ltd.')

@pytest.mark.parametrize('currency',['dollars in thousands','Canadian dollars in thousands','U.S dollars in thousands and CAD','U.S dollars in unknown'])
def test_unreviewed_currency_or_scale_is_held(currency):
    tree=caption_tree(currency=currency)
    with pytest.raises(tab.TabularFinancialError,match='layout_currency_caption_unproved'):
        layout.context(tree.get_element_by_id('values'),'Example Ltd.')

@pytest.mark.parametrize('between',['<p>Another company</p>','<table><tr><td>Intervening table</td></tr></table>'])
def test_caption_does_not_cross_substantive_content(between):
    tree=caption_tree(between=between)
    assert layout.context(tree.get_element_by_id('values'),'Example Ltd.') is None

@pytest.mark.parametrize('heading',['Unaudited Non-GAAP EBITDA Reconciliations','Adjusted Consolidated Statements of Income','Pro forma Consolidated Balance Sheets'])
def test_adjusted_or_non_gaap_table_is_excluded(heading):
    tree=caption_tree(heading=heading)
    assert layout.context(tree.get_element_by_id('values'),'Example Ltd.') is None

@pytest.mark.parametrize('prefix',['not ','hypothetical ','if '])
def test_explicit_us_gaap_reporting_declaration_rejects_negation(prefix):
    raw=('<p>'+prefix+'The Company reports financial results in accordance with U.S. GAAP</p>').encode()
    with pytest.raises(reporting.TabularReportingError,match='table_accounting_basis_unproved'):
        reporting.declaration(raw,{'facts':[{'currency':'USD'}]})


@pytest.fixture(scope='module')
def qualified_original():
    from app.services import sec_financial_discovery as discovery,sec_financial_discovery_reader as reader
    now=datetime(2026,11,7,12,tzinfo=timezone.utc)
    kw=dict(symbol=PROOF['symbol'],cik=PROOF['cik'],available_by=ARGS['available_by'],observed_at=now)
    state=discovery.inspect(COMPANY,**kw)['state']
    candidate=next(c for c in discovery._inventory(COMPANY,symbol=kw['symbol'],cik=kw['cik'],available_by=kw['available_by'])
                   if c['cover_url']==PROOF['cover_url'])
    key=discovery._fingerprint(candidate)
    cover=discovery._observe(original(candidate['cover_url']),candidate,now)
    state['receipts'][key]=cover
    class Originals:
        def __init__(self):self.calls=[]
        def get(self,url):self.calls.append(url);return original(url)
    facts=json.dumps({'cik':int(PROOF['cik']),'facts':{}}).encode()
    client=Originals()
    result=reader.inspect_candidate(COMPANY,facts,checkpoint=state,accession=candidate['accession'],client=client,**kw)
    assert client.calls==[PROOF['cover_url'],PROOF['statement_url']]
    assert result==reader.inspect_candidate(COMPANY,facts,checkpoint=state,accession=candidate['accession'],client=Originals(),**kw)
    assert result['receipt']['status']=='qualified' and result['receipt']['kind']=='html_table'
    assert not result['receipt']['financial_publication_eligible']
    return result,cover,kw,facts


def test_original_reaches_existing_panel_without_invented_ttm_or_parent_income(qualified_original):
    from app.services import sec_financial_statements as statements,sec_fundamentals as core
    from types import SimpleNamespace
    result,cover,kw,facts=qualified_original
    args=dict(symbol=kw['symbol'],cik=kw['cik'],observed_at=kw['observed_at'],filing_supplement=result['supplement'])
    panel=statements.project(facts,COMPANY,**args)
    assert panel==statements.project(facts,COMPANY,**args)
    assert panel['status']=='partial' and not panel['annual']
    assert panel['quarterly'][-1]['revenue']==11534000
    assert all(row['netIncome'] is None and row['freeCashFlow'] is None for row in panel['quarterly'])
    assert panel['summary']['revenueTtm'] is None and panel['summary']['netIncomeTtm'] is None
    row=SimpleNamespace(source='sec_edgar',fetched_at=kw['observed_at'],payload_json=json.dumps(panel))
    assert statements.cached_row_payload(row,kw['symbol'],now=kw['observed_at'])==panel
    values=core.project_fundamentals(facts,COMPANY,**args)
    assert values==core.project_fundamentals(facts,COMPANY,**args)
    assert values['current_ratio']==panel['summary']['currentRatio']
    assert not {'free_cash_flow','roe','net_margin','ebitda'} & values.keys()


@pytest.mark.parametrize('reason',['financial_hint_not_qualified','table_accounting_basis_unproved'])
def test_only_legacy_relevant_holds_retry_under_existing_worker_bounds(qualified_original,reason):
    from app.services import sec_financial_discovery_reader as reader
    from app.services import sec_statement_declarations,sec_comparative_basis,sec_statement_sections,sec_statement_heading_order
    result,cover,kw,facts=qualified_original
    receipt=deepcopy(result['receipt'])
    assert reader.validate_receipt(receipt,cover,now=kw['observed_at'])==receipt
    receipt.update(status='held',kind=None,current_period=None,
        holds=[dict(accession=receipt['candidate']['accession'],reason=reason)],
        accounting_policy=sec_statement_declarations.POLICY,comparative_policy=sec_comparative_basis.POLICY,
        statement_section_policy=sec_statement_sections.POLICY,statement_heading_policy=sec_statement_heading_order.POLICY,
        statement_layout_policy=layout.POLICY)
    assert reader.validate_receipt(receipt,cover,now=kw['observed_at']) is None
    receipt['statement_caption_rows_policy']=layout.CAPTION_ROWS_POLICY
    assert reader.validate_receipt(receipt,cover,now=kw['observed_at']) is None
    from app.services.sec_statement_font_context import POLICY as font_policy
    receipt['statement_font_policy']=font_policy
    assert reader.validate_receipt(receipt,cover,now=kw['observed_at'])==receipt
    receipt['holds'][0]['reason']='table_translation_column_unproved'
    receipt.pop('statement_caption_rows_policy')
    assert reader.validate_receipt(receipt,cover,now=kw['observed_at'])==receipt
