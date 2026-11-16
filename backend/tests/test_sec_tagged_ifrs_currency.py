"""Full original, ambiguity, identity, unchanged-reader and consumer checks."""
from copy import deepcopy
from datetime import date,datetime,timezone
from pathlib import Path
from types import SimpleNamespace
import gzip,hashlib,json,socket
import pytest
from lxml import html
from app.services import sec_statement_currency as native,sec_tagged_ifrs_currency as tagged
from app.services import sec_document_sets as sets,sec_inline_financials as inline

ROOT=Path(__file__).parent/'fixtures/sec_tagged_ifrs_currency'
MANIFEST=json.loads((ROOT/'manifest.json').read_text())
CIK='0001709048';ACCESSION='0001709048-26-000261'
COMPANY='https://data.sec.gov/submissions/CIK'+CIK+'.json'
FACTS='https://data.sec.gov/api/xbrl/companyfacts/CIK'+CIK+'.json'
COVER=MANIFEST['selections']['GFS']['url']
STATEMENT=MANIFEST['selections']['GFS']['statement_links'][0]
NOW=datetime(2026,11,9,tzinfo=timezone.utc);DAY=NOW.date()


def source(url):
    meta=MANIFEST['sources'][url];raw=gzip.decompress((ROOT/meta['file']).read_bytes())
    assert len(raw)==meta['bytes'] and hashlib.sha256(raw).hexdigest()==meta['sha256']
    return raw


class Client:
    def __init__(self,overrides=None):self.overrides=overrides or {};self.calls=[]
    def get(self,url):
        self.calls.append(url)
        return self.overrides[url] if url in self.overrides else source(url)


def candidate():
    return next(c for c in sets.candidates(source(COMPANY),symbol='GFS',cik=CIK,available_by=DAY) if c['accession']==ACCESSION)


def acquire(client=None):
    return native.acquire_candidate(candidate(),source(COMPANY),symbol='GFS',cik=CIK,available_by=DAY,client=client or Client())


def supplement(client=None):
    return native.acquire(json.loads(source(FACTS)),source(COMPANY),symbol='GFS',cik=CIK,
        available_by=DAY,client=client or Client(),candidate_accessions=(ACCESSION,))


@pytest.fixture(autouse=True)
def offline(monkeypatch):
    monkeypatch.setattr(socket.socket,'connect',lambda *a,**k:pytest.fail('Unexpected network'))


def test_actual_complete_original_single_currency_and_exact_repeat():
    client=Client();parsed=acquire(client)
    assert client.calls==[COVER,STATEMENT]
    assert parsed['evidence']['reported_facts']==40
    proof=parsed['evidence']['statement_currency_declaration']
    assert proof['methodology']==tagged.POLICY and proof['status']=='tagged'
    assert len(proof['anchors'])==7 and proof['currency']=='USD'
    assert native.declaration(source(STATEMENT),url=STATEMENT)['status']=='unavailable'
    assert native.bind(parsed,proof)==proof
    assert native.bind({'evidence':parsed['evidence']},proof)==proof
    assert acquire()==parsed
    assert len(native.normalized_records(parsed,source(COMPANY),symbol='GFS',cik=CIK,available_by=DAY)['revenue'])==4


def mutate(kind):
    cover=html.fromstring(source(COVER));statement=html.fromstring(source(STATEMENT))
    if kind=='no_current_declaration':
        for node in statement.iter():
            if node.text:node.text=node.text.replace('interim financial statements','interim information').replace('interim condensed consolidated financial statements','interim condensed consolidated information')
    elif kind=='future_declaration':
        for node in statement.iter():
            if node.text:node.text=node.text.replace('have been prepared','will be prepared')
    elif kind in {'EUR','mixed_currency'}:
        unit=next(n for n in cover.iter() if n.tag=='xbrli:unit' and n.get('id')=='usd')
        if kind=='EUR':unit[0].text='iso4217:EUR'
        else:
            second=deepcopy(unit);second.set('id','eur');second[0].text='iso4217:EUR';unit.addnext(second)
            node=next(n for n in statement.iter() if n.get('name')=='ifrs-full:Assets')
            node.set('unitref','eur')
    elif kind=='wrong_cik':
        for node in cover.iter():
            if node.tag=='xbrli:identifier':node.text='0000000001'
    elif kind=='missing_anchor':
        for node in list(statement.iter()):
            if node.get('name')=='ifrs-full:Assets':node.getparent().remove(node)
    elif kind=='wrong_namespace':statement.set('xmlns:ifrs-full','https://example.invalid/ifrs-full')
    elif kind=='explicit_currency_conflict':
        statement.find('body').insert(0,html.fromstring('<p>Our reporting currency is EUR.</p>'))
    elif kind=='cover_currency_conflict':
        cover.find('body').insert(0,html.fromstring('<p>Our reporting currency is EUR.</p>'))
    elif kind=='accounting_conflict':
        statement.find('body').insert(0,html.fromstring('<p>Our financial statements are prepared in accordance with U.S. GAAP.</p>'))
    elif kind=='duplicate_fact_conflict':
        node=next(n for n in statement.iter() if n.get('name')=='ifrs-full:Assets')
        other=deepcopy(node);other.text='999';other.set('id','conflicting-assets');node.addnext(other)
    elif kind=='nil_anchor':
        for node in statement.iter():
            if node.get('name')=='ifrs-full:Assets':node.set('xsi:nil','true')
    elif kind=='wrong_period':
        for node in cover.iter():
            if node.tag in {'xbrli:instant','xbrli:enddate'} and node.text=='2026-09-30':node.text='2026-06-30'
    elif kind!='unchanged':raise AssertionError(kind)
    return {COVER:html.tostring(cover,encoding='ascii'),STATEMENT:html.tostring(statement,encoding='ascii')}


def test_serialized_complete_original_control():
    result=acquire(Client(mutate('unchanged')))
    assert result['facts']==acquire()['facts']


@pytest.mark.parametrize('kind',['no_current_declaration','future_declaration','EUR','mixed_currency','wrong_cik',
    'missing_anchor','wrong_namespace','explicit_currency_conflict','cover_currency_conflict','accounting_conflict',
    'duplicate_fact_conflict','nil_anchor','wrong_period'])
def test_complete_original_mutations_do_not_authorize_new_basis(kind):
    client=Client(mutate(kind))
    with pytest.raises((native.StatementCurrencyError,inline.InlineFactsError)):
        tagged.acquire_candidate(candidate(),source(COMPANY),symbol='GFS',cik=CIK,available_by=DAY,client=client)
    assert len(set(client.calls))<=2


@pytest.mark.parametrize('field,value',[('currency','EUR'),('namespace','us-gaap'),('source_sha256','0'*64),
    ('cik','0000000001'),('accession','0000000001-26-000001'),('report_date','2026-06-30'),
    ('filed','2026-11-05'),('status','declared'),('statement_declarations',[]),('anchors',[]),('binding_sha256','0'*64)])
def test_changed_proof_is_rejected(field,value):
    parsed=acquire();proof=deepcopy(parsed['evidence']['statement_currency_declaration']);proof[field]=value
    with pytest.raises(native.StatementCurrencyError):native.bind(parsed,proof)


def test_changed_fact_value_and_ambiguous_currency_are_rejected():
    parsed=acquire();proof=parsed['evidence']['statement_currency_declaration']
    changed=deepcopy(parsed);changed['facts']['Assets']['units']['USD'][0]['val']+=1
    with pytest.raises(native.StatementCurrencyError):native.bind(changed,proof)
    changed=deepcopy(parsed);changed['evidence']['currency_observation']['current_statement_currencies']=['EUR','USD']
    with pytest.raises(native.StatementCurrencyError):native.bind(changed,proof)


@pytest.mark.parametrize('symbol',['CHKP','DOX'])
def test_plain_dollar_sign_and_generic_gaap_originals_remain_held(symbol):
    row=MANIFEST['selections'][symbol];company=source('https://data.sec.gov/submissions/CIK'+row['cik']+'.json')
    c=next(c for c in sets.candidates(company,symbol=symbol,cik=row['cik'],available_by=DAY) if c['accession']==row['accession'])
    with pytest.raises(native.StatementCurrencyError):
        native.acquire_candidate(c,company,symbol=symbol,cik=row['cik'],available_by=DAY,client=Client())


def test_source_bound_core_panel_cache_and_repeat():
    from app.services import sec_fundamentals as core,sec_financial_statements as panels
    from app.services.sec_tagged_foreign import cached_currency_valid
    parsed=supplement();kw=dict(symbol='GFS',cik=CIK,observed_at=NOW,filing_supplement=parsed)
    values=core.project_fundamentals(source(FACTS),source(COMPANY),**kw)
    assert values['period_date']==date(2026,9,30)
    expected=dict(current_ratio=5758/2236,free_cash_flow=430_000_000,
        gross_margin=2035/7149*100,operating_margin=839/7149*100,net_margin=197/7149*100,
        revenue_growth=(7149/6791-1)*100,roe=1.6665961676748022,
        operating_margin_expansion=14.077241238563403,fcf_margin=430/7149*100,fcf_growth=-63.0901287553648)
    for key,value in expected.items():assert values[key]==pytest.approx(value)
    assert 'net_debt_to_ebitda' not in values and 'eps_growth' not in values
    assert core.project_fundamentals(source(FACTS),source(COMPANY),**kw)==values
    assert cached_currency_valid(json.loads(values['source_evidence_json']))
    panel=panels.project(source(FACTS),source(COMPANY),**kw)
    assert panel==panels.project(source(FACTS),source(COMPANY),**kw)
    assert panel['reportingCurrency']=='USD' and panel['quarterly']
    row=SimpleNamespace(source='sec_edgar',fetched_at=NOW,payload_json=json.dumps(panel))
    assert panels.cached_row_payload(row,'GFS',now=NOW)==panel
    bad=deepcopy(panel);bad['reportingCurrency']='EUR';row.payload_json=json.dumps(bad)
    assert panels.cached_row_payload(row,'GFS',now=NOW) is None
