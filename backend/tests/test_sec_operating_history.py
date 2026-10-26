"""Complete-source operating history, cache and consumer transition checks."""
from copy import deepcopy
from datetime import date,datetime,timezone
from functools import lru_cache
from pathlib import Path
from unittest.mock import patch
import gzip,hashlib,json,socket
import pytest
from app.services import sec_operating_history as op,sec_inline_financials as inline,sec_fundamentals as sec,sec_financial_statements as statements

ROOT=Path(__file__).parent/'fixtures/sec_operating_history'
NOW=datetime(2026,10,26,5,20,tzinfo=timezone.utc)
DAY=date(2026,10,25)
CIK='0001866692'
MANIFEST=json.loads((ROOT/'sources.json').read_text())

@lru_cache(maxsize=4)
def raw(name):
    meta=MANIFEST[name];value=gzip.decompress((ROOT/meta['file']).read_bytes())
    assert len(value)==meta['bytes'] and hashlib.sha256(value).hexdigest()==meta['sha256']
    return value

class Client:
    def __init__(self):self.calls=[]
    def get(self,url):
        self.calls.append(url)
        matches=[n for n,m in MANIFEST.items() if m['url']==url]
        assert len(matches)==1,'Unretained source '+url
        return raw(matches[0])

def inputs():return json.loads(raw('AMPL-facts.json')),json.loads(raw('AMPL-company.json'))

@lru_cache(maxsize=1)
def acquired():
    with patch.object(socket.socket,'connect',side_effect=AssertionError('Unexpected network')):
        client=Client();item=inline.acquire(raw('AMPL-facts.json'),raw('AMPL-company.json'),symbol='AMPL',cik=CIK,observed_at=NOW,client=client)
        assert len(client.calls)==len(set(client.calls))==2
        return item

def project(item):
    return sec.project_fundamentals(raw('AMPL-facts.json'),raw('AMPL-company.json'),symbol='AMPL',cik=CIK,observed_at=NOW,filing_supplement=item)

def test_complete_originals_restore_operating_history_without_changing_other_values():
    before=project(None);item=acquired();after=project(item);proof=item['evidence']['operating_statement_history']
    assert [r['value'] for r in proof['rows']]==[-51865000,-107383000,-96005000]
    assert after['operating_margin']==(-96005000-59314000+51292000)/374369000*100
    old_margin=(-107383000-51292000+51865000)/316571000*100
    assert after['operating_margin_expansion']==after['operating_margin']-old_margin
    old_evidence=json.loads(before['source_evidence_json'])
    assert all(after[k]==before[k] for k in old_evidence['calculations'])
    assert project(item)==after
    evidence=json.loads(after['source_evidence_json'])
    assert evidence['inline_filing_supplement']['operating_statement_history']==proof
    panel=statements.project(raw('AMPL-facts.json'),raw('AMPL-company.json'),symbol='AMPL',cik=CIK,observed_at=NOW,filing_supplement=item)
    assert panel['sourceEvidence']['inlineFilingSupplement']['operating_statement_history']==proof
    assert op.cached_valid(proof,cik=CIK)
    assert all(r['reported_operating_subtotal'] is None for r in proof['rows'])

@pytest.mark.parametrize('change',['hash','value','currency','period','issuer','accession','duplicate','bool','negative_expense','missing','method'])
def test_cached_proof_corruptions_rejected(change):
    proof=deepcopy(acquired()['evidence']['operating_statement_history']);row=proof['rows'][0]
    if change=='hash':row['source_sha256']='bad'
    elif change=='value':row['value']+=1
    elif change=='currency':row['unit']='EUR'
    elif change=='period':row['end']='2030-01-01'
    elif change=='issuer':row['source_cik']='0000000001'
    elif change=='accession':row['accession']='0000000001-26-000001'
    elif change=='duplicate':proof['rows'].append(deepcopy(row))
    elif change=='bool':row['value']=True
    elif change=='negative_expense':row['operating_expenses']=-1
    elif change=='missing':del row['source_sha256']
    else:proof['methodology']='other'
    assert not op.cached_valid(proof,cik=CIK)

@pytest.mark.parametrize('change',['raw','proof','displayed_proof','document_missing'])
def test_consumer_revalidates_originals_and_retained_proof(change):
    item=deepcopy(acquired());history=item['operating_history'];url=next(iter(history['documents']))
    if change=='raw':history['documents'][url]=history['documents'][url].replace(b'159,415',b'159,416')
    elif change=='proof':history['evidence']['rows'][0]['value']+=1
    elif change=='displayed_proof':item['evidence']['operating_statement_history']=None
    else:del history['documents'][url]
    with pytest.raises((ValueError,KeyError)):
        project(item)

@pytest.mark.parametrize('change',['conflict','expense_conflict','different_accession','negative_expense','identity','future','stale'])
def test_planning_never_fills_reported_conflicts_or_unaligned_components(change):
    facts,company=inputs();day=DAY
    expected=op.plan(facts,company,cik=CIK,available_by=day);target=expected['rows'][0]['gross']
    if change=='conflict':
        rows=facts['facts']['us-gaap']['OperatingIncomeLoss']['units']['USD']
        base=dict(start=target['start'],end=target['end'],filed=target['filed'],accn=target['accession'],form='10-Q')
        rows.extend([{**base,'val':1},{**base,'val':2}])
    elif change in ('expense_conflict','different_accession','negative_expense'):
        rows=facts['facts']['us-gaap']['OperatingExpenses']['units']['USD']
        selected=[r for r in rows if r['start']==target['start'] and r['end']==target['end'] and r['filed']==target['filed']]
        assert selected
        if change=='expense_conflict':rows.append({**selected[0],'val':selected[0]['val']+1})
        elif change=='different_accession':
            for r in selected:r['accn']='0000000001-25-000001'
        else:
            for r in selected:r['val']=-1
    elif change=='identity':
        company['cik']=1
        with pytest.raises(ValueError):op.plan(facts,company,cik=CIK,available_by=day)
        return
    elif change=='future':day=date(2024,1,1)
    else:day=date(2030,1,1)
    plan=op.plan(facts,company,cik=CIK,available_by=day)
    assert not plan or all(r['gross']['start']!=target['start'] or r['gross']['end']!=target['end'] for r in plan['rows'])

def test_explicit_calendar_year_header_and_ambiguous_duration():
    from app.services.sec_tabular_financials import period_for,TabularFinancialError
    assert period_for('Year Ended December 31, 2025','income',DAY)==(date(2025,1,1),date(2025,12,31),12)
    for header in ('Three years ended December 31, 2025','3 years ended December 31, 2025','Six months ended Year Ended December 31, 2025'):
        with pytest.raises(TabularFinancialError):period_for(header,'income',DAY)


def test_cache_readers_preserve_proof_and_avoid_refetch(monkeypatch):
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    from app.models import InsightsSnapshot
    from app.services import sec_fundamentals_preparation as prep,sec_directory
    monkeypatch.setattr(socket.socket,'connect',lambda *a,**k:pytest.fail('Unexpected network'))
    monkeypatch.setenv('SEC_FUNDAMENTALS_WARMING_ENABLED','1')
    monkeypatch.setattr(sec_directory,'directory',lambda:{'AMPL':{'cik':CIK}})
    class Clock(datetime):
        @classmethod
        def now(cls,tz=None):return NOW
    engine=create_engine('sqlite://');InsightsSnapshot.__table__.create(engine);factory=sessionmaker(bind=engine);client=Client()
    try:
        with patch.object(prep,'SessionLocal',factory),patch.object(prep,'datetime',Clock),patch.object(prep,'DirectSourceClient',lambda:client):
            payload=prep.prepare('AMPL');assert payload['values']['operating_margin_expansion']==project(acquired())['operating_margin_expansion']
            count=len(client.calls);assert prep.prepare('AMPL')==payload and len(client.calls)==count
        with patch.object(statements,'SessionLocal',factory),patch.object(statements,'datetime',Clock),patch.object(statements,'DirectSourceClient',lambda:client):
            panel=statements.prepared('AMPL');count=len(client.calls);assert statements.prepared('AMPL')==panel and len(client.calls)==count
        with factory() as db:
            for key,field in [('sec-current-fundamentals:AMPL:v1','calculation_evidence'),('sec-financials:AMPL:v1','sourceEvidence')]:
                row=db.get(InsightsSnapshot,key);saved=json.loads(row.payload_json)
                nested='inline_filing_supplement' if field=='calculation_evidence' else 'inlineFilingSupplement'
                saved[field][nested]['operating_statement_history']['rows'][0]['value']+=1
                row.payload_json=json.dumps(saved)
            db.commit();assert statements.cached_payload(db,'AMPL',now=NOW) is None
        with patch.object(prep,'SessionLocal',factory):assert prep._cached('AMPL',NOW) is None
    finally:engine.dispose()


def test_source_score_and_watchlist_recovery_repeat_crossing_rollback(monkeypatch):
    import test_sec_tagged_foreign_consumers as consumer
    urls={m['url']:raw(n) for n,m in MANIFEST.items()}
    monkeypatch.setattr(consumer,'originals',lambda symbol:(raw('AMPL-facts.json'),raw('AMPL-company.json'),CIK,urls))
    values=project(acquired());metrics={k:values[k] for k in json.loads(values['source_evidence_json'])['calculations']}
    monkeypatch.setattr(consumer,'EXPECTED',{'AMPL':{'metrics':metrics}});monkeypatch.setattr(consumer,'NOW',NOW)
    consumer.test_original_source_score_and_alert_recovery_repeat_crossing_rollback('AMPL','revenue_growth',monkeypatch)


@pytest.mark.parametrize('change', ['nil', 'nil_with_content', 'hidden', 'dimensions', 'column_context', 'wrong_issuer', 'subtotal_conflict', 'subtotal_alias', 'subtotal_unreadable'])
def test_complete_statement_rejects_invalid_operating_components(change):
    import re
    from lxml import html, etree
    facts, company = inputs()
    filing = op.plan(facts, company, cik=CIK, available_by=DAY)['rows'][-1]['filing']
    original = raw('AMPL-0001193125-26-057847.htm')
    tree = html.fromstring(original)
    expenses = [n for n in inline.nodes(tree, 'nonfraction') if n.get('name') == 'us-gaap:OperatingExpenses']
    assert len(expenses) == 3
    if change in ('nil', 'nil_with_content'):
        for n in expenses:
            n.set('xmlns:xsi', 'http://www.w3.org/2001/XMLSchema-instance')
            n.set('xsi:nil', 'true')
            if change == 'nil':
                n.text = None
                for child in list(n):n.remove(child)
    elif change == 'hidden':
        for n in expenses:
            parent = n.getparent(); index = parent.index(n)
            parent.remove(n); hidden = html.fragment_fromstring('<ix:hidden></ix:hidden>'); hidden.append(n); parent.insert(index, hidden)
    elif change == 'dimensions':
        refs = {n.get('contextref') for n in expenses}
        for context in inline.nodes(tree, 'context'):
            if context.get('id') in refs:
                entity = inline.nodes(context, 'entity')[0]
                entity.append(html.fragment_fromstring('<xbrli:segment></xbrli:segment>'))
    elif change == 'column_context':
        refs = [n.get('contextref') for n in expenses]
        for n, ref in zip(expenses, refs[1:]+refs[:1]):n.set('contextref', ref)
    elif change == 'wrong_issuer':
        count = 0
        for n in tree.iter():
            if n.text and 'AMPLITUDE, INC.' in n.text:
                n.text = n.text.replace('AMPLITUDE, INC.', 'UNRELATED COMPANY, INC.');count += 1
        assert count
    else:
        # An explicit primary-statement subtotal must agree with the derived
        # components; use a complete copied row with a contradictory label/value.
        row = expenses[0].xpath('ancestor::tr')[-1]
        added = deepcopy(row)
        label = added.xpath('./td|./th')[0]
        attrs = dict(label.attrib);label.clear();label.attrib.update(attrs)
        label.text = 'Operating income (loss)' if change == 'subtotal_alias' else 'Operating loss'
        if change == 'subtotal_unreadable':
            for cell in added.xpath('./td|./th')[1:]:
                attrs = dict(cell.attrib);cell.clear();cell.attrib.update(attrs);cell.text = 'Unavailable'
        row.addnext(added)
    mutated = etree.tostring(tree, method='html')
    assert mutated != original
    try:
        recovered = op.statement_rows(mutated, filing, issuer=company['name'])
    except ValueError:
        return
    assert not recovered, change


def test_foreign_reporting_transition_does_not_reuse_domestic_history():
    facts, company = inputs()
    assert op.plan(facts, company, cik=CIK, available_by=DAY)
    recent = company['filings']['recent']
    item = dict(accessionNumber='0001193125-26-999999', form='20-F', filingDate='2026-10-20',
                reportDate='2026-06-30', primaryDocument='annual.htm')
    for key, value in item.items():recent[key].insert(0, value)
    assert op.plan(facts, company, cik=CIK, available_by=DAY) is None


@pytest.mark.parametrize('heading', [[], [None], [123], [''], ['  ']])
def test_cached_proof_rejects_malformed_statement_headings(heading):
    proof = deepcopy(acquired()['evidence']['operating_statement_history'])
    proof['rows'][0]['statement_heading'] = heading
    assert not op.cached_valid(proof, cik=CIK)


def test_optional_history_respects_total_source_budget_and_preserves_base(monkeypatch):
    base = {'evidence': {'cik': CIK, 'methodology': 'existing_source'}}
    class ReadClient:
        def __init__(self):self.calls=[]
        def get(self,url):self.calls.append(url);return url.encode()
    def primary(*args, client, **kwargs):
        for i in range(5):client.get(str(i))
        return deepcopy(base)
    def history(*args, client, **kwargs):
        assert client.get('0') == b'0'  # Shared original is not refetched.
        client.get('5');client.get('6')
        pytest.fail('Seventh original admitted')
    monkeypatch.setattr(inline, '_acquire', primary)
    monkeypatch.setattr(op, 'acquire', history)
    client = ReadClient()
    result = inline.acquire(raw('AMPL-facts.json'), raw('AMPL-company.json'), symbol='AMPL', cik=CIK, observed_at=NOW, client=client)
    assert result == base and client.calls == [str(i) for i in range(6)]


def test_optional_transport_failure_is_not_disguised_as_complete(monkeypatch):
    from app.clients.direct_sources import DirectSourceError
    monkeypatch.setattr(inline, '_acquire', lambda *a, **kw: None)
    def failed(*a, **kw):raise DirectSourceError('source_unavailable')
    monkeypatch.setattr(op, 'acquire', failed)
    with pytest.raises(DirectSourceError):
        inline.acquire(raw('AMPL-facts.json'), raw('AMPL-company.json'), symbol='AMPL', cik=CIK, observed_at=NOW, client=Client())


def test_complete_statement_serialization_control_retains_all_period_values():
    from lxml import html, etree
    facts, company = inputs()
    filing = op.plan(facts, company, cik=CIK, available_by=DAY)['rows'][-1]['filing']
    original = raw('AMPL-0001193125-26-057847.htm')
    serialized = etree.tostring(html.fromstring(original), method='html')
    before = op.statement_rows(original, filing, issuer=company['name'])
    after = op.statement_rows(serialized, filing, issuer=company['name'])
    assert len(before) == len(after) == 3
    assert {k:v['value'] for k,v in before.items()} == {k:v['value'] for k,v in after.items()}
