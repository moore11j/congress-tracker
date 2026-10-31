"""Complete-source fiscal reconciliation and deliberate ambiguity rejection."""
from copy import deepcopy
from pathlib import Path
import gzip,hashlib,json,socket
import pytest
from lxml import html
from app.services import sec_revenue_periods as periods, sec_inline_financials as inline

ROOT = Path(__file__).parent/'fixtures/sec_revenue_periods'
NAME = '0000202058-26-000015.htm'
ISSUER = 'L3Harris Technologies, Inc.'


def sources():
    manifest=json.loads((ROOT/'sources.json').read_text())
    result={}
    for name,meta in manifest.items():
        raw=gzip.decompress((ROOT/(name+'.gz')).read_bytes())
        assert len(raw)==meta['bytes'] and hashlib.sha256(raw).hexdigest()==meta['sha256']
        result[name]=raw
    return result,manifest


def filing():
    return dict(cik='0000202058',accession=NAME[:-4],form='10-K',filed='2026-02-12',report_date='2026-01-02',
                url='https://www.sec.gov/Archives/edgar/data/202058/000020205826000015/hrs-20260102.htm')


def test_complete_original_reconciles_52_and_53_week_years(monkeypatch):
    monkeypatch.setattr(socket.socket,'connect',lambda *a,**k:pytest.fail('Network forbidden'))
    data,manifest=sources(); raw=data[NAME]; before=hashlib.sha256(raw).hexdigest()
    proof=periods.qualify(raw,filing(),issuer=ISSUER)
    assert proof==periods.qualify(raw,filing(),issuer=ISSUER)
    assert proof['source_sha256']==before==manifest[NAME]['sha256']
    assert [(r['start'],r['end'],r['value']) for r in proof['annual']]==[
        ('2025-01-04','2026-01-02',21865000000),('2023-12-30','2025-01-03',21325000000),
        ('2022-12-31','2023-12-29',19419000000)]
    assert {(p['start'],p['end'],p['value']) for p in proof['contradictions']}=={
        ('2025-10-04','2026-01-02',21865000000),('2024-09-28','2025-01-03',21325000000)}
    assert all(r['tag']==periods.TAG for r in proof['annual'])
    assert raw==data[NAME]  # Raw contradictory tags stay intact.


@pytest.mark.parametrize('case',[
    'wrong_owner','no_owner','quoted_owner','negated_owner','competing_owner','fiscal_end','fiscal_conflict',
    'narrative_title','duplicate_statement','hidden_statement','header_year','header_scale','header_span',
    'product_axis','product_member','extra_dimension','wrong_entity','wrong_unit','unequal_sum','nil',
    'mismatched_component_period','annual_total_other_period','duplicate_fact','hidden_fact',
    'namespace_shadow','dimension_namespace','extra_component','fiscal_column_swap',
])
def test_whole_original_mutation_is_held(case):
    data,_=sources();tree=html.fromstring(data[NAME],parser=html.HTMLParser(encoding='utf-8'))
    table=next(t for t in tree.xpath('//table') if periods._caption(t))
    product=next(n for n in inline.nodes(table,'nonfraction') if n.get('id')=='f-53')
    service=next(n for n in inline.nodes(table,'nonfraction') if n.get('id')=='f-56')
    context=next(n for n in inline.nodes(tree,'context') if n.get('id')==product.get('contextref'))
    member=next(n for n in context.iter() if n.tag=='xbrldi:explicitmember')
    title=table.getprevious()
    block=next(n for n in tree.xpath('//div[not(.//div)][not(.//table)]') if 'Our Consolidated Financial Statements include the accounts' in periods.tables.visible_text(n))
    if case=='wrong_owner':
        block.getparent().remove(block)
        block=html.Element('div');block.text='Our Consolidated Financial Statements include the accounts of Other Corp and its consolidated subsidiaries.';tree.append(block)
    elif case=='no_owner':block.getparent().remove(block)
    elif case=='quoted_owner':block.tag='blockquote'
    elif case=='negated_owner':block.clear();block.text='Not '+f'Our Consolidated Financial Statements include the accounts of {ISSUER} and its consolidated subsidiaries.'
    elif case=='competing_owner':
        n=html.Element('div');n.text='Our Consolidated Financial Statements include the accounts of Other Corp and its consolidated subsidiaries.';tree.append(n)
    elif case in {'fiscal_end','fiscal_conflict'}:
        n=html.Element('div');n.text='The fiscal year ended January 3, 2026 ("fiscal 2025").';tree.append(n)
    elif case=='narrative_title':title.text='See the '+periods.tables.visible_text(title)
    elif case=='duplicate_statement':
        wrapper=html.Element('div');wrapper.append(deepcopy(title));wrapper.append(deepcopy(table));tree.append(wrapper)
    elif case=='hidden_statement':table.set('style','display:none')
    elif case=='header_year':
        cell=next(c for c in table.xpath('.//td') if periods.tables.visible_text(c)=='2025');cell.clear();cell.text='2022'
    elif case=='header_scale':
        cell=next(c for c in table.xpath('.//td') if 'In millions, except' in periods.tables.visible_text(c));cell.clear();cell.text='(In thousands, except per share amounts)'
    elif case=='header_span':
        cell=next(c for c in table.xpath('.//td') if periods.tables.visible_text(c)=='Fiscal Year');cell.set('colspan','2')
    elif case=='product_axis':member.set('dimension','srt:BusinessSegmentsAxis')
    elif case=='product_member':member.text='us-gaap:ServiceMember'
    elif case=='extra_dimension':member.getparent().append(deepcopy(member))
    elif case=='wrong_entity':inline.nodes(context,'identifier')[0].text='0000000001'
    elif case=='wrong_unit':product.set('unitref','shares')
    elif case=='unequal_sum':product.text='15,488'
    elif case=='nil':product.set('xsi:nil','true')
    elif case=='mismatched_component_period':service.set('contextref','c-8')
    elif case=='annual_total_other_period':
        c=next(n for n in inline.nodes(tree,'context') if n.get('id')=='c-10');inline.nodes(c,'startdate')[0].text='2025-07-01'
    elif case=='duplicate_fact':product.getparent().append(deepcopy(product))
    elif case=='hidden_fact':product.set('style','visibility:hidden')
    elif case=='namespace_shadow':member.set('xmlns:srt','https://untrusted.example/srt')
    elif case=='dimension_namespace':tree.set('xmlns:srt','https://untrusted.example/srt')
    elif case=='extra_component':
        row=service.xpath('ancestor::tr')[-1];copy=deepcopy(row);copy[0].clear();copy[0].text='Other';row.addnext(copy)
    elif case=='fiscal_column_swap':
        cells=[c for c in table.xpath('.//td') if periods.tables.visible_text(c) in {'2025','2024'}]
        for cell,label in zip(cells,['2024','2025']):cell.clear();cell.text=label
    mutated=html.tostring(tree,encoding='utf-8')
    with pytest.raises((ValueError,inline.SecFundamentalsError)):
        periods.qualify(mutated,filing(),issuer=ISSUER)


@pytest.mark.parametrize('key,value',[
    ('cik','0000000001'),('accession','0000202058-26-000058'),('form','10-Q'),
    ('filed','2025-12-31'),('report_date','2025-12-31'),
    ('url','https://www.sec.gov/Archives/edgar/data/1/000020205826000015/hrs-20260102.htm'),
])
def test_filing_binding(key,value):
    data,_=sources();meta=filing();meta[key]=value
    with pytest.raises((ValueError,inline.SecFundamentalsError)):
        periods.qualify(data[NAME],meta,issuer=ISSUER)


from datetime import date,datetime,timezone
from unittest.mock import patch
from app.services import sec_fundamentals as sec,sec_financial_statements as statements

NOW=datetime(2026,10,31,23,30,tzinfo=timezone.utc)
DAY=date(2026,10,30)
CIK='0000202058'


class Client:
    issuer_document_urls=frozenset()
    def __init__(self):
        data,manifest=sources();self.urls={meta['url']:data[name] for name,meta in manifest.items()};self.calls=[]
    def get(self,url):self.calls.append(url);return self.urls[url]


@pytest.fixture(scope='module')
def acquired():
    data,_=sources();client=Client()
    with patch.object(socket.socket,'connect',side_effect=AssertionError('Offline only')):
        supplement=inline.acquire(data['LHX-facts.json'],data['LHX-company.json'],symbol='LHX',cik=CIK,
                                  observed_at=NOW,client=client,include_debt_precision=True)
    assert len(client.calls)==2 and len(set(client.calls))==2
    return supplement


def project(supplement):
    data,_=sources()
    return sec.project_fundamentals(data['LHX-facts.json'],data['LHX-company.json'],symbol='LHX',cik=CIK,
                                   observed_at=NOW,filing_supplement=supplement)


def test_current_and_prior_ttm_and_historical_quarters(acquired):
    data,_=sources();value=project(acquired);proof=json.loads(value['source_evidence_json'])
    assert value==project(acquired)
    assert proof['current']['revenue']['value']==21865000000+17685000000-16217000000
    assert value['revenue_growth']==pytest.approx(7.327506899724014)
    assert value['free_cash_flow']==3641000000 and value['roe']==pytest.approx(10.278587951322539)
    assert value['operating_margin_expansion']==pytest.approx(.13497275220635707)
    assert value.get('net_debt_to_ebitda') is None
    panel=statements.project(data['LHX-facts.json'],data['LHX-company.json'],symbol='LHX',cik=CIK,observed_at=NOW,filing_supplement=acquired)
    annual={p['date']:p['revenue'] for p in panel['annual']};quarters={p['date']:p['revenue'] for p in panel['quarterly']}
    assert annual['2026-01-02']==21865000000 and annual['2025-01-03']==21325000000
    assert quarters['2026-01-02']==5648000000 and quarters['2025-01-03']==5523000000
    evidence=panel['sourceEvidence']['periods']['quarterly:2026-01-02']['revenue']
    assert evidence['method']=='aligned_ytd_difference'
    annual_input=next(p for p in evidence['inputs'] if 'period_reconciliation' in p)
    assert annual_input['period_reconciliation']['withdrawn_period_observations'][0]['value']==21865000000
    assert periods.cached_valid(proof['inline_filing_supplement']['reconciled_revenue_periods'],cik=CIK)


@pytest.mark.parametrize('change',['value','source','proof','missing_proof','wrong_state','changed_record','conflicting_annual'])
def test_replay_rejects_modified_proof_or_data(acquired,change):
    data,_=sources();payload=json.loads(data['LHX-facts.json']);company=json.loads(data['LHX-company.json']);supplement=deepcopy(acquired)
    if change=='value':supplement['revenue_periods']['evidence']['annual'][0]['value']+=1
    elif change=='source':supplement['revenue_periods']['raw']+=b'\n'
    elif change=='proof':supplement['evidence']['reconciled_revenue_periods']['source_sha256']='0'*64
    elif change=='missing_proof':del supplement['evidence']['reconciled_revenue_periods']
    elif change=='wrong_state':company['stateOfIncorporation']='NY'
    elif change in {'changed_record','conflicting_annual'}:
        rows=payload['facts']['us-gaap'][periods.TAG]['units']['USD']
        target=next(p for p in rows if p.get('start')=='2025-10-04' and p['end']=='2026-01-02')
        if change=='changed_record':target['val']+=1
        else:rows.append({**target,'start':'2025-01-04','val':1})
    with pytest.raises((ValueError,inline.SecFundamentalsError)):
        sec.project_fundamentals(json.dumps(payload).encode(),json.dumps(company).encode(),symbol='LHX',cik=CIK,observed_at=NOW,filing_supplement=supplement)


@pytest.mark.parametrize('change',['equation','dimension','owner','fiscal','quarter','hash','identity','duplicate'])
def test_persisted_proof_corruption_is_rejected(acquired,change):
    proof=json.loads(json.dumps(acquired['revenue_periods']['evidence']))
    assert periods.cached_valid(proof,cik=CIK)
    if change=='equation':proof['annual'][0]['value']+=1
    elif change=='dimension':proof['annual'][0]['components']['products']['members']=[]
    elif change=='owner':proof['owner']['issuer']='Other Corp'
    elif change=='fiscal':proof['annual'][0]['fiscal_year']='2024'
    elif change=='quarter':proof['contradictions'][0]['value']+=1
    elif change=='hash':proof['source_sha256']='bad'
    elif change=='identity':proof['filing']['cik']='0000000001'
    else:proof['annual'].append(deepcopy(proof['annual'][0]))
    assert not periods.cached_valid(proof,cik=CIK)


def test_cache_repeat_source_score_watchlist_rebaseline_and_crossing(acquired,monkeypatch):
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    from app.models import InsightsSnapshot
    from app.services import sec_fundamentals_preparation as prep,sec_directory
    monkeypatch.setattr(socket.socket,'connect',lambda *a,**k:pytest.fail('Unexpected network'))
    monkeypatch.setenv('SEC_FUNDAMENTALS_WARMING_ENABLED','1')
    monkeypatch.setattr(sec_directory,'directory',lambda:{'LHX':{'cik':CIK}})
    class Clock(datetime):
        @classmethod
        def now(cls,tz=None):return NOW
    engine=create_engine('sqlite://');InsightsSnapshot.__table__.create(engine);factory=sessionmaker(bind=engine);client=Client()
    try:
        with patch.object(prep,'SessionLocal',factory),patch.object(prep,'datetime',Clock),patch.object(prep,'DirectSourceClient',lambda:client):
            payload=prep.prepare('LHX');assert payload['values']['revenue_growth']==project(acquired)['revenue_growth']
            count=len(client.calls);assert prep.prepare('LHX')==payload and len(client.calls)==count
        with patch.object(statements,'SessionLocal',factory),patch.object(statements,'datetime',Clock),patch.object(statements,'DirectSourceClient',lambda:client):
            panel=statements.prepared('LHX');count=len(client.calls);assert statements.prepared('LHX')==panel and len(client.calls)==count
        with factory() as db:
            for key,field in [('sec-current-fundamentals:LHX:v1','calculation_evidence'),('sec-financials:LHX:v1','sourceEvidence')]:
                row=db.get(InsightsSnapshot,key);saved=json.loads(row.payload_json)
                nested='inline_filing_supplement' if field=='calculation_evidence' else 'inlineFilingSupplement'
                saved[field][nested]['reconciled_revenue_periods']['annual'][0]['value']+=1
                row.payload_json=json.dumps(saved)
            db.commit();assert statements.cached_payload(db,'LHX',now=NOW) is None
        with patch.object(prep,'SessionLocal',factory):assert prep._cached('LHX',NOW) is None
    finally:engine.dispose()
    import test_sec_tagged_foreign_consumers as consumer
    data,manifest=sources();urls={meta['url']:data[name] for name,meta in manifest.items()}
    monkeypatch.setattr(consumer,'originals',lambda symbol:(data['LHX-facts.json'],data['LHX-company.json'],CIK,urls))
    values=project(acquired);metrics={k:values[k] for k in json.loads(values['source_evidence_json'])['calculations']}
    monkeypatch.setattr(consumer,'EXPECTED',{'LHX':{'metrics':metrics}});monkeypatch.setattr(consumer,'NOW',NOW)
    consumer.test_original_source_score_and_alert_recovery_repeat_crossing_rollback('LHX','revenue_growth',monkeypatch)
