from pathlib import Path
from copy import deepcopy
from datetime import datetime,date,timezone
from functools import lru_cache
from unittest.mock import patch
import gzip,hashlib,json,socket
import pytest
from app.services import sec_revenue_series as series,sec_revenue_tax_tags as tax
from app.services import sec_inline_financials as inline,sec_fundamentals as sec

ROOT=Path(__file__).parent/'fixtures/revenue_tax_tags'
MANIFEST=json.loads((ROOT/'sources.json').read_bytes())
NOW=datetime(2026,11,16,4,30,tzinfo=timezone.utc)
AVAILABLE=date(2026,11,14)

@lru_cache(maxsize=5)
def raw(name):
    value=gzip.decompress((ROOT/(name+'.gz')).read_bytes())
    assert len(value)==MANIFEST[name]['bytes'] and hashlib.sha256(value).hexdigest()==MANIFEST[name]['sha256']
    return value

class Client:
    issuer_document_urls=frozenset()
    def __init__(self):self.calls=[]
    def get(self,url):
        self.calls.append(url)
        names=[name for name,meta in MANIFEST.items() if meta['url']==url]
        assert len(names)==1,url
        return raw(names[0])

@lru_cache(maxsize=1)
def fixture():
    client=Client()
    with patch.object(socket.socket,'connect',side_effect=AssertionError('No network')):
        supplement=inline.acquire(raw('PACB-facts.json'),raw('PACB-company.json'),symbol='PACB',cik='0001299130',observed_at=NOW,client=client)
        value=sec.project_fundamentals(raw('PACB-facts.json'),raw('PACB-company.json'),symbol='PACB',cik='0001299130',observed_at=NOW,filing_supplement=supplement)
    return supplement,value,client.calls

def test_complete_originals_restore_whole_series_and_repeat():
    supplement,value,calls=fixture()
    assert 'revenue_series_hold' not in supplement['evidence']
    proof=supplement['evidence']['revenue_series_selection']
    assert proof['methodology']==series.TAX_VERSION and len(proof['overlap'])==15
    assert series.cached_valid(proof,cik='0001299130')
    evidence=json.loads(value['source_evidence_json'])
    assert evidence['current']['revenue']['value']==161394000
    assert evidence['prior']['revenue']['value']==154584000
    assert {p['tag'] for p in evidence['current']['revenue']['inputs']}=={series.INCLUDING}
    assert not proof['basis_spliced'] and len(set(calls))==3
    assert sec.project_fundamentals(raw('PACB-facts.json'),raw('PACB-company.json'),symbol='PACB',cik='0001299130',observed_at=NOW,filing_supplement=supplement)==value
    prior=deepcopy(supplement);prior['evidence'].pop('revenue_series_selection')
    before=sec.project_fundamentals(raw('PACB-facts.json'),raw('PACB-company.json'),symbol='PACB',cik='0001299130',observed_at=NOW,filing_supplement=prior)
    assert set(value)-set(before)=={'revenue_growth','gross_margin','operating_margin','operating_margin_expansion','net_margin','fcf_margin'}
    assert all(value[k]==v for k,v in before.items() if k!='source_evidence_json')
    from app.services import sec_financial_statements as statements
    args=dict(symbol='PACB',cik='0001299130',observed_at=NOW)
    panel=statements.project(raw('PACB-facts.json'),raw('PACB-company.json'),**args,filing_supplement=supplement)
    old=statements.project(raw('PACB-facts.json'),raw('PACB-company.json'),**args,filing_supplement=prior)
    for kind in ('annual','quarterly'):
        current={r['date']:r for r in panel[kind]}
        for row in old[kind]:
            if row['date'] in current:assert row==current[row['date']]

@pytest.mark.parametrize('field,value',[('policy','wrong'),('selected_concept',series.CONTRACT),('primary_concept','Revenues'),('value',0),('unit','EUR'),('accession','0001299130-26-999999'),('filed','2026-11-06'),('start','2020-01-01')])
def test_cached_and_projection_reject_changed_primary_proof(field,value):
    supplement,_,_=fixture();supplement=deepcopy(supplement)
    proof=supplement['evidence']['revenue_series_selection']
    proof['rows'][0]['original_statement']['tax_tag_reconciliation'][field]=value
    assert not series.cached_valid(proof,cik='0001299130')
    with pytest.raises(ValueError):
        sec.project_fundamentals(raw('PACB-facts.json'),raw('PACB-company.json'),symbol='PACB',cik='0001299130',observed_at=NOW,filing_supplement=supplement)

def test_conflicting_overlap_never_requests_originals():
    payload=json.loads(raw('PACB-facts.json'));company=json.loads(raw('PACB-company.json'))
    for point in payload['facts']['us-gaap'][series.INCLUDING]['units']['USD']:
        if point.get('end')=='2026-09-30':point['val']+=1
    client=Client()
    with pytest.raises(ValueError,match='conflict'):
        series.acquire(payload,company,cik='0001299130',available_by=AVAILABLE,client=client)
    assert not client.calls

def test_tax_tags_are_not_assumed_equivalent_without_primary_statement():
    name='pacb-20260930.htm';meta=MANIFEST[name]
    filing={k:meta[k] for k in ('cik','accession','filed','form','report_date','url')}
    from lxml import html
    tree=html.fromstring(raw(name))
    for node in inline.nodes(tree,'nonfraction'):
        if node.get('name')=='us-gaap:'+series.CONTRACT:
            node.set('name','us-gaap:OtherSalesRevenueNet')
    altered=html.tostring(tree)
    parsed=inline.parse(altered,**filing)
    assert not tax.statement_rows(altered,filing,parsed)

@pytest.mark.parametrize('defect',['hidden_title','notes_title','guarantor_title','intervening_content','numeric_title','hidden_primary','conflicting_primary'])
def test_original_heading_visibility_and_total_conflicts_remain_held(defect):
    from lxml import html
    name='pacb-20260930.htm';meta=MANIFEST[name]
    filing={k:meta[k] for k in ('cik','accession','filed','form','report_date','url')}
    tree=html.fromstring(raw(name))
    nodes=[n for n in inline.nodes(tree,'nonfraction') if n.get('name')=='us-gaap:'+series.CONTRACT and n.xpath('ancestor::table')]
    table=nodes[0].xpath('ancestor::table')[-1];wrapper=table.getparent();title=wrapper.getprevious()
    heading=next(n for n in title.iter('span') if tax.TITLE.fullmatch(' '.join(n.text_content().split())))
    if defect=='hidden_title':title.set('style','display:none')
    elif defect=='notes_title':heading.text='NOTES TO '+heading.text
    elif defect=='guarantor_title':heading.text='GUARANTOR '+heading.text
    elif defect=='intervening_content':wrapper.addprevious(html.fromstring('<div>Separate segment disclosure</div>'))
    elif defect=='numeric_title':title.append(deepcopy(nodes[0]))
    elif defect=='hidden_primary':table.set('style','display:none')
    else:
        for node in nodes:node.text=str(float(node.text_content().replace(',',''))+1)
    altered=html.tostring(tree)
    try:
        parsed=inline.parse(altered,**filing)
        result=tax.statement_rows(altered,filing,parsed)
    except ValueError:
        assert defect in {'conflicting_primary','numeric_title'}
    else:assert not result


def test_recovery_cache_screener_and_watchlist_do_not_emit_false_alerts(monkeypatch):
    from datetime import timedelta
    from sqlalchemy import create_engine,select
    from sqlalchemy.orm import sessionmaker
    from app.db import Base
    from app.models import FundamentalsCache,MonitoringAlert,Security,UserAccount,Watchlist,WatchlistItem,WatchlistAlertRule
    from app.services import fundamentals_cache as cache
    from app.services.custom_alert_rules import evaluate_watchlist_custom_alerts,validate_conditions
    monkeypatch.setenv('FUNDAMENTALS_PROVIDER','sec_edgar')
    supplement,value,_=fixture()
    previous=deepcopy(supplement);previous['evidence'].pop('revenue_series_selection')
    before=sec.project_fundamentals(raw('PACB-facts.json'),raw('PACB-company.json'),symbol='PACB',cik='0001299130',observed_at=NOW,filing_supplement=previous)
    engine=create_engine('sqlite://');Base.metadata.create_all(engine)
    with patch.object(socket.socket,'connect',side_effect=AssertionError('No network')),sessionmaker(bind=engine)() as db:
        user=UserAccount(email='pacb-tax-tag-proof@example.test');security=Security(symbol='PACB',name=value['company_name'],asset_class='equity')
        db.add_all([user,security]);db.flush();watchlist=Watchlist(name='Offline period proof',owner_user_id=user.id);db.add(watchlist);db.flush()
        db.add(WatchlistItem(watchlist_id=watchlist.id,security_id=security.id,target_type='ticker'))
        db.add(WatchlistAlertRule(user_id=user.id,watchlist_id=watchlist.id,name='Revenue growth transition',enabled=True,scope_type='any_watchlist_ticker',match_type='all',delivery='in_app',conditions_json=json.dumps(validate_conditions([dict(metric='revenue_growth',operator='gt',comparison_type='value',comparison_value=0)]))))
        cache.upsert_fundamentals_cache(db,before);db.commit()
        def run(minutes):
            result=evaluate_watchlist_custom_alerts(db,user_id=user.id,watchlist_id=watchlist.id,now=NOW+timedelta(minutes=minutes));db.commit();return result
        assert run(0)['triggered']==0
        cache.upsert_fundamentals_cache(db,value);db.commit()
        row=db.execute(select(FundamentalsCache).where(FundamentalsCache.symbol=='PACB',FundamentalsCache.provider=='sec_edgar')).scalar_one()
        db.refresh(row)
        summary=cache.fundamentals_summary_from_cache_row(row,now=NOW)
        assert summary['metrics']['revenue_growth']['value']==value['revenue_growth']/100
        screen=cache.cache_row_to_screener_row(row)
        for field,mapped in cache.SCREENER_ROW_FIELD_MAP.items():
            if field in value:assert screen.get(mapped)==value[field]
        stamp=row.updated_at;cache.upsert_fundamentals_cache(db,value);db.commit();assert row.updated_at==stamp
        assert run(5)['triggered']==run(10)['triggered']==0
        row.revenue_growth=-1;db.commit();assert run(15)['triggered']==0
        row.revenue_growth=value['revenue_growth'];db.commit();assert run(20)['triggered']==1
        assert run(25)['triggered']==0
        assert len(db.scalars(select(MonitoringAlert)).all())==1
    engine.dispose()
