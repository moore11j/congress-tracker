from copy import deepcopy
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from unittest.mock import patch
import gzip, hashlib, json, socket

import pytest
from lxml import html
from app.services import sec_fundamentals as sec, sec_inline_financials as inline
from app.services import sec_revenue_series as series, sec_revenue_statement_layout as layout

ROOT=Path(__file__).parent/'fixtures/revenue_statement_layout'
MANIFEST=json.loads((ROOT/'sources.json').read_text())
NOW=datetime(2026,11,16,4,30,tzinfo=timezone.utc)
AVAILABLE=NOW.date().replace(day=14)

def raw(name):
    meta=MANIFEST[name];data=gzip.decompress((ROOT/meta['file']).read_bytes())
    assert len(data)==meta['bytes'] and hashlib.sha256(data).hexdigest()==meta['sha256']
    return data

class Client:
    def __init__(self):self.calls=[]
    def get(self,url):
        self.calls.append(url)
        names=[n for n,m in MANIFEST.items() if m['url']==url]
        assert len(names)==1,url
        return raw(names[0])

@lru_cache(maxsize=1)
def fixture():
    client=Client()
    with patch.object(socket.socket,'connect',side_effect=AssertionError('No network')):
        supplement=inline.acquire(raw('HRB-facts.json'),raw('HRB-company.json'),symbol='HRB',cik='0000012659',observed_at=NOW,client=client)
        value=sec.project_fundamentals(raw('HRB-facts.json'),raw('HRB-company.json'),symbol='HRB',cik='0000012659',observed_at=NOW,filing_supplement=supplement)
    return supplement,value,client.calls

def test_complete_originals_actual_acquisition_projection_and_repeat():
    supplement,value,calls=fixture();proof=supplement['evidence']['revenue_series_selection']
    assert series.cached_valid(proof,cik='0000012659')
    assert len(calls)==len(set(calls))==3
    evidence=json.loads(value['source_evidence_json'])
    assert evidence['current']['revenue']['value']==3_966_177_000
    assert evidence['prior']['revenue']['value']==3_770_736_000
    assert value['revenue_growth']==pytest.approx(5.183099532823292)
    assert value['net_margin']==pytest.approx(17.9249942703011)
    assert value['fcf_margin']==pytest.approx(18.83108091242524)
    assert all(p['tag']==series.CONTRACT and p['original_statement']['layout_policy']==layout.POLICY for p in proof['rows'])
    assert {p['original_statement']['statement_concept'] for p in proof['rows']}=={series.CONTRACT,series.TOTAL}
    previous=deepcopy(supplement);previous['evidence'].pop('revenue_series_selection')
    before=sec.project_fundamentals(raw('HRB-facts.json'),raw('HRB-company.json'),symbol='HRB',cik='0000012659',observed_at=NOW,filing_supplement=previous)
    for key in ('current_ratio','free_cash_flow'):assert before[key]==value[key]
    assert before['fcf_growth']==pytest.approx(value['fcf_growth'],abs=1e-12,rel=0)
    assert not {'revenue_growth','net_margin','fcf_margin'} & before.keys()
    assert sec.project_fundamentals(raw('HRB-facts.json'),raw('HRB-company.json'),symbol='HRB',cik='0000012659',observed_at=NOW,filing_supplement=supplement)==value

@pytest.mark.parametrize('defect',['policy','heading','component','dimension','period','unit','duplicate','missing','concept','total','label'])
def test_persisted_subtotal_proof_cannot_be_reassigned(defect):
    proof=deepcopy(fixture()[0]['evidence']['revenue_series_selection'])
    original=next(p['original_statement'] for p in proof['rows'] if 'subtotal_equation' in p['original_statement'])
    part=original['subtotal_equation']['components'][0]
    if defect=='policy':original['layout_policy']='unreviewed'
    elif defect=='heading':original['statement_heading']=['GUARANTOR CONSOLIDATED STATEMENTS OF INCOME']
    elif defect=='component':part['value']+=1000
    elif defect=='dimension':part['member']='us-gaap:ProductMember'
    elif defect=='period':part['start']='2020-01-01'
    elif defect=='unit':part['unit']='EUR'
    elif defect=='duplicate':original['subtotal_equation']['components']*=2
    elif defect=='missing':original.pop('subtotal_equation')
    elif defect=='concept':original['statement_concept']=series.TOTAL
    elif defect=='total':original['subtotal_equation']['total']+=1
    else:original['row_label']='total revenues'
    assert not series.cached_valid(proof,cik='0000012659')
    with pytest.raises(ValueError,match='revenue_series_row_mismatch'):
        series.apply({'revenue':{}},json.loads(raw('HRB-facts.json')),json.loads(raw('HRB-company.json')),cik='0000012659',available_by=AVAILABLE,supplement={'evidence':{'revenue_series_selection':proof}})

@pytest.mark.parametrize('defect',['hidden_title','late_title','notes_title','guarantor','component_value','component_member','duplicate_total','missing_component','namespace_shadow'])
def test_original_statement_guards(defect):
    name='hrb-20260630.htm';meta=MANIFEST[name]
    filing={k:meta[k] for k in ('cik','accession','form','filed','report_date','url')}
    tree=html.fromstring(raw(name),parser=html.HTMLParser(encoding='utf-8'))
    titles=[n for n in tree.xpath('//td|//th') if ' '.join(' '.join(n.itertext()).split()).rstrip(':')=='CONSOLIDATED STATEMENTS OF OPERATIONS AND COMPREHENSIVE INCOME']
    assert len(titles)==1
    title=titles[0];table=title.xpath('ancestor::table')[-1]
    if defect=='hidden_title':title.set('style','display:none')
    elif defect=='late_title':
        row=title.xpath('ancestor::tr')[-1];row.getparent().remove(row);table.append(row)
    elif defect=='notes_title':title.text='NOTES TO CONSOLIDATED STATEMENTS OF OPERATIONS AND COMPREHENSIVE INCOME'
    elif defect=='guarantor':title.text='GUARANTOR CONSOLIDATED STATEMENTS OF OPERATIONS AND COMPREHENSIVE INCOME'
    elif defect=='component_value':
        node=next(n for n in inline.nodes(table,'nonfraction') if n.get('contextref')=='c-4' and n.get('name')=='us-gaap:'+series.CONTRACT);node.text='3,664,041'
    elif defect=='component_member':
        context=next(n for n in inline.nodes(tree,'context') if n.get('id')=='c-4')
        next(n for n in context.iter() if n.tag=='xbrldi:explicitmember').text='us-gaap:ProductMember'
    elif defect=='namespace_shadow':table.set('xmlns:srt','https://example.invalid/false')
    else:
        node=next(n for n in inline.nodes(table,'nonfraction') if n.get('contextref')==('c-4' if defect=='missing_component' else 'c-1') and n.get('name')=='us-gaap:'+series.CONTRACT)
        row=node.xpath('ancestor::tr')[-1]
        if defect=='missing_component':row.getparent().remove(row)
        else:row.addnext(deepcopy(row))
    altered=html.tostring(tree)
    if defect=='namespace_shadow':
        with pytest.raises(inline.InlineFactsError,match='namespace_redefinition'):inline.parse(altered,**filing)
        return
    parsed=inline.parse(altered,**filing)
    assert ('2025-07-01','2026-06-30') not in layout.statement_rows(altered,filing,parsed)

def test_preparsed_bytes_and_filing_identity_must_match():
    name='hrb-20260630.htm';meta=MANIFEST[name];filing={k:meta[k] for k in ('cik','accession','form','filed','report_date','url')}
    parsed=inline.parse(raw(name),**filing)
    with pytest.raises(ValueError,match='original_identity'):layout.statement_rows(raw(name)+b' ',filing,parsed)
    with pytest.raises(ValueError,match='original_identity'):layout.statement_rows(raw(name),{**filing,'accession':'0000012659-26-999999'},parsed)


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
    before=sec.project_fundamentals(raw('HRB-facts.json'),raw('HRB-company.json'),symbol='HRB',cik='0000012659',observed_at=NOW,filing_supplement=previous)
    engine=create_engine('sqlite://');Base.metadata.create_all(engine)
    with patch.object(socket.socket,'connect',side_effect=AssertionError('No network')),sessionmaker(bind=engine)() as db:
        user=UserAccount(email='hrb-period-proof@example.test');security=Security(symbol='HRB',name=value['company_name'],asset_class='equity')
        db.add_all([user,security]);db.flush();watchlist=Watchlist(name='Offline period proof',owner_user_id=user.id);db.add(watchlist);db.flush()
        db.add(WatchlistItem(watchlist_id=watchlist.id,security_id=security.id,target_type='ticker'))
        db.add(WatchlistAlertRule(user_id=user.id,watchlist_id=watchlist.id,name='Revenue growth transition',enabled=True,scope_type='any_watchlist_ticker',match_type='all',delivery='in_app',conditions_json=json.dumps(validate_conditions([dict(metric='revenue_growth',operator='gt',comparison_type='value',comparison_value=0)]))))
        cache.upsert_fundamentals_cache(db,before);db.commit()
        def run(minutes):
            result=evaluate_watchlist_custom_alerts(db,user_id=user.id,watchlist_id=watchlist.id,now=NOW+timedelta(minutes=minutes));db.commit();return result
        assert run(0)['triggered']==0
        cache.upsert_fundamentals_cache(db,value);db.commit()
        row=db.execute(select(FundamentalsCache).where(FundamentalsCache.symbol=='HRB',FundamentalsCache.provider=='sec_edgar')).scalar_one()
        db.refresh(row)
        summary=cache.fundamentals_summary_from_cache_row(row,now=NOW)
        assert summary['status']=='unavailable' and summary['metrics']['revenue_growth']['value']==value['revenue_growth']/100
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
