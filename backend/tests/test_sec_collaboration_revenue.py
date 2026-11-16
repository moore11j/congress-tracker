from copy import deepcopy
from datetime import date, datetime, timezone
from functools import lru_cache
from pathlib import Path
import gzip, hashlib, json, socket
from unittest.mock import patch
from lxml import html
import pytest

from app.services import sec_collaboration_revenue as rental, sec_fundamentals as sec, sec_inline_financials as inline
from app.services import sec_financial_statements as statements, sec_tagged_foreign

ROOT = Path(__file__).parent/'fixtures/sec_collaboration_revenue'
NOW = datetime(2026, 11, 16, 4, tzinfo=timezone.utc)
AVAILABLE = date(2026, 11, 14)
CIK = '0001785530'


@lru_cache(maxsize=5)
def raw(name):
    proof = json.loads((ROOT/'sources.json').read_bytes())[name]
    value = gzip.decompress((ROOT/(name+'.gz')).read_bytes())
    assert len(value) == proof['bytes'] and hashlib.sha256(value).hexdigest() == proof['sha256']
    return value


def inputs():
    return json.loads(raw('HOWL-facts.json')), json.loads(raw('HOWL-company.json'))


class Client:
    issuer_document_urls = frozenset()
    def __init__(self):
        self.calls = []
        self.names = {p['url']: name for name, p in json.loads((ROOT/'sources.json').read_bytes()).items()}
    def get(self, url):
        self.calls.append(url)
        assert url in self.names, 'Uncaptured original'
        return raw(self.names[url])


@lru_cache(maxsize=1)
def acquired():
    client = Client()
    with patch.object(socket.socket, 'connect', side_effect=AssertionError('Offline evidence only')):
        result = inline.acquire(raw('HOWL-facts.json'), raw('HOWL-company.json'), symbol='HOWL', cik=CIK,
                                observed_at=NOW, client=client, include_debt_precision=True)
    assert len(client.calls) == len(set(client.calls)) == 3
    return result


def project(supplement):
    return sec.project_fundamentals(raw('HOWL-facts.json'), raw('HOWL-company.json'), symbol='HOWL',
                                   cik=CIK, observed_at=NOW, filing_supplement=supplement)


def test_complete_originals_recover_revenue_without_splicing_or_overclaiming_classification():
    source = acquired(); after = project(source); without = deepcopy(source)
    without['evidence'].pop('collaboration_revenue_selection')
    before = project(without)
    assert after.get('revenue_growth') is None
    assert after['operating_margin'] == -121.83333333333333
    assert after['net_margin'] == -133.9047619047619
    assert after.get('fcf_margin') is None and after.get('free_cash_flow') is None
    for key,value in before.items():
        if key!='source_evidence_json':assert after.get(key)==value
    evidence = json.loads(after['source_evidence_json'])
    assert evidence['current']['revenue']['value']==21_000_000
    assert evidence['prior']['revenue']['value']==0
    assert all(p['tag']==rental.CONCEPT for p in evidence['current']['revenue']['inputs'])
    assert rental.cached_valid(source['evidence']['collaboration_revenue_selection'],cik=CIK)
    assert sec_tagged_foreign.cached_currency_valid(evidence)
    assert project(source)==after


def test_original_verified_quarters_and_old_reported_history_are_preserved():
    source = acquired(); without = deepcopy(source); without['evidence'].pop('collaboration_revenue_selection')
    kwargs = dict(symbol='HOWL', cik=CIK, observed_at=NOW)
    before = statements.project(raw('HOWL-facts.json'), raw('HOWL-company.json'), filing_supplement=without, **kwargs)
    after = statements.project(raw('HOWL-facts.json'), raw('HOWL-company.json'), filing_supplement=source, **kwargs)
    for kind in ('annual', 'quarterly'):
        rows = {p['date']: p for p in after[kind]}
        for point in before[kind]:
            if point['date'] in rows:
                for key, value in point.items():
                    if value is not None: assert rows[point['date']][key] == value
    assert any(p.get('revenue') == 0 and p['date']=='2026-09-30' for p in after['quarterly'])


@pytest.mark.parametrize('defect', ['value', 'owner', 'unit', 'hash', 'url', 'component', 'heading', 'splice', 'missing_period'])
def test_persisted_proof_corruption_is_held(defect):
    proof = deepcopy(acquired()['evidence']['collaboration_revenue_selection']); point = proof['rows'][0]
    original = point['original_statement']
    if defect == 'value': point['value'] += 1
    elif defect == 'owner': original['source_cik'] = '0000000001'
    elif defect == 'unit': point['unit'] = 'EUR'
    elif defect == 'hash': original['source_sha256'] = 'bad'
    elif defect == 'url': original['source_url'] = 'https://example.com/statement'
    elif defect == 'component': original['revenue_section'].append('management fees')
    elif defect == 'heading': original['statement_heading'] = ['Adjusted consolidated statements of income']
    elif defect == 'splice': proof['basis_spliced'] = True
    else:
        wanted = rental.plan(*inputs(), cik=CIK, available_by=AVAILABLE)
        key = next(iter(wanted['needed']))
        proof['rows'] = [p for p in proof['rows'] if (p['start'], p['end']) != key]
    with pytest.raises(ValueError):
        rental.apply({'revenue': {}}, *inputs(), cik=CIK, available_by=AVAILABLE,
                     supplement={'evidence': {'collaboration_revenue_selection': proof}})


@pytest.mark.parametrize('defect', ['component_label', 'extra_revenue', 'adjusted_title', 'wrong_entity'])
def test_complete_current_statement_rejects_components_adjustments_and_other_entities(defect):
    facts, company = inputs(); wanted = rental.plan(facts, company, cik=CIK, available_by=AVAILABLE)
    filing = next(f for f in wanted['filings'].values() if f['report_date'] == '2026-09-30')
    tree = html.fromstring(raw('howl-20260930.htm')); changes = 0
    for node in list(tree.iter()):
        if not isinstance(node.tag, str): continue
        if defect == 'wrong_entity' and node.tag.lower() == 'xbrli:identifier' and node.text_content().strip().zfill(10) == CIK:
            node.text = '0000000001'; changes += 1
        if node.text and defect == 'adjusted_title' and 'consolidated statements of' in node.text.lower():
            node.text = 'Adjusted '+node.text; changes += 1
        if node.text and defect == 'component_label' and node.text.strip() == 'Collaboration revenue':
            node.text = 'Collaboration revenue component'; changes += 1
        if defect == 'extra_revenue' and node.tag.lower() == 'tr' and ' '.join(node.text_content().split()).strip().lower() == 'operating expenses:':
            node.addprevious(html.fromstring('<tr><td>Management fees</td><td>100</td></tr>')); changes += 1
    assert changes
    changed = html.tostring(tree)
    try:
        result = rental.statement_rows(changed, filing)
    except ValueError:
        result = {}
    assert result == {}


def test_current_supported_total_keeps_priority_and_conflicting_partial_total_is_held():
    facts, company = inputs()
    rental_rows = facts['facts']['us-gaap'][rental.CONCEPT]['units']['USD']
    facts['facts']['us-gaap']['Revenues'] = {'units': {'USD': deepcopy(rental_rows)}}
    assert rental.plan(facts, company, cik=CIK, available_by=AVAILABLE) is None
    wanted = rental.plan(*inputs(), cik=CIK, available_by=AVAILABLE)
    point = next(iter(wanted['needed'].values()))
    conflicting = dict(start=point['start'], end=point['end'], val=point['value']+1,
                       accn=point['accession'], filed=point['filed'], form='10-K')
    facts['facts']['us-gaap']['Revenues']['units']['USD'] = [conflicting]
    with pytest.raises(ValueError, match='total_conflict'):
        rental.plan(facts, company, cik=CIK, available_by=AVAILABLE)


def test_future_proof_cannot_be_admitted_before_its_filing_day():
    with pytest.raises(ValueError):
        rental.apply({'revenue': {}}, *inputs(), cik=CIK, available_by=date(2026, 7, 29), supplement=acquired())

@pytest.mark.parametrize('defect',['hidden_title','hidden_table','intervening_note','intervening_table','nil','changed_amount','duplicate_revenue'])
def test_visibility_nil_and_source_amounts_are_not_relaxed(defect):
    wanted=rental.plan(*inputs(),cik=CIK,available_by=AVAILABLE)
    filing=next(f for f in wanted['filings'].values() if f['report_date']=='2026-09-30')
    tree=html.fromstring(raw('howl-20260930.htm'))
    node=next(n for n in inline.nodes(tree,'nonfraction') if n.get('name')=='us-gaap:'+rental.CONCEPT and n.xpath('ancestor::table'))
    table=node.xpath('ancestor::table')[-1];wrapper=table.getparent()
    title=next(n for n in wrapper.itersiblings(preceding=True) if rental.TITLE.fullmatch(' '.join(n.text_content().split())))
    if defect=='hidden_title':title.set('style','display:none')
    elif defect=='hidden_table':table.set('style','visibility:hidden')
    elif defect=='intervening_note':wrapper.addprevious(html.fromstring('<div>Notes to financial statements</div>'))
    elif defect=='intervening_table':wrapper.addprevious(html.fromstring('<div><table><tr><td>Separate disclosure</td></tr></table></div>'))
    elif defect=='nil':
        node.set('xmlns:xsi','http://www.w3.org/2001/XMLSchema-instance');node.set('xsi:nil','true')
    elif defect=='duplicate_revenue':node.xpath('ancestor::tr')[-1].addnext(deepcopy(node.xpath('ancestor::tr')[-1]))
    else:
        target=next(n for n in inline.nodes(table,'nonfraction') if n.get('name')=='us-gaap:'+rental.CONCEPT and inline._number(n,tree)==21_000_000)
        target.text='100'
    changed=html.tostring(tree)
    try:rows=rental.statement_rows(changed,filing)
    except ValueError:rows={}
    if defect=='changed_amount':
        class Altered(Client):
            def get(self,url):return changed if url==filing['url'] else super().get(url)
        with pytest.raises(ValueError,match='primary_statement_missing'):
            rental.acquire(*inputs(),cik=CIK,available_by=AVAILABLE,client=Altered())
    else:assert not rows


def test_recovery_cache_screener_and_watchlist_do_not_emit_false_alerts(monkeypatch):
    from datetime import timedelta
    from sqlalchemy import create_engine,select
    from sqlalchemy.orm import sessionmaker
    from app.db import Base
    from app.models import FundamentalsCache,MonitoringAlert,Security,UserAccount,Watchlist,WatchlistItem,WatchlistAlertRule
    from app.services import fundamentals_cache as cache
    from app.services.custom_alert_rules import evaluate_watchlist_custom_alerts,validate_conditions
    monkeypatch.setenv('FUNDAMENTALS_PROVIDER','sec_edgar')
    supplement=acquired();value=project(supplement)
    previous=deepcopy(supplement);previous['evidence'].pop('collaboration_revenue_selection')
    before=sec.project_fundamentals(raw('HOWL-facts.json'),raw('HOWL-company.json'),symbol='HOWL',cik='0001785530',observed_at=NOW,filing_supplement=previous)
    engine=create_engine('sqlite://');Base.metadata.create_all(engine)
    with patch.object(socket.socket,'connect',side_effect=AssertionError('No network')),sessionmaker(bind=engine)() as db:
        user=UserAccount(email='howl-collaboration-proof@example.test');security=Security(symbol='HOWL',name=value['company_name'],asset_class='equity')
        db.add_all([user,security]);db.flush();watchlist=Watchlist(name='Offline period proof',owner_user_id=user.id);db.add(watchlist);db.flush()
        db.add(WatchlistItem(watchlist_id=watchlist.id,security_id=security.id,target_type='ticker'))
        db.add(WatchlistAlertRule(user_id=user.id,watchlist_id=watchlist.id,name='Revenue growth transition',enabled=True,scope_type='any_watchlist_ticker',match_type='all',delivery='in_app',conditions_json=json.dumps(validate_conditions([dict(metric='operating_margin',operator='lt',comparison_type='value',comparison_value=0)]))))
        cache.upsert_fundamentals_cache(db,before);db.commit()
        def run(minutes):
            result=evaluate_watchlist_custom_alerts(db,user_id=user.id,watchlist_id=watchlist.id,now=NOW+timedelta(minutes=minutes));db.commit();return result
        assert run(0)['triggered']==0
        cache.upsert_fundamentals_cache(db,value);db.commit()
        row=db.execute(select(FundamentalsCache).where(FundamentalsCache.symbol=='HOWL',FundamentalsCache.provider=='sec_edgar')).scalar_one()
        db.refresh(row)
        summary=cache.fundamentals_summary_from_cache_row(row,now=NOW)
        assert summary['status']=='unavailable' and row.operating_margin==value['operating_margin']
        screen=cache.cache_row_to_screener_row(row)
        for field,mapped in cache.SCREENER_ROW_FIELD_MAP.items():
            if field in value:assert screen.get(mapped)==value[field]
        stamp=row.updated_at;cache.upsert_fundamentals_cache(db,value);db.commit();assert row.updated_at==stamp
        assert run(5)['triggered']==run(10)['triggered']==0
        row.operating_margin=1;db.commit();assert run(15)['triggered']==0
        row.operating_margin=value['operating_margin'];db.commit();assert run(20)['triggered']==1
        assert run(25)['triggered']==0
        assert len(db.scalars(select(MonitoringAlert)).all())==1
    engine.dispose()
