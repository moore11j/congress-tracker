from copy import deepcopy
from datetime import date
import gzip
import hashlib
import json
from pathlib import Path
from lxml import html
import pytest
from app.services import sec_cat_debt as debt, sec_inline_financials as inline
from test_sec_cat_interest import originals, proof, project, Source, NOW


@pytest.fixture(scope='module')
def balance(originals):
    files, manifest = originals
    source = next(s for s in manifest['sources'] if s['name'] == 'interim.html')
    return files['interim.html'], source


def test_two_full_original_statements_and_exact_repeat(originals):
    files, manifest = originals
    expected = {'annual.html': (12_634_000_000,30_696_000_000,9_980_000_000),
                'interim.html': (13_107_000_000,32_039_000_000,6_713_000_000)}
    for source in manifest['sources']:
        if source['name'] not in expected: continue
        parsed = debt.parse(files[source['name']], source)
        assert parsed == debt.parse(files[source['name']], source)
        assert (*[parsed['records'][n]['value'] for n in debt.NAMES],parsed['cash']['value']) == expected[source['name']]
        rows = parsed['liability_rows']
        assert len(rows) == 15
        assert sum(p['value'] for p in rows[:9]) == rows[9]['value']
        assert sum(p['value'] for p in rows[9:14]) == rows[14]['value']
        assert len(parsed['records'][debt.NAMES[0]]['inputs']) == 3
        assert len(parsed['records'][debt.NAMES[1]]['inputs']) == 2


@pytest.mark.parametrize('change', ['missing','unknown','duplicate','negative','cash','total',
    'member','axis','extra_dimension','period','unit','maturity_label','row_label','fair_value'])
def test_changed_or_incomplete_statement_is_held(balance, change):
    raw,source = balance
    tree=html.fromstring(raw)
    table=next(t for t in tree.xpath('//table') if
        {'us-gaap:Assets',debt.CURRENT_TAG} <= {n.get('name') for n in inline.nodes(t,'nonfraction')})
    nodes=inline.nodes(table,'nonfraction')
    short=next(n for n in nodes if n.get('name') == debt.SHORT_TAG)
    ctx=next(c for c in inline.nodes(tree,'context') if c.get('id') == short.get('contextref'))
    if change=='missing': short.getparent().remove(short)
    elif change=='unknown': short.set('name','cat:OtherCurrentDebt')
    elif change=='duplicate': short.addnext(deepcopy(short))
    elif change=='negative': short.set('sign','-')
    elif change in {'cash','total'}:
        tag='us-gaap:CashAndCashEquivalentsAtCarryingValue' if change=='cash' else 'us-gaap:LiabilitiesCurrent'
        n=next(n for n in nodes if n.get('name')==tag)
        if change=='cash': n.addnext(deepcopy(n))
        else: n.text='39,791'
    elif change in {'member','axis','extra_dimension'}:
        member=next(n for n in ctx.iter() if str(n.tag).lower()=='xbrldi:explicitmember')
        if change=='member': member.text=debt.IND
        elif change=='axis': member.set('dimension','srt:OtherAxis')
        else: member.addnext(deepcopy(member))
    elif change=='period': inline.nodes(ctx,'instant')[0].text='2026-06-29'
    elif change=='unit': short.set('unitref','shares')
    elif change=='fair_value': short.set('name','us-gaap:LongTermDebtFairValue')
    else:
        before,after = ('Long-term debt due after one year:', 'Unclear debt:') if change=='maturity_label' else ('Financial Products','Other Products')
        raw=html.tostring(tree).replace(before.encode(),after.encode())
        tree=html.fromstring(raw)
    raw=html.tostring(tree)
    with pytest.raises((debt.CatDebtError,inline.InlineFactsError,KeyError)):
        debt.parse(raw,{**source,'sha256':hashlib.sha256(raw).hexdigest()})


@pytest.mark.parametrize('change', ['cash_value','cash_accession','cash_hash','cash_issuer','cash_missing',
    'reported','conflict','future','source_hash','debt_value','debt_issuer'])
def test_source_binding_and_existing_observations_retained(balance, change):
    raw,source=balance
    parsed=debt.parse(raw,source)
    key=(None,date(2026,6,30))
    records={'leverage_cash':{key:deepcopy(parsed['cash'])}}
    if change=='cash_value': records['leverage_cash'][key]['value']+=1
    elif change=='cash_accession': records['leverage_cash'][key]['accession']='0000018230-26-000099'
    elif change=='cash_hash': records['leverage_cash'][key]['source_sha256']='0'*64
    elif change=='cash_issuer': records['leverage_cash'][key]['source_cik']='0000000001'
    elif change=='cash_missing': records['leverage_cash']={}
    elif change in {'reported','conflict'}: records[debt.NAMES[0]]={key:None if change=='conflict' else deepcopy(parsed['records'][debt.NAMES[0]])}
    elif change=='future': parsed['source']['filed']='2099-01-01'
    elif change=='source_hash': parsed['source']['sha256']='0'*64
    elif change=='debt_value': parsed['records'][debt.NAMES[0]]['value']+=1
    elif change=='debt_issuer': parsed['records'][debt.NAMES[0]]['source_cik']='0000000001'
    before=deepcopy(records)
    out,evidence=debt.apply(records,parsed,available_by=date(2026,10,7),allowed={
        (source['accession'],source['filed'],source['sha256'],source['url'])})
    assert out==records==before
    assert evidence['status']=='held'


def test_optional_debt_failure_keeps_verified_interest(originals,proof,monkeypatch):
    from app.services import sec_cat_interest as cat
    def fail(*args): raise debt.CatDebtError('changed_layout')
    monkeypatch.setattr(debt,'parse',fail)
    client=Source(originals)
    acquired=cat.acquire(proof[0],originals[0]['CAT-company.json'],symbol='CAT',cik=cat.CIK,observed_at=NOW,client=client)
    out=project(originals,interest_supplement=acquired)
    evidence=json.loads(out['source_evidence_json'])
    assert evidence['leverage_preparation']['ebitda']['value']==18_426_000_000
    assert evidence['leverage_preparation']['interest_reconciliation']['debt_reconciliation']['status']=='held'
    assert evidence['net_debt_policy']['reason']=='no_verified_finance_inclusive_debt'
    assert 'net_debt_to_ebitda' not in out
    assert len(client.calls)==2


def test_generic_parser_still_excludes_dimensional_debt(balance):
    raw,source=balance
    standard=inline.parse(raw,cik=debt.CIK,accession=source['accession'],filed=source['filed'],
                          report_date=source['period'],form=source['form'],url=source['url'])
    assert 'LongTermDebtAndCapitalLeaseObligations' not in standard['facts']


from test_custom_alert_fundamental_basis import scenario


def test_actual_cat_replacement_rebaselines_alert_once(originals, proof, scenario):
    db,rule,row,run=scenario
    assert run(0)['initialized']==1
    row.provider='sec_edgar'
    row.net_debt_to_ebitda=None
    row.source_evidence_json=proof[0]['source_evidence_json']
    assert run(5)['unavailable']==1
    completed=project(originals,interest_supplement=proof[1])
    row.net_debt_to_ebitda=completed['net_debt_to_ebitda']
    row.source_evidence_json=completed['source_evidence_json']
    result=run(10)
    assert result['initialized']==1 and result['triggered']==0
    assert run(15)['triggered']==0
    row.net_debt_to_ebitda=1
    assert run(20)['triggered']==0
    row.net_debt_to_ebitda=completed['net_debt_to_ebitda']
    assert run(25)['triggered']==1
    assert run(30)['triggered']==0
    from sqlalchemy import select,func
    from app.models import MonitoringAlert
    assert db.scalar(select(func.count()).select_from(MonitoringAlert))==1
