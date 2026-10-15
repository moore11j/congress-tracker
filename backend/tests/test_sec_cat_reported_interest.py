"""Simulated reporting-format changes, not newly observed CAT filings."""
from copy import deepcopy
from datetime import date
import hashlib
import json
from lxml import html
import pytest
from app.services import sec_cat_interest as cat, sec_inline_financials as inline
from test_sec_cat_interest import originals, Source, NOW


@pytest.fixture(scope='module')
def reported(originals):
    """Keep debt originals, replace split interest with consolidated test facts."""
    files,manifest=deepcopy(originals)
    facts=json.loads(files['CAT-facts.json'])
    reports=[]
    for source in manifest['sources']:
        if source['name'] not in {'annual.html','interim.html'}:continue
        raw=files[source['name']]
        periods,_=cat.parse(raw,source)
        tree=html.fromstring(raw)
        contexts={n.get('id'):n for n in inline.nodes(tree,'context')}
        for (start,end),fact in periods.items():
            anchors=[]
            for node in inline.nodes(tree,'nonfraction'):
                if node.get('name')!='us-gaap:ProfitLoss':continue
                ctx=contexts[node.get('contextref')]
                starts,ends=inline.nodes(ctx,'startdate'),inline.nodes(ctx,'enddate')
                if (len(starts)==len(ends)==1 and starts[0].text_content()==str(start)
                        and ends[0].text_content()==str(end) and not inline.nodes(ctx,'segment')):
                    anchors.append(node)
            assert anchors
            anchor=anchors[0]
            node=deepcopy(anchor)
            node.set('name','us-gaap:InterestExpense')
            node.set('id','synthetic-interest-'+str(start)+'-'+str(end))
            node.attrib.pop('sign',None)
            for child in list(node):node.remove(child)
            node.text=str(int(fact['value']/10**int(node.get('scale','0'))))
            tr=html.Element('tr');label=html.Element('td');label.text='Interest expense'
            td=html.Element('td');td.append(node);tr.extend([label,td])
            next(anchor.iterancestors('table')).append(tr)
            reports.append({'start':str(start),'end':str(end),'val':fact['value'],
                'accn':source['accession'],'filed':source['filed'],'form':source['form']})
        for node in inline.nodes(tree,'nonfraction'):
            if node.get('name') in cat.PARTS:
                node.getparent().remove(node)
        raw=html.tostring(tree)
        files[source['name']]=raw
        source['sha256']=hashlib.sha256(raw).hexdigest()
    facts['facts']['us-gaap']['InterestExpense']={'units':{'USD':reports}}
    files['CAT-facts.json']=json.dumps(facts).encode()
    return files,manifest


def projected(reported, supplement=None):
    from app.services import sec_fundamentals as sec
    files,_=reported
    return sec.project_fundamentals(files['CAT-facts.json'],files['CAT-company.json'],
        symbol='CAT',cik=cat.CIK,observed_at=NOW,interest_supplement=supplement)


@pytest.fixture(scope='module')
def acquired(reported):
    before=projected(reported)
    evidence=json.loads(before['source_evidence_json'])
    assert evidence['leverage_preparation']['ebitda']['status']=='components_complete'
    assert evidence['leverage_preparation']['ebitda']['value']==18_426_000_000
    assert evidence['net_debt_policy']['reason']=='no_verified_finance_inclusive_debt'
    client=Source(reported)
    proof=cat.acquire(before,reported[0]['CAT-company.json'],symbol='CAT',cik=cat.CIK,observed_at=NOW,client=client)
    assert proof['evidence']['status']=='originals_verified'
    assert proof['debt_only'] is True and proof['records']=={} and len(client.calls)==2
    return before,proof


def test_reported_interest_debt_recovery_without_old_split_layout(reported,acquired):
    before,proof=acquired
    after=projected(reported,proof)
    assert after==projected(reported,proof)
    assert after['net_debt_to_ebitda']==38_433_000_000/18_426_000_000
    assert {k:v for k,v in after.items() if k not in {'net_debt_to_ebitda','source_evidence_json'}}=={
        k:v for k,v in before.items() if k!='source_evidence_json'}
    evidence=json.loads(after['source_evidence_json'])
    ebitda=evidence['leverage_preparation']['ebitda']
    assert ebitda['value']==18_426_000_000
    assert all(p['tag']=='InterestExpense' and not p.get('derived')
               for p in ebitda['components']['leverage_interest']['inputs'])
    for component in ebitda['components'].values():
        assert all(p['source_cik']==cat.CIK and p['source_sha256'] for p in component['inputs'])
    client=Source(reported)
    assert cat.acquire(after,reported[0]['CAT-company.json'],symbol='CAT',cik=cat.CIK,
                       observed_at=NOW,client=client) is None
    assert client.calls==[]


@pytest.mark.parametrize('change',['missing_tax_original','conflicting_hash','wrong_issuer','wrong_unit'])
def test_partial_original_binding_cannot_publish_ratio_or_corrupt_ebitda(reported,acquired,change):
    before,proof=deepcopy(acquired)
    selected=[k for k in proof['points'] if k[0]=='IncomeTaxExpenseBenefit' and k[1]=='2026-01-01' and k[2]=='2026-06-30']
    assert len(selected)==1
    key=selected[0]
    if change=='missing_tax_original':del proof['points'][key]
    elif change=='conflicting_hash':proof['points'][key]['source_sha256']='0'*64
    elif change=='wrong_issuer':proof['points'][key]['source_cik']='0000000001'
    else:
        # The companyfacts currency change must not borrow a USD original.
        # Directly exercise application with the otherwise selected records.
        from app.services import sec_leverage
        panel=json.loads(before['source_evidence_json'])['leverage_preparation']
        records={name:{(date.fromisoformat(p['start']),date.fromisoformat(p['end'])):deepcopy(p)
                      for p in panel['ebitda']['components'][name]['inputs']}
                 for name in sec_leverage.DURATION}
        records['leverage_tax'][(date(2026,1,1),date(2026,6,30))]['unit']='EUR'
        out,e=cat.apply(records,proof,symbol='CAT',cik=cat.CIK,available_by=date(2026,10,7))
        assert out['leverage_tax'][(date(2026,1,1),date(2026,6,30))]['unit']=='EUR'
        assert not any(p['end']=='2026-06-30' for p in e['reported_components_verified'])
        return
    after=projected(reported,proof)
    assert 'net_debt_to_ebitda' not in after
    e=json.loads(after['source_evidence_json'])['leverage_preparation']['ebitda']
    assert e['status']=='components_complete' and e['value']==18_426_000_000
    assert {k:v for k,v in before.items() if k!='source_evidence_json'}=={
        k:v for k,v in after.items() if k!='source_evidence_json'}


@pytest.mark.parametrize('at',[1,2])
def test_debt_only_source_failure_preserves_reported_values(reported,acquired,at):
    before,_=acquired
    client=Source(reported,fail_at=at)
    proof=cat.acquire(before,reported[0]['CAT-company.json'],symbol='CAT',cik=cat.CIK,observed_at=NOW,client=client)
    after=projected(reported,proof)
    assert {k:v for k,v in before.items() if k!='source_evidence_json'}=={
        k:v for k,v in after.items() if k!='source_evidence_json'}
    a,b=[json.loads(v['source_evidence_json']) for v in (before,after)]
    assert b['leverage_preparation'].pop('interest_reconciliation')['reason']=='source_unavailable'
    assert a==b
