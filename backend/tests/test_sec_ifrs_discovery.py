"""A qualified IFRS filing does not bypass incomplete universe discovery."""
from copy import deepcopy
import json
from test_sec_ifrs_declarations import sources, NOW, offline
from app.services import sec_financial_discovery as discovery, sec_financial_discovery_reader as reader


def test_original_qualification_retry_and_incomplete_scan_publication_hold():
    raw,args,originals,selected=sources()
    kw=dict(symbol='SPOT',cik=args['cik'],available_by=args['available_by'],observed_at=NOW)
    company=args['company_raw']
    # HTML qualification never consumes companyfacts values. The identity-only
    # stub ensures none can enter this result by a second provider path.
    facts=json.dumps({'cik':int(args['cik'])}).encode()
    plan=discovery.inspect(company,**kw)
    candidate=next(c for c in discovery._inventory(company,symbol='SPOT',cik=args['cik'],available_by=args['available_by'])
                   if c['accession']==selected['accession'])
    cover=discovery._observe(raw,candidate,NOW)
    assert cover['primary_statement'] and cover['status']=='observed'
    key=discovery._fingerprint(candidate)
    state=plan['state'];state['receipts'][key]=cover
    class Client:
        def __init__(self):self.calls=[]
        def get(self,url):self.calls.append(url);return originals[url]
    client=Client()
    result=reader.inspect_candidate(company,facts,checkpoint=state,
        accession=selected['accession'],client=client,**kw)
    assert client.calls==[selected['cover_url']]
    receipt=result['receipt']
    assert receipt['status']=='qualified' and receipt['kind']=='html_table'
    assert receipt['current_period']=='2026-09-30' and not receipt['financial_publication_eligible']
    assert len(receipt['originals'])==1 and not receipt['holds']
    assert result==reader.inspect_candidate(company,facts,checkpoint=state,
        accession=selected['accession'],client=Client(),**kw)
    state['receipts'][key]['qualification']=receipt
    after=discovery.inspect(company,previous=state,**kw)
    assert after['state']['receipts'][key]['qualification']==receipt
    selection=reader.selection_plan(company,checkpoint=state,**kw)
    assert selection['status']=='pending' and selection['pending_covers']>0
    assert not selection['financial_publication_eligible']
    old=deepcopy(state)
    old['receipts'][key]['qualification'].update(status='held',kind=None,current_period=None,
        holds=[{'accession':selected['accession'],'reason':'table_accounting_basis_unproved'}])
    retried=discovery.inspect(company,previous=old,**kw)
    assert 'qualification' not in retried['state']['receipts'][key]
    assert retried['state']['receipts'][key]['document_sha256']==cover['document_sha256']
