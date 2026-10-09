import copy
import hashlib
from dataclasses import FrozenInstanceError
from datetime import date

import pytest

from app.clients.direct_sources import DirectSourceError
from app.services import direct_13f_evidence as evidence
from app.services.direct_feed_collection import parse_document
from test_direct_13f_evidence import nport,peer
from test_direct_13f_publication import document,SINCE
from test_direct_13f_worker import db,stage,pair


def test_prepared_evidence_keeps_availability_and_peer_decisions_without_reparsing(monkeypatch):
    sources=[nport(),nport(symbol='BBB',filed='20261007')]
    comparisons=[peer('0000000001'),peer('0000000002',serial=3)]
    target=document(rows=[('000361105',100,100,'')])
    _,parsed,_=parse_document('sec_13f',target['raw'],target['metadata'])
    expected=evidence.value_consistency_issues(parsed,comparisons)
    prepared=evidence.Prepared13FEvidence.build(sources,comparisons)
    monkeypatch.setattr(evidence,'parse_document',lambda *a,**k:pytest.fail('Repeated peer XML parse'))
    for _ in range(2):
        assert evidence.value_consistency_issues(parsed,comparisons,prepared=prepared)==expected
        assert evidence.nport_identifiers(sources[0],available_by=SINCE,prepared=prepared)[0]['symbol']=='AAA'
        assert evidence.nport_identifiers(sources[1],available_by=SINCE,prepared=prepared)==[]
        assert evidence.nport_identifiers(sources[1],available_by=date(2026,10,7),prepared=prepared)[0]['symbol']=='BBB'
    rows=prepared.identifiers(sources[0],SINCE);rows[0]['symbol']='TAMPERED'
    assert prepared.identifiers(sources[0],SINCE)[0]['symbol']=='AAA'
    with pytest.raises(FrozenInstanceError):prepared._identifiers=()


@pytest.mark.parametrize('tamper',['bytes','checksum','url','metadata','rehashed_bytes'])
def test_prepared_evidence_rejects_source_drift(tamper):
    source=nport();comparison=peer('0000000001')
    prepared=evidence.Prepared13FEvidence.build([source],[comparison])
    changed=copy.deepcopy(comparison if tamper=='metadata' else source)
    if tamper=='bytes':changed['raw']+=b'changed'
    elif tamper=='checksum':changed['content_hash']='wrong'
    elif tamper=='url':changed['url']=changed['url'].replace('1592900/','1/')
    elif tamper=='rehashed_bytes':
        changed['raw']=changed['raw'].replace(b'Example',b'Changed')
        changed['content_hash']=hashlib.sha256(changed['raw']).hexdigest()
    else:changed['metadata']['filing_date']='2026-01-01'
    with pytest.raises(DirectSourceError):
        if tamper=='metadata':prepared.comparison(changed)
        else:prepared.identifiers(changed,SINCE)


def test_actual_batch_parses_each_peer_once_and_repeats_without_new_work(db,monkeypatch):
    from app.services.direct_13f_batch import publish_13f_batch
    for source in pair():stage(db,source)
    calls=[];original=evidence.parse_document
    def counted(*args,**kwargs):
        calls.append(args[2]['key'])
        return original(*args,**kwargs)
    monkeypatch.setattr(evidence,'parse_document',counted)
    result=publish_13f_batch(db,identifier_documents=[],limit=2)
    assert result['processed']==2 and result['feed_events']>0
    assert len(calls)==2 and len(set(calls))==2
    assert publish_13f_batch(db,identifier_documents=[])['processed']==0
    assert len(calls)==2
