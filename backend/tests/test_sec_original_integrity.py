"""Actual nine-document delivery variants retain every financial byte and proof."""
from datetime import datetime,timezone
import gzip,hashlib,json,re
from pathlib import Path
import pytest
from app.services import sec_original_integrity as integrity
from app.services import sec_baba_financials as baba,sec_nvo_financials as nvo

ROOT=Path(__file__).parent/'fixtures'
OBSERVED=json.loads((ROOT/'sec_delivery_observations.json').read_text())['sources']
MODULES={'baba':baba,'nvo':nvo}


def original(symbol,key):
    return gzip.decompress((ROOT/('sec_'+symbol)/(key+'.htm.gz')).read_bytes())


def received(row):
    raw=original(row['symbol'],row['key'])
    match=integrity._TERMINAL.search(raw)
    start,end=match.span('path')
    raw=raw[:start]+row['delivery_path'].encode()+raw[end:]
    # Reconstruct only when the complete received-byte digest proves equality.
    assert len(raw)==row['received_bytes']
    assert hashlib.sha256(raw).hexdigest()==row['received_sha256']
    return raw


@pytest.mark.parametrize('row',OBSERVED,ids=lambda r:r['symbol']+'_'+r['key'])
def test_complete_live_original_receipt_and_content_pin(row):
    raw=received(row);source=MODULES[row['symbol']].SOURCES[row['key']]
    proof=integrity.verified_original(raw,source)
    assert proof['source_sha256']==row['received_sha256']!=row['reviewed_sha256']
    assert proof['reviewed_source_sha256']==row['reviewed_sha256']
    assert proof['document_sha256']==row['document_sha256']==integrity.document_digest(original(row['symbol'],row['key']))
    assert proof['document_integrity_policy']==integrity.POLICY


def walk(value):
    if isinstance(value,dict):
        yield value
        for child in value.values():yield from walk(child)
    elif isinstance(value,list):
        for child in value:yield from walk(child)


@pytest.mark.parametrize('symbol',['baba','nvo'])
def test_complete_live_projection_values_and_all_nested_received_provenance(symbol):
    module=MODULES[symbol]
    prior={key:original(symbol,key) for key in module.SOURCES}
    current={r['key']:received(r) for r in OBSERVED if r['symbol']==symbol}
    company=gzip.decompress((ROOT/('sec_'+symbol)/'company.json.gz').read_bytes())
    def project(bundle):
        return module.project(json.dumps({'cik':int(module.CIK)}).encode(),company,symbol=symbol.upper(),cik=module.CIK,
            observed_at=datetime(2026,10,17,tzinfo=timezone.utc),supplement={symbol+'_originals':bundle})
    old,new=project(prior),project(current)
    assert {k:v for k,v in old.items() if k!='source_evidence_json'}=={k:v for k,v in new.items() if k!='source_evidence_json'}
    assert new==project(current)
    expected={r['url']:r['received_sha256'] for r in OBSERVED if r['symbol']==symbol}
    count=0
    for proof in walk(json.loads(new['source_evidence_json'])):
        if proof.get('source_url') in expected:
            assert proof['source_sha256']==expected[proof['source_url']]
            assert proof['document_integrity_policy']==integrity.POLICY
            count+=1
        if proof.get('cover_url') in expected:
            assert proof['cover_sha256']==expected[proof['cover_url']]
    assert count>15


@pytest.mark.parametrize('symbol',['baba','nvo'])
@pytest.mark.parametrize('change',['amount','append','inline_body','duplicate_script','new_attribute','absolute_url','query','missing','nonterminal','closing_tag','extra_content'])
def test_other_changes_remain_held(symbol,change):
    row=next(r for r in OBSERVED if r['symbol']==symbol and r['key']=='current')
    raw=received(row);match=integrity._TERMINAL.search(raw);tag=match.group()
    if change=='amount':
        before=b'268,953' if symbol=='baba' else b'175,311'
        assert before in raw
        raw=raw.replace(before,b'999,999',1)
    elif change=='append':raw+=b' '
    elif change=='inline_body':raw=raw.replace(b'"></script>',b'">999</script>')
    elif change=='duplicate_script':raw=raw.replace(tag,tag+tag)
    elif change=='new_attribute':raw=raw.replace(tag,tag.replace(b' src=',b' async src='))
    elif change=='absolute_url':raw=raw.replace(row['delivery_path'].encode(),b'https://example.test/script.js')
    elif change=='query':raw=raw.replace(row['delivery_path'].encode(),row['delivery_path'].encode()+b'?value=1')
    elif change=='missing':raw=raw.replace(tag,b'')
    elif change=='nonterminal':raw=raw.replace(tag,b'')+tag
    elif change=='closing_tag':raw=raw.replace(b'</body>',b'</BODY>').replace(b'</HTML>',b'</html>')
    elif change=='extra_content':raw=raw.replace(tag,tag+b'<p>New financial statement</p>')
    assert hashlib.sha256(raw).hexdigest()!=row['received_sha256']
    with pytest.raises(ValueError):integrity.verified_original(raw,MODULES[symbol].SOURCES['current'])


@pytest.mark.parametrize('symbol',['baba','nvo'])
def test_future_delivery_rotation_only_and_missing_review_rejected(symbol):
    raw=original(symbol,'current');match=integrity._TERMINAL.search(raw);start,end=match.span('path')
    changed=raw[:start]+b'/FuturePath/OtherSegment/NewToken'+raw[end:]
    source=MODULES[symbol].SOURCES['current']
    assert integrity.verified_original(changed,source)['source_sha256']==hashlib.sha256(changed).hexdigest()
    with pytest.raises(ValueError):integrity.verified_original(changed,{'sha256':source['sha256']})
    with pytest.raises(ValueError):integrity.verified_original(changed,{**source,'document_sha256':'0'*64})
