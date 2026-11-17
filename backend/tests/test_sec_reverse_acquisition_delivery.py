"""Actual delivery variants preserve financial bytes and received provenance."""
from copy import deepcopy
import gzip,hashlib,json
from pathlib import Path
import pytest
from test_sec_reverse_acquisition import acquire,project,raw,Client,NOW
from app.services import sec_reverse_acquisition as reverse
from app.services import sec_fundamentals as sec

ROOT=Path(__file__).parent/'fixtures/sec_reverse_acquisition_delivery'
def original(name):
    meta=json.loads((ROOT/'sources.json').read_bytes())[name]
    value=gzip.decompress((ROOT/meta['file']).read_bytes())
    assert len(value)==meta['bytes'] and hashlib.sha256(value).hexdigest()==meta['sha256']
    return value

class DeliveryClient(Client):
    def get(self,url):
        self.calls.append(url)
        return original(self.names[url])

def test_actual_delivery_variants_keep_values_and_received_hashes():
    client=DeliveryClient();source=acquire(client,original('VMRK-company.json'))
    value=project(source,company=original('VMRK-company.json'));before=project()
    assert {k:v for k,v in value.items() if k!='source_evidence_json'}=={k:v for k,v in before.items() if k!='source_evidence_json'}
    proof=json.loads(value['source_evidence_json']);assert reverse.cached_valid(proof)
    receipts=proof['financial_continuity']['received_sources']
    for name,reviewed in reverse.review()['sources'].items():
        received=hashlib.sha256(original(reviewed['filename'])).hexdigest()
        assert received!=reviewed['sha256']
        assert receipts[name]['source_sha256']==received
        assert receipts[name]['reviewed_source_sha256']==reviewed['sha256']
    for point in proof['current']['revenue']['inputs']:
        assert point['source_sha256'] in {r['source_sha256'] for r in receipts.values()}
    assert len(client.calls)==len(set(client.calls))==3
    assert project(source,company=original('VMRK-company.json'))==value

@pytest.mark.parametrize('name',['current','annual','comparative'])
@pytest.mark.parametrize('change',['financial','whitespace','script_attribute'])
def test_delivery_policy_does_not_ignore_financial_or_other_document_changes(name,change):
    source=acquire(DeliveryClient(),original('VMRK-company.json'))
    value=source['reverse_acquisition'][name]
    if change=='financial':changed=value.replace(b'2025',b'2024',1)
    elif change=='whitespace':changed=value+b' '
    else:changed=value.replace(b'<script type="text/javascript"',b'<script type="application/javascript"',1)
    assert changed!=value;source['reverse_acquisition'][name]=changed
    with pytest.raises(sec.SecFundamentalsCoverageError,match='original_changed'):project(source)

def test_acquisition_failure_preserves_actionable_integrity_reason():
    with pytest.raises(sec.SecFundamentalsCoverageError,match='reverse_acquisition_original_changed'):
        project({'reverse_acquisition_error':'reverse_acquisition_original_changed'})

def test_vmrk_preparation_rotates_only_its_own_version():
    from app.services.sec_fundamentals_preparation import version_for_symbol
    assert 'reviewed_accounting_delivery_v2' in version_for_symbol('VMRK')
    assert 'reviewed_accounting_delivery_v2' not in version_for_symbol('HRB')
