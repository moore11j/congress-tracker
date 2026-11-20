"""Adversarial grounding and cross-account projection checks for Walnut 2.0."""
from copy import deepcopy
from datetime import datetime, timezone
from decimal import Decimal
import socket

import pytest
from pydantic import ValidationError

from app.intelligence_contract import EvidenceBundle, cache_fingerprint, for_reader


@pytest.fixture
def payload():
    # Synthetic identities/values; no customer records or live source calls.
    return dict(result_id='fixture', operation='analyze_stock', query='Explain retained TEST evidence',
                security_ids=[101], data_cutoff='2026-11-19T21:00:00Z', generated_at='2026-11-19T21:02:00Z',
                last_successful_refresh='2026-11-19T21:01:00Z',
                coverage=dict(state='partial', detail='Financial statements not included'),
                access_projection='premium', projection_revision='projection-1', methodology_version='reported-v1',
                sources=[dict(id='source-1', record_id='retained-123', revision='revision-1', security_id=101,
                              locator='minute close', published_at='2026-11-19T20:44:00Z',
                              event_at='2026-11-19T20:44:00Z', first_available_at='2026-11-19T20:59:00Z')],
                observations=[dict(id='obs-1', source_id='source-1', security_id=101, metric='minute_close',
                                   value='94.51', unit='money_per_share', currency='USD')],
                facts=[dict(id='fact-1', observation_id='obs-1', label='Delayed minute close',
                            value='94.51', unit='money_per_share', currency='USD')],
                interpretations=[dict(text='The displayed quote is delayed.', fact_ids=['fact-1'])],
                limitations=['This fixture does not establish freshness or financial coverage.'])


def test_reported_evidence_roundtrip_is_passive_and_preserves_provenance(payload, monkeypatch):
    def forbidden(*a, **k):
        pytest.fail('Contract validation attempted transport')
    monkeypatch.setattr(socket.socket, 'connect', forbidden)
    bundle=EvidenceBundle.model_validate(payload)
    restored=EvidenceBundle.model_validate_json(bundle.model_dump_json())
    assert restored==bundle
    assert restored.facts[0].value==Decimal('94.51')
    assert restored.sources[0].first_available_at==datetime(2026,11,19,20,59,tzinfo=timezone.utc)
    assert for_reader(restored,authorized_user_id=17,access_projection='premium')==bundle


@pytest.mark.parametrize('field,value', [('value','94.52'),('unit','money'),('currency','EUR'),('observation_id','invented')])
def test_changed_numeric_claim_or_reference_rejected(payload,field,value):
    payload['facts'][0][field]=value
    with pytest.raises(ValidationError):EvidenceBundle.model_validate(payload)


@pytest.mark.parametrize('collection,field,value', [
    ('sources','security_id',102),('observations','security_id',102),
    ('observations','source_id','unretained'),
    ('sources','first_available_at','2026-11-19T21:01:00Z'),
    ('sources','published_at','2026-11-19T21:01:00Z'),
    ('sources','first_available_at','2026-11-19T20:59:00'),
    ('observations','value','NaN'),('observations','value','Infinity'),
    ('observations','currency',None),('observations','unit','ratio'),
    ('sources','url','https://example.com/unretained'),
])
def test_invalid_source_or_measurement_rejected(payload,collection,field,value):
    payload[collection][0][field]=value
    with pytest.raises(ValidationError):EvidenceBundle.model_validate(payload)


@pytest.mark.parametrize('collection',['sources','observations','facts'])
def test_duplicate_identity_rejected_even_when_bytes_equal(payload,collection):
    payload[collection].append(deepcopy(payload[collection][0]))
    with pytest.raises(ValidationError):EvidenceBundle.model_validate(payload)


@pytest.mark.parametrize('collection',['interpretations','risks','watch_next'])
@pytest.mark.parametrize('refs',[[],['invented'],['fact-1','fact-1']])
def test_all_interpretation_categories_require_valid_citations(payload,collection,refs):
    payload[collection]=[dict(text='Unsupported assertion',fact_ids=refs)]
    with pytest.raises(ValidationError):EvidenceBundle.model_validate(payload)


@pytest.mark.parametrize('state',['quiet','unavailable','partial','stale'])
def test_empty_states_remain_explicit_without_fabricated_facts(payload,state):
    for key in ['sources','observations','facts','interpretations']:payload[key]=[]
    payload['coverage']=dict(state=state,detail='No retained qualifying evidence for requested scope')
    bundle=EvidenceBundle.model_validate(payload)
    assert bundle.coverage.state==state and not bundle.facts
    payload['coverage']['state']='complete'
    with pytest.raises(ValidationError):EvidenceBundle.model_validate(payload)


def test_available_partial_evidence_cannot_be_mislabeled_unavailable(payload):
    payload['coverage']['state']='unavailable'
    with pytest.raises(ValidationError):EvidenceBundle.model_validate(payload)


@pytest.mark.parametrize('field',['generated_at','last_successful_refresh','data_cutoff'])
def test_naive_result_timestamps_rejected(payload,field):
    payload[field]='2026-11-19T21:00:00'
    with pytest.raises(ValidationError):EvidenceBundle.model_validate(payload)


def test_result_cannot_claim_future_cutoff_or_refresh(payload):
    for field in ['data_cutoff','last_successful_refresh']:
        changed=deepcopy(payload);changed[field]='2026-11-20T00:00:00Z'
        with pytest.raises(ValidationError):EvidenceBundle.model_validate(changed)


@pytest.mark.parametrize('reader',[None,18])
def test_private_projection_cannot_cross_accounts(payload,reader):
    payload.update(operation='get_thesis_changes',owner_user_id=17)
    bundle=EvidenceBundle.model_validate(payload)
    assert for_reader(bundle,authorized_user_id=17,access_projection='premium')==bundle
    with pytest.raises(PermissionError):for_reader(bundle,authorized_user_id=reader,access_projection='premium')


@pytest.mark.parametrize('projection',['guest','free','pro','admin'])
def test_plan_change_requires_new_projection(payload,projection):
    bundle=EvidenceBundle.model_validate(payload)
    with pytest.raises(PermissionError):for_reader(bundle,authorized_user_id=17,access_projection=projection)


@pytest.mark.parametrize('operation',['get_thesis_changes','get_monitoring_updates'])
def test_personal_operations_cannot_create_shared_bundle(payload,operation):
    payload['operation']=operation
    with pytest.raises(ValidationError):EvidenceBundle.model_validate(payload)


def test_fingerprint_partitions_owners_projections_and_corrections(payload):
    original=cache_fingerprint(EvidenceBundle.model_validate(payload))
    variations=[]
    for change in [{'owner_user_id':17},{'owner_user_id':18},{'access_projection':'pro'}, {'projection_revision':'revision-2'}]:
        changed=deepcopy(payload);changed.update(change)
        variations.append(cache_fingerprint(EvidenceBundle.model_validate(changed)))
    changed=deepcopy(payload);changed['sources'][0]['revision']='corrected-2'
    variations.append(cache_fingerprint(EvidenceBundle.model_validate(changed)))
    assert len(set([original,*variations]))==6


def test_consumption_revalidates_bypassed_model_copy(payload):
    bundle=EvidenceBundle.model_validate(payload)
    corrupted=bundle.model_copy(update={'facts':(bundle.facts[0].model_copy(update={'value':Decimal('1')}),)})
    with pytest.raises(ValidationError):for_reader(corrupted,authorized_user_id=17,access_projection='premium')


def test_size_and_unknown_operation_bounds(payload):
    for change in [{'operation':'execute_sql'},{'security_ids':list(range(1,22))},{'query':'x'*2001},{'sql':'SELECT 1'}]:
        changed=deepcopy(payload);changed.update(change)
        with pytest.raises(ValidationError):EvidenceBundle.model_validate(changed)
