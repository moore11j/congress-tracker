import base64
import hashlib
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path

import pytest

from app.services.senate_backup_evidence import prepare_filing, bind_original, verify_partitions, PROVIDER

FIXTURES = Path(__file__).with_name('fixtures')
SNAPSHOT = json.loads((FIXTURES/'senate_secondary_october_2026.json').read_text())
NOW = datetime(2026, 10, 12, 3, 40, tzinfo=timezone.utc)


def case(index=1):
    filing = deepcopy(SNAPSHOT['filings'][index])
    rows = deepcopy([r for r in SNAPSHOT['trades'] if r['docId'] == filing['docId']])
    return filing, rows


def prepare(filing, rows, **kwargs):
    return prepare_filing(filing, rows, snapshot=deepcopy(SNAPSHOT['snapshot']), observed_at=NOW, **kwargs)


@pytest.mark.parametrize('index,name,count', [(0,'senate_fetterman_2026_10_07.html',2),
                                             (1,'senate_whitehouse_2026_10_01.html',5)])
def test_saved_secondary_rows_bind_to_exact_official_originals(index, name, count, monkeypatch):
    monkeypatch.setattr('requests.sessions.Session.request', lambda *a, **k: pytest.fail('Network forbidden'))
    filing, rows = case(index)
    before = deepcopy((filing, rows))
    prepared = prepare(filing, rows)
    result = bind_original(prepared, base64.b64decode(SNAPSHOT['originals_base64'][filing['docId']]))
    assert result['status'] == 'matched' and len(result['bindings']) == count
    assert not prepared['publishable'] and not result['publishable']
    assert {r['official_source_line_ref'] for r in result['bindings']} == {str(i) for i in range(1,count+1)}
    assert len({r['official_normalized_hash'] for r in result['bindings']}) == count
    assert prepared == prepare(filing, list(reversed(rows)))
    assert bind_original(prepared, base64.b64decode(SNAPSHOT['originals_base64'][filing['docId']])) == result
    assert (filing, rows) == before
    assert all(r['source_provider'] == PROVIDER and r['source_line_ref'] is None for r in prepared['transactions'])
    assert prepared['availability_basis'] == 'secondary_observation'
    assert prepared['observed_at'] != prepared['reported_available_at']


def test_printed_ticker_retained_and_secondary_resolution_never_invented():
    filing, rows = case()
    prepared = prepare(filing, rows)
    assert [r['ticker_normalized'] for r in prepared['transactions']] == ['JPM','ADI','V','V','JPM']
    rows[0].update(printedTicker=None, resolvedTicker='JPM', resolutionStatus='resolved')
    row = prepare(filing, rows)['transactions'][0]
    assert row['ticker_normalized'] is None and row['secondary_resolved_ticker'] == 'JPM'
    assert 'secondary_resolver_requires_independent_evidence' in row['publication_holds']


def test_unknown_type_with_ticker_is_not_silently_equity():
    filing, rows = case()
    rows[0]['assetTypeLabel'] = None
    row = prepare(filing, rows)['transactions'][0]
    assert row['asset_type_normalized'] == 'unresolved' and row['ticker_normalized'] is None
    assert 'instrument_review_required' in row['publication_holds']


def test_dependent_child_bonds_remain_non_equity():
    filing, rows = case(0)
    prepared = prepare(filing, rows)
    assert all(r['asset_type_normalized']=='corporate_bond' and r['owner_normalized']=='dependent'
               and r['ticker_normalized'] is None for r in prepared['transactions'])


@pytest.mark.parametrize('mutation', [
    lambda f,r:r.pop(),
    lambda f,r:r.__setitem__(1,deepcopy(r[0])),
    lambda f,r:r[0].update(rowIndex=-1),
    lambda f,r:r[0].update(rowIndex=True),
    lambda f,r:r[0].update(memberId='F000479'),
    lambda f,r:r[0].update(rawSha256='0'*64),
    lambda f,r:r[0].update(transactionDate='2026-10-02'),
    lambda f,r:r[0].update(amountLow=float('nan')),
    lambda f,r:r[0].update(amountLow=50000,amountHigh=1001),
    lambda f,r:r[0].update(action='purchase',partialSale=True),
    lambda f,r:f.update(sourceUrl='https://example.com/forged'),
    lambda f,r:f.update(identitySource='unresolved'),
    lambda f,r:f.update(extractionStatus='failed'),
])
def test_invalid_parent_row_or_completeness_refused(mutation):
    filing, rows = case(); mutation(filing, rows)
    with pytest.raises(ValueError): prepare(filing, rows)


@pytest.mark.parametrize('mutation', [
    lambda s:s.update(dataset='another/provider'),
    lambda s:s.update(schemaVersion=3),
    lambda s:s['audit'].update(passed=False),
    lambda s:s.update(generatedAt='2026-10-12T04:00:00Z'),
    lambda s:s.update(lastSourceCheckAt='2026-10-10T00:00:00Z'),
    lambda s:s.update(lastSourceCheckAt='2026-10-11T12:00:00Z'),
    lambda s:s.update(generatedAt='2026-10-11T11:32:37'),
])
def test_snapshot_freshness_and_identity_gates(mutation):
    filing, rows = case(); snapshot=deepcopy(SNAPSHOT['snapshot']); mutation(snapshot)
    with pytest.raises(ValueError):prepare_filing(filing,rows,snapshot=snapshot,observed_at=NOW)


def test_amendments_keep_explicit_reconciliation_gate():
    filing, rows = case(); filing['amendedReportDate']='2026-09-20'
    assert 'amendment_reconciliation_required' in prepare(filing, rows)['publication_holds']


def test_original_tampering_or_disagreeing_economics_cannot_bind():
    filing, rows = case(); raw=base64.b64decode(SNAPSHOT['originals_base64'][filing['docId']])
    with pytest.raises(ValueError, match='bytes'):bind_original(prepare(filing,rows),raw+b'changed')
    rows[0]['owner']='self'
    with pytest.raises(ValueError, match='economics'):bind_original(prepare(filing,rows),raw)


def test_secondary_identity_does_not_depend_on_enrichment_or_snapshot_clock():
    filing, rows = case(); first=prepare(filing,rows)
    rows[0]['comment']='Additional annotation'
    second=prepare_filing(filing,rows,snapshot=SNAPSHOT['snapshot'],observed_at=NOW+timedelta(minutes=1))
    assert first['transactions'][0]['secondary_row_id']==second['transactions'][0]['secondary_row_id']
    assert first['transactions'][0]['evidence_sha256']!=second['transactions'][0]['evidence_sha256']
    assert first['transactions'][0]['source_record']['comment'] is None


@pytest.mark.parametrize('mode',['okay','missing','extra','tampered','wrong_size','duplicate'])
def test_partition_validation_requires_complete_exact_bytes(mode):
    raw=b'example partition'; name='data/political_trades/2026-00000-of-00001.parquet'
    record=dict(publicPath=name,size=len(raw),sha256=hashlib.sha256(raw).hexdigest())
    snapshot={'tables':{'political_trades':{'manifests':[{'year':2026,'files':[record]}]}}}
    files={name:raw}
    if mode=='missing':files={}
    if mode=='extra':files['unexpected']=raw
    if mode=='tampered':files[name]=b'x'*len(raw)
    if mode=='wrong_size':record['size']+=1
    if mode=='duplicate':snapshot['tables']['political_trades']['manifests'][0]['files'].append(record.copy())
    if mode=='okay':assert verify_partitions(snapshot,'political_trades',2026,files)=={name:record['sha256']}
    else:
        with pytest.raises(ValueError):verify_partitions(snapshot,'political_trades',2026,files)


def test_identical_lots_are_held_instead_of_guessing_row_identity(monkeypatch):
    from app.services import direct_feed_collection
    filing,rows=case();raw=base64.b64decode(SNAPSHOT['originals_base64'][filing['docId']])
    original_parser=direct_feed_collection.parse_document
    def duplicate(*args,**kwargs):
        text,parsed,reasons=original_parser(*args,**kwargs)
        extra=deepcopy(parsed['transactions'][0]);extra['source_line_ref']='6'
        extra['normalized_hash']='synthetic-extra-lot'
        parsed['transactions'].append(extra)
        return text,parsed,reasons
    monkeypatch.setattr(direct_feed_collection,'parse_document',duplicate)
    rows.append({**deepcopy(rows[0]),'rowIndex':5});filing['extractedRows']+=1
    result=bind_original(prepare(filing,rows),raw)
    assert result['status']=='held' and result['reason']=='ambiguous_identical_lots'
    assert not result['bindings'] and not result['publishable']
