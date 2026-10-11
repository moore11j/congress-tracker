import io
import json

import pytest

from scripts.export_13f_cutover_stream import RecordStream
from scripts.receive_13f_cutover_stream import StreamReceiver, baseline_from_records


def encoded_stream():
    output=io.StringIO()
    writer=RecordStream(output)
    writer.write('metadata',{'transaction_read_only':True,'filing_date':'2026-10-07'})
    writer.write('documents',{'id':1,'raw_base64':'YWJj','metadata':{'key':'original'}})
    writer.finish(transaction_read_only=True,production_writes=0)
    return output.getvalue().splitlines()


def test_complete_stream_binds_records_and_preserves_baseline_shape():
    output=io.BytesIO();reader=StreamReceiver(output)
    for line in encoded_stream():reader.accept(line)
    receipt=reader.finish(0)
    data=baseline_from_records(output.getvalue().splitlines())
    assert receipt['counts']=={'metadata':1,'documents':1}
    assert data['documents'][0]['raw_base64']=='YWJj'
    assert data['transaction_read_only'] and data['positions']==[]


@pytest.mark.parametrize('mutation',['missing_terminal','nonzero_exit','changed_count','changed_digest','duplicate_terminal','record_after_terminal'])
def test_partial_or_tampered_stream_cannot_become_a_baseline(mutation):
    lines=encoded_stream();code=0
    if mutation=='missing_terminal':lines.pop()
    elif mutation=='nonzero_exit':code=1
    elif mutation in {'changed_count','changed_digest'}:
        receipt=json.loads(lines[-1].split('=',1)[1])
        if mutation=='changed_count':receipt['counts']['documents']=2
        else:receipt['sha256']='0'*64
        lines[-1]='13F_COMPLETE='+json.dumps(receipt)
    elif mutation=='duplicate_terminal':lines.append(lines[-1])
    else:lines.append(lines[1])
    reader=StreamReceiver(io.BytesIO())
    with pytest.raises(ValueError):
        for line in lines:reader.accept(line)
        reader.finish(code)


def test_oversized_records_never_emit_completion():
    output=io.StringIO();writer=RecordStream(output)
    writer.bytes=500_000_000
    with pytest.raises(ValueError,match='bound'):writer.write('documents',{'id':1})
    assert not output.getvalue()


def test_reordered_metadata_is_rejected():
    reader=StreamReceiver(io.BytesIO())
    with pytest.raises(ValueError,match='metadata'):reader.accept(encoded_stream()[1])


def test_canonical_batches_preserve_every_row_and_final_partial_batch():
    output=io.StringIO();writer=RecordStream(output)
    writer.write('metadata',{'transaction_read_only':True})
    writer.write('documents',{'id':1})
    for i in range(203):writer.write('positions',{'id':i,'symbol':'AAA'})
    writer.finish(transaction_read_only=True,production_writes=0)
    lines=output.getvalue().splitlines()
    assert len([x for x in lines if x.startswith('13F_BATCH=')])==3
    spool=io.BytesIO();reader=StreamReceiver(spool)
    for line in lines:reader.accept(line)
    assert reader.finish(0)['counts']['positions']==203
    data=baseline_from_records(spool.getvalue().splitlines())
    assert [r['id'] for r in data['positions']]==list(range(203))
