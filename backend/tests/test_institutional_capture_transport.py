import base64
import hashlib
import io
import json

import pytest

from app.services.institutional_capture_transport import RecordStream,StreamReceiver,encode


RAW=b'original holdings'
DOC={'id':1,'metadata':{'cik':'0000012345','filing_date':'2026-10-08'},'status':'parsed','content_hash':hashlib.sha256(RAW).hexdigest(),'raw_base64':base64.b64encode(RAW).decode()}
SCOPE={'ciks':['12345'],'cusips':['123456789'],'periods':[[2026,3]]}


def parser(feed,raw,metadata):
    assert feed=='sec_13f' and raw==RAW and metadata==DOC['metadata']
    return '',{'metadata':{'report_period':'2026-09-30'},'positions':[{'cusip':'123456789'}]},[]


def stream(doc=None,scope=None,**receipt):
    output=io.BytesIO();writer=RecordStream(output)
    writer.write('metadata',{'transport_version':2,'transaction_read_only':True,'filing_date':'2026-10-07','document_count':1})
    writer.write('documents',DOC if doc is None else doc)
    writer.write('positions',{'id':22,'value':100})
    writer.finish(transaction_read_only=True,production_writes=0,source_bytes=len(RAW),query_scope=SCOPE if scope is None else scope,**receipt)
    return output.getvalue().splitlines(keepends=True)


def receive(lines,code=0):
    output=io.BytesIO();reader=StreamReceiver(output,parser)
    for line in lines:reader.accept(line)
    return reader.finish(code),output.getvalue()


def test_complete_scope_replays_original_and_preserves_every_record():
    lines=stream();receipt,spool=receive(lines)
    assert receipt['scope_verified'] and receipt['originals_verified']==1
    assert spool==b''.join(lines[:-1])


@pytest.mark.parametrize('field',['counts','bytes','source_bytes','period_year','period_quarter'])
def test_equal_numeric_values_with_wrong_types_are_rejected(field):
    lines=stream();receipt=json.loads(lines[-1])['complete']
    if field=='counts':receipt['counts']['metadata']=True
    elif field in {'bytes','source_bytes'}:receipt[field]=float(receipt[field])
    elif field=='period_year':receipt['query_scope']['periods'][0][0]=2026.0
    else:receipt['query_scope']['periods'][0][1]=3.0
    lines[-1]=encode({'complete':receipt})+b'\n'
    with pytest.raises(ValueError):receive(lines)


@pytest.mark.parametrize('key',['ciks','cusips','periods'])
def test_missing_query_scope_fails_even_with_valid_transport_checksum(key):
    with pytest.raises(ValueError,match='scope is incomplete'):
        receive(stream(scope={**SCOPE,key:[]}))


@pytest.mark.parametrize('mutation',['truncate','nonzero','digest','count','bytes','source_bytes','no_scope','extra_terminal','extra_record','unknown','duplicate_json_key','nonfinite','readwrite','writes'])
def test_framing_and_receipt_tampering_cannot_be_accepted(mutation):
    lines=stream();code=0
    if mutation=='truncate':lines.pop()
    elif mutation=='nonzero':code=1
    elif mutation=='extra_terminal':lines.append(lines[-1])
    elif mutation=='extra_record':lines.append(lines[1])
    elif mutation=='unknown':lines.insert(1,b'{"unknown":1}\n')
    elif mutation=='duplicate_json_key':lines[1]=lines[1].replace(b'"id":1',b'"id":1,"id":2')
    elif mutation=='nonfinite':lines[2]=lines[2].replace(b'100',b'NaN')
    else:
        receipt=json.loads(lines[-1])['complete']
        if mutation=='digest':receipt['sha256']='0'*64
        elif mutation=='count':receipt['counts']['documents']=2
        elif mutation=='bytes':receipt['bytes']+=1
        elif mutation=='source_bytes':receipt['source_bytes']+=1
        elif mutation=='no_scope':receipt.pop('query_scope')
        elif mutation=='readwrite':receipt['transaction_read_only']=False
        elif mutation=='writes':receipt['production_writes']=1
        lines[-1]=encode({'complete':receipt})+b'\n'
    with pytest.raises(ValueError):receive(lines,code)


@pytest.mark.parametrize('field,value',[('raw_base64',None),('raw_base64','!!!'),('content_hash','0'*64),('id',True),('id',0)])
def test_invalid_source_fails(field,value):
    with pytest.raises(ValueError):receive(stream(doc={**DOC,field:value}))


def test_full_parser_failure_is_not_ignored():
    def reject(*a):raise ValueError('original financial validation failed')
    reader=StreamReceiver(io.BytesIO(),reject)
    with pytest.raises(ValueError,match='financial validation'):
        for line in stream():reader.accept(line)


def test_duplicate_source_and_sources_after_canonical_fail():
    lines=stream()
    for insertion in (2,3):
        changed=lines[:insertion]+[lines[1]]+lines[insertion:]
        with pytest.raises(ValueError):receive(changed)


def test_inventory_count_mismatch_fails_with_recomputed_digest():
    lines=stream();value=json.loads(lines[0]);value['row']['document_count']=2
    lines[0]=encode(value)+b'\n';receipt=json.loads(lines[-1])['complete']
    receipt['sha256']=hashlib.sha256(b''.join(lines[:-1])).hexdigest()
    receipt['bytes']=sum(len(x)-1 for x in lines[:-1]);lines[-1]=encode({'complete':receipt})+b'\n'
    with pytest.raises(ValueError,match='Inventory count mismatch'):receive(lines)


def test_terminal_fields_cannot_override_digest_and_rows_after_finish_fail():
    output=io.BytesIO();writer=RecordStream(output)
    with pytest.raises(ValueError):writer.finish(sha256='forged')
    writer.finish()
    with pytest.raises(ValueError):writer.write('metadata',{})
    with pytest.raises(ValueError):writer.finish()


def test_missing_newline_fails():
    with pytest.raises(ValueError,match='frame'):receive([stream()[0][:-1]])


def test_exact_summary_capacity_and_first_extra_row_are_bounded():
    output=io.BytesIO();writer=RecordStream(output)
    writer.write('metadata',{'transport_version':2,'transaction_read_only':True,'filing_date':'2026-10-07','document_count':1})
    writer.write('documents',DOC)
    for i in range(150000):writer.write('summaries',{'id':i})
    with pytest.raises(ValueError,match='Row count'):writer.write('summaries',{'id':150000})
    writer.finish(transaction_read_only=True,production_writes=0,source_bytes=len(RAW),query_scope=SCOPE)
    receipt,_=receive(output.getvalue().splitlines(keepends=True))
    assert receipt['counts']['summaries']==150000


def test_exact_document_capacity_and_first_extra_row_are_bounded():
    output=io.BytesIO();writer=RecordStream(output)
    writer.write('metadata',{'transport_version':2,'transaction_read_only':True,'filing_date':'2026-10-07','document_count':6000})
    for i in range(6000):writer.write('documents',{**DOC,'id':i+1})
    with pytest.raises(ValueError,match='Row count'):writer.write('documents',{**DOC,'id':6001})
    writer.finish(transaction_read_only=True,production_writes=0,source_bytes=len(RAW)*6000,query_scope=SCOPE)
    receipt,_=receive(output.getvalue().splitlines(keepends=True))
    assert receipt['counts']['documents']==6000 and receipt['originals_verified']==6000
