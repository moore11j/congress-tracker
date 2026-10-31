import base64
import hashlib
import io
import json

import pytest

from app.services.institutional_capture_transport import RecordStream,StreamReceiver,encode,MAX_DOCUMENTS


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
    writer.write('metadata',{'transport_version':2,'transaction_read_only':True,'filing_date':'2026-10-07','document_count':MAX_DOCUMENTS})
    for i in range(MAX_DOCUMENTS):writer.write('documents',{**DOC,'id':i+1})
    with pytest.raises(ValueError,match='Row count'):writer.write('documents',{**DOC,'id':MAX_DOCUMENTS+1})
    writer.finish(transaction_read_only=True,production_writes=0,source_bytes=len(RAW)*MAX_DOCUMENTS,query_scope=SCOPE)
    receipt,_=receive(output.getvalue().splitlines(keepends=True))
    assert receipt['counts']['documents']==MAX_DOCUMENTS and receipt['originals_verified']==MAX_DOCUMENTS


def raw_stream(raw=RAW, status='parsed', scope=SCOPE):
    output=io.BytesIO();writer=RecordStream(output)
    writer.write('metadata',dict(transport_version=3,transaction_read_only=True,
                 filing_date='2026-10-07',document_count=1))
    row={k:v for k,v in DOC.items() if k!='raw_base64'}
    row.update(status=status,content_hash=hashlib.sha256(raw).hexdigest() if raw else None)
    writer.write_document(row,raw)
    writer.write('positions',{'id':22,'value':100})
    writer.finish(transaction_read_only=True,production_writes=0,source_bytes=len(raw) if raw else 0,
                  query_scope=scope)
    return output.getvalue()


def raw_receive(data,parser=parser):
    destination=io.BytesIO();receiver=StreamReceiver(destination,parser)
    receiver.read(io.BytesIO(data))
    return receiver.finish(0),destination.getvalue()


def test_raw_transport_preserves_legacy_replay_rows_and_complete_source_scope():
    proof,actual=raw_receive(raw_stream())
    _,expected=receive(stream())
    assert actual==expected and proof['originals_verified']==1
    assert proof['scope_verified'] and proof['source_bytes']==len(RAW)


@pytest.mark.parametrize('raw',[b'\n{"complete":{}}\n',b'\x00\xff\r\n\x80',b'original without newline'])
def test_original_framing_is_binary_safe(raw):
    def exact(feed,original,metadata):
        assert original==raw
        return parser(feed,RAW,metadata)
    proof,spool=raw_receive(raw_stream(raw),parser=exact)
    assert proof['originals_verified']==1
    row=json.loads(spool.splitlines()[1])['row']
    assert base64.b64decode(row['raw_base64'])==raw


def test_raw_reader_handles_short_body_reads():
    class ShortReader(io.BytesIO):
        def read(self,n=-1):return super().read(min(n,3))
    receiver=StreamReceiver(io.BytesIO(),parser)
    receiver.read(ShortReader(raw_stream()))
    assert receiver.finish(0)['originals_verified']==1


@pytest.mark.parametrize('mutation',['length_bool','length_float','length_negative','length_oversize',
                                    'missing_length','ambiguous','truncate_body','delimiter','hash',
                                    'extra_record','missing_receipt','duplicate_json','source_budget'])
def test_raw_framing_failures_cannot_accept_a_baseline(mutation,monkeypatch):
    from app.services import institutional_capture_transport as transport
    data=raw_stream();header,rest=data.split(b'\n',1);doc,body=rest.split(b'\n',1)
    record=json.loads(doc)
    changes={'length_bool':True,'length_float':float(len(RAW)),'length_negative':-1,'length_oversize':30_000_001}
    if mutation in changes:record['row']['raw_length']=changes[mutation]
    elif mutation=='missing_length':record['row'].pop('raw_length')
    elif mutation=='ambiguous':record['row']['raw_base64']=DOC['raw_base64']
    elif mutation=='truncate_body':body=RAW[:-1]
    elif mutation=='delimiter':body=RAW+b'!'+body[len(RAW)+1:]
    elif mutation=='hash':record['row']['content_hash']='0'*64
    elif mutation=='missing_receipt':body=body[:body.rfind(b'{"complete"')]
    elif mutation=='source_budget':monkeypatch.setattr(transport,'MAX_SOURCE_BYTES',len(RAW)-1)
    doc=encode(record)
    if mutation=='duplicate_json':doc=doc.replace(b'"raw_length":17',b'"raw_length":17,"raw_length":17')
    changed=header+b'\n'+doc+b'\n'+body
    if mutation=='extra_record':changed+=b'{"table":"positions","row":{}}\n'
    with pytest.raises(ValueError):raw_receive(changed)


def test_raw_null_quarantine_is_preserved_but_parsed_null_is_rejected():
    proof,spool=raw_receive(raw_stream(None,'quarantined',{'ciks':['12345'],'cusips':[],'periods':[]}))
    assert proof['originals_verified']==0 and proof['source_bytes']==0
    assert json.loads(spool.splitlines()[1])['row']['raw_base64'] is None
    with pytest.raises(ValueError,match='missing original'):raw_receive(raw_stream(None))


@pytest.mark.parametrize('key',['ciks','cusips','periods'])
def test_raw_sources_still_require_complete_original_query_scope(key):
    with pytest.raises(ValueError,match='scope is incomplete'):
        raw_receive(raw_stream(scope={**SCOPE,key:[]}))


def test_raw_write_cannot_mix_encodings_or_versions():
    writer=RecordStream(io.BytesIO())
    with pytest.raises(ValueError,match='version 3'):writer.write_document({},RAW)
    writer.write('metadata',dict(transport_version=3))
    with pytest.raises(ValueError,match='write_document'):writer.write('documents',DOC)
    with pytest.raises(ValueError,match='Ambiguous'):writer.write_document(DOC,RAW)
    with pytest.raises(ValueError,match='Source byte'):writer.write_document({},b'')


def test_raw_read_retains_the_uncompressed_transport_bound(monkeypatch):
    from app.services import institutional_capture_transport as transport
    data=raw_stream();first=data.index(b'\n')
    monkeypatch.setattr(transport,'MAX_TRANSPORT_BYTES',first+1)
    with pytest.raises(ValueError,match='Transport byte bound'):raw_receive(data)
