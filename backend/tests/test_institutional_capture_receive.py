import json
from pathlib import Path
import sys
import pytest

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'scripts'))
from receive_institutional_capture import receive,backend
sys.path.insert(0,str(backend/'tests'))
from app.services.institutional_capture_file import prepare_file
from app.services.institutional_capture_transport import RecordStream,TABLES
from test_institutional_capture_transport import DOC,RAW,SCOPE,parser

def prepare(root,broken=False,version=3):
    def producer(output):
        writer=RecordStream(output)
        writer.write('metadata',dict(transport_version=version,transaction_read_only=True,document_count=1,filing_date='2026-10-07'))
        if version==3:writer.write_document({k:v for k,v in DOC.items() if k!='raw_base64'},RAW)
        else:writer.write('documents',DOC)
        writer.write('positions',{'id':22})
        return writer.finish(transaction_read_only=True,production_writes=0,source_bytes=len(RAW),query_scope={**SCOPE,'cusips':[]} if broken else SCOPE)
    return prepare_file(root,'a'*32,producer,lambda:None)

@pytest.mark.parametrize('version',[2,3])
def test_complete_local_baseline_preserves_rows_and_empty_tables(tmp_path,version):
    prepare(tmp_path,version=version);receipt=receive(tmp_path,'a'*32,parser)
    baseline=json.loads((tmp_path/('a'*32+'.baseline.json')).read_bytes())
    assert baseline['documents']==[DOC] and baseline['positions']==[{'id':22}]
    assert all(baseline[t]==[] for t in TABLES if t not in {'documents','positions'})
    assert receipt['scope_verified'] and receipt['originals_verified']==1
    with pytest.raises(FileExistsError):receive(tmp_path,'a'*32,parser)

def test_missing_source_scope_never_publishes_accepted_baseline(tmp_path):
    prepare(tmp_path,True)
    with pytest.raises(ValueError,match='scope is incomplete'):receive(tmp_path,'a'*32,parser)
    assert not (tmp_path/('a'*32+'.baseline.json')).exists()
    assert not (tmp_path/('a'*32+'.accepted.json')).exists()


def test_manifest_receipt_tampering_cannot_publish_a_baseline(tmp_path):
    prepare(tmp_path)
    path=tmp_path/('a'*32+'.complete.json')
    value=json.loads(path.read_bytes());value['stream_receipt']['source_bytes']+=1
    path.write_text(json.dumps(value))
    with pytest.raises(ValueError,match='manifest receipt mismatch'):receive(tmp_path,'a'*32,parser)
    assert not (tmp_path/('a'*32+'.baseline.json')).exists()
    assert not (tmp_path/('a'*32+'.accepted.json')).exists()
