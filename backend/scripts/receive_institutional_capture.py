"""Verify an already approved/downloaded capture locally. No network or production access."""
import gzip
import hashlib
import json
import os
from pathlib import Path
import sys

backend=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(backend));os.environ.setdefault('DATABASE_URL','sqlite://')
from app.services.institutional_capture_transport import StreamReceiver,TABLES,encode
from app.services.institutional_capture_file import verify_file


def receive(root,cycle,parser=None):
    root=Path(root);manifest=verify_file(root,cycle)
    destination=root/(cycle+'.baseline.json');receipt_path=root/(cycle+'.accepted.json')
    spool=root/(cycle+'.records.gz');pending=root/(cycle+'.baseline.pending.json')
    if any(p.exists() for p in (destination,receipt_path,spool,pending)):
        raise FileExistsError('Local capture already processed or partial; inspect before retry')
    with spool.open('xb') as raw,gzip.GzipFile(fileobj=raw,mode='wb',compresslevel=1) as records:
        receiver=StreamReceiver(records,parser)
        with gzip.open(root/manifest['filename'],'rb') as source:
            receiver.read(source)
        proof=receiver.finish(0)
    if {k:v for k,v in proof.items() if k not in {'originals_verified','scope_verified'}}!=manifest['stream_receipt']:
        raise ValueError('Capture manifest receipt mismatch')
    if verify_file(root,cycle)!=manifest:raise ValueError('Capture changed during acceptance')
    # Materialize the legacy baseline contract in table order without loading the
    # full current cohort into memory. Out-of-order records fail before acceptance.
    with pending.open('xb') as output,gzip.open(spool,'rb') as records:
        metadata=json.loads(records.readline())
        if metadata['table']!='metadata' or set(metadata['row']) & set(TABLES):
            raise ValueError('Invalid baseline metadata')
        output.write(encode(metadata['row'])[:-1]);index=-1;first=True
        for line in records:
            item=json.loads(line);target=TABLES.index(item['table'])
            if target<index:raise ValueError('Interleaved canonical tables cannot form baseline')
            while index<target:
                if index>=0:output.write(b']')
                index+=1;output.write(b','+encode(TABLES[index])+b':[');first=True
            if not first:output.write(b',')
            output.write(encode(item['row']));first=False
        if index>=0:output.write(b']')
        for table in TABLES[index+1:]:output.write(b','+encode(table)+b':[]')
        output.write(b',"summary_evidence":'+encode(proof.get('summary_evidence'))+b'}')
        output.flush();os.fsync(output.fileno())
    digest=hashlib.sha256();size=0
    with pending.open('rb') as source:
        while chunk:=source.read(1024*1024):digest.update(chunk);size+=len(chunk)
    accepted={**proof,'baseline_sha256':digest.hexdigest(),'baseline_bytes':size,
              'compressed_sha256':manifest['compressed_sha256'],'request_id':cycle}
    os.link(pending,destination)
    with receipt_path.open('xb') as receipt:receipt.write(encode(accepted));receipt.flush();os.fsync(receipt.fileno())
    pending.unlink()
    return accepted


if __name__=='__main__':
    result=receive(sys.argv[1],sys.argv[2])
    print(json.dumps({k:result[k] for k in ('request_id','originals_verified','scope_verified','baseline_sha256','baseline_bytes')}))
