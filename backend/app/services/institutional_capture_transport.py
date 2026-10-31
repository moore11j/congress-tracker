"""Bounded offline transport/receiver. This module never connects to production."""
import base64
from collections import Counter
from datetime import date
import hashlib
import json
import re

TABLES=('documents','filings','positions','mapping_positions','mapping_filings',
        'changes','activity','events','holders','summaries')
MAX_DOCUMENTS=8500
MAX_SOURCE_BYTES=1_000_000_000
MAX_RECORD_BYTES=50_000_000
MAX_TRANSPORT_BYTES=2_000_000_000
_NO_BODY=object()


def row_bound(table):
    return 1 if table=='metadata' else MAX_DOCUMENTS if table=='documents' else 5000 if table in {'filings','mapping_filings','holders'} else 150000 if table=='summaries' else 100000


def encode(value):
    return json.dumps(value,sort_keys=True,separators=(',',':'),default=str,allow_nan=False).encode()


def _pairs(items):
    result={}
    for key,value in items:
        if key in result:raise ValueError('Duplicate JSON key')
        result[key]=value
    return result


def _invalid_constant(value):
    raise ValueError('Nonfinite JSON number')


def decode(raw):
    return json.loads(raw,object_pairs_hook=_pairs,parse_constant=_invalid_constant)


class RecordStream:
    """Version 2 JSONL or version 3 length-framed originals under one gzip stream."""
    def __init__(self,output):
        self.output=output;self.digest=hashlib.sha256();self.counts=Counter();self.bytes=0;self.done=False
        self.version=None

    def write(self,table,row):
        if table=='documents' and self.version==3:
            raise ValueError('Version 3 originals require write_document')
        self._write(table,row)

    def write_document(self,row,raw):
        if self.version!=3:raise ValueError('Raw originals require version 3')
        if not isinstance(row,dict) or {'raw_base64','raw_length'} & row.keys():
            raise ValueError('Ambiguous original encoding')
        if raw is not None and (not isinstance(raw,bytes) or not 1<=len(raw)<=30_000_000):
            raise ValueError('Source byte bound')
        self._write('documents',{**row,'raw_length':len(raw) if raw is not None else None},
                    b'' if raw is None else raw)

    def _write(self,table,row,body=_NO_BODY):
        if self.done:raise ValueError('Record after terminal receipt')
        if table not in (*TABLES,'metadata') or not isinstance(row,dict):raise ValueError('Invalid record')
        if (table=='metadata' and self.counts) or (table!='metadata' and self.counts['metadata']!=1):raise ValueError('Metadata must be first and unique')
        if table=='metadata':self.version=row.get('transport_version')
        raw=encode({'table':table,'row':row})
        size=len(raw)+(len(body) if body is not _NO_BODY else 0)
        if size>MAX_RECORD_BYTES or self.bytes+size>MAX_TRANSPORT_BYTES:raise ValueError('Transport byte bound')
        if self.counts[table]>=row_bound(table):raise ValueError('Row count exceeds bound')
        self.output.write(raw+b'\n');self.digest.update(raw+b'\n')
        if body is not _NO_BODY:
            self.output.write(body);self.output.write(b'\n');self.digest.update(body);self.digest.update(b'\n')
        self.bytes+=size;self.counts[table]+=1

    def finish(self,**extra):
        if self.done:raise ValueError('Duplicate terminal receipt')
        if {'counts','bytes','sha256'} & extra.keys():raise ValueError('Reserved receipt field')
        result={**extra,'counts':dict(self.counts),'bytes':self.bytes,'sha256':self.digest.hexdigest()}
        raw=encode({'complete':result})
        if len(raw)>MAX_RECORD_BYTES:raise ValueError('Receipt byte bound')
        self.output.write(raw+b'\n');self.done=True
        return result


class StreamReceiver:
    """Spool untrusted records, but accept only after full original-source closure."""
    def __init__(self,destination,parser=None):
        self.destination=destination;self.parser=parser;self.digest=hashlib.sha256()
        self.counts=Counter();self.bytes=0;self.receipt=None;self.metadata=None
        self.ids=set();self.source_bytes=0;self.originals_verified=0
        self.required_ciks=set();self.required_cusips=set();self.required_periods=set()
        self.canonical_started=False

    def _document(self,row):
        ident=row.get('id')
        if type(ident) is not int or ident<=0 or ident in self.ids:raise ValueError('Invalid or repeated source identity')
        self.ids.add(ident)
        metadata=row.get('metadata')
        if not isinstance(metadata,dict):raise ValueError('Missing source metadata')
        cik=str(metadata.get('cik',''))
        if not re.fullmatch(r'[0-9]{1,10}',cik) or int(cik)==0:raise ValueError('Invalid source CIK')
        filing=date.fromisoformat(metadata['filing_date'])
        current=filing>=date.fromisoformat(self.metadata['filing_date'])
        if current:self.required_ciks.add(str(int(cik)))
        encoded=row.get('raw_base64')
        if encoded is None:
            if row.get('status')=='parsed':raise ValueError('Parsed document missing original')
            return
        if not isinstance(encoded,str) or len(encoded)>40_000_000:raise ValueError('Source byte bound')
        raw=base64.b64decode(encoded,validate=True)
        if not raw or len(raw)>30_000_000:raise ValueError('Source byte bound')
        self.source_bytes+=len(raw)
        if self.source_bytes>MAX_SOURCE_BYTES:raise ValueError('Total source byte bound')
        if hashlib.sha256(raw).hexdigest()!=row.get('content_hash'):raise ValueError('Original hash mismatch')
        if row.get('status')!='parsed':return
        if self.parser is None:
            from app.services.direct_feed_collection import parse_document
            self.parser=parse_document
        _,parsed,_reasons=self.parser('sec_13f',raw,metadata)
        self.required_cusips.update(r['cusip'] for r in parsed['positions'])
        if current:
            period=date.fromisoformat(parsed['metadata']['report_period'])
            self.required_periods.add((period.year,(period.month-1)//3+1))
        self.originals_verified+=1

    def read(self,source):
        """Read bounded headers and exact raw bodies; embedded newlines are data."""
        while line:=source.readline(MAX_RECORD_BYTES+2):
            if len(line)>MAX_RECORD_BYTES+1 or not line.endswith(b'\n'):
                raise ValueError('Record byte bound or incomplete frame')
            item=decode(line[:-1])
            if (self.metadata is not None and self.metadata['transport_version']==3 and
                    isinstance(item,dict) and item.get('table')=='documents'):
                row=item.get('row')
                if not isinstance(row,dict) or 'raw_length' not in row:
                    raise ValueError('Missing original length')
                length=row['raw_length']
                if length is not None and (type(length) is not int or not 1<=length<=30_000_000):
                    raise ValueError('Source byte bound')
                size=length or 0
                if len(line)-1+size>MAX_RECORD_BYTES or self.bytes+len(line)-1+size>MAX_TRANSPORT_BYTES:
                    raise ValueError('Transport byte bound')
                body=bytearray()
                while len(body)<size:
                    chunk=source.read(min(1024*1024,size-len(body)))
                    if not chunk:raise ValueError('Truncated original body')
                    body.extend(chunk)
                if source.read(1)!=b'\n':raise ValueError('Missing original delimiter')
                self.accept(line,source_bytes=bytes(body))
            else:self.accept(line)

    def accept(self,line,*,source_bytes=_NO_BODY):
        if self.receipt is not None:raise ValueError('Data after terminal receipt')
        if not isinstance(line,bytes) or not line.endswith(b'\n'):raise ValueError('Incomplete JSONL frame')
        raw=line[:-1]
        if len(raw)>MAX_RECORD_BYTES:raise ValueError('Record byte bound')
        item=decode(raw)
        if not isinstance(item,dict):raise ValueError('Invalid frame')
        if set(item)=={'complete'}:
            if source_bytes is not _NO_BODY:raise ValueError('Unexpected original body')
            r=item['complete']
            if not isinstance(r,dict):raise ValueError('Invalid receipt')
            counts=r.get('counts')
            if (not isinstance(counts,dict) or not all(type(v) is int and v>=0 for v in counts.values()) or
                type(r.get('bytes')) is not int or type(r.get('source_bytes')) is not int):
                raise ValueError('Invalid receipt numeric types')
            if (r.get('counts')!=dict(self.counts) or r.get('bytes')!=self.bytes or
                r.get('sha256')!=self.digest.hexdigest() or self.counts['metadata']!=1 or not self.counts['documents'] or
                r.get('transaction_read_only') is not True or type(r.get('production_writes')) is not int or r['production_writes']!=0 or
                r.get('source_bytes')!=self.source_bytes):raise ValueError('Terminal receipt mismatch')
            scope=r.get('query_scope',{})
            try:
                ciks=scope['ciks'];cusips=scope['cusips'];periods=scope['periods']
                if not all(isinstance(v,list) for v in (ciks,cusips,periods)):raise ValueError('Invalid scope lists')
                if not all(isinstance(v,str) for v in (*ciks,*cusips)):raise ValueError('Invalid scope identifiers')
                if not all(isinstance(p,list) and len(p)==2 and type(p[0]) is int and type(p[1]) is int and
                           1900<=p[0]<=2200 and 1<=p[1]<=4 for p in periods):raise ValueError('Invalid scope periods')
                actual_periods={tuple(p) for p in periods}
                if (not self.required_ciks<=set(ciks) or not self.required_cusips<=set(cusips) or
                    not self.required_periods<=actual_periods):raise ValueError('Original-source query scope is incomplete')
            except (KeyError,TypeError):raise ValueError('Missing or invalid query scope') from None
            if self.metadata.get('document_count')!=self.counts['documents']:raise ValueError('Inventory count mismatch')
            self.receipt=r
            return
        if set(item)!={'table','row'}:raise ValueError('Unknown frame')
        table,row=item['table'],item['row']
        if table not in (*TABLES,'metadata') or not isinstance(row,dict):raise ValueError('Unknown record')
        normalized=row
        size=len(raw)
        if table=='documents' and self.metadata is not None and self.metadata['transport_version']==3:
            length=row.get('raw_length',_NO_BODY)
            if ('raw_base64' in row or length is _NO_BODY or
                (length is not None and (type(length) is not int or not 1<=length<=30_000_000)) or
                not isinstance(source_bytes,bytes) or len(source_bytes)!=(length or 0)):
                raise ValueError('Invalid original frame')
            size+=len(source_bytes)
            normalized={k:v for k,v in row.items() if k!='raw_length'}
            normalized['raw_base64']=base64.b64encode(source_bytes).decode() if length is not None else None
        elif source_bytes is not _NO_BODY:raise ValueError('Unexpected original body')
        if size>MAX_RECORD_BYTES or self.bytes+size>MAX_TRANSPORT_BYTES:raise ValueError('Transport byte bound')
        if table=='metadata':
            if self.counts or type(row.get('transport_version')) is not int or row['transport_version'] not in (2,3) or row.get('transaction_read_only') is not True:raise ValueError('Invalid initial metadata')
            if type(row.get('document_count')) is not int or not 1<=row['document_count']<=MAX_DOCUMENTS:raise ValueError('Inventory count bound')
            if row.get('filing_date')!='2026-10-07':raise ValueError('Unexpected cutover boundary')
            self.metadata=row
        elif self.metadata is None:raise ValueError('Missing initial metadata')
        elif table=='documents':
            if self.canonical_started:raise ValueError('Sources after canonical records')
            self._document(normalized)
        else:self.canonical_started=True
        if self.counts[table]>=row_bound(table):raise ValueError('Row count exceeds bound')
        self.digest.update(line)
        if source_bytes is not _NO_BODY:self.digest.update(source_bytes);self.digest.update(b'\n')
        self.bytes+=size;self.counts[table]+=1
        # Preserve the accepted local replay contract; only the wire format changes.
        if table=='metadata' and row['transport_version']==3:normalized={**row,'transport_version':2}
        self.destination.write(line if normalized is row else encode({'table':table,'row':normalized})+b'\n')

    def finish(self,returncode):
        if returncode!=0 or self.receipt is None:raise ValueError('Incomplete export; partial spool is not a baseline')
        return {**self.receipt,'originals_verified':self.originals_verified,'scope_verified':True}
