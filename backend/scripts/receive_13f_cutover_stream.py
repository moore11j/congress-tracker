"""Validate a complete export stream and rebuild its baseline on the local host."""
import base64
from collections import Counter
import hashlib
import json
import zlib


TABLES = ('documents','filings','positions','mapping_positions','mapping_filings',
          'changes','activity','events','holders','summaries')


class StreamReceiver:
    def __init__(self, destination):
        self.destination = destination
        self.digest = hashlib.sha256()
        self.counts = Counter()
        self.bytes = 0
        self.receipt = None

    def accept(self, line):
        if line.startswith(('13F_RECORD=','13F_BATCH=')):
            if self.receipt is not None:
                raise ValueError('Record after terminal receipt')
            encoded=line.split('=',1)[1].strip()
            if len(encoded)>70_000_000:
                raise ValueError('Oversized encoded record')
            packed=base64.b64decode(encoded,validate=True)
            decoder=zlib.decompressobj()
            raw=decoder.decompress(packed,50_000_001)
            if len(raw)>50_000_000 or not decoder.eof or decoder.unused_data:
                raise ValueError('Oversized or incomplete compressed record')
            records=raw.split(b'\n') if line.startswith('13F_BATCH=') else [raw]
            if not 1<=len(records)<=100:
                raise ValueError('Oversized record batch')
            for raw in records:
                if self.bytes+len(raw)>500_000_000:
                    raise ValueError('Oversized export')
                record=json.loads(raw)
                table=record['table']
                if table not in (*TABLES,'metadata') or not isinstance(record['row'],dict):
                    raise ValueError('Unknown export record')
                if table=='metadata' and self.counts:
                    raise ValueError('Metadata must occur first and once')
                if table!='metadata' and self.counts['metadata']!=1:
                    raise ValueError('Missing initial metadata')
                self.counts[table]+=1
                bound=1 if table=='metadata' else 1000 if table=='documents' else 5000 if table in {'filings','mapping_filings','holders'} else 100000
                if self.counts[table]>bound:
                    raise ValueError('Row count exceeds bound')
                self.digest.update(raw+b'\n')
                self.bytes+=len(raw)
                self.destination.write(raw+b'\n')
        elif line.startswith('13F_COMPLETE='):
            if self.receipt is not None:
                raise ValueError('Duplicate terminal receipt')
            receipt=json.loads(line.split('=',1)[1])
            if (receipt.get('sha256')!=self.digest.hexdigest() or
                receipt.get('counts')!=dict(self.counts) or receipt.get('bytes')!=self.bytes or
                self.counts['metadata']!=1 or not self.counts['documents'] or
                receipt.get('transaction_read_only') is not True or receipt.get('production_writes')!=0):
                raise ValueError('Terminal export receipt mismatch')
            self.receipt=receipt

    def finish(self, returncode):
        if returncode!=0 or self.receipt is None:
            raise ValueError('Incomplete export; do not use partial records')
        return self.receipt


def baseline_from_records(lines):
    """Call only after StreamReceiver.finish succeeds; materialize on the client."""
    result={key:[] for key in TABLES}
    for raw in lines:
        item=json.loads(raw)
        if item['table']=='metadata':
            result.update(item['row'])
        else:
            result[item['table']].append(item['row'])
    return result
