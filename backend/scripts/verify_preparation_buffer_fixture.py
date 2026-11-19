from pathlib import Path
import os,sys,json,gzip,hashlib,gc,time,resource,signal,socket
b=Path(__file__).resolve().parents[1]
os.environ['DATABASE_URL']='sqlite://'
sys.path.insert(0,str(b));sys.path.insert(0,str(b/'tests'))
fixture=b/'tests/fixtures/sec_13f_preparation_buffers'
def denied(*a,**k):raise AssertionError('No external transport')
socket.socket.connect=denied
resource.setrlimit(resource.RLIMIT_CPU,(35,40));signal.alarm(90)
from sqlalchemy import create_engine,insert,select
from sqlalchemy.orm import Session
from app.services import direct_13f_preparation as p
from app.services.direct_feed_store import DirectFeedDocument,DirectFeedRevision
from app.db import Base
from test_direct_13f_publication import document
row=json.loads((fixture/'jpm-2026q2-metadata.json').read_bytes());row['raw']=gzip.decompress((fixture/'jpm-2026q2.txt.gz').read_bytes())
assert hashlib.sha256(row['raw']).hexdigest()==row['content_hash']
import tempfile
owned=tempfile.TemporaryDirectory(prefix='walnut-buffer-fixture-');path=Path(owned.name)/'fixture.sqlite3'
engine=create_engine('sqlite:///'+str(path));Base.metadata.create_all(engine)
with Session(engine) as db:
 for i,source in enumerate([row,document(serial=1)],1):
  m=source['metadata'];db.execute(insert(DirectFeedDocument),[dict(id=i,feed='sec_13f',source_key=m['key'],source_url=m['url'],metadata_json=json.dumps(m),content_hash=source['content_hash'],status='parsed')])
  db.execute(insert(DirectFeedRevision),[dict(id=i,document_id=i,content_hash=source['content_hash'],source_text='',source_bytes=source['raw'],parsed_json='{}')]);db.commit()
del row,source,m;gc.collect()
def rss():return next(int(x.split()[1]) for x in Path('/proc/self/status').read_text().splitlines() if x.startswith('VmRSS:'))
checks=[];baseline=rss();cpu=time.process_time();wall=time.monotonic()
with Session(engine) as db:
 def checkpoint():checks.append(rss())
 result=p.advance_preparation(db,checkpoint=checkpoint);db.commit()
 cached=db.get(p.Prepared13FParse,1);payload=cached.payload_json
 assert result['processed']==1 and result['cursor']==1
 receipt=dict(baseline_rss_kib=baseline,checkpoint_rss_kib=checks,max_checkpoint_kib=max(checks),post_parse_checkpoint_kib=checks[-1],peak_rss_kib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,cpu_seconds=time.process_time()-cpu,wall_seconds=time.monotonic()-wall,result=result,payload_sha256=hashlib.sha256(payload.encode()).hexdigest(),payload_bytes=len(payload.encode()),original_sha256='9691a80faaf95c1198a8ae84509fa49548c9f3d108203105154d23b141abdfc1',external_calls=0,production_writes=0)
engine.dispose();owned.cleanup()
assert receipt['payload_sha256']=='c86d2b74db07fda5b3c3fc7fcf95b72fa2a0f5fa9386ae4ac3728c2a85e1f65a'
assert receipt['post_parse_checkpoint_kib']-receipt['baseline_rss_kib']<128*1024, 'Completed parse retained too much memory at checkpoint'
print(json.dumps(receipt))
