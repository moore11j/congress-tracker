import os,json,hashlib,socket,argparse,sys
from pathlib import Path
from unittest.mock import patch
from datetime import datetime
os.environ.update(DATABASE_URL='sqlite:///:memory:',SEC_FUNDAMENTALS_WARMING_ENABLED='1',FUNDAMENTALS_PROVIDER='fmp')
from sqlalchemy import create_engine,select,func
from sqlalchemy.orm import sessionmaker
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from app.db import Base
from app.models import InsightsSnapshot,FundamentalsCache,Event
from app.services import sec_fundamentals_preparation as prep,sec_directory
from app.services.direct_feed_collection import validate_directory
parser=argparse.ArgumentParser(description='Replay the saved twenty-company SEC ratio source capture without network access')
parser.add_argument('--source',type=Path,required=True);parser.add_argument('--output',type=Path,required=True)
args=parser.parse_args();assert not args.output.exists()
source=args.source;manifest=json.loads((source/'sources.json').read_text())
raws={}
for name,meta in manifest['sources'].items():
 raw=(source/name).read_bytes();assert hashlib.sha256(raw).hexdigest()==meta['sha256'];raws[meta['url']]=raw
identities={r['symbol']:r for r in validate_directory(json.loads((source/'directory.json').read_bytes()))}
symbols=sorted(name[:-13] for name in manifest['sources'] if name.endswith('-company.json'))
assert len(symbols)==20
engine=create_engine('sqlite:///:memory:');Base.metadata.create_all(engine);factory=sessionmaker(bind=engine)
prep.SessionLocal=factory;sec_directory.SessionLocal=factory
reads=[]
def offline(self,url):
 assert url in raws,url;reads.append(url);return raws[url]
def no_io(*a,**k):raise AssertionError('External request')
results=[]
with patch('socket.socket.connect',no_io),patch.object(sec_directory,'directory',lambda:identities),patch.object(prep.DirectSourceClient,'get',offline):
 for symbol in symbols:
  result=prep.prepare(symbol);results.append({'symbol':symbol,'status':result['status'],'reason':result.get('reason'),'metrics':len(result['values'])})
 with factory() as db:
  before=[(r.kind,r.payload_json,str(r.fetched_at)) for r in db.scalars(select(InsightsSnapshot).order_by(InsightsSnapshot.kind))]
  assert db.scalar(select(func.count()).select_from(FundamentalsCache))==0
  assert db.scalar(select(func.count()).select_from(Event))==0
 count=len(reads)
 for symbol in symbols:prep.prepare(symbol)
 assert len(reads)==count==40
 with factory() as db:assert [(r.kind,r.payload_json,str(r.fetched_at)) for r in db.scalars(select(InsightsSnapshot).order_by(InsightsSnapshot.kind))]==before
report={'status':'passed','companies':results,'cached_observations':len(before),'repeat_identical':True,'source_manifest_sha256':hashlib.sha256((source/'sources.json').read_bytes()).hexdigest(),'external_requests':0,'canonical_writes':0,'emails':0,'limitation':'Source-bound offline replay of the October9 public SEC capture, not new live company-facts acquisition.'}
out=args.output;assert not out.exists();out.write_text(json.dumps(report,indent=2),encoding='utf-8');print(report)
