import copy
import pytest
from test_growth_video import db, Storage
from test_growth_product_ad import aligned
from app.models import UserAccount
from app.services import growth_research_ad as research, growth_product_ad as product, growth_video_store as store, growth_video_pipeline as pipeline, research_briefs
from app.routers import growth_video as api

def seed_source(db):
 research_briefs.ensure_research_brief_store_schema(db)
 source={'id':research.SOURCE_ID,'status':'approved_scheduled','primary_ticker':'NVDA','created_at':store.now(),
  'article':{'title':research.SOURCE_TITLE,'slug':'who-is-buying-nvidia-stock-in-the-latest-13f-filings'}}
 research_briefs._upsert_db_draft(db,source);db.commit()
 return source

def test_research_campaign_retains_approved_source_and_rejects_tampering(db):
 seed_source(db);item=product.create_job(db,1,hook='ownership')
 assert item['status']=='CREATIVE_READY'
 assert product.validate(item)['source_research_brief_id']==research.SOURCE_ID
 item['payload']['research_source']['article']['title']='Unrelated research'
 with pytest.raises(ValueError,match='provenance'):product.validate(item)

def test_research_capture_and_one_voice_reach_manual_review(db):
 seed_source(db);item=product.create_job(db,1,hook='ownership');storage=Storage();calls=[]
 class Narrator:
  def generate(self,script,voice,model,*,continuous):
   calls.append(script);assert continuous
   return b'audio',aligned(script)
 def capture(shot):return b'footage',b'png',{'component':shot,'media_type':'video/mp4'}
 def render(board,captures,audio,read):
  assert len(captures)==6
  return b'0000ftypvideo',{'duration':31.45,'width':1080,'height':1920}
 item=api.decision(item['id'],api.Decision(action='render'),db.get(UserAccount,1),db)
 for _ in range(10):status=pipeline.advance(db,item['id'],storage=storage,capture=capture,narrator=Narrator(),renderer=render)
 assert status=='READY_FOR_REVIEW' and len(calls)==1
 assert store.job(db,item['id'])['payload']['research_source']['article']['title']==research.SOURCE_TITLE

def test_research_ad_does_not_assert_unverified_amounts():
 board=research.creative()
 assert '$' not in board['narration'] and '%' not in board['narration']
 assert 'quarter-end holdings, not live trades' in board['narration']
 assert board['brand']['accent']=='#6ee7b7'
 assert board['storyboard'][0]['walnut_url']==research.TICKER_URL
