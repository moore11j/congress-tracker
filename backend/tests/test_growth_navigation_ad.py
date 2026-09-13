import copy
import math
import pytest
from test_growth_video import db, Storage
from test_growth_product_ad import aligned
from app.models import UserAccount
from app.services import growth_navigation_ad as nav, growth_product_ad as product, growth_video_store as store, growth_video_pipeline as pipeline, research_briefs
from app.services.growth_navigation_capture import pointer_arc
from app.services.growth_navigation_render import action_knots, source_frame_at
from app.routers import growth_video as api


def seed(db):
 research_briefs.ensure_research_brief_store_schema(db)
 source={'id':nav.SOURCE_ID,'status':'published','primary_ticker':'NVDA','created_at':store.now(),
  'article':{'title':nav.SOURCE_TITLE,'slug':nav.BRIEF_PATH.split('/')[-1]}}
 research_briefs._upsert_db_draft(db,source);db.commit()


def test_navigation_preserves_published_source_and_review_gate(db):
 seed(db);item=product.create_job(db,1,hook='navigation');calls=[]
 class Narrator:
  def generate(self,script,voice,model,*,continuous):
   calls.append(script);assert continuous;return b'audio',aligned(script)
 def capture(shot):return b'footage',b'png',{'component':shot,'media_type':'video/mp4'}
 def render(board,captures,audio,read):
  assert len(captures)==7 and board['caption'].count('https://')==3
  return b'0000ftypvideo',{'duration':33,'width':1080,'height':1920}
 item=api.decision(item['id'],api.Decision(action='render'),db.get(UserAccount,1),db)
 for _ in range(11):status=pipeline.advance(db,item['id'],storage=Storage(),capture=capture,narrator=Narrator(),renderer=render)
 assert status=='READY_FOR_REVIEW' and len(calls)==1
 saved=store.job(db,item['id']);assert saved['payload']['research_source']['status']=='published'
 tampered=copy.deepcopy(saved);tampered['payload']['creative']['first_comment']='Changed link'
 with pytest.raises(ValueError,match='reviewed campaign'):product.validate(tampered)


def test_navigation_actions_follow_spoken_phrase_timing():
 script='Click Ownership, then scroll to Institutional Holders.'
 scene={'narration':script,'start':0,'end':6}
 audio={'alignment':{'character_start_times_seconds':[i*.1 for i in range(len(script))]}}
 asset={'frames':[{} for _ in range(72)],'action_markers':[{'phrase':'Click Ownership','frame':0},{'phrase':'scroll','frame':18}]}
 knots=action_knots(scene,asset,audio,script)
 spoken_scroll=script.index('scroll')*.1
 assert source_frame_at(knots,spoken_scroll)==18
 frames=[source_frame_at(knots,i/24) for i in range(145)]
 assert all(a<=b for a,b in zip(frames,frames[1:])) and frames[-1]==71
 asset['action_markers'][0]['frame']=5
 assert source_frame_at(action_knots(scene,asset,audio,script),0)==5
 asset['action_markers'][1]['phrase']='unspoken action'
 with pytest.raises(ValueError,match='matching narration'):action_knots(scene,asset,audio,script)


def test_pointer_arrives_quickly_curves_and_settles():
 points=pointer_arc((0,0),(400,100));assert points[-1]==(400,100)
 distances=[math.dist(a,b) for a,b in zip([(0,0),*points],points)]
 assert distances[0]>distances[-1]*5
 assert any(abs(y-x*.25)>1 for x,y in points[:-1])


def test_intro_words_hold_the_first_frame_until_the_scroll_instruction():
 script='Next, scroll down.'
 scene={'narration':script,'start':0,'end':3}
 audio={'alignment':{'character_start_times_seconds':[i*.1 for i in range(len(script))]}}
 asset={'frames':[{} for _ in range(40)],'action_markers':[{'phrase':'scroll down','frame':0}]}
 knots=action_knots(scene,asset,audio,script)
 assert source_frame_at(knots,.3)==0 and source_frame_at(knots,.7)>0


def test_social_copy_and_brand_are_reviewable():
 board=nav.creative()
 assert board['brand_tagline']=="Don't follow a signal. Follow the evidence."
 assert 'Your research starts here' not in str(board)
 assert 'clickable' not in board['cta'].lower()
 assert '#NVDA' in board['caption'] and nav.TICKER_URL in board['caption']
 assert nav.BRIEF_URL in board['caption'] and 'NVIDIA links below.' in board['narration']
 assert 'links in the comments' not in str(board)
 assert len(board['caption'])<=2200
 assert '$' not in board['narration'] and '%' not in board['narration']


def test_existing_navigation_drafts_remain_valid_after_copy_revision(db):
 seed(db)
 item=product.create_job(db,1,hook='navigation')
 legacy=nav.creative(1)
 item['payload']['creative']=legacy
 item['payload']['campaign_hash']=nav.digest(legacy)
 assert product.validate(item)==legacy
 item['payload']['creative']['cta']='Tampered'
 with pytest.raises(ValueError,match='reviewed campaign'):product.validate(item)
