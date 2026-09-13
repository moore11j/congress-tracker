"""Reviewed NVIDIA brief-to-screen campaign with immutable research provenance."""
from pathlib import Path
from sqlalchemy import text
from app.services import growth_video_store as store
from app.services.growth_video_domain import digest, now

CAMPAIGN = "nvda_ownership_research_v3"
SOURCE_ID = "rb_1789221682029_a265ac"
SOURCE_TITLE = "Who is buying NVIDIA stock in the latest 13F filings?"
TICKER_URL = "https://app.walnutmarkets.com/ticker/NVDA"
INSTITUTION_URL = "https://app.walnutmarkets.com/institution/0001462245"
LOGO = Path(__file__).parents[1] / "assets/growth/walnut-logo-v3.png"
BEATS = [
 ("v3_chart", "Who's buying NVIDIA? Here's how I'd check it in Walnut.", "Who's buying NVIDIA?", "Start with the evidence.", TICKER_URL),
 ("v3_ownership", "Open NVIDIA, then Ownership. Start with the names behind the stock.", "Who's behind the stock?", "NVIDIA / Ownership", TICKER_URL),
 ("v3_activity", "Next, check institutional activity: who added, who reduced,", "Who added? Who reduced?", "NVIDIA / Institutional activity", TICKER_URL),
 ("v3_dates", "and which reporting period you're looking at.", "Check the reporting period.", "Filing date ≠ trade date", TICKER_URL),
 ("v3_profile", "Open a manager's profile and inspect its filing history.", "Open the manager.", "Hightower Advisors / Filing history", INSTITUTION_URL),
 ("v3_history", "These are reported quarter-end holdings, not live trades.", "Quarter-end holdings.", "13F filings are delayed snapshots.", INSTITUTION_URL),
 ("v3_brief", "That's the question behind our NVIDIA research brief.", "Who is buying NVIDIA?", "From Walnut's NVIDIA research brief", None),
 ("v3_cta", "Follow the evidence in Walnut Markets.", "Follow the evidence.", "Explore NVIDIA in Walnut", None),
]

def creative():
 scenes=[]
 for i,(shot,voice,title,subhead,url) in enumerate(BEATS,1):
  scenes.append({"sequence":i,"shot":shot,"duration_seconds":max(2,round(len(voice.split())/2.7)),
   "narration":voice,"on_screen_text":title,"subhead":subhead,"walnut_url":url,
   "capture_target":shot,"capture_action":"record" if url else "none",
   "visual_type":"walnut_recording" if url else "editorial_card","transition":"cut",
   "statement_ids":["research_"+shot],"evidence_ids":["research_"+shot]})
 return {"schema_version":3,"campaign_id":CAMPAIGN,"format":"product_investigation",
  "creative_angle":"brief_to_product_evidence","hook_id":"ownership","hook":BEATS[0][1],
  "storyboard":scenes,"scenes":scenes,"narration":" ".join(s["narration"] for s in scenes),
  "target_duration_seconds":sum(s["duration_seconds"] for s in scenes),
  "source_research_brief_id":SOURCE_ID,"source_research_title":SOURCE_TITLE,
  "caption":"Who's buying NVIDIA? Follow the question through Walnut's ticker, reported holders, institutional activity and filing history. 13F filings describe quarter-end holdings, not live trades. Explore NVIDIA in Walnut Markets. Some features require a paid plan.",
  "caption_statement_ids":[],"evidence_ids":["research_"+s["shot"] for s in scenes],
  "cta":"Explore NVIDIA in Walnut Markets.","target_url":TICKER_URL,
  "voice_direction":"Natural, confident male founder. Curious and conversational, one continuous take.",
  "voice_id":"iP95p4xoKVk53GoZ742B","voice_model":"eleven_v3",
  "warnings":["Research walkthrough; not investment advice. Some features require a paid plan.",
    "13F filings are delayed quarter-end snapshots, not live trading.",
    "Ownership data QA: Hightower NVDA reported value differs across product surfaces. Conflicting dollar/share fields are excluded from footage and narration. Resolve before any value-led campaign.",
    "The source brief was approved/scheduled when selected. This ad does not publish or change its schedule."],
  "brand":{"font_stack":"ui-sans-serif, system-ui, sans-serif","windows_font":"Segoe UI",
    "background":"#020617","accent":"#6ee7b7","text":"#f1f5f9","logo":"walnut-logo-v3.png"}}

def validate(item):
 p=item['payload']; expected=creative()
 if p.get('creative')!=expected or p.get('campaign_hash')!=digest(expected):
  raise ValueError('Research ad differs from its reviewed campaign.')
 source=p.get('research_source',{})
 if source.get('id')!=SOURCE_ID or not source.get('article') or p.get('research_source_hash')!=digest(source):
  raise ValueError('Research ad source provenance changed.')
 return expected

def create_job(db,actor,platform='instagram',parent=None,feedback=''):
 source=store.research_source(db,SOURCE_ID)
 if source.get('primary_ticker')!='NVDA' or source.get('article',{}).get('title')!=SOURCE_TITLE:
  raise ValueError('The reviewed NVIDIA research brief is no longer available.')
 store.consume_budget(db,'creatives',store.config(db)['creative_limit'])
 row=db.execute(text('SELECT id FROM growth_content_opportunities WHERE source_key=:key'),{'key':CAMPAIGN}).first()
 if row:opp=store.opportunity(db,row[0])
 else:
  opp={'id':store.uid('co'),'topic':"Who's buying NVIDIA? — Walnut research walkthrough",'opportunity_type':'product_campaign',
   'tickers':['NVDA'],'score':0,'component_scores':{},'factual_data_timestamp':now(),'destination_url':TICKER_URL,
   'research_brief_id':SOURCE_ID,'campaign_id':CAMPAIGN,'reason':'Owner-directed adaptation of an approved NVIDIA research brief.',
   'search_signal':{'status':'not_scored','queries':[]},'suggested_format':'product_investigation'}
  db.execute(text('INSERT INTO growth_content_opportunities VALUES (:id,:key,0,:at,:payload)'),{'id':opp['id'],'key':CAMPAIGN,'at':now(),'payload':store.dumps(opp)});db.commit()
 board=creative()
 payload={'campaign_id':CAMPAIGN,'product_hook':'ownership','creative':board,'campaign_hash':digest(board),
  'research_source':source,'research_source_hash':digest(source),
  'model_metadata':{'provider':'reviewed_research_adaptation','version':3,'brief_version':store.brief(db)['version_id'],'created_at':now()}}
 return store.create_job(db,opp['id'],actor,platform,'product_investigation',parent=parent,feedback=feedback,reviewed_product=payload)
