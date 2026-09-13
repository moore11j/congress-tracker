"""V4: a reproducible click-by-click journey, with reviewed social copy."""
from sqlalchemy import text
from app.services import growth_video_store as store
from app.services.growth_video_domain import digest, now
from app.services.growth_research_ad import LOGO, TICKER_URL, INSTITUTION_URL

CAMPAIGN = 'nvda_navigation_v4'
SOURCE_ID = 'rb_1789151207553_89bc04'
SOURCE_TITLE = 'Are institutions still buying NVIDIA stock after Q2 2026 filings?'
INSIGHTS_URL = 'https://app.walnutmarkets.com/insights'
BRIEF_PATH = '/research/are-institutions-still-buying-nvidia-stock-after-q2-2026-filings'
BRIEF_URL = 'https://walnutmarkets.com' + BRIEF_PATH
TAGLINE = "Don't follow a signal. Follow the evidence."
BEATS = [
 ('v4_search', "Who's buying NVIDIA? Search NVDA in Walnut.", "Who's buying NVIDIA?", 'Search NVDA', TICKER_URL),
 ('v4_ownership', 'Click Ownership, then scroll to Institutional Holders.', 'Find the holders.', 'NVDA → Ownership → Institutional Holders', TICKER_URL),
 ('v4_activity', 'Next, scroll down to Institutional activity.', 'Follow the activity.', 'Scroll down → Institutional activity', TICKER_URL),
 ('v4_manager', "Open a manager's name, then scroll to Filing History.", 'Open the source.', 'Manager name → Filing History', INSTITUTION_URL),
 ('v4_filings', 'These are quarter-end holdings, not live trades.', 'Check the filing date.', '13F holdings are delayed snapshots', INSTITUTION_URL),
 ('v4_insights', 'For research, click Insights, then scroll to Research Briefs.', 'Find the research.', 'Insights → Research Briefs', INSIGHTS_URL),
 ('v4_brief', 'Open the NVIDIA brief to read the analysis.', 'Open the NVIDIA brief.', 'Insights → Research Briefs → Read brief', BRIEF_URL),
 ('v4_cta', "Don't follow a signal. Follow the evidence. NVIDIA links in the comments.", 'Follow the evidence.', 'Walnut Markets', None),
]


def _legacy_creative():
 scenes=[]
 for i,(shot,voice,title,subhead,url) in enumerate(BEATS,1):
  scenes.append({'sequence':i,'shot':shot,'narration':voice,'on_screen_text':title,'subhead':subhead,
   'duration_seconds':max(3,round(len(voice.split())/2.5)), 'walnut_url':url,
   'capture_target':shot,'capture_action':'record' if url else 'none',
   'visual_type':'walnut_recording' if url else 'brand_card','transition':'cut',
   'statement_ids':['navigation_'+shot],'evidence_ids':['navigation_'+shot]})
 return {'schema_version':4,'campaign_id':CAMPAIGN,'format':'product_investigation','creative_angle':'follow_the_actual_navigation',
  'hook_id':'navigation','hook':BEATS[0][1],'alternate_hooks':[],'storyboard':scenes,'scenes':scenes,
  'narration':' '.join(s['narration'] for s in scenes),'target_duration_seconds':sum(s['duration_seconds'] for s in scenes),
  'source_research_brief_id':SOURCE_ID,'source_research_title':SOURCE_TITLE,
  'caption':"Who's buying NVIDIA? Here's where to look in Walnut: search NVDA, open Ownership, check Institutional activity, then inspect a manager's filings. Find the written research under Insights → Research Briefs. Save this walkthrough for your next research session. #NVIDIA #NVDA #StockResearch #WalnutMarkets",
  'first_comment':f"Follow along in Walnut:\nNVIDIA ticker: {TICKER_URL}\nResearch briefs: {INSIGHTS_URL}#research-briefs\nNVIDIA brief: {BRIEF_URL}\n\nWhich stock should we walk through next?\nResearch only, not investment advice. Institutional tools shown require a paid plan. 13F filings report delayed quarter-end holdings.",
  'posting_notes':'Draft only. Review the caption and first comment together before posting; add the comment with the video. URLs are written out without promising they are clickable. No automatic posting or pinning.',
  'caption_statement_ids':[],'evidence_ids':['navigation_'+s['shot'] for s in scenes],
  'cta':'NVIDIA links in the comments.','target_url':TICKER_URL,'brand_tagline':TAGLINE,'brand_tagline_source':'https://walnutmarkets.com/',
  'voice_direction':'Natural, confident male founder. Conversational, continuous narration, timed to visible clicks and scrolls.',
  'voice_id':'iP95p4xoKVk53GoZ742B','voice_model':'eleven_v3',
  'warnings':['Navigation demonstration, not a conclusion that institutions are buying NVDA.',
   'Some numeric fields are obscured because cross-surface institutional values and brief counts conflict. No figures are rewritten or narrated.',
   'Published brief is shown as a navigation destination; its inconsistent aggregate counts are not endorsed by this video.',
   'Capture load waits may be shortened in the edit. Navigation events remain in order and are synchronized to narration.'],
  'brand':{'font_stack':'ui-sans-serif, system-ui, sans-serif','windows_font':'Segoe UI','background':'#020617','accent':'#6ee7b7','text':'#f1f5f9','logo':LOGO.name}}


def creative(revision=2):
 """Version social copy so existing reviewed drafts remain verifiable."""
 board=_legacy_creative()
 if revision==1:return board
 if revision!=2:raise ValueError('Unknown navigation campaign revision.')
 board['campaign_revision']=2
 board['storyboard'][-1]['narration']="Don't follow a signal. Follow the evidence. NVIDIA links below."
 board['storyboard'][-1]['duration_seconds']=max(3,round(len(board['storyboard'][-1]['narration'].split())/2.5))
 board['narration']=' '.join(scene['narration'] for scene in board['storyboard'])
 board['target_duration_seconds']=sum(scene['duration_seconds'] for scene in board['storyboard'])
 board['cta']='NVIDIA links below.'
 board['caption']=("Who's buying NVIDIA? Follow the steps in Walnut: search NVDA, open Ownership, check Institutional activity, then inspect a manager's filings. Find the written research under Insights → Research Briefs.\n\n"
  f"NVIDIA links below:\nNVIDIA ticker: {TICKER_URL}\nNVIDIA research brief: {BRIEF_URL}\nMore research: {INSIGHTS_URL}#research-briefs\n\n"
  "Save this walkthrough for your next research session. Research only, not investment advice. Institutional tools shown require a paid plan. 13F filings report delayed quarter-end holdings.\n\n#NVIDIA #NVDA #StockResearch #WalnutMarkets")
 board['first_comment']='Which stock should we walk through next?'
 board['posting_notes']='Links are included in the caption. Review the video and caption before publishing. The first comment is optional; no posting or pinning is required. URLs are written out without promising they are clickable.'
 return board


def validate(item):
 p=item['payload'];expected=creative(p.get('creative',{}).get('campaign_revision',1));source=p.get('research_source',{})
 if p.get('creative')!=expected or p.get('campaign_hash')!=digest(expected):
  raise ValueError('Navigation ad differs from its reviewed campaign.')
 if source.get('id')!=SOURCE_ID or source.get('status')!='published' or p.get('research_source_hash')!=digest(source):
  raise ValueError('Published navigation source provenance changed.')
 return expected


def create_job(db,actor,platform='instagram',parent=None,feedback=''):
 if platform not in {'instagram','tiktok'}:raise ValueError('Unsupported video platform.')
 source=store.research_source(db,SOURCE_ID)
 if source.get('status')!='published' or source.get('article',{}).get('title')!=SOURCE_TITLE:
  raise ValueError('The reviewed published NVIDIA brief is unavailable.')
 store.consume_budget(db,'creatives',store.config(db)['creative_limit'])
 row=db.execute(text('SELECT id FROM growth_content_opportunities WHERE source_key=:key'),{'key':CAMPAIGN}).first()
 if row:opp=store.opportunity(db,row[0])
 else:
  opp={'id':store.uid('co'),'topic':"Who's buying NVIDIA? — Follow the clicks",'opportunity_type':'product_campaign','tickers':['NVDA'],
   'score':0,'component_scores':{},'factual_data_timestamp':now(),'destination_url':TICKER_URL,'research_brief_id':SOURCE_ID,
   'campaign_id':CAMPAIGN,'reason':'Owner-directed navigation demo with published research destination.',
   'search_signal':{'status':'not_scored','queries':[]},'suggested_format':'product_investigation'}
  db.execute(text('INSERT INTO growth_content_opportunities VALUES (:id,:key,0,:at,:payload)'),{'id':opp['id'],'key':CAMPAIGN,'at':now(),'payload':store.dumps(opp)});db.commit()
 board=creative()
 payload={'campaign_id':CAMPAIGN,'product_hook':'navigation','creative':board,'campaign_hash':digest(board),
  'research_source':source,'research_source_hash':digest(source),
  'model_metadata':{'provider':'reviewed_navigation_campaign','version':4,'brief_version':store.brief(db)['version_id'],'created_at':now()}}
 return store.create_job(db,opp['id'],actor,platform,'product_investigation',parent=parent,feedback=feedback,reviewed_product=payload)
