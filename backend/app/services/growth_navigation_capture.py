"""Record real clicks and wheel scrolls, retaining action markers for voice sync."""
import io
import math
import re
import subprocess
import tempfile
import time
from pathlib import Path
from urllib.parse import urlsplit
from PIL import Image, ImageFilter
from app.services.growth_navigation_ad import TICKER_URL, INSTITUTION_URL, INSIGHTS_URL, BRIEF_URL, BRIEF_PATH, SOURCE_TITLE
from app.services.growth_video_domain import now, digest

VIEWPORT={'width':1040,'height':1000}
SHOTS={'v4_search','v4_ownership','v4_activity','v4_manager','v4_filings','v4_insights','v4_brief'}


def pointer_arc(start,end,steps=8,bend=18):
 """Fast curved travel, ease out, slight overshoot, then a small correction."""
 dx,dy=end[0]-start[0],end[1]-start[1];length=max(1,math.hypot(dx,dy))
 points=[]
 for i in range(1,steps+1):
  u=i/steps;q=1-(1-u)**3
  overshoot=1.025 if i<steps else 1
  side=math.sin(math.pi*q)*bend
  points.append((start[0]+dx*q*overshoot-dy/length*side,start[1]+dy*q*overshoot+dx/length*side))
 points[-1]=tuple(end)
 return points


class Recorder:
 def __init__(self,page,root):
  self.page=page;self.root=root;self.frames=[];self.markers=[];self.events=[];self.pointer=(670,180);self.clicked=False
  self.camera=None;self.deadline=time.monotonic()+360

 def box(self,loc):
  b=loc.bounding_box()
  if not b:raise ValueError('Navigation target is missing.')
  return b

 def mark(self,phrase):
  self.markers.append({'phrase':phrase,'frame':len(self.frames)})

 def frame(self):
  if time.monotonic()>self.deadline:raise ValueError('Navigation capture exceeded its time budget.')
  # The page stays unmodified. Obscure account identity and suspect numbers in
  # captured pixels only; never replace a value with an invented number.
  masks=self.page.evaluate('''() => {
   const out=[];
   const add=e=>{
    const b=e.getBoundingClientRect();let l=b.left,t=b.top,r=b.right,d=b.bottom;
    for(let p=e.parentElement;p;p=p.parentElement){const s=getComputedStyle(p),q=p.getBoundingClientRect();
     if(/auto|scroll|hidden|clip/.test(s.overflowX)){l=Math.max(l,q.left);r=Math.min(r,q.right);}
     if(/auto|scroll|hidden|clip/.test(s.overflowY)){t=Math.max(t,q.top);d=Math.min(d,q.bottom);}
    }
    if(r>l&&d>t&&d>0&&t<1000)out.push({x:l,y:t,width:r-l,height:d-t});
   };
   document.querySelectorAll('button').forEach(e=>{if(/Hello,|Following NVDA|Follow NVDA/.test(e.textContent||''))add(e);});
   document.querySelectorAll('table').forEach(table=>{
    const label=table.getAttribute('aria-label')||'';
    const head=(table.querySelector('thead')?.textContent||'').toLowerCase();
    table.querySelectorAll('tbody tr').forEach(row=>Array.from(row.querySelectorAll('td')).forEach((td,i)=>{
     if((head.includes('holder')&&i>0)||(label==='Institutional activity'&&(i===1||i===3))||(/^\\s*[$€£]/.test(td.textContent||'')))add(td);
    }));
   });
   return out;
  }''')
  im=Image.open(io.BytesIO(self.page.screenshot(animations='disabled',timeout=20000))).convert('RGB')
  for b in masks:
   rect=(max(0,int(b['x'])),max(0,int(b['y'])),min(1040,math.ceil(b['x']+b['width'])),min(1000,math.ceil(b['y']+b['height'])))
   if rect[2]>rect[0] and rect[3]>rect[1]:im.paste(im.crop(rect).filter(ImageFilter.GaussianBlur(14)),rect)
  im.save(self.root/f'frame-{len(self.frames):03}.png')
  self.frames.append({'cursor':list(self.pointer),'click':self.clicked,'camera':self.camera or {'x':0,'y':0,**VIEWPORT},'page_url':self.page.url,'masked_regions':len(masks)})
  self.clicked=False

 def hold(self,n=6):
  for _ in range(n):self.frame()

 def move(self,loc,steps=7):
  b=self.box(loc);end=(b['x']+b['width']*.48,b['y']+b['height']*.53)
  if not 0<=end[1]<=1000:
   # Lazy-loaded content can shift a table after positioning. Keep the corrective
   # wheel gesture in the footage rather than jumping straight to its result.
   self.scroll_to(loc,top=380,steps=10)
   b=self.box(loc);end=(b['x']+b['width']*.48,b['y']+b['height']*.53)
  if not 0<=end[1]<=1000:
   geometry=loc.evaluate("e=>{let a=[];for(let p=e;p;p=p.parentElement){if(p.scrollHeight>p.clientHeight+4)a.push([p.tagName,p.clientHeight,p.scrollHeight,p.scrollTop,getComputedStyle(p).overflowY]);}return a;}")
   raise ValueError(f'Click target is outside the visible navigation: {loc.inner_text()[:80]} at {tuple(round(v) for v in end)}; scroll containers {geometry}.')
  for point in pointer_arc(self.pointer,end,steps):
   self.pointer=point;self.page.mouse.move(*point);self.frame()
  self.hold(2)

 def click(self,loc,label):
  self.move(loc)
  b=self.box(loc);expected=(b['x']+b['width']*.48,b['y']+b['height']*.53)
  if math.dist(expected,self.pointer)>3:self.move(loc,steps=5)
  self.clicked=True;self.frame()
  # Locators also wait for a target to settle after asynchronous table hydration.
  # This is a real browser click; it cannot activate an unrelated row underneath.
  loc.click(position={'x':b['width']*.48,'y':b['height']*.53},timeout=20000)
  self.events.append({'type':'click','target':label,'frame':len(self.frames)-1,'page_url':self.page.url})

 def circle(self,loc):
  # Use the words' bounds, not a block heading's full layout width.
  b=loc.evaluate("e=>{const r=document.createRange();r.selectNodeContents(e);const b=r.getBoundingClientRect();return {x:b.x,y:b.y,width:b.width,height:b.height};}")
  cx=b['x']+b['width']/2;cy=b['y']+b['height']/2
  rx=min(165,b['width']/2+13);ry=min(27,b['height']/2+10)
  start=(cx+rx,cy)
  for point in pointer_arc(self.pointer,start,5,bend=-12):
   self.pointer=point;self.page.mouse.move(*point);self.frame()
  for i in range(1,17):
   a=2*math.pi*i/16;point=(cx+rx*math.cos(a),cy+ry*math.sin(a))
   self.pointer=point;self.page.mouse.move(*point);self.frame()
  self.events.append({'type':'circle','frame':len(self.frames)-16,'target':loc.inner_text()[:100]})
  self.hold(5)

 def scroll_to(self,loc,top=150,steps=20):
  delta=self.box(loc)['y']-top
  self.events.append({'type':'wheel_scroll','frame':len(self.frames),'distance':round(delta)})
  # Keep the wheel outside nested table scrollers and chart canvases. Otherwise
  # the gesture can pan a chart or skip table rows instead of moving the page.
  self.pointer=(1018,620);self.page.mouse.move(*self.pointer)
  previous=0
  for i in range(1,steps+1):
   u=i/steps;q=3*u*u-2*u*u*u
   offset=round(delta*q);self.page.mouse.wheel(0,offset-previous);previous=offset
   self.page.wait_for_timeout(20);self.frame()
  self.hold(4)
  remaining=self.box(loc)['y']-top
  if abs(remaining)>100:
   for i in range(8):
    self.page.mouse.wheel(0,round(remaining/8));self.page.wait_for_timeout(30);self.frame()
   self.hold(3)
  landed=self.box(loc)
  # At the bottom of a document the browser cannot align a short final section
  # at the top. A visible heading/link remains a valid natural scroll endpoint.
  lower=450 if landed['height']>600 else 870
  if not 0<=landed['y']<=lower:
   raise ValueError(f"Scroll did not reveal {loc.inner_text()[:70]} (y={round(landed['y'])}).")


def capture_navigation_shot(shot,*,owner_id,session_token=None):
 from playwright.sync_api import sync_playwright
 import imageio_ffmpeg
 if shot not in SHOTS or owner_id is None:raise ValueError('Authorized navigation capture required.')
 if session_token is None:
  from app.auth import sign_session_payload
  session_token=sign_session_payload({'uid':owner_id,'exp':int(time.time())+900})
 initial=INSIGHTS_URL if shot in {'v4_search','v4_brief'} else INSTITUTION_URL if shot in {'v4_filings','v4_insights'} else TICKER_URL
 with tempfile.TemporaryDirectory(prefix='walnut-navigation-') as folder,sync_playwright() as pw:
  root=Path(folder);browser=pw.chromium.launch(headless=True)
  context=browser.new_context(viewport=VIEWPORT,device_scale_factor=1,color_scheme='dark',locale='en-US',timezone_id='UTC')
  context.add_cookies([{'name':'ct_session','value':session_token,'domain':host,'path':'/','secure':True,'httpOnly':True,'sameSite':'None','expires':time.time()+900} for host in ['app.walnutmarkets.com','walnutmarkets.com','api.walnutmarkets.com','congress-tracker-api.fly.dev']])
  context.add_cookies([{'name':name,'value':value,'domain':host,'path':'/','secure':True} for host in ['app.walnutmarkets.com','walnutmarkets.com'] for name,value in [('ct_auth_hint','1'),('walnut_privacy_consent','v1.a0.m0')]])
  page=context.new_page();search_requests=[]
  page.on('response',lambda response:search_requests.append({'path':urlsplit(response.url).path,'status':response.status}) if 'search' in urlsplit(response.url).path else None)
  allowed_paths={urlsplit(u).path for u in [TICKER_URL,INSTITUTION_URL,INSIGHTS_URL,BRIEF_URL]}
  def guard(route):
   p=urlsplit(route.request.url)
   if p.scheme in {'http','https'}:
    if p.hostname not in {'app.walnutmarkets.com','walnutmarkets.com','api.walnutmarkets.com','congress-tracker-api.fly.dev','fonts.googleapis.com','fonts.gstatic.com'}:return route.abort()
    if route.request.method not in {'GET','HEAD','OPTIONS'}:return route.abort()
    if p.path.startswith('/api/') and not (p.path in {'/api/auth/me','/api/entitlements','/api/events'} or p.path.startswith(('/api/tickers/NVDA','/api/ticker/NVDA','/api/tickers/SPY','/api/institutions/0001462245','/api/search/','/api/research/','/api/insights/','/api/market','/api/macro'))):return route.abort()
   if route.request.is_navigation_request() and route.request.frame==page.main_frame and p.path not in allowed_paths:return route.abort()
   route.continue_()
  page.route('**/*',guard)
  response=page.goto(initial,wait_until='domcontentloaded',timeout=60000)
  if not response or response.status!=200:raise ValueError('Navigation source is unavailable.')
  page.get_by_role('combobox',name='Global search').wait_for(timeout=60000)
  page.wait_for_timeout(2200)
  r=Recorder(page,root)
  holders=page.get_by_role('heading',name='Institutional Holders',exact=True)
  activity=page.locator('#institutional-activity')
  manager=page.get_by_role('link',name='Hightower Advisors, LLC',exact=True)
  history=page.get_by_role('heading',name='Filing History',exact=True)
  briefs=page.locator('#research-briefs')
  source_text=[]
  def ready_holders():
   page.get_by_role('button',name='Ownership',exact=True).click()
   holders.wait_for(timeout=60000)
   page.get_by_text('BlackRock, Inc.',exact=True).wait_for(timeout=60000)
   page.wait_for_timeout(700)
  def position(loc,top=150):
   # This is the unrecorded starting position for a scene. Site CSS enables
   # smooth scrolling, so numeric scrollBy can leave a click target in transit.
   loc.evaluate("(e,top)=>window.scrollTo({top:window.scrollY+e.getBoundingClientRect().top-top,behavior:'instant'})",top)
   page.wait_for_timeout(350)
  def ready_briefs():
   briefs.locator('a[href$="'+BRIEF_PATH+'"]').wait_for(timeout=60000)
  if shot=='v4_search':
   r.hold(8);r.mark('Search')
   search=page.get_by_role('combobox',name='Global search');r.click(search,'Global search')
   for char in 'NVDA':search.press_sequentially(char);r.hold(2)
   result=page.get_by_role('option',name=re.compile('^NVIDIA Corporation'))
   try:result.wait_for(timeout=20000)
   except Exception:
    visible=[x for x in page.get_by_role('button').all_text_contents() if 'Hello,' not in x]
    raise ValueError('NVIDIA search result missing. Input='+repr(search.input_value())+' requests='+repr(search_requests)+' Search UI: '+repr(visible[:18])) from None
   r.hold(5);r.click(result,'NVIDIA search result')
   page.get_by_role('heading',level=1).filter(has_text='NVDA').wait_for(timeout=60000)
   page.wait_for_timeout(1200);r.hold(12)
  elif shot=='v4_ownership':
   page.get_by_role('button',name='Ownership',exact=True).wait_for(timeout=60000)
   r.mark('Click Ownership');r.click(page.get_by_role('button',name='Ownership',exact=True),'Ownership')
   holders.wait_for(timeout=60000);page.get_by_text('BlackRock, Inc.',exact=True).wait_for(timeout=60000)
   page.wait_for_timeout(600);r.hold(5);r.mark('scroll');r.scroll_to(holders);r.circle(holders)
   source_text.append(holders.locator('xpath=ancestor::section[1]').inner_text())
  elif shot=='v4_activity':
   ready_holders();position(holders);manager.wait_for(timeout=60000)
   r.mark('scroll down');r.scroll_to(activity,steps=30)
   r.circle(activity.get_by_role('heading',name='Institutional activity',exact=True))
   source_text.append(activity.inner_text())
  elif shot=='v4_manager':
   manager.wait_for(timeout=60000);page.wait_for_timeout(1600)
   position(activity)
   manager.scroll_into_view_if_needed(timeout=30000);page.wait_for_timeout(500)
   r.mark('Open a manager');r.click(manager,"Hightower Advisors, LLC")
   page.get_by_role('heading',level=1).filter(has_text='Hightower').wait_for(timeout=60000)
   history.wait_for(timeout=60000);page.get_by_text('13F-HR/A',exact=True).wait_for(timeout=60000)
   page.wait_for_timeout(1300);r.hold(7);r.mark('scroll');r.scroll_to(history,steps=26);r.hold(8)
   source_text.append(history.locator('xpath=ancestor::section[1]').inner_text())
  elif shot=='v4_filings':
   history.wait_for(timeout=60000);page.get_by_text('13F-HR/A',exact=True).wait_for(timeout=60000)
   page.wait_for_timeout(1300);position(history)
   r.hold(8);r.circle(page.get_by_text('13F-HR/A',exact=True));r.hold(20)
   source_text.append(history.locator('xpath=ancestor::section[1]').inner_text())
  elif shot=='v4_insights':
   r.hold(5);r.mark('click Insights');r.click(page.get_by_role('link',name='Insights',exact=True),'Insights')
   ready_briefs();page.wait_for_timeout(1100);r.hold(6);r.mark('scroll');r.scroll_to(briefs,steps=26)
   r.circle(briefs.get_by_role('heading',name=re.compile('^Research Briefs$',re.I)))
  else:
   ready_briefs();position(briefs);r.hold(5);r.mark('Open the NVIDIA brief')
   link=briefs.locator('a[href$="'+BRIEF_PATH+'"]');r.click(link,'Published NVIDIA research brief')
   heading=page.get_by_role('heading',level=1,name=SOURCE_TITLE,exact=True);heading.wait_for(timeout=60000)
   page.wait_for_timeout(500)
   b=r.box(heading);r.camera={'x':0,'y':max(0,b['y']-60),'width':1040,'height':min(1000-b['y']+60,b['height']+170)}
   r.hold(20);source_text.append(heading.inner_text())
  r.hold(6)
  final_focus=None
  if shot in {'v4_manager','v4_filings'}:
   b=r.box(history.locator('xpath=ancestor::section[1]'))
   y=max(0,b['y']-35)
   final_focus={'x':max(0,b['x']-8),'y':y,'width':min(760,1040-b['x']),'height':min(1000-y,b['height']+60)}
  end_url=page.url;thumb=(root/'frame-000.png').read_bytes()
  context.close();browser.close();output=root/'capture.mp4'
  subprocess.run([imageio_ffmpeg.get_ffmpeg_exe(),'-y','-v','error','-framerate','12','-i',str(root/'frame-%03d.png'),'-c:v','libx264','-crf','16','-preset','veryfast','-pix_fmt','yuv420p',str(output)],check=True,capture_output=True,timeout=120)
  source='\n'.join(source_text)
  return output.read_bytes(),thumb,{'page_url':end_url,'initial_url':initial,'component':shot,'captured_at':now(),
   'viewport':VIEWPORT,'crop':{'x':0,'y':0,**VIEWPORT},'focus_panels':[{'x':0,'y':0,**VIEWPORT}],
   'media_type':'video/mp4','trim_start':0,'frame_rate':12,'clip_duration':len(r.frames)/12,
   'capture_method':'actual_browser_clicks_and_wheel_scrolls','frames':r.frames,'action_markers':r.markers,'navigation_events':r.events,'final_focus':final_focus,
   'source_text':source[:16000],'source_hash':digest(source),'authorized_product_demo':True,'public_context':False,
   'focus_note':'Real navigation. Account identity and suspect numeric fields obscured in captured pixels. Loading waits shortened; narration-matched action markers retained.'}
