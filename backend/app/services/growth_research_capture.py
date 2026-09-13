"""Real, focused browser recordings for the reviewed NVIDIA research narrative.

Focus windows omit inconsistent dollar/share fields discovered during source QA.
The names, actions and dates are original browser pixels, never rewritten data.
"""
import math
import re
import subprocess
import tempfile
import time
from pathlib import Path
from urllib.parse import urlsplit
from app.services.growth_research_ad import BEATS, TICKER_URL, INSTITUTION_URL
from app.services.growth_video_domain import now, digest

VIEWPORT={"width":1040,"height":1100}

def capture_research_shot(shot, *, owner_id, session_token=None):
 from playwright.sync_api import sync_playwright
 import imageio_ffmpeg
 urls={s[0]:s[4] for s in BEATS if s[4]}
 if shot not in urls or owner_id is None:raise ValueError('Authorized research capture required.')
 if session_token is None:
  from app.auth import sign_session_payload
  session_token=sign_session_payload({'uid':owner_id,'exp':int(time.time())+900})
 url=urls[shot]
 with tempfile.TemporaryDirectory(prefix='walnut-research-') as folder,sync_playwright() as pw:
  root=Path(folder);browser=pw.chromium.launch(headless=True)
  context=browser.new_context(viewport=VIEWPORT,device_scale_factor=1,color_scheme='dark',locale='en-US',timezone_id='UTC')
  context.add_cookies([{'name':'ct_session','value':session_token,'domain':h,'path':'/','secure':True,'httpOnly':True,'sameSite':'None','expires':time.time()+900} for h in ['app.walnutmarkets.com','api.walnutmarkets.com','congress-tracker-api.fly.dev']])
  context.add_cookies([{'name':'ct_auth_hint','value':'1','domain':'app.walnutmarkets.com','path':'/','secure':True},
   {'name':'walnut_privacy_consent','value':'v1.a0.m0','domain':'app.walnutmarkets.com','path':'/','secure':True}])
  page=context.new_page()
  def guard(route):
   p=urlsplit(route.request.url)
   if p.scheme in {'http','https'}:
    if p.hostname not in {'app.walnutmarkets.com','api.walnutmarkets.com','congress-tracker-api.fly.dev','fonts.googleapis.com','fonts.gstatic.com'}:return route.abort()
    if route.request.method not in {'GET','HEAD','OPTIONS'}:return route.abort()
    if p.path.startswith('/api/') and not (p.path in {'/api/auth/me','/api/entitlements'} or p.path.startswith(('/api/tickers/NVDA','/api/tickers/SPY','/api/institutions/0001462245'))):return route.abort()
   if route.request.is_navigation_request() and route.request.frame==page.main_frame and route.request.url!=url:return route.abort()
   route.continue_()
  page.route('**/*',guard)
  response=page.goto(url,wait_until='domcontentloaded',timeout=60000)
  if not response or response.status!=200:raise ValueError('Research page unavailable.')
  page.get_by_role('heading',level=1).wait_for(timeout=60000)
  if shot=='v3_chart':
   target=page.get_by_role('heading',name='NVDA vs S&P 500 (SPY)',exact=True).locator('xpath=ancestor::section[1]')
   target.evaluate("e=>e.scrollIntoView({block:'center'})")
   target.locator('canvas').first.wait_for(state='visible',timeout=45000)
  elif shot=='v3_ownership':
   page.get_by_role('button',name='Ownership',exact=True).click()
   target=page.get_by_role('heading',name='Institutional Holders',exact=True).locator('xpath=ancestor::section[1]')
   target.get_by_text('BlackRock, Inc.',exact=True).wait_for(timeout=60000)
  elif shot in {'v3_activity','v3_dates'}:
   target=page.locator('#institutional-activity')
   target.get_by_role('link',name='Hightower Advisors, LLC',exact=True).wait_for(timeout=60000)
  elif shot=='v3_profile':
   target=page.get_by_role('heading',level=1)
  else:
   target=page.get_by_role('heading',name='Filing History',exact=True).locator('xpath=ancestor::section[1]')
   target.get_by_text('13F-HR/A',exact=True).wait_for(timeout=60000)
  target.evaluate("e=>e.scrollIntoView({block:'start'})")
  page.evaluate('window.scrollBy(0,-100)')
  page.evaluate('Promise.race([document.fonts.ready,new Promise(r=>setTimeout(r,3000))])')
  page.wait_for_timeout(1500)
  source=target.inner_text()
  if re.search(r'Unlock with|Sign in to unlock',source,re.I):raise ValueError('Research capture is gated.')
  def box(locator):
   b=locator.bounding_box()
   if not b:raise ValueError('Missing research focus window.')
   return b
  b=box(target)
  panels=[]
  if shot=='v3_ownership':
   table=target.locator('table'); first=box(table.locator('tbody tr').first.locator('td').first)
   last=box(table.locator('tbody tr').nth(5).locator('td').first)
   # A name-column close-up, not a numerical ranking or investment claim.
   panels=[{'x':first['x'],'y':max(0,b['y']),'width':first['width'],'height':min(780,last['y']+last['height']-b['y']+45)}]
  elif shot in {'v3_activity','v3_dates'}:
   table=target.locator('table'); rows=table.locator('tbody tr')
   left=box(rows.first.locator('td').first);right=box(rows.first.locator('td').last)
   second=box(rows.nth(1).locator('td').first)
   top=box(table.locator('thead'))['y'];height=second['y']+second['height']-top+8
   panels=[{'x':left['x'],'y':top,'width':left['width'],'height':height}]
   if shot=='v3_activity':panels.append({'x':right['x'],'y':top,'width':right['width'],'height':height})
  elif shot=='v3_profile':
   panels=[{'x':b['x'],'y':b['y'],'width':min(b['width'],950),'height':118}]
  else:
   panels=[{'x':b['x'],'y':b['y'],'width':b['width'],'height':min(b['height'],760)}]
  panels=[{k:int(v) for k,v in p.items()} for p in panels]
  for p in panels:
   p['x']=max(0,p['x']);p['y']=max(0,p['y']);p['width']=min(p['width'],1040-p['x']);p['height']=min(p['height'],1100-p['y']-20)
   if min(p['width'],p['height'])<50:raise ValueError('Research focus does not fit viewport.')
  thumb=page.screenshot(animations='disabled')
  deadline=time.monotonic()+240
  for frame in range(72):
   if time.monotonic()>deadline:raise ValueError('Research recording exceeded time budget.')
   if shot=='v3_chart':
    c=box(target.locator('canvas').first);page.mouse.move(c['x']+c['width']*(.15+.7*frame/72),c['y']+c['height']*.4)
   else:
    p=panels[0];page.mouse.move(p['x']+p['width']*.6,p['y']+min(p['height']*.75,55+frame*1.8))
    if shot=='v3_ownership' and frame>15 and frame<45:page.mouse.wheel(0,1)
   page.screenshot(path=str(root/f'frame-{frame:03}.png'),animations='disabled',timeout=20000)
  context.close();browser.close()
  output=root/'capture.mp4'
  subprocess.run([imageio_ffmpeg.get_ffmpeg_exe(),'-y','-v','error','-framerate','12','-i',str(root/'frame-%03d.png'),'-c:v','libx264','-crf','16','-preset','veryfast','-pix_fmt','yuv420p',str(output)],check=True,capture_output=True,timeout=120)
  return output.read_bytes(),thumb,{'page_url':url,'page_title':'NVIDIA' if url==TICKER_URL else 'Hightower Advisors, LLC',
   'component':shot,'captured_at':now(),'viewport':VIEWPORT,'crop':{'x':0,'y':0,**VIEWPORT},'focus_panels':panels,
   'media_type':'video/mp4','trim_start':0,'frame_rate':12,'clip_duration':6,'capture_method':'real_browser_frames',
   'source_text':source[:16000],'source_hash':digest(source),'authorized_product_demo':True,'public_context':False,
   'focus_note':'Original browser pixels magnified. Dollar/share fields excluded due to source QA; no values rewritten.'}
