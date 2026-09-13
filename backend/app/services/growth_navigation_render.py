"""Action-aligned navigation footage with restrained Walnut branding."""
import bisect
import math
import subprocess
import tempfile
import time
from pathlib import Path
from PIL import Image, ImageDraw
from app.services.growth_research_render import brand_font, BG, MINT, WHITE, MUTED
from app.services.growth_native_render import timeline, centered
from app.services.growth_navigation_ad import LOGO

W,H,FPS=1080,1920,24


def action_knots(scene,asset,audio,script):
 """Map actual recorded actions to their spoken phrases, in the same order."""
 alignment=audio['alignment'];start=script.index(scene['narration'])
 duration=scene['end']-scene['start'];count=len(asset['frames'])
 knots=[(0.,0.)]
 for marker in asset['action_markers']:
  index=scene['narration'].find(marker['phrase'])
  if index<0:raise ValueError('A recorded action has no matching narration.')
  t=max(0,alignment['character_start_times_seconds'][start+index]-scene['start'])
  frame=marker['frame']
  if not 0<=frame<count:raise ValueError('Action marker exceeds footage.')
  if t==0 and frame==0:continue
  if t<=knots[-1][0] or frame<=knots[-1][1]:raise ValueError('Action markers are out of order.')
  knots.append((t,float(frame)))
 if knots[-1][0]>=duration or knots[-1][1]>=count-1:raise ValueError('No room for action completion.')
 knots.append((duration,float(count-1)))
 return knots


def source_frame_at(knots,t):
 i=max(0,min(len(knots)-2,bisect.bisect_right([k[0] for k in knots],t)-1))
 a,b=knots[i],knots[i+1];u=max(0,min(1,(t-a[0])/(b[0]-a[0])))
 return a[1]+(b[1]-a[1])*u


def render_navigation_video(creative,captures,audio,read_asset,*,frame_observer=None):
 import imageio_ffmpeg
 ffmpeg=imageio_ffmpeg.get_ffmpeg_exe();scenes,captions,duration=timeline(creative,audio)
 expected={s['shot'] for s in scenes if s['walnut_url']}
 if not expected.issubset(captures):raise ValueError('Missing navigation footage.')
 base=Image.new('RGB',(W,H),BG);logo=Image.open(LOGO).convert('RGB')
 base.paste(logo.resize((100,100),Image.Resampling.LANCZOS),(64,125));d=ImageDraw.Draw(base)
 d.text((184,144),'Walnut',font=brand_font(43,True),fill=WHITE)
 d.text((186,195),'Market Terminal',font=brand_font(22),fill=MINT)
 d.line((64,255,1016,255),fill='#1e293b',width=1)
 centered(d,'Research only · Not investment advice · Paid features shown',1710,brand_font(18),MUTED)
 centered(d,'walnutmarkets.com',1760,brand_font(25),MINT)
 with tempfile.TemporaryDirectory(prefix='walnut-nav-render-') as folder:
  root=Path(folder);voice=root/'voice.mp3';voice.write_bytes(read_asset(audio));decoded={};knots={}
  for scene in scenes:
   shot=scene['shot']
   if shot not in expected:continue
   a=captures[shot]
   if not a.get('frames') or not a.get('navigation_events'):raise ValueError('Navigation actions are missing.')
   w,h=a['viewport']['width'],a['viewport']['height'];size=w*h*3
   clip=root/f'{shot}.mp4';clip.write_bytes(read_asset(a));raw=root/f'{shot}.rgb'
   subprocess.run([ffmpeg,'-y','-v','error','-i',str(clip),'-vf','fps=12','-pix_fmt','rgb24','-f','rawvideo',str(raw)],check=True,capture_output=True,timeout=90)
   count=raw.stat().st_size//size
   if count!=len(a['frames']):raise ValueError('Frame metadata does not match recorded footage.')
   decoded[shot]=(raw,w,h,size,count);knots[shot]=action_knots(scene,a,audio,creative['narration'])
  output=root/'video.mp4'
  command=[ffmpeg,'-y','-v','error','-f','rawvideo','-pix_fmt','rgb24','-s',f'{W}x{H}','-r',str(FPS),'-i','pipe:0','-i',str(voice),
   '-c:v','libx264','-preset','veryfast','-crf','18','-threads','2','-g','1','-bf','0','-pix_fmt','yuv420p','-c:a','aac','-b:a','192k','-ar','48000',
   '-af','loudnorm=I=-16:TP=-1.5:LRA=11,apad=pad_dur=0.45','-t',str(duration),'-movflags','+faststart',str(output)]
  with (root/'encode.log').open('wb') as log:
   process=subprocess.Popen(command,stdin=subprocess.PIPE,stdout=subprocess.DEVNULL,stderr=log);started=time.monotonic()
   try:
    for n in range(math.ceil(duration*FPS)):
     if time.monotonic()-started>900:raise ValueError('Navigation render exceeded time budget.')
     t=n/FPS;scene=scenes[max(0,bisect.bisect_right([s['start'] for s in scenes],t)-1)];shot=scene['shot']
     im=base.copy();d=ImageDraw.Draw(im)
     if shot in decoded:
      centered(d,scene['on_screen_text'],295,brand_font(66,True),WHITE,max_width=952)
      centered(d,scene['subhead'],460,brand_font(26),MINT,max_width=956)
      raw,w,h,size,count=decoded[shot];a=captures[shot];f=source_frame_at(knots[shot],t-scene['start']);index=min(count-1,int(f));fraction=f-index
      with raw.open('rb') as stream:stream.seek(index*size);pic=Image.frombytes('RGB',(w,h),stream.read(size))
      current=a['frames'][index];following=a['frames'][min(count-1,index+1)]
      x,y=[p+(q-p)*fraction for p,q in zip(current['cursor'],following['cursor'])]
      pd=ImageDraw.Draw(pic)
      # Pointer follows the actual capture trace; interpolation only smooths the
      # 12 fps trace for 24 fps delivery. Short click rings clarify real clicks.
      recent=next((i for i in range(index,max(-1,index-4),-1) if a['frames'][i]['click']),None)
      if recent is not None:
       cx,cy=a['frames'][recent]['cursor'];radius=13+(index-recent+fraction)*4
       pd.ellipse((cx-radius,cy-radius,cx+radius,cy+radius),outline=MINT,width=2)
      pd.polygon([(x,y),(x+2,y+23),(x+8,y+17),(x+15,y+28),(x+20,y+25),(x+13,y+14),(x+23,y+13)],fill='#f8fafc',outline=BG,width=2)
      c=current['camera'];pic=pic.crop((int(c['x']),int(c['y']),int(c['x']+c['width']),int(c['y']+c['height'])))
      scale=min(968/pic.width,914/pic.height);dw,dh=round(pic.width*scale),round(pic.height*scale)
      px=(W-dw)//2;py=542+(914-dh)//2
      im.paste(pic.resize((dw,dh),Image.Resampling.LANCZOS),(px,py));d=ImageDraw.Draw(im)
      d.rounded_rectangle((px-2,py-2,px+dw+2,py+dh+2),radius=8,outline='#334155',width=2)
      centered(d,'Actual Walnut navigation · Selected figures obscured',1479,brand_font(19),MUTED)
     else:
      centered(d,'Walnut Markets',335,brand_font(68,True),WHITE)
      im.paste(logo.resize((280,280),Image.Resampling.LANCZOS),(400,640));d=ImageDraw.Draw(im)
      centered(d,creative['brand_tagline'],1020,brand_font(61,True),WHITE,max_width=820)
      centered(d,'NVIDIA links in the comments',1220,brand_font(31),MINT)
     caption=next((c for c in captions if c['start']<=t<c['end']),None)
     if caption:centered(d,caption['text'],1540,brand_font(52,True),WHITE,max_width=940)
     gap=14;bar=(952-gap*(len(scenes)-1))/len(scenes)
     for i in range(len(scenes)):
      x=64+i*(bar+gap);d.rounded_rectangle((x,1674,x+bar,1678),radius=2,fill=MINT if i<scene['sequence'] else '#1e293b')
     if frame_observer:frame_observer(n,im)
     process.stdin.write(im.tobytes())
    process.stdin.close()
    if process.wait(timeout=120):raise ValueError('Navigation encoding failed.')
   finally:
    if process.poll() is None:process.kill();process.wait()
  content=output.read_bytes()
  if content[4:8]!=b'ftyp' or len(content)>200*1024*1024:raise ValueError('Invalid navigation export.')
  return content,{'provider':'walnut_native','width':W,'height':H,'duration':duration,'frame_rate':FPS,'encoding':'h264_intra',
   'continuous_narration':True,'shot_count':len(scenes),'caption_count':len(captions),'template_version':4,'font':Path(brand_font(24).path).name,
   'brand_accent':MINT,'logo_asset':LOGO.name,'research_brief_id':creative['source_research_brief_id'],'action_alignment':knots,
   'navigation_events':{shot:captures[shot]['navigation_events'] for shot in expected},'rendered_cursor':'recorded_curved_travel_pause_circle_click'}
