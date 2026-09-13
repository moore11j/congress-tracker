"""Walnut brand composition: original logo, system typography, real product pixels."""
import bisect
import math
import os
import subprocess
import tempfile
import time
from functools import lru_cache
from pathlib import Path
from PIL import Image, ImageDraw, ImageFont
from app.services.growth_native_render import timeline, centered
from app.services.growth_research_ad import LOGO

W,H,FPS=1080,1920,24
BG='#020617';MINT='#6ee7b7';WHITE='#f1f5f9';MUTED='#94a3b8'

@lru_cache(maxsize=32)
def brand_font(size,bold=False):
 # Match the website's system-ui font on the rendering host. On the owner's
 # Windows machine this is Segoe UI; do not redistribute proprietary OS fonts.
 paths=[os.getenv('GROWTH_BRAND_FONT_SEMIBOLD' if bold else 'GROWTH_BRAND_FONT',''),
  'C:/Windows/Fonts/seguisb.ttf' if bold else 'C:/Windows/Fonts/segoeui.ttf',
  '/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf' if bold else '/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf']
 for path in paths:
  if path and Path(path).is_file():return ImageFont.truetype(path,size)
 raise ValueError('No system UI font available for branded video.')

def render_research_video(creative,captures,audio,read_asset,*,frame_observer=None):
 import imageio_ffmpeg
 ffmpeg=imageio_ffmpeg.get_ffmpeg_exe()
 scenes,captions,duration=timeline(creative,audio)
 expected={s['shot'] for s in scenes if s['walnut_url']}
 if not expected.issubset(captures):raise ValueError('Missing narration-matched research footage.')
 logo=Image.open(LOGO).convert('RGB').resize((112,112),Image.Resampling.LANCZOS)
 base=Image.new('RGB',(W,H),BG);d=ImageDraw.Draw(base)
 base.paste(logo,(64,135));d.text((191,159),'Walnut',font=brand_font(43,True),fill=WHITE)
 d.text((193,211),'Market Terminal',font=brand_font(23),fill=MINT)
 d.line((64,279,1016,279),fill='#1e293b',width=1)
 # One restrained line in the lower safe area, like the website's legal footer.
 centered(d,'Research only · Not investment advice · Paid features shown',1680,brand_font(18),MUTED)
 centered(d,'walnutmarkets.com',1730,brand_font(25),MINT)
 with tempfile.TemporaryDirectory(prefix='walnut-brand-render-') as folder:
  root=Path(folder);voice=root/'voice.mp3';voice.write_bytes(read_asset(audio));decoded={}
  for shot in sorted(expected):
   a=captures[shot]
   if not a.get('focus_panels'):raise ValueError('Missing reviewed screen focus.')
   clip=root/f'{shot}.mp4';clip.write_bytes(read_asset(a));raw=root/f'{shot}.rgb'
   subprocess.run([ffmpeg,'-y','-v','error','-i',str(clip),'-t','6','-vf','fps=12','-pix_fmt','rgb24','-f','rawvideo',str(raw)],check=True,capture_output=True,timeout=90)
   w,h=a['viewport']['width'],a['viewport']['height'];size=w*h*3
   count=raw.stat().st_size//size
   if count<2:raise ValueError('Empty product recording.')
   decoded[shot]=(raw,w,h,size,count)
  def footage(shot,phase):
   raw,w,h,size,count=decoded[shot]
   index=min(count-1,int(phase*(count-1)))
   with raw.open('rb') as f:
    f.seek(index*size);im=Image.frombytes('RGB',(w,h),f.read(size))
   cursor=captures[shot].get('cursor_path',[])
   if cursor:
    # Browser screenshots omit the pointer; restore its recorded coordinates.
    x,y=cursor[min(index,len(cursor)-1)];d=ImageDraw.Draw(im)
    d.polygon([(x,y),(x+2,y+23),(x+8,y+17),(x+15,y+28),(x+20,y+25),(x+13,y+14),(x+23,y+13)],fill='#f8fafc',outline='#020617',width=2)
   panels=captures[shot]['focus_panels'];pieces=[]
   for p in panels:pieces.append(im.crop((p['x'],p['y'],p['x']+p['width'],p['y']+p['height'])))
   if len(pieces)==1:return pieces[0]
   # Two labelled source columns from the same rows, retaining their alignment.
   joined=Image.new('RGB',(sum(p.width for p in pieces)+24*(len(pieces)-1),max(p.height for p in pieces)),BG)
   x=0
   for pic in pieces:joined.paste(pic,(x,0));x+=pic.width+24
   return joined
  def place(im,pic,top,bottom,max_width=952):
   scale=min(max_width/pic.width,(bottom-top)/pic.height)
   size=(round(pic.width*scale),round(pic.height*scale));pic=pic.resize(size,Image.Resampling.LANCZOS)
   x=(W-size[0])//2;y=top+(bottom-top-size[1])//2
   ImageDraw.Draw(im).rounded_rectangle((x-2,y-2,x+size[0]+2,y+size[1]+2),radius=12,outline='#334155',width=2)
   im.paste(pic,(x,y))
  output=root/'video.mp4'
  # Independent video frames preserve static brand elements across cuts. The
  # inter-frame export showed missing overlay blocks during decoded-frame QA.
  command=[ffmpeg,'-y','-v','error','-f','rawvideo','-pix_fmt','rgb24','-s',f'{W}x{H}','-r',str(FPS),'-i','pipe:0','-i',str(voice),
   '-c:v','libx264','-preset','veryfast','-crf','18','-threads','2','-g','1','-bf','0','-pix_fmt','yuv420p','-c:a','aac','-b:a','192k','-ar','48000',
   '-af','loudnorm=I=-16:TP=-1.5:LRA=11,apad=pad_dur=0.45','-t',str(duration),'-movflags','+faststart',str(output)]
  with (root/'ffmpeg.log').open('wb') as errors:
   process=subprocess.Popen(command,stdin=subprocess.PIPE,stdout=subprocess.DEVNULL,stderr=errors);started=time.monotonic()
   try:
    for n in range(math.ceil(duration*FPS)):
     if time.monotonic()-started>900:raise ValueError('Research render exceeded its time budget.')
     t=n/FPS;s=scenes[max(0,bisect.bisect_right([s['start'] for s in scenes],t)-1)]
     phase=max(0,min(1,(t-s['start'])/(s['end']-s['start'])));shot=s['shot']
     im=base.copy();d=ImageDraw.Draw(im)
     centered(d,s['on_screen_text'],330,brand_font(74,True),WHITE,max_width=946)
     centered(d,s['subhead'],518,brand_font(28),MINT)
     if shot in decoded:
      pic=footage(shot,phase)
      if shot=='v3_profile':
       place(im,pic,655,885);place(im,footage('v3_history',phase),935,1290)
      else:place(im,pic,615,1340)
      d=ImageDraw.Draw(im)
      centered(d,'WALNUT / '+('INSTITUTION' if 'institution/' in captures[shot]['page_url'] else 'NVDA')+'  ·  '+captures[shot]['captured_at'][:10],1382,brand_font(21),MUTED)
     elif shot=='v3_brief':
      d.rounded_rectangle((64,640,1016,1238),radius=28,fill='#0f172a',outline='#334155',width=2)
      d.text((112,691),'RESEARCH BRIEF',font=brand_font(27,True),fill=MINT)
      centered(d,'Who is buying NVIDIA stock\nin the latest 13F filings?',805,brand_font(61,True),WHITE,max_width=842)
      centered(d,'Holders. Position changes. Filing dates.',1103,brand_font(29),MUTED)
     else:
      big=Image.open(LOGO).convert('RGB').resize((390,390),Image.Resampling.LANCZOS);im.paste(big,(345,615));d=ImageDraw.Draw(im)
      d.rounded_rectangle((170,1090,910,1200),radius=20,fill=MINT)
      centered(d,'EXPLORE NVIDIA',1113,brand_font(42,True),BG)
      centered(d,'Your research starts here.',1260,brand_font(33),WHITE)
     caption=next((c for c in captions if c['start']<=t<c['end']),None)
     if caption:
      d=ImageDraw.Draw(im);centered(d,caption['text'],1470,brand_font(56,True),WHITE,max_width=920)
     d=ImageDraw.Draw(im)
     for i in range(len(scenes)):
      x=64+i*121;d.rounded_rectangle((x,1620,x+101,1624),radius=2,fill=MINT if i<s['sequence'] else '#1e293b')
     if frame_observer:frame_observer(n,im)
     process.stdin.write(im.tobytes())
    process.stdin.close()
    if process.wait(timeout=120):raise ValueError('Research video encode failed.')
   finally:
    if process.poll() is None:process.kill();process.wait()
  content=output.read_bytes()
  if content[4:8]!=b'ftyp' or len(content)>200*1024*1024:raise ValueError('Invalid research video output.')
  return content,{'provider':'walnut_native','width':W,'height':H,'duration':duration,'frame_rate':FPS,'encoding':'h264_intra',
   'continuous_narration':True,'shot_count':len(scenes),'caption_count':len(captions),'template_version':3,
   'font':Path(brand_font(24).path).name,'brand_accent':MINT,'logo_asset':LOGO.name,
   'research_brief_id':creative['source_research_brief_id'],'focused_real_browser_pixels':True}
