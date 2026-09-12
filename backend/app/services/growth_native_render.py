"""Bounded 1080x1920 product-ad compositor using Walnut-owned footage and audio.

No generative model redraws the UI. One unbroken narration drives shot boundaries
and phrase captions. Rendering uses local FFmpeg, with no per-render provider fee.
"""
from __future__ import annotations

import bisect
import math
import os
from pathlib import Path
import re
import subprocess
import tempfile
import time
from functools import lru_cache

from PIL import Image, ImageDraw, ImageFont, ImageOps
from app.services.growth_product_ad import BACKGROUND

WIDTH, HEIGHT, FPS = 1080, 1920, 24


@lru_cache(maxsize=24)
def font(size, bold=False):
    candidates = [os.getenv("GROWTH_AD_FONT_BOLD" if bold else "GROWTH_AD_FONT", ""),
        "C:/Windows/Fonts/arialbd.ttf" if bold else "C:/Windows/Fonts/arial.ttf",
        "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf" if bold else "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf" if bold else "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"]
    for path in candidates:
        if path and Path(path).is_file():
            return ImageFont.truetype(path, size)
    raise ValueError("Install a TrueType font for the native video worker.")


def timeline(creative, audio):
    """Use actual character timing; fail rather than fabricate synchronization."""
    script = creative["narration"]
    alignment = audio.get("alignment") or {}
    chars = "".join(alignment.get("characters", []))
    starts = alignment.get("character_start_times_seconds", [])
    ends = alignment.get("character_end_times_seconds", [])
    duration = audio["duration"]
    if chars != script or len(chars) != len(starts) or len(chars) != len(ends):
        raise ValueError("Continuous voice alignment does not match the script. Review narration before rendering.")
    if not 10 <= duration <= 60 or any(not math.isfinite(v) or v < 0 or v > duration + .25 for v in starts + ends):
        raise ValueError("Invalid narration timing.")
    if any(b < a for a, b in zip(starts, starts[1:])) or any(b < a for a, b in zip(starts, ends)):
        raise ValueError("Narration alignment is not monotonic.")
    scenes, cursor = [], 0
    for scene in creative["storyboard"]:
        index = script.find(scene["narration"], cursor)
        if index < 0:
            raise ValueError("A product scene is missing from the continuous narration.")
        scenes.append({**scene, "start": starts[index]})
        cursor = index + len(scene["narration"])
    scenes[0]["start"] = 0
    for i, scene in enumerate(scenes):
        scene["end"] = scenes[i+1]["start"] if i+1 < len(scenes) else duration + .45
    words = list(re.finditer(r"\S+", script))
    captions = []
    for i in range(0, len(words), 4):
        chunk = words[i:i+4]
        captions.append({"text": " ".join(w.group() for w in chunk), "start": starts[chunk[0].start()], "end": ends[chunk[-1].end()-1]})
    return scenes, captions, duration + .45


def centered(draw, text, y, fnt, fill, *, max_width=900, line_height=None):
    lines = []
    for paragraph in text.split("\n"):
        line = ""
        for word in paragraph.split():
            candidate = (line + " " + word).strip()
            if draw.textlength(candidate, font=fnt) > max_width and line:
                lines.append(line)
                line = word
            else:
                line = candidate
        lines.append(line)
    step = line_height or int(fnt.size * 1.16)
    for line in lines:
        draw.text((WIDTH/2, y), line, font=fnt, fill=fill, anchor="mt", stroke_width=0)
        y += step
    return y


def render_product_video(creative, captures, audio, read_asset):
    import imageio_ffmpeg
    ffmpeg = imageio_ffmpeg.get_ffmpeg_exe()
    scenes, captions, duration = timeline(creative, audio)
    if duration > 60:
        raise ValueError("Product ad exceeds 60 seconds.")
    base = ImageOps.fit(Image.open(BACKGROUND).convert("RGB"), (WIDTH, HEIGHT))
    shades = Image.new("RGBA", (WIDTH, HEIGHT), (0,0,0,0))
    sd = ImageDraw.Draw(shades)
    for y in range(HEIGHT):
        sd.line((0,y,WIDTH,y), fill=(2,8,15,85 if y < 900 else 115))
    title_font, small_font, caption_font = font(82, True), font(31), font(57, True)
    label_font, number_font = font(30, True), font(160, True)
    with tempfile.TemporaryDirectory(prefix="walnut-render-") as directory:
        root = Path(directory)
        audio_path = root / "voice.mp3"
        audio_path.write_bytes(read_asset(audio))
        frames = {}
        # Decode short crops to temporary files; never hold an entire video in RAM.
        for shot, asset in captures.items():
            clip = root / f"{shot}.webm"
            clip.write_bytes(read_asset(asset))
            crop = asset["crop"]
            w = 820
            h = round(w * crop["height"] / crop["width"])
            if h > 890:
                h = 890
                w = round(h * crop["width"] / crop["height"])
            filt = f"crop={crop['width']}:{crop['height']}:{crop['x']}:{crop['y']},scale={w}:{h},fps=12"
            raw_path=root/f"{shot}.rgb"
            cmd = [ffmpeg,"-y","-v","error","-ss",str(asset["trim_start"]),"-i",str(clip),"-t","5","-vf",filt,"-f","rawvideo","-pix_fmt","rgb24",str(raw_path)]
            subprocess.run(cmd, capture_output=True, timeout=90, check=True)
            size = w*h*3
            if raw_path.stat().st_size < size:
                raise ValueError("Recorded product footage is empty.")
            frames[shot] = (raw_path, size, w, h, raw_path.stat().st_size//size)
        output = root / "video.mp4"
        cmd = [ffmpeg,"-y","-v","error","-f","rawvideo","-pix_fmt","rgb24","-s",f"{WIDTH}x{HEIGHT}","-r",str(FPS),"-i","pipe:0",
               "-i",str(audio_path),"-c:v","libx264","-preset","veryfast","-crf","19","-threads","2","-pix_fmt","yuv420p",
               "-c:a","aac","-b:a","192k","-ar","48000","-af","loudnorm=I=-16:TP=-1.5:LRA=11,apad=pad_dur=0.45","-t",str(duration),"-movflags","+faststart",str(output)]
        with (root / "ffmpeg.log").open("wb") as errors:
            process = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=errors)
            started = time.monotonic()
            try:
                for frame_no in range(math.ceil(duration*FPS)):
                    if time.monotonic() - started > 900:
                        raise ValueError("Native rendering exceeded its time budget.")
                    t = frame_no/FPS
                    scene = scenes[max(0,bisect.bisect_right([s["start"] for s in scenes],t)-1)]
                    phase = min(1,(t-scene["start"])/max(.1,scene["end"]-scene["start"]))
                    # Subtle camera push gives the original lifestyle image motion.
                    z = 1 + .035*phase
                    bw,bh = int(WIDTH/z),int(HEIGHT/z)
                    im = base.crop(((WIDTH-bw)//2,(HEIGHT-bh)//2,(WIDTH+bw)//2,(HEIGHT+bh)//2)).resize((WIDTH,HEIGHT),Image.Resampling.BILINEAR).convert("RGBA")
                    im.alpha_composite(shades)
                    d = ImageDraw.Draw(im)
                    centered(d,"WALNUT  /  MARKETS",186,label_font,"#f1d5a3")
                    shot = scene["shot"]
                    if shot in frames:
                        centered(d,scene["on_screen_text"],294,font(66,True),"white")
                        centered(d,scene["subhead"],382,small_font,"#d2dbe6")
                        raw_path,size,w,h,count = frames[shot]
                        n = min(count-1, int(phase*(count-1)))
                        with raw_path.open("rb") as source_frames:
                            source_frames.seek(n*size)
                            pic = Image.frombytes("RGB",(w,h),source_frames.read(size))
                        # Camera settles on each live component; footage itself scrolls/hover-inspects.
                        display_w = int(900+22*phase)
                        display_h = int(h*display_w/w)
                        if display_h > 870:
                            display_h=870
                            display_w=int(w*display_h/h)
                        pic=pic.resize((display_w,display_h),Image.Resampling.LANCZOS).convert("RGBA")
                        x,y=(WIDTH-display_w)//2,480+(870-display_h)//2
                        d.rounded_rectangle((x-9,y-9,x+display_w+9,y+display_h+9),radius=20,fill="#0b1523",outline="#bd9865",width=2)
                        im.alpha_composite(pic,(x,y))
                        d=ImageDraw.Draw(im)
                        date=str(captures[shot]["captured_at"])[:10]
                        centered(d,f"ACTUAL WALNUT SCREEN  •  {date}",1330,font(25),"#bdc6d3")
                    else:
                        y=490 if shot!="cta" else 535
                        centered(d,scene["on_screen_text"],y,font(100,True),"#ffffff",line_height=114)
                        centered(d,scene["subhead"],925,font(52,True),"#ecd0a0")
                        if shot=="cta":
                            d.rounded_rectangle((205,1060,875,1164),radius=20,fill="#d8b37c")
                            centered(d,"TRY WALNUT FREE",1081,font(43,True),"#101823")
                            centered(d,"walnutmarkets.com",1204,font(39),"#ffffff")
                    caption=next((c for c in captions if c["start"]<=t<c["end"]),None)
                    if caption:
                        d.rounded_rectangle((72,1370,1008,1543),radius=20,fill=(4,10,18,220))
                        centered(d,caption["text"],1395,caption_font,"#ffffff",max_width=856)
                    centered(d,"Product demo • Some features require a paid plan",1580,font(25),"#c0c6d0")
                    centered(d,"Not a recommendation to buy NVIDIA",1620,font(25),"#c0c6d0")
                    d.rectangle((72,1674,1008,1678),fill="#39414a")
                    d.rectangle((72,1674,72+936*t/duration,1678),fill="#d8b37c")
                    process.stdin.write(im.convert("RGB").tobytes())
                process.stdin.close()
                if process.wait(timeout=120):
                    raise ValueError("Native video encoding failed.")
            finally:
                if process.poll() is None:
                    process.kill()
                    process.wait()
        content = output.read_bytes()
        if content[4:8] != b"ftyp" or len(content) > 200*1024*1024:
            raise ValueError("Invalid native MP4 output.")
        return content, {"provider":"walnut_native","width":WIDTH,"height":HEIGHT,"duration":duration,"frame_rate":FPS,
                         "continuous_narration":True,"shot_count":len(scenes),"caption_count":len(captions),"template_version":2}
