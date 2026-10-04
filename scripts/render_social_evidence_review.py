"""Offline evidence-card comparison over retained video/audio; never publishes.

Requires Pillow and imageio-ffmpeg. Config paths are relative to the config file.
The lower presenter/caption band stays unchanged. Only the upper panel changes.
"""
import argparse
import json
import subprocess
import sys
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont


def render(config_path, output, tools_dir=None):
    if tools_dir:
        sys.path.insert(0, str(tools_dir.resolve()))
    import imageio_ffmpeg

    cfg = json.loads(config_path.read_text(encoding="utf-8"))
    base = config_path.parent
    source = (base / cfg["source_video"]).resolve()
    screenshot = Image.open(base / cfg["evidence_image"]).convert("RGB")
    scenes = cfg["scenes"]
    if not scenes or scenes[0]["start"] != 0:
        raise ValueError("Scenes must begin at zero.")
    if any(a["start"] >= b["start"] for a, b in zip(scenes, scenes[1:])):
        raise ValueError("Scene times must increase.")
    output.parent.mkdir(parents=True, exist_ok=True)
    ffmpeg = imageio_ffmpeg.get_ffmpeg_exe()
    font = lambda size: ImageFont.truetype(cfg["font"], size)
    args = [ffmpeg, "-y", "-i", str(source)]
    filters = []
    for i, scene in enumerate(scenes):
        panel = Image.new("RGB", (1080, 1380), "#0D1117")
        draw = ImageDraw.Draw(panel)
        draw.text((64, 248), cfg["review_label"], font=font(25), fill="#A7B8B2")
        draw.text((64, 304), cfg["subject_label"], font=font(30), fill="#29D981")
        y = 385
        heading_size = 112 if len(scene["heading"]) <= 21 else 72
        for line in scene["heading"].split("\n"):
            if draw.textlength(line, font=font(heading_size)) > 848:
                raise ValueError("Heading exceeds the safe area.")
            draw.text((64, y), line, font=font(heading_size), fill="#F3F7F6")
            y += heading_size + 14
        for line in scene["context"].split("\n"):
            if draw.textlength(line, font=font(34)) > 848:
                raise ValueError("Context exceeds the safe area.")
            draw.text((64, y+20), line, font=font(34), fill="#A7B8B2")
            y += 47
        x, top, width, height = scene["crop"]
        if min(x, top) < 0 or min(width, height) <= 0 or x+width > screenshot.width or top+height > screenshot.height:
            raise ValueError("Evidence crop exceeds the actual screenshot.")
        crop = screenshot.crop((x, top, x+width, top+height))
        scale = min(848/crop.width, 430/crop.height)
        crop = crop.resize((round(crop.width*scale), round(crop.height*scale)), Image.Resampling.LANCZOS)
        panel.paste(crop, (64+(848-crop.width)//2, 815))
        draw.line((64, 775, 912, 775), fill="#284338", width=2)
        draw.text((64, 1290), cfg["source_label"], font=font(27), fill="#A7B8B2")
        png = output.parent / f"evidence-scene-{i+1}.png"
        panel.save(png)
        args += ["-i", str(png)]
        previous = "0:v" if i == 0 else f"v{i}"
        end = scenes[i+1]["start"] if i+1 < len(scenes) else 86400
        filters.append(f"[{previous}][{i+1}:v]overlay=0:0:enable='gte(t,{scene['start']})*lt(t,{end})'[v{i+1}]")
    args += ["-filter_complex", ";".join(filters), "-map", f"[v{len(scenes)}]", "-map", "0:a:0",
             "-c:v", "libx264", "-preset", "fast", "-crf", "19", "-pix_fmt", "yuv420p",
             "-c:a", "copy", "-movflags", "+faststart", str(output)]
    with (output.parent / "render.log").open("w") as log:
        subprocess.run(args, stderr=log, check=True, timeout=240)
    subprocess.run([ffmpeg, "-v", "error", "-i", str(output), "-f", "null", "-"], check=True, timeout=90)
    audio_hashes = []
    for path in (source, output):
        result = subprocess.run([ffmpeg, "-v", "error", "-i", str(path), "-map", "0:a:0", "-c", "copy", "-f", "hash", "-hash", "sha256", "-"], check=True, capture_output=True, timeout=45)
        audio_hashes.append(result.stdout.decode().strip())
    if audio_hashes[0] != audio_hashes[1]:
        raise ValueError("Source audio changed.")
    result = {"output": str(output), "audio_preserved": True, "scene_count": len(scenes),
              "status": "historical layout comparison; not publishable as current research",
              "voice_changed": False, "full_decode": "passed"}
    (output.parent / "validation.json").write_text(json.dumps(result, indent=2))
    print(json.dumps(result), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--tools-dir", type=Path)
    opts = parser.parse_args()
    render(opts.config, opts.output, opts.tools_dir)
