"""Private asset storage and replaceable narration/render provider adapters."""
from __future__ import annotations

import base64
import hashlib
import io
import json
import os
from typing import Protocol
from urllib.parse import urlsplit

import requests
from sqlalchemy import text

from app.services.growth_video_domain import digest, now
from app.services.growth_video_store import dumps, uid


class AssetStore:
    def __init__(self):
        import boto3
        from botocore.config import Config
        self.bucket = os.getenv("GROWTH_ASSET_BUCKET", "")
        if not self.bucket:
            raise ValueError("Configure GROWTH_ASSET_BUCKET and private S3 access before capture.")
        endpoint = os.getenv("GROWTH_S3_ENDPOINT_URL") or None
        if endpoint and urlsplit(endpoint).scheme != "https":
            raise ValueError("The private object-storage endpoint must use HTTPS.")
        self.client = boto3.client("s3", endpoint_url=endpoint,
            region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
            config=Config(signature_version="s3v4", connect_timeout=10, read_timeout=60, retries={"max_attempts": 2}))

    def put(self, db, job_id, kind, content, mime, metadata):
        sha = hashlib.sha256(content).hexdigest()
        extension = {"image/png": "png", "video/webm": "webm", "video/mp4": "mp4", "audio/mpeg": "mp3"}[mime]
        key = f"ai-growth/{job_id}/{sha}.{extension}"
        self.client.put_object(Bucket=self.bucket, Key=key, Body=content, ContentType=mime)
        record = {"id": uid("ga"), "object_key": key, "kind": kind, "sha256": sha, "bytes": len(content),
                  "content_type": mime, "created_at": now(), **metadata}
        db.execute(text("INSERT INTO growth_video_assets VALUES (:id,:job,:kind,:key,:at,:payload)"),
                   {"id": record["id"], "job": job_id, "kind": kind, "key": key, "at": now(), "payload": dumps(record)})
        db.commit()
        return record

    def url(self, asset, *, download=False, expires=900):
        from botocore.exceptions import BotoCoreError
        params = {"Bucket": self.bucket, "Key": asset["object_key"]}
        if download:
            params["ResponseContentDisposition"] = 'attachment; filename="walnut-research.mp4"'
        try:
            return self.client.generate_presigned_url("get_object", Params=params, ExpiresIn=expires)
        except BotoCoreError:
            raise ValueError("Private asset signing failed. Check the server's S3 credentials.") from None


class NarrationProvider(Protocol):
    def generate(self, script: str, voice: str, model: str) -> tuple[bytes, dict]: ...


def timed_captions(script, duration, alignment=None):
    """Use character timing where available; otherwise bounded phrase timing."""
    words = script.split()
    phrases = [" ".join(words[i:i+6]) for i in range(0, len(words), 6)]
    alignment = alignment or {}
    chars = "".join(alignment.get("characters") or [])
    starts = alignment.get("character_start_times_seconds") or []
    ends = alignment.get("character_end_times_seconds") or []
    exact = chars == script and len(starts) == len(chars) == len(ends)
    output, cursor = [], 0
    for i, phrase in enumerate(phrases):
        index = script.find(phrase, cursor)
        start = starts[index] if exact and index >= 0 else duration * i / len(phrases)
        end = ends[index + len(phrase) - 1] if exact and index >= 0 else duration * (i+1) / len(phrases)
        output.append({"text": phrase, "start": max(0, start), "end": min(duration, end)})
        cursor = index + len(phrase)
    return output


class ElevenLabsNarration:
    def generate(self, script, voice, model, *, continuous=False):
        key = os.getenv("ELEVENLABS_API_KEY", "")
        if not key or not voice:
            raise ValueError("Configure ELEVENLABS_API_KEY and an ElevenLabs voice. Captures remain saved.")
        payload = {"text": script, "model_id": model}
        if continuous:
            payload["voice_settings"] = {"stability": .5}
        response = requests.post(f"https://api.elevenlabs.io/v1/text-to-speech/{voice}/with-timestamps",
            headers={"xi-api-key": key}, params={"output_format": "mp3_44100_128"},
            json=payload, timeout=(10, 120))
        if response.status_code >= 400:
            raise ValueError(f"ElevenLabs returned HTTP {response.status_code}. Check provider configuration.")
        result = response.json()
        content = base64.b64decode(result["audio_base64"], validate=True)
        # Read the real MP3 duration; never truncate audio to a guessed word count.
        from mutagen.mp3 import MP3
        duration = MP3(io.BytesIO(content)).info.length
        if not 0 < duration <= (60 if continuous else 25):
            raise ValueError("Narration duration is outside the supported scene range.")
        return content, {"script": script, "provider": "elevenlabs", "voice": voice, "model": model,
            "duration": duration, "captions": timed_captions(script, duration, result.get("alignment")),
            "alignment": result.get("alignment"), "continuous": continuous,
            "generated_at": now(), "script_hash": digest(script)}


class VideoRenderer(Protocol):
    def create_render(self, spec: dict, template_id: str, content_id: str) -> str: ...
    def get_render_status(self, render_id: str) -> dict: ...
    def fetch_asset(self, result: dict) -> bytes: ...


class CreatomateVideoRenderer:
    endpoint = "https://api.creatomate.com/v2/renders"

    def _headers(self):
        key = os.getenv("CREATOMATE_API_KEY", "")
        if not key:
            raise ValueError("Configure CREATOMATE_API_KEY before rendering. Completed stages remain saved.")
        return {"Authorization": "Bearer " + key}

    def create_render(self, spec, template_id, content_id):
        # Optional editor templates must expose this named composition.
        payload = {"template_id": template_id, "modifications": {"Content.elements": spec["elements"], "Content.duration": spec["duration"]}} if template_id else dict(spec)
        payload["metadata"] = content_id
        response = requests.post(self.endpoint, headers=self._headers(), json=payload, timeout=(10, 90))
        if response.status_code >= 400:
            raise ValueError(f"Creatomate returned HTTP {response.status_code}. Check provider dashboard before retrying.")
        data = response.json()
        # API v2 returns one render object; accept the legacy singleton array too.
        render = data[0] if isinstance(data, list) and len(data) == 1 else data
        if not isinstance(render, dict) or not isinstance(render.get("id"), str) or not render["id"].strip():
            raise ValueError("Creatomate submission outcome is uncertain. Check the provider dashboard before retrying.")
        return render["id"]

    def get_render_status(self, render_id):
        response = requests.get(self.endpoint + "/" + render_id, headers=self._headers(), timeout=(10, 30))
        if response.status_code >= 400:
            raise ValueError(f"Creatomate status returned HTTP {response.status_code}; the existing render ID is retained.")
        return response.json()

    def fetch_asset(self, result):
        url = result.get("url", "")
        parsed = urlsplit(url)
        host = parsed.hostname or ""
        # This exact B2 bucket is used by Creatomate's live v2 render delivery.
        verified_b2 = host == "f002.backblazeb2.com" and parsed.path.startswith("/file/creatomate-c8xg3hsxdu/")
        provider_host = any(host.endswith(suffix) for suffix in (".creatomate.com", ".amazonaws.com"))
        if parsed.scheme != "https" or parsed.username or parsed.port not in (443, None) or not (provider_host or verified_b2):
            raise ValueError("Unexpected render asset host. Download blocked.")
        with requests.get(url, stream=True, allow_redirects=False, timeout=(10, 60)) as response:
            if response.status_code != 200:
                raise ValueError("Rendered MP4 could not be fetched.")
            chunks, size = [], 0
            for chunk in response.iter_content(1024 * 1024):
                size += len(chunk)
                if size > 200 * 1024 * 1024:
                    raise ValueError("Rendered asset exceeds the 200 MB limit.")
                chunks.append(chunk)
        content = b"".join(chunks)
        if content[4:8] != b"ftyp":
            raise ValueError("Renderer returned an invalid MP4 asset.")
        return content


def render_spec(creative, captures, audio, storage):
    elements = []
    elapsed = 0.0
    accent = {"research_finding": "#68e0b5", "product_investigation": "#90caff", "search_explanation": "#d1bbff"}[creative["format"]]

    def label(value, time, duration, y, *, size=48, color="#ffffff", height="16%"):
        return {"type": "text", "text": value, "time": time, "duration": duration, "x": "48%", "y": y,
                "width": "80%", "height": height, "font_family": "Inter", "font_size": size,
                "fill_color": color, "x_alignment": "50%", "y_alignment": "50%"}

    for scene in creative["storyboard"]:
        key = str(scene["sequence"])
        voice = audio[key]
        duration = max(scene["duration_seconds"], voice["duration"] + .35)
        elements.append({"type": "audio", "source": storage.url(voice, expires=7200), "time": elapsed, "duration": voice["duration"]})
        if key in captures:
            asset = captures[key]
            media = {"type": "video" if asset["content_type"] == "video/webm" else "image",
                     "source": storage.url(asset, expires=7200), "time": elapsed, "duration": duration,
                     "x": "48%", "y": "45%", "width": "84%", "height": "48%", "fit": "contain"}
            if media["type"] == "video":
                media.update({"trim_start": asset.get("trim_start", 0), "volume": "0%"})
            if scene["transition"] == "fade":
                media["animations"] = [{"time": 0, "duration": .3, "type": "fade"}]
            if media["type"] == "video":
                # Creatomate forbids combining loop with trim_start. Repeat the
                # verified five-second excerpt as explicit timeline elements.
                offset = 0.0
                while offset < duration:
                    length = min(5.0, duration - offset)
                    elements.append({**media, "time": elapsed + offset, "duration": length, "trim_duration": length,
                                     "animations": media.get("animations", []) if offset == 0 else []})
                    offset += length
            else:
                elements.append(media)
        else:
            elements.append(label(scene["on_screen_text"], elapsed, duration, "43%", size=60, color=accent, height="38%"))
        for caption in voice["captions"]:
            elements.append(label(caption["text"], elapsed + caption["start"], max(.1, caption["end"] - caption["start"]), "74%", size=48))
        elapsed += duration
    if elapsed > 60:
        raise ValueError("Narration exceeds 60 seconds. Shorten the script before rendering.")
    elements.extend([
        label("WALNUT MARKETS", 0, elapsed, "12%", size=38, color=accent, height="5%"),
        label("Dated research • Sources available in the full brief", 0, elapsed, "83%", size=23, color="#9baabd", height="5%"),
    ])
    return {"output_format": "mp4", "width": 1080, "height": 1920, "frame_rate": 30,
            "duration": elapsed, "fill_color": "#07111f", "elements": elements}
