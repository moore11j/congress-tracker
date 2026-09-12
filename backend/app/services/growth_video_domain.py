"""Strict creative contracts. Model-written financial prose is not evidence."""
from __future__ import annotations

import hashlib
import json
import math
import re
from datetime import datetime, timezone
from typing import Literal
from urllib.parse import urlsplit

from pydantic import BaseModel, ConfigDict, Field

FORMATS = ("research_finding", "product_investigation", "search_explanation")
BRIEF_FIELDS = (
    "product", "positioning", "north_star", "audience_segments", "audience_pains",
    "desired_outcomes", "competitors", "differentiation", "features", "pricing",
    "brand_voice", "prohibited_language", "preferred_language", "content_principles",
    "site_audit", "growth_opportunities", "action_plan", "successful_examples",
    "rejected_examples", "platform_instructions", "reddit_lessons", "tiktok", "instagram",
)
COPY = {
    "hook_context": "Before following a stock idea, check the context.",
    "hook_research": "Start with the evidence. Then build your view.",
    "hook_question": "Researching this stock? Start here.",
    "hook_investigate": "Here is a way to investigate a stock idea.",
    "context": "Compare the dated figures with the full research before drawing a conclusion.",
    "interpretation": "A single data point is a starting point for research.",
    "caution": "This is research, not a promise of future returns.",
    "cta": "Read the full research on Walnut Markets.",
}
DEFAULT_WEIGHTS = {"search_demand": .15, "evidence": .25, "freshness": .15,
                   "audience_fit": .1, "visual": .15, "conversion": .1, "novelty": .1}


def now():
    return datetime.now(timezone.utc).isoformat()


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, default=str).encode()).hexdigest()


def date_value(value):
    result = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return result if result.tzinfo else result.replace(tzinfo=timezone.utc)


def allowed_route(url: str) -> str:
    parsed = urlsplit(url)
    if parsed.scheme != "https" or parsed.netloc != "walnutmarkets.com" or parsed.query or parsed.fragment:
        raise ValueError("Capture requires a canonical public Walnut URL without parameters.")
    if not re.fullmatch(r"/research/[a-z0-9]+(?:-[a-z0-9]+)*", parsed.path):
        raise ValueError("This Walnut route is not supported by the capture allowlist.")
    return parsed.path


def score_opportunity(*, age_days, evidence_count, impressions=0, keyword_volume=None, prior_count=0, weights=None):
    weights = weights or DEFAULT_WEIGHTS
    if set(weights) != set(DEFAULT_WEIGHTS) or any(not math.isfinite(v) or v < 0 for v in weights.values()) or sum(weights.values()) <= 0:
        raise ValueError("Scoring weights must be finite, nonnegative and have a positive total.")
    keyword_bonus = min(20, math.log10(1 + max(0, keyword_volume)) * 5) if keyword_volume is not None else 0
    components = {"search_demand": min(100, math.log1p(max(0, impressions)) * 15 + keyword_bonus),
                  "evidence": min(100, evidence_count * 40),
                  "freshness": max(0, 100 - max(0, age_days) * 4),
                  "audience_fit": 80, "visual": 90, "conversion": 75,
                  "novelty": max(0, 100 - prior_count * 35)}
    score = sum(components[k] * weights[k] for k in weights) / sum(weights.values())
    return round(score, 2), {k: round(v, 2) for k, v in components.items()}


class Strict(BaseModel):
    model_config = ConfigDict(extra="forbid")


class Scene(Strict):
    sequence: int = Field(ge=1, le=8)
    duration_seconds: int = Field(ge=3, le=15)
    statement_ids: list[str] = Field(min_length=1, max_length=3)
    visual_type: Literal["walnut_screenshot", "walnut_recording", "title_card", "data_card", "cta_card"]
    capture_target: Literal["research_header", "research_summary", "none"]
    capture_action: Literal["screenshot", "record", "none"]
    transition: Literal["fade", "cut"]


class Storyboard(Strict):
    format: Literal["research_finding", "product_investigation", "search_explanation"]
    creative_angle: Literal["dated_evidence", "investigate_before_following", "answer_the_question"]
    hook_id: str
    alternate_hook_ids: list[str] = Field(min_length=1, max_length=3)
    caption_statement_ids: list[str] = Field(min_length=1, max_length=4)
    scenes: list[Scene] = Field(min_length=3, max_length=8)


def statement_library(evidence):
    return {**COPY, **{e["id"]: e["statement"] for e in evidence}}


def validate_storyboard(raw, opportunity, evidence):
    board = Storyboard.model_validate(raw)
    allowed_route(opportunity["destination_url"])
    library = statement_library(evidence)
    evidence_ids = {e["id"] for e in evidence}
    if not evidence_ids:
        raise ValueError("Verified financial evidence is required.")
    for item in evidence:
        if not item.get("source_record_id") or not item.get("effective_date") or not item.get("source_hash"):
            raise ValueError("Evidence provenance is incomplete.")
    hooks = [board.hook_id, *board.alternate_hook_ids]
    if any(h not in COPY or not h.startswith("hook_") for h in hooks):
        raise ValueError("Choose a supported hook. Unsupported financial prose is rejected.")
    if any(s not in library for s in board.caption_statement_ids):
        raise ValueError("Caption contains an unsupported claim.")
    elapsed = 0
    scenes = []
    used = set()
    for index, scene in enumerate(board.scenes, 1):
        if scene.sequence != index or any(s not in library for s in scene.statement_ids):
            raise ValueError("Storyboard sequence or statement provenance is invalid.")
        capture = scene.visual_type.startswith("walnut_")
        if capture != (scene.capture_target != "none") or capture != (scene.capture_action != "none"):
            raise ValueError("Capture action and visual type must match.")
        if capture and scene.capture_action != ("record" if scene.visual_type == "walnut_recording" else "screenshot"):
            raise ValueError("Unsupported capture command.")
        claims = [s for s in scene.statement_ids if s in evidence_ids]
        used.update(claims)
        narration = " ".join(library[s] for s in scene.statement_ids)
        # Avoid squeezing a lengthy factual statement into an unreadable scene.
        if len(narration.split()) > scene.duration_seconds * 3:
            raise ValueError("Scene is too short for its narration; increase duration.")
        scenes.append({**scene.model_dump(), "start": elapsed, "end": elapsed + scene.duration_seconds,
                       "narration": narration, "on_screen_text": narration,
                       "evidence_ids": claims, "walnut_url": opportunity["destination_url"] if capture else None,
                       "crop": "contain", "highlight": None,
                       "expected_asset_type": "video" if scene.capture_action == "record" else "image"})
        elapsed += scene.duration_seconds
    if not 15 <= elapsed <= 60 or not used or not any(s["walnut_url"] for s in scenes):
        raise ValueError("Video must last 15–60 seconds and contain evidence and a real Walnut capture.")
    if board.scenes[0].statement_ids != [board.hook_id] or board.scenes[-1].statement_ids != ["cta"]:
        raise ValueError("The first scene must use the selected hook and the last must use the CTA.")
    return {**board.model_dump(), "storyboard": scenes, "target_duration_seconds": elapsed,
            "hook": library[board.hook_id], "alternate_hooks": [library[s] for s in board.alternate_hook_ids],
            "narration": " ".join(s["narration"] for s in scenes),
            "caption": " ".join(library[s] for s in board.caption_statement_ids),
            "cta": COPY["cta"], "target_url": opportunity["destination_url"], "evidence_ids": sorted(used),
            "warnings": ["Dated research snapshot; no claim of current pricing."]}
