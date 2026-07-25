from __future__ import annotations

import json
import logging
import os
import re
import threading
import time
import uuid
from copy import deepcopy
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import requests
from fastapi import HTTPException
from sqlalchemy import desc, func, select, text
from sqlalchemy.orm import Session

from app.db import SessionLocal
from app.models import Event, FundamentalsCache, GovernmentContract, QuoteCache, Security, TickerFinancialsCache, TickerMeta, UserAccount
from app.services.ai_marketing import (
    AI_MARKETING_IMAGE_GENERATION_ENABLED,
    AI_MARKETING_IMAGE_MODEL,
    AI_MARKETING_IMAGE_QUALITY,
    AI_MARKETING_IMAGE_SIZE,
    DEFAULT_AI_MARKETING_IMAGE_MODEL,
    DEFAULT_AI_MARKETING_IMAGE_QUALITY,
    DEFAULT_AI_MARKETING_IMAGE_SIZE,
    OPENAI_API_KEY,
    resolved_setting_value,
)
from app.services.confirmation_score import get_confirmation_score_bundles_for_tickers
from app.utils.symbols import normalize_symbol

RESEARCH_BRIEF_PROMPT_VERSION = "research_brief_v1"
RESEARCH_BRIEF_GENERATOR_MODEL = "RESEARCH_BRIEF_GENERATOR_MODEL"
RESEARCH_BRIEF_MODEL_DEFAULT = "RESEARCH_BRIEF_MODEL_DEFAULT"
RESEARCH_BRIEF_MODEL_OPTIONS = "RESEARCH_BRIEF_MODEL_OPTIONS"
RESPONSES_ENDPOINT = "https://api.openai.com/v1/responses"
IMAGES_ENDPOINT = "https://api.openai.com/v1/images/generations"
STORE_ENV = "RESEARCH_BRIEF_DRAFT_STORE_PATH"
MOCK_ENV = "RESEARCH_BRIEF_GENERATOR_MOCK"
DEFAULT_RESEARCH_BRIEF_MODEL = "gpt-5.6-terra"
DEFAULT_RESEARCH_BRIEF_MODEL_OPTIONS = ["gpt-5.6-luna", "gpt-5.6-terra", "gpt-5.6-sol"]
RESEARCH_BRIEF_MODEL_LABELS = {
    "gpt-5.6-luna": "GPT-5.6 Luna",
    "gpt-5.6-terra": "GPT-5.6 Terra",
    "gpt-5.6-sol": "GPT-5.6 Sol",
}
logger = logging.getLogger(__name__)
RESEARCH_BRIEF_JOB_SAFE_ERROR = "Research brief generation failed. Try again or reduce research depth."
RESEARCH_BRIEF_JOB_STALE_ERROR = "Research brief generation timed out. Please start a fresh draft."
RESEARCH_BRIEF_OPENAI_TIMEOUT_SECONDS = "RESEARCH_BRIEF_OPENAI_TIMEOUT_SECONDS"
RESEARCH_BRIEF_THUMBNAIL_TIMEOUT_SECONDS = "RESEARCH_BRIEF_THUMBNAIL_TIMEOUT_SECONDS"
RESEARCH_BRIEF_JOB_STALE_SECONDS = "RESEARCH_BRIEF_JOB_STALE_SECONDS"
RESEARCH_BRIEF_MODEL_DESCRIPTIONS = {
    "gpt-5.6-luna": "Fast / cheaper",
    "gpt-5.6-terra": "Balanced",
    "gpt-5.6-sol": "Deep research / highest quality",
}

ANGLE_OPTIONS = {
    "Full company DD",
    "Bull case",
    "Bear case",
    "Earnings setup",
    "Post-earnings review",
    "Momentum analysis",
    "Fundamental analysis",
    "Valuation analysis",
    "Technical setup",
    "Congress activity",
    "Insider activity",
    "Institutional activity",
    "Government contracts",
    "Macro or sector impact",
    "Peer comparison",
    "Custom",
}
TIME_HORIZON_OPTIONS = {"Near term", "3-6 months", "6-12 months", "Long term", "Custom"}
AUDIENCE_OPTIONS = {"General investors", "Active traders", "Long-term investors", "Professional / advanced", "Reddit DD", "Walnut Research Brief"}
JUDGMENT_OPTIONS = {"Let the data decide", "Bull case", "Bear case", "Balanced debate"}
LENGTH_OPTIONS = {"Short: 800-1,200 words", "Standard: 1,500-2,500 words", "Deep dive: 3,000-5,000 words"}
TONE_OPTIONS = {"Walnut market-native", "Institutional research", "Reddit DD", "Concise executive brief"}
EXTERNAL_RESEARCH_MODE_OPTIONS = {"Off", "Standard", "Deep"}
SECTION_FORMAT_OPTIONS = [
    "Walnut Research Brief",
    "Reddit DD - Issue / Risk / Data / Conclusion",
    "Reddit DD - Bull Case / Bear Case / The Data / The Call",
    "ValueInvesting - Business / Valuation / Risks / Margin of Safety",
    "X Thread",
    "Internal Analyst Note",
]
STATUS_OPTIONS = {"generating", "draft", "ready_for_review", "published", "unpublished", "failed"}
JUDGMENT_VALUES = {"bullish", "bearish", "mixed", "macro", "policy", "neutral"}
MAX_COMPARISON_TICKERS = 5
KEY_RESEARCH_FIELDS = [
    "revenue",
    "revenue growth",
    "guidance",
    "gross margin",
    "operating margin",
    "EBITDA / adjusted EBITDA",
    "free cash flow",
    "capex",
    "cash",
    "debt",
    "share count",
    "backlog / orders / RPO",
    "dilution / ATM / offering history",
    "major customer concentration",
    "government contracts",
    "reported institutional activity",
    "insider activity",
    "Congress activity",
    "price/volume and technicals",
    "peer comparison data",
]
DEFAULT_SECTIONS = [
    "Executive thesis",
    "What changed",
    "Business and fundamentals",
    "Valuation",
    "Price / volume and technicals",
    "Congress activity",
    "Insider activity",
    "Reported institutional activity",
    "Government contracts",
    "Options flow",
    "Catalysts",
    "Risks",
    "What to watch next",
    "Final Walnut judgment",
    "Data freshness and limitations",
]
CONFIRMATION_SCORE_SECTION_HEADING = "Walnut confirmation score"
CROSS_SOURCE_CONFIRMATIONS_SECTION_HEADING = "Cross-source confirmations"
PUBLISHED_STATIC_SLUGS = {"mu-dd"}
UNSUPPORTED_LANGUAGE = [
    "buy now",
    "better buy",
    "guaranteed return",
    "guaranteed returns",
    "beat the market",
    "risk-free",
    "can't lose",
    "will moon",
]
PUBLISH_COPY_FORBIDDEN_PATTERNS = [
    r"\bresearch request\b",
    r"\bsupplied research request\b",
    r"\bsupplied research context\b",
    r"\bsupplied materials?\b",
    r"\bsupplied context\b",
    r"\bsupplied q[1-4] figures?\b",
    r"\bresearch configuration\b",
    r"\bmarked available in the research configuration\b",
    r"\bprovided comparison confirmation\b",
    r"\bno reviewed consensus source was supplied\b",
    r"\breviewed materials do not provide\b",
    r"\bthis configuration\b",
    r"\bin this research configuration\b",
    r"\bpublication context\b",
    r"\buser request\b",
    r"\bmodel was asked\b",
    r"\bgenerated from\b",
    r"\bthe prompt\b",
    r"\bprompt\b",
]
PUBLISH_COPY_FORBIDDEN_RE = re.compile("|".join(PUBLISH_COPY_FORBIDDEN_PATTERNS), re.IGNORECASE)
MISSING_DATA_AWKWARD_RE = re.compile(
    r"\b(not supplied|was supplied|were supplied|no .* was supplied|reviewed materials do not provide|supplied materials|supplied context|research configuration)\b",
    re.IGNORECASE,
)
PLACEHOLDER_HEADINGS = {"intro", "hook", "intro / hook"}
SINGLETON_HEADINGS = {
    "the call": "The call",
    "sources": "Sources",
    "what to watch next": "What to watch next",
    "data freshness and limitations": "Data freshness and limitations",
}
REDDIT_BULL_BEAR_OUTLINE = [
    "Executive thesis",
    "Bull case",
    "Bear case",
    "The data",
    "The call",
    "What to watch next",
    "Sources",
    "Data freshness and limitations",
]

_STORE_LOCK = threading.Lock()
_ACTIVE_GENERATIONS: set[str] = set()
_JOB_WORKER_LOCK = threading.Lock()
_JOB_WORKERS: dict[str, threading.Thread] = {}


def research_brief_model(db: Session | None = None) -> str:
    configured = os.getenv(RESEARCH_BRIEF_MODEL_DEFAULT, "").strip() or os.getenv(RESEARCH_BRIEF_GENERATOR_MODEL, "").strip()
    if configured:
        return configured
    return DEFAULT_RESEARCH_BRIEF_MODEL


def research_brief_model_options(db: Session | None = None) -> list[str]:
    configured = [item.strip() for item in os.getenv(RESEARCH_BRIEF_MODEL_OPTIONS, "").split(",") if item.strip()]
    default = research_brief_model(db)
    options = configured or list(DEFAULT_RESEARCH_BRIEF_MODEL_OPTIONS)
    if default not in options:
        options.insert(0, default)
    return list(dict.fromkeys(options))


def research_brief_model_descriptions(db: Session | None = None) -> dict[str, str]:
    options = research_brief_model_options(db)
    labels = ["Fast / cheaper", "Balanced", "Deep research / highest quality"]
    return {model: RESEARCH_BRIEF_MODEL_DESCRIPTIONS.get(model) or labels[min(index, len(labels) - 1)] for index, model in enumerate(options)}


def research_brief_model_labels(db: Session | None = None) -> dict[str, str]:
    return {model: RESEARCH_BRIEF_MODEL_LABELS.get(model) or model for model in research_brief_model_options(db)}


def _selected_research_model(config: dict[str, Any], db: Session | None = None) -> str:
    options = research_brief_model_options(db)
    selected = str(config.get("selected_model") or "").strip()
    if selected:
        if selected not in options:
            raise HTTPException(status_code=422, detail="Selected research model is not configured for this environment.")
        return selected
    serious = (
        str(config.get("section_format") or "") != "X Thread"
        and str(config.get("intended_audience") or "") != "General investors"
        and (
            "valuation" in str(config.get("desired_angle") or "").lower()
            or "dcf" in str(config.get("research_question") or "").lower()
            or "dd" in str(config.get("section_format") or "").lower()
            or str(config.get("intended_audience") or "") in {"Reddit DD", "Walnut Research Brief", "Professional / advanced"}
        )
    )
    return options[-1] if serious else research_brief_model(db)


def draft_store_path() -> Path:
    configured = os.getenv(STORE_ENV, "").strip()
    if configured:
        return Path(configured)
    return Path(__file__).resolve().parents[2] / ".local" / "research_brief_drafts.json"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _iso(value: Any) -> str | None:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat() if value.tzinfo else value.replace(tzinfo=timezone.utc).isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


def _load_json(value: str | None) -> Any:
    if not value:
        return None
    try:
        return json.loads(value)
    except Exception:
        return None


def _compact(value: Any, *, limit: int = 5000) -> Any:
    if isinstance(value, dict):
        return {str(k): _compact(v, limit=limit) for k, v in value.items() if not _is_internal_key(str(k))}
    if isinstance(value, list):
        return [_compact(item, limit=limit) for item in value[:25]]
    if isinstance(value, str):
        return value[:limit]
    return value


def _is_internal_key(key: str) -> bool:
    lowered = key.lower()
    return any(term in lowered for term in ("provider", "cache", "secret", "token", "credential", "diagnostic", "raw"))


def sanitize_research_brief_copy(markdown: str) -> str:
    text = str(markdown or "")
    if not text:
        return ""
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    repaired_blocks: list[str] = []
    for block in re.split(r"(\n{2,})", text):
        if not block or block.startswith("\n"):
            repaired_blocks.append(block)
            continue
        repaired_blocks.append(_sanitize_copy_block(block))
    cleaned = "".join(repaired_blocks)
    cleaned = re.sub(r"[ \t]+\n", "\n", cleaned)
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
    return cleaned.strip()


def _sanitize_copy_block(block: str) -> str:
    if re.search(r"(?m)^#{1,6}\s+", block):
        lines = block.splitlines()
        output: list[str] = []
        pending: list[str] = []

        def flush_pending() -> None:
            if not pending:
                return
            cleaned = _sanitize_copy_text_block("\n".join(pending))
            if cleaned:
                output.append(cleaned)
            pending.clear()

        for line in lines:
            if re.match(r"^#{1,6}\s+", line):
                flush_pending()
                output.append(line)
            else:
                pending.append(line)
        flush_pending()
        return "\n".join(output).strip()
    return _sanitize_copy_text_block(block)


def _sanitize_copy_text_block(block: str) -> str:
    if block.lstrip().startswith("|"):
        return "" if PUBLISH_COPY_FORBIDDEN_RE.search(block) else block
    pieces = re.split(r"(?<=[.!?])(\s+)", block)
    output: list[str] = []
    index = 0
    while index < len(pieces):
        sentence = pieces[index]
        separator = pieces[index + 1] if index + 1 < len(pieces) else ""
        replacement = _rewrite_internal_workflow_sentence(sentence)
        if replacement is not None:
            if replacement and replacement not in output:
                output.append(replacement)
                output.append(separator)
        else:
            output.append(_rewrite_internal_phrases(sentence))
            output.append(separator)
        index += 2
    return "".join(output).strip()


def _rewrite_internal_workflow_sentence(sentence: str) -> str | None:
    lowered = sentence.lower()
    if not PUBLISH_COPY_FORBIDDEN_RE.search(sentence):
        return None
    if "eps" in lowered and any(term in lowered for term in ("estimate", "consensus")):
        return "Current EPS consensus estimates were not verified in reviewed sources, so they are omitted from the analysis."
    if "revenue" in lowered and any(term in lowered for term in ("estimate", "consensus")):
        return "Current revenue consensus estimates were not verified in reviewed sources, so they are omitted from the analysis."
    if "price" in lowered and ("volume" in lowered or "technical" in lowered):
        return "Current Walnut data does not provide enough ticker-specific price/volume detail to support a full technical read, so price action is not central to this brief."
    if "tax" in lowered:
        return "Diluted EPS should be read with caution if one-time tax items affected the quarter."
    if "guidance" in lowered:
        return "Current guidance was not verified in reviewed sources, so the analysis focuses on reported results and the questions management needs to answer."
    if "options" in lowered or "implied move" in lowered:
        return "Options flow is omitted because no reliable options-implied move was verified in reviewed sources."
    if "congress" in lowered or "insider" in lowered:
        return "No ticker-specific Congress or insider activity was material enough to affect this setup."
    if "q1" in lowered and ("figures" in lowered or "operating" in lowered):
        return "Q1 operating figures should be read alongside official earnings materials and SEC filings."
    return ""


def _rewrite_internal_phrases(sentence: str) -> str:
    replacements = {
        r"\breferenced in the research request\b": "not independently verified",
        r"\bfrom the supplied research context\b": "from reviewed Walnut data",
        r"\bfrom supplied research context\b": "from reviewed Walnut data",
        r"\bsupplied research context\b": "reviewed Walnut data",
        r"\bsupplied materials?\b": "reviewed sources",
        r"\bsupplied context\b": "reviewed Walnut data",
        r"\bavailable Walnut context\b": "available Walnut data",
        r"\bWalnut context\b": "Walnut data",
        r"\bresearch configuration\b": "reviewed data",
        r"\bprovided comparison confirmation\b": "comparison data",
        r"\bno reviewed consensus source was supplied\b": "no reliable consensus source was verified",
        r"\breviewed materials do not provide\b": "reviewed sources did not verify",
    }
    rewritten = sentence
    for pattern, replacement in replacements.items():
        rewritten = re.sub(pattern, replacement, rewritten, flags=re.IGNORECASE)
    return rewritten


def clean_research_brief_markdown(markdown: str, section_format: str, section_heading: str | None = None) -> str:
    text = sanitize_research_brief_copy(markdown)
    if not text:
        return ""
    text = _remove_redundant_leading_heading(text, section_heading)
    parts = re.split(r"(?m)^(##\s+.+?)\s*$", text)
    if len(parts) == 1:
        return text.strip()
    intro = parts[0].strip()
    sections: list[dict[str, str]] = []
    pending_empty_heading: str | None = None
    for index in range(1, len(parts), 2):
        heading = re.sub(r"^##\s+", "", parts[index]).strip()
        body = parts[index + 1].strip() if index + 1 < len(parts) else ""
        normalized = _canonical_heading(heading)
        if _is_placeholder_heading(normalized):
            if body:
                intro = "\n\n".join(part for part in [intro, body] if part).strip()
            pending_empty_heading = None
            continue
        if not body:
            pending_empty_heading = normalized
            continue
        if pending_empty_heading:
            normalized = pending_empty_heading
            pending_empty_heading = None
        if sections and _heading_key(sections[-1]["heading"]) == _heading_key(normalized):
            sections[-1]["body"] = _merge_markdown_bodies(sections[-1]["body"], body)
            continue
        sections.append({"heading": normalized, "body": body})
    sections = _merge_singleton_sections(sections)
    if section_format == "Reddit DD - Bull Case / Bear Case / The Data / The Call":
        sections = _order_outline_sections(sections, REDDIT_BULL_BEAR_OUTLINE)
        intro = _merge_intro_into_first_section(intro, sections)
    output: list[str] = []
    if intro:
        output.append(intro)
    for section in sections:
        output.append(f"## {section['heading']}\n\n{section['body'].strip()}")
    return "\n\n".join(output).strip()


def _remove_redundant_leading_heading(markdown: str, section_heading: str | None) -> str:
    if not section_heading:
        return markdown
    match = re.match(r"^\s*##\s+(.+?)\s*\n+", markdown)
    if not match:
        return markdown
    if _heading_key(match.group(1)) == _heading_key(section_heading):
        return markdown[match.end() :].strip()
    return markdown


def _canonical_heading(heading: str) -> str:
    cleaned = re.sub(r"\s+", " ", str(heading or "").strip().strip("#")).strip()
    key = _heading_key(cleaned)
    for singleton_key, canonical in SINGLETON_HEADINGS.items():
        if key == singleton_key or key.startswith(f"{singleton_key}:"):
            return canonical
    return cleaned


def _heading_key(heading: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", str(heading or "").lower()).strip()


def _is_placeholder_heading(heading: str) -> bool:
    return _heading_key(heading) in PLACEHOLDER_HEADINGS


def _merge_markdown_bodies(left: str, right: str) -> str:
    lines: list[str] = []
    seen = set()
    for line in [*str(left or "").splitlines(), "", *str(right or "").splitlines()]:
        key = line.strip()
        if key and key in seen:
            continue
        if key:
            seen.add(key)
        lines.append(line)
    return re.sub(r"\n{3,}", "\n\n", "\n".join(lines)).strip()


def _merge_singleton_sections(sections: list[dict[str, str]]) -> list[dict[str, str]]:
    merged: list[dict[str, str]] = []
    singleton_indexes: dict[str, int] = {}
    for section in sections:
        key = _heading_key(section["heading"])
        if key in SINGLETON_HEADINGS:
            section["heading"] = SINGLETON_HEADINGS[key]
            if key in singleton_indexes:
                target = merged[singleton_indexes[key]]
                target["body"] = _merge_markdown_bodies(target["body"], section["body"])
                continue
            singleton_indexes[key] = len(merged)
        merged.append(section)
    return merged


def _order_outline_sections(sections: list[dict[str, str]], outline: list[str]) -> list[dict[str, str]]:
    order = {_heading_key(heading): index for index, heading in enumerate(outline)}
    return [item[1] for item in sorted(enumerate(sections), key=lambda item: (order.get(_heading_key(item[1]["heading"]), len(order) + item[0]), item[0]))]


def _merge_intro_into_first_section(intro: str, sections: list[dict[str, str]]) -> str:
    if intro and sections:
        sections[0]["body"] = _merge_markdown_bodies(intro, sections[0]["body"])
        return ""
    return intro


def sanitize_research_brief_article(article: dict[str, Any], config: dict[str, Any], context: dict[str, Any] | None = None) -> dict[str, Any]:
    section_format = str(config.get("section_format") or "Walnut Research Brief")
    sanitized = deepcopy(article)
    before = json.dumps(sanitized, sort_keys=True, default=str)
    for key in ("title", "subtitle", "summary", "preview_body"):
        if isinstance(sanitized.get(key), str):
            sanitized[key] = sanitize_research_brief_copy(sanitized[key]).lstrip("# ").strip()
    for key in ("key_points", "catalysts", "risks", "watch_items", "data_freshness", "missing_data_notes"):
        if isinstance(sanitized.get(key), list):
            sanitized[key] = _dedupe_strings([sanitize_research_brief_copy(str(item)) for item in sanitized[key]])
    sections = sanitized.get("sections") if isinstance(sanitized.get("sections"), list) else []
    cleaned_sections: list[dict[str, Any]] = []
    for index, section in enumerate(sections):
        if not isinstance(section, dict):
            continue
        heading = sanitize_research_brief_copy(str(section.get("heading") or "")).lstrip("# ").strip() or f"Section {index + 1}"
        heading = _canonical_heading(heading)
        if _is_placeholder_heading(heading):
            heading = "Executive thesis"
        body = clean_research_brief_markdown(str(section.get("body_markdown") or ""), section_format, section_heading=heading)
        if not body:
            continue
        cleaned_sections.extend(_article_sections_from_clean_markdown(body, heading, section, index))
    sanitized["sections"] = _merge_article_sections(cleaned_sections, section_format)
    sanitized = _apply_confirmation_preferences(sanitized, config, context or {})
    after = json.dumps(sanitized, sort_keys=True, default=str)
    if after != before:
        sanitized["_copy_sanitizer_repairs"] = 1 + int(sanitized.get("_copy_sanitizer_repairs") or 0)
    return sanitized


def _article_sections_from_clean_markdown(body: str, fallback_heading: str, source_section: dict[str, Any], index: int) -> list[dict[str, Any]]:
    parts = re.split(r"(?m)^(##\s+.+?)\s*$", body)
    if len(parts) == 1:
        return [
            {
                **source_section,
                "heading": fallback_heading,
                "key": str(source_section.get("key") or _slugify(fallback_heading, fallback=f"section-{index + 1}")),
                "body_markdown": body.strip(),
            }
        ]
    sections: list[dict[str, Any]] = []
    intro = parts[0].strip()
    if intro:
        sections.append(
            {
                **source_section,
                "heading": fallback_heading,
                "key": str(source_section.get("key") or _slugify(fallback_heading, fallback=f"section-{index + 1}")),
                "body_markdown": intro,
            }
        )
    for part_index in range(1, len(parts), 2):
        heading = _canonical_heading(re.sub(r"^##\s+", "", parts[part_index]).strip())
        section_body = parts[part_index + 1].strip() if part_index + 1 < len(parts) else ""
        if not section_body:
            continue
        sections.append(
            {
                **source_section,
                "heading": heading,
                "key": _slugify(heading, fallback=f"section-{index + 1}-{part_index}"),
                "body_markdown": section_body,
            }
        )
    return sections


def _merge_article_sections(sections: list[dict[str, Any]], section_format: str) -> list[dict[str, Any]]:
    merged: list[dict[str, Any]] = []
    by_heading: dict[str, int] = {}
    for section in sections:
        heading = _canonical_heading(str(section.get("heading") or ""))
        key = _heading_key(heading)
        if key in by_heading:
            existing = merged[by_heading[key]]
            existing["body_markdown"] = _merge_markdown_bodies(str(existing.get("body_markdown") or ""), str(section.get("body_markdown") or ""))
            continue
        section["heading"] = heading
        by_heading[key] = len(merged)
        merged.append(section)
    if section_format == "Reddit DD - Bull Case / Bear Case / The Data / The Call":
        order = {_heading_key(heading): index for index, heading in enumerate(REDDIT_BULL_BEAR_OUTLINE)}
        merged = [item[1] for item in sorted(enumerate(merged), key=lambda item: (order.get(_heading_key(item[1].get("heading")), len(order) + item[0]), item[0]))]
    return merged


def _apply_confirmation_preferences(article: dict[str, Any], config: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
    include_score = bool(config.get("include_confirmation_score"))
    include_cross_source = bool(config.get("include_cross_source_confirmations"))
    sanitized = deepcopy(article)
    sections = sanitized.get("sections") if isinstance(sanitized.get("sections"), list) else []
    sections = [_strip_confirmation_content_from_section(section, include_score=include_score, include_cross_source=include_cross_source) for section in sections if isinstance(section, dict)]
    sections = [section for section in sections if str(section.get("body_markdown") or "").strip()]
    sanitized["sections"] = sections

    if include_score:
        score_text = _confirmation_score_sentence(context)
        if score_text and not _article_mentions_confirmation_score(sanitized):
            sanitized["sections"] = _append_or_merge_generated_section(
                sanitized["sections"],
                CONFIRMATION_SCORE_SECTION_HEADING,
                score_text,
                key="walnut_confirmation_score",
            )
    if include_cross_source:
        commentary = _cross_source_confirmation_commentary(context)
        if commentary and not _article_mentions_cross_source_confirmations(sanitized):
            sanitized["sections"] = _append_or_merge_generated_section(
                sanitized["sections"],
                CROSS_SOURCE_CONFIRMATIONS_SECTION_HEADING,
                commentary,
                key="cross_source_confirmations",
            )
    return sanitized


def _strip_confirmation_content_from_section(section: dict[str, Any], *, include_score: bool, include_cross_source: bool) -> dict[str, Any]:
    cleaned = dict(section)
    heading = str(cleaned.get("heading") or "")
    heading_key = _heading_key(heading)
    body = str(cleaned.get("body_markdown") or "")
    if not include_score and ("confirmation score" in heading_key or "walnut confirmation score" in heading_key):
        cleaned["body_markdown"] = ""
        return cleaned
    if not include_cross_source and ("cross source confirmation" in heading_key or "cross source confirmations" in heading_key):
        cleaned["body_markdown"] = ""
        return cleaned
    if not include_score:
        body = _remove_sentences_matching(body, r"\bconfirmation score\b")
    if not include_cross_source:
        body = _remove_sentences_matching(body, r"\bcross-source confirmations?\b|\bcross source confirmations?\b")
    cleaned["body_markdown"] = body.strip()
    return cleaned


def _remove_sentences_matching(text: str, pattern: str) -> str:
    blocks: list[str] = []
    for block in re.split(r"(\n{2,})", str(text or "")):
        if not block or block.startswith("\n"):
            blocks.append(block)
            continue
        if block.lstrip().startswith(("- ", "* ", "1. ")):
            lines = [line for line in block.splitlines() if not re.search(pattern, line, flags=re.IGNORECASE)]
            blocks.append("\n".join(lines))
            continue
        pieces = re.split(r"(?<=[.!?])(\s+)", block)
        kept: list[str] = []
        index = 0
        while index < len(pieces):
            sentence = pieces[index]
            separator = pieces[index + 1] if index + 1 < len(pieces) else ""
            if not re.search(pattern, sentence, flags=re.IGNORECASE):
                kept.append(sentence)
                kept.append(separator)
            index += 2
        blocks.append("".join(kept).strip())
    return re.sub(r"\n{3,}", "\n\n", "".join(blocks)).strip()


def _append_or_merge_generated_section(sections: list[dict[str, Any]], heading: str, body: str, *, key: str) -> list[dict[str, Any]]:
    next_sections = [dict(section) for section in sections]
    target_key = _heading_key(heading)
    for section in next_sections:
        if _heading_key(str(section.get("heading") or "")) == target_key:
            section["body_markdown"] = _merge_markdown_bodies(str(section.get("body_markdown") or ""), body)
            return next_sections
    next_sections.append({"key": key, "heading": heading, "body_markdown": body})
    return next_sections


def _primary_confirmation(context: dict[str, Any]) -> dict[str, Any]:
    primary = context.get("primary") if isinstance(context.get("primary"), dict) else {}
    confirmation = primary.get("confirmation") if isinstance(primary.get("confirmation"), dict) else {}
    return confirmation


def _confirmation_score_value(context: dict[str, Any]) -> int | None:
    confirmation = _primary_confirmation(context)
    value = confirmation.get("score", confirmation.get("confirmation_score"))
    try:
        score = int(round(float(value)))
    except (TypeError, ValueError):
        return None
    return score if score > 0 else None


def _confirmation_score_sentence(context: dict[str, Any]) -> str:
    score = _confirmation_score_value(context)
    if score is None:
        return ""
    confirmation = _primary_confirmation(context)
    direction = str(confirmation.get("direction") or confirmation.get("confirmation_direction") or "").strip().lower()
    direction_text = f" The score direction is {direction}." if direction in {"bullish", "bearish", "neutral", "mixed"} else ""
    return f"Walnut's proprietary confirmation score is {score}/100.{direction_text} This score is separate from the underlying data."


def _cross_source_confirmation_commentary(context: dict[str, Any]) -> str:
    confirmation = _primary_confirmation(context)
    sources = confirmation.get("sources") if isinstance(confirmation.get("sources"), dict) else {}
    supported: list[str] = []
    contradicted: list[str] = []
    mixed: list[str] = []
    for key, source in sources.items():
        if not isinstance(source, dict) or source.get("present") is False or source.get("locked") is True:
            continue
        label = _confirmation_source_label(str(key))
        direction = str(source.get("direction") or "").lower()
        if direction == "bullish":
            supported.append(label)
        elif direction == "bearish":
            contradicted.append(label)
        elif direction == "mixed":
            mixed.append(label)
    clauses: list[str] = []
    if supported:
        clauses.append(f"supported by {', '.join(supported[:4])}")
    if contradicted:
        clauses.append(f"contradicted by {', '.join(contradicted[:4])}")
    if mixed:
        clauses.append(f"mixed in {', '.join(mixed[:4])}")
    if not clauses:
        return "Cross-source confirmations are not strong enough to change the thesis; no single data category should be treated as decisive."
    return f"The setup is {', and '.join(clauses)}. These are underlying data categories and should be read qualitatively."


def _confirmation_source_label(key: str) -> str:
    labels = {
        "price_volume": "price/volume",
        "fundamentals": "fundamentals",
        "institutional_activity": "reported institutional activity",
        "congress": "Congress activity",
        "insiders": "insider activity",
        "government_contracts": "government contracts",
        "options_flow": "options flow",
        "macro_positioning": "macro positioning",
        "signals": "signal data",
    }
    return labels.get(key, key.replace("_", " "))


def _article_body_text(article: dict[str, Any]) -> str:
    sections = article.get("sections") if isinstance(article.get("sections"), list) else []
    return "\n\n".join(
        f"{section.get('heading') or ''}\n{section.get('body_markdown') or ''}"
        for section in sections
        if isinstance(section, dict)
    )


def _article_mentions_confirmation_score(article: dict[str, Any]) -> bool:
    return bool(re.search(r"\bconfirmation score\b", _article_body_text(article), flags=re.IGNORECASE))


def _article_mentions_cross_source_confirmations(article: dict[str, Any]) -> bool:
    return bool(re.search(r"\bcross[- ]source confirmations?\b", _article_body_text(article), flags=re.IGNORECASE))


def _read_store() -> dict[str, Any]:
    path = draft_store_path()
    if not path.exists():
        return {"drafts": [], "audit": [], "jobs": []}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {"drafts": [], "audit": [], "jobs": []}
    if not isinstance(payload, dict):
        return {"drafts": [], "audit": [], "jobs": []}
    drafts = payload.get("drafts") if isinstance(payload.get("drafts"), list) else []
    audit = payload.get("audit") if isinstance(payload.get("audit"), list) else []
    jobs = payload.get("jobs") if isinstance(payload.get("jobs"), list) else []
    return {"drafts": drafts, "audit": audit, "jobs": jobs}


def _write_store(payload: dict[str, Any]) -> None:
    path = draft_store_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    tmp.replace(path)


def _json_dump(value: Any) -> str:
    return json.dumps(value, sort_keys=True, default=str)


def ensure_research_brief_store_schema(db: Session) -> None:
    db.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS research_brief_generation_jobs (
                id TEXT PRIMARY KEY,
                status TEXT NOT NULL,
                client_request_id TEXT,
                created_by_admin_id INTEGER,
                created_by_admin_email TEXT,
                ticker TEXT,
                request_payload_json TEXT,
                model TEXT,
                external_research_mode TEXT,
                section_format TEXT,
                generate_thumbnail BOOLEAN,
                progress_step TEXT,
                progress_message TEXT,
                source_links_count INTEGER DEFAULT 0,
                numeric_claims_count INTEGER DEFAULT 0,
                validation_status TEXT,
                draft_id TEXT,
                draft_payload_json TEXT,
                error_message_safe TEXT,
                error_details_internal TEXT,
                duration_ms INTEGER,
                created_at TEXT,
                started_at TEXT,
                updated_at TEXT,
                completed_at TEXT,
                failed_at TEXT
            )
            """
        )
    )
    db.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS research_brief_drafts (
                id TEXT PRIMARY KEY,
                status TEXT,
                created_by INTEGER,
                primary_ticker TEXT,
                slug TEXT,
                updated_at TEXT,
                published_at TEXT,
                payload_json TEXT NOT NULL
            )
            """
        )
    )
    db.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS ix_research_brief_jobs_admin_request ON research_brief_generation_jobs (created_by_admin_id, client_request_id)"))
    db.execute(text("CREATE INDEX IF NOT EXISTS ix_research_brief_jobs_status_created ON research_brief_generation_jobs (status, created_at)"))
    db.execute(text("CREATE INDEX IF NOT EXISTS ix_research_brief_drafts_status_updated ON research_brief_drafts (status, updated_at)"))
    try:
        db.execute(text("ALTER TABLE research_brief_generation_jobs ADD COLUMN IF NOT EXISTS updated_at TEXT"))
    except Exception:
        try:
            db.execute(text("ALTER TABLE research_brief_generation_jobs ADD COLUMN updated_at TEXT"))
        except Exception:
            pass
    db.commit()


def _job_from_row(row: Any) -> dict[str, Any]:
    data = dict(row or {})
    data["request_payload_json"] = _load_json(data.get("request_payload_json")) or {}
    data["draft_payload_json"] = _load_json(data.get("draft_payload_json")) if data.get("draft_payload_json") else None
    data["generate_thumbnail"] = bool(data.get("generate_thumbnail"))
    return data


def _db_job(db: Session, job_id: str) -> dict[str, Any] | None:
    ensure_research_brief_store_schema(db)
    row = db.execute(text("SELECT * FROM research_brief_generation_jobs WHERE id = :id"), {"id": job_id}).mappings().first()
    return _job_from_row(row) if row else None


def _upsert_db_job(db: Session, job: dict[str, Any]) -> None:
    ensure_research_brief_store_schema(db)
    defaults = {
        "source_links_count": 0,
        "numeric_claims_count": 0,
        "validation_status": None,
        "draft_id": None,
        "draft_payload_json": None,
        "error_message_safe": None,
        "error_details_internal": None,
        "duration_ms": None,
        "started_at": None,
        "updated_at": None,
        "completed_at": None,
        "failed_at": None,
    }
    normalized_job = {**defaults, **job}
    normalized_job["updated_at"] = _now()
    params = {
        **normalized_job,
        "request_payload_json": _json_dump(normalized_job.get("request_payload_json") or {}),
        "draft_payload_json": _json_dump(normalized_job.get("draft_payload_json")) if normalized_job.get("draft_payload_json") else None,
        "generate_thumbnail": bool(normalized_job.get("generate_thumbnail")),
    }
    db.execute(
        text(
            """
            INSERT INTO research_brief_generation_jobs (
                id, status, client_request_id, created_by_admin_id, created_by_admin_email, ticker,
                request_payload_json, model, external_research_mode, section_format, generate_thumbnail,
                progress_step, progress_message, source_links_count, numeric_claims_count, validation_status,
                draft_id, draft_payload_json, error_message_safe, error_details_internal, duration_ms,
                created_at, started_at, updated_at, completed_at, failed_at
            ) VALUES (
                :id, :status, :client_request_id, :created_by_admin_id, :created_by_admin_email, :ticker,
                :request_payload_json, :model, :external_research_mode, :section_format, :generate_thumbnail,
                :progress_step, :progress_message, :source_links_count, :numeric_claims_count, :validation_status,
                :draft_id, :draft_payload_json, :error_message_safe, :error_details_internal, :duration_ms,
                :created_at, :started_at, :updated_at, :completed_at, :failed_at
            )
            ON CONFLICT(id) DO UPDATE SET
                status = excluded.status,
                progress_step = excluded.progress_step,
                progress_message = excluded.progress_message,
                source_links_count = excluded.source_links_count,
                numeric_claims_count = excluded.numeric_claims_count,
                validation_status = excluded.validation_status,
                draft_id = excluded.draft_id,
                draft_payload_json = excluded.draft_payload_json,
                error_message_safe = excluded.error_message_safe,
                error_details_internal = excluded.error_details_internal,
                duration_ms = excluded.duration_ms,
                started_at = excluded.started_at,
                updated_at = excluded.updated_at,
                completed_at = excluded.completed_at,
                failed_at = excluded.failed_at
            """
        ),
        params,
    )
    db.commit()


def _upsert_db_draft(db: Session, draft: dict[str, Any]) -> None:
    ensure_research_brief_store_schema(db)
    article = draft.get("article") or {}
    params = {
        "id": draft.get("id"),
        "status": draft.get("status"),
        "created_by": draft.get("created_by"),
        "primary_ticker": draft.get("primary_ticker"),
        "slug": article.get("slug"),
        "updated_at": draft.get("updated_at"),
        "published_at": draft.get("published_at"),
        "payload_json": _json_dump(draft),
    }
    db.execute(
        text(
            """
            INSERT INTO research_brief_drafts (id, status, created_by, primary_ticker, slug, updated_at, published_at, payload_json)
            VALUES (:id, :status, :created_by, :primary_ticker, :slug, :updated_at, :published_at, :payload_json)
            ON CONFLICT(id) DO UPDATE SET
                status = excluded.status,
                primary_ticker = excluded.primary_ticker,
                slug = excluded.slug,
                updated_at = excluded.updated_at,
                published_at = excluded.published_at,
                payload_json = excluded.payload_json
            """
        ),
        params,
    )
    db.commit()


def _db_draft(db: Session, draft_id: str) -> dict[str, Any] | None:
    ensure_research_brief_store_schema(db)
    row = db.execute(text("SELECT payload_json FROM research_brief_drafts WHERE id = :id"), {"id": draft_id}).mappings().first()
    payload = _load_json(row["payload_json"]) if row else None
    return payload if isinstance(payload, dict) else None


def _db_drafts(db: Session, status: str | None = None) -> list[dict[str, Any]]:
    ensure_research_brief_store_schema(db)
    if status and status != "all":
        rows = db.execute(text("SELECT payload_json FROM research_brief_drafts WHERE status = :status ORDER BY updated_at DESC"), {"status": status}).mappings().all()
    else:
        rows = db.execute(text("SELECT payload_json FROM research_brief_drafts ORDER BY updated_at DESC")).mappings().all()
    drafts: list[dict[str, Any]] = []
    for row in rows:
        payload = _load_json(row["payload_json"])
        if isinstance(payload, dict):
            drafts.append(payload)
    return drafts


def _append_audit(store: dict[str, Any], *, action: str, admin: UserAccount, draft_id: str | None, metadata: dict[str, Any] | None = None) -> None:
    audit = store.setdefault("audit", [])
    audit.append(
        {
            "action": action,
            "draft_id": draft_id,
            "admin_id": admin.id,
            "admin_email": getattr(admin, "email", None),
            "at": _now(),
            "metadata": metadata or {},
        }
    )
    del audit[:-250]


def _slugify(value: str, fallback: str) -> str:
    raw = (value or fallback).strip().lower()
    raw = raw.replace("$", "")
    slug = re.sub(r"[^a-z0-9]+", "-", raw).strip("-")
    return slug[:96] or fallback.lower()


def normalize_supported_symbol(db: Session, raw: str | None) -> tuple[str, dict[str, Any]]:
    symbol = normalize_symbol(raw) if raw else None
    if not symbol:
        raise HTTPException(status_code=422, detail="Ticker symbol is required.")
    meta = db.get(TickerMeta, symbol)
    security = db.execute(select(Security).where(func.upper(Security.symbol) == symbol)).scalar_one_or_none()
    fundamentals = (
        db.execute(
            select(FundamentalsCache)
            .where(func.upper(FundamentalsCache.symbol) == symbol)
            .order_by(desc(FundamentalsCache.fetched_at))
            .limit(1)
        ).scalar_one_or_none()
    )
    quote = db.get(QuoteCache, symbol)
    if not any([meta, security, fundamentals, quote]):
        raise HTTPException(status_code=422, detail=f"{symbol} is not a supported Walnut ticker yet.")
    identity = {
        "symbol": symbol,
        "company_name": (meta.company_name if meta else None) or (fundamentals.company_name if fundamentals else None) or (security.name if security else None) or symbol,
        "exchange": (meta.exchange if meta else None) or (fundamentals.exchange if fundamentals else None),
        "sector": (meta.sector if meta else None) or (fundamentals.sector if fundamentals else None) or (security.sector if security else None),
        "industry": (meta.industry if meta else None) or (fundamentals.industry if fundamentals else None),
        "country": (meta.country if meta else None) or (fundamentals.country if fundamentals else None),
    }
    return symbol, identity


def normalize_comparison_tickers(config: dict[str, Any], *, primary_ticker: str | None = None) -> list[str]:
    values: list[Any] = []
    raw_list = config.get("comparison_tickers")
    if isinstance(raw_list, list):
        values.extend(raw_list)
    elif raw_list:
        values.append(raw_list)
    if config.get("comparison_ticker"):
        values.append(config.get("comparison_ticker"))

    symbols: list[str] = []
    seen: set[str] = set()
    for value in values:
        for part in str(value or "").split(","):
            symbol = normalize_symbol(part)
            if not symbol or symbol in seen:
                continue
            symbols.append(symbol)
            seen.add(symbol)

    if len(symbols) > MAX_COMPARISON_TICKERS:
        raise HTTPException(status_code=422, detail=f"Comparison tickers are limited to {MAX_COMPARISON_TICKERS} symbols.")

    primary = normalize_symbol(primary_ticker)
    if primary and primary in seen:
        raise HTTPException(status_code=422, detail="Primary ticker cannot appear in comparison tickers.")

    return symbols


def _latest_fundamentals(db: Session, symbol: str) -> dict[str, Any] | None:
    row = (
        db.execute(
            select(FundamentalsCache)
            .where(func.upper(FundamentalsCache.symbol) == symbol)
            .order_by(desc(FundamentalsCache.fetched_at))
            .limit(1)
        ).scalar_one_or_none()
    )
    if not row:
        return None
    return {
        "as_of": _iso(row.fetched_at),
        "period_date": _iso(row.period_date),
        "status": row.status,
        "market_cap": row.market_cap,
        "price": row.price,
        "volume": row.volume,
        "avg_volume": row.avg_volume,
        "revenue_growth": row.revenue_growth,
        "eps_growth": row.eps_growth,
        "gross_margin": row.gross_margin,
        "operating_margin": row.operating_margin,
        "net_margin": row.net_margin,
        "roe": row.roe,
        "roic": row.roic,
        "trailing_pe": row.trailing_pe,
        "forward_pe": row.forward_pe,
        "price_to_sales": row.price_to_sales,
        "ev_to_ebitda": row.ev_to_ebitda,
        "debt_to_equity": row.debt_to_equity,
        "net_debt_to_ebitda": row.net_debt_to_ebitda,
        "free_cash_flow": row.free_cash_flow,
        "fcf_yield": row.fcf_yield,
        "eps_ttm": row.eps_ttm,
    }


def _quote(db: Session, symbol: str) -> dict[str, Any] | None:
    row = db.get(QuoteCache, symbol)
    if not row:
        return None
    return {"price": row.price, "market_cap": row.market_cap, "as_of": _iso(row.asof_ts)}


def _cached_financials_snapshot(db: Session, symbol: str) -> dict[str, Any] | None:
    row = (
        db.execute(
            select(TickerFinancialsCache)
            .where(func.upper(TickerFinancialsCache.symbol) == symbol)
            .order_by(desc(TickerFinancialsCache.fetched_at))
            .limit(1)
        ).scalar_one_or_none()
    )
    if not row:
        return None
    payload = _load_json(row.payload_json)
    if not isinstance(payload, dict):
        return None
    subsections = payload.get("subsections") if isinstance(payload.get("subsections"), dict) else {}
    sections = payload.get("sections") if isinstance(payload.get("sections"), dict) else {}
    analyst_estimates = subsections.get("analyst_estimates") if isinstance(subsections.get("analyst_estimates"), dict) else {}
    valuation = subsections.get("valuation") if isinstance(subsections.get("valuation"), dict) else {}
    income = subsections.get("income") if isinstance(subsections.get("income"), dict) else {}
    cash_flow = subsections.get("cash_flow") if isinstance(subsections.get("cash_flow"), dict) else {}
    health = subsections.get("health") if isinstance(subsections.get("health"), dict) else {}
    earnings_subsection = subsections.get("earnings") if isinstance(subsections.get("earnings"), dict) else {}
    return _compact(
        {
            "status": payload.get("status") or row.status,
            "as_of": _iso(row.fetched_at),
            "latest_quarter": (payload.get("summary") or {}).get("latestQuarter") if isinstance(payload.get("summary"), dict) else None,
            "forecasts": payload.get("forecasts") or analyst_estimates.get("data") or sections.get("analyst_estimates"),
            "earnings": payload.get("earnings") or sections.get("earnings") or earnings_subsection.get("data"),
            "valuation": valuation.get("data") or sections.get("valuation") or payload.get("valuation_metrics"),
            "income": income.get("data") or sections.get("income"),
            "cash_flow": cash_flow.get("data") or sections.get("cashFlow"),
            "health": health.get("data") or sections.get("health"),
        },
        limit=2500,
    )


def _recent_events(db: Session, symbol: str, event_types: list[str], *, limit: int = 8) -> list[dict[str, Any]]:
    rows = (
        db.execute(
            select(Event)
            .where(func.upper(Event.symbol) == symbol)
            .where(Event.event_type.in_(event_types))
            .order_by(desc(Event.ts))
            .limit(limit)
        )
        .scalars()
        .all()
    )
    items: list[dict[str, Any]] = []
    for row in rows:
        payload = _load_json(row.payload_json) or {}
        items.append(
            {
                "event_type": row.event_type,
                "date": _iso(row.event_date or row.ts),
                "member_name": row.member_name,
                "trade_type": row.trade_type or row.transaction_type,
                "amount_range_min": row.amount_min,
                "amount_range_max": row.amount_max,
                "title": payload.get("title") or payload.get("headline"),
                "summary": payload.get("summary") or payload.get("description"),
                "source_document_url": row.source_document_url,
            }
        )
    return items


def _government_contracts(db: Session, symbol: str) -> dict[str, Any]:
    rows = (
        db.execute(
            select(GovernmentContract)
            .where(func.upper(GovernmentContract.symbol) == symbol)
            .order_by(desc(GovernmentContract.award_date))
            .limit(8)
        )
        .scalars()
        .all()
    )
    total = sum(float(row.award_amount or 0) for row in rows)
    return {
        "recent_count": len(rows),
        "recent_award_amount": total,
        "items": [
            {
                "award_date": _iso(row.award_date),
                "award_amount": row.award_amount,
                "awarding_agency": row.awarding_agency,
                "description": row.description,
                "source_url": row.source_url,
            }
            for row in rows
        ],
    }


def _has_value(mapping: Any, keys: list[str]) -> bool:
    if not isinstance(mapping, dict):
        return False
    for key in keys:
        value = mapping.get(key)
        if value is not None and value != "":
            return True
    return False


def _has_nested_value(mapping: Any, paths: list[tuple[str, ...]]) -> bool:
    if not isinstance(mapping, dict):
        return False
    for path in paths:
        value: Any = mapping
        for key in path:
            if not isinstance(value, dict):
                value = None
                break
            value = value.get(key)
        if isinstance(value, list) and value:
            return True
        if isinstance(value, dict) and any(item is not None and item != "" for item in value.values()):
            return True
        if value is not None and value != "":
            return True
    return False


def _research_data_availability(primary: dict[str, Any], external_research: dict[str, Any]) -> dict[str, bool]:
    quote = primary.get("quote") if isinstance(primary.get("quote"), dict) else {}
    fundamentals = primary.get("fundamentals") if isinstance(primary.get("fundamentals"), dict) else {}
    financials = primary.get("financials") if isinstance(primary.get("financials"), dict) else {}
    confirmation = primary.get("confirmation") if isinstance(primary.get("confirmation"), dict) else {}
    official_facts = (external_research.get("official_facts") or {}) if isinstance(external_research, dict) else {}
    government_contracts = primary.get("government_contracts") if isinstance(primary.get("government_contracts"), dict) else {}

    has_price = _has_value(quote, ["price"]) or _has_value(fundamentals, ["price"])
    has_volume = _has_value(fundamentals, ["volume", "avg_volume"])
    has_confirmation = bool(confirmation)
    has_forecast_revenue = _has_nested_value(
        financials,
        [
            ("forecasts", "nextQuarter", "revenueEstimate"),
            ("forecasts", "nextQuarter", "estimatedRevenueAvg"),
            ("forecasts", "nextFiscalYear", "revenueEstimate"),
        ],
    )
    has_forecast_eps = _has_nested_value(
        financials,
        [
            ("forecasts", "nextQuarter", "epsEstimate"),
            ("forecasts", "nextFiscalYear", "epsEstimate"),
        ],
    )
    return {
        "revenue": _has_value(fundamentals, ["revenue_growth"]) or "revenue" in official_facts or has_forecast_revenue,
        "revenue growth": _has_value(fundamentals, ["revenue_growth"]),
        "revenue consensus": has_forecast_revenue,
        "eps consensus": has_forecast_eps,
        "gross margin": _has_value(fundamentals, ["gross_margin"]) or "gross_margin" in official_facts,
        "operating margin": _has_value(fundamentals, ["operating_margin"]) or "operating_margin" in official_facts,
        "free cash flow": _has_value(fundamentals, ["free_cash_flow", "fcf_yield"]) or "free_cash_flow" in official_facts,
        "capex": "capex" in official_facts,
        "cash": "cash" in official_facts or _has_nested_value(financials, [("health", "cashAndCashEquivalents"), ("health", "cash")]),
        "debt": _has_value(fundamentals, ["debt_to_equity", "net_debt_to_ebitda"]) or "debt" in official_facts,
        "share count": "shares" in official_facts,
        "price": has_price,
        "current price": has_price,
        "volume": has_volume,
        "price/volume and technicals": has_price or has_volume or has_confirmation,
        "technical levels": has_confirmation,
        "reported institutional activity": bool(primary.get("institutional_activity")),
        "insider activity": bool(primary.get("insider_activity")),
        "congress activity": bool(primary.get("congress_activity")),
        "government contracts": bool((government_contracts or {}).get("recent_count") or (government_contracts or {}).get("items")),
        "valuation data": _has_value(fundamentals, ["forward_pe", "trailing_pe", "price_to_sales", "ev_to_ebitda"])
        or _has_nested_value(financials, [("valuation", "forwardPE"), ("valuation", "trailingPE"), ("valuation", "forward_pe")]),
        "peer comparison data": False,
    }


def _missing_note_field(note: str) -> str:
    return str(note or "").split(":", 1)[0].strip().lower()


def _filter_missing_data_notes(notes: list[str], availability: dict[str, bool]) -> list[str]:
    filtered: list[str] = []
    for note in notes:
        field = _missing_note_field(note)
        if field and availability.get(field):
            continue
        filtered.append(note)
    return _dedupe_strings(filtered)


def assemble_research_context(db: Session, payload: dict[str, Any]) -> dict[str, Any]:
    symbol, identity = normalize_supported_symbol(db, payload.get("ticker"))
    comparison_symbols = normalize_comparison_tickers(payload, primary_ticker=symbol)
    comparison_identities: dict[str, dict[str, Any]] = {}
    for comparison_symbol in comparison_symbols:
        try:
            normalized_comparison, comparison_identity = normalize_supported_symbol(db, comparison_symbol)
        except HTTPException as exc:
            if exc.status_code == 422:
                raise HTTPException(status_code=422, detail=f"{comparison_symbol} is not currently supported as a comparison ticker.") from exc
            raise
        comparison_identities[normalized_comparison] = comparison_identity

    symbols = [symbol] + list(comparison_identities.keys())
    fundamentals = {item: _latest_fundamentals(db, item) for item in symbols}
    quotes = {item: _quote(db, item) for item in symbols}
    financials = {item: _cached_financials_snapshot(db, item) for item in symbols}
    try:
        confirmation = get_confirmation_score_bundles_for_tickers(db, symbols, lookback_days=30)
    except Exception:
        confirmation = {}

    missing: list[str] = []
    for item in symbols:
        if not fundamentals.get(item):
            missing.append(f"{item}: fundamentals unavailable")
        if not quotes.get(item):
            missing.append(f"{item}: quote unavailable")
        if not confirmation.get(item):
            missing.append(f"{item}: confirmation score unavailable")
    external_research = discover_external_research(symbol, identity, mode=payload.get("external_research_mode") or "Standard")
    primary_context = {
        "identity": identity,
        "quote": quotes.get(symbol),
        "fundamentals": fundamentals.get(symbol),
        "financials": financials.get(symbol),
        "confirmation": _compact(confirmation.get(symbol)),
        "congress_activity": _recent_events(db, symbol, ["congress_trade", "congress_treasury_trade", "congress_crypto_trade"]),
        "insider_activity": _recent_events(db, symbol, ["insider_trade"]),
        "institutional_activity": _recent_events(
            db,
            symbol,
            [
                "institutional_accumulation",
                "institutional_distribution",
                "new_institutional_position",
                "major_holder_reduction",
                "major_holder_exit",
                "cluster_accumulation",
                "cluster_distribution",
                "smart_money_confirmation",
                "crowded_long",
                "contrarian_accumulation",
            ],
        ),
        "government_contracts": _government_contracts(db, symbol),
    }
    data_availability = _research_data_availability(primary_context, external_research)
    if external_research.get("missing_data_notes"):
        missing.extend(_filter_missing_data_notes(external_research["missing_data_notes"], data_availability))

    context = {
        "generated_at": _now(),
        "external_research_mode": payload.get("external_research_mode") or "Standard",
        "section_format": payload.get("section_format") or "Walnut Research Brief",
        "include_confirmation_score": bool(payload.get("include_confirmation_score")),
        "include_cross_source_confirmations": bool(payload.get("include_cross_source_confirmations")),
        "primary": primary_context,
        "external_research": external_research,
        "data_availability": data_availability,
        "comparison": None,
        "comparisons": [],
        "missing_data_notes": _dedupe_strings(missing),
        "limitations": [
            "13F activity is reported with filing lag and is not real-time.",
            "Congress and insider activity should not be interpreted as intent or wrongdoing.",
            "Missing Walnut data is unavailable, not zero and not automatically bearish.",
        ],
    }
    for comparison_symbol, comparison_identity in comparison_identities.items():
        comparison_context = {
            "identity": comparison_identity,
            "quote": quotes.get(comparison_symbol),
            "fundamentals": fundamentals.get(comparison_symbol),
            "financials": financials.get(comparison_symbol),
            "confirmation": _compact(confirmation.get(comparison_symbol)),
            "congress_activity": _recent_events(db, comparison_symbol, ["congress_trade", "congress_treasury_trade", "congress_crypto_trade"]),
            "insider_activity": _recent_events(db, comparison_symbol, ["insider_trade"]),
            "institutional_activity": _recent_events(
                db,
                comparison_symbol,
                [
                    "institutional_accumulation",
                    "institutional_distribution",
                    "new_institutional_position",
                    "major_holder_reduction",
                    "major_holder_exit",
                    "cluster_accumulation",
                    "cluster_distribution",
                    "smart_money_confirmation",
                    "crowded_long",
                    "contrarian_accumulation",
                ],
            ),
            "government_contracts": _government_contracts(db, comparison_symbol),
        }
        context["comparisons"].append(comparison_context)
    if context["comparisons"]:
        context["comparison"] = context["comparisons"][0]
    return context


def discover_external_research(symbol: str, identity: dict[str, Any], *, mode: str) -> dict[str, Any]:
    normalized_mode = _choice(mode, EXTERNAL_RESEARCH_MODE_OPTIONS, "Standard")
    if normalized_mode == "Off":
        return {
            "mode": "Off",
            "reviewed_sources": [],
            "source_notes": ["External research mode is off; only Walnut data was reviewed."],
            "official_facts": {},
            "missing_data_notes": [],
        }
    reviewed_sources = [
        {
            "label": "SEC EDGAR company search",
            "url": f"https://www.sec.gov/edgar/search/#/q={symbol}&dateRange=all",
            "source_type": "filing_search",
        },
        {
            "label": f"{symbol} Nasdaq market activity",
            "url": f"https://www.nasdaq.com/market-activity/stocks/{symbol.lower()}",
            "source_type": "reputable_market_source",
        }
    ]
    source_notes = [
        "Reviewed Walnut data and public/official source discovery. Report missing values as 'Not found in reviewed sources' when they cannot be supported.",
    ]
    official_facts: dict[str, Any] = {}
    sec_record = _sec_company_record(symbol)
    if sec_record:
        cik = str(sec_record.get("cik_str") or "").zfill(10)
        company = str(sec_record.get("title") or identity.get("company_name") or symbol).strip()
        reviewed_sources.extend(
            [
                {
                    "label": f"{company} SEC company filings",
                    "url": f"https://www.sec.gov/edgar/browse/?CIK={cik}&owner=exclude",
                    "source_type": "official_filing",
                },
                {
                    "label": f"{company} SEC company facts",
                    "url": f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json",
                    "source_type": "official_filing_data",
                },
            ]
        )
        official_facts = _sec_company_facts(cik)
        source_notes.append(f"Matched {symbol} to SEC CIK {cik} for official filings and company-facts review.")
    else:
        source_notes.append(f"SEC ticker mapping did not return a CIK for {symbol}; EDGAR symbol search remains attached for manual review.")
    if normalized_mode == "Deep":
        source_notes.append("Deep mode also attaches a reputable public market reference for price/volume review.")
    missing_fields = _missing_key_fields(official_facts)
    return {
        "mode": normalized_mode,
        "reviewed_sources": reviewed_sources,
        "source_notes": source_notes,
        "official_facts": official_facts,
        "missing_data_notes": [f"{field}: Not found in reviewed sources" for field in missing_fields],
    }


def _sec_company_record(symbol: str) -> dict[str, Any] | None:
    try:
        response = requests.get(
            "https://www.sec.gov/files/company_tickers.json",
            headers={"User-Agent": "Walnut Markets research generator contact@walnutmarkets.com"},
            timeout=8,
        )
    except requests.RequestException:
        return None
    if response.status_code >= 400:
        return None
    try:
        payload = response.json()
    except ValueError:
        return None
    records = payload.values() if isinstance(payload, dict) else payload if isinstance(payload, list) else []
    for record in records:
        if isinstance(record, dict) and str(record.get("ticker") or "").upper() == symbol.upper():
            return record
    return None


def _sec_company_facts(cik: str) -> dict[str, Any]:
    try:
        response = requests.get(
            f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json",
            headers={"User-Agent": "Walnut Markets research generator contact@walnutmarkets.com"},
            timeout=10,
        )
    except requests.RequestException:
        return {}
    if response.status_code >= 400:
        return {}
    try:
        payload = response.json()
    except ValueError:
        return {}
    us_gaap = ((payload.get("facts") or {}).get("us-gaap") or {}) if isinstance(payload, dict) else {}
    facts = {
        "revenue": _latest_sec_fact(us_gaap, ["Revenues", "RevenueFromContractWithCustomerExcludingAssessedTax", "SalesRevenueNet"]),
        "gross_profit": _latest_sec_fact(us_gaap, ["GrossProfit"]),
        "operating_income": _latest_sec_fact(us_gaap, ["OperatingIncomeLoss"]),
        "net_income": _latest_sec_fact(us_gaap, ["NetIncomeLoss"]),
        "operating_cash_flow": _latest_sec_fact(us_gaap, ["NetCashProvidedByUsedInOperatingActivities"]),
        "capex": _latest_sec_fact(us_gaap, ["PaymentsToAcquirePropertyPlantAndEquipment"]),
        "cash": _latest_sec_fact(us_gaap, ["CashAndCashEquivalentsAtCarryingValue", "CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents"]),
        "debt": _latest_sec_fact(us_gaap, ["LongTermDebt", "LongTermDebtAndFinanceLeaseObligationsCurrentAndNoncurrent"]),
        "shares": _latest_sec_fact(us_gaap, ["EntityCommonStockSharesOutstanding", "CommonStocksIncludingAdditionalPaidInCapital"]),
    }
    revenue = facts.get("revenue", {}).get("value") if isinstance(facts.get("revenue"), dict) else None
    gross_profit = facts.get("gross_profit", {}).get("value") if isinstance(facts.get("gross_profit"), dict) else None
    operating_income = facts.get("operating_income", {}).get("value") if isinstance(facts.get("operating_income"), dict) else None
    if revenue:
        if gross_profit is not None:
            facts["gross_margin"] = {"value": round((float(gross_profit) / float(revenue)) * 100, 2), "unit": "%", "derived_from": ["gross_profit", "revenue"]}
        if operating_income is not None:
            facts["operating_margin"] = {"value": round((float(operating_income) / float(revenue)) * 100, 2), "unit": "%", "derived_from": ["operating_income", "revenue"]}
    ocf = facts.get("operating_cash_flow", {}).get("value") if isinstance(facts.get("operating_cash_flow"), dict) else None
    capex = facts.get("capex", {}).get("value") if isinstance(facts.get("capex"), dict) else None
    if ocf is not None and capex is not None:
        facts["free_cash_flow"] = {"value": float(ocf) - abs(float(capex)), "unit": "USD", "derived_from": ["operating_cash_flow", "capex"]}
    return {key: value for key, value in facts.items() if value}


def _latest_sec_fact(us_gaap: dict[str, Any], names: list[str]) -> dict[str, Any] | None:
    for name in names:
        units = ((us_gaap.get(name) or {}).get("units") or {}) if isinstance(us_gaap.get(name), dict) else {}
        rows: list[dict[str, Any]] = []
        for unit, values in units.items():
            if isinstance(values, list):
                rows.extend({**row, "unit": unit, "taxonomy": name} for row in values if isinstance(row, dict) and row.get("val") is not None)
        if rows:
            row = sorted(rows, key=lambda item: (str(item.get("end") or ""), str(item.get("filed") or "")), reverse=True)[0]
            return {
                "value": row.get("val"),
                "unit": row.get("unit"),
                "period_end": row.get("end"),
                "filed": row.get("filed"),
                "form": row.get("form"),
                "taxonomy": row.get("taxonomy"),
            }
    return None


def _missing_key_fields(facts: dict[str, Any]) -> list[str]:
    fact_keys = set(facts.keys())
    required = {
        "revenue",
        "gross_margin",
        "operating_margin",
        "free_cash_flow",
        "capex",
        "cash",
        "debt",
        "shares",
    }
    missing = [field for field in required if field not in fact_keys]
    missing.extend(["guidance", "EBITDA / adjusted EBITDA", "backlog / orders / RPO", "dilution / ATM / offering history", "major customer concentration"])
    return missing


def _dedupe_strings(values: list[str]) -> list[str]:
    return list(dict.fromkeys(str(value) for value in values if str(value).strip()))


def validate_config(config: dict[str, Any]) -> dict[str, Any]:
    ticker = config.get("ticker")
    prompt = str(config.get("research_question") or "").strip()
    if not ticker:
        raise HTTPException(status_code=422, detail="Ticker is required.")
    if len(prompt) < 12:
        raise HTTPException(status_code=422, detail="Research question must be more specific.")
    normalized_ticker = normalize_symbol(ticker)
    comparison_tickers = normalize_comparison_tickers(config, primary_ticker=normalized_ticker)
    normalized = {
        "ticker": normalized_ticker or ticker,
        "research_question": prompt[:3000],
        "desired_angle": _choice(config.get("desired_angle"), ANGLE_OPTIONS, "Full company DD"),
        "comparison_ticker": comparison_tickers[0] if comparison_tickers else None,
        "comparison_tickers": comparison_tickers,
        "time_horizon": _choice(config.get("time_horizon"), TIME_HORIZON_OPTIONS, "Near term"),
        "intended_audience": _choice(config.get("intended_audience"), AUDIENCE_OPTIONS, "Walnut Research Brief"),
        "judgment_preference": _choice(config.get("judgment_preference"), JUDGMENT_OPTIONS, "Let the data decide"),
        "additional_context": str(config.get("additional_context") or "")[:4000],
        "include_sections": _sections(config.get("include_sections")),
        "length": _choice(config.get("length"), LENGTH_OPTIONS, "Standard: 1,500-2,500 words"),
        "tone": _choice(config.get("tone"), TONE_OPTIONS, "Walnut market-native"),
        "external_research_mode": _choice(config.get("external_research_mode"), EXTERNAL_RESEARCH_MODE_OPTIONS, "Standard"),
        "section_format": _choice_from_list(config.get("section_format"), SECTION_FORMAT_OPTIONS, "Walnut Research Brief"),
        "include_charts": bool(config.get("include_charts")),
        "include_source_links": bool(config.get("include_source_links")),
        "include_confirmation_score": bool(config.get("include_confirmation_score")),
        "include_cross_source_confirmations": bool(config.get("include_cross_source_confirmations")),
        "generate_thumbnail": bool(config.get("generate_thumbnail", _default_generate_thumbnail(config))),
        "selected_model": str(config.get("selected_model") or "").strip(),
        "hero_image": config.get("hero_image") or "",
    }
    normalized["selected_model"] = _selected_research_model(normalized)
    if normalized["desired_angle"] == "Peer comparison" and not normalized["comparison_tickers"]:
        raise HTTPException(status_code=422, detail="Comparison tickers are required for peer comparison briefs.")
    return normalized


def _choice(value: Any, choices: set[str], fallback: str) -> str:
    text = str(value or "").strip()
    return text if text in choices else fallback


def _choice_from_list(value: Any, choices: list[str], fallback: str) -> str:
    text = str(value or "").strip()
    return text if text in choices else fallback


def _default_generate_thumbnail(config: dict[str, Any]) -> bool:
    text = " ".join(str(config.get(key) or "") for key in ("section_format", "intended_audience", "tone"))
    if "Internal Analyst Note" in text:
        return False
    return True


def _sections(value: Any) -> list[str]:
    if not isinstance(value, list):
        return list(DEFAULT_SECTIONS)
    cleaned = [str(item).strip() for item in value if str(item).strip() in DEFAULT_SECTIONS]
    return cleaned or list(DEFAULT_SECTIONS)


def generate_research_brief(db: Session, admin: UserAccount, config: dict[str, Any], progress_callback: Any | None = None) -> dict[str, Any]:
    normalized_config = validate_config(config)
    normalized_config["selected_model"] = _selected_research_model(normalized_config, db)
    if progress_callback:
        progress_callback("loading_walnut_data", "Loading Walnut data.")
        if normalized_config.get("external_research_mode") != "Off":
            progress_callback("finding_sources", "Finding source context.")
    context = assemble_research_context(db, normalized_config)
    actor_key = f"admin:{admin.id}"
    if actor_key in _ACTIVE_GENERATIONS:
        raise HTTPException(status_code=429, detail="A research brief generation is already running for this Admin session.")
    _ACTIVE_GENERATIONS.add(actor_key)
    try:
        started = time.perf_counter()
        if progress_callback:
            progress_callback("generating_brief", "Generating research brief.")
        article = _mock_article(normalized_config, context) if os.getenv(MOCK_ENV) == "1" else _call_openai(db, normalized_config, context)
        article = sanitize_research_brief_article(article, normalized_config, context)
        if normalized_config.get("generate_thumbnail"):
            if progress_callback:
                progress_callback("generating_thumbnail", "Generating thumbnail.")
            try:
                article["thumbnail_asset"] = generate_thumbnail_asset(db, normalized_config, article)
                if article["thumbnail_asset"].get("url") and not article.get("hero_image"):
                    article["hero_image"] = article["thumbnail_asset"]["url"]
            except Exception as exc:
                logger.warning("research_brief_thumbnail_failed ticker=%s error=%s", normalized_config.get("ticker"), exc.__class__.__name__)
                article["thumbnail_asset"] = {
                    "image_title": str(article.get("title") or normalized_config.get("ticker") or "Walnut research")[:120],
                    "image_prompt": "",
                    "asset_type": _thumbnail_asset_type(normalized_config),
                    "url": "",
                    "thumbnail_url": "",
                    "source_notes": "Thumbnail generation failed; text draft was saved.",
                    "created_at": _now(),
                }
        if progress_callback:
            progress_callback("validating_claims", "Validating generated draft.")
        validation = validate_article(article, context)
        if progress_callback:
            progress_callback("saving_draft", "Saving generated draft.")
        draft = _new_draft(admin, normalized_config, context, article, validation, elapsed_ms=int((time.perf_counter() - started) * 1000))
        with _STORE_LOCK:
            store = _read_store()
            store["drafts"].append(draft)
            _append_audit(store, action="generate", admin=admin, draft_id=draft["id"], metadata={"ticker": normalized_config["ticker"]})
            _write_store(store)
        try:
            _upsert_db_draft(db, draft)
        except Exception as exc:
            logger.warning("research_brief_draft_db_persist_failed draft_id=%s error=%s", draft.get("id"), exc.__class__.__name__)
        return draft
    finally:
        _ACTIVE_GENERATIONS.discard(actor_key)


def enqueue_research_brief_generation_job(db: Session, admin: UserAccount, config: dict[str, Any]) -> dict[str, Any]:
    normalized_config = validate_config(config)
    normalized_config["selected_model"] = _selected_research_model(normalized_config, db)
    client_request_id = str(config.get("client_request_id") or uuid.uuid4()).strip()[:120]
    now = _now()
    ensure_research_brief_store_schema(db)
    existing = db.execute(
        text(
            """
            SELECT * FROM research_brief_generation_jobs
            WHERE created_by_admin_id = :admin_id AND client_request_id = :client_request_id
            ORDER BY created_at DESC
            LIMIT 1
            """
        ),
        {"admin_id": admin.id, "client_request_id": client_request_id},
    ).mappings().first()
    if existing:
        existing_job = _job_from_row(existing)
        payload = _job_response_payload(existing_job)
        if payload.get("status") == "queued":
            _start_research_brief_job_worker(str(payload["job_id"]))
        return payload
    with _STORE_LOCK:
        store = _read_store()
        jobs = store.setdefault("jobs", [])
        for job in jobs:
            if job.get("created_by_admin_id") == admin.id and job.get("client_request_id") == client_request_id:
                payload = _job_response_payload(job)
                if job.get("status") in {"queued", "running"}:
                    _start_research_brief_job_worker(str(job["id"]))
                return payload
        job = {
            "id": f"rbj_{uuid.uuid4().hex}",
            "status": "queued",
            "client_request_id": client_request_id,
            "created_by_admin_id": admin.id,
            "created_by_admin_email": getattr(admin, "email", None),
            "ticker": normalized_config["ticker"],
            "request_payload_json": normalized_config,
            "model": normalized_config.get("selected_model"),
            "external_research_mode": normalized_config.get("external_research_mode"),
            "section_format": normalized_config.get("section_format"),
            "generate_thumbnail": bool(normalized_config.get("generate_thumbnail")),
            "progress_step": "queued",
            "progress_message": "Research brief generation queued.",
            "source_links_count": 0,
            "numeric_claims_count": 0,
            "validation_status": None,
            "draft_id": None,
            "error_message_safe": None,
            "error_details_internal": None,
            "created_at": now,
            "started_at": None,
            "updated_at": now,
            "completed_at": None,
            "failed_at": None,
        }
        jobs.append(job)
        _write_store(store)
        _upsert_db_job(db, job)
        payload = _job_response_payload(job)
    logger.info(
        "research_brief_job_created job_id=%s ticker=%s model=%s external_research_mode=%s generate_thumbnail=%s",
        payload["job_id"],
        payload.get("ticker"),
        payload.get("model"),
        payload.get("external_research_mode"),
        payload.get("generate_thumbnail"),
    )
    _start_research_brief_job_worker(payload["job_id"])
    return payload


def get_research_brief_generation_job(job_id: str, db: Session | None = None) -> dict[str, Any]:
    if db is not None:
        job = _db_job(db, job_id)
        if job:
            job = _fail_stale_running_job(job_id, job, db)
            payload = _job_response_payload(job)
            if payload["status"] == "queued":
                _start_research_brief_job_worker(job_id)
            return payload
    with _STORE_LOCK:
        job = _find_job(_read_store(), job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Research brief generation job not found.")
        if _job_is_stale_running(job):
            job["status"] = "failed"
            job["progress_step"] = "failed"
            job["progress_message"] = RESEARCH_BRIEF_JOB_STALE_ERROR
            job["error_message_safe"] = RESEARCH_BRIEF_JOB_STALE_ERROR
            job["error_details_internal"] = "Stale running research brief job heartbeat expired."
            job["failed_at"] = _now()
            _write_store(store)
        payload = _job_response_payload(job)
    if payload["status"] == "queued":
        _start_research_brief_job_worker(job_id)
    return payload


def list_generation_jobs(status: str | None = None, db: Session | None = None) -> dict[str, Any]:
    if db is not None:
        ensure_research_brief_store_schema(db)
        if status and status != "all":
            rows = db.execute(text("SELECT * FROM research_brief_generation_jobs WHERE status = :status ORDER BY created_at DESC"), {"status": status}).mappings().all()
        else:
            rows = db.execute(text("SELECT * FROM research_brief_generation_jobs ORDER BY created_at DESC")).mappings().all()
        return {"items": [_job_response_payload(_job_from_row(row)) for row in rows]}
    with _STORE_LOCK:
        jobs = [_job_response_payload(job) for job in (_read_store().get("jobs") or [])]
    if status and status != "all":
        jobs = [job for job in jobs if job.get("status") == status]
    return {"items": sorted(jobs, key=lambda item: item.get("created_at") or "", reverse=True)}


def get_research_brief_generation_job_draft(job_id: str, db: Session | None = None) -> dict[str, Any]:
    job = get_research_brief_generation_job(job_id, db)
    if job["status"] != "completed" or not job.get("draft_id"):
        raise HTTPException(status_code=409, detail="Research brief generation is not complete yet.")
    if db is not None:
        db_job = _db_job(db, job_id)
        draft_payload = (db_job or {}).get("draft_payload_json")
        if isinstance(draft_payload, dict):
            return _draft_with_comparison_tickers(deepcopy(draft_payload))
    return get_draft(str(job["draft_id"]), db=db)


def run_research_brief_generation_job(job_id: str, db: Session | None = None) -> None:
    owns_db = db is None
    session = db or SessionLocal()
    started = time.perf_counter()
    try:
        job = _mark_job_running(job_id, session)
        if job.get("status") == "completed" or job.get("_skip_worker"):
            return
        admin = session.get(UserAccount, job.get("created_by_admin_id"))
        if not admin:
            raise HTTPException(status_code=404, detail="Admin account not found for research brief generation job.")

        def progress(step: str, message: str) -> None:
            _update_job_progress(job_id, step, message, session)

        draft = generate_research_brief(session, admin, dict(job.get("request_payload_json") or {}), progress_callback=progress)
        validation = draft.get("validation") or {}
        _complete_job(job_id, draft, duration_ms=int((time.perf_counter() - started) * 1000), db=session)
        logger.info(
            "research_brief_job_completed job_id=%s ticker=%s model=%s external_research_mode=%s generate_thumbnail=%s duration_ms=%s source_links_count=%s numeric_claims_count=%s validation_status=%s",
            job_id,
            draft.get("primary_ticker"),
            draft.get("model"),
            (draft.get("config") or {}).get("external_research_mode"),
            (draft.get("config") or {}).get("generate_thumbnail"),
            int((time.perf_counter() - started) * 1000),
            validation.get("source_link_count"),
            len(validation.get("numeric_claims") or []),
            validation.get("status"),
        )
    except Exception as exc:
        _fail_job(job_id, exc, duration_ms=int((time.perf_counter() - started) * 1000), db=session)
    finally:
        if owns_db:
            session.close()


def _start_research_brief_job_worker(job_id: str) -> None:
    with _JOB_WORKER_LOCK:
        existing = _JOB_WORKERS.get(job_id)
        if existing and existing.is_alive():
            return
        thread = threading.Thread(target=run_research_brief_generation_job, args=(job_id,), name=f"research-brief-job-{job_id}", daemon=True)
        _JOB_WORKERS[job_id] = thread
        thread.start()


def _mark_job_running(job_id: str, db: Session | None = None) -> dict[str, Any]:
    if db is not None:
        job = _db_job(db, job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Research brief generation job not found.")
        if job.get("status") == "completed":
            return deepcopy(job)
        if job.get("status") in {"failed", "cancelled"}:
            raise HTTPException(status_code=409, detail="Research brief generation job is not active.")
        if job.get("status") == "running" and _job_is_stale_running(job):
            raise TimeoutError("Stale running research brief job heartbeat expired.")
        if job.get("status") == "running":
            payload = deepcopy(job)
            payload["_skip_worker"] = True
            return payload
        job["status"] = "running"
        job["started_at"] = job.get("started_at") or _now()
        job["progress_step"] = "loading_walnut_data"
        job["progress_message"] = "Starting research brief generation."
        _upsert_db_job(db, job)
        payload = deepcopy(job)
        logger.info(
            "research_brief_job_started job_id=%s ticker=%s model=%s external_research_mode=%s generate_thumbnail=%s",
            job_id,
            payload.get("ticker"),
            payload.get("model"),
            payload.get("external_research_mode"),
            payload.get("generate_thumbnail"),
        )
        return payload
    with _STORE_LOCK:
        store = _read_store()
        job = _find_job(store, job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Research brief generation job not found.")
        if job.get("status") == "completed":
            return deepcopy(job)
        if job.get("status") in {"failed", "cancelled"}:
            raise HTTPException(status_code=409, detail="Research brief generation job is not active.")
        if job.get("status") == "running" and _job_is_stale_running(job):
            raise TimeoutError("Stale running research brief job heartbeat expired.")
        job["status"] = "running"
        job["started_at"] = job.get("started_at") or _now()
        job["updated_at"] = _now()
        job["progress_step"] = "loading_walnut_data"
        job["progress_message"] = "Starting research brief generation."
        _write_store(store)
        payload = deepcopy(job)
    logger.info(
        "research_brief_job_started job_id=%s ticker=%s model=%s external_research_mode=%s generate_thumbnail=%s",
        job_id,
        payload.get("ticker"),
        payload.get("model"),
        payload.get("external_research_mode"),
        payload.get("generate_thumbnail"),
    )
    return payload


def _update_job_progress(job_id: str, step: str, message: str, db: Session | None = None) -> None:
    if db is not None:
        job = _db_job(db, job_id)
        if not job or job.get("status") not in {"queued", "running"}:
            return
        job["status"] = "running"
        job["progress_step"] = step
        job["progress_message"] = message
        job["updated_at"] = _now()
        _upsert_db_job(db, job)
        payload = deepcopy(job)
        logger.info(
            "research_brief_job_step job_id=%s ticker=%s model=%s external_research_mode=%s generate_thumbnail=%s progress_step=%s",
            job_id,
            payload.get("ticker"),
            payload.get("model"),
            payload.get("external_research_mode"),
            payload.get("generate_thumbnail"),
            step,
        )
        return
    with _STORE_LOCK:
        store = _read_store()
        job = _find_job(store, job_id)
        if not job or job.get("status") not in {"queued", "running"}:
            return
        job["status"] = "running"
        job["progress_step"] = step
        job["progress_message"] = message
        job["updated_at"] = _now()
        _write_store(store)
        payload = deepcopy(job)
    logger.info(
        "research_brief_job_step job_id=%s ticker=%s model=%s external_research_mode=%s generate_thumbnail=%s progress_step=%s",
        job_id,
        payload.get("ticker"),
        payload.get("model"),
        payload.get("external_research_mode"),
        payload.get("generate_thumbnail"),
        step,
    )


def _complete_job(job_id: str, draft: dict[str, Any], *, duration_ms: int, db: Session | None = None) -> None:
    validation = draft.get("validation") or {}
    if db is not None:
        job = _db_job(db, job_id)
        if not job:
            return
        job["status"] = "completed"
        job["progress_step"] = "completed"
        job["progress_message"] = "Research brief draft generated."
        job["draft_id"] = draft.get("id")
        job["draft_payload_json"] = draft
        job["source_links_count"] = validation.get("source_link_count") or 0
        job["numeric_claims_count"] = len(validation.get("numeric_claims") or [])
        job["validation_status"] = validation.get("status")
        job["completed_at"] = _now()
        job["updated_at"] = job["completed_at"]
        job["failed_at"] = None
        job["error_message_safe"] = None
        job["error_details_internal"] = None
        job["duration_ms"] = duration_ms
        _upsert_db_draft(db, draft)
        _upsert_db_job(db, job)
        return
    with _STORE_LOCK:
        store = _read_store()
        job = _find_job(store, job_id)
        if not job:
            return
        job["status"] = "completed"
        job["progress_step"] = "completed"
        job["progress_message"] = "Research brief draft generated."
        job["draft_id"] = draft.get("id")
        job["source_links_count"] = validation.get("source_link_count") or 0
        job["numeric_claims_count"] = len(validation.get("numeric_claims") or [])
        job["validation_status"] = validation.get("status")
        job["completed_at"] = _now()
        job["updated_at"] = job["completed_at"]
        job["failed_at"] = None
        job["error_message_safe"] = None
        job["error_details_internal"] = None
        job["duration_ms"] = duration_ms
        _write_store(store)


def _fail_job(job_id: str, exc: Exception, *, duration_ms: int, db: Session | None = None) -> None:
    safe_error = _safe_job_error(exc)
    if db is not None:
        job = _db_job(db, job_id)
        if job:
            job["status"] = "failed"
            job["progress_step"] = "failed"
            job["progress_message"] = safe_error
            job["error_message_safe"] = safe_error
            job["error_details_internal"] = f"{exc.__class__.__name__}: {str(exc)[:1000]}"
            job["failed_at"] = _now()
            job["updated_at"] = job["failed_at"]
            job["duration_ms"] = duration_ms
            _upsert_db_job(db, job)
            payload = deepcopy(job)
        else:
            payload = {"ticker": None, "model": None, "external_research_mode": None, "generate_thumbnail": None}
        logger.warning(
            "research_brief_job_failed job_id=%s ticker=%s model=%s external_research_mode=%s generate_thumbnail=%s duration_ms=%s error=%s",
            job_id,
            payload.get("ticker"),
            payload.get("model"),
            payload.get("external_research_mode"),
            payload.get("generate_thumbnail"),
            duration_ms,
            exc.__class__.__name__,
        )
        return
    with _STORE_LOCK:
        store = _read_store()
        job = _find_job(store, job_id)
        if job:
            job["status"] = "failed"
            job["progress_step"] = "failed"
            job["progress_message"] = safe_error
            job["error_message_safe"] = safe_error
            job["error_details_internal"] = f"{exc.__class__.__name__}: {str(exc)[:1000]}"
            job["failed_at"] = _now()
            job["updated_at"] = job["failed_at"]
            job["duration_ms"] = duration_ms
            _write_store(store)
            payload = deepcopy(job)
        else:
            payload = {"ticker": None, "model": None, "external_research_mode": None, "generate_thumbnail": None}
    logger.warning(
        "research_brief_job_failed job_id=%s ticker=%s model=%s external_research_mode=%s generate_thumbnail=%s duration_ms=%s error=%s",
        job_id,
        payload.get("ticker"),
        payload.get("model"),
        payload.get("external_research_mode"),
        payload.get("generate_thumbnail"),
        duration_ms,
        exc.__class__.__name__,
    )


def _safe_job_error(exc: Exception) -> str:
    if isinstance(exc, TimeoutError):
        return RESEARCH_BRIEF_JOB_STALE_ERROR
    if isinstance(exc, HTTPException) and exc.status_code == 422:
        detail = str(exc.detail or "").strip()
        if detail and not re.search(r"\b(provider|internal|cache|raw|token|credential|diagnostic)s?\b", detail, flags=re.IGNORECASE):
            return detail[:300]
    return RESEARCH_BRIEF_JOB_SAFE_ERROR


def _fail_stale_running_job(job_id: str, job: dict[str, Any], db: Session) -> dict[str, Any]:
    if not _job_is_stale_running(job):
        return job
    job = deepcopy(job)
    job["status"] = "failed"
    job["progress_step"] = "failed"
    job["progress_message"] = RESEARCH_BRIEF_JOB_STALE_ERROR
    job["error_message_safe"] = RESEARCH_BRIEF_JOB_STALE_ERROR
    job["error_details_internal"] = "Stale running research brief job heartbeat expired."
    job["failed_at"] = _now()
    job["updated_at"] = job["failed_at"]
    _upsert_db_job(db, job)
    logger.warning(
        "research_brief_job_failed job_id=%s ticker=%s model=%s external_research_mode=%s generate_thumbnail=%s duration_ms=%s error=%s",
        job_id,
        job.get("ticker"),
        job.get("model"),
        job.get("external_research_mode"),
        job.get("generate_thumbnail"),
        job.get("duration_ms"),
        "StaleRunningJob",
    )
    return job


def _job_is_stale_running(job: dict[str, Any]) -> bool:
    if job.get("status") != "running":
        return False
    heartbeat = _parse_iso_datetime(job.get("updated_at") or job.get("started_at") or job.get("created_at"))
    if heartbeat is None:
        return True
    return (datetime.now(timezone.utc) - heartbeat).total_seconds() > _env_float(RESEARCH_BRIEF_JOB_STALE_SECONDS, 300.0)


def _parse_iso_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc) if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.astimezone(timezone.utc) if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _find_job(store: dict[str, Any], job_id: str) -> dict[str, Any] | None:
    for job in store.get("jobs") or []:
        if job.get("id") == job_id:
            return job
    return None


def _job_response_payload(job: dict[str, Any]) -> dict[str, Any]:
    return {
        "job_id": job.get("id"),
        "status": job.get("status"),
        "client_request_id": job.get("client_request_id"),
        "ticker": job.get("ticker"),
        "model": job.get("model"),
        "external_research_mode": job.get("external_research_mode"),
        "section_format": job.get("section_format"),
        "generate_thumbnail": job.get("generate_thumbnail"),
        "progress_step": job.get("progress_step"),
        "progress_message": job.get("progress_message"),
        "source_links_count": job.get("source_links_count") or 0,
        "numeric_claims_count": job.get("numeric_claims_count") or 0,
        "validation_status": job.get("validation_status"),
        "draft_id": job.get("draft_id"),
        "error_message_safe": job.get("error_message_safe"),
        "created_at": job.get("created_at"),
        "started_at": job.get("started_at"),
        "updated_at": job.get("updated_at"),
        "completed_at": job.get("completed_at"),
        "failed_at": job.get("failed_at"),
    }


def _call_openai(db: Session, config: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
    api_key = resolved_setting_value(db, OPENAI_API_KEY)
    if not api_key:
        raise HTTPException(status_code=503, detail="OpenAI API key missing. Configure OPENAI_API_KEY before generating.")
    model = _selected_research_model(config, db)
    response = requests.post(
        RESPONSES_ENDPOINT,
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        json={
            "model": model,
            "input": _prompt(config, context),
            "store": False,
            "max_output_tokens": _max_output_tokens(config["length"]),
            "text": {"format": {"type": "json_schema", "name": "walnut_research_brief", "schema": article_schema(), "strict": True}},
        },
        timeout=_env_float(RESEARCH_BRIEF_OPENAI_TIMEOUT_SECONDS, 90.0),
    )
    if response.status_code == 429:
        raise HTTPException(status_code=429, detail="OpenAI rate limit hit. Try again later.")
    if response.status_code >= 400:
        raise HTTPException(status_code=502, detail="OpenAI generation failed. Check model, quota, and prompt size.")
    data = response.json()
    text = _response_text(data)
    try:
        parsed = json.loads(text)
    except Exception:
        raise HTTPException(status_code=502, detail="OpenAI returned invalid structured research JSON.")
    if not isinstance(parsed, dict):
        raise HTTPException(status_code=502, detail="OpenAI returned an invalid article payload.")
    parsed["_generation_usage"] = data.get("usage") if isinstance(data.get("usage"), dict) else {}
    parsed["_model"] = model
    return parsed


def _response_text(data: dict[str, Any]) -> str:
    if isinstance(data.get("output_text"), str):
        return data["output_text"]
    for output in data.get("output") or []:
        for content in output.get("content") or []:
            if isinstance(content.get("text"), str):
                return content["text"]
    return ""


def _max_output_tokens(length: str) -> int:
    if length.startswith("Short"):
        return 3500
    if length.startswith("Deep"):
        return 9000
    return 6000


def _env_float(name: str, fallback: float) -> float:
    try:
        return max(float(os.getenv(name, "") or fallback), 1.0)
    except (TypeError, ValueError):
        return fallback


def _prompt(config: dict[str, Any], context: dict[str, Any]) -> str:
    section_format = _section_format_instructions(config.get("section_format") or "Walnut Research Brief")
    prompt_config = dict(config)
    prompt_config.pop("comparison_ticker", None)
    return "\n".join(
        [
            "You are Walnut's senior market research editor writing a publishable due-diligence brief.",
            "Use Walnut data, external research notes, and reviewed public source links. Do not invent metrics, quotes, filings, historical changes, catalysts, or source links.",
            "When Walnut data misses a key field, use official/public reviewed sources first. If still unavailable, say 'Not found in reviewed sources' once in Data limitations, not repeatedly field by field.",
            "Treat data_availability as authoritative. Do not say price, volume, price/volume and technicals, revenue consensus, EPS consensus, gross margin, free cash flow, valuation, reported institutional activity, insider activity, Congress activity, or government contracts are missing when data_availability marks that field available.",
            "Only list fields from missing_data_notes as missing. If a dataset is available but empty or limited, describe the actual availability/result instead of calling the whole category not found.",
            "Only include Walnut's proprietary confirmation score if include_confirmation_score is true. Only include cross-source confirmation commentary if include_cross_source_confirmations is true. Keep these concepts separate.",
            "The confirmation score is Walnut's proprietary score. Cross-source confirmations are qualitative supporting or contradicting data categories such as price/volume, fundamentals, reported institutional activity, Congress activity, insider activity, government contracts, options flow, and macro positioning. Use 'data,' not 'stack.'",
            "Never cite the admin prompt, user request, research request, supplied materials, supplied context, research configuration, or model instructions as a source. User-provided numbers are leads to verify, not sources.",
            "Any publishable research/DD post must include at least two credible source links, and valuation/DD work should include an official/company/filing source when possible.",
            "Separate underlying data from Walnut confirmation score. Missing data is unavailable, not zero and not bearish.",
            "Use 'data', not 'stack'. Use 'reported' or 'disclosed' for Congress, insider, and institutional activity. For 13F data, say 'reported institutional activity', 'filing date', and 'quarter-end holdings'; never imply live institutional buying.",
            "Never expose provider, internal, cache, raw, token, credential, or diagnostic wording in user-facing copy.",
            "For DCF/valuation briefs, do not produce a fake DCF when inputs are missing. Separate reported numbers from assumptions and say when a DCF cannot be anchored.",
            "Do not imply financial advice, guaranteed returns, congressional intent, insider wrongdoing, or real-time 13F activity.",
            "Write directly, specifically, and professionally. Avoid generic AI phrasing and marketing filler.",
            "Use comparison_tickers only where relevant. Do not force every comparison ticker into every section. If comparison data is unavailable, say so clearly. Do not invent data. Use the comparisons to compare growth, margins, capex, valuation, cash flow, and market setup where available.",
            "End with a clear Walnut judgment plus a brief research-only disclaimer.",
            "The JSON summary is the Insights preview body. Keep it 1-3 sentences and do not duplicate the full post body.",
            "Section format instructions:",
            section_format,
            "Key missing-field search checklist:",
            json.dumps(KEY_RESEARCH_FIELDS, indent=2),
            "Return only JSON matching the provided schema.",
            "Admin configuration:",
            json.dumps(prompt_config, indent=2, sort_keys=True),
            "Walnut research context:",
            json.dumps(context, indent=2, sort_keys=True, default=str)[:18000],
        ]
    )


def _section_format_instructions(section_format: str) -> str:
    if section_format == "Reddit DD - Issue / Risk / Data / Conclusion":
        return (
            "Use this markdown structure: Intro / hook; The issue; The risk / opportunity; The data; Conclusion; Sources; optional What to watch next. "
            "The issue explains what the market is debating and why now. The risk / opportunity names what can go right, what can go wrong, what kills the thesis, and what confirms it. "
            "The data uses concrete sourced numbers without burying unavailable fields. Conclusion makes a bullish, bearish, mixed, or insufficient-data call. Research only. Not investment advice."
        )
    if section_format == "Reddit DD - Bull Case / Bear Case / The Data / The Call":
        return "Use this markdown structure: Executive thesis; Bull case; Bear case; The data; The call; What to watch next; Sources; Data freshness and limitations. Do not include an Intro / hook heading."
    if section_format == "ValueInvesting - Business / Valuation / Risks / Margin of Safety":
        return "Use this markdown structure: Business; Valuation; Risks; Margin of safety; Sources; What to watch next. Emphasize cash flow, downside case, assumptions, and valuation limits."
    if section_format == "X Thread":
        return "Write as a concise X thread draft with numbered posts, each source-backed and readable without hype."
    if section_format == "Internal Analyst Note":
        return "Write as an internal analyst note. Thumbnail generation is optional and the tone can be more terse, but unsupported claims still fail validation."
    return "Use Walnut Research Brief sections with a clear thesis, data, risks, catalysts, conclusion, sources, and data limitations."


def article_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "required": [
            "title",
            "slug",
            "subtitle",
            "summary",
            "preview_body",
            "judgment",
            "confidence",
            "primary_ticker",
            "comparison_tickers",
            "category",
            "reading_minutes",
            "sections",
            "key_points",
            "catalysts",
            "risks",
            "watch_items",
            "data_freshness",
            "missing_data_notes",
            "source_links",
            "suggested_card",
            "seo",
        ],
        "properties": {
            "title": {"type": "string"},
            "slug": {"type": "string"},
            "subtitle": {"type": "string"},
            "summary": {"type": "string"},
            "preview_body": {"type": "string"},
            "judgment": {"type": "string", "enum": sorted(JUDGMENT_VALUES)},
            "confidence": {"type": "string", "enum": ["low", "medium", "high"]},
            "primary_ticker": {"type": "string"},
            "comparison_tickers": {"type": "array", "items": {"type": "string"}},
            "category": {"type": "string"},
            "reading_minutes": {"type": "integer"},
            "sections": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["key", "heading", "body_markdown"],
                    "properties": {"key": {"type": "string"}, "heading": {"type": "string"}, "body_markdown": {"type": "string"}},
                },
            },
            "key_points": {"type": "array", "items": {"type": "string"}},
            "catalysts": {"type": "array", "items": {"type": "string"}},
            "risks": {"type": "array", "items": {"type": "string"}},
            "watch_items": {"type": "array", "items": {"type": "string"}},
            "data_freshness": {"type": "array", "items": {"type": "string"}},
            "missing_data_notes": {"type": "array", "items": {"type": "string"}},
            "source_links": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["label", "url", "source_type"],
                    "properties": {
                        "label": {"type": "string"},
                        "url": {"type": "string"},
                        "source_type": {"type": "string"},
                    },
                },
            },
            "suggested_card": {
                "type": "object",
                "additionalProperties": False,
                "required": ["title", "description", "judgment", "tickers"],
                "properties": {
                    "title": {"type": "string"},
                    "description": {"type": "string"},
                    "judgment": {"type": "string"},
                    "tickers": {"type": "array", "items": {"type": "string"}},
                },
            },
            "seo": {
                "type": "object",
                "additionalProperties": False,
                "required": ["title", "description"],
                "properties": {"title": {"type": "string"}, "description": {"type": "string"}},
            },
        },
    }


def validate_article(article: dict[str, Any], context: dict[str, Any], draft_id: str | None = None) -> dict[str, Any]:
    warnings: list[dict[str, Any]] = []
    blocking = False
    title = str(article.get("title") or "").strip()
    body = "\n\n".join(str(section.get("body_markdown") or "") for section in article.get("sections") or [] if isinstance(section, dict))
    rendered_markdown = "\n\n".join(
        f"## {section.get('heading')}\n\n{section.get('body_markdown') or ''}"
        for section in article.get("sections") or []
        if isinstance(section, dict)
    )
    slug = _slugify(str(article.get("slug") or title), fallback=f"{context['primary']['identity']['symbol'].lower()}-research-brief")
    repair_count = int(article.get("_copy_sanitizer_repairs") or 0)
    labels = {
        "structure": "repaired" if repair_count else "passed",
        "internal_language": "repaired" if repair_count else "passed",
        "source_support": "passed",
        "missing_data_language": "repaired" if repair_count else "passed",
    }
    if not title:
        warnings.append(_warning("missing_title", "Title is required.", blocking=True))
        blocking = True
    if len(body) < 800:
        warnings.append(_warning("thin_body", "Article body appears too short for a professional research brief.", blocking=True))
        blocking = True
    summary_text = f"{article.get('summary') or ''}\n{article.get('preview_body') or ''}"
    if "not investment advice" not in body.lower() and "not investment advice" not in summary_text.lower():
        warnings.append(_warning("missing_disclaimer", "Research-only / not-investment-advice language is missing.", blocking=True))
        blocking = True
    lowered = f"{title}\n{body}".lower()
    include_confirmation_score = bool(context.get("include_confirmation_score"))
    include_cross_source_confirmations = bool(context.get("include_cross_source_confirmations"))
    source_link_count = _source_link_count(article, body)
    if source_link_count == 0:
        warnings.append(_warning("missing_source_links", "This draft has no source links. Regenerate with External Research Mode enabled or add sources manually.", blocking=True))
        labels["source_support"] = "failed"
        blocking = True
    elif source_link_count < 2:
        warnings.append(_warning("insufficient_source_links", "Research briefs need at least 2 credible source links before publishing.", blocking=True))
        labels["source_support"] = "failed"
        blocking = True
    for phrase in UNSUPPORTED_LANGUAGE:
        if phrase in lowered:
            warnings.append(_warning("unsupported_language", f"Unsupported language detected: {phrase}", blocking=True))
            blocking = True
    if "not supplied" in lowered:
        warnings.append(_warning("not_supplied_language", "Use 'Not found in reviewed sources' once in Data limitations instead of repeated 'not supplied' language.", blocking=True))
        labels["missing_data_language"] = "failed"
        blocking = True
    missing_language_hits = _missing_data_language_hits(lowered)
    if missing_language_hits:
        warnings.append(_warning("awkward_missing_data_language", f"Missing-data wording needs cleanup: {', '.join(missing_language_hits)}.", blocking=True))
        labels["missing_data_language"] = "failed"
        blocking = True
    internal_language_hits = _internal_language_hits(lowered)
    if internal_language_hits:
        warnings.append(_warning("internal_workflow_language", f"Internal workflow language remains: {', '.join(internal_language_hits)}.", blocking=True))
        labels["internal_language"] = "failed"
        blocking = True
    structure_issues = _markdown_structure_issues(rendered_markdown)
    if structure_issues:
        warnings.append(_warning("markdown_structure", f"Markdown structure needs cleanup: {', '.join(structure_issues)}.", blocking=True))
        labels["structure"] = "failed"
        blocking = True
    if re.search(r"\b(provider|internal|cache|raw|token|credential|diagnostic)s?\b", lowered):
        warnings.append(_warning("internal_wording", "Provider/internal/cache/source-system wording must not appear in user-facing output.", blocking=True))
        labels["internal_language"] = "failed"
        blocking = True
    if "confirmation score equals" in lowered or "confirmation stack" in lowered:
        warnings.append(_warning("confirmation_score_blended", "Confirmation score must remain separate from underlying data.", blocking=True))
        blocking = True
    if include_confirmation_score and _confirmation_score_value(context) is None:
        warnings.append(_warning("confirmation_score_unavailable", "Walnut confirmation score was requested but could not be loaded for the primary ticker.", blocking=True))
        blocking = True
    if not include_confirmation_score and re.search(r"\bconfirmation score\b", lowered):
        warnings.append(_warning("confirmation_score_not_requested", "Walnut confirmation score is unchecked, but the post body mentions the confirmation score.", blocking=True))
        blocking = True
    if not include_cross_source_confirmations and _article_mentions_cross_source_confirmations(article):
        warnings.append(_warning("cross_source_confirmations_not_requested", "Cross-source confirmations are unchecked, but the post body includes cross-source confirmation commentary.", blocking=True))
        blocking = True
    if include_cross_source_confirmations and _conflates_confirmation_score_with_data(lowered):
        warnings.append(_warning("confirmation_score_conflated", "Cross-source data categories must not be described as the proprietary confirmation score.", blocking=True))
        blocking = True
    contradicted_fields = _available_data_missing_claims(lowered, context)
    if contradicted_fields:
        warnings.append(
            _warning(
                "available_data_marked_missing",
                f"Draft says available Walnut data is missing: {', '.join(contradicted_fields)}.",
                blocking=True,
            )
        )
        blocking = True
    numeric_claims = sorted(set(re.findall(r"(?<![A-Za-z])(?:\$?\d[\d,]*(?:\.\d+)?%?|\d+\s?bps)(?![A-Za-z])", body)))[:80]
    if numeric_claims and not _context_has_numbers(context):
        warnings.append(_warning("numeric_claims_without_context", "Numeric claims detected while source context has few numeric fields.", blocking=True))
        labels["source_support"] = "failed"
        blocking = True
    if _duplicate_slug(slug, draft_id=draft_id):
        warnings.append(_warning("duplicate_slug", f"Slug '{slug}' is already published or reserved.", blocking=True))
        blocking = True
    if not article.get("hero_image"):
        warnings.append(_warning("missing_hero_image", "No hero image selected; the public page will use the polished fallback.", blocking=False))
    return {
        "status": "failed" if blocking else "passed",
        "warnings": warnings,
        "numeric_claims": numeric_claims,
        "source_link_count": source_link_count,
        "estimated_reading_minutes": max(1, round(len(body.split()) / 220)),
        "labels": labels,
    }


def _internal_language_hits(lowered_text: str) -> list[str]:
    hits: list[str] = []
    labels = [
        ("research request", r"\bresearch request\b"),
        ("supplied research context", r"\bsupplied research context\b"),
        ("supplied materials", r"\bsupplied materials?\b"),
        ("supplied context", r"\bsupplied context\b"),
        ("research configuration", r"\bresearch configuration\b|\bthis configuration\b|\bin this research configuration\b"),
        ("provided comparison confirmation", r"\bprovided comparison confirmation\b"),
        ("user request", r"\buser request\b"),
        ("prompt", r"\bprompt\b"),
        ("generated from", r"\bgenerated from\b"),
        ("model was asked", r"\bmodel was asked\b"),
    ]
    for label, pattern in labels:
        if re.search(pattern, lowered_text):
            hits.append(label)
    return hits


def _missing_data_language_hits(lowered_text: str) -> list[str]:
    hits: list[str] = []
    labels = [
        ("not supplied", r"\bnot supplied\b"),
        ("was supplied", r"\b(?:was|were)\s+supplied\b"),
        ("no reviewed consensus source was supplied", r"\bno reviewed consensus source was supplied\b"),
        ("reviewed materials do not provide", r"\breviewed materials do not provide\b"),
    ]
    for label, pattern in labels:
        if re.search(pattern, lowered_text):
            hits.append(label)
    return hits


def _markdown_structure_issues(markdown: str) -> list[str]:
    issues: list[str] = []
    sections = _markdown_h2_sections(markdown)
    previous_had_body = True
    counts: dict[str, int] = {}
    for section in sections:
        key = _heading_key(section["heading"])
        counts[key] = counts.get(key, 0) + 1
        if _is_placeholder_heading(section["heading"]):
            issues.append("placeholder heading")
        if not section["body"].strip():
            issues.append(f"empty heading: {section['heading']}")
        if not previous_had_body:
            issues.append("consecutive H2 headings without body text")
        previous_had_body = bool(section["body"].strip())
    if any(count > 1 for count in counts.values()):
        issues.append("duplicate headings")
    return _dedupe_strings(issues)


def _markdown_h2_sections(markdown: str) -> list[dict[str, str]]:
    lines = str(markdown or "").replace("\r\n", "\n").replace("\r", "\n").splitlines()
    sections: list[dict[str, str]] = []
    current: dict[str, Any] | None = None
    for line in lines:
        match = re.match(r"^##\s+(.+?)\s*$", line)
        if match:
            if current is not None:
                sections.append({"heading": current["heading"], "body": "\n".join(current["body_lines"]).strip()})
            current = {"heading": match.group(1).strip(), "body_lines": []}
            continue
        if current is not None:
            current["body_lines"].append(line)
    if current is not None:
        sections.append({"heading": current["heading"], "body": "\n".join(current["body_lines"]).strip()})
    return sections


def _available_data_missing_claims(lowered_text: str, context: dict[str, Any]) -> list[str]:
    availability = context.get("data_availability") if isinstance(context.get("data_availability"), dict) else {}
    missing_terms = r"(?:not found|not available|unavailable|missing|could not find|couldn't find|not directly reviewed)"
    checks = {
        "current price": [r"current\s+\w*\s*price", r"share\s+price", r"stock\s+price"],
        "volume": [r"\bvolume\b"],
        "price/volume and technicals": [r"price\s*/\s*volume", r"\btechnicals?\b", r"technical\s+levels?"],
        "revenue consensus": [r"revenue\s+consensus", r"q[1-4]\s+revenue"],
        "eps consensus": [r"eps\s+consensus", r"q[1-4]\s+eps"],
        "gross margin": [r"gross\s+margin"],
        "free cash flow": [r"free\s+cash\s+flow", r"\bfcf\b"],
        "reported institutional activity": [r"reported\s+institutional\s+activity", r"institutional\s+activity"],
        "insider activity": [r"insider\s+activity"],
        "congress activity": [r"congress\s+activity"],
        "government contracts": [r"government\s+contracts?"],
        "valuation data": [r"valuation\s+data", r"\bvaluation\b"],
    }
    contradicted: list[str] = []
    for field, synonyms in checks.items():
        if not availability.get(field):
            continue
        for synonym in synonyms:
            if re.search(rf"{synonym}.{{0,90}}{missing_terms}|{missing_terms}.{{0,90}}{synonym}", lowered_text):
                contradicted.append(field)
                break
    return contradicted


def _conflates_confirmation_score_with_data(lowered_text: str) -> bool:
    data_terms = r"(?:price/?volume|price and volume|fundamentals|reported institutional activity|congress activity|insider activity|government contracts|options flow|macro positioning|underlying data|data categories)"
    patterns = [
        rf"confirmation score\s+(?:is|equals|represents|is derived from|is based on|comes from)\s+.{{0,80}}{data_terms}",
        rf"{data_terms}.{{0,80}}\s+(?:are|is)\s+the\s+confirmation score",
        r"confirmation score\s+and\s+underlying data\s+are\s+the\s+same",
    ]
    return any(re.search(pattern, lowered_text) for pattern in patterns)


def _source_link_count(article: dict[str, Any], body: str) -> int:
    urls = set(re.findall(r"https?://[^\s)\]>\"']+", body))
    source_links = article.get("source_links") if isinstance(article.get("source_links"), list) else []
    for item in source_links:
        if isinstance(item, dict) and str(item.get("url") or "").startswith(("http://", "https://")):
            urls.add(str(item["url"]))
    return len(urls)


def _warning(code: str, message: str, *, blocking: bool) -> dict[str, Any]:
    return {"code": code, "message": message, "blocking": blocking}


def _context_has_numbers(context: dict[str, Any]) -> bool:
    return bool(re.search(r"\d", json.dumps(context, default=str)))


def _duplicate_slug(slug: str, draft_id: str | None = None) -> bool:
    if slug in PUBLISHED_STATIC_SLUGS:
        return True
    store = _read_store()
    for draft in store.get("drafts", []):
        if draft.get("id") == draft_id:
            continue
        if draft.get("status") == "published" and draft.get("article", {}).get("slug") == slug:
            return True
    return False


def _new_draft(admin: UserAccount, config: dict[str, Any], context: dict[str, Any], article: dict[str, Any], validation: dict[str, Any], *, elapsed_ms: int) -> dict[str, Any]:
    created = _now()
    slug = _slugify(str(article.get("slug") or article.get("title") or config["ticker"]), fallback=f"{config['ticker'].lower()}-research-brief")
    article = deepcopy(article)
    article["slug"] = slug
    return {
        "id": f"rb_{int(time.time() * 1000)}",
        "status": "draft",
        "created_by": admin.id,
        "created_by_email": getattr(admin, "email", None),
        "created_at": created,
        "updated_at": created,
        "published_at": None,
        "model": article.get("_model") or config.get("selected_model") or research_brief_model(None),
        "prompt_version": RESEARCH_BRIEF_PROMPT_VERSION,
        "research_context_timestamp": context.get("generated_at"),
        "primary_ticker": context["primary"]["identity"]["symbol"],
        "comparison_ticker": (config.get("comparison_tickers") or [None])[0],
        "comparison_tickers": list(config.get("comparison_tickers") or []),
        "config": config,
        "article": {k: v for k, v in article.items() if not str(k).startswith("_")},
        "validation": validation,
        "diagnostics": {
            "elapsed_ms": elapsed_ms,
            "storage": "local_json",
            "usage": article.get("_generation_usage") or {},
        },
        "research_context": context,
    }


def generate_thumbnail_asset(db: Session, config: dict[str, Any], article: dict[str, Any]) -> dict[str, Any]:
    title = str(article.get("title") or config.get("ticker") or "Walnut research").strip()
    conclusion = _article_conclusion(article)
    asset_type = _thumbnail_asset_type(config)
    prompt = _research_thumbnail_prompt(title=title, ticker=str(config.get("ticker") or ""), conclusion=conclusion, asset_type=asset_type)
    model = os.getenv(AI_MARKETING_IMAGE_MODEL, "").strip() or DEFAULT_AI_MARKETING_IMAGE_MODEL
    asset = {
        "image_title": title[:120],
        "image_prompt": prompt,
        "asset_type": asset_type,
        "url": "",
        "thumbnail_url": "",
        "source_notes": f"Prompt generated from final post conclusion. Image model: {model}. Official Walnut logo should be overlaid or preserved.",
        "created_at": _now(),
    }
    if not _env_flag_enabled(AI_MARKETING_IMAGE_GENERATION_ENABLED):
        return asset
    api_key = resolved_setting_value(db, OPENAI_API_KEY)
    if not api_key:
        asset["source_notes"] += " Image generation skipped because OPENAI_API_KEY is not configured."
        return asset
    try:
        response = requests.post(
            IMAGES_ENDPOINT,
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            json={
                "model": model,
                "prompt": prompt,
                "size": os.getenv(AI_MARKETING_IMAGE_SIZE, "").strip() or DEFAULT_AI_MARKETING_IMAGE_SIZE,
                "quality": os.getenv(AI_MARKETING_IMAGE_QUALITY, "").strip() or DEFAULT_AI_MARKETING_IMAGE_QUALITY,
                "output_format": "jpeg",
            },
            timeout=_env_float(RESEARCH_BRIEF_THUMBNAIL_TIMEOUT_SECONDS, 45.0),
        )
    except requests.RequestException:
        asset["source_notes"] += " Image generation request failed; prompt is saved for retry."
        return asset
    if response.status_code >= 400:
        asset["source_notes"] += " Image generation failed; prompt is saved for retry."
        return asset
    data = response.json()
    image_data = (data.get("data") or [{}])[0] if isinstance(data, dict) else {}
    b64_image = str(image_data.get("b64_json") or "").strip()
    if b64_image:
        data_uri = f"data:image/jpeg;base64,{b64_image}"
        asset["url"] = data_uri
        asset["thumbnail_url"] = data_uri
    return asset


def _env_flag_enabled(name: str) -> bool:
    return os.getenv(name, "").strip().lower() in {"1", "true", "yes", "on"}


def _article_conclusion(article: dict[str, Any]) -> str:
    sections = article.get("sections") if isinstance(article.get("sections"), list) else []
    for section in sections:
        heading = str((section or {}).get("heading") or "").lower()
        if "conclusion" in heading or "judgment" in heading or "call" in heading:
            return str((section or {}).get("body_markdown") or "")[:700]
    if sections:
        return str((sections[-1] or {}).get("body_markdown") or "")[:700]
    return str(article.get("summary") or article.get("preview_body") or "")[:700]


def _thumbnail_asset_type(config: dict[str, Any]) -> str:
    section_format = str(config.get("section_format") or "")
    audience = str(config.get("intended_audience") or "")
    if "X Thread" in section_format:
        return "X thumbnail"
    if "Reddit" in section_format or "Reddit" in audience:
        return "Reddit DD cover image"
    if "Internal Analyst Note" in section_format:
        return "research hero image"
    return "Insights card image"


def _research_thumbnail_prompt(*, title: str, ticker: str, conclusion: str, asset_type: str) -> str:
    ticker_text = f"${ticker.upper()}" if ticker else "the ticker"
    return (
        f"Create a clean, readable dark Walnut Markets {asset_type} for {ticker_text}. "
        "Use the real Walnut logo area only; do not invent a Walnut logo, company logo, fake data, fake chart, source footer, or tiny unreadable text. "
        "Branding: dark finance editorial background, emerald/teal accents, generous negative space, mobile-readable title. "
        f"Readable title: {title}. "
        f"Generate the image from this final post conclusion, not the raw prompt: {conclusion[:600]}. "
        "The image should look premium and specific to the market story without generic AI finance art or clutter."
    )


def _mock_article(config: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
    symbol = context["primary"]["identity"]["symbol"]
    company = context["primary"]["identity"].get("company_name") or symbol
    question = config["research_question"]
    source_links = (context.get("external_research") or {}).get("reviewed_sources") or [
        {"label": "SEC EDGAR company search", "url": f"https://www.sec.gov/edgar/search/#/q={symbol}&dateRange=all", "source_type": "filing_search"},
        {"label": f"{symbol} Nasdaq market activity", "url": f"https://www.nasdaq.com/market-activity/stocks/{symbol.lower()}", "source_type": "reputable_market_source"},
    ]
    body = (
        f"{company} ({symbol}) deserves a focused research review because the current question is specific: {question}\n\n"
        "The available Walnut data should be read as evidence, not as a recommendation. The confirmation score is a separate Walnut signal, while fundamentals, price action, public filings, reported institutional activity, government contracts, and event history are the underlying data.\n\n"
        "The strongest constructive case is that available company and market data still support a credible thesis. The strongest risk case is that missing or stale data can hide a change in the cycle, and unavailable data should not be treated as bearish or bullish by itself.\n\n"
        "What matters next is whether the observable data improves or deteriorates: fundamentals, tape confirmation, public filings, reported activity, catalysts, and risk signals. Research only. Not investment advice.\n\n"
        "Sources:\n"
        + "\n".join(f"- [{item.get('label')}]({item.get('url')})" for item in source_links[:2])
    )
    return {
        "title": f"{symbol} DD: {question.rstrip('?')}",
        "slug": f"{symbol.lower()}-dd-draft",
        "subtitle": f"A Walnut research brief on {company}.",
        "summary": f"Draft research brief for {symbol}. Research only. Not investment advice.",
        "preview_body": f"{symbol} has a mixed setup: available data supports review, but missing fields keep the conclusion cautious. Research only. Not investment advice.",
        "judgment": "mixed",
        "confidence": "medium",
        "primary_ticker": symbol,
        "comparison_tickers": list(config.get("comparison_tickers") or []),
        "category": context["primary"]["identity"].get("sector") or "Research",
        "reading_minutes": 4,
        "sections": [
            {"key": "thesis", "heading": "Executive thesis", "body_markdown": body},
            {"key": "watch", "heading": "What to watch next", "body_markdown": "Watch the next fundamentals refresh, price/volume confirmation, and new public filings before changing the thesis."},
        ],
        "key_points": ["Separate confirmation score from underlying data.", "Treat missing data as unavailable, not directional."],
        "catalysts": ["Next earnings update", "Material public filing or contract update"],
        "risks": ["Cycle deterioration", "Stale or incomplete data"],
        "watch_items": ["Fundamentals refresh", "Price/volume confirmation", "Public filings"],
        "data_freshness": [context.get("generated_at") or ""],
        "missing_data_notes": context.get("missing_data_notes") or [],
        "source_links": source_links[:4],
        "suggested_card": {
            "title": f"{symbol} DD research brief",
            "description": f"A research-only Walnut DD brief for {symbol}.",
            "judgment": "mixed",
            "tickers": [symbol],
        },
        "seo": {"title": f"{symbol} DD | Walnut Research", "description": f"Walnut research brief for {symbol}. Not investment advice."},
    }


def list_drafts(status: str | None = None, db: Session | None = None) -> dict[str, Any]:
    if db is not None:
        try:
            drafts = [_draft_with_comparison_tickers(draft) for draft in _db_drafts(db, status=status)]
            return {"items": sorted(drafts, key=lambda item: item.get("updated_at") or "", reverse=True)}
        except Exception as exc:
            logger.warning("research_brief_db_list_drafts_failed error=%s", exc.__class__.__name__)
    with _STORE_LOCK:
        drafts = deepcopy(_read_store().get("drafts", []))
    drafts = [_draft_with_comparison_tickers(draft) for draft in drafts]
    if status and status != "all":
        drafts = [draft for draft in drafts if draft.get("status") == status]
    return {"items": sorted(drafts, key=lambda item: item.get("updated_at") or "", reverse=True)}


def get_draft(draft_id: str, db: Session | None = None) -> dict[str, Any]:
    if db is not None:
        try:
            draft = _db_draft(db, draft_id)
            if draft:
                return _draft_with_comparison_tickers(deepcopy(draft))
        except Exception as exc:
            logger.warning("research_brief_db_get_draft_failed draft_id=%s error=%s", draft_id, exc.__class__.__name__)
    for draft in _read_store().get("drafts", []):
        if draft.get("id") == draft_id:
            return _draft_with_comparison_tickers(deepcopy(draft))
    raise HTTPException(status_code=404, detail="Research brief draft not found.")


def _draft_with_comparison_tickers(draft: dict[str, Any]) -> dict[str, Any]:
    config = draft.setdefault("config", {})
    source_config = dict(config)
    if draft.get("comparison_tickers") and not source_config.get("comparison_tickers"):
        source_config["comparison_tickers"] = draft.get("comparison_tickers")
    if draft.get("comparison_ticker") and not source_config.get("comparison_ticker"):
        source_config["comparison_ticker"] = draft.get("comparison_ticker")
    comparison_tickers = normalize_comparison_tickers(source_config)
    config["comparison_tickers"] = comparison_tickers
    config["comparison_ticker"] = comparison_tickers[0] if comparison_tickers else None
    draft["comparison_tickers"] = comparison_tickers
    draft["comparison_ticker"] = comparison_tickers[0] if comparison_tickers else None
    article = draft.get("article")
    if isinstance(article, dict) and not isinstance(article.get("comparison_tickers"), list):
        article["comparison_tickers"] = comparison_tickers
    return draft


def update_draft(admin: UserAccount, draft_id: str, article_patch: dict[str, Any], status: str | None = None, db: Session | None = None) -> dict[str, Any]:
    if db is not None:
        draft = _db_draft(db, draft_id)
        if draft:
            article = draft.setdefault("article", {})
            article.update({k: v for k, v in article_patch.items() if k in article_schema()["properties"] or k in {"hero_image", "thumbnail_asset"}})
            article["slug"] = _slugify(str(article.get("slug") or article.get("title") or draft.get("primary_ticker")), fallback=f"{draft.get('primary_ticker', 'brief').lower()}-research-brief")
            draft["article"] = sanitize_research_brief_article(article, draft.get("config") or {}, draft.get("research_context") or {})
            if status:
                draft["status"] = _normalize_status(status)
            draft["validation"] = validate_article(draft["article"], draft.get("research_context") or {}, draft_id=draft_id)
            draft["updated_at"] = _now()
            _upsert_db_draft(db, draft)
            return deepcopy(draft)
    with _STORE_LOCK:
        store = _read_store()
        for draft in store.get("drafts", []):
            if draft.get("id") == draft_id:
                article = draft.setdefault("article", {})
                article.update({k: v for k, v in article_patch.items() if k in article_schema()["properties"] or k in {"hero_image", "thumbnail_asset"}})
                article["slug"] = _slugify(str(article.get("slug") or article.get("title") or draft.get("primary_ticker")), fallback=f"{draft.get('primary_ticker', 'brief').lower()}-research-brief")
                draft["article"] = sanitize_research_brief_article(article, draft.get("config") or {}, draft.get("research_context") or {})
                if status:
                    draft["status"] = _normalize_status(status)
                draft["validation"] = validate_article(draft["article"], draft.get("research_context") or {}, draft_id=draft_id)
                draft["updated_at"] = _now()
                _append_audit(store, action="save", admin=admin, draft_id=draft_id)
                _write_store(store)
                return deepcopy(draft)
    raise HTTPException(status_code=404, detail="Research brief draft not found.")


def refresh_research_sources(db: Session, admin: UserAccount, draft_id: str) -> dict[str, Any]:
    draft = _db_draft(db, draft_id)
    if draft:
        config = validate_config(draft.get("config") or {})
        symbol = str(draft.get("primary_ticker") or config.get("ticker") or "").upper()
        identity = ((draft.get("research_context") or {}).get("primary") or {}).get("identity") or {"symbol": symbol}
        external = discover_external_research(symbol, identity, mode=config.get("external_research_mode") or "Standard")
        context = draft.setdefault("research_context", {})
        context["external_research"] = external
        context["external_research_mode"] = external.get("mode")
        context["generated_at"] = _now()
        context["data_availability"] = _research_data_availability(context.get("primary") or {}, external)
        filtered_missing = _filter_missing_data_notes(external.get("missing_data_notes") or [], context["data_availability"])
        existing_missing = context.get("missing_data_notes") if isinstance(context.get("missing_data_notes"), list) else []
        context["missing_data_notes"] = _filter_missing_data_notes([*existing_missing, *filtered_missing], context["data_availability"])
        article = draft.setdefault("article", {})
        article["missing_data_notes"] = _filter_missing_data_notes([*(article.get("missing_data_notes") or []), *filtered_missing], context["data_availability"])
        article["source_links"] = _dedupe_source_links([*(article.get("source_links") or []), *(external.get("reviewed_sources") or [])])
        draft["article"] = sanitize_research_brief_article(article, config, context)
        draft["validation"] = validate_article(draft["article"], context, draft_id=draft_id)
        draft["updated_at"] = _now()
        _upsert_db_draft(db, draft)
        return deepcopy(draft)

    with _STORE_LOCK:
        store = _read_store()
        for draft in store.get("drafts", []):
            if draft.get("id") != draft_id:
                continue
            config = validate_config(draft.get("config") or {})
            symbol = str(draft.get("primary_ticker") or config.get("ticker") or "").upper()
            identity = ((draft.get("research_context") or {}).get("primary") or {}).get("identity") or {"symbol": symbol}
            external = discover_external_research(symbol, identity, mode=config.get("external_research_mode") or "Standard")
            context = draft.setdefault("research_context", {})
            context["external_research"] = external
            context["external_research_mode"] = external.get("mode")
            context["generated_at"] = _now()
            context["data_availability"] = _research_data_availability(context.get("primary") or {}, external)
            filtered_missing = _filter_missing_data_notes(external.get("missing_data_notes") or [], context["data_availability"])
            existing_missing = context.get("missing_data_notes") if isinstance(context.get("missing_data_notes"), list) else []
            context["missing_data_notes"] = _filter_missing_data_notes([*existing_missing, *filtered_missing], context["data_availability"])
            article = draft.setdefault("article", {})
            article["missing_data_notes"] = _filter_missing_data_notes([*(article.get("missing_data_notes") or []), *filtered_missing], context["data_availability"])
            article["source_links"] = _dedupe_source_links([*(article.get("source_links") or []), *(external.get("reviewed_sources") or [])])
            draft["article"] = sanitize_research_brief_article(article, config, context)
            draft["validation"] = validate_article(draft["article"], context, draft_id=draft_id)
            draft["updated_at"] = _now()
            _append_audit(store, action="refresh_sources", admin=admin, draft_id=draft_id, metadata={"mode": external.get("mode")})
            _write_store(store)
            return deepcopy(draft)
    raise HTTPException(status_code=404, detail="Research brief draft not found.")


def _dedupe_source_links(values: list[Any]) -> list[dict[str, str]]:
    links: list[dict[str, str]] = []
    seen: set[str] = set()
    for item in values:
        if not isinstance(item, dict):
            continue
        url = str(item.get("url") or "").strip()
        if not url or url in seen:
            continue
        seen.add(url)
        links.append(
            {
                "label": str(item.get("label") or url).strip()[:180],
                "url": url,
                "source_type": str(item.get("source_type") or "source").strip()[:80],
            }
        )
    return links[:12]


def publish_draft(admin: UserAccount, draft_id: str, *, confirm: bool, db: Session | None = None) -> dict[str, Any]:
    if not confirm:
        raise HTTPException(status_code=422, detail="Publish requires explicit confirmation.")
    if db is not None:
        draft = _db_draft(db, draft_id)
        if draft:
            draft["article"] = sanitize_research_brief_article(draft.get("article") or {}, draft.get("config") or {}, draft.get("research_context") or {})
            validation = validate_article(draft.get("article") or {}, draft.get("research_context") or {}, draft_id=draft_id)
            if validation["status"] != "passed":
                draft["validation"] = validation
                _upsert_db_draft(db, draft)
                raise HTTPException(status_code=422, detail="Resolve validation failures before publishing.")
            draft["status"] = "published"
            draft["published_at"] = draft.get("published_at") or _now()
            draft["updated_at"] = _now()
            draft["validation"] = validation
            _upsert_db_draft(db, draft)
            return deepcopy(draft)
    with _STORE_LOCK:
        store = _read_store()
        for draft in store.get("drafts", []):
            if draft.get("id") == draft_id:
                draft["article"] = sanitize_research_brief_article(draft.get("article") or {}, draft.get("config") or {}, draft.get("research_context") or {})
                validation = validate_article(draft.get("article") or {}, draft.get("research_context") or {}, draft_id=draft_id)
                if validation["status"] != "passed":
                    draft["validation"] = validation
                    _write_store(store)
                    raise HTTPException(status_code=422, detail="Resolve validation failures before publishing.")
                draft["status"] = "published"
                draft["published_at"] = draft.get("published_at") or _now()
                draft["updated_at"] = _now()
                draft["validation"] = validation
                _append_audit(store, action="publish", admin=admin, draft_id=draft_id)
                _write_store(store)
                return deepcopy(draft)
    raise HTTPException(status_code=404, detail="Research brief draft not found.")


def unpublish_draft(admin: UserAccount, draft_id: str, *, confirm: bool, db: Session | None = None) -> dict[str, Any]:
    if not confirm:
        raise HTTPException(status_code=422, detail="Unpublish requires explicit confirmation.")
    if db is not None:
        draft = _db_draft(db, draft_id)
        if draft:
            draft["status"] = "unpublished"
            draft["updated_at"] = _now()
            _upsert_db_draft(db, draft)
            return deepcopy(draft)
    with _STORE_LOCK:
        store = _read_store()
        for draft in store.get("drafts", []):
            if draft.get("id") == draft_id:
                draft["status"] = "unpublished"
                draft["updated_at"] = _now()
                _append_audit(store, action="unpublish", admin=admin, draft_id=draft_id)
                _write_store(store)
                return deepcopy(draft)
    raise HTTPException(status_code=404, detail="Research brief draft not found.")


def delete_draft(admin: UserAccount, draft_id: str, *, confirm_text: str, db: Session | None = None) -> dict[str, Any]:
    if confirm_text != "DELETE":
        raise HTTPException(status_code=422, detail="Delete requires typing DELETE.")
    if db is not None:
        ensure_research_brief_store_schema(db)
        result = db.execute(text("DELETE FROM research_brief_drafts WHERE id = :id"), {"id": draft_id})
        db.commit()
        if getattr(result, "rowcount", 0):
            return {"ok": True, "deleted": draft_id}
    with _STORE_LOCK:
        store = _read_store()
        before = len(store.get("drafts", []))
        store["drafts"] = [draft for draft in store.get("drafts", []) if draft.get("id") != draft_id]
        if len(store["drafts"]) == before:
            raise HTTPException(status_code=404, detail="Research brief draft not found.")
        _append_audit(store, action="delete", admin=admin, draft_id=draft_id)
        _write_store(store)
    return {"ok": True, "deleted": draft_id}


def _normalize_status(status: str) -> str:
    normalized = str(status or "").strip().lower().replace("-", "_")
    if normalized not in STATUS_OPTIONS:
        raise HTTPException(status_code=422, detail="Unsupported draft status.")
    return normalized


def published_cards(db: Session | None = None) -> dict[str, Any]:
    if db is not None:
        try:
            drafts = [draft for draft in _db_drafts(db, status="published") if draft.get("status") == "published"]
        except Exception as exc:
            logger.warning("research_brief_db_published_cards_failed error=%s", exc.__class__.__name__)
            drafts = [draft for draft in _read_store().get("drafts", []) if draft.get("status") == "published"]
    else:
        drafts = [draft for draft in _read_store().get("drafts", []) if draft.get("status") == "published"]
    cards = []
    for draft in drafts:
        article = draft.get("article") or {}
        suggested = article.get("suggested_card") if isinstance(article.get("suggested_card"), dict) else {}
        slug = article.get("slug")
        if not slug:
            continue
        cards.append(
            {
                "slug": slug,
                "route": f"/research/{slug}",
                "title": suggested.get("title") or article.get("title") or slug,
                "description": suggested.get("description") or article.get("summary") or "",
                "tickers": suggested.get("tickers") or [draft.get("primary_ticker")],
                "category": article.get("category") or "Research",
                "judgment": suggested.get("judgment") or article.get("judgment") or "mixed",
                "publishedAt": (draft.get("published_at") or draft.get("updated_at") or "")[:10],
                "readingMinutes": article.get("reading_minutes") or draft.get("validation", {}).get("estimated_reading_minutes") or 8,
                "generated": True,
            }
        )
    return {"items": cards}


def published_article(slug: str, db: Session | None = None) -> dict[str, Any]:
    normalized = _slugify(slug, fallback=slug)
    if db is not None:
        try:
            ensure_research_brief_store_schema(db)
            row = db.execute(
                text("SELECT payload_json FROM research_brief_drafts WHERE status = 'published' AND slug = :slug LIMIT 1"),
                {"slug": normalized},
            ).mappings().first()
            payload = _load_json(row["payload_json"]) if row else None
            if isinstance(payload, dict):
                return deepcopy(payload)
        except Exception as exc:
            logger.warning("research_brief_db_published_article_failed slug=%s error=%s", normalized, exc.__class__.__name__)
    for draft in _read_store().get("drafts", []):
        article = draft.get("article") or {}
        if draft.get("status") == "published" and article.get("slug") == normalized:
            return deepcopy(draft)
    raise HTTPException(status_code=404, detail="Research brief not found.")
