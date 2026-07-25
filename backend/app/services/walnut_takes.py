from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

import requests
from sqlalchemy.orm import Session

from app.services.ai_marketing import (
    AI_MARKETING_MODEL,
    OPENAI_API_KEY,
    _record_openai_usage_cost,
    _rewrite_public_walnut_voice,
    resolved_setting_value,
)

logger = logging.getLogger(__name__)

OPENAI_RESPONSES_ENDPOINT = "https://api.openai.com/v1/responses"
DEFAULT_WALNUT_TAKE_MODEL = "gpt-5.6-sol"
VALID_BIASES = {"bullish", "bearish", "neutral"}
WALNUT_TAKE_MAX_CHARS = 125
WALNUT_SUMMARY_MAX_CHARS = 190
WALNUT_TAKE_PROMPT_VERSION = "market_read_v4"

BULLISH_READ_PHRASES = (
    "stabilize trade",
    "stabilise trade",
    "stabilize relations",
    "stabilise relations",
    "trade relations stabilize",
    "stable trade relations",
    "tariff relief",
    "tariff pause",
    "trade deal",
    "trade progress",
    "opposite of an ai slowdown",
    "opposite of ai slowdown",
    "coming next in ai",
    "ai demand",
    "continued ai demand",
    "demand remains strong",
    "demand is strong",
    "accelerating ai",
    "ai buildout continues",
)

BEARISH_READ_PHRASES = (
    "beyond stretched",
    "stretched valuation",
    "stretched valuations",
    "valuations are stretched",
    "valuation pressure",
    "overvalued",
    "weak cash flow",
    "weaker cash flow",
    "cash flow is weakening",
    "cash flow weakening",
    "free cash flow is weakening",
    "bond market anxiety",
    "market anxiety is growing",
    "anxiety is growing",
    "capex budget",
    "capex budgets",
    "spending worries",
    "spending concern",
    "investors worry",
    "investors are growing uneasy",
    "growing uneasy",
    "higher discount-rate concern",
    "margin risk",
    "geopolitical risk",
    "risk-off",
)


def enrich_walnut_takes(
    db: Session,
    items: list[dict[str, Any]],
    *,
    previous_items: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    previous_by_key = {
        key: item
        for item in previous_items or []
        if isinstance(item, dict) and (key := _article_cache_key(item))
    }
    enriched: list[dict[str, Any]] = []
    missing: list[dict[str, Any]] = []

    for item in items:
        if not isinstance(item, dict):
            continue
        cached = previous_by_key.get(_article_cache_key(item))
        if _has_openai_take(cached):
            enriched.append(_merge_take(item, cached))
            continue
        if _has_openai_take(item):
            enriched.append(item)
            continue
        enriched_item = {**item, **_fallback_take(item)}
        enriched.append(enriched_item)
        missing.append(enriched_item)

    if not missing:
        return enriched

    api_key = resolved_setting_value(db, OPENAI_API_KEY)
    if not api_key:
        logger.info("walnut_takes_openai_skipped reason=missing_key count=%s", len(missing))
        return enriched

    try:
        generated = _generate_openai_takes(db, api_key=api_key, articles=missing)
    except Exception:
        logger.exception("walnut_takes_openai_failed count=%s", len(missing))
        return enriched

    generated_by_id = {item["id"]: item for item in generated if isinstance(item.get("id"), str)}
    generated_at = datetime.now(timezone.utc).isoformat()
    output: list[dict[str, Any]] = []
    for item in enriched:
        article_id = _article_id(item)
        generated_item = generated_by_id.get(article_id)
        if not generated_item:
            output.append(item)
            continue
        output.append(
            {
                **item,
                "walnut_summary": _clean_text(generated_item.get("summary"), limit=WALNUT_SUMMARY_MAX_CHARS) or item.get("walnut_summary"),
                "walnut_take_bias": _calibrated_bias(item, generated_item.get("bias"), generated_item=generated_item),
                "walnut_take": _clean_take_text(item, generated_item=generated_item) or item.get("walnut_take"),
                "walnut_take_source": "openai",
                "walnut_take_model": _walnut_take_model(db),
                "walnut_take_prompt_version": WALNUT_TAKE_PROMPT_VERSION,
                "walnut_take_generated_at": generated_at,
            }
        )
    return output


def _generate_openai_takes(db: Session, *, api_key: str, articles: list[dict[str, Any]]) -> list[dict[str, Any]]:
    model = _walnut_take_model(db)
    request_payload = {
        "model": model,
        "input": _prompt(articles),
        "store": False,
        "text": {"verbosity": "low"},
    }
    response = requests.post(
        OPENAI_RESPONSES_ENDPOINT,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        json=request_payload,
        timeout=35,
    )
    if response.status_code >= 400:
        raise RuntimeError(f"OpenAI Walnut Take request failed with status {response.status_code}.")
    data = response.json()
    _record_openai_usage_cost(db, model=model, data=data, feature="walnut_takes", commit=False)
    parsed = _extract_json_payload(_extract_responses_text(data))
    raw_items = parsed.get("items") if isinstance(parsed, dict) else None
    if not isinstance(raw_items, list):
        raise ValueError("OpenAI Walnut Take response did not include items.")
    return [item for item in raw_items if isinstance(item, dict)]


def _prompt(articles: list[dict[str, Any]]) -> str:
    compact_articles = [
        {
            "id": _article_id(item),
            "title": item.get("title"),
            "source": item.get("site") or item.get("source"),
            "published_at": item.get("published_at"),
            "symbol": item.get("symbol"),
            "provider_summary": item.get("summary"),
            "provider_market_read": item.get("market_read"),
        }
        for item in articles
    ]
    return "\n".join(
        [
            "You generate Walnut Takes for a market intelligence news list.",
            "For each article, return a concise factual summary and a market-impact bias.",
            "Allowed bias values: bullish, bearish, neutral.",
            f"The take must be one compact sentence of {WALNUT_TAKE_MAX_CHARS} characters or fewer.",
            "The take must be a complete sentence ending with a period. Never use ellipses or trail off.",
            "Do not fill the character budget. Prefer 45-90 characters when possible.",
            "For broad market articles, bullish means supportive for risk assets; bearish means pressure, risk-off, higher discount-rate concern, or margin/cash-flow risk.",
            "Valuation stretch, spending worries, falling prices, weak cash flow, bond-market anxiety, or AI capex-budget concerns are bearish unless the article gives a clear positive offset.",
            "Constructive trade stabilization, tariff relief, resilient AI demand, or evidence that AI spending is not slowing are bullish unless the article says the benefit failed or reversed.",
            "Neutral is only for genuinely mixed or unclear impact, not for obvious caution or pressure headlines.",
            "The provider_market_read is weak context only; override it when the title or summary implies a different read.",
            "Use first-person plural for our own views. Say 'our take' if needed, not 'Walnut's take.'",
            "Do not provide trading instructions, price targets, guarantees, or hype.",
            "Do not invent facts beyond the title, summary, ticker, source, and existing market read.",
            "Return only valid JSON with this exact shape:",
            '{"items":[{"id":"article id","summary":"one sentence","bias":"bullish|bearish|neutral","take":"Walnut take text"}]}',
            "Articles:",
            json.dumps(compact_articles, sort_keys=True),
        ]
    )


def _fallback_take(item: dict[str, Any]) -> dict[str, Any]:
    summary = _clean_text(item.get("summary"), limit=WALNUT_SUMMARY_MAX_CHARS) or _clean_text(item.get("title"), limit=WALNUT_SUMMARY_MAX_CHARS) or "Summary unavailable."
    bias = _calibrated_bias(item, item.get("market_read"))
    return {
        "walnut_summary": summary,
        "walnut_take_bias": bias,
        "walnut_take": _fallback_take_text(item, bias=bias),
        "walnut_take_source": "fallback",
    }


def _fallback_take_text(item: dict[str, Any], *, bias: str) -> str:
    concise = _concise_take_text(item, bias=bias)
    if concise:
        return concise
    if bias == "bullish":
        return "The headline is bullish, but follow-through needs confirmation."
    if bias == "bearish":
        return "The headline is bearish unless the pressure proves isolated."
    return "The market impact is mixed until clearer data arrives."


def _has_openai_take(item: dict[str, Any] | None) -> bool:
    if not isinstance(item, dict):
        return False
    return (
        item.get("walnut_take_source") == "openai"
        and item.get("walnut_take_prompt_version") == WALNUT_TAKE_PROMPT_VERSION
        and isinstance(item.get("walnut_take"), str)
        and bool(str(item.get("walnut_take")).strip())
        and _clean_bias(item.get("walnut_take_bias")) in VALID_BIASES
    )


def _merge_take(item: dict[str, Any], cached: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(cached, dict):
        return item
    keys = ("walnut_summary", "walnut_take_bias", "walnut_take", "walnut_take_source", "walnut_take_model", "walnut_take_prompt_version", "walnut_take_generated_at")
    return {**item, **{key: cached[key] for key in keys if key in cached}}


def _article_cache_key(item: dict[str, Any]) -> str:
    url = _clean_text(item.get("url"), limit=500)
    if url:
        return f"url:{url}"
    title = _clean_text(item.get("title"), limit=220)
    published = _clean_text(item.get("published_at"), limit=80)
    return f"title:{title}|published:{published}" if title else ""


def _article_id(item: dict[str, Any]) -> str:
    key = _article_cache_key(item)
    return key if key else f"title:{_clean_text(item.get('title'), limit=80) or 'unknown'}"


def _walnut_take_model(db: Session) -> str:
    return resolved_setting_value(db, AI_MARKETING_MODEL) or DEFAULT_WALNUT_TAKE_MODEL


def _clean_bias(value: Any) -> str:
    bias = str(value or "").strip().lower()
    return bias if bias in VALID_BIASES else "neutral"


def _calibrated_bias(item: dict[str, Any], value: Any, *, generated_item: dict[str, Any] | None = None) -> str:
    text = _market_read_text(item, generated_item=generated_item)
    if _contains_any(text, BEARISH_READ_PHRASES):
        return "bearish"
    if _contains_any(text, BULLISH_READ_PHRASES):
        return "bullish"
    return _clean_bias(value)


def _market_read_text(item: dict[str, Any], *, generated_item: dict[str, Any] | None = None) -> str:
    fields = [
        item.get("title"),
        item.get("summary"),
        item.get("walnut_summary"),
        item.get("site"),
        item.get("source"),
        item.get("symbol"),
        item.get("market_read"),
    ]
    if isinstance(generated_item, dict):
        fields.extend([generated_item.get("summary"), generated_item.get("take")])
    return " ".join(str(field or "") for field in fields).lower()


def _contains_any(text: str, phrases: tuple[str, ...]) -> bool:
    return any(phrase in text for phrase in phrases)


def _clean_take_text(item: dict[str, Any], *, generated_item: dict[str, Any]) -> str:
    bias = _calibrated_bias(item, generated_item.get("bias"), generated_item=generated_item)
    text = _rewrite_public_walnut_voice(" ".join(str(generated_item.get("take") or "").split()))
    if _is_complete_sentence(text) and len(text) <= WALNUT_TAKE_MAX_CHARS and "..." not in text:
        return text
    concise = _concise_take_text(item, bias=bias)
    return concise or _complete_sentence_under_limit(text, limit=WALNUT_TAKE_MAX_CHARS)


def _concise_take_text(item: dict[str, Any], *, bias: str) -> str:
    text = _market_read_text(item)
    if _contains_any(text, ("beyond stretched", "stretched valuation", "stretched valuations", "weak cash flow", "weaker cash flow", "cash flow is weakening")):
        return "Stretched valuations and weaker cash flow are bearish for risk assets."
    if _contains_any(text, ("bond market anxiety", "capex budget", "capex budgets", "growing uneasy")):
        return "AI capex anxiety is bearish for credit and risk appetite."
    if _contains_any(text, ("spending worries", "spending concern", "investors worry")):
        return "Tech spending worries are bearish until demand offsets the pressure."
    if _contains_any(text, ("massive attack", "geopolitical risk", "shipping", "escalation")):
        return "Geopolitical escalation is bearish for broad risk sentiment."
    if _contains_any(text, ("stabilize trade", "stabilise trade", "stabilize relations", "stabilise relations", "trade relations")):
        return "Stabilizing trade ties is bullish for policy risk and sentiment."
    if _contains_any(text, ("lisa su", "opposite of an ai slowdown", "opposite of ai slowdown", "coming next in ai")):
        return "Resilient AI demand is bullish for AMD and the AI buildout."
    if bias == "bullish":
        return "The headline is bullish, but follow-through needs confirmation."
    if bias == "bearish":
        return "The headline is bearish unless the pressure proves isolated."
    return "The market impact is mixed until clearer data arrives."


def _is_complete_sentence(text: str) -> bool:
    return bool(text) and text[-1] in ".!?" and "..." not in text


def _complete_sentence_under_limit(value: str, *, limit: int) -> str:
    text = " ".join(str(value or "").split()).replace("...", "")
    if not text:
        return "The market impact is mixed until clearer data arrives."
    sentence_end = max(text.rfind(".", 0, limit + 1), text.rfind("!", 0, limit + 1), text.rfind("?", 0, limit + 1))
    if sentence_end >= 20:
        return text[: sentence_end + 1]
    clipped = text[: max(0, limit - 1)].rstrip()
    last_break = clipped.rfind(" ")
    if last_break > limit * 0.6:
        clipped = clipped[:last_break].rstrip()
    clipped = clipped.rstrip(" ,;:-.!?")
    return f"{clipped}." if clipped else "The market impact is mixed until clearer data arrives."


def _clean_text(value: Any, *, limit: int) -> str:
    text = " ".join(str(value or "").split())
    if len(text) <= limit:
        return text
    clipped = text[: max(0, limit - 1)].rstrip()
    last_break = clipped.rfind(" ")
    if last_break > limit * 0.7:
        clipped = clipped[:last_break].rstrip()
    return clipped.rstrip(" ,;:-.!?") + "."


def _extract_responses_text(data: dict[str, Any]) -> str:
    output_text = data.get("output_text")
    if isinstance(output_text, str) and output_text.strip():
        return output_text.strip()
    texts: list[str] = []
    for item in _walk_dicts(data):
        item_type = str(item.get("type") or "")
        text = item.get("text")
        if item_type in {"output_text", "text"} and isinstance(text, str) and text.strip():
            texts.append(text.strip())
    return "\n".join(dict.fromkeys(texts))


def _walk_dicts(value: Any):
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from _walk_dicts(child)
    elif isinstance(value, list):
        for child in value:
            yield from _walk_dicts(child)


def _extract_json_payload(text: str) -> Any:
    stripped = (text or "").strip()
    if not stripped:
        return None
    candidates = [stripped]
    start = stripped.find("{")
    end = stripped.rfind("}")
    if start >= 0 and end > start:
        candidates.append(stripped[start : end + 1])
    for candidate in candidates:
        try:
            return json.loads(candidate)
        except (TypeError, ValueError):
            continue
    return None
