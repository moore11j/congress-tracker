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
    resolved_setting_value,
)

logger = logging.getLogger(__name__)

OPENAI_RESPONSES_ENDPOINT = "https://api.openai.com/v1/responses"
DEFAULT_WALNUT_TAKE_MODEL = "gpt-5.6-sol"
VALID_BIASES = {"bullish", "bearish", "neutral"}


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
                "walnut_summary": _clean_text(generated_item.get("summary"), limit=220) or item.get("walnut_summary"),
                "walnut_take_bias": _clean_bias(generated_item.get("bias")),
                "walnut_take": _clean_text(generated_item.get("take"), limit=320) or item.get("walnut_take"),
                "walnut_take_source": "openai",
                "walnut_take_model": _walnut_take_model(db),
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
            "The take should explain the market read in one or two compact sentences.",
            "Do not provide trading instructions, price targets, guarantees, or hype.",
            "Do not invent facts beyond the title, summary, ticker, source, and existing market read.",
            "Return only valid JSON with this exact shape:",
            '{"items":[{"id":"article id","summary":"one sentence","bias":"bullish|bearish|neutral","take":"Walnut take text"}]}',
            "Articles:",
            json.dumps(compact_articles, sort_keys=True),
        ]
    )


def _fallback_take(item: dict[str, Any]) -> dict[str, Any]:
    summary = _clean_text(item.get("summary"), limit=220) or _clean_text(item.get("title"), limit=220) or "Summary unavailable."
    bias = _clean_bias(item.get("market_read"))
    return {
        "walnut_summary": summary,
        "walnut_take_bias": bias,
        "walnut_take": _fallback_take_text(item, bias=bias),
        "walnut_take_source": "fallback",
    }


def _fallback_take_text(item: dict[str, Any], *, bias: str) -> str:
    summary = _clean_text(item.get("summary"), limit=220)
    if summary:
        return summary
    if bias == "bullish":
        return "Positive operating signal, but follow-through needs confirmation from fundamentals and broader market context."
    if bias == "bearish":
        return "Negative market signal, but the durability depends on whether the pressure spreads beyond the initial headline."
    return "The market impact is not clear from the available article data."


def _has_openai_take(item: dict[str, Any] | None) -> bool:
    if not isinstance(item, dict):
        return False
    return (
        item.get("walnut_take_source") == "openai"
        and isinstance(item.get("walnut_take"), str)
        and bool(str(item.get("walnut_take")).strip())
        and _clean_bias(item.get("walnut_take_bias")) in VALID_BIASES
    )


def _merge_take(item: dict[str, Any], cached: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(cached, dict):
        return item
    keys = ("walnut_summary", "walnut_take_bias", "walnut_take", "walnut_take_source", "walnut_take_model", "walnut_take_generated_at")
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


def _clean_text(value: Any, *, limit: int) -> str:
    text = " ".join(str(value or "").split())
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 3)].rstrip() + "..."


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
