"""Deterministic route checks; never fetch arbitrary model-generated URLs."""
from __future__ import annotations

import re
from urllib.parse import urlsplit

WALNUT_HOSTS = {"walnutmarkets.com", "www.walnutmarkets.com", "app.walnutmarkets.com"}
LEGACY_TICKER_LINK = re.compile(
    r"(?<![\w/])(?:https?://(?:www\.|app\.)?walnutmarkets\.com)?"
    r"/ticker/([A-Za-z0-9.^-]+)/(?:earnings|financials)/?(?:\?[^\s)\]]*)?(?:\#[^\s)\]]*)?(?=$|[\s)\]])",
    re.IGNORECASE,
)


def repair_research_links(value):
    """Repair known legacy destinations in a copy of article fields."""
    if isinstance(value, str):
        return LEGACY_TICKER_LINK.sub(lambda m: f"https://app.walnutmarkets.com/ticker/{m[1].upper()}#financials", value)
    if isinstance(value, list):
        return [repair_research_links(item) for item in value]
    if isinstance(value, dict):
        return {key: repair_research_links(item) for key, item in value.items()}
    return value


def invalid_ticker_links(article: dict) -> list[str]:
    """Ticker pages have tabs, not nested routes. Unknown subpages block publication."""
    urls = [str(item.get("url") or "") for item in (article.get("source_links") or []) if isinstance(item, dict)]
    body = "\n".join(str(item.get("body_markdown") or "") for item in (article.get("sections") or []) if isinstance(item, dict))
    urls.extend(re.findall(r"\]\(([^\s)]+)\)", body))
    urls.extend(re.findall(r"https?://[^\s<>)]*", body))
    invalid = set()
    for value in urls:
        try:
            parsed = urlsplit(value)
        except ValueError:
            continue
        if parsed.netloc and parsed.hostname not in WALNUT_HOSTS:
            continue
        if parsed.path.startswith("/ticker/") and not re.fullmatch(r"/ticker/[A-Za-z0-9.^-]+/?", parsed.path):
            invalid.add(value)
    return sorted(invalid)


def imprecise_release_sources(article: dict) -> list[str]:
    """Flag a specific release label backed only by a site's homepage."""
    labels = []
    for item in article.get("source_links", []) or []:
        if not isinstance(item, dict):
            continue
        label = str(item.get("label") or item.get("title") or "")
        try:
            url = urlsplit(str(item.get("url") or ""))
        except ValueError:
            continue
        if url.scheme in {"http", "https"} and url.path in {"", "/"} and re.search(r"\b(?:Q[1-4]|fiscal|quarter)\b", label, re.I) and re.search(r"\b(?:results|release)\b", label, re.I):
            labels.append(label)
    return labels
