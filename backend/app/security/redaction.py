from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any
from urllib.parse import SplitResult, urlsplit, urlunsplit

REDACTED = "[REDACTED]"

_SENSITIVE_KEY_PARTS = (
    "authorization",
    "bearer",
    "cookie",
    "database_url",
    "db_url",
    "secret",
    "token",
    "api_key",
    "apikey",
    "password",
    "passwd",
    "stripe",
    "fmp",
    "massive",
    "polygon",
)
_USERINFO_RE = re.compile(r"//([^/\s@]+):([^/\s@]*)@")
_QUERY_RE = re.compile(r"\?.*$")


def redact_secret_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, str) and value == "":
        return ""
    return REDACTED


def _is_sensitive_key(key: Any) -> bool:
    normalized = str(key or "").strip().lower().replace("-", "_")
    return any(part in normalized for part in _SENSITIVE_KEY_PARTS)


def redact_url(url: str | None) -> str | None:
    if url is None:
        return None
    if url == "":
        return ""
    try:
        parsed = urlsplit(str(url))
    except Exception:
        return "<unparseable-url>"

    if not parsed.scheme:
        return _QUERY_RE.sub("?[REDACTED]", _USERINFO_RE.sub(f"//{REDACTED}:{REDACTED}@", str(url)))

    netloc = parsed.hostname or ""
    if parsed.port is not None:
        netloc = f"{netloc}:{parsed.port}"
    sanitized = SplitResult(parsed.scheme, netloc, parsed.path, "", "")
    return urlunsplit(sanitized)


def redact_database_url(url: str | None) -> str | None:
    return redact_url(url)


def safe_config_for_log(mapping: Mapping[str, Any] | None) -> dict[str, Any]:
    if mapping is None:
        return {}
    safe: dict[str, Any] = {}
    for key, value in mapping.items():
        if _is_sensitive_key(key):
            safe[str(key)] = redact_database_url(value) if "url" in str(key).lower() else redact_secret_value(value)
        else:
            safe[str(key)] = value
    return safe
