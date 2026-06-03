from __future__ import annotations

import re
from typing import Any
from urllib.parse import urlparse

VARIABLE_PATTERN = re.compile(r"{{\s*([a-zA-Z0-9_]+)\s*}}")
LINK_VARIABLES = {"verification_url", "reset_url", "statement_url"}


def render_template_string(template: str, context: dict[str, Any], allowed_variables: list[str]) -> str:
    allowed = set(allowed_variables)
    referenced = set(VARIABLE_PATTERN.findall(template or ""))
    unknown = sorted(referenced - allowed)
    if unknown:
        raise ValueError(f"Template contains unsupported variables: {', '.join(unknown)}")

    _validate_link_context(context)

    def replace(match: re.Match[str]) -> str:
        key = match.group(1)
        value = context.get(key)
        if value is None:
            return ""
        return str(value)

    return VARIABLE_PATTERN.sub(replace, template or "")


def _validate_link_context(context: dict[str, Any]) -> None:
    for key in LINK_VARIABLES:
        value = context.get(key)
        if value is None or value == "":
            continue
        parsed = urlparse(str(value))
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ValueError(f"{key} must be an http or https URL.")
