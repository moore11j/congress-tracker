from __future__ import annotations

import re
from html import escape as html_escape
from typing import Any
from urllib.parse import urlparse

VARIABLE_PATTERN = re.compile(r"{{\s*([a-zA-Z0-9_]+)\s*}}")
TRUSTED_HTML_VARIABLE_PATTERN = re.compile(r"{{{\s*([a-zA-Z0-9_]+)\s*}}}")
LINK_VARIABLES = {"verification_url", "reset_url", "statement_url", "activity_url", "digest_url", "signal_url"}
TRUSTED_HTML_VARIABLES = {
    "items_html",
    "signals_html",
    "congress_trades_html",
    "insider_trades_html",
    "government_contracts_html",
    "institutional_activity_html",
    "upcoming_events_html",
    "market_news_html",
}


def render_template_string(
    template: str,
    context: dict[str, Any],
    allowed_variables: list[str],
    *,
    html: bool = False,
    trusted_html_variables: set[str] | None = None,
) -> str:
    allowed = set(allowed_variables)
    trusted_allowed = trusted_html_variables or TRUSTED_HTML_VARIABLES
    referenced = set(VARIABLE_PATTERN.findall(template or "")) | set(TRUSTED_HTML_VARIABLE_PATTERN.findall(template or ""))
    unknown = sorted(referenced - allowed)
    if unknown:
        raise ValueError(f"Template contains unsupported variables: {', '.join(unknown)}")
    untrusted_html = sorted(set(TRUSTED_HTML_VARIABLE_PATTERN.findall(template or "")) - trusted_allowed)
    if untrusted_html:
        raise ValueError(f"Template contains untrusted HTML variables: {', '.join(untrusted_html)}")

    _validate_link_context(context)

    def replace_trusted_html(match: re.Match[str]) -> str:
        key = match.group(1)
        value = context.get(key)
        if value is None:
            return ""
        return str(value)

    def replace(match: re.Match[str]) -> str:
        key = match.group(1)
        value = context.get(key)
        if value is None:
            return ""
        rendered = str(value)
        return html_escape(rendered, quote=True) if html else rendered

    rendered_template = TRUSTED_HTML_VARIABLE_PATTERN.sub(replace_trusted_html, template or "")
    return VARIABLE_PATTERN.sub(replace, rendered_template)


def _validate_link_context(context: dict[str, Any]) -> None:
    for key in LINK_VARIABLES:
        value = context.get(key)
        if value is None or value == "":
            continue
        parsed = urlparse(str(value))
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ValueError(f"{key} must be an http or https URL.")
