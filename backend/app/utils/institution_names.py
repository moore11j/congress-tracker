"""Presentation-only institutional names; never change CIKs or stored identities."""
import re
from copy import deepcopy

BRANDS = {"blackrock": "BlackRock", "jpmorgan": "JPMorgan", "invesco": "Invesco",
          "dimensional": "Dimensional", "vanguard": "Vanguard", "morgan": "Morgan"}
ACRONYMS = set("LLC LLP LP PLC AG SA UBS FMR BNY BNP HSBC RBC TD CIBC BMO TIAA US USA UK PNC KKR AQR DWS DNB SEI GMO CWA CWM LPL RIA ETF ETFs".upper().split())
SMALL_WORDS = {"of", "and", "the", "for", "de", "van", "von"}


def institution_display_name(value):
    if not isinstance(value, str) or not value.strip():
        return None
    value = " ".join(value.split())
    # Respect existing mixed-case branding rather than guessing its spelling.
    def word(match):
        token = match.group()
        lower = token.lower()
        if match.start() > 0 and value[match.start()-1:match.start()] == "/" and value[match.end():match.end()+1] == "/":
            return token.upper()
        if lower in BRANDS:
            return BRANDS[lower]
        if token.upper() in ACRONYMS:
            return token.upper()
        if token != token.upper() and token != lower:
            return token
        if lower in SMALL_WORDS and match.start() > 0:
            return lower
        return re.sub(r"(^|['’])([a-z])", lambda m: m[1] + m[2].upper(), lower)
    return re.sub(r"[A-Za-z]+(?:['’][A-Za-z]+)?", word, value)


def institutional_names_in_context(context):
    names = set()
    def visit(value):
        if isinstance(value, dict):
            for key, item in value.items():
                if key in {"holder_name", "institution_name", "manager_name"} and isinstance(item, str) and item.strip():
                    names.add(item.strip())
                elif isinstance(item, (dict, list)):
                    visit(item)
        elif isinstance(value, list):
            for item in value:
                visit(item)
    visit(context)
    return names


def normalize_article_institution_names(article, context):
    """Change only known institution names in reader-facing text, not metadata/URLs."""
    names = institutional_names_in_context(context)
    replacements = {name.casefold(): institution_display_name(name) for name in names}
    if not names:
        return deepcopy(article)
    pattern = re.compile(r"(?<!\w)(?:" + "|".join(re.escape(n) for n in sorted(names, key=len, reverse=True)) + r")(?!\w)", re.I)
    protected = re.compile(r"(https?://[^\s<>\)]+|<[^>]+>|`[^`]*`)")
    fields = {"title", "heading", "body_markdown", "summary", "executive_summary", "insights_card_title",
              "insights_preview_body", "seo_title", "seo_description", "meta_description", "judgment_explanation",
              "catalysts", "risks", "key_takeaways", "preview_body", "subtitle", "description",
              "key_points", "watch_items", "reddit_post", "body", "label"}
    def text(value):
        parts = protected.split(value)
        return "".join(part if i % 2 else pattern.sub(lambda m: replacements[m.group().casefold()], part)
                       for i, part in enumerate(parts))
    def visit(value, enabled=False):
        if isinstance(value, dict):
            return {key: visit(item, key in fields) for key, item in value.items()}
        if isinstance(value, list):
            return [visit(item, enabled) for item in value]
        return text(value) if enabled and isinstance(value, str) else value
    return visit(deepcopy(article))
