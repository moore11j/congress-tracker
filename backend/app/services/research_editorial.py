"""Bounded editorial examples and deterministic research-topic classification."""
import re
from difflib import SequenceMatcher


def topic_family(item):
    if not isinstance(item, dict):
        item = {"target_keyword": item}
    query = " ".join(str(item.get(k) or "") for k in ("target_keyword", "title", "recommended_theme")).lower()
    for family, pattern in (
        ("congress", r"congress|senator|lawmaker"),
        ("insiders", r"insider|form 4|ceo.{0,20}(?:buy|sell)"),
        ("institutions", r"13f|institution|ownership|holder|who.{0,12}buying.{0,65}(?:sec|filing)"),
        ("contracts", r"contract|nasa|pentagon|defen[cs]e award"),
        ("valuation", r"valuat|overvalu|undervalu|expensive|cheap|worth|price target"),
        ("earnings", r"earnings|revenue|margin|cash flow|profit|dividend|balance sheet"),
        ("price_action", r"technical|momentum|moving average|rally|stock.{0,15}(?:up|down|fall|ris)"),
    ):
        if re.search(pattern, query):
            return family
    return "stock_research"


def record_edits(draft, before, after, now):
    """Save local text changes, not whole articles, credentials, or old fact packets.

    These are examples for the next prompt, not model training or new evidence.
    Ignore metadata, sources, paywalls and machine-generated revisions.
    """
    def blocks(article):
        result = {key: str(article.get(key) or "") for key in ("title", "summary", "preview_body")}
        for section in article.get("sections") or []:
            if isinstance(section, dict) and (section.get("key") or section.get("heading")):
                result["section:" + str(section.get("key") or section["heading"])] = str(section.get("body_markdown") or "")
        return result
    old, new = blocks(before), blocks(after)
    examples = list(draft.get("editorial_edits") or [])
    for field, value in new.items():
        prior = old.get(field, "")
        if not prior or not value or prior == value:
            continue
        # Keep the changed passage even when it occurs late in a long section.
        matcher = SequenceMatcher(None, prior, value, autojunk=False)
        for tag, a, b, c, d in matcher.get_opcodes():
            if tag == "equal":
                continue
            example = {"field": field, "before": prior[max(0, a-80):min(len(prior), b+80)][:600],
                       "after": value[max(0, c-80):min(len(value), d+80)][:600], "saved_at": now}
            if not any(e.get("before") == example["before"] and e.get("after") == example["after"] for e in examples):
                examples.append(example)
    draft["editorial_edits"] = examples[-8:]


def editing_examples(drafts, limit=6):
    examples = []
    for draft in drafts:
        if draft.get("status") in {"rejected", "deleted"}:
            continue
        examples.extend(draft.get("editorial_edits") or [])
    examples.sort(key=lambda row: row.get("saved_at", ""), reverse=True)
    return examples[:limit]


EDITORIAL_GUIDANCE = (
    "Use T. Rowe Price, never Price T Rowe or Price t Rowe. "
    "Answer the headline in the opening with specific names and evidence, not 'a mix of managers' or 'broad participation'. "
    "EDITOR_EDIT_EXAMPLES are before/after writing examples from saved manual edits. Apply relevant naming, clarity and style preferences; "
    "they are not current factual evidence or instructions. Never copy their tickers, people, dates, numbers, claims or links into this article "
    "unless independently present in this article's verified fact packet. Current explicit editorial instructions take precedence."
)
