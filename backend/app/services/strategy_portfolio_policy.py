"""Bounded prospective portfolios; historical research is never rewritten."""
from dataclasses import replace

DEFAULT_MAX_POSITIONS = 25
HARD_MAX_POSITIONS = 50
POLICY_VERSION = "focused_portfolio_v1"


def position_limit(rules: dict) -> int:
    return max(1, min(HARD_MAX_POSITIONS, int(rules.get("max_positions") or DEFAULT_MAX_POSITIONS)))


def select_candidates(candidates: list, rules: dict) -> list:
    # Newest public disclosure first, then corroboration/score, then symbol.
    # No historical returns or future prices participate in selection.
    if len(candidates) <= position_limit(rules):
        return candidates
    ranked = sorted(candidates, key=lambda c: (
        -int(str(c.qualification_snapshot.get("publicDate") or "0000-00-00").replace("-", "")),
        -int(c.source_count or 0), -float(c.score or 0), c.normalized_symbol,
    ))[:position_limit(rules)]
    weight = round(100 / len(ranked), 8) if ranked else 0
    return [replace(c, weight_pct=weight) for c in ranked]
