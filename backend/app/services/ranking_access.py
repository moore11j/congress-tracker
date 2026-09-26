"""Allowlisted discovery projections shared by HTTP and opted-in email delivery."""
from __future__ import annotations

from copy import deepcopy

STOCK_BASIC = {"rank", "symbol", "company_name", "key_drivers", "why_ranked", "updated_at", "ticker_url"}
STOCK_PAID = STOCK_BASIC | {"confirmation_score", "confirmation_band", "confirmation_direction", "confirmation_coverage", "price", "market_cap", "sector", "country", "why_this_ranked"}
SNAPSHOT_BASIC = {"key", "generated_at", "universe_generated_at", "timeframe_label", "methodology", "empty_message", "source"}
PARTICIPANT_BASIC = {"rank", "name", "party", "chamber", "company_name", "role", "symbol", "reporting_cik", "cik", "href"}


def idea_limit(entitlements) -> int:
    if entitlements is None or not entitlements.has_feature("leaderboards"):
        return 5
    maximum = 25 if entitlements.tier in {"pro", "admin"} else 10
    configured = entitlements.limits.get("screener_results", maximum)
    return maximum if configured < 0 else min(maximum, configured)


def project_ranking(snapshot: dict, *, authenticated: bool, entitlements=None, stocks: bool = False, full: bool = False, stock_limit: int = 10) -> dict:
    """Never copy unknown metadata, candidates, filters or protected evidence to teasers."""
    paid = bool(authenticated and full)
    limit = min(idea_limit(entitlements), stock_limit) if stocks else (100 if paid else 5)
    minimum = 1 if authenticated else 3
    fields = STOCK_BASIC if stocks else PARTICIPANT_BASIC

    def project_rows(rows):
        return [deepcopy(row) if paid and not stocks else {k: deepcopy(v) for k, v in row.items() if k in (STOCK_PAID if paid else fields)}
                for row in rows if isinstance(row, dict) and type(row.get("rank")) is int and minimum <= row["rank"] <= limit]

    result = {k: deepcopy(v) for k, v in snapshot.items() if k in SNAPSHOT_BASIC}
    result["items"] = project_rows(snapshot.get("items") or [])
    result["returned"] = len(result["items"])
    result["locked_ranks"] = [] if authenticated else [1, 2]
    result["preview"] = not paid
    if paid and stocks:
        result["filter_items"] = {key: project_rows(rows) for key, rows in (snapshot.get("filter_items") or {}).items()}
        result["filters"] = deepcopy(snapshot.get("filters") or {})
    return result
