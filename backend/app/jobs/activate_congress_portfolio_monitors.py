"""Activate daily prospective monitoring for each supported published strategy."""

from __future__ import annotations

import argparse
import json
from datetime import datetime
from zoneinfo import ZoneInfo

from sqlalchemy import func, select

from app.db import SessionLocal
from app.models import StrategyDefinition, StrategyVersion
from app.services.strategy_candidate_resolver import validate_strategy_candidate_rules


def _loads(value: str | None) -> dict:
    try:
        parsed = json.loads(value or "{}")
    except (TypeError, ValueError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def activate(*, apply: bool) -> dict:
    today = datetime.now(ZoneInfo("America/Los_Angeles")).date()
    with SessionLocal() as db:
        strategies = db.execute(
            select(StrategyDefinition)
            .where(StrategyDefinition.status == "published", StrategyDefinition.category == "congress")
            .order_by(StrategyDefinition.id.asc())
        ).scalars().all()
        candidates: list[tuple[StrategyDefinition, dict, dict]] = []
        for strategy in strategies:
            rule = _loads(strategy.rule_json)
            member_id = str(rule.get("member_bioguide_id") or "").strip().upper()
            if rule.get("kind") == "replicated_individual_congress_portfolio" and member_id:
                candidates.append((strategy, rule, {
                    "candidate_source": "congress_member_disclosures",
                    "member_bioguide_id": rule["member_bioguide_id"],
                    "entry_timing": "next_trading_day_after_daily_ingest",
                    "exit_rule": "matching_reported_sale",
                }))

        other_published = db.execute(
            select(StrategyDefinition)
            .where(StrategyDefinition.status == "published", StrategyDefinition.category.in_(["congress", "insider", "cross_source"]))
            .order_by(StrategyDefinition.id.asc())
        ).scalars().all()
        for strategy in other_published:
            rule = _loads(strategy.rule_json)
            if rule.get("kind") == "replicated_individual_congress_portfolio":
                continue
            if strategy.category == "cross_source" and rule.get("pair") == "congress_insider":
                candidates.append((strategy, rule, {
                    "candidate_source": "cross_source_disclosure_alignment",
                    "alignment_lookback_days": int(rule.get("lookback_days") or 90),
                    "entry_timing": "next_trading_day_after_daily_ingest",
                }))
            elif rule.get("source") in {"congress", "insider"}:
                candidates.append((strategy, rule, {
                    "candidate_source": "disclosure_portfolio",
                    "trade_source": rule["source"],
                    "technical_rule": rule.get("technical_rule"),
                    "holding_period_days": int(rule.get("holding_period_days") or 90),
                    "entry_timing": "next_trading_day_after_daily_ingest",
                }))

        result = {"apply": apply, "effectiveFrom": today.isoformat(), "activated": [], "skipped": []}
        for strategy, rule, rules in candidates:
            existing = db.execute(
                select(StrategyVersion)
                .where(StrategyVersion.strategy_id == strategy.id, StrategyVersion.status == "active")
                .limit(1)
            ).scalar_one_or_none()
            if existing is not None:
                result["skipped"].append({"slug": strategy.slug, "reason": "active_version_exists", "version": int(existing.version)})
                continue
            validate_strategy_candidate_rules(rules)
            if not apply:
                result["activated"].append({"slug": strategy.slug, "candidateSource": rules["candidate_source"], "mode": "dry_run"})
                continue
            next_version = int(
                db.execute(select(func.max(StrategyVersion.version)).where(StrategyVersion.strategy_id == strategy.id)).scalar_one()
                or 0
            ) + 1
            version = StrategyVersion(
                strategy_id=strategy.id,
                version=next_version,
                status="active",
                rules_json=json.dumps(rules, sort_keys=True, separators=(",", ":")),
                parameters_json=json.dumps({"schedule": "daily_after_congress_ingest", "weighting": "equal_weight"}, sort_keys=True, separators=(",", ":")),
                universe_json=json.dumps({"label": "Congress Trades" if rules["candidate_source"] == "congress_member_disclosures" else "Insider Trades" if rules.get("trade_source") == "insider" else "Congress Trades" if rules.get("trade_source") == "congress" else "Congress + Insider Trades", "source": rules["candidate_source"]}, sort_keys=True, separators=(",", ":")),
                methodology="Daily prospective monitoring from newly ingested public filings. New positions enter on the next available trading day after Walnut's daily ingest and exits follow the strategy's stored rules.",
                effective_from=today,
                created_by="activate_congress_portfolio_monitors",
            )
            db.add(version)
            result["activated"].append({"slug": strategy.slug, "candidateSource": rules["candidate_source"], "version": next_version})
        if apply:
            db.commit()
        return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true", help="Create and activate monitor versions. Defaults to dry run.")
    args = parser.parse_args()
    print(json.dumps(activate(apply=bool(args.apply)), sort_keys=True))


if __name__ == "__main__":
    main()
