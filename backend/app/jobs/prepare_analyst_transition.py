"""Prepare the analyst display-only ranking without selecting it publicly.

Run in a fresh process with ANALYST_PROVIDER=finnhub, an explicit expected
candidate count, and --apply after a dry run. Market inputs are cache-only;
no quote fetch, historical rewrite, portfolio evaluation or email is performed.
"""
from __future__ import annotations

import argparse
import json
from sqlalchemy import select, text
from app.db import SessionLocal
from app.models import ConfirmationMethodologyVersion
from app.services.confirmation_score import CONFIRMATION_SCORING_VERSION
from app.services.outcome_ledger import (
    CURRENT_CONFIRMATION_METHODOLOGY_VERSION, current_methodology_configuration,
    register_confirmation_methodology_version,
)
from app.services.top_stocks import refresh_top_stocks_leaderboard, staged_top_stocks_key


def prepare_analyst_transition(db, *, expected_candidates: int) -> dict:
    if not CONFIRMATION_SCORING_VERSION.endswith("_analyst_display_only_v1"):
        raise ValueError("A fresh process with the Finnhub analyst selector is required")
    if not 0 < expected_candidates <= 5000:
        raise ValueError("Expected candidate count must be between one and 5000")
    existing = db.scalar(select(ConfirmationMethodologyVersion).where(
        ConfirmationMethodologyVersion.version == CURRENT_CONFIRMATION_METHODOLOGY_VERSION))
    configuration = current_methodology_configuration()
    if existing and json.loads(existing.configuration_json) != configuration:
        raise ValueError("Prepared methodology configuration differs")
    result = refresh_top_stocks_leaderboard(
        db, staged=True, expected_candidates=expected_candidates, commit=False)
    register_confirmation_methodology_version(db, make_current=False)
    return {"scoring_version": CONFIRMATION_SCORING_VERSION,
            "methodology_version": CURRENT_CONFIRMATION_METHODOLOGY_VERSION,
            "ranking_key": staged_top_stocks_key(), "candidate_count": expected_candidates,
            "generated_at": result["generated_at"], "ranked_items": result["returned"],
            "public_selector_changed": False, "historical_records_changed": False,
            "market_inputs_refreshed": False}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--expected-candidates", type=int, required=True)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    with SessionLocal() as db:
        if db.get_bind().dialect.name == "postgresql":
            db.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
            db.execute(text("SET LOCAL statement_timeout='30s'"))
            db.execute(text("SET LOCAL lock_timeout='3s'"))
            if not db.scalar(text("SELECT pg_try_advisory_xact_lock(hashtext('analyst-transition:prepare:v1'))")):
                raise RuntimeError("Another analyst preparation owns the lock")
        result = prepare_analyst_transition(db, expected_candidates=args.expected_candidates)
        if args.apply:
            db.commit()
        else:
            db.rollback()
        result["applied"] = args.apply
        print("ANALYST_PREPARATION=" + json.dumps(result))


if __name__ == "__main__":
    main()
