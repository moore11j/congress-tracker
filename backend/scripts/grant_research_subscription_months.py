"""Optional hourly sandbox job. Balance reads also allocate any due paid month.

Default is a configuration-free plan. --apply requires the isolated credit
sandbox configuration and never calls Stripe or modifies live account tables.
"""
import argparse
import json

from sqlalchemy import select
from app.routers.research_credits import configured
from app.services.research_credits import coverage
from app.services.research_credit_subscriptions import grant_due


def run(store, after="", limit=1000):
    with store.engine.connect() as conn:
        users = conn.execute(select(coverage.c.user_id).distinct().where(coverage.c.user_id > after)
            .order_by(coverage.c.user_id).limit(limit)).scalars().all()
    added = sum(bool(grant_due(store, user_id)) for user_id in users)
    return {"checked": len(users), "granted": added, "next_after": users[-1] if len(users) == limit else None}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--after-user", default="")
    args = parser.parse_args()
    if args.apply:
        store, _, _, _ = configured()
        print(json.dumps(run(store, args.after_user)))
    else:
        print(json.dumps({"mode": "plan_only", "sandbox_only": True,
            "purpose": "Grant the current month from verified paid subscription coverage; no catch-up stacking",
            "schedule": "Hourly; continue with --after-user if next_after is returned"}))
