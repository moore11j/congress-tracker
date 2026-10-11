from __future__ import annotations

import argparse
import logging

from app.jobs.research_preflight import campaign_idle


logger = logging.getLogger(__name__)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run due Walnut Research Brief campaigns and scheduled publications.")
    parser.add_argument("--limit", type=int, default=10)
    parser.add_argument("--generation-only", action="store_true")
    parser.add_argument("--publish-only", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    if campaign_idle(generation=not args.publish_only, publication=not args.generation_only):
        logger.info("research_campaigns_idle preflight=true")
        return 0
    from app.db import SessionLocal
    from app.services.research_briefs import run_due_research_campaign_generation, run_due_scheduled_research_publications
    db = SessionLocal()
    try:
        result: dict[str, object] = {}
        if not args.publish_only:
            result["generation"] = run_due_research_campaign_generation(db, limit=args.limit)
        if not args.generation_only:
            result["publication"] = run_due_scheduled_research_publications(db, limit=args.limit)
        logger.info("research_campaigns_job_completed result=%s", result)
        return 0
    finally:
        db.close()


if __name__ == "__main__":
    raise SystemExit(main())
