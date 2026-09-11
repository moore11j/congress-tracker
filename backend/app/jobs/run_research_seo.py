"""Daily SEO has its own opt-in gate; existing campaign settings are unchanged."""
import logging

from app.db import SessionLocal
from app.services.research_seo import run_daily_plan


def main():
    logging.basicConfig(level=logging.INFO)
    with SessionLocal() as db:
        result = run_daily_plan(db)
        logging.getLogger(__name__).info("research_seo_completed result=%s", result)
        return 1 if result.get("status") == "failed" else 0


if __name__ == "__main__":
    raise SystemExit(main())
