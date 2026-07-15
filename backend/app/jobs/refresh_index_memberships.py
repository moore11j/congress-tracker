from __future__ import annotations

import logging

from app.db import SessionLocal
from app.services.index_memberships import refresh_all_index_memberships_from_provider

logger = logging.getLogger(__name__)


def run() -> list[dict]:
    db = SessionLocal()
    try:
        results = refresh_all_index_memberships_from_provider(db)
        payload = [result.__dict__ for result in results]
        logger.info("index_membership_refresh_job_complete results=%s", payload)
        return payload
    finally:
        db.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    for result in run():
        print(result)
