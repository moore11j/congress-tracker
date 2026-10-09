"""Install only reviewed, checksummed SEC identifier evidence in shared staging."""
import argparse
import os
from pathlib import Path

from app.background_job_guard import check_background_job_guard
from app.clients.direct_sources import DirectSourceClient
from app.db import SessionLocal
from app.jobs.collect_direct_feeds import collector_lock
from app.services.direct_13f_identifiers import stage_reviewed_identifiers
from app.services.direct_feed_store import dumps


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--manifest', type=Path, default=Path(__file__).resolve().parents[2] / 'config/sec_13f_identifiers.json')
    args = parser.parse_args()
    if os.getenv('DIRECT_FEEDS_MODE', 'off').lower() != 'shadow':
        print(dumps({'status': 'disabled'})); return
    guard = check_background_job_guard('direct-13f-identifier-staging')
    if not guard.proceed:
        print(dumps(guard.to_dict())); return
    with collector_lock() as acquired:
        if not acquired:
            print(dumps({'status': 'busy'})); return
        with SessionLocal() as db:
            print(dumps(stage_reviewed_identifiers(db, DirectSourceClient(), args.manifest)))


if __name__ == '__main__':
    main()
