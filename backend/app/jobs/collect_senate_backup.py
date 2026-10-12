"""Opt-in secondary Senate staging, sharing the existing source collector lock."""
from datetime import datetime, timezone
import argparse
import os

from app.services.direct_feed_store import dumps


def collect(db, *, years, now=None, session=None):
    from app.services.direct_feed_store import DirectFeedRun
    from app.services.senate_backup_collection import download_snapshot, stage_bundle, FEED
    if db.new or db.dirty or db.deleted:
        raise ValueError('Collector session has unrelated pending changes')
    clock = now or datetime.now(timezone.utc)
    run = DirectFeedRun(started_at=clock, status='running', report_json=dumps({'sources':[FEED]}))
    db.add(run);db.commit()
    identifier=run.id
    db.rollback()
    try:
        # Neither network nor Parquet parsing holds an existing DB transaction.
        bundle=download_snapshot(years=years,observed_at=clock,session=session)
        result=stage_bundle(db,bundle,now=now)
        db.commit()
    except Exception as exc:
        db.rollback()
        run=db.get(DirectFeedRun,identifier)
        run.status='failed';run.finished_at=datetime.now(timezone.utc)
        run.report_json=dumps({'sources':[FEED],'error_type':type(exc).__name__,
                               'publication_enabled':False})
        db.commit()
        raise
    run=db.get(DirectFeedRun,identifier)
    run.status='ok';run.finished_at=datetime.now(timezone.utc)
    run.report_json=dumps({'sources':[FEED],**result,'publication_enabled':False})
    db.commit()
    return {'run_id':identifier,**result}


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--years',nargs='+',type=int,required=True)
    args=parser.parse_args(argv)
    if os.getenv('SENATE_BACKUP_COLLECTION_ENABLED','false').strip().lower()!='true':
        print(dumps({'status':'disabled'}));return
    from app.background_job_guard import check_background_job_guard
    guard=check_background_job_guard('senate-backup-collection')
    if not guard.proceed:
        print(dumps(guard.to_dict()));return
    from app.jobs.collect_direct_feeds import collector_lock
    from app.db import SessionLocal
    with collector_lock() as acquired:
        if not acquired:
            print(dumps({'status':'busy'}));return
        with SessionLocal() as db:
            print(dumps(collect(db,years=args.years)))


if __name__=='__main__':
    main()
