"""Skip ORM startup only when exact automatic prior selection has no work."""
from datetime import datetime, timedelta, timezone
import os

from app.services.direct_13f_prior_selection import normalize_candidates, select_priors


def read_selection(*, since, limit, recheck_hours, lookback_days, now):
    if not 1 <= limit <= 20 or not 1 <= recheck_hours <= 168 or not 1 <= lookback_days <= 93 or since > now.date():
        raise ValueError('Invalid automatic prior discovery bounds')
    dsn = os.getenv('DATABASE_URL', '')
    if dsn.startswith('postgresql+psycopg://'):
        dsn = dsn.replace('postgresql+psycopg://', 'postgresql://', 1)
    if not dsn.startswith(('postgres://', 'postgresql://')):
        return None
    import psycopg
    start = max(since, now.date() - timedelta(days=lookback_days))
    with psycopg.connect(dsn, connect_timeout=2,
            application_name=f'walnut:prior-preflight:{os.getpid()}',
            options='-c default_transaction_read_only=on -c statement_timeout=1500 -c lock_timeout=500') as conn:
        with conn.cursor() as cursor:
            cursor.execute("""SELECT id,content_hash,metadata_json,
                reconciliation_json::jsonb->'_prior_collection'
                FROM direct_feed_documents WHERE feed='sec_13f' AND status='parsed'
                AND metadata_json::jsonb->>'filing_date' >= %s
                AND metadata_json::jsonb->>'filing_date' <= %s
                ORDER BY metadata_json::jsonb->>'filing_date',id LIMIT 20001""",
                (start.isoformat(), now.date().isoformat()))
            candidates = normalize_candidates(cursor.fetchall())
            ids = {receipt['prior_document_id'] for _, _, _, receipt in candidates
                   if receipt and receipt.get('status') == 'collected'}
            priors = {}
            if ids:
                cursor.execute("""SELECT id,status,content_hash,metadata_json
                    FROM direct_feed_documents WHERE id=ANY(%s)""", (list(ids),))
                priors = {row[0]: tuple(row[1:]) for row in cursor.fetchall()}
    return select_priors(candidates, priors, now=now, start=start, limit=limit, recheck_hours=recheck_hours)


def early_result(args, *, now=None):
    # Explicit source selection and previews keep their existing full worker.
    if (not args.since or args.preview or os.getenv('DIRECT_FEEDS_MODE', 'off').lower() != 'shadow'
            or os.getenv('BACKGROUND_JOBS_PAUSED', '').strip().lower() in {'1','true','yes','on'}):
        return None
    try:
        picked = read_selection(since=args.since, limit=args.limit, recheck_hours=args.recheck_hours,
                                lookback_days=args.lookback_days, now=now or datetime.now(timezone.utc))
        if picked is not None and picked['eligible'] == 0 and not picked['document_ids']:
            return {'status': 'idle', 'selection': picked, 'public_writes': 0}
    except Exception:
        # An uncertain/read-failed preflight must never discard due collection.
        pass
    return None
