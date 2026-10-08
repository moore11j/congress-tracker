"""Add isolated direct-feed tables before updated workers start.

No source selection, provider requests, canonical publication or email.
"""
from sqlalchemy import inspect
from app.db import engine
from app.services.direct_feed_store import ensure_direct_feed_schema, dumps


def main():
    ensure_direct_feed_schema(engine)
    required = {'direct_feed_documents', 'direct_feed_revisions', 'direct_feed_runs', 'feed_source_controls',
        'direct_feed_publications', 'congress_repair_archives', 'congress_repair_receipts', 'congress_row_bindings'}
    schema = inspect(engine)
    if not required <= set(schema.get_table_names()):
        raise RuntimeError('Direct-feed schema is incomplete')
    if 'source_bytes' not in {c['name'] for c in schema.get_columns('direct_feed_revisions')}:
        raise RuntimeError('Exact source-byte storage is missing')
    print(dumps({'status': 'schema_ready', 'tables': sorted(required), 'source_selection_changed': False,
        'canonical_writes': 0, 'email_deliveries': 0}))


if __name__ == '__main__':
    main()
