"""Add concurrent lookup indexes without rewriting canonical events."""
import argparse
import json

from sqlalchemy import text
from app.db import engine
from app.services.direct_congress_repair import postgres_lookup_index_statements


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apply', action='store_true')
    args = parser.parse_args()
    if engine.dialect.name != 'postgresql':
        raise ValueError('Concurrent lookup indexes require PostgreSQL')
    results = []
    with engine.connect().execution_options(isolation_level='AUTOCOMMIT') as conn:
        conn.detach()
        conn.execute(text("SET statement_timeout = '180s'"))
        conn.execute(text("SET lock_timeout = '2s'"))
        for name, ddl in postgres_lookup_index_statements(conn.dialect):
            def inspect():
                return conn.execute(text('SELECT i.indisvalid, i.indrelid = CAST(:table_name AS regclass) AS correct_table, '
                    'pg_get_indexdef(i.indexrelid) AS definition FROM pg_index i '
                    'WHERE i.indexrelid = to_regclass(:name)'), {'name': name, 'table_name': 'events'}).mappings().one_or_none()
            existing = inspect()
            if existing is None and args.apply:
                conn.exec_driver_sql(ddl)
                existing = inspect()
            if existing is not None and (not existing['indisvalid'] or not existing['correct_table']):
                raise ValueError(f'{name} is invalid or belongs to another table; inspect before retrying')
            results.append({'index': name, 'present': existing is not None,
                            'valid': bool(existing and existing['indisvalid']),
                            'definition': existing['definition'] if existing else None})
    print(json.dumps({'indexes': results, 'canonical_row_writes': 0, 'apply_requested': args.apply}))


if __name__ == '__main__':
    main()
