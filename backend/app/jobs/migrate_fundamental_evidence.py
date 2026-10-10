"""Add nullable source evidence before workers import the updated financial models."""
import argparse
import json
from sqlalchemy import inspect, text
from app.db import engine

TABLES=('fundamentals_cache','fundamentals_snapshots')


def migrate(bind, *, apply=False):
    if bind.dialect.name not in {'postgresql','sqlite'}:
        raise ValueError('Unsupported migration database')
    result={'applied':apply,'tables':{}}
    with bind.begin() as conn:
        if bind.dialect.name=='postgresql':
            conn.execute(text("SET LOCAL statement_timeout='20s'"))
            conn.execute(text("SET LOCAL lock_timeout='2s'"))
            if apply and not conn.scalar(text("SELECT pg_try_advisory_xact_lock(hashtext('fundamental-evidence:migration:v1'))")):
                raise RuntimeError('Another evidence migration is active')
        inspector=inspect(conn)
        for table in TABLES:
            if not inspector.has_table(table):
                raise ValueError('Required financial table is absent: '+table)
            columns={c['name']:c for c in inspector.get_columns(table)}
            column=columns.get('source_evidence_json')
            if column is not None and (not column['nullable'] or str(column['type']).upper()!='TEXT'):
                raise ValueError('Existing source evidence column has an unexpected definition')
            result['tables'][table]='existing' if column else ('added' if apply else 'pending')
            if apply and column is None:
                # Names are the fixed internal table allowlist above.
                conn.execute(text(f'ALTER TABLE {table} ADD COLUMN source_evidence_json TEXT'))
    return result


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apply',action='store_true')
    args=parser.parse_args()
    print(json.dumps(migrate(engine,apply=args.apply)))


if __name__=='__main__':main()
