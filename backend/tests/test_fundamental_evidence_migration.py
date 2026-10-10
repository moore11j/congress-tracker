from sqlalchemy import create_engine,inspect,text
import pytest
from app.jobs.migrate_fundamental_evidence import migrate,TABLES


def test_nullable_evidence_migration_preserves_legacy_rows_and_repeats():
    engine=create_engine('sqlite:///:memory:')
    with engine.begin() as conn:
        for table in TABLES:
            conn.execute(text(f'CREATE TABLE {table} (symbol TEXT, provider TEXT)'))
            conn.execute(text(f"INSERT INTO {table} VALUES ('ABC','fmp')"))
    assert set(migrate(engine)['tables'].values())=={'pending'}
    assert 'source_evidence_json' not in {c['name'] for c in inspect(engine).get_columns(TABLES[0])}
    assert set(migrate(engine,apply=True)['tables'].values())=={'added'}
    assert set(migrate(engine,apply=True)['tables'].values())=={'existing'}
    with engine.connect() as conn:
        for table in TABLES:
            assert conn.execute(text(f'SELECT * FROM {table}')).all()==[('ABC','fmp',None)]


def test_missing_financial_schema_is_not_created_by_evidence_migration():
    with pytest.raises(ValueError,match='absent'):
        migrate(create_engine('sqlite:///:memory:'),apply=True)
