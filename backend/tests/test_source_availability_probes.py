from datetime import date
import re

import pytest
from sqlalchemy import create_engine, event
from sqlalchemy.orm import Session

from app.db import Base
from app.models import GovernmentContract, InstitutionalActivityEvent, InstitutionalSymbolSummary
from app.services.government_contracts import (
    get_government_contracts_overlay_availability, get_government_contracts_signals_for_symbols,
)
from app.services.institutional_activity import get_institutional_activity_summaries_for_symbols


@pytest.fixture
def db():
    engine = create_engine('sqlite:///:memory:')
    Base.metadata.create_all(engine, tables=[GovernmentContract.__table__,
        InstitutionalActivityEvent.__table__, InstitutionalSymbolSummary.__table__])
    with Session(engine) as session:
        yield session
    engine.dispose()


def prevent_global_counts(db):
    def check(conn,cursor,statement,parameters,context,many):
        sql = ' '.join(statement.lower().split())
        assert not re.search(r'select count\(\*\).* from (institutional_symbol_summary|government_contracts)$',sql), sql
    event.listen(db.get_bind(),'before_cursor_execute',check)
    return check


def test_institutional_empty_and_absent_symbol_remain_distinct_without_global_count(db):
    prevent_global_counts(db)
    summaries, availability = get_institutional_activity_summaries_for_symbols(db,['AAPL'])
    assert availability['status']=='not_configured' and summaries['AAPL']['status']=='not_configured'
    db.add(InstitutionalSymbolSummary(symbol='OTHER',normalized_symbol='OTHER',report_year=2026,report_quarter=2))
    db.commit()
    summaries, availability = get_institutional_activity_summaries_for_symbols(db,['aapl'])
    assert availability['status']=='ok' and summaries['AAPL']['status']=='no_data'


def test_government_empty_absent_and_populated_summaries_keep_values_without_global_count(db):
    prevent_global_counts(db)
    assert get_government_contracts_signals_for_symbols(db,['AAPL'])['AAPL']['status']=='unavailable'
    db.add(GovernmentContract(symbol='OTHER',award_date=date.today(),award_amount=2_000_000,source='test'))
    db.commit()
    absent = get_government_contracts_signals_for_symbols(db,['aapl'])['AAPL']
    assert absent['status']=='ok' and absent['active'] is False and absent['contract_count']==0
    db.add(GovernmentContract(symbol='aapl',award_date=date.today(),award_amount=3_000_000,source='test'))
    db.add(GovernmentContract(symbol='AAPL',award_date=date.today(),award_amount=4_000_000,source='test'))
    db.commit()
    present = get_government_contracts_signals_for_symbols(db,['AAPL'])['AAPL']
    assert present['active'] is True and present['contract_count']==2
    assert present['total_award_amount']==7_000_000 and present['largest_award_amount']==4_000_000


def test_public_overlay_still_reports_exact_dataset_count(db):
    for index in range(3):
        db.add(GovernmentContract(symbol='OTHER',award_date=date.today(),award_amount=index,source='test'))
    db.commit()
    availability = get_government_contracts_overlay_availability(db)
    assert availability['status']=='ok' and availability['indexed_row_count']==3
    assert get_government_contracts_overlay_availability(db,feature_enabled=False)['status']=='disabled'


def test_missing_tables_and_disabled_institutions_do_not_become_empty_data():
    engine=create_engine('sqlite:///:memory:')
    with Session(engine) as db:
        assert get_government_contracts_signals_for_symbols(db,['AAPL'])['AAPL']['status']=='unavailable'
        summaries, availability=get_institutional_activity_summaries_for_symbols(db,['AAPL'])
        assert availability['status']=='unavailable' and summaries['AAPL']['status']=='unavailable'
        summaries, availability=get_institutional_activity_summaries_for_symbols(db,['AAPL'],feature_enabled=False)
        assert availability['status']=='disabled' and summaries['AAPL']['status']=='disabled'
    engine.dispose()
