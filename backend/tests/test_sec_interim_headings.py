"""Shared interim titles retain existing issuer and accounting exclusions."""
from datetime import date
import pytest
from app.services import sec_tabular_financials as tables
from test_sec_tabular_financials import simple

@pytest.mark.parametrize('title,kind', [
    ('Condensed consolidated interim balance sheets','balance'),
    ('Condensed consolidated interim statements of operations','income'),
    ('Consolidated interim statements of cash flows','cashflow'),
    ('Unaudited consolidated interim statements of comprehensive income','income'),
])
def test_explicit_interim_title_kind(title,kind):
    assert tables.table_kind(title)==kind

@pytest.mark.parametrize('prefix',['Adjusted','Non-GAAP','Pro forma','Reconciliation of','Notes to'])
def test_interim_titles_do_not_admit_adjusted_or_note_tables(prefix):
    assert tables.table_kind(prefix+' consolidated interim statements of operations') is None


def test_interim_title_preserves_period_currency_and_issuer_checks():
    table=simple(heading='Condensed consolidated interim statements of operations')
    result=tables.parse_table(table,issuer='Example Corp',filed=date(2026,9,1))
    assert result['observations'][0]['value']==1250000
    with pytest.raises(tables.TabularFinancialError,match='table_issuer_unproved'):
        tables.parse_table(table,issuer='Other Corp',filed=date(2026,9,1))
    unknown=simple(heading='Consolidated interim statements of operations',unit='$',scale='In thousands')
    with pytest.raises(tables.TabularFinancialError,match='column_currency_unproved'):
        tables.parse_table(unknown,issuer='Example Corp',filed=date(2026,9,1))
