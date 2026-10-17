"""Complete-source and adversarial checks for shared HTML column evidence."""
from datetime import date
from pathlib import Path
import gzip
import hashlib
import json
import socket
from unittest.mock import patch
import pytest
from lxml import html
from app.services import sec_tabular_financials as tab

AVAILABLE=date(2026,10,16)
FOLDER=Path(__file__).parent/'fixtures/sec_tabular_financials'
COUNTS={'CGNT':0,'NIVF':16,'NTB':10,'WB':18,'YI':36,'YQ':33,'ZEPP':28}


def source(symbol):
    meta=json.loads((FOLDER/'sources.json').read_text())[symbol];raw={}
    for kind,entry in meta['files'].items():
        value=gzip.decompress((FOLDER/entry['file']).read_bytes())
        assert len(value)==entry['bytes'] and hashlib.sha256(value).hexdigest()==entry['sha256']
        raw[kind]=value
    args=dict(source_url=meta['files']['statement']['url'],company_raw=raw['company'],
        cover_raw=raw['cover'],cover_url=meta['files']['cover']['url'],symbol=symbol,cik=meta['cik'],available_by=AVAILABLE)
    return raw['statement'],args


def point(result,metric,start,end='2026-06-30',currency='USD'):
    selected=[f for f in result['facts'] if (f['metric'],f['start'],f['end'],f['currency'])==(metric,start,end,currency)]
    assert len(selected)==1
    return selected[0]


@pytest.mark.parametrize('symbol',COUNTS)
def test_complete_public_originals_repeat_without_network_or_duplicate_observations(symbol):
    raw,args=source(symbol)
    with patch.object(socket.socket,'connect',side_effect=AssertionError('Network forbidden')):
        result=tab.extract(raw,**args)
        assert tab.extract(raw,**args)==result
    assert len(result['facts'])==COUNTS[symbol]
    assert result['publication_eligible'] is False and not result['conflicts']
    keys=[(f['metric'],f['start'],f['end'],f['currency']) for f in result['facts']]
    assert len(keys)==len(set(keys))
    assert result['source']['statement_sha256']==hashlib.sha256(raw).hexdigest()
    assert all(f['occurrences'] for f in result['facts'])
    assert all('reconciliation' not in ' '.join(t['heading']).lower() for t in result['tables'])


@pytest.mark.parametrize('symbol,metric,start,currency,value',[
    ('NIVF','current_assets',None,'USD',11126846),
    ('NIVF','current_liabilities',None,'USD',10508521),
    ('NIVF','revenue','2026-01-01','USD',1924881),
    ('NTB','total_equity',None,'USD',1148675000),
    ('NTB','consolidated_income','2026-01-01','USD',109535000),
    ('WB','revenue','2026-01-01','USD',875151000),
    ('YI','current_assets',None,'CNY',1723085000),
    ('YI','current_assets',None,'USD',253951000),
    ('YI','operating_cash_flow','2026-01-01','CNY',-103158000),
    ('YQ','current_assets',None,'CNY',559829000),
    ('YQ','current_liabilities',None,'CNY',316129000),
    ('ZEPP','revenue','2026-01-01','USD',115072000),
])
def test_independently_reviewed_original_values_units_and_periods(symbol,metric,start,currency,value):
    raw,args=source(symbol);result=tab.extract(raw,**args)
    assert point(result,metric,start,currency=currency)['value']==value


def test_disclosure_date_is_not_used_as_statement_end_or_native_currency():
    raw,args=source('NIVF');result=tab.extract(raw,**args)
    assert result['source']['report_date']=='2026-10-07'
    assert point(result,'revenue','2026-01-01')['end']=='2026-06-30'
    raw,args=source('YI');result=tab.extract(raw,**args)
    cny=point(result,'current_assets',None,currency='CNY')
    usd=point(result,'current_assets',None,currency='USD')
    assert cny['value']!=usd['value'] and cny['occurrences'][0]['column_start']!=usd['occurrences'][0]['column_start']
    assert 'reportingCurrency' not in result


def test_quarter_and_halfyear_columns_never_mix_and_bank_current_ratio_is_not_inferred():
    raw,args=source('WB');result=tab.extract(raw,**args)
    assert point(result,'revenue','2026-04-01')['value']==453826000
    assert point(result,'revenue','2026-01-01')['value']==875151000
    raw,args=source('NTB');result=tab.extract(raw,**args)
    assert not any(f['metric'] in {'current_assets','current_liabilities'} for f in result['facts'])
    assert len(point(result,'consolidated_income','2026-01-01')['occurrences'])==2
    assert all(t['kind']=='income' for t in result['tables'] if any(o['metric']=='consolidated_income' for o in t['observations']))


def test_bare_dollar_symbols_do_not_authorize_a_statement_currency():
    raw,args=source('CGNT');result=tab.extract(raw,**args)
    assert not result['facts']
    assert {h['reason'] for h in result['holds']}=={'column_currency_unproved'}


def simple(*,amount='1,250',unit='USD',scale='In thousands of U.S. dollars',heading='UNAUDITED CONSOLIDATED STATEMENTS OF OPERATIONS',issuer='Example Corporation'):
    raw=f"<html><body><p>{issuer}</p><p>{heading}</p><p>({scale})</p><table><tr><td></td><td colspan='2'>Six Months Ended June 30,</td></tr><tr><td></td><td>2026</td><td>2025</td></tr><tr><td></td><td>{unit}</td><td>{unit}</td></tr><tr><td>Revenues</td><td>{amount}</td><td>1,000</td></tr></table></body></html>"
    return html.fromstring(raw).xpath('//table')[0]


def test_missing_dash_is_not_zero_but_an_explicit_zero_is_retained():
    kwargs=dict(issuer='Example Corp',filed=date(2026,9,1))
    result=tab.parse_table(simple(amount='\u2014'),**kwargs)
    assert len(result['observations'])==1
    result=tab.parse_table(simple(amount='0'),**kwargs)
    assert result['observations'][0]['value']==0


@pytest.mark.parametrize('change,reason',[
    ({'unit':'RMB'},'column_currency_conflict'),
    ({'unit':'AUD'},'column_currency_conflict'),
    ({'unit':'XYZ'},'column_currency_unproved'),
    ({'unit':'¥'},'column_currency_unproved'),
    ({'unit':'$','scale':'In thousands'},'column_currency_unproved'),
    ({'scale':'USD'},'table_scale_unproved'),
    ({'issuer':'Example subsidiary'},'table_issuer_unproved'),
])
def test_unproved_or_conflicting_table_basis_is_held(change,reason):
    with pytest.raises(tab.TabularFinancialError,match=reason):
        tab.parse_table(simple(**change),issuer='Example Corporation',filed=date(2026,9,1))


def test_adjusted_and_proforma_tables_are_not_financial_observations():
    for heading in ('Reconciliation of GAAP and Non-GAAP Results','Pro forma Consolidated Statements of Operations'):
        assert tab.parse_table(simple(heading=heading),issuer='Example Corp',filed=date(2026,9,1)) is None


@pytest.mark.parametrize('field,value',[
    ('source_url','https://example.test/statement.htm'),('symbol','OTHER'),('cik','1'),
    ('cover_url','https://www.sec.gov/Archives/edgar/data/1981662/000121390026107570/other.htm'),
    ('available_by',date(2026,1,1)),('cover_raw',b'<html>No exhibit link</html>'),
])
def test_directory_cik_accession_availability_and_cover_link_are_required(field,value):
    raw,args=source('NIVF');args[field]=value
    with pytest.raises(ValueError):tab.extract(raw,**args)


@pytest.mark.parametrize('attribute,value',[('colspan','0'),('colspan','129'),('colspan','bad'),('rowspan','101'),('rowspan','0')])
def test_invalid_or_excessive_grid_spans_fail_closed(attribute,value):
    table=html.fromstring(f'<table><tr><td {attribute}="{value}">x</td></tr></table>')
    with pytest.raises(tab.TabularFinancialError):tab.grid(table)


def test_rowspan_preserves_column_alignment_and_origin():
    table=html.fromstring('<table><tr><td rowspan="2">Label</td><td>A</td></tr><tr><td>B</td></tr></table>')
    rows=tab.grid(table)
    assert [(c['text'],c['start'],c['row']) for c in rows[1]]==[('Label',0,0),('B',1,1)]


def test_conflicting_repeated_table_never_emits_two_or_arbitrarily_selected_facts():
    raw,args=source('NIVF');tree=html.fromstring(raw);table=tree.xpath('//table')[0]
    duplicate=html.fromstring(html.tostring(table).replace(b'11,126,846',b'99,126,846'))
    # Recreate its local source heading so this is a second identified statement.
    for text in ('NEWGENIVF GROUP LIMITED','UNAUDITED CONSOLIDATED BALANCE SHEETS','(Stated in US Dollars)'):
        node=html.Element('p');node.text=text;tree.append(node)
    tree.append(duplicate)
    result=tab.extract(html.tostring(tree),**args)
    assert any(c['metric']=='current_assets' and c['end']=='2026-06-30' for c in result['conflicts'])
    assert not any(f['metric']=='current_assets' and f['end']=='2026-06-30' for f in result['facts'])


def test_future_statement_period_is_held_even_in_an_old_directory_filing():
    table=simple();table.xpath('./tr')[1].xpath('./td')[1].text='2027'
    with pytest.raises(tab.TabularFinancialError,match='column_period_after_filing'):
        tab.parse_table(table,issuer='Example Corp',filed=date(2026,9,1))


def test_other_declared_currency_and_fiscal_halfyear_are_read_without_usd_or_calendar_year_assumptions():
    table=simple(unit='CAD',scale='In thousands of Canadian dollars')
    table.xpath('./tr')[0].xpath('./td')[1].text='Six Months Ended July 31,'
    result=tab.parse_table(table,issuer='Example Corp',filed=date(2026,9,1))
    point=result['observations'][0]
    assert point['currency']=='CAD' and point['start']=='2026-02-01' and point['end']=='2026-07-31'
    assert point['value']==1250000


@pytest.mark.parametrize('text', ['UNAUDITED', 'In thousands', 'EURopean business', 'CADence', 'USDollars'])
def test_currency_codes_require_complete_tokens(text):
    assert not tab.currencies(text)


def test_income_labels_in_cashflow_reconciliation_are_not_income_statement_facts():
    assert tab.metric_for('Net income', 'cashflow', 'Example') is None
    assert tab.metric_for('Revenues', 'cashflow', 'Example') is None
    assert tab.metric_for('Net income', 'income', 'Example') == 'consolidated_income'


def test_earlier_parent_mention_cannot_authorize_other_company_statement():
    table=simple(issuer='Acquired Other Corporation')
    previous=html.Element('p');previous.text='Example Corporation'
    table.getparent().insert(0,previous)
    with pytest.raises(tab.TabularFinancialError,match='table_issuer_unproved'):
        tab.parse_table(table,issuer='Example Corporation',filed=date(2026,9,1))
