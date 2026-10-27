"""Comparative statement captions cannot mix column years or durations."""
from datetime import date,datetime,timezone
import pytest
from lxml import html
from app.services import sec_tabular_financials as tables,sec_financial_discovery_reader as reader
from test_sec_tabular_financials import simple

def table_with_context(context,*,column='Six Months Ended June 30,'):
 table=simple();table.xpath('./tr')[0].xpath('./td')[1].text=column
 node=html.Element('p');node.text=context;table.addprevious(node)
 return table

@pytest.mark.parametrize('caption',[
 'For the Six Months Ended June 30, 2026 and 2025',
 'For the three and six months ended June 30, 2026 and June 30, 2025',
 'For three and six months ended June 30, 2026 and June 30, 2025',
 'For the three, six and nine months ended June 30, 2026 and 2025',
])
def test_comparative_caption_preserves_each_explicit_column(caption):
 result=tables.parse_table(table_with_context(caption),issuer='Example Corp',filed=date(2026,9,1))
 assert [(r['start'],r['end'],r['value']) for r in result['observations']]==[
  ('2026-01-01','2026-06-30',1250000),('2025-01-01','2025-06-30',1000000)]

@pytest.mark.parametrize('caption',[
 'For the six months ended September 30, 2026 and 2025',
 'For the six months ended June 30, 2024 and 2023',
 'For the three months ended June 30, 2026 and 2025',
 'For the two and six months ended June 30, 2026 and 2025',
 'For the six months ended June 30, 2026 and September 30, 2025',
 'For two years ended June 30, 2026 and 2025',
])
def test_conflicting_or_unproved_comparative_context_is_held(caption):
 with pytest.raises(tables.TabularFinancialError):
  tables.parse_table(table_with_context(caption),issuer='Example Corp',filed=date(2026,9,1))

@pytest.mark.parametrize('column',['','Six Months Ended','Six Months Ended June 35,','Six Months Ended June 30, 2024'])
def test_caption_does_not_supply_missing_or_replace_invalid_own_column(column):
 caption='For the six months ended June 30, 2026 and 2025'
 with pytest.raises(tables.TabularFinancialError):
  tables.parse_table(table_with_context(caption,column=column),issuer='Example Corp',filed=date(2026,9,1))

def test_actual_column_future_date_still_fails():
 table=table_with_context('For the six months ended June 30, 2027 and 2025')
 table.xpath('./tr')[1].xpath('./td')[1].text='2027'
 with pytest.raises(tables.TabularFinancialError,match='after_filing'):
  tables.parse_table(table,issuer='Example Corp',filed=date(2026,9,1))

def test_discovery_rechecks_previous_qualification_version():
 assert reader.validate_receipt({'version':'sec_financial_discovery_qualification_v2'}, {},now=datetime.now(timezone.utc)) is None


def test_complete_asc_original_quarter_columns_match_independent_tagged_values(monkeypatch):
 import socket
 from test_sec_statement_currency_scope import originals,Client,NOW
 from app.services import sec_inline_financials as inline,sec_financial_statements as panels
 monkeypatch.setattr(socket.socket,'connect',lambda *a,**k:pytest.fail('No network'))
 facts,company,cik,urls=originals('ASC')
 url=next(u for u in urls if u.endswith('asc-20260630x6k.htm'));raw=urls[url]
 args=dict(source_url=url,company_raw=company,cover_raw=raw,cover_url=url,symbol='ASC',cik=cik,available_by=date(2026,10,17))
 extracted=tables.extract(raw,**args);assert extracted==tables.extract(raw,**args)
 quarter=[r for r in extracted['facts'] if r['metric']=='revenue' and r['months']==3]
 assert len(quarter)==2 and {r['end'] for r in quarter}=={'2025-06-30','2026-06-30'}
 assert len([r for r in extracted['facts'] if r['metric']=='revenue' and r['months']==6])==2
 supplement=inline.acquire(facts,company,symbol='ASC',cik=cik,observed_at=NOW,client=Client(urls))
 panel=panels.project(facts,company,symbol='ASC',cik=cik,observed_at=NOW,filing_supplement=supplement)
 tagged={(r['periodStart'],r['date']):r['revenue'] for r in panel['quarterly']}
 for row in quarter:
  assert row['currency']=='USD' and tagged[row['start'],row['end']]==row['value']
  assert all(o['header'] for o in row['occurrences'])


@pytest.mark.parametrize('unit,currency',[('€','EUR'),('£','GBP')])
def test_comparative_period_repair_preserves_unicode_currency_columns(unit,currency):
 table=simple(unit=unit,scale='In thousands of '+currency)
 node=html.Element('p');node.text='For six months ended June 30, 2026 and 2025';table.addprevious(node)
 result=tables.parse_table(table,issuer='Example Corp',filed=date(2026,9,1))
 assert {r['currency'] for r in result['observations']}=={currency}
 assert result['observations'][0]['value']==1250000
