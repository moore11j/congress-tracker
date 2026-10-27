"""Complete originals and nearest-issuer boundaries across page furniture."""
from pathlib import Path
from datetime import date
from unittest.mock import patch
import gzip,hashlib,json,socket
import pytest
from lxml import html
from app.services import sec_tabular_financials as tables,sec_primary_income as income
from test_sec_tabular_financials import simple

ROOT=Path(__file__).parent/'fixtures/sec_statement_navigation'
MANIFEST=json.loads((ROOT/'sources.json').read_text(encoding='utf-8'))
def source(name):
 m=MANIFEST[name];raw=gzip.decompress((ROOT/m['file']).read_bytes());assert len(raw)==m['bytes'] and hashlib.sha256(raw).hexdigest()==m['sha256']
 return raw,m['filing']
def rows(name):
 raw,f=source(name);filing={k:v for k,v in f.items() if k!='issuer'}
 with patch.object(socket.socket,'connect',side_effect=AssertionError('Offline original')):
  result=income.statement_rows(raw,filing,f['issuer'])
  assert result==income.statement_rows(raw,filing,f['issuer'])
 return result

@pytest.mark.parametrize('name,expected',[
 ('CASY-current.html',{('2026-05-01','2026-07-31'):273720000,('2025-05-01','2025-07-31'):215355000}),
 ('GTX-current.html',{('2026-04-01','2026-06-30'):101000000,('2025-04-01','2025-06-30'):87000000,('2026-01-01','2026-06-30'):196000000,('2025-01-01','2025-06-30'):149000000}),
])
def test_original_tax_wording_keeps_period_currency_arithmetic_and_identity(name,expected):
 result=rows(name);assert {(p['start'],p['end']):p['value'] for p in result.values()}==expected
 assert all(p['pretax']-p['tax']==p['value'] and p['unit']=='USD' and p['source_sha256']==MANIFEST[name]['sha256'] for p in result.values())

def test_annual_layout_is_readable_but_incompatible_pretax_tag_stays_held():
 raw,f=source('CASY-annual.html');tree=html.fromstring(raw)
 table=next(t for t in tree.xpath('//table') if not t.xpath('.//table') and 'CONSOLIDATED STATEMENTS OF INCOME' in ' '.join(tables.preceding_text(t,nonempty=True)) and 'Total revenue' in tables.visible_text(t))
 panel=tables.parse_table(table,issuer=f['issuer'],filed=date.fromisoformat(f['filed']),currency_scope={'issuer':f['issuer'],'currency':'USD','basis':'diagnostic; does not qualify consolidated earnings'})
 assert {(p['end'],p['value']) for p in panel['observations'] if p['metric']=='consolidated_income'}=={('2026-04-30',714448000),('2025-04-30',546520000),('2024-04-30',501972000)}
 assert any('Table of Contents'==x for x in panel['heading'])
 # The visible pretax row is tagged IncomeLossAttributableToParent. Page
 # furniture and label support never convert that tag to a pretax concept.
 assert rows('CASY-annual.html')=={}

def with_blocks(blocks,issuer='Example Corporation'):
 table=simple(issuer=issuer);parent=table.getparent();heading=parent.xpath('./p')[1]
 for text in blocks:
  n=html.Element('p');n.text=text;heading.addprevious(n)
 return table

@pytest.mark.parametrize('blocks',[
 ['32','Table of Contents'],
 ['See accompanying Notes to Consolidated Financial Statements.','32','Table of Contents'],
 ['See notes to unaudited condensed consolidated financial statements.','2','Table of Contents'],
])
def test_exact_furniture_preserves_closest_explicit_issuer_and_original_heading(blocks):
 table=with_blocks(blocks);p=tables.parse_table(table,issuer='Example Corp',filed=date(2026,9,1))
 assert [x['value'] for x in p['observations']]==[1250000,1000000]
 assert all(x in p['heading'] for x in blocks)

@pytest.mark.parametrize('intervening',[
 'Unrelated Subsidiary Inc.','Acquired Target Corporation',
 'See accompanying statements of Another Company.','Table of Contents for Other Issuer',
 'Related company results','Summary of acquired business',
])
def test_navigation_cannot_cross_a_competing_or_unknown_owner(intervening):
 with pytest.raises(tables.TabularFinancialError,match='table_issuer_unproved'):
  tables.parse_table(with_blocks([intervening,'32','Table of Contents']),issuer='Example Corp',filed=date(2026,9,1))

def test_navigation_does_not_supply_missing_or_distant_issuer():
 with pytest.raises(tables.TabularFinancialError,match='table_issuer_unproved'):
  tables.parse_table(with_blocks(['Table of Contents'],issuer=''),issuer='Example Corp',filed=date(2026,9,1))
 with pytest.raises(tables.TabularFinancialError,match='table_issuer_unproved'):
  tables.parse_table(with_blocks(['Table of Contents']*9),issuer='Example Corp',filed=date(2026,9,1))

@pytest.mark.parametrize('change',['tag','tax_value','tax_label','issuer','dimension'])
def test_new_tax_wording_does_not_override_source_conflicts(change):
 raw,f=source('CASY-current.html');tree=html.fromstring(raw);filing={k:v for k,v in f.items() if k!='issuer'};issuer=f['issuer']
 from app.services import sec_inline_financials as inline
 taxes=[n for n in inline.nodes(tree,'nonfraction') if n.get('name')=='us-gaap:IncomeTaxExpenseBenefit']
 if change=='tag':
  for n in taxes:n.set('name','us-gaap:OtherNonoperatingIncomeExpense')
 elif change=='tax_value':
  for n in taxes:n.text='1'
 elif change=='tax_label':
  for n in taxes:
   row=n.xpath('ancestor::tr')[-1];cell=row.xpath('./td|./th')[0];cell.clear();cell.text='Deferred tax assets'
 elif change=='issuer':issuer='Unrelated Company'
 else:
  for n in inline.nodes(tree,'context'):n.append(html.Element('xbrli:segment'))
 try:result=income.statement_rows(html.tostring(tree),filing,issuer)
 except ValueError:result={}
 assert not result

def test_serializing_complete_source_preserves_new_positive_case():
 raw,f=source('CASY-current.html');filing={k:v for k,v in f.items() if k!='issuer'}
 result=income.statement_rows(html.tostring(html.fromstring(raw)),filing,f['issuer'])
 assert len(result)==2
