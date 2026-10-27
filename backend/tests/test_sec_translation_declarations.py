"""Explicit translation direction, aliases and complete SEC source qualification."""
from datetime import date,datetime,timezone
from pathlib import Path
import gzip,hashlib,json,socket
import pytest
from app.services import sec_tabular_reporting as reporting,sec_document_sets as sets
from app.services import sec_financial_discovery as discovery,sec_financial_discovery_reader as reader
from app.services import sec_fundamentals as core,sec_financial_statements as panels

FACTS={'facts':[{'currency':'CNY'},{'currency':'USD'}]}
BASIS='The financial statements are prepared in accordance with U.S. GAAP. '

@pytest.mark.parametrize('wording',[
 'The translation of RMB into US$ is solely for the convenience of the reader.',
 'This announcement contains translations of certain Renminbi (RMB) amounts into U.S. dollars (US$) solely for the convenience of the reader.',
 'The translation of CNY to USD is for the convenience of the readers.',
 'The translations from RMB into USD are for the convenience of the reader.',
 'The conversion of RMB into USD is for the convenience of the reader.',
])
def test_explicit_native_to_convenience_direction(wording):
 result=reporting.declaration(('<html><p>'+BASIS+wording+'</p></html>').encode(),FACTS)
 assert result['currency']=='CNY' and result['accounting_basis']=='US GAAP'
 assert {(r['currency'],r['convenience_currency']) for r in result['translations']}=={('CNY','USD')}

@pytest.mark.parametrize('wording',[
 'The hypothetical translation of RMB into USD is for the convenience of the reader.',
 'No translation of RMB into USD is for the convenience of the reader.',
 'The translation of RMB (USD) into USD is for the convenience of the reader.',
 'The translation of RMB into USD (CNY) is for the convenience of the reader.',
 'The translation of RMB into RMB is for the convenience of the reader.',
 'The translation of certain amounts into USD is for the convenience of the reader.',
 'The translation of RMB into USD is for a business transaction. '+'Other unrelated material. '*40+'For the convenience of the reader.',
 'Translations of RMB into USD and translations of USD into RMB are for the convenience of the reader.',
])
def test_unproved_or_conflicting_direction_remains_held(wording):
 with pytest.raises(reporting.TabularReportingError):
  reporting.declaration(('<html><p>'+BASIS+wording+'</p></html>').encode(),FACTS)

def originals():
 folder=Path(__file__).parent/'fixtures/sec_translation_declarations'
 manifest=json.loads((folder/'sources.json').read_text());raw={}
 for key,meta in manifest.items():
  raw[key]=gzip.decompress((folder/meta['file']).read_bytes())
  assert hashlib.sha256(raw[key]).hexdigest()==meta['sha256'] and len(raw[key])==meta['bytes']
 return raw,manifest

def test_complete_original_qualification_preserves_native_values_and_exact_repeat(monkeypatch):
 monkeypatch.setattr(socket.socket,'connect',lambda *a,**k:pytest.fail('No network'))
 raw,manifest=originals();now=datetime(2026,10,27,10,48,tzinfo=timezone.utc)
 common=dict(symbol='BZUN',cik='0001625414',available_by=date(2026,10,26),observed_at=now)
 plan=discovery.inspect(raw['company'],**common)
 candidate=next(c for c in sets.candidates(raw['company'],symbol='BZUN',cik='0001625414',available_by=common['available_by']) if c['accession']=='0001104659-26-102080')
 identity={k:candidate[k] for k in discovery.IDENTITY_FIELDS}
 cover=discovery._observe(raw['cover'],identity,now)
 state=plan['state'];state['receipts'][discovery._fingerprint(identity)]=cover
 sources={manifest[k]['url']:raw[k] for k in ['cover','statement']}
 class Client:
  def get(self,url):return sources[url]
 result=reader.inspect_candidate(raw['company'],raw['facts'],checkpoint=state,accession=candidate['accession'],client=Client(),**common)
 assert result==reader.inspect_candidate(raw['company'],raw['facts'],checkpoint=state,accession=candidate['accession'],client=Client(),**common)
 assert result['receipt']['status']=='qualified'
 supplement=result['supplement'];evidence=supplement['evidence']['table_reporting']
 assert evidence['currency']=='CNY' and evidence['accounting_basis']=='US GAAP'
 checked=reporting.validate(supplement,company_raw=raw['company'],symbol='BZUN',cik='0001625414',available_by=common['available_by'])
 assert all(p['unit']=='CNY' for values in checked['records'].values() for p in values.values() if p)
 args=dict(symbol='BZUN',cik='0001625414',observed_at=now,filing_supplement=supplement)
 first=panels.project(raw['facts'],raw['company'],**args)
 assert first==panels.project(raw['facts'],raw['company'],**args)
 values=core.project_fundamentals(raw['facts'],raw['company'],**args)
 assert values==core.project_fundamentals(raw['facts'],raw['company'],**args)
 assert values['current_ratio']==first['summary']['currentRatio']==6751999000/3554552000
 revenues={r['date']:r['revenue'] for r in first['quarterly']}
 assert revenues['2026-06-30']==2742973000 and revenues['2025-06-30']==2552661000
 assert all(r['eps'] is None and r['freeCashFlow'] is None for r in first['quarterly'])
 assert result['receipt']['financial_publication_eligible'] is False

def test_previous_qualification_version_requires_original_revalidation():
 assert reader.validate_receipt({'version':'sec_financial_discovery_qualification_v1'}, {}, now=datetime.now(timezone.utc)) is None


@pytest.mark.parametrize('word',['INCOME','LOSS'])
def test_split_complete_statement_title_retains_values(word):
 from test_sec_tabular_financials import simple
 from app.services import sec_tabular_financials as tables
 table=simple(heading='UNAUDITED CONSOLIDATED STATEMENTS OF COMPREHENSIVE</p><p>'+word)
 result=tables.parse_table(table,issuer='Example Corporation',filed=date(2026,9,1))
 assert result['kind']=='income' and result['observations'][0]['value']==1250000

@pytest.mark.parametrize('fragment',['NON-GAAP INCOME','INCOME OF OTHER COMPANY','Other Company</p><p>INCOME'])
def test_split_title_cannot_join_unrelated_or_adjusted_caption(fragment):
 from test_sec_tabular_financials import simple
 from app.services import sec_tabular_financials as tables
 table=simple(heading='UNAUDITED CONSOLIDATED STATEMENTS OF COMPREHENSIVE</p><p>'+fragment)
 assert tables.parse_table(table,issuer='Example Corporation',filed=date(2026,9,1)) is None

@pytest.mark.parametrize('paragraphs',[9,20])
def test_empty_layout_does_not_discard_closest_issuer(paragraphs):
 from test_sec_tabular_financials import simple
 from app.services import sec_tabular_financials as tables
 from lxml import html
 table=simple();parent=table.getparent()
 for _ in range(paragraphs):
  blank=html.Element('p');blank.text=' ';parent.insert(1,blank)
 result=tables.parse_table(table,issuer='Example Corporation',filed=date(2026,9,1))
 assert result['observations'][0]['value']==1250000

@pytest.mark.parametrize('mode',['different_owner','too_distant','adjusted'])
def test_fallback_does_not_borrow_parent_owner_or_cross_search_bound(mode):
 from test_sec_tabular_financials import simple
 from app.services import sec_tabular_financials as tables
 from lxml import html
 table=simple();parent=table.getparent()
 for _ in range(80 if mode=='too_distant' else 12):parent.insert(1,html.Element('p'))
 if mode=='different_owner':
  other=html.Element('p');other.text='Other Corporation';parent.insert(len(parent)-3,other)
 elif mode=='adjusted':parent[-3].text='Non-GAAP CONSOLIDATED STATEMENTS OF OPERATIONS'
 if mode=='adjusted':assert tables.parse_table(table,issuer='Example Corporation',filed=date(2026,9,1)) is None
 else:
  with pytest.raises(tables.TabularFinancialError,match='issuer_unproved'):
   tables.parse_table(table,issuer='Example Corporation',filed=date(2026,9,1))


def test_new_equivalent_wording_preserves_existing_complete_proof():
 legacy='Translations from RMB to USD are for the convenience of the readers.'
 additional='Translations of certain RMB amounts into USD are for the convenience of the readers.'
 old=reporting.declaration(('<p>'+BASIS+legacy+'</p>').encode(),FACTS)
 for text in (additional+legacy,legacy+additional):
  assert reporting.declaration(('<p>'+BASIS+text+'</p>').encode(),FACTS)==old

@pytest.mark.parametrize('text',[
 'Translations from RMB to USD and translations of USD into RMB are for the convenience of the readers.',
 'Translations of RMB into USD and translations from USD to RMB are for the convenience of the readers.',
])
def test_new_and_legacy_conflicting_directions_are_both_checked(text):
 with pytest.raises(reporting.TabularReportingError,match='translation_basis_unproved'):
  reporting.declaration(('<p>'+BASIS+text+'</p>').encode(),FACTS)
