"""Complete-source declarations are evidence, not inferred listing currencies."""
from copy import deepcopy
import hashlib
import json
import pytest
from app.services import sec_statement_currency as currency, sec_inline_financials as inline

URL = 'https://www.sec.gov/Archives/edgar/data/1/000000000126000002/statement.htm'


@pytest.mark.parametrize('text,expected', [
    ('Our reporting currency is USD.', 'USD'),
    ("The Company's reporting currency is the Renminbi (RMB).", 'CNY'),
    ('We use the US Dollar as our reporting currency.', 'USD'),
    ('The interim financial statements are presented in Euro.', 'EUR'),
    ('These condensed consolidated financial statements are presented in thousands of Canadian dollars.', 'CAD'),
    ('Our presentation currency is Brazilian reais.', 'BRL'),
])
def test_explicit_currency_declarations(text, expected):
    raw=('<html><body>'+text+'</body></html>').encode()
    proof=currency.declaration(raw,url=URL)
    assert proof['currency']==expected and proof['status']=='declared'
    assert proof['source_sha256']==hashlib.sha256(raw).hexdigest()
    assert proof['declarations'][0]['text'] in text


@pytest.mark.parametrize('text', [
    'Our functional currency is EUR.',
    'Our reporting currency was EUR.',
    'Our reporting currency is not EUR.',
    'We will use EUR as our reporting currency.',
    'Our reporting currency is dollars.',
    'Our reporting currency is $ and shares are listed in USD.',
    'Our reporting currency is USDollars.',
    '<script>Our reporting currency is USD.</script>',
    '<blockquote>Our reporting currency is EUR.</blockquote>',
])
def test_listing_functional_future_quoted_or_ambiguous_dollars_are_not_declarations(text):
    assert currency.declaration(('<html>'+text+'</html>').encode(),url=URL)['status']=='unavailable'


def test_conflicting_declarations_are_held_and_repeated_identical_declarations_agree():
    for text, status in [('Our reporting currency is USD. Our presentation currency is EUR.', 'ambiguous'),
                         ('Our reporting currency is USD. We use US dollars as our reporting currency.', 'declared')]:
        proof=currency.declaration(('<html>'+text+'</html>').encode(),url=URL)
        assert proof['status']==status and len(proof['declarations'])==2


@pytest.mark.parametrize('symbol,expected', [('ADSE','EUR'),('ALC','USD')])
def test_complete_ifrs_statement_currency_and_tagged_facts_agree(symbol, expected):
    from test_sec_document_sets import originals
    args=originals(symbol)
    parsed=inline.parse(**args)
    proof=currency.declaration(args['raw'],url=args['url'])
    bound=currency.bind(parsed,proof)
    assert bound['currency']==expected and bound['namespace']=='ifrs-full'
    assert bound['cik']==args['cik'] and bound['accession']==args['accession']


def nami():
    from test_sec_document_set_cohort import originals
    from app.services import sec_fundamentals as sec
    facts,company,cik,urls=originals('NAMI')
    url=next(u for u in urls if u.endswith('ex99-1.htm'))
    cover=next(u for u in urls if u.endswith('6k_jinxin.htm'))
    recent=json.loads(company)['filings']['recent'];index=recent['accessionNumber'].index('0001213900-26-101993')
    args=dict(raw=urls[url],url=url,cover_raw=urls[cover],cover_url=cover,
              cik=cik,accession='0001213900-26-101993',filed=recent['filingDate'][index],
              report_date='2026-06-30',form='6-K',document_set=True,
              basis={'namespace':'us-gaap','currency':'CNY','tags':sec.TAGS})
    return args


def test_convenience_usd_is_not_nami_reporting_currency():
    args=nami()
    parsed=inline.parse(**args)
    proof=currency.declaration(args['raw'],url=args['url'])
    assert currency.bind(parsed,proof)['currency']=='CNY'
    assert parsed['evidence']['currency_observation']['current_statement_currencies']==['CNY','USD']
    args['basis']['currency']='USD'
    with pytest.raises(currency.StatementCurrencyError,match='basis_mismatch'):
        currency.bind(inline.parse(**args),proof)


@pytest.mark.parametrize('field,value', [('currency','USD'),('source_sha256','0'*64),('source_url',URL),('status','ambiguous'),('methodology','other')])
def test_binding_rejects_substituted_evidence(field,value):
    args=nami();parsed=inline.parse(**args)
    proof=currency.declaration(args['raw'],url=args['url']);proof[field]=value
    with pytest.raises(currency.StatementCurrencyError):currency.bind(parsed,proof)


@pytest.mark.parametrize('url', [None, 42, 'https://www.sec.gov:bad/statement.htm',
    URL.replace('www.sec.gov', 'www.sec.gov.evil.example'), URL + '?other=1',
    URL.replace('/data/1/', '/data/12345678901/'), URL.replace('https:', 'http:')])
def test_invalid_source_addresses_have_typed_holds(url):
    with pytest.raises(currency.StatementCurrencyError, match='source_url'):
        currency.declaration(b'<html>Our reporting currency is USD.</html>', url=url)


@pytest.mark.parametrize('field,value', [('cik','0000000001'), ('accession','0000000001-26-000002'),
    ('sha256','invalid')])
def test_binding_checks_exact_filing_identity_and_digest_shape(field,value):
    args=nami(); parsed=inline.parse(**args)
    proof=currency.declaration(args['raw'],url=args['url'])
    parsed['evidence'][field]=value
    if field=='sha256': proof['source_sha256']=value
    with pytest.raises(currency.StatementCurrencyError): currency.bind(parsed,proof)


def acquisition_inputs(symbol):
    from test_sec_document_sets import originals,directory
    from test_sec_document_set_cohort import originals as cohort
    if symbol=='NAMI':
        args=nami();_,company,_,urls=cohort(symbol)
    else:
        args=originals(symbol);company=directory(symbol)
        urls={args['url']:args['raw'],args['cover_url']:args['cover_raw']}
    return args,company,urls


@pytest.mark.parametrize('symbol,expected', [('ADSE','EUR'),('ALC','USD'),('NAMI','CNY')])
def test_shared_native_acquisition_uses_explicit_basis_and_fetches_each_original_once(symbol,expected,monkeypatch):
    import socket
    from datetime import date
    from app.services import sec_document_sets as sets
    monkeypatch.setattr(socket.socket,'connect',lambda *a,**kw:pytest.fail('Unexpected network'))
    args,company,urls=acquisition_inputs(symbol);available=date(2026,10,17)
    metadata=next(c for c in sets.candidates(company,symbol=symbol,cik=args['cik'],available_by=available)
                  if c['cover_url']==args['cover_url'])
    class Client:
        def __init__(self):self.calls=[]
        def get(self,url):self.calls.append(url);return urls[url]
    client=Client()
    parsed=currency.acquire_candidate(metadata,company,symbol=symbol,cik=args['cik'],available_by=available,client=client)
    assert client.calls==[args['cover_url'],args['url']]
    assert parsed['evidence']['statement_currency_declaration']['currency']==expected
    assert parsed['evidence']['document_set_discovery']['directory_sha256']==hashlib.sha256(company).hexdigest()
    assert {unit for data in parsed['facts'].values() for unit in data['units']}=={expected}
    client2=Client()
    assert parsed==currency.acquire_candidate(metadata,company,symbol=symbol,cik=args['cik'],available_by=available,client=client2)
    if symbol=='NAMI':
        assert parsed['evidence']['currency_observation']['current_statement_currencies']==['CNY','USD']
    else:
        basis=currency.accounting_basis(args['raw'],url=args['url'])[0]
        assert basis['tags']['net_income']!=basis['tags']['consolidated_income']
        assert basis['tags']['equity']!=basis['tags']['consolidated_equity']


def test_unqualified_candidate_never_reaches_transport():
    from datetime import date
    from app.services import sec_document_sets as sets
    args,company,urls=acquisition_inputs('ADSE');available=date(2026,10,17)
    metadata=next(c for c in sets.candidates(company,symbol='ADSE',cik=args['cik'],available_by=available)
                  if c['cover_url']==args['cover_url'])
    metadata['directory_sha256']='0'*64
    class Client:
        def get(self,url):pytest.fail('Unqualified candidate reached transport')
    with pytest.raises(inline.InlineFactsError,match='candidate_not_in_directory'):
        currency.acquire_candidate(metadata,company,symbol='ADSE',cik=args['cik'],available_by=available,client=Client())


@pytest.mark.parametrize('mixed',[False,True])
def test_unused_taxonomy_declaration_is_not_a_second_accounting_basis(mixed):
    from test_sec_document_sets import originals
    args=originals('ADSE');raw=args['raw']
    if mixed:
        raw=raw.replace(b'</body>', b'<ix:nonFraction name="us-gaap:Revenues">1</ix:nonFraction></body>')
        with pytest.raises(currency.StatementCurrencyError,match='taxonomy'):
            currency.accounting_basis(raw,url=args['url'])
    else:
        assert currency.accounting_basis(raw,url=args['url'])[0]['namespace']=='ifrs-full'


@pytest.mark.parametrize('symbol',['ADSE','ALC','NAMI'])
def test_normalized_records_retain_original_amount_currency_dates_and_provenance(symbol):
    from datetime import date
    from app.services import sec_document_sets as sets
    args,company,urls=acquisition_inputs(symbol);available=date(2026,10,17)
    metadata=next(c for c in sets.candidates(company,symbol=symbol,cik=args['cik'],available_by=available)
                  if c['cover_url']==args['cover_url'])
    class Client:
        def get(self,url):return urls[url]
    parsed=currency.acquire_candidate(metadata,company,symbol=symbol,cik=args['cik'],available_by=available,client=Client())
    before=deepcopy(parsed)
    records=currency.normalized_records(parsed,company,symbol=symbol,cik=args['cik'],available_by=available)
    from app.services.sec_fundamentals import TAGS
    assert set(TAGS) <= set(records)
    assert parsed==before and records['revenue'] and records['assets']
    assert records==currency.normalized_records(parsed,company,symbol=symbol,cik=args['cik'],available_by=available)
    for name,series in records.items():
        for (start,end),point in series.items():
            if point is None:continue
            assert point['unit']==args['basis']['currency']
            assert point['source_sha256']==hashlib.sha256(args['raw']).hexdigest()
            assert point['source_url']==args['url']
            originals=parsed['facts'][point['tag']]['units'][point['unit']]
            assert any(p['val']==point['value'] and p.get('start')==point['start'] and p['end']==point['end'] for p in originals)
            assert (start is None)==(name in currency.INSTANT_METRICS)
    tampered=deepcopy(parsed)
    tampered['provenance'].clear()
    with pytest.raises(currency.StatementCurrencyError,match='fact_provenance'):
        currency.normalized_records(tampered,company,symbol=symbol,cik=args['cik'],available_by=available)
    tampered=deepcopy(parsed)
    tampered['evidence']['document_set_discovery']['directory_sha256']='0'*64
    with pytest.raises(currency.StatementCurrencyError,match='directory_binding'):
        currency.normalized_records(tampered,company,symbol=symbol,cik=args['cik'],available_by=available)
