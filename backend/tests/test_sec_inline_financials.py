from copy import deepcopy
from datetime import date, datetime, timezone
import hashlib
import json
import pytest
from app.services import sec_inline_financials as inline, sec_fundamentals as sec
from test_sec_fundamentals import sources

NOW = datetime(2026, 10, 10, 23, tzinfo=timezone.utc)
TAG = 'RegulatedAndUnregulatedOperatingRevenue'
ACC = '0000000001-26-000002'
URL = 'https://www.sec.gov/Archives/edgar/data/1/000000000126000002/report.htm'


def context(key='parent', segment='', cik='1'):
    return f'<xbrli:context id="{key}"><xbrli:entity><xbrli:identifier scheme="http://www.sec.gov/CIK">{cik}</xbrli:identifier>{segment}</xbrli:entity><xbrli:period><xbrli:startDate>2026-01-01</xbrli:startDate><xbrli:endDate>2026-06-30</xbrli:endDate></xbrli:period></xbrli:context>'


def fact(value='70', ctx='parent', tag=TAG, attrs=''):
    return f'<ix:nonFraction name="us-gaap:{tag}" contextRef="{ctx}" unitRef="usd" {attrs}>{value}</ix:nonFraction>'


def document(extra='', primary=None):
    return ('''<html xmlns:ix="http://www.xbrl.org/2013/inlineXBRL" xmlns:us-gaap="http://fasb.org/us-gaap/2026" xmlns:dei="http://xbrl.sec.gov/dei/2026" xmlns:xbrli="http://www.xbrl.org/2003/instance" xmlns:iso4217="http://www.xbrl.org/2003/iso4217" xmlns:ixt="http://www.xbrl.org/inlineXBRL/transformation/2020-02-12"><body>''' + context() +
        '<xbrli:unit id="usd"><xbrli:measure>iso4217:USD</xbrli:measure></xbrli:unit>' +
        '<ix:nonNumeric name="dei:EntityCentralIndexKey" contextRef="parent">1</ix:nonNumeric>' +
        '<ix:nonNumeric name="dei:DocumentType" contextRef="parent">10-Q</ix:nonNumeric>' +
        '<ix:nonNumeric name="dei:DocumentPeriodEndDate" contextRef="parent">2026-06-30</ix:nonNumeric>' +
        (fact() if primary is None else primary) + extra + '</body></html>').encode()


def parse(raw, **kwargs):
    args = dict(cik='1', accession=ACC, filed='2026-08-01', report_date='2026-06-30', form='10-Q', url=URL)
    args.update(kwargs)
    return inline.parse(raw, **args)


def lagging():
    data, company = sources()
    for content in data['facts']['us-gaap'].values():
        content['units']['USD'] = [r for r in content['units']['USD'] if r['end'] != '2026-06-30']
    company['filings'] = {'recent': dict(accessionNumber=[ACC], form=['10-Q'], filingDate=['2026-08-01'], reportDate=['2026-06-30'], primaryDocument=['report.htm'])}
    return data, company


class Source:
    def __init__(self): self.reads = []
    def get(self, url):
        self.reads.append(url)
        assert url == URL
        return document()


def acquire(data, company, source, **kwargs):
    return inline.acquire(json.dumps(data).encode(), json.dumps(company).encode(), symbol='ABC', cik='1', observed_at=kwargs.get('now', NOW), client=source)


def test_same_cik_segmented_subsidiary_and_other_entities_are_excluded():
    extra = context('subsidiary', '<xbrli:segment><member>subsidiary</member></xbrli:segment>') + fact('999', 'subsidiary')
    extra += context('other', cik='2') + fact('888', 'other')
    result = parse(document(extra))
    assert result['facts'][TAG]['units']['USD'][0]['val'] == 70
    assert result['evidence']['reported_facts'] == 1
    assert next(iter(result['provenance'].values()))['context_id'] == 'parent'


@pytest.mark.parametrize('replacement', [
    ('http://fasb.org/us-gaap/2026', 'https://example.com/fake'),
    ('http://www.xbrl.org/2013/inlineXBRL', 'https://example.com/fake'),
    ('contextRef="parent">1</ix:nonNumeric>', 'contextRef="parent">2</ix:nonNumeric>'),
    ('>10-Q</ix:nonNumeric>', '>10-K</ix:nonNumeric>'),
    ('>2026-06-30</ix:nonNumeric>', '>2026-03-31</ix:nonNumeric>'),
    ('iso4217:USD</xbrli:measure>', 'iso4217:EUR</xbrli:measure>'),
    ('<body>', '<body xmlns:ixt="https://example.com/spoof">'),
])
def test_bad_namespaces_identity_dates_and_currency_fail_closed(replacement):
    with pytest.raises(inline.InlineFactsError): parse(document().replace(*(v.encode() for v in replacement)))


@pytest.mark.parametrize('extra,reason', [
    (context(), 'duplicate_context'),
    ('<xbrli:unit id="usd"><xbrli:measure>iso4217:USD</xbrli:measure></xbrli:unit>', 'duplicate_unit'),
    (fact('71'), 'conflicting_reported_facts'),
])
def test_conflicting_identifiers_and_values_are_held(extra, reason):
    with pytest.raises(inline.InlineFactsError, match=reason): parse(document(extra))


def test_identical_duplicate_is_one_fact_and_original_bytes_have_provenance():
    raw = document(fact())
    result = parse(raw)
    assert result['evidence']['reported_facts'] == 1
    proof = next(iter(result['provenance'].values()))
    assert proof['source_sha256'] == hashlib.sha256(raw).hexdigest()
    assert proof['source_url'] == URL and proof['source_cik'] == '0000000001'


@pytest.mark.parametrize('value,attrs,expected', [
    ('1,234.5', 'format="ixt:num-dot-decimal" scale="6"', 1234500000),
    ('12', 'sign="-" scale="3"', -12000),
    ('-', 'format="ixt:fixed-zero"', 0),
])
def test_reported_transformations(value, attrs, expected):
    r = parse(document(primary=fact(value, attrs=attrs)))
    assert r['facts'][TAG]['units']['USD'][0]['val'] == expected


@pytest.mark.parametrize('value,attrs', [
    ('1,23', 'format="ixt:num-dot-decimal"'), ('NaN', ''), ('-', ''),
    ('12', 'scale="99"'), ('12', 'continuedAt="other"'), ('12', 'xsi:nil="true"'),
    ('12', 'format="ixt:other"'), ('12', 'sign="+"'),
    ('12', 'format="fake:num-dot-decimal"'),
])
def test_unsupported_numeric_facts_do_not_become_values(value, attrs):
    with pytest.raises(inline.InlineFactsError): parse(document(primary=fact(value, attrs=attrs)))


def test_one_original_fetch_only_for_new_completed_filing_day():
    data, company = lagging(); source = Source()
    assert acquire(data, company, source)['evidence']['accession'] == ACC
    assert source.reads == [URL]
    source.reads.clear()
    assert acquire(data, company, source, now=NOW.replace(month=8, day=1)) is None
    assert source.reads == []
    current, _ = sources()
    assert acquire(current, company, source) is None
    unsupported = deepcopy(data); unsupported['facts'] = {'ifrs-full': {}}
    assert acquire(unsupported, company, source) is None
    assert source.reads == []


@pytest.mark.parametrize('change', ['wrong_cik', 'wrong_ticker', 'shape', 'ambiguous', 'unsafe_filename', 'bad_accession'])
def test_acquisition_rejects_unverified_directory_before_fetch(change):
    data, company = lagging(); r = company['filings']['recent']; source = Source()
    if change == 'wrong_cik': company['cik'] = 2
    if change == 'wrong_ticker': company['tickers'] = ['XYZ']
    if change == 'shape': r['primaryDocument'] = []
    if change == 'unsafe_filename': r['primaryDocument'] = ['../../other.htm']
    if change == 'bad_accession': r['accessionNumber'] = ['invalid']
    if change == 'ambiguous':
        for values in r.values(): values.append(values[0])
        r['accessionNumber'][1] = '0000000001-26-000003'
    with pytest.raises(inline.InlineFactsError): acquire(data, company, source)
    assert source.reads == []


def test_projection_preserves_raw_hash_and_uses_total_revenue_consistently():
    data, company = sources()
    # Total revenue history is larger than its contract-revenue component.
    gaap = data['facts']['us-gaap']
    gaap[TAG] = deepcopy(gaap['RevenueFromContractWithCustomerExcludingAssessedTax'])
    for row in gaap[TAG]['units']['USD']: row['val'] *= 2
    gaap[TAG]['units']['USD'].pop()
    raw = json.dumps(data).encode(); original = deepcopy(data)
    filing = parse(document(primary=fact('140')))
    result = sec.project_fundamentals(raw, json.dumps(company).encode(), symbol='ABC', cik='1', observed_at=NOW, filing_supplement=filing)
    evidence = json.loads(result['source_evidence_json'])
    assert evidence['current']['revenue']['value'] == 260
    assert evidence['facts']['sha256'] == hashlib.sha256(raw).hexdigest()
    current = [v for v in evidence['current']['revenue']['inputs'] if v['accession'] == ACC]
    assert len(current) == 1 and current[0]['source_url'] == URL
    assert data == original
    assert result == sec.project_fundamentals(raw, json.dumps(company).encode(), symbol='ABC', cik='1', observed_at=NOW, filing_supplement=filing)
    with pytest.raises(inline.InlineFactsError): inline.merge(data, filing, available_by=date(2026, 7, 1))
    other = deepcopy(data); other['cik'] = 2
    with pytest.raises(inline.InlineFactsError): inline.merge(other, filing, available_by=date(2026, 10, 9))


@pytest.mark.parametrize('basis', [None, {'namespace':'us-gaap', 'currency':'USD', 'tags':sec.TAGS}])
def test_large_complete_filing_preserves_values_and_hashes_once(monkeypatch, basis):
    # A real PFG filing is 16,055,207 bytes. Keep every byte, including padding.
    raw = document(extra='<!--' + 'x'*1_000_000 + '-->')
    raw = raw.replace(b'</body>', (b'<!--' + b'x'*1_000_000 + b'-->')*16 + b'</body>')
    expected = hashlib.sha256(raw).hexdigest()
    original = inline.hashlib.sha256
    calls = []
    def count(value):
        calls.append(len(value))
        return original(value)
    monkeypatch.setattr(inline.hashlib, 'sha256', count)
    result = parse(raw, basis=basis)
    assert result['facts'][TAG]['units']['USD'][0]['val'] == 70
    assert result['evidence']['sha256'] == expected
    assert all(p['source_sha256'] == expected for p in result['provenance'].values())
    assert calls == [len(raw)]


def test_oversized_inline_source_is_rejected_before_dom_allocation(monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError('DOM allocated for oversized source')
    monkeypatch.setattr(inline.html, 'fromstring', forbidden)
    with pytest.raises(inline.InlineFactsError, match='filing_size'):
        parse(b' '*(inline.MAX_INLINE_BYTES+1))


def report_date_document(value, transform='ixt:date-month-day-year'):
    return document().replace(
        b'contextRef="parent">2026-06-30</ix:nonNumeric>',
        f'contextRef="parent" format="{transform}">{value}</ix:nonNumeric>'.encode())


@pytest.mark.parametrize('namespace', sorted(inline.TRR))
@pytest.mark.parametrize('value', ['6/30/2026', '06.30.26', '6 - 30 - 2026', ' 6/30/2026 '])
def test_numeric_report_date_preserves_original_provenance(namespace, value):
    raw = report_date_document(value).replace(
        b'http://www.xbrl.org/inlineXBRL/transformation/2020-02-12', namespace.encode())
    result = parse(raw)
    assert result['facts'][TAG]['units']['USD'][0]['end'] == '2026-06-30'
    assert next(iter(result['provenance'].values()))['source_sha256'] == hashlib.sha256(raw).hexdigest()
    assert result == parse(raw)


@pytest.mark.parametrize('value,expected', [('2/29/24', '2024-02-29'), ('1/2/9', '2009-01-02'), ('12/31/99', '2099-12-31')])
def test_numeric_report_date_short_year_is_2000_based(value, expected):
    raw = report_date_document(value).replace(b'2026-06-30', expected.encode()).replace(b'2026-01-01', (expected[:4]+'-01-01').encode())
    assert parse(raw, report_date=expected)['facts'][TAG]['units']['USD'][0]['end'] == expected


@pytest.mark.parametrize('value', ['2/30/2026', '2/29/2025', '13/1/2026', '0/30/2026', '6/0/2026', '6/30/026', '2026/6/30', '6/30/20260', '6/30', '6/30/2026/1'])
def test_invalid_numeric_report_dates_are_rejected(value):
    with pytest.raises(inline.InlineFactsError):
        parse(report_date_document(value))


@pytest.mark.parametrize('value,transform', [('6/29/2026', 'ixt:date-month-day-year'), ('6/30/2026', 'fake:date-month-day-year'), ('6/30/2026', 'ixt:other')])
def test_numeric_report_date_does_not_bypass_identity_or_transform_guards(value, transform):
    with pytest.raises(inline.InlineFactsError):
        parse(report_date_document(value, transform))


def test_unused_local_namespace_alias_does_not_rebind_financial_namespaces():
    extra='<span xmlns:d5p1="http://www.w3.org/1999/xlink" d5p1:href="#note">note</span>'
    result=parse(document(extra))
    assert result['facts'][TAG]['units']['USD'][0]['val']==70
    assert result['evidence']['reported_facts']==1


@pytest.mark.parametrize('prefix', ['ix','xbrli','iso4217','us-gaap','dei','ixt'])
def test_local_root_namespace_rebinding_remains_rejected(prefix):
    extra=f'<span xmlns:{prefix}="https://example.com/fake">note</span>'
    with pytest.raises(inline.InlineFactsError,match='namespace_redefinition'):
        parse(document(extra))


@pytest.mark.parametrize('value', ['1 234', '1\u00a0234', '1,234'])
def test_reviewed_legacy_numeric_and_report_date_transforms(value):
    raw=document(primary=fact(value,attrs='format="ixt:numdotdecimal"')).replace(b'transformation/2020-02-12',b'transformation/2015-02-26')
    raw=raw.replace(b'name="dei:DocumentPeriodEndDate"',b'name="dei:DocumentPeriodEndDate" format="ixt:datemonthdayyearen"')
    raw=raw.replace(b'>2026-06-30</ix:nonNumeric>',b'>June 30, 2026</ix:nonNumeric>')
    raw=raw.replace('\u00a0'.encode(),b'&#160;')
    assert parse(raw)['facts'][TAG]['units']['USD'][0]['val']==1234


@pytest.mark.parametrize('value', ['1 23', '1,23', '1.234,56', '1e3', '-1234'])
def test_legacy_numeric_transform_rejects_unreviewed_formats(value):
    raw=document(primary=fact(value,attrs='format="ixt:numdotdecimal"')).replace(b'transformation/2020-02-12',b'transformation/2015-02-26')
    with pytest.raises(inline.InlineFactsError): parse(raw)


@pytest.mark.parametrize('tag', ['AssetsCurrent', 'NetIncomeLoss'])
def test_partial_companyfacts_update_does_not_hide_missing_revenue_history(tag):
    data, company = lagging()
    # These are current facts from another line, not proof that revenue arrived.
    current, _ = sources()
    row = next(r for r in current['facts']['us-gaap'][tag]['units']['USD'] if r['end'] == '2026-06-30')
    data['facts']['us-gaap'][tag]['units']['USD'].append(row)
    source = Source()
    supplement = acquire(data, company, source)
    assert supplement['evidence']['accession'] == ACC
    assert source.reads == [URL]


def test_complete_revenue_history_keeps_no_request_path_with_partial_other_metrics():
    data, company = lagging()
    current, _ = sources()
    tag = 'RevenueFromContractWithCustomerExcludingAssessedTax'
    data['facts']['us-gaap'][tag] = current['facts']['us-gaap'][tag]
    source = Source()
    assert acquire(data, company, source) is None
    assert source.reads == []


def test_partial_recovery_does_not_fetch_older_report_or_same_day_filing():
    data, company = lagging()
    point = sources()[0]['facts']['us-gaap']['AssetsCurrent']['units']['USD'][-1]
    data['facts']['us-gaap']['AssetsCurrent']['units']['USD'].append(point)
    source = Source()
    assert acquire(data, company, source, now=NOW.replace(month=8, day=1)) is None
    company['filings']['recent']['reportDate'] = ['2026-03-31']
    assert acquire(data, company, source) is None
    assert source.reads == []
