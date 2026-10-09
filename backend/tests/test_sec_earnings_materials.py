import copy
import json

import pytest

from app.services.sec_earnings_materials import discover_earnings_filings, prepare_earnings_release, reconcile_release_receipts


def company(**updates):
    recent = {'accessionNumber': ['0001234567-26-000001'], 'filingDate': ['2026-07-30'],
              'form': ['8-K'], 'primaryDocument': ['report.htm'], 'items': ['2.02,9.01'],
              'acceptanceDateTime': ['2026-07-30T20:00:00Z']}
    recent.update(updates)
    return json.dumps({'cik': 1234567, 'tickers': ['TEST'], 'filings': {'recent': recent}}).encode()


def filing():
    return discover_earnings_filings(company(), symbol='TEST', cik='1234567')[0]


def submission(*, form='8-K', title='Test Reports Second Quarter Results', description='Press release', extra='', body_extra=''):
    body = '<html><body><h1>' + title + '</h1><p>Revenue and net income increased. ' + 'Financial operating information. ' * 50 + '</p>' + body_extra + '</body></html>'
    return (f'''<SEC-HEADER>
<ACCEPTANCE-DATETIME>20260730160000
ACCESSION NUMBER: 0001234567-26-000001
CONFORMED SUBMISSION TYPE: {form}
FILED AS OF DATE: 20260730
CENTRAL INDEX KEY: 0001234567
ITEM INFORMATION: Results of Operations and Financial Condition
</SEC-HEADER>
<DOCUMENT>
<TYPE>{form}
<FILENAME>report.htm
<TEXT><html><body><table><tr><td><a href="release.htm">99.1</a></td><td>{description}</td></tr></table></body></html></TEXT>
</DOCUMENT>
<DOCUMENT>
<TYPE>EX-99.1
<FILENAME>release.htm
<TEXT>{body}</TEXT>
</DOCUMENT>{extra}''').encode()


def test_identity_provenance_and_repeat():
    row = prepare_earnings_release(submission(), filing=filing())
    assert row['status'] == 'prepared'
    assert row['publication_eligible'] is False
    assert row['availability_status'] == 'unverified'
    assert row['header_accepted_local_raw'] == '20260730160000'
    assert row['submissions_accepted_at'] == '2026-07-30T20:00:00Z'
    assert reconcile_release_receipts([row, row]) == [row]
    assert row['release']['url'].endswith('/release.htm')


def test_past_tense_results_release():
    row = prepare_earnings_release(submission(title='Test today announced the following results for the quarter ended June 30'), filing=filing())
    assert row['status'] == 'prepared'


@pytest.mark.parametrize('before,after', [('0001234567-26-000001','0001234567-26-000002'),
    ('CENTRAL INDEX KEY: 0001234567','CENTRAL INDEX KEY: 0001234568'),
    ('FILED AS OF DATE: 20260730','FILED AS OF DATE: 20260729'),
    ('<FILENAME>release.htm','<FILENAME>../release.htm'),
    ('<TYPE>8-K','<TYPE>10-Q'),
    ('ITEM INFORMATION: Results of Operations and Financial Condition','ITEM INFORMATION: Other Events')])
def test_reject_mismatch(before, after):
    with pytest.raises(ValueError):
        prepare_earnings_release(submission().replace(before.encode(),after.encode()), filing=filing())


@pytest.mark.parametrize('updates', [{'items': []}, {'items': [None]}, {'form': []}])
def test_discovery_rejects_bad_columns(updates):
    with pytest.raises(ValueError):
        discover_earnings_filings(company(**updates), symbol='TEST', cik=1234567)


def test_discovery_requires_exact_issuer_and_item():
    with pytest.raises(ValueError):
        discover_earnings_filings(company(), symbol='OTHER', cik=1234567)
    assert discover_earnings_filings(company(items=['12.02']), symbol='TEST', cik=1234567) == []


@pytest.mark.parametrize('title,description', [
    ('Test Production, Deliveries & Deployments', 'Press release'),
    ('Test Reports Second Quarter Results', 'Earnings release financial supplement'),
    ('CFO Commentary on Second Quarter Results', 'CFO Commentary'),
    ('Test Reports Second Quarter Results', 'Exhibit 99.1')])
def test_other_material_is_held(title, description):
    row = prepare_earnings_release(submission(title=title, description=description), filing=filing())
    assert row['reason'] == 'no_verified_earnings_release'


def test_amendment_held():
    discovered = discover_earnings_filings(company(form=['8-K/A']), symbol='TEST', cik=1234567)[0]
    assert prepare_earnings_release(submission(form='8-K/A'), filing=discovered)['reason'] == 'amendment_requires_review'


def test_duplicate_filename_rejected():
    extra = '<DOCUMENT>\n<TYPE>EX-99.2\n<FILENAME>release.htm\n<TEXT>other</TEXT>\n</DOCUMENT>'
    with pytest.raises(ValueError, match='Duplicate'):
        prepare_earnings_release(submission(extra=extra), filing=filing())


def test_conflicting_revision_rejected_and_cross_filing_copy_held():
    row = prepare_earnings_release(submission(), filing=filing())
    revision = {**row, 'source_sha256': 'changed'}
    with pytest.raises(ValueError, match='revision'):
        reconcile_release_receipts([row, revision])
    another = copy.deepcopy(row)
    another['canonical_key'] += ':other'
    assert all(r['reason'] == 'duplicate_content_across_filings' for r in reconcile_release_receipts([row, another]))


def test_hidden_html_does_not_enter_evidence():
    row = prepare_earnings_release(submission(body_extra='<script>secret</script><style>secret</style><ix:hidden>secret</ix:hidden>'), filing=filing())
    assert 'secret' not in row['release']['text']


def test_earnings_discovery_survives_high_volume_issuer_list_cap():
    payload = json.loads(company())
    recent = payload['filings']['recent']
    for i in range(2001):
        for key in recent:
            recent[key].append({'accessionNumber': f'0001234567-26-{i+2:06d}',
                'filingDate': '2026-08-01', 'form': '424B2', 'items': '',
                'primaryDocument': 'note.htm', 'acceptanceDateTime': '2026-08-01T20:00:00Z'}[key])
    rows = discover_earnings_filings(json.dumps(payload).encode(), symbol='TEST', cik=1234567)
    assert len(rows) == 1
    assert rows[0]['accession_number'] == '0001234567-26-000001'


def test_multiple_release_candidates_are_held():
    raw = submission()
    original = raw.split(b'<DOCUMENT>')[2]
    extra = original.replace(b'release.htm', b'second.htm').replace(b'EX-99.1', b'EX-99.2')
    raw = raw.replace(b'</table>', b'<tr><td><a href="second.htm">Press release</a></td></tr></table>')
    assert prepare_earnings_release(raw + b'<DOCUMENT>' + extra, filing=filing())['reason'] == 'ambiguous_releases'


def test_unrelated_old_filename_does_not_block_earnings_discovery():
    payload=json.loads(company()); recent=payload['filings']['recent']
    extra={'accessionNumber':'0001234567-14-000001','filingDate':'2014-01-31','form':'SC 13G/A',
           'primaryDocument':'wd-40.co..txt','items':'','acceptanceDateTime':'2014-01-31T20:00:00Z'}
    for key in recent: recent[key].append(extra[key])
    assert len(discover_earnings_filings(json.dumps(payload).encode(),symbol='TEST',cik=1234567))==1
    with pytest.raises(ValueError,match='filename unsafe'):
        discover_earnings_filings(company(primaryDocument=['../release.htm']),symbol='TEST',cik=1234567)
