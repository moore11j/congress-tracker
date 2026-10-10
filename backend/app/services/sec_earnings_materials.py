"""Conservative SEC earnings-release discovery and evidence preparation.

These receipts are staging evidence, not published research documents or events.
Never infer a transcript, fiscal quarter, or release timestamp from an 8-K date.
"""
from datetime import date, datetime, timezone
import hashlib
import re
from urllib.parse import urljoin
from zoneinfo import ZoneInfo

from lxml import html

from app.services.sec_company_filings import parse_company_filings


def discover_earnings_filings(raw, *, symbol, cik, limit=20):
    import json

    if not 1 <= limit <= 100:
        raise ValueError('SEC earnings discovery limit outside 1..100')
    company = json.loads(raw)
    recent = company['filings']['recent']
    required = ('accessionNumber', 'filingDate', 'form', 'primaryDocument', 'items')
    if any(not isinstance(recent.get(key), list) for key in required):
        raise ValueError('SEC earnings columns missing')
    size = len(recent['accessionNumber'])
    if size > 50000 or any(len(recent[key]) != size for key in required):
        raise ValueError('SEC earnings columns misaligned or oversized')
    if 'acceptanceDateTime' in recent and (not isinstance(recent['acceptanceDateTime'], list) or len(recent['acceptanceDateTime']) != size):
        raise ValueError('SEC earnings acceptance column misaligned')
    items = recent.get('items')
    if not isinstance(items, list) or len(items) != len(recent['accessionNumber']):
        raise ValueError('SEC filing items column missing or misaligned')
    by_accession = {}
    for accession, value in zip(recent['accessionNumber'], items):
        if not isinstance(value, str):
            raise ValueError('SEC filing items malformed')
        codes = sorted(set(part.strip() for part in value.split(',') if part.strip()))
        if accession in by_accession and by_accession[accession] != codes:
            raise ValueError('Conflicting SEC filing items')
        by_accession[accession] = codes
    # High-volume issuers can have thousands of other filings newer than their
    # earnings release. Filter the validated columns before the list's 2k cap.
    selected = [i for i, accession in enumerate(recent['accessionNumber'])
                if recent['form'][i] in {'8-K', '8-K/A'} and '2.02' in by_accession[accession]]
    company['filings']['recent'] = {key: [values[i] for i in selected]
        for key, values in recent.items() if isinstance(values, list) and len(values) == len(items)}
    earnings = parse_company_filings(json.dumps(company).encode(), symbol=symbol, cik=cik)
    result = []
    for row in earnings['items']:
        if row['form_type'] not in {'8-K', '8-K/A'} or '2.02' not in by_accession[row['accession_number']]:
            continue
        accession = row['accession_number']
        base = f'https://www.sec.gov/Archives/edgar/data/{int(cik)}/{accession.replace("-", "")}/'
        result.append({**row, 'cik': str(cik).zfill(10),
                       'company_sha256': hashlib.sha256(raw).hexdigest(),
                       'submission_url': base + accession + '.txt'})
    return result[:limit]


def _one(pattern, text):
    values = re.findall(pattern, text, re.M | re.I | re.S)
    if len(values) != 1:
        raise ValueError('SEC submission identity missing or ambiguous')
    return values[0].strip()


def _visible(raw):
    doc = html.fromstring(raw)
    for node in doc.xpath('//script|//style|//head|//*[local-name()="hidden" or local-name()="ix:hidden"]'):
        node.drop_tree()
    # itertext with separators avoids joined table cells/paragraph words.
    return doc, ' '.join(' '.join(doc.itertext()).split())


def prepare_earnings_release(raw, *, filing):
    if not isinstance(raw, bytes) or len(raw) > 30_000_000:
        raise ValueError('SEC submission bytes missing or oversized')
    text = raw.decode('utf-8', errors='strict')
    header = _one(r'<SEC-HEADER>(.*?)</SEC-HEADER>', text)
    accession = _one(r'^ACCESSION NUMBER:\s*([^\r\n]+)', header)
    cik = _one(r'^\s*CENTRAL INDEX KEY:\s*(\d+)', header).zfill(10)
    form = _one(r'^CONFORMED SUBMISSION TYPE:\s*([^\r\n]+)', header)
    filed = datetime.strptime(_one(r'^FILED AS OF DATE:\s*(\d{8})', header), '%Y%m%d').date().isoformat()
    header_accepted = _one(r'<ACCEPTANCE-DATETIME>(\d{14})', header)
    datetime.strptime(header_accepted, '%Y%m%d%H%M%S')
    if (accession != filing['accession_number'] or cik != filing['cik']
            or form != filing['form_type'] or filed != filing['filing_date']):
        raise ValueError('SEC submission does not match discovered filing')
    base = f'https://www.sec.gov/Archives/edgar/data/{int(cik)}/{accession.replace("-", "")}/'
    if filing['submission_url'] != base + accession + '.txt':
        raise ValueError('SEC submission URL mismatch')
    date.fromisoformat(filed)
    receipt = {'symbol': filing['symbol'], 'cik': cik, 'accession_number': accession,
               'canonical_key': f'sec:{cik}:{accession}:earnings_release',
               'source_url': filing['submission_url'], 'source_sha256': hashlib.sha256(raw).hexdigest(),
               'company_sha256': filing['company_sha256'], 'filing_date': filed,
               'submissions_accepted_at': filing.get('accepted_date'),
               'header_accepted_local_raw': header_accepted,
               'availability_status': 'unverified', 'publication_eligible': False,
               'status': 'held', 'reason': None, 'candidates': []}
    if form != '8-K':
        return {**receipt, 'reason': 'amendment_requires_review'}
    if not re.search(r'^ITEM INFORMATION:\s*Results of Operations and Financial Condition\s*$', header, re.M):
        raise ValueError('SEC header does not confirm Item 2.02')
    documents = {}
    for block in re.findall(r'<DOCUMENT>(.*?)</DOCUMENT>', text, re.S | re.I):
        kind = _one(r'^<TYPE>([^\r\n]+)', block)
        filename = _one(r'^<FILENAME>([^\r\n]+)', block)
        if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._-]*', filename) or '..' in filename:
            raise ValueError('SEC exhibit filename unsafe')
        if filename in documents:
            raise ValueError('Duplicate SEC document filename')
        body = re.findall(r'<TEXT>(.*?)</TEXT>', block, re.S | re.I)
        if len(body) != 1:
            raise ValueError('SEC document text missing or ambiguous')
        documents[filename] = (kind, body[0])
    primary = [body for kind, body in documents.values() if kind == '8-K']
    if len(primary) != 1:
        raise ValueError('SEC primary 8-K missing or ambiguous')
    primary_doc, _ = _visible(primary[0])
    descriptions = {}
    for link in primary_doc.xpath('//a[@href]'):
        filename = link.get('href')
        if filename not in documents:
            continue
        # Numeric exhibit links (e.g. XOM) carry the description in the table row.
        rows = link.xpath('ancestor::tr[1]')
        description = ' '.join((rows[0] if rows else link).itertext())
        descriptions.setdefault(filename, []).append(description)
    selected = []
    for filename, (kind, body) in documents.items():
        if not re.fullmatch(r'EX-99(?:\.\d+)?', kind, re.I) or not filename.lower().endswith(('.htm', '.html')):
            continue
        _, content = _visible(body)
        description = ' '.join(' '.join(descriptions.get(filename, [])).split())
        lead = content[:1800]
        excluded = re.search(r'financial supplement|cfo commentary|investor relations data summary|presentation|production.{0,30}deliveries', description + ' ' + content[:400], re.I)
        release = re.search(r'(?:earnings|press|news)\s+release', description, re.I)
        results = re.search(r'\b(?:reports?|reported|announces?|announced)\b.{0,160}\b(?:results|net income)\b|\bearnings release\b|\b(?:operating|financial) results\b.{0,160}\b(?:are|were) summarized\b', lead, re.I)
        financial = re.search(r'\b(?:revenues?|net income|net earnings|earnings per|eps)\b', lead, re.I)
        accepted = bool(release and results and financial and not excluded and len(content) >= 1000)
        receipt['candidates'].append({'filename': filename, 'description': description,
                                      'selected': accepted, 'excluded_supplement_or_other_material': bool(excluded)})
        if accepted:
            selected.append({'url': base + filename, 'filename': filename, 'text': content,
                             'text_sha256': hashlib.sha256(content.encode()).hexdigest(),
                             'exhibit_sha256': hashlib.sha256(body.encode()).hexdigest(),
                             'material_kind': 'earnings_release'})
    if len(selected) != 1:
        return {**receipt, 'reason': 'ambiguous_releases' if selected else 'no_verified_earnings_release'}
    return {**receipt, 'status': 'prepared', 'release': selected[0]}


def reconcile_release_receipts(receipts):
    """Collapse exact repeats; conflicting revisions and cross-filing copies need review."""
    by_key, content_keys = {}, {}
    for receipt in receipts:
        key = receipt['canonical_key']
        if key in by_key and by_key[key] != receipt:
            raise ValueError('Conflicting earnings receipt revision')
        by_key[key] = receipt
        if receipt['status'] == 'prepared':
            identity = (receipt['cik'], receipt['release']['text_sha256'])
            content_keys.setdefault(identity, set()).add(key)
    duplicates = {key for keys in content_keys.values() if len(keys) > 1 for key in keys}
    return [({**row, 'status': 'held', 'reason': 'duplicate_content_across_filings'} if key in duplicates else row)
            for key, row in sorted(by_key.items())]


def verify_filing_availability(receipt, index_raw):
    """Verify SEC header/index acceptance and retain API clock discrepancies.

    The result is EDGAR acceptance, not public availability or issuer press time.
    The SEC defines ACCEPTANCE-DATETIME in the complete filing header. Require
    the matching index's corroboration; never repair the API's supplied value.
    """
    if not isinstance(index_raw, bytes) or len(index_raw) > 2_000_000:
        raise ValueError('SEC index bytes missing or oversized')
    doc, text = _visible(index_raw)
    index_url = receipt['source_url'][:-4] + '-index.html'
    links = {urljoin(index_url, a.get('href')) for a in doc.xpath('//a[@href]')}
    complete_urls = {receipt['source_url'],
                    f'https://www.sec.gov/Archives/edgar/data/{int(receipt["cik"])}/{receipt["accession_number"]}.txt'}
    if not complete_urls.intersection(links) or not re.search(r'CIK\s*:\s*' + receipt['cik'] + r'\b', text):
        raise ValueError('SEC index identity mismatch')
    fields = {}
    for node in doc.xpath('//*[contains(concat(" ",normalize-space(@class)," ")," infoHead ")]'):
        key = ' '.join(node.itertext()).strip()
        next_node = node.getnext()
        if key in fields or next_node is None:
            raise ValueError('SEC index fields ambiguous')
        fields[key] = ' '.join(' '.join(next_node.itertext()).split())
    if fields.get('Filing Date') != receipt['filing_date']:
        raise ValueError('SEC index filing date mismatch')
    accepted = datetime.strptime(fields['Accepted'], '%Y-%m-%d %H:%M:%S')
    if accepted.strftime('%Y%m%d%H%M%S') != receipt['header_accepted_local_raw']:
        raise ValueError('SEC index/header acceptance disagreement')
    local = accepted.replace(tzinfo=ZoneInfo('America/New_York'))
    # Reject ambiguous/nonexistent DST wall times, even though normal EDGAR
    # filing hours would not ordinarily produce them.
    if local.utcoffset() != local.replace(fold=1).utcoffset():
        raise ValueError('SEC acceptance local time is ambiguous')
    utc = local.astimezone(timezone.utc)
    if utc.astimezone(ZoneInfo('America/New_York')).replace(tzinfo=None) != accepted:
        raise ValueError('SEC acceptance local time does not exist')
    supplied = receipt.get('submissions_accepted_at')
    indexed = datetime.fromisoformat(supplied.replace('Z', '+00:00')) if supplied else None
    agrees = indexed is not None and indexed.tzinfo is not None and indexed == utc
    comparison = ('missing' if indexed is None else 'timezone_missing' if indexed.tzinfo is None
                  else 'agrees' if agrees else 'conflict')
    result = {**receipt, 'index_sha256': hashlib.sha256(index_raw).hexdigest(),
              'index_url': index_url, 'sec_accepted_at': utc.isoformat(),
              'availability_status': 'verified_sec_acceptance',
              'acceptance_evidence': {
                  'policy': 'sec_header_index_v2', 'timezone': 'America/New_York',
                  'header_local_raw': receipt['header_accepted_local_raw'],
                  'index_local_raw': fields['Accepted'], 'submissions_raw': supplied,
                  'submissions_comparison': comparison,
                  'submissions_delta_seconds': int((indexed-utc).total_seconds()) if indexed is not None and indexed.tzinfo is not None else None,
                  # A later conflicting API value cannot make the publisher
                  # eligible before that instant. This is a conservative guard,
                  # not a claim that either value proves public availability.
                  'observation_not_before': max(utc, indexed).isoformat() if indexed is not None and indexed.tzinfo is not None else utc.isoformat(),
              }}
    # Canonical publication additionally requires persistent source validation
    # and duplicate reconciliation in the database writer.
    return result
