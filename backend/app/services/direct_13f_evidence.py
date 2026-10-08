"""Pure SEC identifier and cross-filing value checks for offline publication."""
from collections import defaultdict
from datetime import date, datetime
from decimal import Decimal
import hashlib
import re
from statistics import median
from xml.etree import ElementTree as ET

from app.clients.direct_sources import DirectSourceError
from app.services.direct_feed_collection import parse_document


def nport_identifiers(document, *, available_by: date):
    raw = document['raw']
    digest = hashlib.sha256(raw).hexdigest()
    if digest != document['content_hash']:
        raise DirectSourceError('N-PORT checksum mismatch')
    text = raw.decode('utf-8')
    header = text.split('</SEC-HEADER>', 1)[0]
    accession = re.search(r'ACCESSION NUMBER:\s*(\d{10}-\d{2}-\d{6})', header)
    filed = re.search(r'FILED AS OF DATE:\s*(\d{8})', header)
    form = re.search(r'CONFORMED SUBMISSION TYPE:\s*([^\s]+)', header)
    cik = re.search(r'CENTRAL INDEX KEY:\s*(\d+)', header)
    if not all((accession, filed, form, cik)) or form[1] != 'NPORT-P':
        raise DirectSourceError('Expected original N-PORT submission identity')
    expected_url = f'https://www.sec.gov/Archives/edgar/data/{int(cik[1])}/{accession[1]}.txt'
    if document['url'] != expected_url:
        raise DirectSourceError('N-PORT URL and submission identity mismatch')
    filing_date = datetime.strptime(filed[1], '%Y%m%d').date()
    xmls = re.findall(r'<XML>\s*(.*?)\s*</XML>', text, re.S)
    if len(xmls) != 1:
        raise DirectSourceError('Expected one N-PORT XML document')
    root = ET.fromstring(xmls[0])
    if (root.findtext('.//{*}submissionType') != 'NPORT-P'
            or root.findtext('.//{*}regCik', '').zfill(10) != cik[1].zfill(10)):
        raise DirectSourceError('N-PORT XML identity mismatch')
    period = date.fromisoformat(root.findtext('.//{*}repPdDate'))
    if period > filing_date:
        raise DirectSourceError('N-PORT period after disclosure')
    if filing_date > available_by:
        return []
    rows = []
    for index, node in enumerate(root.findall('.//{*}invstOrSec'), 1):
        cusip = node.findtext('{*}cusip', '').strip().upper()
        identifiers = node.findall('{*}identifiers/{*}ticker')
        if (node.findtext('{*}assetCat') != 'EC' or node.findtext('{*}units') != 'NS'
                or node.find('{*}derivativeInfo') is not None
                or not re.fullmatch(r'[A-Z0-9]{9}', cusip) or len(identifiers) != 1):
            continue
        symbol = identifiers[0].get('value', '').strip().upper().replace('/', '-').replace('.', '-')
        if not re.fullmatch(r'[A-Z]{1,6}(?:-[A-Z])?', symbol):
            continue
        rows.append({'cusip': cusip, 'symbol': symbol, 'source_url': expected_url,
                     'source_sha256': digest, 'source_row': index, 'filing_date': filing_date.isoformat(),
                     'report_period': period.isoformat(), 'issuer': node.findtext('{*}name')})
    return rows


def value_consistency_issues(parsed, documents):
    """Hold extreme unit discrepancies corroborated by two independent filers.

    Never rescale source numbers. Equal quarter/CUSIP equity holdings should
    imply comparable quarter-end prices; this is a diagnostic, not a quote feed.
    """
    meta = parsed['metadata']
    peers = defaultdict(dict)
    def prices(rows):
        totals = defaultdict(lambda: [Decimal(0), Decimal(0)])
        for row in rows:
            if row.get('putCall') or row.get('shareType') != 'SH':
                continue
            total = totals[row['cusip']]
            total[0] += Decimal(str(row['shares']))
            total[1] += Decimal(str(row['valueUsd']))
        return {cusip: value / shares for cusip, (shares, value) in totals.items() if shares > 0 and value > 0}
    for document in documents:
        if hashlib.sha256(document['raw']).hexdigest() != document['content_hash']:
            raise DirectSourceError('13F comparison source checksum mismatch')
        try:
            _, peer, reasons = parse_document('sec_13f', document['raw'], document['metadata'])
        except DirectSourceError:
            continue
        pm = peer['metadata']
        if (reasons or pm['cik'] == meta['cik'] or pm['report_period'] != meta['report_period']
                or pm['filing_date'] > meta['filing_date']):
            continue
        for cusip, price in prices(peer['positions']).items():
            peers[cusip].setdefault(pm['cik'], []).append((price, pm['key']))
    issues = []
    for cusip, price in prices(parsed['positions']).items():
        # Multiple accessions for one manager need amendment reconciliation.
        candidates = [rows[0] for rows in peers[cusip].values() if len(rows) == 1]
        if len(candidates) < 2:
            continue
        observed = [p for p, _ in candidates]
        center = median(observed)
        agreeing = [(p, key) for p, key in candidates if Decimal('.95') <= p / center <= Decimal('1.05')]
        if len(agreeing) < 2:
            continue
        ratio = center / price
        if ratio >= 100 or ratio <= Decimal('.01'):
            issues.append({'cusip': cusip, 'peer_to_reported_unit_value_ratio': str(ratio),
                           'peer_accessions': sorted(key for _, key in agreeing)})
    return issues
