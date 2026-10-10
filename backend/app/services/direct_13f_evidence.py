"""Pure SEC identifier and cross-filing value checks for offline publication."""
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
import hashlib
import json
import re
from statistics import median
from xml.etree import ElementTree as ET

from app.clients.direct_sources import DirectSourceError
from app.services.direct_feed_collection import parse_document


def _evidence_key(document):
    if hashlib.sha256(document['raw']).hexdigest() != document['content_hash']:
        raise DirectSourceError('Prepared evidence checksum mismatch')
    return json.dumps({key:document.get(key) for key in ('content_hash','url','metadata','feed')},
                      sort_keys=True,default=str)


def _equity_totals(rows):
    totals = defaultdict(lambda: [Decimal(0), Decimal(0)])
    for row in rows:
        if row.get('putCall') or row.get('shareType') != 'SH':
            continue
        total = totals[row['cusip']]
        total[0] += Decimal(str(row['shares']))
        total[1] += Decimal(str(row['valueUsd']))
    return tuple((cusip, shares, value) for cusip, (shares, value) in totals.items())


def _equity_prices(totals, cusips=None):
    return {cusip: value / shares for cusip, shares, value in totals
            if (cusips is None or cusip in cusips) and shares > 0 and value > 0}


@dataclass(frozen=True)
class Prepared13FEvidence:
    """Per-batch immutable parses; every reuse still checks original bytes/identity.

    JSON strings keep returned dictionaries from mutating cached evidence. No
    process-wide cache retains large source files after the bounded batch ends.
    """
    _identifiers: tuple
    _comparisons: tuple
    _value_comparisons: tuple

    @classmethod
    def build(cls, identifier_documents, comparison_documents):
        if len(identifier_documents)>50 or not 1<=len(comparison_documents)<=500:
            raise ValueError('Prepared evidence exceeds document bounds')
        if sum(len(d['raw']) for d in (*identifier_documents,*comparison_documents))>100_000_000:
            raise ValueError('Prepared evidence exceeds byte bounds')
        identifiers=[];comparisons=[];values=[]
        for document in identifier_documents:
            key=_evidence_key(document)
            identifiers.append((key,json.dumps(nport_identifiers(document,available_by=date.max))))
        for document in comparison_documents:
            key=_evidence_key(document)
            try:
                _,parsed,reasons=parse_document('sec_13f',document['raw'],document['metadata'])
                value=json.dumps([parsed,reasons],default=str)
                compact=(json.dumps(parsed['metadata'],default=str),json.dumps(reasons),_equity_totals(parsed['positions']))
            except DirectSourceError:
                value=None
                compact=None
            values.append((key,compact))
            comparisons.append((key,value))
        return cls(tuple(identifiers),tuple(comparisons),tuple(values))

    def _lookup(self, document, records):
        key=_evidence_key(document)
        for saved,value in records:
            if saved==key:return json.loads(value) if value is not None else None
        raise DirectSourceError('Prepared evidence identity changed')

    def identifiers(self, document, available_by):
        rows=self._lookup(document,self._identifiers)
        return [row for row in rows if row['filing_date']<=available_by.isoformat()]

    def comparison(self, document):
        return self._lookup(document,self._comparisons)

    def comparison_values(self, document, cusips):
        # Immutable Decimal totals avoid reconstructing every peer position for
        # every filing; division still uses the caller's Decimal context.
        key=_evidence_key(document)
        for saved, compact in self._value_comparisons:
            if saved == key:
                if compact is None:return None
                metadata,reasons,totals=compact
                return json.loads(metadata),json.loads(reasons),_equity_prices(totals,cusips)
        raise DirectSourceError('Prepared evidence identity changed')


def nport_identifiers(document, *, available_by: date, prepared=None):
    if prepared is not None:
        if not isinstance(prepared,Prepared13FEvidence):raise TypeError('Invalid prepared evidence')
        return prepared.identifiers(document,available_by)
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


def value_consistency_issues(parsed, documents, *, prepared=None):
    """Hold extreme unit discrepancies corroborated by two independent filers.

    Never rescale source numbers. Equal quarter/CUSIP equity holdings should
    imply comparable quarter-end prices; this is a diagnostic, not a quote feed.
    """
    if prepared is not None and not isinstance(prepared,Prepared13FEvidence):
        raise TypeError('Invalid prepared evidence')
    meta = parsed['metadata']
    peers = defaultdict(dict)
    target_prices = _equity_prices(_equity_totals(parsed['positions']))
    for document in documents:
        if hashlib.sha256(document['raw']).hexdigest() != document['content_hash']:
            raise DirectSourceError('13F comparison source checksum mismatch')
        if prepared is not None:
            saved=prepared.comparison_values(document,target_prices)
            if saved is None:continue
            pm,reasons,peer_prices=saved
        else:
            try:
                _, peer, reasons = parse_document('sec_13f', document['raw'], document['metadata'])
            except DirectSourceError:
                continue
            pm = peer['metadata']
            peer_prices = _equity_prices(_equity_totals(peer['positions']),target_prices)
        if (reasons or pm['cik'] == meta['cik'] or pm['report_period'] != meta['report_period']
                or pm['filing_date'] > meta['filing_date']):
            continue
        for cusip, price in peer_prices.items():
            peers[cusip].setdefault(pm['cik'], []).append((price, pm['key']))
    issues = []
    for cusip, price in target_prices.items():
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
