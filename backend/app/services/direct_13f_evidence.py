"""Pure SEC identifier and cross-filing value checks for offline publication."""
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
import hashlib
import json
import re
import tempfile
import sys
from threading import Lock
from statistics import median
from xml.etree import ElementTree as ET

from app.clients.direct_sources import DirectSourceError
from app.services.direct_feed_collection import parse_document


def _verify_original(document, verified_original=None):
    raw = document['raw']
    # Only the exact immutable bytes object verified during preparation can
    # reuse its checksum. Replacements and mutable buffers are always hashed.
    if type(raw) is bytes and raw is verified_original:
        return
    if hashlib.sha256(raw).hexdigest() != document['content_hash']:
        raise DirectSourceError('Prepared evidence checksum mismatch')


def _identity_key(document):
    return json.dumps({key:document.get(key) for key in ('content_hash','url','metadata','feed')},
                      sort_keys=True,default=str)


def _evidence_key(document):
    _verify_original(document)
    return _identity_key(document)


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


MAX_COMPARISON_CACHE_DOCUMENTS = 10_000
MAX_COMPARISON_CACHE_BYTES = 100_000_000


class PreparedComparisonCache:
    """Bounded batch-local original parses shared by grouping and preparation.

    Keys bind transport hashes and full source identity. Records contain only
    immutable strings, tuples and Decimals, never mutable parser dictionaries.
    No process-wide or database cache survives the publication batch.
    """
    def __init__(self):
        self._records = {}
        self._bytes = 0

    def record(self, document):
        key = _evidence_key(document)
        if key in self._records:
            return key, *self._records[key]
        if len(self._records) >= MAX_COMPARISON_CACHE_DOCUMENTS or self._bytes + len(document['raw']) > MAX_COMPARISON_CACHE_BYTES:
            raise ValueError('Comparison parse cache exceeds complete scan bounds')
        try:
            _, parsed, reasons = parse_document('sec_13f', document['raw'], document['metadata'])
            value = json.dumps([parsed, reasons], default=str)
            compact = (json.dumps(parsed['metadata'], default=str), json.dumps(reasons),
                       _equity_totals(parsed['positions']))
        except DirectSourceError:
            value = compact = None
        self._records[key] = (value, compact)
        self._bytes += len(document['raw'])
        return key, value, compact

    def period(self, document):
        _, _, compact = self.record(document)
        return json.loads(compact[0])['report_period'] if compact is not None else None


MAX_STREAM_SUMMARY_BYTES = 20_000_000


@dataclass(frozen=True)
class VerifiedPeerRecord:
    identity: str
    metadata_json: str
    content_hash: str
    source_bytes: int
    compact: tuple | None


MAX_STREAM_SPOOL_BYTES = 128_000_000
MAX_STREAM_RESIDENT_BYTES = 20_000_000


class _PeerSummarySpool:
    """Private batch-local storage; never reuse a persisted derived cache.

    TemporaryFile is unlinked immediately on POSIX and delete-on-close on
    Windows. A killed worker leaves no named cache to trust on another run.
    Only freshly verified original-derived immutable values enter this file.
    """
    def __init__(self):
        self.file = tempfile.TemporaryFile(mode='w+b')
        self.size = 0
        self.sealed = False
        self.lock = Lock()

    def append(self, compact):
        if self.sealed:
            raise ValueError('Verified peer spool is sealed')
        if compact is None:
            return None
        raw = json.dumps(compact, default=str, separators=(',', ':')).encode()
        if len(raw) > MAX_STREAM_SUMMARY_BYTES:
            raise ValueError('Single peer summary exceeds memory budget')
        if self.size + len(raw) > MAX_STREAM_SPOOL_BYTES:
            raise ValueError('Complete peer summaries exceed temporary storage budget')
        offset = self.size
        self.file.seek(offset)
        if self.file.write(raw) != len(raw):
            raise OSError('Incomplete peer summary write')
        self.size += len(raw)
        return offset, len(raw), hashlib.sha256(raw).hexdigest()

    def read(self, location):
        if location is None:
            return None
        if not self.sealed:
            raise ValueError('Verified peer spool is not sealed')
        offset, length, digest = location
        if offset < 0 or length < 0 or length > MAX_STREAM_SUMMARY_BYTES or offset + length > self.size:
            raise DirectSourceError('Invalid peer summary location')
        with self.lock:
            self.file.seek(offset)
            raw = self.file.read(length)
        if len(raw) != length or hashlib.sha256(raw).hexdigest() != digest:
            raise DirectSourceError('Verified peer summary checksum mismatch')
        metadata, reasons, totals = json.loads(raw)
        return metadata, reasons, tuple((cusip, Decimal(shares), Decimal(value))
                                       for cusip, shares, value in totals)

    def seal(self):
        self.file.flush()
        self.sealed = True

    def close(self):
        self.file.close()


@dataclass(frozen=True)
class _SpooledPeerRecord:
    identity: str
    metadata_json: str
    content_hash: str
    source_bytes: int
    location: tuple | None
    spool: _PeerSummarySpool

    @property
    def compact(self):
        return self.spool.read(self.location)


def _peer_record_size(record):
    payload = record.location if isinstance(record, _SpooledPeerRecord) else record.compact
    return len(json.dumps((record.identity, record.metadata_json, record.content_hash,
                           record.source_bytes, payload), default=str).encode())


def _immutable_resident_size(value):
    """Conservative owned-value estimate; repeated shared values count again.

    JSON length understates Decimal and tuple storage substantially. Count the
    actual runtime object sizes before retaining them. This excludes parser
    working memory and does not replace the host's available-memory guard.
    """
    size = sys.getsizeof(value)
    if isinstance(value, (tuple, frozenset)):
        size += sum(_immutable_resident_size(item) for item in value)
    return size


def _peer_record_resident_size(record):
    payload = record.location if isinstance(record, _SpooledPeerRecord) else record.compact
    return (sys.getsizeof(record) + sys.getsizeof(record.__dict__) +
            sum(_immutable_resident_size(value) for value in
                (record.identity, record.metadata_json, record.content_hash,
                 record.source_bytes, payload)) + 8)  # retained sequence pointer


@dataclass(frozen=True)
class VerifiedPeerCohort:
    """Immutable values derived from every verified original in one cohort.

    Transport bytes are released after verification and parsing. Only target
    CUSIPs are retained, and a consumer outside that declared scope fails.
    Metadata and totals are immutable values, never a saved unverified parse.
    """
    records: tuple
    period: str | None
    cusips: frozenset | None
    summary_bytes: int
    cusip_names: tuple | None = None
    spool: _PeerSummarySpool | None = None
    resident_bytes_estimate: int = 0

    def close(self):
        if self.spool is not None:
            self.spool.close()

    def __len__(self):
        return len(self.records)

    def __iter__(self):
        for record in self.records:
            yield {'feed': 'sec_13f', 'metadata': json.loads(record.metadata_json),
                   'content_hash': record.content_hash}

    @property
    def source_bytes(self):
        return sum(record.source_bytes for record in self.records)

    def values_for(self, parsed):
        wanted = {row['cusip'] for row in parsed['positions']}
        if ((self.period is not None and parsed['metadata']['report_period'] != self.period)
                or (self.cusips is not None and not wanted <= self.cusips)):
            raise DirectSourceError('Target falls outside verified peer evidence scope')
        # One immutable name dictionary replaces repeated CUSIP strings in
        # peer rows. Decimal totals remain unchanged; divide in caller context.
        wanted_ids = ({index for index, name in enumerate(self.cusip_names) if name in wanted}
                      if self.cusip_names is not None else None)
        for record in self.records:
            compact = record.compact
            if compact is None:
                continue
            metadata, reasons, totals = compact
            prices = (_equity_prices(totals, wanted) if wanted_ids is None else
                {self.cusip_names[index]: value / shares for index, shares, value in totals
                 if index in wanted_ids and shares > 0 and value > 0})
            yield json.loads(metadata), json.loads(reasons), prices


def prepare_streamed_peer_groups(documents, *, scopes, spill_to_disk=False):
    """Scan complete originals, retaining bounded summaries instead of raw files.

    scopes maps report periods to target CUSIPs; None retains complete values
    for callers whose target cannot be parsed. Invalid originals remain in
    every cohort, matching the existing fail-closed selection semantics.
    """
    from app.services.direct_feed_worker import _source_url
    groups = {period: [] for period in sorted(scopes)} if scopes is not None else {None: []}
    names = ({period: tuple(sorted(scopes[period])) for period in groups}
             if scopes is not None else {None: None})
    indexes = {period: {name: index for index, name in enumerate(pool)}
               for period, pool in names.items() if pool is not None}
    sizes = {period: len(json.dumps(pool).encode()) if pool is not None else 0
             for period, pool in names.items()}
    total_size = sum(sizes.values())
    heap_sizes = {period: (_immutable_resident_size(names[period]) +
                          _immutable_resident_size(frozenset(scopes[period]) if scopes is not None else None) +
                          512) if spill_to_disk else 0 for period in groups}
    total_heap = sum(heap_sizes.values())
    if total_size > MAX_STREAM_SUMMARY_BYTES or (spill_to_disk and total_heap > MAX_STREAM_RESIDENT_BYTES):
        raise ValueError('Complete streamed peer summaries exceed memory budget')
    spool = None
    def spill(record):
        return _SpooledPeerRecord(record.identity, record.metadata_json, record.content_hash,
                                  record.source_bytes, spool.append(record.compact), spool)
    try:
        for document in documents:
            if document.get('feed') != 'sec_13f':
                raise ValueError('Wrong comparison evidence feed')
            _source_url(document['metadata'])
            identity = _evidence_key(document)
            compact = None
            periods = groups
            try:
                _, parsed, reasons = parse_document('sec_13f', document['raw'], document['metadata'])
            except DirectSourceError:
                pass
            else:
                period = parsed['metadata']['report_period']
                periods = [period] if period in groups else []
                if scopes is None:
                    periods = [None]
                if periods:
                    wanted = scopes[period] if scopes is not None else None
                    totals = tuple(row for row in _equity_totals(parsed['positions'])
                                   if wanted is None or row[0] in wanted)
                    if wanted is not None:
                        totals = tuple((indexes[period][cusip], shares, value)
                                       for cusip, shares, value in totals)
                    compact = (json.dumps(parsed['metadata'], default=str), json.dumps(reasons), totals)
                del parsed
            if not periods:
                continue
            record = VerifiedPeerRecord(identity, json.dumps(document['metadata'], sort_keys=True),
                                        document['content_hash'], len(document['raw']), compact)
            size = _peer_record_size(record)
            heap_size = _peer_record_resident_size(record) if spill_to_disk else 0
            if spool is None and spill_to_disk and (
                    total_size + size * len(periods) > MAX_STREAM_SUMMARY_BYTES or
                    total_heap + heap_size * len(periods) > MAX_STREAM_RESIDENT_BYTES):
                spool = _PeerSummarySpool()
                for saved_period, records in groups.items():
                    groups[saved_period] = [spill(saved) for saved in records]
                    sizes[saved_period] = (len(json.dumps(names[saved_period]).encode())
                                           if names[saved_period] is not None else 0)
                    sizes[saved_period] += sum(_peer_record_size(saved) for saved in groups[saved_period])
                    heap_sizes[saved_period] = (_immutable_resident_size(names[saved_period]) +
                        _immutable_resident_size(frozenset(scopes[saved_period]) if scopes is not None else None) + 512 +
                        sum(_peer_record_resident_size(saved) for saved in groups[saved_period]))
                # The loop variable otherwise retains the last old totals list
                # until the complete scan returns, defeating the early spill.
                del records
                heap_sizes[next(iter(groups))] += 16_384  # file/buffer/lock allowance
                total_size = sum(sizes.values())
                total_heap = sum(heap_sizes.values())
            if spool is not None:
                record = spill(record)
                size = _peer_record_size(record)
                heap_size = _peer_record_resident_size(record)
            for period in periods:
                total_size += size
                total_heap += heap_size
                if total_size > MAX_STREAM_SUMMARY_BYTES or (spill_to_disk and total_heap > MAX_STREAM_RESIDENT_BYTES):
                    raise ValueError('Complete streamed peer summaries exceed memory budget')
                groups[period].append(record)
                sizes[period] += size
                heap_sizes[period] += heap_size
        if any(not records for records in groups.values()):
            raise ValueError('Missing staged comparison period evidence')
        if spool is not None:
            spool.seal()
        return {period: VerifiedPeerCohort(tuple(records), period,
                    frozenset(scopes[period]) if scopes is not None else None, sizes[period], names[period], spool, heap_sizes[period])
                for period, records in groups.items()}

    except BaseException:
        if spool is not None:
            spool.close()
        raise


@dataclass(frozen=True)
class Prepared13FEvidence:
    """Per-batch immutable parses; every reuse still checks original bytes/identity.

    JSON strings keep returned dictionaries from mutating cached evidence.
    Retained immutable originals prove unchanged bytes by object identity; any
    replacement or mutable buffer still requires a checksum. Full source
    identity is checked on every lookup. No process-wide cache retains files
    after the bounded batch ends.
    """
    _identifiers: tuple
    _comparisons: tuple
    _value_comparisons: tuple
    _streamed_cohort: VerifiedPeerCohort | None = None

    @classmethod
    def build_for_cohort(cls, identifier_documents, cohort):
        if not isinstance(cohort, VerifiedPeerCohort) or not len(cohort):
            raise ValueError('Missing verified peer cohort')
        if len(identifier_documents) > 50 or sum(len(d['raw']) for d in identifier_documents) > 100_000_000:
            raise ValueError('Identifier evidence exceeds bounds')
        identifiers = []
        for document in identifier_documents:
            key = _evidence_key(document)
            identifiers.append((key, json.dumps(nport_identifiers(document, available_by=date.max)),
                                document['raw'] if type(document['raw']) is bytes else None))
        return cls(tuple(identifiers), (), (), cohort)

    @classmethod
    def build(cls, identifier_documents, comparison_documents, *, comparison_cache=None):
        if len(identifier_documents)>50 or not 1<=len(comparison_documents)<=500:
            raise ValueError('Prepared evidence exceeds document bounds')
        if sum(len(d['raw']) for d in (*identifier_documents,*comparison_documents))>100_000_000:
            raise ValueError('Prepared evidence exceeds byte bounds')
        identifiers=[];comparisons=[];values=[]
        for document in identifier_documents:
            key=_evidence_key(document)
            identifiers.append((key,json.dumps(nport_identifiers(document,available_by=date.max)),
                                document['raw'] if type(document['raw']) is bytes else None))
        cache = comparison_cache if comparison_cache is not None else PreparedComparisonCache()
        if not isinstance(cache, PreparedComparisonCache):
            raise TypeError('Invalid comparison parse cache')
        for document in comparison_documents:
            key, value, compact = cache.record(document)
            original = document['raw'] if type(document['raw']) is bytes else None
            values.append((key, compact, original))
            comparisons.append((key, value, original))
        return cls(tuple(identifiers),tuple(comparisons),tuple(values))

    def _lookup(self, document, records):
        key=_identity_key(document)
        for saved,value,original in records:
            if saved==key:
                _verify_original(document, original)
                return json.loads(value) if value is not None else None
        raise DirectSourceError('Prepared evidence identity changed')

    def identifiers(self, document, available_by):
        rows=self._lookup(document,self._identifiers)
        return [row for row in rows if row['filing_date']<=available_by.isoformat()]

    def comparison(self, document):
        return self._lookup(document,self._comparisons)

    def comparison_values(self, document, cusips):
        # Immutable Decimal totals avoid reconstructing every peer position for
        # every filing; division still uses the caller's Decimal context.
        key=_identity_key(document)
        for saved, compact, original in self._value_comparisons:
            if saved == key:
                _verify_original(document, original)
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
    if isinstance(documents, VerifiedPeerCohort):
        if prepared is None or prepared._streamed_cohort is not documents:
            raise DirectSourceError('Verified cohort and prepared evidence do not match')
        values = documents.values_for(parsed)
    else:
        def original_values():
            for document in documents:
                if prepared is not None:
                    saved = prepared.comparison_values(document, target_prices)
                    if saved is not None:
                        yield saved
                else:
                    if hashlib.sha256(document['raw']).hexdigest() != document['content_hash']:
                        raise DirectSourceError('13F comparison source checksum mismatch')
                    try:
                        _, peer, reasons = parse_document('sec_13f', document['raw'], document['metadata'])
                    except DirectSourceError:
                        continue
                    yield peer['metadata'], reasons, _equity_prices(_equity_totals(peer['positions']), target_prices)
        values = original_values()
    for pm, reasons, peer_prices in values:
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
