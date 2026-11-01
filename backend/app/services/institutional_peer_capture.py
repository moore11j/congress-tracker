"""Bounded canonical peers for an already verified institutional source capture.

No provider acquisition, canonical mutation, customer tables or publication.
The caller owns approval, the read-only transaction, serialization and budgets.
"""
from collections import Counter, defaultdict
from datetime import date, datetime, timezone
import hashlib
import re

from sqlalchemy import func, select
from app.models import (Event, InstitutionalFiling, InstitutionalPosition,
    InstitutionalPositionChange, InstitutionalActivityEvent, InstitutionalHolder,
    InstitutionalSymbolSummary)
from app.services.direct_feed_store import DirectFeedDocument
from app.services.institutional_capture_transport import encode, decode

MAX_SCOPE_BYTES = 3_000_000
MAX_STREAM_BYTES = 2_000_000_000
MAX_RECORD_BYTES = 5_000_000
LIMITS = {'filings': 5000, 'holders': 5000, 'positions': 1_000_000,
    'changes': 1_000_000, 'activity': 250_000, 'summaries': 150_000, 'events': 100_000}
MODELS = dict(filings=InstitutionalFiling, holders=InstitutionalHolder,
    positions=InstitutionalPosition, changes=InstitutionalPositionChange,
    activity=InstitutionalActivityEvent, summaries=InstitutionalSymbolSummary, events=Event)
POLICY = 'all_mapped_symbols_for_source_cusips_in_report_and_following_quarters_v1'


def digest(value):
    return hashlib.sha256(encode(value)).hexdigest()


def validate_scope(scope):
    if (not isinstance(scope, dict) or set(scope) != {'version', 'parent_capture_id',
            'parent_baseline_sha256', 'documents', 'period_cusips'} or
            type(scope['version']) is not int or scope['version'] != 1 or
            not re.fullmatch(r'[a-f0-9]{32}', str(scope['parent_capture_id'])) or
            not re.fullmatch(r'[a-f0-9]{64}', str(scope['parent_baseline_sha256'])) or
            not isinstance(scope['documents'], list) or not 1 <= len(scope['documents']) <= 8500 or
            not isinstance(scope['period_cusips'], dict) or not 1 <= len(scope['period_cusips']) <= 64 or
            len(encode(scope)) > MAX_SCOPE_BYTES):
        raise ValueError('invalid_peer_scope')
    identifiers = set()
    for document in scope['documents']:
        if (not isinstance(document, dict) or set(document) != {'id', 'content_hash', 'metadata_sha256'} or
                type(document['id']) is not int or document['id'] <= 0 or document['id'] in identifiers or
                any(not re.fullmatch(r'[a-f0-9]{64}', str(document[k])) for k in ('content_hash', 'metadata_sha256'))):
            raise ValueError('invalid_peer_source_inventory')
        identifiers.add(document['id'])
    total = 0
    for period, cusips in scope['period_cusips'].items():
        parsed = date.fromisoformat(period)
        if (parsed.isoformat() != period or (parsed.month, parsed.day) not in {(3,31),(6,30),(9,30),(12,31)} or
                not 1900 <= parsed.year <= 2100 or not isinstance(cusips, list) or
                not 1 <= len(cusips) <= 30000 or cusips != sorted(set(cusips)) or
                any(not isinstance(c, str) or not re.fullmatch(r'[A-Z0-9*@#.\-]{1,32}', c) for c in cusips)):
            raise ValueError('invalid_peer_period_cusips')
        total += len(cusips)
    if total > 100000:
        raise ValueError('peer_scope_pair_bound')
    return scope


def read_scope(path, expected_sha256):
    from pathlib import Path
    path = Path(path)
    if path.is_symlink() or not 1 <= path.stat().st_size <= MAX_SCOPE_BYTES:
        raise ValueError('invalid_peer_scope_file')
    raw = path.read_bytes()
    if hashlib.sha256(raw).hexdigest() != expected_sha256:
        raise ValueError('peer_scope_hash_changed')
    value = validate_scope(decode(raw))
    if raw != encode(value):
        raise ValueError('peer_scope_not_canonical')
    return value


def _inventory(db, scope):
    expected = {d['id']: d for d in scope['documents']}
    ids = sorted(expected)
    observed = set()
    for offset in range(0, len(ids), 200):
        query = select(DirectFeedDocument.id, DirectFeedDocument.feed, DirectFeedDocument.status,
            DirectFeedDocument.content_hash, DirectFeedDocument.metadata_json).where(
                DirectFeedDocument.id.in_(ids[offset:offset+200]))
        for row in db.execute(query).mappings():
            wanted = expected[row['id']]
            if (row['feed'] != 'sec_13f' or row['status'] != 'parsed' or
                    row['content_hash'] != wanted['content_hash'] or
                    digest(decode(row['metadata_json'])) != wanted['metadata_sha256']):
                raise ValueError('reviewed_peer_source_changed')
            observed.add(row['id'])
    if observed != set(expected):
        raise ValueError('reviewed_peer_source_missing')


def mapped_scopes(db, scope):
    """Include every existing symbol candidate, including ambiguous mappings.

    This is conservative canonical capture scope, not authority to resolve a
    source CUSIP or assign a ticker during publication.
    """
    wanted = sorted({c for values in scope['period_cusips'].values() for c in values})
    mappings = defaultdict(set)
    pairs = 0
    for offset in range(0, len(wanted), 100):
        query = select(InstitutionalPosition.cusip, InstitutionalPosition.normalized_symbol).where(
            InstitutionalPosition.cusip.in_(wanted[offset:offset+100]),
            InstitutionalPosition.normalized_symbol.is_not(None)).distinct().limit(50001)
        for cusip, symbol in db.execute(query):
            if not symbol or len(symbol) > 32:
                raise ValueError('invalid_peer_symbol')
            mappings[cusip].add(symbol)
            pairs += 1
            if pairs > 50000:
                raise ValueError('peer_mapping_bound')
    periods = defaultdict(set)
    for key, cusips in scope['period_cusips'].items():
        day = date.fromisoformat(key); year, quarter = day.year, day.month // 3
        symbols = {s for c in cusips for s in mappings[c]}
        periods[(year, quarter)].update(symbols)
        periods[(year + (quarter == 4), quarter % 4 + 1)].update(symbols)
    if sum(map(len, periods.values())) > 150000:
        raise ValueError('peer_symbol_period_bound')
    return {f'{y}Q{q}': sorted(symbols) for (y,q), symbols in sorted(periods.items())}


def bounded_pk_records(db, model, query, page_size=10000):
    """Bound sparse predicates by unfiltered primary-key ranges, including tails."""
    if type(page_size) is not int or not 1 <= page_size <= 10000:
        raise ValueError('invalid_peer_page_size')
    key = model.id
    after = None
    while True:
        frontier = select(key).order_by(key).limit(page_size)
        if after is not None:
            frontier = frontier.where(key > after)
        upper = db.scalar(select(func.max(frontier.subquery().c.id)))
        if upper is None:
            return
        bounded = query.where(key <= upper)
        if after is not None:
            bounded = bounded.where(key > after)
        yield from db.execute(bounded.order_by(key).execution_options(yield_per=100)).mappings()
        after = upper


class PeerStream:
    def __init__(self, output):
        self.output = output
        self.hash = hashlib.sha256()
        self.counts = Counter()
        self.bytes = 0
        self.done = False

    def write(self, table, row):
        if self.done or table not in (*MODELS, 'metadata') or not isinstance(row, dict):
            raise ValueError('invalid_peer_record')
        if (table == 'metadata' and self.counts) or (table != 'metadata' and self.counts['metadata'] != 1):
            raise ValueError('peer_metadata_order')
        maximum = 1 if table == 'metadata' else LIMITS[table]
        if self.counts[table] >= maximum:
            raise ValueError('peer_table_bound')
        raw = encode(dict(table=table, row=row)) + b'\n'
        if len(raw) > MAX_RECORD_BYTES or self.bytes + len(raw) > MAX_STREAM_BYTES:
            raise ValueError('peer_stream_byte_bound')
        self.output.write(raw)
        self.hash.update(raw); self.bytes += len(raw); self.counts[table] += 1

    def finish(self, **extra):
        if self.done or {'counts', 'bytes', 'sha256'} & set(extra):
            raise ValueError('invalid_peer_terminal')
        result = dict(extra, counts=dict(self.counts), bytes=self.bytes, sha256=self.hash.hexdigest())
        raw = encode({'complete': result}) + b'\n'
        if len(raw) > MAX_RECORD_BYTES:
            raise ValueError('peer_terminal_bound')
        self.output.write(raw); self.done = True
        return result


def export_peers(db, output, scope):
    """Export complete selected canonical peers; never silently truncate a table."""
    validate_scope(scope)
    _inventory(db, scope)
    periods = mapped_scopes(db, scope)
    stream = PeerStream(output)
    stream.write('metadata', dict(version=1, kind='institutional_canonical_peers',
        scope_sha256=digest(scope), parent_capture_id=scope['parent_capture_id'],
        parent_baseline_sha256=scope['parent_baseline_sha256'], policy=POLICY,
        symbol_periods=periods, captured_at=datetime.now(timezone.utc).isoformat(),
        source_documents=len(scope['documents']), transaction_read_only=True))
    # Full parent/holder tables are small and preserve supersession, inactive and
    # historical parent identities instead of inventing missing parents locally.
    for name in ('filings', 'holders'):
        table = MODELS[name].__table__
        query = select(table).order_by(*table.primary_key.columns).execution_options(yield_per=100)
        for row in db.execute(query).mappings():
            stream.write(name, dict(row))
    for name in ('positions', 'changes', 'activity', 'summaries'):
        model = MODELS[name]
        for period, symbols in periods.items():
            year, quarter = map(int, period.split('Q'))
            for offset in range(0, len(symbols), 100):
                query = select(model.__table__).where(model.report_year == year, model.report_quarter == quarter,
                    model.normalized_symbol.in_(symbols[offset:offset+100])).order_by(model.id)
                for row in db.execute(query.execution_options(yield_per=100)).mappings():
                    stream.write(name, dict(row))
    from app.services.institutional_activity import INSTITUTIONAL_EVENT_SOURCE
    query = select(Event.__table__).where(Event.source_provider == INSTITUTIONAL_EVENT_SOURCE)
    # Preserve all institutional event identities, even stale/orphaned ones. A
    # parent-only join would hide discrepancies that the reconciliation must see.
    for row in bounded_pk_records(db, Event, query):
        stream.write('events', dict(row))
    return stream


def receive_peers(source, destination, expected_scope):
    """Validate into a spool only. A receipt is not permission to apply records."""
    validate_scope(expected_scope)
    expected_hash = digest(expected_scope)
    counts = Counter(); hashes = defaultdict(hashlib.sha256); seen = defaultdict(set)
    total = 0; running = hashlib.sha256(); metadata = None; complete = None
    for line in iter(lambda: source.readline(MAX_RECORD_BYTES + 1), b''):
        if len(line) > MAX_RECORD_BYTES or not line.endswith(b'\n') or complete is not None:
            raise ValueError('invalid_peer_transport')
        item = decode(line)
        if set(item) == {'complete'}:
            complete = item['complete']
            if (not isinstance(complete, dict) or
                    set(complete) != {'counts', 'bytes', 'sha256', 'transaction_read_only', 'production_writes'} or
                    not isinstance(complete.get('counts'), dict) or
                    any(type(v) is not int for v in complete['counts'].values()) or
                    type(complete.get('bytes')) is not int or
                    type(complete.get('production_writes')) is not int or
                    complete.get('counts') != dict(counts) or complete.get('bytes') != total or
                    complete.get('sha256') != running.hexdigest() or
                    complete.get('transaction_read_only') is not True or complete.get('production_writes') != 0):
                raise ValueError('peer_terminal_mismatch')
            continue
        if set(item) != {'table', 'row'}:
            raise ValueError('invalid_peer_frame')
        name, row = item['table'], item['row']
        if name == 'metadata':
            if metadata is not None or counts or not isinstance(row, dict):
                raise ValueError('duplicate_peer_metadata')
            if (set(row) != {'version', 'kind', 'scope_sha256', 'parent_capture_id', 'parent_baseline_sha256',
                    'policy', 'symbol_periods', 'captured_at', 'source_documents', 'transaction_read_only'} or
                    type(row.get('version')) is not int or row.get('version') != 1 or
                    row.get('kind') != 'institutional_canonical_peers' or
                    row.get('scope_sha256') != expected_hash or row.get('policy') != POLICY or
                    row.get('parent_capture_id') != expected_scope['parent_capture_id'] or
                    row.get('parent_baseline_sha256') != expected_scope['parent_baseline_sha256'] or
                    type(row.get('source_documents')) is not int or row.get('source_documents') != len(expected_scope['documents']) or
                    row.get('transaction_read_only') is not True):
                raise ValueError('peer_scope_receipt_mismatch')
            permitted = set()
            for value in expected_scope['period_cusips']:
                day = date.fromisoformat(value); year, quarter = day.year, day.month // 3
                permitted.update((f'{year}Q{quarter}', f'{year + (quarter == 4)}Q{quarter % 4 + 1}'))
            periods = row['symbol_periods']
            if not isinstance(periods, dict) or set(periods) != permitted:
                raise ValueError('peer_period_scope_mismatch')
            pairs = 0
            for symbols in periods.values():
                if (not isinstance(symbols, list) or any(not isinstance(s, str) or not 1 <= len(s) <= 32 for s in symbols)
                        or symbols != sorted(set(symbols))):
                    raise ValueError('invalid_peer_symbol_scope')
                pairs += len(symbols)
            if pairs > 150000:
                raise ValueError('peer_symbol_period_bound')
            metadata = row
        else:
            if metadata is None or name not in MODELS or not isinstance(row, dict):
                raise ValueError('unexpected_peer_table')
            model = MODELS[name]; table = model.__table__
            if set(row) != {c.name for c in table.c}:
                raise ValueError('peer_schema_mismatch')
            keys = list(table.primary_key.columns); assert len(keys) == 1
            key = row[keys[0].name]
            if (name == 'holders' and not re.fullmatch(r'[0-9]{1,10}', str(key))) or (
                    name != 'holders' and (type(key) is not int or key <= 0)):
                raise ValueError('invalid_peer_primary_identity')
            if key is None or key in seen[name] or counts[name] >= LIMITS[name]:
                raise ValueError('duplicate_or_excess_peer_row')
            seen[name].add(key)
            if name in ('positions', 'changes', 'activity', 'summaries'):
                period = f"{row['report_year']}Q{row['report_quarter']}"
                if row['normalized_symbol'] not in metadata['symbol_periods'].get(period, []):
                    raise ValueError('peer_row_outside_scope')
            if name == 'events':
                from app.services.institutional_activity import INSTITUTIONAL_EVENT_SOURCE
                if row['source_provider'] != INSTITUTIONAL_EVENT_SOURCE:
                    raise ValueError('noninstitutional_event')
        counts[name] += 1; total += len(line)
        if total > MAX_STREAM_BYTES:
            raise ValueError('peer_stream_byte_bound')
        running.update(line); hashes[name].update(encode(row) + b'\n')
        destination.write(line)
    if metadata is None or complete is None:
        raise ValueError('incomplete_peer_capture')
    return dict(acceptance='canonical_peer_scope_integrity_only', counts=dict(counts),
        scope_sha256=expected_hash, stream_sha256=running.hexdigest(),
        row_hashes={k:v.hexdigest() for k,v in hashes.items()}, bytes=total,
        production_writes=0, emails=0, full_cutover_ready=False)
