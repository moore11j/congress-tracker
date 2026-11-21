"""Bounded original-only pieces of a frozen institutional inventory.

This transport does not contain canonical reference rows and cannot constitute
consumer/publication acceptance. Each request still requires explicit approval
and the existing capture worker's exclusive queue ownership and resource guards.
"""
from datetime import date, datetime, timezone
import hashlib
import json
import re
import time

from app.services.institutional_capture_transport import encode

MAX_DOCUMENTS = 20_000
MAX_SOURCE_BYTES = 4_000_000_000
PIECE_DOCUMENTS = 250
PIECE_BYTES = 64_000_000
MAX_PIECES = 128
ACCEPTANCE = 'originals_only_canonical_reference_and_consumer_validation_missing'
_SHA = re.compile(r'[a-f0-9]{64}')
_TOKEN = re.compile(r'[0-9]+:\([0-9]+,[0-9]+\)')

# These tokens bind document metadata as well as the revision. They are valid
# only within the short-lived request, not permanent logical source identities.
INVENTORY_SQL = """
SELECT d.id, d.content_hash, d.status, d.xmin::text || ':' || d.ctid::text,
       r.id, r.xmin::text || ':' || r.ctid::text,
       coalesce(octet_length(r.source_bytes), 0),
       coalesce(octet_length(d.metadata_json), 0) +
       coalesce(octet_length(d.reconciliation_json), 0) +
       coalesce(octet_length(d.error), 0),
       coalesce(octet_length(r.parsed_json), 0)
FROM direct_feed_documents d LEFT JOIN direct_feed_revisions r
ON r.document_id=d.id AND r.content_hash=d.content_hash
WHERE d.feed='sec_13f' {selection} ORDER BY d.id LIMIT :maximum
"""


def digest(value):
    return hashlib.sha256(encode(value)).hexdigest()


def validate_rows(rows, maximum=MAX_DOCUMENTS):
    if not isinstance(rows, list) or not 1 <= len(rows) <= maximum:
        raise ValueError('inventory_document_bound')
    previous = 0
    for row in rows:
        if not isinstance(row, list) or len(row) != 9:
            raise ValueError('invalid_inventory_row')
        ident, sha, status, token, revision, revision_token, raw, metadata, parsed = row
        if (type(ident) is not int or ident <= previous or
                not isinstance(status, str) or not 1 <= len(status) <= 64 or
                not isinstance(token, str) or not _TOKEN.fullmatch(token) or
                sha is not None and (not isinstance(sha, str) or not _SHA.fullmatch(sha)) or
                any(type(n) is not int or n < 0 for n in (raw, metadata, parsed))):
            raise ValueError('invalid_inventory_identity')
        if revision is None:
            if revision_token is not None or raw or parsed:
                raise ValueError('invalid_missing_revision')
        elif (type(revision) is not int or revision <= 0 or
              not isinstance(revision_token, str) or not _TOKEN.fullmatch(revision_token)):
            raise ValueError('invalid_revision_identity')
        if (raw > 30_000_000 or raw + metadata + parsed > 80_000_000 or
                raw and (sha is None or revision is None) or
                status == 'parsed' and not raw):
            raise ValueError('original_or_memory_bound')
        if estimated_bytes(row) > PIECE_BYTES:
            raise ValueError('single_original_transport_bound')
        previous = ident
    if sum(row[6] for row in rows) > MAX_SOURCE_BYTES:
        raise ValueError('inventory_source_byte_bound')


def estimated_bytes(row):
    # JSON escaping expands a one-byte control character to at most six bytes.
    # Reserve framing/identity overhead separately. Cached parses are read to
    # propose scope, but never included in the wire payload.
    # Also reserve room for the scope derived from cached parses. An exact final
    # frame check still fails closed if a pathological parse exceeds this estimate.
    return row[6] + row[6] // 4 + 6 * row[7] + 2048


def snapshot_rows(db, identifiers=None):
    from sqlalchemy import text
    if db.scalar(text('SHOW transaction_read_only')) != 'on':
        raise ValueError('readonly_transaction_required')
    params = dict(maximum=MAX_DOCUMENTS + 1)
    selection = ''
    if identifiers is not None:
        if (not isinstance(identifiers, list) or not 1 <= len(identifiers) <= PIECE_DOCUMENTS or
                any(type(n) is not int or n <= 0 for n in identifiers) or
                identifiers != sorted(set(identifiers))):
            raise ValueError('invalid_piece_identifiers')
        selection = 'AND d.id = ANY(:identifiers)'
        params.update(identifiers=identifiers, maximum=PIECE_DOCUMENTS + 1)
    rows = [list(row) for row in db.execute(text(INVENTORY_SQL.format(selection=selection)), params)]
    validate_rows(rows, PIECE_DOCUMENTS if identifiers is not None else MAX_DOCUMENTS)
    return rows


def make_plan(rows, *, snapshot_at=None):
    validate_rows(rows)
    snapshot_at = int(time.time()) if snapshot_at is None else snapshot_at
    if type(snapshot_at) is not int or snapshot_at < 0:
        raise ValueError('invalid_snapshot_time')
    groups, current, size = [], [], 0
    for row in rows:
        amount = estimated_bytes(row)
        if current and (len(current) >= PIECE_DOCUMENTS or size + amount > PIECE_BYTES):
            groups.append(current)
            current, size = [], 0
        current.append(row)
        size += amount
    groups.append(current)
    if len(groups) > MAX_PIECES:
        raise ValueError('piece_count_bound')
    inventory_sha256 = digest(rows)
    return dict(version=1, inventory_sha256=inventory_sha256, snapshot_at=snapshot_at,
                document_count=len(rows), source_bytes=sum(row[6] for row in rows),
                acceptance=ACCEPTANCE, pieces=[dict(index=i, total=len(groups),
                    inventory_sha256=inventory_sha256, snapshot_at=snapshot_at,
                    rows=group, rows_sha256=digest(group))
                    for i, group in enumerate(groups)])


def validate_piece(piece):
    if not isinstance(piece, dict) or set(piece) != {'index', 'total', 'inventory_sha256', 'snapshot_at', 'rows', 'rows_sha256'}:
        raise ValueError('invalid_piece')
    if (type(piece['index']) is not int or type(piece['total']) is not int or
            not 0 <= piece['index'] < piece['total'] <= MAX_PIECES or
            not isinstance(piece['inventory_sha256'], str) or not _SHA.fullmatch(piece['inventory_sha256']) or
            type(piece['snapshot_at']) is not int or piece['snapshot_at'] < 0):
        raise ValueError('invalid_piece_identity')
    validate_rows(piece['rows'], PIECE_DOCUMENTS)
    if (piece['rows_sha256'] != digest(piece['rows']) or
            sum(estimated_bytes(row) for row in piece['rows']) > PIECE_BYTES or
            len(encode(piece)) > 60_000):
        raise ValueError('piece_integrity_or_byte_bound')
    return piece


def validate_plan(plan):
    if not isinstance(plan, dict) or not isinstance(plan.get('pieces'), list):
        raise ValueError('invalid_plan')
    rows = []
    for piece in plan['pieces']:
        validate_piece(piece)
        rows.extend(piece['rows'])
        if len(rows) > MAX_DOCUMENTS:
            raise ValueError('inventory_document_bound')
    if make_plan(rows, snapshot_at=plan.get('snapshot_at')) != plan:
        raise ValueError('incomplete_or_changed_plan')
    return plan


def export_piece(db, stream, piece):
    """One caller-owned read-only repeatable-read transaction, never all originals."""
    validate_piece(piece)
    from sqlalchemy import text
    identifiers = [row[0] for row in piece['rows']]
    inventory = snapshot_rows(db)
    selected = set(identifiers)
    if (digest(inventory) != piece['inventory_sha256'] or
            [row for row in inventory if row[0] in selected] != piece['rows']):
        raise ValueError('source_inventory_changed')
    captured_at = datetime.now(timezone.utc)
    stream.write('metadata', dict(captured_at=captured_at, transport_version=3,
        transaction_read_only=True, filing_date='2026-10-07', document_count=len(identifiers),
        original_piece=piece, acceptance=ACCEPTANCE))
    query = text("""SELECT d.id,d.metadata_json,d.content_hash,d.status,d.error,
        d.reconciliation_json,r.source_bytes,r.parsed_json
        FROM direct_feed_documents d LEFT JOIN direct_feed_revisions r
        ON r.document_id=d.id AND r.content_hash=d.content_hash
        WHERE d.feed='sec_13f' AND d.id=ANY(:identifiers) ORDER BY d.id""")
    ciks, cusips, periods, count, total = set(), set(), set(), 0, 0
    for row in db.execute(query.execution_options(yield_per=1), dict(identifiers=identifiers)).mappings():
        expected = piece['rows'][count]
        raw = row['source_bytes']
        if row['id'] != expected[0] or (len(raw) if raw else 0) != expected[6]:
            raise ValueError('source_inventory_changed')
        metadata = json.loads(row['metadata_json'])
        parsed = None
        if raw:
            if hashlib.sha256(raw).hexdigest() != row['content_hash']:
                raise ValueError('original_hash_mismatch')
            parsed = json.loads(row['parsed_json'] or '{}')
            cusips.update(item['cusip'] for item in parsed.get('positions', []))
            total += len(raw)
        if metadata['filing_date'] >= '2026-10-07':
            ciks.add(metadata['cik'].lstrip('0'))
            if raw and row['status'] == 'parsed':
                period = date.fromisoformat(parsed['metadata']['report_period'])
                periods.add((period.year, (period.month - 1) // 3 + 1))
        stream.write_document(dict(id=row['id'], metadata=metadata, content_hash=row['content_hash'],
            status=row['status'], error=row['error'],
            prior_collection=json.loads(row['reconciliation_json'] or '{}').get('_prior_collection')), raw or None)
        if stream.bytes > PIECE_BYTES:
            raise ValueError('actual_piece_byte_bound')
        count += 1
    if count != len(identifiers) or total != sum(row[6] for row in piece['rows']):
        raise ValueError('incomplete_original_piece')
    return dict(captured_at=captured_at, source_bytes=total, transaction_read_only=True,
                production_writes=0, original_piece_sha256=digest(piece), acceptance=ACCEPTANCE,
                query_scope=dict(ciks=sorted(ciks), cusips=sorted(cusips), periods=sorted(periods)))


def finish_piece(stream, receipt):
    """Check the whole uncompressed piece, including its final scope/receipt."""
    terminal = dict(complete={**receipt, 'counts': dict(stream.counts),
        'bytes': stream.bytes, 'sha256': stream.digest.hexdigest()})
    if stream.bytes + 2 * sum(stream.counts.values()) + len(encode(terminal)) + 1 > PIECE_BYTES:
        raise ValueError('complete_piece_byte_bound')
    return stream.finish(**receipt)
