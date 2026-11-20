"""Resumable original-verified 13F parses, isolated from canonical publication.

PostgreSQL row versions detect even edits that fail to update a saved SHA.
Logical IDs and source SHA remain the identities; xmin/ctid are invalidation
tokens only. An extended transaction-age bound prevents xmin wraparound reuse.
SQLite rehearsals rehash original bytes instead of relying on row versions.
"""
from datetime import datetime, timezone
from decimal import Decimal
from functools import lru_cache
import hashlib
import json
from pathlib import Path

from sqlalchemy import BigInteger, DateTime, Text, func, literal, literal_column, select, text
from sqlalchemy.orm import Mapped, mapped_column, undefer

from app.db import Base
from app.services.direct_feed_store import DirectFeedDocument as Document, DirectFeedRevision as Revision
from app.services.direct_13f_evidence import VerifiedPeerRecord, verified_peer_records

PAGE_SIZE = 250
CHUNK_DOCUMENTS = 25
CHUNK_BYTES = 8_000_000
MAX_SOURCE_BYTES = 30_000_000
MAX_PARSE_BYTES = 30_000_000
MAX_XID_AGE = 1_000_000_000


class Prepared13FParse(Base):
    __tablename__ = 'prepared_13f_parses'
    document_id: Mapped[int] = mapped_column(primary_key=True)
    revision_id: Mapped[int] = mapped_column(BigInteger)
    source_identity: Mapped[str] = mapped_column(Text)
    source_token: Mapped[str] = mapped_column(Text)
    parser_version: Mapped[str]
    payload_json: Mapped[str] = mapped_column(Text, deferred=True)
    payload_sha256: Mapped[str]
    verified_xid: Mapped[int] = mapped_column(BigInteger)
    verified_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class Prepared13FProgress(Base):
    __tablename__ = 'prepared_13f_progress'
    key: Mapped[str] = mapped_column(primary_key=True)
    cursor: Mapped[int] = mapped_column(BigInteger, default=0)
    parser_version: Mapped[str]


def ensure_prepared_13f_schema(bind):
    with bind.begin() as connection:
        if connection.dialect.name == 'postgresql':
            connection.execute(text("SET LOCAL lock_timeout='500ms'"))
            connection.execute(text("SET LOCAL statement_timeout='5s'"))
        for model in (Prepared13FParse, Prepared13FProgress):
            model.__table__.create(connection, checkfirst=True)


@lru_cache(maxsize=1)
def parser_version():
    # Include parser dependencies and the serialization/selection code itself.
    app = Path(__file__).resolve().parents[1]
    digest = hashlib.sha256(b'prepared-13f-v1\0')
    for name in ('clients/direct_sources.py', 'clients/sec_edgar.py', 'services/direct_feed_collection.py',
                 'services/direct_feed_worker.py', 'services/direct_13f_evidence.py',
                 'services/direct_13f_preparation.py'):
        digest.update(name.encode() + b'\0' + (app / name).read_bytes().replace(b'\r\n', b'\n'))
    return digest.hexdigest()


def _xid(db):
    return int(db.scalar(text('SELECT txid_current()'))) if db.bind.dialect.name == 'postgresql' else 0


def _inventory(db, *, after=0, checkpoint=lambda: None, with_payloads=False, descending=False):
    postgres = db.bind.dialect.name == 'postgresql'
    token = (literal_column("direct_feed_revisions.xmin::text || ':' || direct_feed_revisions.ctid::text || ':' || direct_feed_revisions.tableoid::text")
             if postgres else literal('sqlite'))
    raw = literal(None) if postgres else Revision.source_bytes
    while True:
        checkpoint()
        rows = db.execute(select(Document.id, Document.metadata_json, Document.source_key,
            Document.source_url, Document.content_hash, Revision.id.label('revision_id'),
            func.length(Revision.source_bytes).label('size'), token.label('token'),
            raw.label('raw'), func.length(Prepared13FParse.payload_json).label('payload_size'), Prepared13FParse).outerjoin(Revision,
                (Revision.document_id == Document.id) & (Revision.content_hash == Document.content_hash))
            .outerjoin(Prepared13FParse, Prepared13FParse.document_id == Document.id)
            .where(Document.feed == 'sec_13f', Document.status == 'parsed',
                   (Document.id < after if after else literal(True)) if descending else Document.id > after)
            .order_by(Document.id.desc() if descending else Document.id).limit(PAGE_SIZE)
            .execution_options(populate_existing=True)).all()
        if not rows:
            return
        loaded_until = 0
        for index, row in enumerate(rows):
            if with_payloads and index >= loaded_until:
                ids, total = [], 0
                for candidate in rows[index:]:
                    if candidate.payload_size is not None and not 0 < candidate.payload_size <= MAX_PARSE_BYTES:
                        raise ValueError('Prepared parse exceeds byte budget')
                    if ids and (len(ids) >= CHUNK_DOCUMENTS or total + (candidate.payload_size or 0) > CHUNK_BYTES):
                        break
                    loaded_until += 1
                    if candidate.Prepared13FParse is not None:
                        ids.append(candidate.id)
                        total += candidate.payload_size
                if ids:
                    list(db.scalars(select(Prepared13FParse).where(Prepared13FParse.document_id.in_(ids))
                                    .options(undefer(Prepared13FParse.payload_json))))
            if row.revision_id is None or row.size is None or not 0 < row.size <= MAX_SOURCE_BYTES:
                raise ValueError('Missing or oversized staged original source')
            metadata = json.loads(row.metadata_json)
            from app.services.direct_feed_worker import _source_url
            _source_url(metadata)
            if metadata['key'] != row.source_key or metadata['url'] != row.source_url:
                raise ValueError('Staged comparison identity changed')
            document = dict(feed='sec_13f', metadata=metadata, content_hash=row.content_hash)
            from app.services.direct_13f_evidence import _identity_key
            identity = _identity_key(document)
            version = row.token if postgres else hashlib.sha256(row.raw).hexdigest()
            yield dict(id=row.id, revision_id=row.revision_id, size=row.size,
                       document=document, identity=identity, token=version,
                       cached=row.Prepared13FParse)
            if row.Prepared13FParse is not None and row.Prepared13FParse not in db.dirty:
                db.expire(row.Prepared13FParse, ['payload_json'])
            after = row.id


def _current(row, version, xid):
    cached = row['cached']
    return (cached is not None and cached.parser_version == version
            and cached.revision_id == row['revision_id'] and cached.source_identity == row['identity']
            and cached.source_token == row['token'] and 0 <= xid - cached.verified_xid < MAX_XID_AGE)


def _payload_hash(identity, version, payload):
    return hashlib.sha256((identity + '\0' + version + '\0' + payload).encode()).hexdigest()


def _checked_payload(cached):
    payload = cached.payload_json
    if len(payload.encode()) > MAX_PARSE_BYTES or _payload_hash(cached.source_identity, cached.parser_version, payload) != cached.payload_sha256:
        raise ValueError('Prepared parse checksum changed')
    return payload


def _decode(cached, row):
    payload = _checked_payload(cached)
    compact = json.loads(payload)
    if compact is not None:
        metadata, reasons, totals = compact
        compact = (metadata, reasons, tuple((cusip, Decimal(shares), Decimal(value)) for cusip, shares, value in totals))
        if any(not shares.is_finite() or not value.is_finite() for _, shares, value in compact[2]):
            raise ValueError('Prepared parse contains nonfinite totals')
    return VerifiedPeerRecord(row['identity'], json.dumps(row['document']['metadata'], sort_keys=True),
                              row['document']['content_hash'], row['size'], compact)


def advance_preparation(db, *, max_documents=CHUNK_DOCUMENTS, max_bytes=CHUNK_BYTES,
                        checkpoint=lambda: None):
    """One bounded chunk; caller commits derived rows and cursor together.

    No canonical rows, source selections or event/alert records are written.
    Pending earlier rows are revisited after the cursor wraps. Complete readers
    independently check every live original, so cursor position grants no trust.
    """
    if not 1 <= max_documents <= CHUNK_DOCUMENTS or not 1 <= max_bytes <= CHUNK_BYTES:
        raise ValueError('Invalid preparation chunk limits')
    if db.new or db.dirty or db.deleted:
        raise ValueError('Preparation session has unrelated pending changes')
    if db.bind.dialect.name == 'postgresql' and not db.scalar(text('SELECT pg_try_advisory_xact_lock(13700413)')):
        return dict(status='busy', processed=0)
    version, xid = parser_version(), _xid(db)
    state = db.get(Prepared13FProgress, 'peers', with_for_update=True)
    if state is None:
        state = Prepared13FProgress(key='peers', cursor=0, parser_version=version)
        db.add(state)
        db.flush()
    elif state.parser_version != version:
        state.cursor, state.parser_version = 0, version
    count = size = parsed = reused = 0
    cursor = state.cursor
    for row in _inventory(db, after=cursor, checkpoint=checkpoint):
        checkpoint()
        if _current(row, version, xid):
            cursor = row['id']
            continue
        if count and (count >= max_documents or size + row['size'] > max_bytes):
            break
        raw = db.scalar(select(Revision.source_bytes).where(Revision.id == row['revision_id']))
        if raw is None or len(raw) != row['size'] or hashlib.sha256(raw).hexdigest() != row['document']['content_hash']:
            raise ValueError('Staged comparison checksum changed')
        cached = row['cached']
        if cached is not None and cached.source_identity == row['identity'] and cached.parser_version == version:
            record = _decode(cached, row)
            reused += 1
        else:
            record = next(verified_peer_records([{**row['document'], 'raw': raw}]))
            parsed += 1
        payload = json.dumps(record.compact, default=str, separators=(',', ':'))
        # Compact strings can retain arenas from the full parsed filing. Only
        # the serialized value is needed beyond this point or at a checkpoint.
        del record
        if len(payload.encode()) > MAX_PARSE_BYTES:
            raise ValueError('Prepared parse exceeds byte budget')
        if cached is None:
            cached = Prepared13FParse(document_id=row['id'])
            db.add(cached)
        cached.revision_id, cached.source_identity = row['revision_id'], row['identity']
        cached.source_token, cached.parser_version = row['token'], version
        cached.payload_json = payload
        cached.payload_sha256 = _payload_hash(row['identity'], version, payload)
        cached.verified_xid, cached.verified_at = xid, datetime.now(timezone.utc)
        count += 1
        size += len(raw)
        del raw
        cursor = row['id']
        db.flush()
    else:
        cursor = 0
    state.cursor = cursor
    db.flush()
    return dict(status='sweep_complete' if cursor == 0 else 'preparing', processed=count,
                parsed=parsed, reused=reused, source_bytes=size, cursor=cursor,
                publication_writes=0)


def prepared_records(db, *, checkpoint=lambda: None):
    """Yield only complete current-source parses; any pending member aborts."""
    version, xid = parser_version(), _xid(db)
    count = 0
    for row in _inventory(db, checkpoint=checkpoint, with_payloads=True, descending=True):
        checkpoint()
        if not _current(row, version, xid):
            raise ValueError('Complete institutional preparation pending')
        yield _decode(row['cached'], row)
        count += 1
    if not count:
        raise ValueError('Missing complete institutional inventory')


def preparation_fingerprint(db, *, checkpoint=lambda: None):
    digest = hashlib.sha256()
    count = 0
    version, xid = parser_version(), _xid(db)
    for row in _inventory(db, checkpoint=checkpoint, with_payloads=True, descending=True):
        if not _current(row, version, xid):
            raise ValueError('Complete institutional preparation pending')
        _checked_payload(row['cached'])
        digest.update(json.dumps([row['id'], row['revision_id'], row['identity'], row['token'],
                                 row['cached'].payload_sha256], separators=(',', ':')).encode() + b'\n')
        count += 1
    if not count:
        raise ValueError('Missing complete institutional inventory')
    return dict(documents=count, sha256=digest.hexdigest(), parser_version=version)
