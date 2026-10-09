"""Reviewed original N-PORT evidence stored in shared durable staging."""
from datetime import date
import hashlib
import json
from pathlib import Path
import re

from sqlalchemy import select

from app.clients.direct_sources import DirectSourceError
from app.services.direct_13f_evidence import nport_identifiers
from app.services.direct_feed_store import DirectFeedDocument, DirectFeedRevision, discover, record_document


def reviewed_entries(path):
    raw = Path(path).read_bytes()
    if len(raw) > 1_000_000:
        raise ValueError('Identifier manifest exceeds size limit')
    entries = json.loads(raw)['documents']
    if not isinstance(entries, list) or not 1 <= len(entries) <= 50:
        raise ValueError('Supply one to fifty reviewed identifier sources')
    seen = set()
    for row in entries:
        match = re.fullmatch(r'https://www\.sec\.gov/Archives/edgar/data/[1-9]\d*/(\d{10}-\d{2}-\d{6})\.txt', row['url'])
        if not match or not re.fullmatch(r'[0-9a-f]{64}', row['content_hash']):
            raise ValueError('Invalid reviewed SEC source identity')
        key = match[1]
        if key in seen:
            raise ValueError('Duplicate reviewed identifier accession')
        seen.add(key)
    return entries


def _saved(db, entry):
    key = entry['url'].rsplit('/', 1)[1][:-4]
    doc = db.scalar(select(DirectFeedDocument).where(DirectFeedDocument.feed == 'sec_nport', DirectFeedDocument.source_key == key))
    if doc is None:
        return None
    if doc.status != 'parsed' or doc.source_url != entry['url'] or doc.content_hash != entry['content_hash']:
        raise DirectSourceError('Stored identifier identity differs from reviewed manifest')
    revision = db.scalar(select(DirectFeedRevision).where(DirectFeedRevision.document_id == doc.id,
                                                       DirectFeedRevision.content_hash == doc.content_hash))
    if revision is None or revision.source_bytes is None:
        raise DirectSourceError('Stored identifier original bytes are unavailable')
    return {'raw': revision.source_bytes, 'url': entry['url'], 'content_hash': entry['content_hash']}


def load_staged_identifiers(db, path):
    documents = []
    total = 0
    for entry in reviewed_entries(path):
        document = _saved(db, entry)
        if document is None:
            raise DirectSourceError('Reviewed identifier source has not been staged')
        total += len(document['raw'])
        if len(document['raw']) > 30_000_000 or total > 50_000_000:
            raise DirectSourceError('Stored identifier corpus exceeds byte limits')
        if hashlib.sha256(document['raw']).hexdigest() != document['content_hash']:
            raise DirectSourceError('Stored identifier checksum changed')
        documents.append(document)
    # Publication reparses N-PORT identities and filters availability per target.
    return documents


def stage_reviewed_identifiers(db, client, path):
    if db.new or db.dirty or db.deleted:
        raise ValueError('Identifier session has unrelated changes')
    results = []
    total = 0
    for entry in reviewed_entries(path):
        document = _saved(db, entry)
        reused = document is not None
        db.rollback()
        if document is None:
            document = {'raw': client.get(entry['url']), 'url': entry['url'], 'content_hash': entry['content_hash']}
        total += len(document['raw'])
        if len(document['raw']) > 30_000_000 or total > 50_000_000:
            raise DirectSourceError('Reviewed identifier corpus exceeds byte limits')
        rows = nport_identifiers(document, available_by=date.max)
        if not reused:
            key = entry['url'].rsplit('/', 1)[1][:-4]
            staged = discover(db, 'sec_nport', {'key': key, 'url': entry['url']})
            record_document(db, staged, document['raw'], document['raw'].decode(), {'identifiers': rows})
            db.commit()
        results.append({'url': entry['url'], 'content_hash': entry['content_hash'],
                        'identifiers': len(rows), 'reused': reused})
    return {'documents': results, 'bytes': total, 'public_writes': 0}
