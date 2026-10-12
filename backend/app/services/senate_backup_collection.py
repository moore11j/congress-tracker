"""Bounded, checksum-bound secondary evidence collection; never publishes events."""
from collections import defaultdict
from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
from tempfile import TemporaryDirectory
from threading import Timer
import time
from urllib.parse import urljoin, urlsplit

from app.services.senate_backup_evidence import DATASET, prepare_filing, verify_partitions, _clock

BASE = f'https://huggingface.co/datasets/{DATASET}/resolve/'
MAX_MANIFEST = 2_000_000
MAX_TOTAL = 20_000_000
MAX_FILES = 8
MAX_TABLE_ROWS = 100_000
MAX_FILINGS = 2000
TABLES = ('political_filings', 'political_trades')
FEED = 'senate_backup'


def _json(raw):
    def pairs(items):
        value = {}
        for key, item in items:
            if key in value:
                raise ValueError('Duplicate JSON field')
            value[key] = item
        return value
    return json.loads(raw, object_pairs_hook=pairs,
                      parse_constant=lambda value: (_ for _ in ()).throw(ValueError('Nonfinite JSON')))


def _bytes(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()


def _snapshot(raw, now):
    if not isinstance(raw, bytes) or not 0 < len(raw) <= MAX_MANIFEST:
        raise ValueError('Manifest exceeds byte limit')
    value = _json(raw)
    # Check identity/freshness even for an empty or rejected Senate partition.
    checked, generated, observed = map(_clock, (value['lastSourceCheckAt'], value['generatedAt'], now))
    if (value.get('schemaVersion') != 2 or value.get('dataset') != DATASET
            or value.get('audit', {}).get('passed') is not True
            or not 0 <= (observed-checked).total_seconds() <= 86400
            or not checked <= generated <= observed):
        raise ValueError('Snapshot identity, audit or freshness rejected')
    return value


def _inventory(snapshot, years):
    if (not isinstance(years, (tuple, list)) or not 1 <= len(years) <= 2
            or len(set(years)) != len(years)
            or any(type(y) is not int or not 2012 <= y <= 2100 for y in years)):
        raise ValueError('One or two explicit disclosure years required')
    result = {}
    for table in TABLES:
        for year in years:
            matches = [m for m in snapshot['tables'][table]['manifests'] if m['year'] == year]
            if len(matches) != 1 or not matches[0]['files']:
                raise ValueError('Missing or ambiguous year manifest')
            for entry in matches[0]['files']:
                path = entry['publicPath']
                if (not isinstance(path, str)
                        or not re.fullmatch(rf'data/{table}/{year}-\d{{5}}-of-\d{{5}}\.parquet', path)
                        or path in result or type(entry['size']) is not int
                        or not 0 < entry['size'] <= MAX_TOTAL
                        or not re.fullmatch(r'[0-9a-f]{64}', entry['sha256'])):
                    raise ValueError('Invalid partition manifest')
                result[path] = dict(entry, table=table, year=year)
    if len(result) > MAX_FILES or sum(r['size'] for r in result.values()) > MAX_TOTAL:
        raise ValueError('Snapshot partition budget exceeded')
    return result


def _get(session, url, limit, deadline):
    for _ in range(5):
        parsed = urlsplit(url)
        if (parsed.scheme != 'https' or parsed.username or parsed.password
                or parsed.port not in (None, 443) or parsed.hostname not in {
                    'huggingface.co', 'cdn-lfs.huggingface.co', 'cdn-lfs.hf.co',
                    'cdn-lfs-us-1.hf.co', 'cas-bridge.xethub.hf.co', 'us.aws.cdn.hf.co'}):
            raise ValueError('Unapproved snapshot redirect')
        remaining = deadline-time.monotonic()
        if remaining <= 0:
            raise TimeoutError('Snapshot download budget exhausted')
        with session.get(url, stream=True, allow_redirects=False,
                         timeout=(min(10, remaining), min(20, remaining))) as response:
            if response.status_code in (301, 302, 303, 307, 308):
                url = urljoin(url, response.headers['Location'])
                continue
            response.raise_for_status()
            length = response.headers.get('Content-Length')
            if length is not None and (not length.isdigit() or int(length) > limit):
                raise ValueError('Snapshot response exceeds byte limit')
            payload = bytearray()
            for chunk in response.iter_content(65536):
                if time.monotonic() > deadline:
                    raise TimeoutError('Snapshot download budget exhausted')
                if len(payload)+len(chunk) > limit:
                    raise ValueError('Snapshot response exceeds byte limit')
                payload.extend(chunk)
            return bytes(payload), response.headers.get('X-Repo-Commit')
    raise ValueError('Too many snapshot redirects')


def download_snapshot(*, years, observed_at=None, session=None):
    """Download only fixed dataset paths, pinned to the manifest's repository commit."""
    import requests
    now = observed_at or datetime.now(timezone.utc)
    owned = session is None
    session = session or requests.Session()
    try:
        deadline = time.monotonic()+180
        raw, revision = _get(session, BASE+'main/snapshot.json', MAX_MANIFEST, deadline)
        snapshot = _snapshot(raw, now)
        if not isinstance(revision, str) or not re.fullmatch(r'[0-9a-f]{40}', revision):
            raise ValueError('Snapshot repository revision is missing')
        inventory = _inventory(snapshot, years)
        files = {}
        for path, entry in inventory.items():
            files[path], _ = _get(session, BASE+revision+'/'+path, entry['size'], deadline)
        verify_bundle(raw, files, years=years, observed_at=now)
        return dict(manifest=raw, files=files, years=list(years), observed_at=_clock(now).isoformat(),
                    repository_revision=revision)
    finally:
        if owned:
            session.close()


def verify_bundle(raw, files, *, years, observed_at):
    snapshot = _snapshot(raw, observed_at)
    inventory = _inventory(snapshot, years)
    if set(files) != set(inventory):
        raise ValueError('Incomplete or extra snapshot files')
    for table in TABLES:
        for year in years:
            verify_partitions(snapshot, table, year, {p:files[p] for p, e in inventory.items()
                                                     if e['table']==table and e['year']==year})
    return snapshot, inventory


@contextmanager
def _decoder(paths):
    import duckdb
    with duckdb.connect(':memory:', config={'threads':1, 'memory_limit':'128MB',
            'max_temp_directory_size':'0B', 'autoinstall_known_extensions':False,
            'autoload_known_extensions':False}) as connection:
        connection.execute('SET allowed_paths=?', [paths])
        connection.execute('SET enable_external_access=false')
        connection.execute('SET lock_configuration=true')
        timer = Timer(30, connection.interrupt)
        timer.daemon = True
        timer.start()
        try:
            yield connection
        finally:
            timer.cancel()
            timer.join()


def decode_bundle(bundle):
    """Decode local verified Parquet only, with one thread and no remote extensions."""
    snapshot, inventory = verify_bundle(bundle['manifest'], bundle['files'], years=bundle['years'],
                                         observed_at=bundle['observed_at'])
    decoded = {table:[] for table in TABLES}
    decoded_bytes = 0
    with TemporaryDirectory(prefix='walnut-senate-') as directory:
        paths = {}
        for i, (public_path, raw) in enumerate(bundle['files'].items()):
            path = Path(directory)/f'{i}.parquet'
            path.write_bytes(raw)
            paths[public_path] = str(path.resolve())
        with _decoder(list(paths.values())) as connection:
            for public_path, path in paths.items():
                table = inventory[public_path]['table']
                result = connection.execute('SELECT * FROM read_parquet(?) LIMIT ?',
                                             [path, MAX_TABLE_ROWS+1])
                columns = [c[0] for c in result.description]
                if 'chamber' not in columns or 'docId' not in columns or 'filingDate' not in columns:
                    raise ValueError('Snapshot columns missing')
                count = 0
                while batch := result.fetchmany(256):
                    count += len(batch)
                    if count > MAX_TABLE_ROWS:
                        raise ValueError('Partition row budget exceeded')
                    for values in batch:
                        serialized = json.dumps(dict(zip(columns, values)), default=str, allow_nan=False)
                        decoded_bytes += len(serialized.encode())
                        if decoded_bytes > MAX_TOTAL:
                            raise ValueError('Decoded byte budget exceeded')
                        row = _json(serialized)
                        if row['chamber'] not in ('senate', 'house'):
                            raise ValueError('Unknown disclosure chamber')
                        if row['chamber'] == 'senate':
                            if str(row['filingDate'])[:4] != str(inventory[public_path]['year']):
                                raise ValueError('Disclosure year disagrees with partition')
                            decoded[table].append(row)
                            if len(decoded[table]) > MAX_TABLE_ROWS:
                                raise ValueError('Senate row budget exceeded')
    filings = decoded['political_filings']
    if not filings or len(filings) > MAX_FILINGS:
        raise ValueError('Empty or oversized Senate filing scope')
    by_id, rows = {}, defaultdict(list)
    for filing in filings:
        if filing['docId'] in by_id:
            raise ValueError('Duplicate Senate filing')
        by_id[filing['docId']] = filing
    for row in decoded['political_trades']:
        if row['docId'] not in by_id:
            raise ValueError('Orphan Senate trade')
        rows[row['docId']].append(row)
    # All-or-nothing conversion prevents a malformed filing becoming silent absence.
    return [prepare_filing(by_id[key], rows[key], snapshot=snapshot,
                           observed_at=bundle['observed_at']) for key in sorted(by_id)]


def stage_bundle(db, bundle, *, now=None):
    """Stage atomically in isolated tables. Caller owns commit; no source selection."""
    _snapshot(bundle['manifest'], now or datetime.now(timezone.utc))
    prepared = decode_bundle(bundle)
    from sqlalchemy import select
    from app.services.direct_feed_store import DirectFeedDocument, DirectFeedRevision, discover, record_document
    digest = hashlib.sha256(bundle['manifest']).hexdigest()
    latest = db.scalar(select(DirectFeedDocument).where(DirectFeedDocument.feed == FEED+'_snapshot')
                       .order_by(DirectFeedDocument.checked_at.desc(), DirectFeedDocument.id.desc()).limit(1))
    if latest is not None:
        previous = db.scalar(select(DirectFeedRevision).where(
            DirectFeedRevision.document_id == latest.id, DirectFeedRevision.content_hash == latest.content_hash))
        if (previous is None or not previous.source_bytes
                or hashlib.sha256(previous.source_bytes).hexdigest() != latest.content_hash):
            raise ValueError('Previous snapshot evidence is missing')
        prior_clock = _clock(_json(previous.source_bytes)['generatedAt'])
        next_clock = _clock(_json(bundle['manifest'])['generatedAt'])
        if next_clock < prior_clock or next_clock == prior_clock and digest != latest.content_hash:
            raise ValueError('Snapshot rollback or conflicting generation')
    with db.begin_nested():
        manifest = discover(db, FEED+'_snapshot', dict(key=digest, url=BASE+'main/snapshot.json'))
        record_document(db, manifest, bundle['manifest'], bundle['manifest'].decode(),
                        {'years':bundle['years'], 'repository_revision':bundle.get('repository_revision'),
                         'publishable':False})
        for path, raw in bundle['files'].items():
            file_hash = hashlib.sha256(raw).hexdigest()
            document = discover(db, FEED+'_partition', dict(key=file_hash, url=BASE+'main/'+path))
            record_document(db, document, raw, '', {'public_path':path, 'publishable':False})
        for item in prepared:
            raw = _bytes({'filing':item['source_record'],
                          'trades':[r['source_record'] for r in item['transactions']]})
            document = discover(db, FEED, dict(key=item['filing_id'], url=item['document_url'],
                                                snapshot_sha256=digest))
            evidence_hash = hashlib.sha256(raw).hexdigest()
            prior = db.scalar(select(DirectFeedRevision).where(
                DirectFeedRevision.document_id==document.id, DirectFeedRevision.content_hash==evidence_hash))
            # Preserve first observation for identical evidence, even on a newer snapshot.
            if prior is not None:
                item = _json(prior.parsed_json)
            else:
                item['snapshot_manifest_sha256'] = digest
            record_document(db, document, raw, raw.decode(), item,
                            reasons=('secondary_publication_not_enabled',))
        db.flush()
    return dict(status='staged', manifest_sha256=digest, filings=len(prepared),
                rows=sum(len(p['transactions']) for p in prepared), publishable=False)
