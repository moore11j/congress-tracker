"""Bounded prior-quarter acquisition into staging, without canonical writes."""
from datetime import date, timedelta
import hashlib
import json
import re

from sqlalchemy import select

from app.clients.direct_sources import DirectSourceError
from app.services.direct_feed_collection import parse_document
from app.services.direct_feed_store import (
    DirectFeedDocument, DirectFeedRevision, DirectFeedRun, discover, dumps,
    record_document, utcnow,
)


def prior_metadata(history, current):
    """Require one original known by the current filing date; never pick an amendment."""
    meta = current['metadata']
    cik = meta['cik'].zfill(10)
    if str(history.get('cik', '')).zfill(10) != cik:
        raise DirectSourceError('Prior discovery company identity mismatch')
    period = date.fromisoformat(meta['report_period'])
    if (period.month, period.day) not in {(3, 31), (6, 30), (9, 30), (12, 31)}:
        raise DirectSourceError('Expected quarter-end report period')
    prior = date(period.year, ((period.month - 1) // 3) * 3 + 1, 1) - timedelta(days=1)
    rows = history['filings']['recent']
    fields = ('form', 'reportDate', 'filingDate', 'accessionNumber')
    if len({len(rows[k]) for k in fields}) != 1:
        raise DirectSourceError('SEC submission columns have inconsistent lengths')
    records = [dict(zip(fields, values)) for values in zip(*(rows[k] for k in fields))]
    target = [r for r in records if r['accessionNumber'] == meta['key']]
    if len(target) != 1 or any(target[0][k] != v for k, v in {
        'form': '13F-HR', 'reportDate': period.isoformat(), 'filingDate': meta['filing_date']}.items()):
        raise DirectSourceError('Current filing absent or inconsistent in SEC history')
    candidates = [r for r in records if r['form'].startswith('13F')
                  and r['reportDate'] == prior.isoformat() and r['filingDate'] <= meta['filing_date']]
    if len(candidates) != 1 or candidates[0]['form'] != '13F-HR':
        raise DirectSourceError('Prior quarter missing from recent history, amended or nonunique')
    row = candidates[0]
    accession = row['accessionNumber']
    if not re.fullmatch(r'\d{10}-\d{2}-\d{6}', accession) or date.fromisoformat(row['filingDate']) < prior:
        raise DirectSourceError('Invalid prior accession or filing date')
    return {'key': accession, 'cik': cik, 'name': history.get('name') or meta.get('name'),
            'form': '13F-HR', 'filing_date': row['filingDate'],
            'url': f'https://www.sec.gov/Archives/edgar/data/{int(cik)}/{accession}.txt'}, prior.isoformat()


def _saved_source(db, document):
    revision = db.scalar(select(DirectFeedRevision).where(
        DirectFeedRevision.document_id == document.id,
        DirectFeedRevision.content_hash == document.content_hash))
    if not revision or not revision.source_bytes or hashlib.sha256(revision.source_bytes).hexdigest() != document.content_hash:
        raise DirectSourceError('Staged source checksum unavailable or changed')
    meta = json.loads(document.metadata_json)
    if meta['key'] != document.source_key or meta['url'] != document.source_url:
        raise DirectSourceError('Staged source identity changed')
    text, parsed, reasons = parse_document('sec_13f', revision.source_bytes, meta)
    if reasons:
        raise DirectSourceError('Staged source requires reconciliation')
    return revision.source_bytes, text, parsed, meta


def collect_13f_priors(db, client, *, document_ids):
    if not 1 <= len(document_ids) <= 20 or len(set(document_ids)) != len(document_ids):
        raise ValueError('Supply one to twenty distinct current document IDs')
    if db.new or db.dirty or db.deleted:
        raise ValueError('Collector session has unrelated changes')
    run = DirectFeedRun(report_json=dumps({'sources': ['sec_13f_priors'], 'document_ids': document_ids}))
    db.add(run); db.commit(); run_id = run.id
    results = []
    for document_id in document_ids:
        try:
            doc = db.get(DirectFeedDocument, document_id)
            if not doc or doc.feed != 'sec_13f' or doc.status != 'parsed':
                raise DirectSourceError('Current staged 13F is not parsed')
            _, _, current, metadata = _saved_source(db, doc)
            expected = (doc.content_hash, doc.metadata_json)
            url = 'https://data.sec.gov/submissions/CIK' + current['metadata']['cik'].zfill(10) + '.json'
            db.rollback()  # No database transaction remains open during HTTP.
            history_raw = client.get(url)
            history = json.loads(history_raw)
            prior, period = prior_metadata(history, current)
            saved = db.scalar(select(DirectFeedDocument).where(
                DirectFeedDocument.feed == 'sec_13f', DirectFeedDocument.source_key == prior['key']))
            reuse = bool(saved and saved.status == 'parsed')
            if saved and saved.status == 'parsed':
                raw, source_text, parsed, saved_meta = _saved_source(db, saved)
                if any(saved_meta[k] != prior[k] for k in ('key', 'cik', 'form', 'filing_date', 'url')):
                    raise DirectSourceError('Existing prior source conflicts with SEC history')
                prior = saved_meta  # Preserve existing name/metadata and publication bindings.
            else:
                db.rollback()
                raw = client.get(prior['url'])
                source_text, parsed, reasons = parse_document('sec_13f', raw, prior)
                if reasons:
                    raise DirectSourceError('Prior submission requires amendment/omission reconciliation')
            if parsed['metadata']['report_period'] != period:
                raise DirectSourceError('Prior report period differs from SEC history')
            doc = db.get(DirectFeedDocument, document_id, populate_existing=True)
            if doc.status != 'parsed' or (doc.content_hash, doc.metadata_json) != expected:
                raise DirectSourceError('Current source changed during prior acquisition')
            receipt = discover(db, 'sec_13f_history', {'key': current['metadata']['cik'], 'url': url})
            record_document(db, receipt, history_raw, history_raw.decode(), history)
            staged = saved if reuse else discover(db, 'sec_13f', prior)
            if not reuse:
                record_document(db, staged, raw, source_text, parsed)
            result = {'document_id': document_id, 'status': 'collected', 'prior_document_id': staged.id,
                      'accession': prior['key'], 'content_hash': staged.content_hash,
                      'history_sha256': receipt.content_hash, 'report_period': period}
            db.commit(); results.append(result)
        except Exception as exc:
            db.rollback()
            results.append({'document_id': document_id, 'status': 'held', 'reason': f'{type(exc).__name__}: {exc}'[:1000]})
            if isinstance(exc, DirectSourceError) and str(exc).startswith((
                'Source transport failed', 'Source cooldown', 'Source HTTP 403:', 'Source HTTP 429:')):
                break
    report = {'sources': ['sec_13f_priors'], 'results': results,
              'requested': len(document_ids), 'processed': len(results), 'public_writes': 0}
    run = db.get(DirectFeedRun, run_id)
    run.status = 'partial' if any(r['status'] == 'held' for r in results) else 'collected'
    run.finished_at = utcnow(); run.report_json = dumps(report); db.commit()
    return {'run_id': run_id, 'status': run.status, **report}
