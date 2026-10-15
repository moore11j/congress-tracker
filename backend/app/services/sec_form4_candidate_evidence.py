"""Distinguish owner-specific SEC transactions using retained original evidence."""
from collections import OrderedDict
import hashlib
import json
import re
from xml.etree.ElementTree import ParseError

from sqlalchemy import select

from app.clients.direct_sources import DirectSourceError
from app.models import InsiderTransactionNormalized, SecForm4Filing
from app.services.direct_feed_collection import parse_document
from app.services.direct_feed_store import DirectFeedDocument, DirectFeedRevision, dumps


def _cik(value):
    value = str(value or '').strip()
    return value.zfill(10) if re.fullmatch(r'[0-9]{1,10}', value) and int(value) else None


class OtherOwnerEvidence:
    """A bounded cache used only within one atomic filing projection.

    It retains parsed evidence, not transport bytes, and is never shared across
    transactions or workers. Missing or changed evidence cannot authorize a skip.
    """
    MAX_SOURCES = 32
    MAX_BYTES = 8 * 1024 * 1024

    def __init__(self, db):
        self.db = db
        self.cache = OrderedDict()
        self.bytes = 0

    def _original(self, verification):
        from app.services.direct_feed_worker import DirectFeedPublication, _source_url
        key = (verification.get('accession'), verification.get('sha256'), verification.get('url'))
        if not all(isinstance(v, str) and v for v in key):
            return None
        if key in self.cache:
            self.cache.move_to_end(key)
            return self.cache[key][0]
        accession, digest, url = key
        document = self.db.scalar(select(DirectFeedDocument).where(
            DirectFeedDocument.feed == 'sec_form4', DirectFeedDocument.source_key == accession))
        if (document is None or document.status != 'parsed' or document.content_hash != digest
                or document.source_url != url):
            return None
        receipt = self.db.scalar(select(DirectFeedPublication).where(
            DirectFeedPublication.document_id == document.id))
        if receipt is None or receipt.status != 'published' or receipt.source_hash != digest:
            return None
        revision = self.db.scalar(select(DirectFeedRevision).where(
            DirectFeedRevision.document_id == document.id, DirectFeedRevision.content_hash == digest))
        if revision is None or not revision.source_bytes or hashlib.sha256(revision.source_bytes).hexdigest() != digest:
            return None
        try:
            metadata, report = json.loads(document.metadata_json), json.loads(receipt.report_json)
            if (metadata['key'] != accession or metadata['url'] != url
                    or hashlib.sha256(dumps(metadata).encode()).hexdigest() != receipt.metadata_hash):
                return None
            _source_url(metadata)
            source_text, parsed, reasons = parse_document('sec_form4', revision.source_bytes, metadata)
            if reasons:
                return None
            rows = {row['normalized_hash']: row for row in parsed['transactions']}
            event_ids = set(report['event_ids'])
            if (len(rows) != len(parsed['transactions']) or len(event_ids) != len(rows)
                    or set(report['normalized_hashes']) != set(rows)):
                return None
        except (ValueError, KeyError, TypeError, ParseError, DirectSourceError):
            return None
        filing = self.db.scalar(select(SecForm4Filing).where(SecForm4Filing.accession_number == accession))
        if (filing is None or filing.document_hash != digest or filing.source_url != url
                or filing.raw_xml_text != source_text
                or _cik(filing.reporting_owner_cik) != _cik(parsed['filing']['reporting_owner_cik'])
                or _cik(filing.issuer_cik) != _cik(parsed['filing']['issuer_cik'])):
            return None
        result = {'rows': rows, 'event_ids': event_ids, 'filing_id': filing.id}
        size = len(revision.source_bytes)
        if size <= self.MAX_BYTES:
            while self.cache and (len(self.cache) >= self.MAX_SOURCES or self.bytes + size > self.MAX_BYTES):
                _, (_, removed_size) = self.cache.popitem(last=False)
                self.bytes -= removed_size
            self.cache[key] = result, size
            self.bytes += size
        return result

    def is_distinct(self, event, payload, current):
        owner = _cik(current.get('reporting_owner_cik'))
        verification = payload.get('sec_verification')
        if (not owner or event.source_provider != 'sec_edgar' or event.source != 'sec_edgar'
                or not isinstance(verification, dict) or verification.get('feed') != 'sec_form4'
                or verification.get('accession') == current.get('accession_number')
                or event.source_document_url != verification.get('url')):
            return False
        identifier = payload.get('normalized_transaction_id')
        if type(identifier) is not int or identifier <= 0:
            return False
        normalized = self.db.get(InsiderTransactionNormalized, identifier)
        if (normalized is None or normalized.is_duplicate
                or normalized.normalized_hash != event.source_filing_id
                or normalized.accession_number != verification.get('accession')
                or not _cik(normalized.reporting_owner_cik)
                or _cik(normalized.reporting_owner_cik) == owner
                or _cik(payload.get('reporting_cik')) != _cik(normalized.reporting_owner_cik)):
            return False
        original = self._original(verification)
        if original is None or event.id not in original['event_ids'] or normalized.form4_filing_id != original['filing_id']:
            return False
        row = original['rows'].get(normalized.normalized_hash)
        if row is None or event.symbol != row['ticker_normalized']:
            return False
        # Compare every parsed field, including owner, dates, lots, economics,
        # derivative/ownership flags and source URLs. Ignore mutable enrichment.
        return (dumps({field: getattr(normalized, field) for field in row}) == dumps(row)
                and dumps({field: payload.get(field) for field in row}) == dumps(row))
