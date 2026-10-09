"""Bind direct SEC research text to the same guarded filing as press alerts."""
import json

from sqlalchemy import select

from app.models import ResearchSourceDocument, Security
from app.services.direct_feed_store import DirectFeedDocument
from app.services.feed_source_control import require_selected_source
from app.services.research_evidence import upsert_source_document
from app.services.sec_earnings_store import reconcile_staged_earnings
from app.services.sec_press_events import FEED, publish_sec_release


def prepare_sec_research_document(db, *, security_id, accession):
    """Caller owns commit; no fetch, model request, matching or delivery here.

    Research and alerts share publication boundaries and canonical identity.
    Exact legacy research matches are reused without relabeling or rewriting.
    """
    require_selected_source(db, FEED, 'sec_edgar')
    security = db.scalar(select(Security).where(Security.id == security_id).with_for_update())
    if security is None:
        raise ValueError('SEC research security missing')
    staged = db.scalar(select(DirectFeedDocument).where(DirectFeedDocument.feed == FEED,
                                                       DirectFeedDocument.source_key == accession))
    if staged is None:
        raise ValueError('SEC research source has not been staged')
    result = reconcile_staged_earnings(db, staged.id, security_id=security.id)
    if result['status'] == 'held':
        return {'status': 'held', 'reason': result['reason'], 'created': False}
    published = publish_sec_release(db, staged.id)
    if published['status'] == 'held':
        return {'status': 'held', 'reason': published['reason'], 'created': False}
    parsed = json.loads(staged.parsed_json)
    release = parsed['release']
    if result['status'] == 'matched':
        document = db.get(ResearchSourceDocument, result['existing_document_id'])
        if document is None or document.content_hash != release['text_sha256']:
            raise ValueError('Matched SEC research source changed')
        created = False
    else:
        document, created = upsert_source_document(db, security_id=security.id,
            document_type='press_release', source_provider='sec_edgar', external_id=parsed['canonical_key'],
            content=release['text'], title=f'{security.symbol} earnings release filed {parsed["filing_date"]}',
            source_url=release['url'], published_at=None, filing_type=json.loads(staged.metadata_json)['filing']['form_type'])
    if document.content_hash != release['text_sha256']:
        raise ValueError('SEC research normalized text differs from captured source')
    return {'status': 'prepared', 'created': created, 'document': document,
            'source_text': release['text'], 'canonical_event_id': published['event_id']}
