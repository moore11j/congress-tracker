"""Independent identity and read-only canonical plans for secondary Senate rows."""
from copy import deepcopy
from datetime import date
import re

from app.services.direct_congress_reconciliation import (
    _key, _same_name, _REVIEWED_NAMES, name_tokens, reconcile_direct_congress)
from app.services.senate_backup_evidence import PROVIDER, _hash

# Reviewed October 12, 2026 against these independent official records. Exact
# spellings only: this is not a general Jim/James or Tim/Timothy substitution.
REVIEWED_DISCLOSURE_NAMES = {
    ('james', 'banks'): ('B001299', (
        'https://history.house.gov/People/Listing/B/BANKS,-James-E--(B001299)/',)),
    ('william', 'f', 'hagerty'): ('H000601', (
        'https://www.senate.gov/legislative/LIS/roll_call_votes/vote1151/vote_115_1_00160.htm',
        'https://www.hagerty.senate.gov/about/')),
    ('m', 'michael', 'rounds'): ('R000605', (
        'https://www.rounds.senate.gov/imo/media/doc/rounds_milcon_cds_119th.pdf',
        'https://www.rounds.senate.gov/imo/media/doc/senator_rounds_official_biography_052025.pdf')),
    ('timothy', 'p', 'sheehy'): ('S001232', (
        'https://www.sheehy.senate.gov/wp-content/uploads/2025/12/SHEEHY_MILITARY_SUMMARY.pdf',)),
    ('thomas', 'h', 'tuberville'): ('T000278', (
        'https://www.senate.gov/states/AL/senators.shtml',)),
}


def resolve_member(prepared, directory):
    """Require a unique independent name/active-Senate-term match before using the hint.

    A secondary Bioguide ID is a cross-check, never an identity override. This
    path does not invent the official website's '(Senator)' filer-type label.
    """
    filing = prepared['source_record']
    day = date.fromisoformat(filing['filingDate']).isoformat()
    source = name_tokens(' '.join(filter(None, (filing.get('filerFirst'), filing.get('filerLast')))))
    reviewed = REVIEWED_DISCLOSURE_NAMES.get(source)
    candidates = []
    for person in directory:
        identifier = person.get('id', {}).get('bioguide', '')
        if not re.fullmatch(r'[A-Z][0-9]{6}', identifier):
            continue
        name = person.get('name', {})
        names = [name.get('official_full'), ' '.join(filter(None,
                 (name.get('first'), name.get('middle'), name.get('last'))))]
        if name.get('nickname'):
            names.append(f"{name['nickname']} {name.get('last', '')}")
        if (not source or not any(_same_name(source, name_tokens(n)) for n in names)
                and _REVIEWED_NAMES.get(source) != identifier
                and (reviewed is None or reviewed[0] != identifier)):
            continue
        terms = [t for t in person.get('terms', []) if t.get('type') == 'sen'
                 and t['start'] <= day < t['end']]
        if len(terms) != 1:
            continue
        term = terms[0]
        candidates.append(dict(bioguide_id=identifier, first_name=name.get('first'),
            last_name=name.get('last'), chamber='senate', state=term.get('state'),
            party=term.get('party'), term_start=term['start'], term_end=term['end']))
    if len(candidates) != 1:
        return dict(status='held', reason='Independent member name/term missing or ambiguous',
                    candidates=len(candidates), directory_sha256=_hash(directory))
    member = candidates[0]
    if (member['bioguide_id'] != filing['memberId']
            or not re.fullmatch('[A-Z]{2}', member['state'] or '')):
        return dict(status='held', reason='Secondary member claim conflicts with independent directory',
                    directory_sha256=_hash(directory))
    return dict(status='resolved', member=member, directory_sha256=_hash(directory),
                reviewed_name_sources=list(reviewed[1]) if reviewed else [],
                identity_basis='independent_name_and_active_senate_term')


def reconcile(prepared, directory, *, filings, transactions, securities, members, events,
              member_trades, member_securities):
    """Plan against complete bounded canonical/member populations; never writes.

    The caller must capture all document rows and all candidate member trades
    and events under the Congress writer lock before executing any future plan.
    No result of this read-only function alone is publication authorization.
    """
    if prepared.get('source_provider') != PROVIDER or prepared.get('publishable') is not False:
        raise ValueError('Expected held secondary evidence')
    identity = resolve_member(prepared, directory)
    if identity['status'] != 'resolved':
        return {**identity, 'publishable':False}
    member = identity['member']
    source = deepcopy(prepared['transactions'])
    reasons = set(prepared['publication_holds']) - {
        'secondary_canonical_reconciliation_required', 'independent_member_term_required'}
    for row in source:
        reasons.update(row['publication_holds'])
        if row['member_id'] != member['bioguide_id'] or row['source_line_ref'] is not None:
            raise ValueError('Secondary row identity is inconsistent')
        # Private adapter keys for the generic planner. Never stored or exposed
        # as an official printed row number; public bindings use secondary_row_id.
        row['source_line_ref'] = 'secondary:'+row['secondary_row_id']
        row['normalized_hash'] = row['evidence_sha256']
        row['security_name_raw'] = row['issuer_name_raw']
    parsed = dict(metadata=dict(url=prepared['document_url'], filing_id=prepared['filing_id'],
        filing_date=prepared['source_record']['filingDate']), transactions=source)
    plan = reconcile_direct_congress(parsed, member, filings=filings, transactions=transactions,
                                     securities=securities, members=members, events=events)
    reasons.update(plan['reasons'])
    def economic(row):
        return _key(day=row['transaction_date'], owner=row['owner_normalized'],
                    action=row['transaction_type_normalized'], lower=row['amount_low'],
                    upper=row['amount_high'], symbol=row['ticker_normalized'], description=row['issuer_name_raw'])
    if plan['status'] == 'new':
        keys = {economic(row) for row in source}
        member_ids = {m['id'] for m in members if m['bioguide_id'] == member['bioguide_id']}
        for trade in member_trades:
            if trade['member_id'] not in member_ids:
                raise ValueError('Member-wide trade scope contains another identity')
            security = member_securities.get(trade.get('security_id'), {})
            key = _key(day=trade['trade_date'], owner=trade['owner_type'], action=trade['transaction_type'],
                lower=trade['amount_range_min'], upper=trade['amount_range_max'], symbol=security.get('symbol'),
                description=trade['description'] or security.get('name'))
            if key in keys:
                reasons.add('Source economics overlap another legacy transaction; verify filing identity')
    if plan['status'] == 'existing':
        from app.services.direct_congress_repair import _event_matches
        by_ref = {row['source_line_ref']:row for row in source}
        by_event = {row['id']:row for row in events}
        for match in plan['matched']:
            row = by_ref[match['source_line_ref']]
            if row['asset_type_normalized'] == 'stock':
                if (len(match['event_ids']) != 1 or not _event_matches(
                        by_event[match['event_ids'][0]], row, member, match['transaction_id'],
                        plan['filing_ids'][0], correct_disclosure=True)):
                    reasons.add('Existing public event disagrees with secondary evidence')
            elif match['event_ids']:
                reasons.add('Existing non-stock public event requires explicit reconciliation')
    # Keep secondary and official identity namespaces visibly separate.
    matched = [dict(secondary_row_id=m['source_line_ref'].removeprefix('secondary:'),
                    transaction_id=m['transaction_id'], event_ids=m['event_ids']) for m in plan['matched']]
    return dict(status='held' if reasons else plan['status'], reasons=sorted(reasons),
                member=member, directory_sha256=identity['directory_sha256'],
                identity_basis=identity['identity_basis'], filing_ids=plan['filing_ids'], matched=matched,
                reviewed_name_sources=identity.get('reviewed_name_sources', []),
                new_secondary_rows=[r.removeprefix('secondary:') for r in plan['new_source_rows']],
                ambiguous_secondary_rows=[r.removeprefix('secondary:') for r in plan['ambiguous_source_rows']],
                existing_only_ids=plan['existing_only_ids'], orphan_event_ids=plan['orphan_event_ids'],
                overlap_event_ids=plan['overlap_event_ids'], publishable=False,
                remaining_gate='locked_publication_and_recovery_validation')


def load_staged_evidence(db, document_id, *, expected_hash=None):
    """Reconstruct held evidence from its immutable bytes and original manifest."""
    from datetime import datetime, timezone
    import hashlib
    import json
    from sqlalchemy import select
    from app.services.direct_feed_store import DirectFeedDocument, DirectFeedRevision
    from app.services.senate_backup_collection import FEED, MAX_TOTAL, _snapshot
    from app.services.senate_backup_evidence import prepare_filing
    from app.services.direct_congress_worker import _context
    from app.services.direct_congress_repair import _record

    if db.new or db.dirty or db.deleted:
        raise ValueError('Inspector session has unrelated pending changes')
    document = db.get(DirectFeedDocument, document_id)
    if document is None or document.feed != FEED:
        raise ValueError('Not a staged Senate backup filing')
    def revision(row):
        item = db.scalar(select(DirectFeedRevision).where(
            DirectFeedRevision.document_id == row.id, DirectFeedRevision.content_hash == row.content_hash))
        if (item is None or not item.source_bytes or len(item.source_bytes) > MAX_TOTAL
                or hashlib.sha256(item.source_bytes).hexdigest() != item.content_hash):
            raise ValueError('Staged source bytes are missing or inconsistent')
        return item
    def manifest(digest, observed):
        row = db.scalar(select(DirectFeedDocument).where(
            DirectFeedDocument.feed == FEED+'_snapshot', DirectFeedDocument.source_key == digest))
        if row is None or row.content_hash != digest:
            raise ValueError('Snapshot manifest receipt is missing')
        return _snapshot(revision(row).source_bytes, observed)
    if expected_hash is not None and document.content_hash != expected_hash:
        raise ValueError('Staged source changed since publication')
    item = revision(document)
    prepared = json.loads(item.parsed_json)
    if document.parsed_json != item.parsed_json:
        raise ValueError('Mutable staging differs from its immutable revision')
    raw = json.loads(item.source_bytes)
    original_digest = prepared['snapshot_manifest_sha256']
    original_snapshot = manifest(original_digest, prepared['observed_at'])
    rebuilt = prepare_filing(raw['filing'], raw['trades'], snapshot=original_snapshot,
                             observed_at=prepared['observed_at'])
    rebuilt['snapshot_manifest_sha256'] = original_digest
    if rebuilt != prepared or document.source_key != prepared['filing_id'] or document.source_url != prepared['document_url']:
        raise ValueError('Prepared evidence differs from its captured source')
    return document, prepared, item.content_hash, manifest


def inspect_staged(db, document_id, directory, *, now=None):
    """Recheck immutable staging evidence and current canonical rows without writing."""
    from datetime import datetime, timezone
    import json
    from sqlalchemy import select
    from app.services.direct_feed_store import DirectFeedDocument
    from app.services.senate_backup_collection import FEED
    from app.services.direct_congress_worker import _context
    from app.services.direct_congress_repair import _record
    document, prepared, source_hash, manifest = load_staged_evidence(db, document_id)
    # Identical evidence can retain its first observation while a newer complete
    # snapshot verifies that it is still present. Require that latest receipt fresh.
    latest_digest = json.loads(document.metadata_json)['snapshot_sha256']
    current = db.scalar(select(DirectFeedDocument).where(DirectFeedDocument.feed == FEED+'_snapshot')
                        .order_by(DirectFeedDocument.checked_at.desc(), DirectFeedDocument.id.desc()).limit(1))
    if current is None or current.source_key != latest_digest:
        raise ValueError('Filing is absent from the latest staged snapshot scope')
    latest_snapshot = manifest(latest_digest, now or datetime.now(timezone.utc))
    identity = resolve_member(prepared, directory)
    if identity['status'] != 'resolved':
        return {**identity, 'publishable':False}
    state, stored = _context(db, prepared['document_url'], identity['member'])
    if stored is not None:
        state['members'] = list({m['id']:m for m in state['members']+[_record(stored)]}.values())
    plan = reconcile(prepared, directory,
        **{key:state[key] for key in ('filings', 'transactions', 'securities', 'members', 'events')},
        member_trades=state.get('member_trades', []), member_securities=state.get('member_securities', {}))
    return {**plan, 'document_id':document_id, 'source_evidence_sha256':source_hash,
            'latest_snapshot_sha256':latest_digest,
            'latest_source_checked_at':latest_snapshot['lastSourceCheckAt']}
