"""Read-only, one-to-one reconciliation for direct Congress disclosures.

This produces reviewable plans. It never publishes, repairs legacy identities,
or treats an ambiguous economic match as permission to insert another trade.
"""
from collections import defaultdict
from datetime import date
from decimal import Decimal
import json
import re
import unicodedata
from urllib.parse import urlsplit

from app.services.official_congress import normalize_congress_owner, normalize_congress_transaction_type


# Senate historian's index explicitly equates Addison Mitchell and Mitch.
# https://www.senate.gov/about/resources/pdf/baker-richard-a-full-transcript-with-index.pdf
_REVIEWED_NAMES = {('a', 'mitchell', 'mcconnell'): 'M000355'}


def name_tokens(value):
    value = unicodedata.normalize('NFKD', value or '').encode('ascii', 'ignore').decode()
    words = re.findall(r'[a-z0-9]+', value.lower())
    if words and words[0] in {'hon', 'rep', 'sen', 'senator', 'representative'}:
        words.pop(0)
    if words and words[-1] in {'jr', 'sr', 'ii', 'iii', 'iv'}:
        words.pop()
    return tuple(words)


def _same_name(source, candidate):
    # Only middle names may be absent or represented by matching initials.
    # No first-name nickname inference, last-name-only or district-only match.
    if source == candidate:
        return True
    if len(source) < 2 or len(candidate) < 2 or source[0] != candidate[0] or source[-1] != candidate[-1]:
        return False
    a, b = source[1:-1], candidate[1:-1]
    return not a or not b or (len(a) == len(b) and all(
        x == y or (min(len(x), len(y)) == 1 and x[0] == y[0]) for x, y in zip(a, b)))


def resolve_direct_member(metadata, chamber, directory):
    day = date.fromisoformat(metadata['filing_date'])
    source = name_tokens(metadata.get('member_name'))
    if not source:
        return {'status': 'held', 'reason': 'Missing disclosed member name'}
    if chamber == 'senate' and not (metadata.get('filer_type') or '').endswith('(Senator)'):
        return {'status': 'held', 'reason': 'Disclosure is not identified as a Senator filing'}
    candidates = []
    for person in directory:
        identifier = person.get('id', {}).get('bioguide', '')
        if not re.fullmatch(r'[A-Z][0-9]{6}', identifier):
            continue
        name = person.get('name', {})
        names = [name.get('official_full'), ' '.join(filter(None, [name.get('first'), name.get('middle'), name.get('last')]))]
        if name.get('nickname'):
            names.append(f"{name['nickname']} {name.get('last', '')}")
        if not any(_same_name(source, name_tokens(candidate)) for candidate in names) and _REVIEWED_NAMES.get(source) != identifier:
            continue
        terms = [term for term in person.get('terms', [])
                 if term.get('type') == ('rep' if chamber == 'house' else 'sen')
                 and term['start'] <= day.isoformat() < term['end']]
        if len(terms) != 1:
            continue
        term = terms[0]
        if chamber == 'house':
            district = re.fullmatch(r'([A-Z]{2})(\d{2})', metadata.get('district') or '')
            if not district or term.get('state') != district[1] or term.get('district') != int(district[2]):
                continue
        candidates.append({'bioguide_id': identifier, 'first_name': name.get('first'),
            'last_name': name.get('last'), 'chamber': chamber, 'state': term.get('state'),
            'party': term.get('party'), 'term_start': term['start'], 'term_end': term['end']})
    if len(candidates) != 1:
        return {'status': 'held', 'reason': 'Member identity/term is missing or ambiguous', 'candidates': len(candidates)}
    return {'status': 'resolved', 'member': candidates[0]}


def document_identity(url):
    """Canonicalize only the two official document URL families."""
    try:
        parsed = urlsplit(url or '')
        port = parsed.port
    except ValueError:
        return None
    if parsed.scheme not in {'http', 'https'} or parsed.username or parsed.password or port not in {None, 80, 443}:
        return None
    if parsed.query or parsed.fragment:
        return None
    path = parsed.path.rstrip('/')
    if parsed.hostname in {'disclosures-clerk.house.gov', 'clerk.house.gov'}:
        match = re.fullmatch(r'/public_disc/ptr-pdfs/(\d{4})/(\d+)\.pdf', path, re.I)
        if match:
            return ('house', match[1], match[2])
    if parsed.hostname == 'efdsearch.senate.gov':
        match = re.fullmatch(r'/search/view/(ptr|paper)/([a-f0-9-]{36})', path, re.I)
        if match:
            return ('senate', match[2].lower())
    return None


def _amount(value):
    return None if value is None else Decimal(str(value)).normalize()


def _key(*, day, owner, action, lower, upper, symbol, description):
    return (str(day or '')[:10], normalize_congress_owner(owner), normalize_congress_transaction_type(action),
            _amount(lower), _amount(upper), symbol or None, None if symbol else name_tokens(description))


def reconcile_direct_congress(parsed, member, *, filings, transactions, securities, members, events):
    """Reconcile entire filing populations; any leftover/ambiguous row holds it.

    Inputs are complete bounded snapshots. Production callers must fetch all
    candidate filings and their transactions under the feed writer lock.
    """
    metadata, source_rows = parsed['metadata'], parsed['transactions']
    identity = document_identity(metadata['url'])
    if identity is None or identity[0] != member['chamber'] or identity[-1] != metadata['filing_id']:
        raise ValueError('Unrecognized official source document')
    hashes = [row['normalized_hash'] for row in source_rows]
    if (not source_rows or len(hashes) != len(set(hashes)) or any(
            row['chamber'] != identity[0] or row['filing_id'] != metadata['filing_id'] for row in source_rows)):
        raise ValueError('Direct source row population identity is incomplete or repeated')
    candidates = [row for row in filings if document_identity(row.get('document_url')) == identity]
    member_by_id = {row['id']: row for row in members}
    security_by_id = {row['id']: row for row in securities}
    existing = [row for row in transactions if row['filing_id'] in {f['id'] for f in candidates}]
    linked_events = defaultdict(list)
    document_events, orphan_events = [], []
    for event in events:
        payload = json.loads(event.get('payload_json') or '{}')
        tx_id = payload.get('transaction_id')
        if tx_id is not None:
            linked_events[tx_id].append(event['id'])
        event_url = event.get('source_document_url') or payload.get('document_url') or payload.get('link')
        if document_identity(event_url) == identity:
            document_events.append(event['id'])
            if tx_id not in {row['id'] for row in existing}:
                orphan_events.append(event['id'])
    reasons = []
    if any(row['owner_normalized'] not in {'self', 'spouse', 'joint', 'dependent'} for row in source_rows):
        reasons.append('Source transaction owner is unresolved')
    if orphan_events:
        reasons.append('Source document has public events outside its legacy transaction population')
    if len(candidates) > 1:
        reasons.append('Multiple legacy filings refer to the same source document')
    for filing in candidates:
        stored_member = member_by_id.get(filing['member_id'], {})
        if stored_member.get('bioguide_id') != member['bioguide_id'] or stored_member.get('chamber') != member['chamber']:
            reasons.append('Legacy filing member identity differs')
        if str(filing.get('filing_date'))[:10] != metadata['filing_date']:
            reasons.append('Legacy filing disclosure date differs')
    source_groups, existing_groups = defaultdict(list), defaultdict(list)
    for row in source_rows:
        key = _key(day=row['transaction_date'], owner=row['owner_normalized'],
            action=row['transaction_type_normalized'], lower=row['amount_low'], upper=row['amount_high'],
            symbol=row['ticker_normalized'], description=row['issuer_name_raw'] or row['security_name_raw'])
        source_groups[key].append(row)
    overlap_events = []
    if not candidates:
        for event in events:
            if event.get('member_bioguide_id') != member['bioguide_id'] or event.get('chamber') != member['chamber']:
                continue
            payload = json.loads(event.get('payload_json') or '{}')
            raw = payload.get('raw') or {}
            key = _key(day=payload.get('transaction_date') or payload.get('trade_date') or raw.get('transactionDate'),
                owner=payload.get('owner_type') or raw.get('owner'),
                action=event.get('trade_type') or event.get('transaction_type'),
                lower=event.get('amount_min'), upper=event.get('amount_max'), symbol=event.get('symbol'),
                description=payload.get('security_description') or payload.get('description'))
            if key in source_groups:
                overlap_events.append(event['id'])
        if overlap_events:
            reasons.append('New source filing overlaps existing public trade economics; verify source binding')
    for row in existing:
        security = security_by_id.get(row.get('security_id'), {})
        key = _key(day=row['trade_date'], owner=row['owner_type'], action=row['transaction_type'],
            lower=row['amount_range_min'], upper=row['amount_range_max'], symbol=security.get('symbol'),
            description=row['description'] or security.get('name'))
        existing_groups[key].append(row)
    matched, new, ambiguous, unmatched_existing = [], [], [], []
    for key in set(source_groups) | set(existing_groups):
        source, stored = source_groups[key], existing_groups[key]
        if not source:
            unmatched_existing.extend(row['id'] for row in stored)
        elif not stored:
            new.extend(row['source_line_ref'] for row in source)
        elif len(source) != 1 or len(stored) != 1:
            ambiguous.extend(row['source_line_ref'] for row in source)
        else:
            direct, legacy = source[0], stored[0]
            legacy_member = member_by_id.get(legacy['member_id'], {})
            security = security_by_id.get(legacy.get('security_id'), {})
            # A ticker match alone cannot establish instrument identity.
            is_equity = direct['asset_type_normalized'] == 'stock'
            type_ok = ((is_equity and security.get('asset_class', '').lower() in {'stock', 'stocks', 'equity'})
                or (direct['asset_type_normalized'] not in {'stock', 'unresolved', 'option', 'etf', 'etn'}
                    and not direct['ticker_normalized'] and legacy.get('security_id') is None))
            if not type_ok or legacy_member.get('bioguide_id') != member['bioguide_id'] or str(legacy.get('report_date'))[:10] != metadata['filing_date']:
                ambiguous.append(direct['source_line_ref'])
            else:
                event_ids = linked_events[legacy['id']]
                if (is_equity and len(event_ids) != 1) or len(event_ids) > 1:
                    reasons.append('Matched equity transaction has missing or multiple public events')
                matched.append({'source_line_ref': direct['source_line_ref'], 'transaction_id': legacy['id'],
                                'event_ids': sorted(event_ids)})
    if ambiguous:
        reasons.append('Multiple lots or unverified legacy instrument/member/date identity')
    if unmatched_existing:
        reasons.append('Legacy filing contains transactions absent from the parsed source')
    if candidates and new:
        reasons.append('Existing filing has unmatched source rows; reconcile before adding')
    return {'status': 'held' if reasons else ('existing' if candidates else 'new'),
        'reasons': sorted(set(reasons)), 'filing_ids': sorted(row['id'] for row in candidates),
        'matched': sorted(matched, key=lambda row: row['transaction_id']),
        'new_source_rows': sorted(new), 'ambiguous_source_rows': sorted(ambiguous),
        'existing_only_ids': sorted(unmatched_existing), 'document_event_ids': sorted(document_events),
        'orphan_event_ids': sorted(orphan_events), 'overlap_event_ids': sorted(overlap_events)}
