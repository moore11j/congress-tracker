"""Exact-byte economic transcriptions awaiting description/security review.

This separate registry cannot authorize publication. It preserves reviewed
economic fields and visual row positions without approving candidate names or
third-party ticker mappings. Completed stock-only reviews retain their format.
"""
from datetime import date
import hashlib
import json
from pathlib import Path

REGISTRY = Path(__file__).resolve().parents[2] / 'config' / 'house_ptr_preparations.json'
METADATA_KEYS = {'key', 'filing_id', 'filing_date', 'member_name', 'district', 'form', 'url'}
AMOUNTS = {(1001,15000), (15001,50000), (50001,100000), (100001,250000),
           (250001,500000), (500001,1000000), (1000001,5000000),
           (5000001,25000000), (25000001,50000000)}
HOLD = 'House scan descriptions and security identities require review'


def prepared_house_scan(raw, metadata, page_count):
    registry_bytes = REGISTRY.read_bytes()
    registry = json.loads(registry_bytes)
    if registry.get('schema_version') != 1:
        raise ValueError('Unsupported House preparation registry')
    digest = hashlib.sha256(raw).hexdigest()
    matches = [r for r in registry['preparations'] if r['source_sha256'] == digest]
    if not matches:
        return None
    if len(matches) != 1:
        raise ValueError('Ambiguous House economic preparation')
    review = matches[0]
    if (set(review['metadata']) != METADATA_KEYS
            or review['metadata'] != {key: metadata.get(key) for key in METADATA_KEYS}
            or review['page_count'] != page_count
            or review.get('review_scope') != 'economic_fields_only'
            or review.get('publication_eligible') is not False
            or review.get('amendment_flag') is not False
            or review.get('review_method') != 'rendered_page_visual_review'):
        raise ValueError('House economic preparation differs from source or review scope')
    counts = review['page_row_counts']
    rows = review['rows']
    if (len(counts) != page_count or any(type(n) is not int or n < 0 for n in counts)
            or not 1 <= len(rows) <= 2000 or len(rows) != review['row_count'] or sum(counts) != len(rows)):
        raise ValueError('House economic preparation page population incomplete')
    expected = [(page, position) for page, count in enumerate(counts, 1) for position in range(1, count+1)]
    transactions = []
    for row, (page, position) in zip(rows, expected, strict=True):
        fields = row['reviewed_economics']
        if (row['page'] != page or row['row'] != position
                or row['source_line_ref'] != f'p{page}:r{position}'
                or not isinstance(row['candidate_asset_description'], str)
                or not 1 <= len(row['candidate_asset_description'].strip()) <= 1000
                or fields['ownerCodeRaw'] not in {'SP', 'DC', 'JT', None}
                or fields['actionCodeRaw'] not in {'Purchase', 'Sale'}
                or type(fields['partialSale']) is not bool
                or type(fields['capGainsOver200']) is not bool
                or fields['partialSale'] and fields['actionCodeRaw'] != 'Sale'
                or type(fields['amountLow']) is not int or type(fields['amountHigh']) is not int
                or (fields['amountLow'], fields['amountHigh']) not in AMOUNTS
                or not date.fromisoformat(fields['transactionDate']) <= date.fromisoformat(fields['notificationDate'])
                       <= date.fromisoformat(metadata['filing_date'])):
            raise ValueError('House economic preparation row fields incomplete')
        # No raw or resolved ticker is accepted by this preparation format.
        if set(row) != {'page', 'row', 'source_line_ref', 'candidate_asset_description', 'reviewed_economics'}:
            raise ValueError('Unexpected House preparation identity enrichment')
        transactions.append({
            'source_line_ref': row['source_line_ref'], 'source_line_ref_kind': 'visual_position',
            'source_page': page, 'source_row': position,
            'owner': fields['ownerCodeRaw'], 'owner_disclosure_status': 'not_disclosed' if fields['ownerCodeRaw'] is None else 'reported',
            'assetDescription': row['candidate_asset_description'], 'description_reviewed': False,
            'asset_type': 'unresolved', 'symbol': None,
            'transaction_type_raw': 'Partial Sale' if fields['partialSale'] else fields['actionCodeRaw'],
            'transaction_type': fields['actionCodeRaw'].lower(),
            'transaction_date': fields['transactionDate'], 'notification_date': fields['notificationDate'],
            'amount': f"${fields['amountLow']:,} - ${fields['amountHigh']:,}",
            'capital_gains_over_200': fields['capGainsOver200'],
            'source_notes': 'Economics visually reviewed; asset description is an unverified transcription candidate.',
        })
    report = {**metadata, 'document_url': metadata['url'], 'transactions': transactions,
        'parser_version': 'official_congress_rows_v2', 'amendment_flag': False,
        'preparation_review': {k: review[k] for k in ('source_sha256', 'reviewed_on', 'review_scope', 'review_method',
                                                     'page_count', 'row_count', 'page_row_counts', 'publication_eligible')},
        'review_holds': [HOLD], 'preparation_registry_sha256': hashlib.sha256(registry_bytes).hexdigest()}
    return 'Reviewed economic fields; unverified asset descriptions\n' + json.dumps(review, sort_keys=True), report
