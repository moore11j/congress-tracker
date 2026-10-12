"""Reviewed scan descriptions do not authorize a security or publication."""
import hashlib
import json
from pathlib import Path

REGISTRY = Path(__file__).resolve().parents[2] / 'config' / 'house_ptr_descriptions.json'
STATUSES = {'matched_visible_description', 'corrected_from_original', 'unclear_printed_characters'}
INDICATORS = {'equity_candidate', 'index_linked_instrument', 'hybrid_security', 'option',
              'depositary_receipt', 'exchange_traded_fund', 'municipal_debt', 'corporate_debt'}


def reviewed_descriptions(preparation):
    raw = REGISTRY.read_bytes()
    review = json.loads(raw)
    if review.get('schema_version') != 1:
        raise ValueError('Unsupported House description review')
    if review.get('source_sha256') != preparation['source_sha256']:
        return {}, None
    if (review.get('metadata') != preparation['metadata']
            or review.get('page_count') != preparation['page_count']
            or review.get('row_count') != preparation['row_count']
            or review.get('page_row_counts') != preparation['page_row_counts']
            or review.get('review_method') != 'rendered_page_visual_review'
            or review.get('review_scope') != 'asset_descriptions_trust_headings_and_instrument_indicators'
            or review.get('publication_eligible') is not False
            or type(review.get('security_mappings_approved')) is not int
            or review.get('security_mappings_approved') != 0
            or len(review.get('rows', [])) != len(preparation['rows'])):
        raise ValueError('House description review differs from original preparation')
    rows = {}
    for candidate, row in zip(preparation['rows'], review['rows'], strict=True):
        position = candidate['source_line_ref']
        status = row.get('description_status')
        description = row.get('reviewed_description')
        if (row.get('source_line_ref') != position or position in rows
                or row.get('page') != candidate['page'] or row.get('row') != candidate['row']
                or row.get('candidate_description') != candidate['candidate_asset_description']
                or status not in STATUSES
                or (status == 'unclear_printed_characters' and description is not None)
                or (status != 'unclear_printed_characters' and
                    (not isinstance(description, str) or not 1 <= len(description.strip()) <= 1000))
                or (status == 'matched_visible_description' and description != row.get('candidate_description'))
                or (status == 'corrected_from_original' and
                    (description == row.get('candidate_description') or not row.get('description_note')))
                or row.get('disclosed_instrument_indicator') not in INDICATORS
                or not isinstance(row.get('trust_heading'), str) or not row['trust_heading'].strip()
                or (row.get('account_heading') is not None and not isinstance(row['account_heading'], str))
                or row.get('security_identity_status') != 'unverified'
                or row.get('mapped_ticker') is not None or row.get('publication_eligible') is not False):
            raise ValueError('House description row identity or review scope differs')
        rows[position] = row
    return rows, {'source_sha256': review['source_sha256'], 'reviewed_on': review['reviewed_on'],
                  'review_scope': review['review_scope'], 'row_count': len(rows),
                  'registry_sha256': hashlib.sha256(raw).hexdigest(), 'publication_eligible': False}
