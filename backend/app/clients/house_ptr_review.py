"""Exact-document transcriptions of visually reviewed House checkbox forms.

This is a bounded review fallback, not OCR. Changed or unreviewed bytes never
inherit a transcription. Ticker enrichment remains separate from printed data.
"""
from datetime import date
import hashlib
import json
from pathlib import Path
import re


REGISTRY = Path(__file__).resolve().parents[2] / 'config' / 'house_ptr_reviews.json'
AMOUNTS = {'B': '$15,001 - $50,000', 'C': '$50,001 - $100,000'}


def reviewed_house_scan(raw, metadata, page_count):
    registry_bytes = REGISTRY.read_bytes()
    registry = json.loads(registry_bytes)
    if registry.get('schema_version') != 1:
        raise ValueError('Unsupported House scan review registry')
    sha = hashlib.sha256(raw).hexdigest()
    matches = [r for r in registry['reviews'] if r['source_sha256'] == sha]
    if not matches:
        return None
    if len(matches) != 1:
        raise ValueError('Ambiguous House scan review')
    review = matches[0]
    if (review['metadata'] != {k: metadata.get(k) for k in review['metadata']}
            or set(review['metadata']) != {'key', 'filing_id', 'filing_date', 'member_name', 'district', 'form', 'url'}
            or review['page_count'] != page_count or review['amendment_flag'] is not False
            or review['review_method'] != 'rendered_page_visual_review'):
        raise ValueError('House scan review differs from discovery or page population')
    rows = review['rows']
    if (not rows or len(rows) != review['row_count']
            or [r['source_line_ref'] for r in rows] != [str(i + 1) for i in range(len(rows))]
            or set(r['page'] for r in rows) != set(range(1, page_count + 1))):
        raise ValueError('House scan review row coverage incomplete')
    transactions = []
    for row in rows:
        mapping = row['security_mapping']
        if (row['owner'] not in {'JT', 'SP', 'DC', 'self'} or not row['assetDescription']
                or row['action_checkbox'] != 'Partial Sale' or row['amount_checkbox'] not in AMOUNTS
                or not date.fromisoformat(row['transaction_date']) <= date.fromisoformat(row['notification_date'])
                <= date.fromisoformat(metadata['filing_date'])
                or not re.fullmatch(r'[A-Z][A-Z0-9.\-]{0,9}', mapping['ticker'])
                or mapping['asset_type'] != 'stock' or not mapping['issuer'] or not mapping['security_title']
                or not re.fullmatch(r'https://www\.sec\.gov/Archives/edgar/data/\d+/\d+/[^/]+\.htm', mapping['source_url'])
                or date.fromisoformat(mapping['report_period']) > date.fromisoformat(row['transaction_date'])):
            raise ValueError('House scan review fields or security evidence incomplete')
        transactions.append({
            'source_line_ref': row['source_line_ref'], 'source_line_ref_kind': 'printed',
            'owner': row['owner'], 'symbol': None, 'assetDescription': row['assetDescription'],
            'asset_type_code': None, 'asset_type': 'stock',
            'transaction_type_raw': row['action_checkbox'], 'transaction_type': 'sale',
            'transaction_date': row['transaction_date'], 'notification_date': row['notification_date'],
            'amount': AMOUNTS[row['amount_checkbox']], 'source_page': row['page'],
            'source_notes': 'Visually reviewed checkbox form; ticker independently mapped from SEC listing.',
            'reviewed_security_mapping': mapping,
        })
    report = {**metadata, 'document_url': metadata['url'], 'transactions': transactions,
              'parser_version': 'official_congress_rows_v2', 'amendment_flag': False,
              'scan_review': {k: review[k] for k in ('source_sha256', 'reviewed_on', 'review_method', 'page_count', 'row_count')},
              'review_registry_sha256': hashlib.sha256(registry_bytes).hexdigest()}
    # Do not represent a reviewed transcription as an extracted PDF text layer.
    return 'Reviewed transcription of scanned House PTR\n' + json.dumps(review, sort_keys=True), report
