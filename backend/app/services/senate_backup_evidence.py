"""Normalize secondary Senate evidence without claiming official row identity.

This boundary has no transport, database, publication or alert side effects.
Downloaded partition checksums and completeness must be verified by the caller.
"""
from collections import defaultdict
from copy import deepcopy
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
import re
from uuid import UUID

VERSION = 'community_senate_rows_v1'
PROVIDER = 'community_congressional_disclosures'
DATASET = 'austin-starks/congressional-stock-trades'
MAX_ROWS = 10000
MAX_PARTITION_BYTES = 50_000_000
OWNERS = {'self': 'self', 'spouse': 'spouse', 'joint': 'joint', 'dependent_child': 'dependent'}
ASSETS = {'Stock': 'stock', 'Stock Option': 'option', 'Corporate Bond': 'corporate_bond',
          'Municipal Security': 'municipal_bond', 'Non-Public Stock': 'private_stock', 'Other': 'other'}


def _hash(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':'),
                                    allow_nan=False).encode()).hexdigest()


def _clock(value):
    result = datetime.fromisoformat(value.replace('Z', '+00:00')) if isinstance(value, str) else value
    if not isinstance(result, datetime) or result.tzinfo is None:
        raise ValueError('Snapshot clocks require an explicit timezone')
    return result.astimezone(timezone.utc)


def _day(value):
    if not isinstance(value, str) or not re.fullmatch(r'\d{4}-\d{2}-\d{2}', value):
        raise ValueError('Invalid disclosure date')
    return date.fromisoformat(value)


def _amount(value):
    if value is None:
        return None
    if isinstance(value, bool):
        raise ValueError('Invalid amount')
    try:
        result = Decimal(str(value))
    except InvalidOperation as exc:
        raise ValueError('Invalid amount') from exc
    if not result.is_finite() or result < 0 or result > Decimal('1e15'):
        raise ValueError('Invalid amount')
    return float(result)


def verify_partitions(snapshot, table, year, files):
    """Require the complete checksum-bound year partition before decoding rows."""
    if table not in {'political_filings', 'political_trades'} or type(year) is not int:
        raise ValueError('Unsupported snapshot partition')
    manifests = [m for m in snapshot['tables'][table]['manifests'] if m['year'] == year]
    if len(manifests) != 1 or not manifests[0]['files']:
        raise ValueError('Missing or ambiguous year manifest')
    expected = manifests[0]['files']
    paths = [f['publicPath'] for f in expected]
    if len(set(paths)) != len(paths) or set(paths) != set(files):
        raise ValueError('Incomplete or duplicated partition set')
    for record in expected:
        raw = files[record['publicPath']]
        if (not isinstance(raw, bytes) or not 0 < len(raw) <= MAX_PARTITION_BYTES
                or len(raw) != record['size'] or hashlib.sha256(raw).hexdigest() != record['sha256']):
            raise ValueError('Partition size or checksum mismatch')
    return {r['publicPath']: r['sha256'] for r in expected}


def prepare_filing(filing, rows, *, snapshot, observed_at, max_age=timedelta(hours=24)):
    """Preserve every reported row and name its unresolved publication requirements."""
    if not isinstance(rows, list) or len(rows) > MAX_ROWS:
        raise ValueError('Incomplete or oversized filing rows')
    filing, rows = deepcopy(filing), deepcopy(rows)
    now = _clock(observed_at)
    generated = _clock(snapshot['generatedAt'])
    checked = _clock(snapshot['lastSourceCheckAt'])
    if (snapshot.get('schemaVersion') != 2 or snapshot.get('dataset') != DATASET
            or snapshot.get('audit', {}).get('passed') is not True):
        raise ValueError('Snapshot identity/schema/audit is not accepted')
    if max_age <= timedelta(0) or not now-max_age <= checked <= generated <= now:
        raise ValueError('Snapshot is stale or has inconsistent clocks')
    doc_id = filing['docId']
    if str(UUID(doc_id)) != doc_id or filing.get('chamber') != 'senate':
        raise ValueError('Invalid Senate document identity')
    url = filing['sourceUrl']
    if url not in {f'https://efdsearch.senate.gov/search/view/{kind}/{doc_id}/' for kind in ('ptr', 'paper')}:
        raise ValueError('Document URL does not match its identity')
    if not re.fullmatch(r'[0-9a-f]{64}', filing.get('rawSha256') or ''):
        raise ValueError('Missing original source hash')
    filed = _day(filing['filingDate'])
    if filed > checked.date():
        raise ValueError('Filing is later than the source check')
    if (type(filing.get('extractedRows')) is not int or len(rows) != filing['extractedRows']
            or len(rows) > MAX_ROWS):
        raise ValueError('Incomplete or oversized filing rows')
    if filing.get('extractionStatus') != 'ok':
        raise ValueError('Source extraction did not succeed')
    member_id = filing.get('memberId')
    if not re.fullmatch(r'[A-Z][0-9]{6}', member_id or ''):
        raise ValueError('Missing member identity')
    if filing.get('filerKey') != 'member:'+member_id:
        raise ValueError('Member keys disagree')
    if filing.get('identitySource') not in {'legislators', 'override'}:
        raise ValueError('Unresolved member identity')
    holds = {'secondary_canonical_reconciliation_required', 'independent_member_term_required'}
    if filing.get('amendedReportDate') is not None:
        _day(filing['amendedReportDate']); holds.add('amendment_reconciliation_required')
    if '/paper/' in url:
        holds.add('paper_original_review_required')
    prepared, indices = [], set()
    for row in rows:
        for field in ('docId', 'chamber', 'sourceUrl', 'rawSha256', 'memberId', 'filingDate', 'filerKey'):
            if row.get(field) != filing.get(field):
                raise ValueError('Reported row and parent disagree: '+field)
        index = row.get('rowIndex')
        if type(index) is not int or index < 0 or index in indices:
            raise ValueError('Invalid or repeated secondary row index')
        indices.add(index)
        row_holds = []
        if not isinstance(row.get('assetDescription'), str) or not row['assetDescription'].strip():
            row_holds.append('asset_description_missing')
        tx_day = row.get('transactionDate')
        if tx_day is None:
            row_holds.append('transaction_date_missing')
        elif _day(tx_day) > filed:
            raise ValueError('Transaction is after disclosure')
        owner = OWNERS.get(row.get('owner'))
        action = row.get('action')
        if owner is None: row_holds.append('owner_unresolved')
        if action not in {'purchase', 'sale'}: row_holds.append('action_review_required')
        if type(row.get('partialSale')) is not bool or (row['partialSale'] and action != 'sale'):
            raise ValueError('Partial sale conflicts with action')
        asset = ASSETS.get(row.get('assetTypeLabel'), 'unresolved')
        if asset in {'unresolved', 'option', 'other'}: row_holds.append('instrument_review_required')
        printed, resolved = row.get('printedTicker'), row.get('resolvedTicker')
        symbol = printed.strip().upper() if isinstance(printed, str) and printed.strip() else None
        if symbol is not None and not re.fullmatch(r'[A-Z][A-Z0-9]{0,5}(?:[./-][A-Z])?', symbol):
            row_holds.append('printed_ticker_invalid'); symbol = None
        if symbol:
            symbol = symbol.replace('.', '-').replace('/', '-')
            if row.get('resolutionStatus') != 'printed': row_holds.append('ticker_status_conflict')
        if resolved:
            row_holds.append('secondary_resolver_requires_independent_evidence')
        if asset == 'stock' and not symbol: row_holds.append('stock_ticker_unresolved')
        if asset != 'stock': symbol = None
        low, high = _amount(row.get('amountLow')), _amount(row.get('amountHigh'))
        if low is None: row_holds.append('amount_missing')
        if low is not None and high is not None and high < low:
            raise ValueError('Amount range is inverted')
        item = dict(filing_id=doc_id, source_provider=PROVIDER, chamber='senate', member_id=member_id,
            member_name_raw=' '.join(filter(None, (filing.get('filerFirst'), filing.get('filerLast')))),
            owner_normalized=owner, transaction_date=tx_day, disclosure_date=str(filed),
            ticker_raw=printed, ticker_normalized=symbol, secondary_resolved_ticker=resolved,
            issuer_name_raw=row.get('assetDescription'), asset_type_raw=row.get('assetTypeLabel'),
            asset_type_normalized=asset, transaction_type_normalized=action,
            transaction_type_raw=row.get('actionCodeRaw'), partial_sale=row['partialSale'],
            amount_low=low, amount_high=high, document_url=url,
            source_line_ref=None, secondary_row_index=index, parser_version=VERSION,
            amendment_flag=filing.get('amendedReportDate') is not None,
            source_record=row, publication_holds=sorted(row_holds))
        # This is an external row identity, never an official or economic-event ID.
        item['secondary_row_id'] = _hash([VERSION, doc_id, index])
        item['evidence_sha256'] = _hash(row)
        prepared.append(item)
    return dict(status='prepared', source_provider=PROVIDER, parser_version=VERSION,
        filing_id=doc_id, document_url=url, claimed_original_sha256=filing['rawSha256'],
        snapshot_generated_at=generated.isoformat(), snapshot_source_checked_at=checked.isoformat(),
        observed_at=now.isoformat(), reported_available_at=str(filing.get('availableAt')),
        availability_basis='secondary_observation', source_record=filing,
        transactions=sorted(prepared, key=lambda r:r['secondary_row_index']),
        publication_holds=sorted(holds), publishable=False)


def bind_original(prepared, raw):
    """Match complete economic multisets; ambiguous identical lots require review.

    This verifies extraction only, not canonical reconciliation or publication.
    Reparse the matching bytes through Walnut's official parser.
    """
    if hashlib.sha256(raw).hexdigest() != prepared['claimed_original_sha256']:
        raise ValueError('Original bytes do not match the secondary claim')
    from app.services.direct_feed_collection import parse_document
    filing = prepared['source_record']
    report_day = _day(filing.get('reportDate') or filing['filingDate'])
    metadata = dict(filing_id=prepared['filing_id'], url=prepared['document_url'],
        filing_date=filing['filingDate'], report_date=str(report_day),
        member_name=' '.join(filter(None, (filing.get('filerFirst'), filing.get('filerLast')))),
        report_title=f'Periodic Transaction Report for {report_day:%m/%d/%Y}',
        amendment_flag=filing.get('amendedReportDate') is not None)
    _, parsed, reasons = parse_document('senate_ptr', raw, metadata)
    if reasons:
        return {'status':'held', 'reason':'official_parser_hold', 'details':reasons,
                'bindings':[], 'publishable':False}
    official_rows = parsed['transactions']
    fields = ('owner_normalized', 'transaction_date', 'disclosure_date', 'ticker_normalized',
              'asset_type_normalized', 'transaction_type_normalized', 'amount_low', 'amount_high',
              'issuer_name_raw')
    def key(row, official=False):
        values = [str(row.get(f)) if row.get(f) is not None else None for f in fields]
        values.append('partial' in (row.get('transaction_type_raw') or '').lower()
                      if official else row['partial_sale'])
        return tuple(values)
    original = defaultdict(list)
    for row in official_rows:
        if (row.get('source_provider') != 'official_senate' or row.get('filing_id') != prepared['filing_id']
                or row.get('document_url') != prepared['document_url'] or not row.get('source_line_ref')):
            raise ValueError('Official parser identity disagrees')
        original[key(row, True)].append(row)
    candidates = defaultdict(list)
    for row in prepared['transactions']: candidates[key(row)].append(row)
    if set(original) != set(candidates) or any(len(original[k]) != len(candidates[k]) for k in original):
        raise ValueError('Complete reported economics disagree with the original')
    if any(len(rows) != 1 for rows in original.values()):
        return {'status':'held', 'reason':'ambiguous_identical_lots', 'bindings':[], 'publishable':False}
    bindings = [dict(secondary_row_id=candidates[k][0]['secondary_row_id'],
                     official_source_line_ref=rows[0]['source_line_ref'],
                     official_normalized_hash=rows[0]['normalized_hash']) for k, rows in original.items()]
    return {'status':'matched', 'bindings':sorted(bindings,key=lambda b:b['secondary_row_id']),
            'publishable':False, 'remaining_gate':'canonical_identity_and_member_reconciliation'}
