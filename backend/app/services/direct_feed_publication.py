"""Source-bound Form 4 projection shared by rehearsal and the guarded worker."""
from datetime import date, datetime, timezone
import hashlib
import json
import math

from sqlalchemy import select

from app.models import Event, InsiderTransaction, InsiderTransactionNormalized, SecForm4Filing
from app.services.direct_feed_collection import parse_document
from app.services.direct_feed_store import dumps, reconcile_insider


def rehearse_new_form4(db, document, *, publish_since: date):
    bind = db.get_bind()
    if bind.dialect.name != 'sqlite' or bind.url.database != ':memory:':
        raise ValueError('Publication rehearsal requires in-memory SQLite')
    result = _project_new_form4(db, document, publish_since=publish_since)
    return {**result, 'status': 'rehearsed' if result['status'] == 'published' else result['status'],
            'production_writes': 0}


def _project_new_form4(db, document, *, publish_since: date):
    # The public entry point is direct_feed_worker, which owns source selection,
    # locking, the transaction, durable receipt and enrichment queue.
    if db.new or db.dirty or db.deleted:
        raise ValueError('Rehearsal session has unrelated pending changes')
    raw, metadata = document['raw'], document['metadata']
    digest = hashlib.sha256(raw).hexdigest()
    if document['feed'] != 'sec_form4' or digest != document['content_hash']:
        raise ValueError('Wrong feed or source checksum')
    source_text, parsed, reasons = parse_document('sec_form4', raw, metadata)
    held = lambda reason: {'status': 'held', 'reason': reason, 'inserted_events': 0, 'inserted_transactions': 0}
    if reasons:
        return held('; '.join(reasons))
    if date.fromisoformat(metadata['filing_date']) < publish_since:
        return held('Historical import is outside the approved alert publication window')
    rows = parsed['transactions']
    for row in rows:
        if (not row.get('ticker_normalized') or not row.get('transaction_date') or
                row['transaction_date'] > row['filing_date'] or row.get('shares') is None or
                not math.isfinite(row['shares']) or row['shares'] < 0 or
                row['price'] is not None and (not math.isfinite(row['price']) or row['price'] < 0) or
                row['value'] is not None and not math.isfinite(row['value'])):
            return held('Invalid transaction identity/date/economics')
    reconciliation = reconcile_insider(db, parsed)
    if reconciliation['ambiguous'] or reconciliation['existing_only_ids']:
        return held('Existing filing or economic identity requires reconciliation')
    if reconciliation['matched']:
        return {'status': 'existing', 'matched': len(reconciliation['matched']),
                'inserted_events': 0, 'inserted_transactions': 0}
    filing = db.scalar(select(SecForm4Filing).where(SecForm4Filing.accession_number == metadata['key']))
    if filing is not None:
        return held('Existing filing without complete canonical rows')
    # Legacy rows/events can exist without normalized records. Conservatively
    # hold their same-symbol/date candidates instead of guessing that SEC is new.
    for row in rows:
        if db.scalar(select(InsiderTransactionNormalized.id).where(
                InsiderTransactionNormalized.ticker_normalized == row['ticker_normalized'],
                InsiderTransactionNormalized.transaction_date == row['transaction_date'],
                InsiderTransactionNormalized.reporting_owner_cik == row['reporting_owner_cik']).limit(1)) is not None:
            return held('Cross-accession owner/date candidate requires reconciliation')
        if db.scalar(select(InsiderTransaction.id).where(
                InsiderTransaction.symbol == row['ticker_normalized'],
                InsiderTransaction.transaction_date == row['transaction_date']).limit(1)) is not None:
            return held('Unnormalized provider candidate requires reconciliation')
        candidates = db.scalars(select(Event).where(Event.event_type == 'insider_trade', Event.symbol == row['ticker_normalized']))
        for event in candidates:
            payload = json.loads(event.payload_json or '{}')
            provider = payload.get('raw') or {}
            if (payload.get('accession_number') == metadata['key'] or
                    str(payload.get('transaction_date') or provider.get('transactionDate') or '')[:10] == str(row['transaction_date'])):
                return held('Unmapped existing event requires reconciliation')
    filing_data = parsed['filing']
    with db.begin_nested():
        filing = SecForm4Filing(accession_number=metadata['key'], issuer_cik=filing_data['issuer_cik'],
            issuer_name=filing_data['issuer_name'], issuer_trading_symbol=filing_data['issuer_trading_symbol'],
            reporting_owner_cik=filing_data['reporting_owner_cik'], reporting_owner_name=filing_data['reporting_owner_name'],
            filing_date=date.fromisoformat(metadata['filing_date']), source_url=metadata['url'], document_hash=digest,
            raw_xml_text=source_text, raw_metadata_json=dumps(metadata), parser_status='parsed',
            parser_version='direct_sec_publication_v1', parsed_at=datetime.now(timezone.utc))
        db.add(filing)
        db.flush()
        for index, row in enumerate(rows, 1):
            transaction = InsiderTransactionNormalized(form4_filing_id=filing.id, **row)
            db.add(transaction)
            db.flush()
            market = row['transaction_code'] in {'P', 'S'} and not row['is_derivative']
            side = ('purchase' if row['transaction_code'] == 'P' else 'sale') if market else None
            payload = {**row, 'transaction_date': str(row['transaction_date']), 'filing_date': str(row['filing_date']),
                'external_id': 'sec_form4:' + row['normalized_hash'], 'symbol': row['ticker_normalized'],
                'insider_name': row['reporting_owner_name'], 'reporting_cik': row['reporting_owner_cik'],
                'is_market_trade': market, 'trade_type_canonical': side, 'normalized_transaction_id': transaction.id,
                'sec_verification': {'feed': 'sec_form4', 'accession': metadata['key'], 'url': metadata['url'],
                                     'sha256': digest, 'source_rows': [str(index)]}}
            # Daily index gives a filing date, not an intraday acceptance time.
            # Preserve both dates and do not represent transaction day as availability.
            filed = datetime.combine(row['filing_date'], datetime.min.time(), tzinfo=timezone.utc)
            amount = round(row['value']) if row['value'] is not None else None
            db.add(Event(event_type='insider_trade', ts=filed, event_date=filed, symbol=row['ticker_normalized'],
                source='sec_edgar', source_provider='sec_edgar', source_document_url=metadata['url'],
                source_filing_id=row['normalized_hash'], parser_version='direct_sec_publication_v1',
                data_source='insider', trade_type=side, transaction_type=row['transaction_code'],
                amount_min=amount, amount_max=amount, payload_json=dumps(payload)))
        db.flush()
    return {'status': 'published', 'inserted_events': len(rows), 'inserted_transactions': len(rows),
            'normalized_hashes': [row['normalized_hash'] for row in rows]}
