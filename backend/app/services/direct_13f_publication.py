"""Complete-original 13F projection shared by rehearsal and guarded publication."""
from datetime import date, datetime, timezone
from decimal import Decimal
import hashlib
import json

from sqlalchemy import select, func

from app.models import (Event, InstitutionalActivityEvent, InstitutionalFiling,
                        InstitutionalPosition, InstitutionalPositionChange)
from app.services.direct_feed_collection import parse_document
from app.services.direct_feed_store import dumps, reconcile_institutional
from app.services import institutional_activity as activity
from app.services.institutional_sec_snapshot import mapped_symbol, snapshot_digest


RECEIPT_KEY = '_walnut_direct_13f'


def _positions(db, filing):
    return list(db.scalars(select(InstitutionalPosition).where(InstitutionalPosition.filing_id == filing.id)))


def _positions_digest(db, filing):
    rows = [{key: getattr(row, key) for key in (
        'cik', 'report_year', 'report_quarter', 'filing_date', 'cusip', 'normalized_symbol',
        'shares', 'value_usd', 'put_call', 'portfolio_weight')} for row in _positions(db, filing)]
    return hashlib.sha256(dumps(sorted(rows, key=lambda r: (r['cusip'] or '', r['put_call'] or '', r['normalized_symbol'] or ''))).encode()).hexdigest()


def _receipt(filing):
    return json.loads(filing.raw_metadata_json or '{}').get(RECEIPT_KEY, {})


def _filing_digest(filing):
    fields = ('cik', 'accession_number', 'filing_date', 'report_year', 'report_quarter',
              'report_period_end', 'form_type', 'is_amendment', 'superseded_by', 'filing_url')
    return hashlib.sha256(dumps({key: getattr(filing, key) for key in fields}).encode()).hexdigest()


def _derived_state(db, filing, publish_since):
    if filing.filing_date < publish_since:
        return 'historical_no_alerts', None
    year, quarter = activity._previous_quarter(filing.report_year, filing.report_quarter)
    candidates = list(db.scalars(select(InstitutionalFiling).where(
        func.ltrim(InstitutionalFiling.cik, '0') == filing.cik.lstrip('0'), InstitutionalFiling.report_year == year,
        InstitutionalFiling.report_quarter == quarter)))
    if len(candidates) != 1:
        return 'waiting_complete_prior_quarter', None
    prior = candidates[0]
    proof = _receipt(prior)
    if (prior.cik != filing.cik or prior.is_amendment or prior.superseded_by or prior.filing_date > filing.filing_date
            or not proof.get('complete') or proof.get('filing_sha256') != _filing_digest(prior)
            or proof.get('positions_sha256') != _positions_digest(db, prior)):
        return 'waiting_verified_prior_quarter', None
    positions = _positions(db, prior) + _positions(db, filing)
    if any(not p.normalized_symbol for p in positions if not p.put_call):
        return 'waiting_equity_symbol_mapping', prior
    # A symbol rename with the same CUSIP must not leave the older symbol's
    # summary stale or create a false new/exit pair through other consumers.
    prior_symbols = {p.cusip: p.normalized_symbol for p in _positions(db, prior) if not p.put_call}
    if any(p.cusip in prior_symbols and prior_symbols[p.cusip] != p.normalized_symbol
           for p in _positions(db, filing) if not p.put_call):
        return 'waiting_symbol_transition', prior
    # A changed CUSIP for the same symbol may be a reorganization rather than
    # a sale followed by a purchase. Require explicit corporate-action treatment.
    def identities(rows):
        result = {}
        for position in rows:
            if not position.put_call:
                result.setdefault(position.normalized_symbol, set()).add(position.cusip)
        return result
    before, after = identities(_positions(db, prior)), identities(_positions(db, filing))
    if any(before[symbol] != after[symbol] for symbol in before.keys() & after.keys()):
        return 'waiting_security_transition', prior
    return 'ready', prior


def rehearse_new_13f(db, document, *, publish_since: date, identifier_documents=(), comparison_documents=()):
    bind = db.get_bind()
    if bind.dialect.name != 'sqlite' or bind.url.database != ':memory:':
        raise ValueError('Publication rehearsal requires in-memory SQLite')
    result = _project_new_13f(db, document, publish_since=publish_since,
        identifier_documents=identifier_documents, comparison_documents=comparison_documents)
    return {**result, 'status': 'rehearsed' if result['status'] == 'projected' else result['status'], 'production_writes': 0}


def _project_new_13f(db, document, *, publish_since: date, identifier_documents=(), comparison_documents=(), prepared_evidence=None):
    if db.new or db.dirty or db.deleted:
        raise ValueError('Rehearsal session has unrelated pending changes')
    raw, discovery = document['raw'], document['metadata']
    digest = hashlib.sha256(raw).hexdigest()
    if document['feed'] != 'sec_13f' or digest != document['content_hash']:
        raise ValueError('Wrong feed or source checksum')
    _, parsed, reasons = parse_document('sec_13f', raw, discovery)
    held = lambda reason: {'status': 'held', 'reason': reason, 'inserted_filings': 0,
                          'inserted_positions': 0, 'feed_events': 0, 'production_writes': 0}
    if reasons:
        return held('; '.join(reasons))
    metadata, rows = parsed['metadata'], parsed['positions']
    from app.services.direct_13f_evidence import nport_identifiers, value_consistency_issues
    value_issues = value_consistency_issues(parsed, comparison_documents, prepared=prepared_evidence)
    if value_issues:
        return {**held('Independent SEC holdings indicate a value-unit discrepancy; no automatic rescaling'),
                'value_issues': value_issues}
    identifiers = [row for source in identifier_documents
                   for row in nport_identifiers(source, available_by=date.fromisoformat(metadata['filing_date']), prepared=prepared_evidence)]
    if any(r.get('shareType') != 'SH' or (r.get('putCall') or '').upper() not in {'', 'PUT', 'CALL'} for r in rows):
        return held('Principal amounts or unknown option types require separate security semantics')
    classes = {}
    for row in rows:
        key = (row['cusip'].strip().upper(), (row.get('putCall') or '').upper())
        classes.setdefault(key, set()).add((row.get('titleOfClass') or '').strip().upper())
    if any(len(values) != 1 for values in classes.values()):
        return held('Conflicting security classes share an aggregate identity')
    reconciliation = reconcile_institutional(db, parsed)
    filing = db.get(InstitutionalFiling, reconciliation['filing_id']) if reconciliation['filing_id'] else None
    created = filing is None
    if filing is not None:
        if reconciliation['unmatched'] or reconciliation['ambiguous'] or reconciliation.get('existing_only_ids'):
            return held('Existing filing requires source reconciliation')
        proof = _receipt(filing)
        if not proof:
            return {'status': 'existing', 'inserted_filings': 0, 'inserted_positions': 0, 'feed_events': 0,
                    'derived_state': 'existing_pipeline_unchanged', 'production_writes': 0}
        if (proof.get('source_sha256') != digest or proof.get('filing_sha256') != _filing_digest(filing)
                or proof.get('positions_sha256') != _positions_digest(db, filing)):
            return held('Published source or position state has changed')
        if proof.get('publish_since') != publish_since.isoformat():
            return held('Publication boundary changed; explicit rebaseline required')
        if proof.get('derived_state') == 'published':
            state, prior = _derived_state(db, filing, publish_since)
            if (state != 'ready' or proof.get('prior_filing_id') != prior.id
                    or proof.get('prior_positions_sha256') != _positions_digest(db, prior)):
                return held('Derived prior-quarter state changed; reconciliation required')
            return {'status': 'existing', 'inserted_filings': 0, 'inserted_positions': 0,
                    'feed_events': 0, 'derived_state': 'published', 'production_writes': 0}
    else:
        # Different/missing accessions in the same holder-quarter need explicit
        # reconciliation, never another representation of the same report.
        for existing in db.scalars(select(InstitutionalFiling).where(
                InstitutionalFiling.report_year == metadata['report_year'],
                InstitutionalFiling.report_quarter == metadata['report_quarter'])):
            if existing.cik.lstrip('0') == metadata['cik'].lstrip('0'):
                return held('Existing holder-quarter accession requires reconciliation')
        for model in (InstitutionalPosition, InstitutionalPositionChange, InstitutionalActivityEvent):
            if db.scalar(select(model.id).where(func.ltrim(model.cik, '0') == metadata['cik'].lstrip('0'),
                    model.report_year == metadata['report_year'], model.report_quarter == metadata['report_quarter']).limit(1)):
                return held('Unlinked holder-quarter records require reconciliation')
        for event in db.scalars(select(Event).where(
                Event.source_provider == activity.INSTITUTIONAL_EVENT_SOURCE,
                func.ltrim(Event.member_bioguide_id, '0') == metadata['cik'].lstrip('0'))):
            payload = json.loads(event.payload_json or '{}')
            if (str(payload.get('report_year')) == str(metadata['report_year'])
                    and str(payload.get('report_quarter')) == str(metadata['report_quarter'])):
                return held('Unlinked holder-quarter feed event requires reconciliation')

    with db.begin_nested():
        inserted = 0
        if created:
            candidate = activity.InstitutionalFilingCandidate(cik=metadata['cik'], holder_name=metadata.get('name'),
                accession_number=metadata['key'], filing_date=date.fromisoformat(metadata['filing_date']),
                report_year=metadata['report_year'], report_quarter=metadata['report_quarter'],
                report_period_end=date.fromisoformat(metadata['report_period']), filing_url=metadata['url'],
                form_type='13F-HR', is_amendment=False, raw=metadata)
            activity.upsert_institutional_holder(db, candidate)
            filing, _ = activity.upsert_institutional_filing(db, candidate)
            mappings = {}
            cusips = {row['cusip'].strip().upper() for row in rows}
            for identifier in identifiers:
                if identifier['cusip'] in cusips:
                    mappings.setdefault(identifier['cusip'], set()).add(identifier['symbol'])
            for cusip, symbol in db.execute(select(InstitutionalPosition.cusip, InstitutionalPosition.normalized_symbol).where(
                    InstitutionalPosition.cusip.in_(cusips), InstitutionalPosition.normalized_symbol.is_not(None),
                    InstitutionalPosition.filing_date <= filing.filing_date).distinct()):
                mappings.setdefault(cusip, set()).add(symbol)
            total = Decimal(metadata['table_value_total_usd'])
            mapped = []
            for row in rows:
                cusip = row['cusip'].strip().upper()
                mapped.append({**row, 'cusip': cusip,
                    'symbol': mapped_symbol(cusip, mappings.get(cusip, set()), filing.report_year, filing.report_quarter),
                    'portfolioWeight': float(Decimal(str(row['valueUsd'])) / total * 100) if total else None})
            meta = json.loads(filing.raw_metadata_json or '{}')
            meta['_walnut_position_source'] = 'sec_edgar_reconciled'
            meta['_walnut_position_snapshot'] = {'version': 1, 'cik': filing.cik, 'year': filing.report_year,
                'quarter': filing.report_quarter, 'accession': filing.accession_number, 'sha256': snapshot_digest(rows),
                'sources': [{'accession': filing.accession_number, 'kind': 'ORIGINAL',
                    'filing_date': metadata['filing_date'], 'urls': sorted({r['sourceUrl'] for r in rows})}]}
            filing.raw_metadata_json = dumps(meta)
            result = activity.upsert_positions_for_filing(db, filing=filing, rows=mapped, reconciled_snapshot=True)
            if result['skipped_positions']:
                raise ValueError('Canonical adapter skipped validated SEC positions')
            db.flush()
            verified = reconcile_institutional(db, parsed)
            if verified['unmatched'] or verified['ambiguous'] or verified.get('existing_only_ids'):
                raise ValueError('Canonical aggregate does not match complete SEC table')
            inserted = result['inserted_positions']
        state, prior = _derived_state(db, filing, publish_since)
        meta = json.loads(filing.raw_metadata_json or '{}')
        identifier_evidence = ([row for row in identifiers if row['cusip'] in cusips] if created
                               else meta.get(RECEIPT_KEY, {}).get('identifier_evidence', []))
        meta[RECEIPT_KEY] = {'complete': True, 'source_sha256': digest,
            'filing_sha256': _filing_digest(filing),
            'positions_sha256': _positions_digest(db, filing), 'source_rows': len(rows),
            'publish_since': publish_since.isoformat(), 'derived_state': state}
        meta[RECEIPT_KEY]['identifier_evidence'] = identifier_evidence
        filing.raw_metadata_json = dumps(meta)
        metrics = {'changes': 0, 'summaries': 0, 'activity_events': 0, 'feed_events': 0}
        if state == 'ready':
            before_events = set(db.scalars(select(Event.id)))
            metrics = activity.process_filing_changes_and_events(db, filing, holder_only=True)
            db.flush()
            proof = {'feed': 'sec_13f', 'accession': filing.accession_number, 'url': metadata['url'],
                     'sha256': digest, 'prior_accession': prior.accession_number,
                     'prior_url': prior.filing_url, 'prior_sha256': _receipt(prior)['source_sha256']}
            published_at = datetime.now(timezone.utc)
            for event in db.scalars(select(Event).where(Event.source_provider == activity.INSTITUTIONAL_EVENT_SOURCE)):
                if event.id in before_events:
                    continue
                payload = json.loads(event.payload_json or '{}')
                # Cluster summaries can include other holders. This pair proves
                # the triggering holder only, never every cluster constituent.
                holder_event = payload.get('cik') == filing.cik
                payload['sec_verification'] = {**proof, 'scope': 'holder_pair' if holder_event else 'triggering_holder_pair'}
                event.ts = published_at
                payload['source_availability'] = {'date': published_at.date().isoformat(),
                    'basis': 'direct_publication', 'observed_at': published_at.isoformat()}
                event.payload_json = dumps(payload)
                if holder_event:
                    event.source_document_url = metadata['url']
            meta[RECEIPT_KEY]['derived_state'] = state = 'published'
            meta[RECEIPT_KEY]['prior_filing_id'] = prior.id
            meta[RECEIPT_KEY]['prior_positions_sha256'] = _positions_digest(db, prior)
            filing.raw_metadata_json = dumps(meta)
        db.flush()
    return {'status': 'projected' if created or state == 'published' else 'existing',
            'inserted_filings': int(created), 'inserted_positions': inserted,
            'derived_state': state, **metrics, 'production_writes': 0}
