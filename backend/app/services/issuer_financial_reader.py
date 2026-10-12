"""Use a reviewed, retained issuer statement when it advances SEC financials."""
from copy import deepcopy
from datetime import date, datetime
import hashlib
import json

from sqlalchemy import text
from app.db import SessionLocal
from app.models import InsightsSnapshot
from app.services.issuer_financial_staging import ACCESSION, KEY, VERSION, project_staged_financials
from app.services.sec_earnings_materials import discover_earnings_filings

METHOD = 'reviewed_issuer_financial_reader_v1'
IDENTITY_FIELDS = frozenset({'symbol','provider','company_name','sector','industry','country',
    'exchange','fetched_at','status','error'})


def supplement(values, company_raw, *, observed_at):
    if values.get('symbol') != 'AZO':
        return values
    result = deepcopy(values)
    evidence = json.loads(result['source_evidence_json'])
    review = {'methodology': METHOD, 'status': 'held'}
    evidence['issuer_release_preparation'] = review
    try:
        if evidence.get('cik') != '0000866787':
            raise ValueError('Issuer financial CIK mismatch')
        filings = discover_earnings_filings(company_raw, symbol='AZO', cik='0000866787', limit=1)
        if not filings or filings[0]['accession_number'] != ACCESSION:
            raise ValueError('Latest earnings filing is not the reviewed financial source')
        with SessionLocal() as db:
            if db.get_bind().dialect.name == 'postgresql':
                db.execute(text('SET TRANSACTION READ ONLY'))
                db.execute(text("SET LOCAL statement_timeout='5s'"))
            row = db.get(InsightsSnapshot, KEY)
            if row is None or row.source != 'issuer_sec_exhibit':
                raise ValueError('Issuer financial preparation missing')
            prepared = json.loads(row.payload_json)
            if prepared.get('staging_version') != VERSION or prepared.get('status') != 'prepared':
                raise ValueError('Issuer financial preparation held or outdated')
            original_observation = datetime.fromisoformat(prepared['observed_at'])
            if original_observation.tzinfo is None or original_observation > observed_at:
                raise ValueError('Issuer financial observation is in the future')
            # Recompute from the retained originals; this function never writes.
            checked = project_staged_financials(db, prepared['source_bundle']['document_id'],
                symbol='AZO', observed_at=original_observation)
            if prepared != checked:
                raise ValueError('Issuer financial receipt differs from retained originals')
        end = date.fromisoformat(prepared['period_end'])
        if values.get('period_date') and date.fromisoformat(str(values['period_date'])) >= end:
            review.update(status='not_needed', reason='SEC financial period is equal or newer')
        else:
            # Never relabel older balance sheet/cash-flow/market metrics as this period.
            result = {k:v for k,v in result.items() if k in IDENTITY_FIELDS}
            result.update(prepared['values'], period_date=end, status='ok', error=None)
            review.update(status='used', receipt=prepared,
                rechecked_company_sha256=hashlib.sha256(company_raw).hexdigest(),
                rechecked_at=observed_at.isoformat(), used_fields=sorted(prepared['values']),
                superseded_sec_period=str(values.get('period_date')) if values.get('period_date') else None)
            def facts(which):
                prefix = 'prior_' if which == 'prior' else ''
                start, stop = prepared[prefix+'period_start'], prepared[prefix+'period_end']
                return {name:{'value':raw[which], 'start':start, 'end':stop,
                    'inputs':[{'value':raw[which], 'start':start, 'end':stop,
                        'unit':raw['unit'], 'accession':ACCESSION,
                        'source_url':prepared['source_url'], 'source_sha256':prepared['source_sha256']}]}
                    for name,raw in prepared['inputs'].items()}
            evidence = {'methodology': METHOD, 'cik':'0000866787',
                'company':evidence.get('company'), 'facts':evidence.get('facts'),
                'available_by':evidence.get('available_by'),
                'availability_basis':prepared['availability_basis'],
                'period_start':prepared['period_start'], 'period_end':prepared['period_end'],
                'current':facts('current'), 'prior':facts('prior'), 'instant':{},
                'calculations':prepared['calculations'], 'issuer_release_preparation':review,
                'limitations':prepared['limitations']+[
                    'Older SEC balance-sheet and cash-flow fields are not mixed into this newer annual period.']}
    except (ValueError, KeyError, TypeError) as exc:
        review['reason'] = str(exc)
    result['source_evidence_json'] = json.dumps(evidence, sort_keys=True)
    return result
