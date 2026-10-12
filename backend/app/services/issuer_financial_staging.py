"""Prepare reviewed financials from retained SEC originals, with no new transport."""
from datetime import datetime, timezone
import hashlib
import json
import re

from app.models import InsightsSnapshot, Security
from app.services.direct_feed_store import dumps
from app.services.issuer_release_financials import AZO_URL, IssuerFinancialError, prepare_autozone_annual
from app.services.sec_earnings_store import read_staged_earnings
from app.services.issuer_financial_registry import PROFILES, key_for

VERSION = 'issuer_financial_staging_v1'
ACCESSION = '0001171843-26-006159'
KEY = 'issuer-financials:AZO:'+ACCESSION+':v1'


def project_staged_financials(db, document_id, *, symbol, observed_at=None):
    """Read and recompute retained evidence without any database writes."""
    observed_at = observed_at or datetime.now(timezone.utc)
    if observed_at.tzinfo is None:
        raise ValueError('Financial preparation observation requires timezone')
    if symbol not in PROFILES:
        raise ValueError('Financial preparation security mismatch')
    profile = PROFILES[symbol]
    accession = profile['accession']
    document, parsed, raw = read_staged_earnings(db, document_id)
    if (parsed['symbol'], parsed['cik'], parsed['accession_number']) != (symbol,profile['cik'],accession):
        raise ValueError('Unreviewed financial filing identity')
    if parsed['availability_status'] != 'verified_sec_acceptance':
        raise ValueError('Financial source availability unverified')
    not_before = datetime.fromisoformat(parsed['acceptance_evidence']['observation_not_before'])
    if not_before.tzinfo is None or observed_at < not_before:
        raise ValueError('Financial source observation precedes acceptance evidence')
    source = {'document_id':document.id, 'submission_url':parsed['source_url'],
        'submission_sha256':parsed['source_sha256'], 'company_sha256':parsed['company_sha256'],
        'index_sha256':parsed['index_sha256'],
        'metadata_sha256':hashlib.sha256(document.metadata_json.encode()).hexdigest(),
        'accession_number':accession, 'canonical_release_key':parsed['canonical_key'],
        'filing_date':parsed['filing_date'], 'sec_accepted_at':parsed['sec_accepted_at'],
        'acceptance_evidence':parsed['acceptance_evidence']}
    result = {'staging_version':VERSION, 'symbol':symbol, 'source':'issuer_sec_exhibit',
        'status':'held', 'values':{}, 'publication_eligible':False,
        'observed_at':observed_at.isoformat(), 'source_bundle':source}
    if parsed['status'] != 'prepared':
        result['reason'] = parsed.get('reason') or 'earnings_release_held'
    else:
        release = parsed['release']
        if release['url'] != profile['url'] or release['filename'] != profile['filename']:
            raise ValueError('Reviewed financial exhibit mismatch')
        bodies=[]
        for block in re.findall(r'<DOCUMENT>(.*?)</DOCUMENT>',raw.decode('utf-8'),re.S|re.I):
            names=re.findall(r'^<FILENAME>([^\r\n]+)',block,re.M|re.I)
            if names==[release['filename']]:
                bodies.extend(re.findall(r'<TEXT>(.*?)</TEXT>',block,re.S|re.I))
        if len(bodies)!=1:
            raise ValueError('Financial exhibit missing or ambiguous')
        exhibit=bodies[0].encode()
        if hashlib.sha256(exhibit).hexdigest()!=release['exhibit_sha256']:
            raise ValueError('Financial exhibit hash mismatch')
        source['exhibit_sha256']=release['exhibit_sha256']
        source['release_text_sha256']=release['text_sha256']
        try:
            if symbol == 'ACN':
                from app.services.issuer_accenture_financials import prepare_accenture_annual
                parser = prepare_accenture_annual
            else:
                parser = prepare_autozone_annual
            result.update(parser(exhibit,source_url=profile['url'],observed_at=observed_at))
        except IssuerFinancialError as exc:
            # Financial format holds do not discard an otherwise valid release.
            result['reason']=str(exc)
    return result


def prepare_staged_financials(db, document_id, *, security_id, observed_at=None):
    """Caller holds issuer lock and controls commit. Never creates canonical data."""
    observed_at = observed_at or datetime.now(timezone.utc)
    security = db.get(Security, security_id)
    if security is None:
        raise ValueError('Financial preparation security mismatch')
    result = project_staged_financials(db, document_id, symbol=security.symbol, observed_at=observed_at)
    key = key_for(security.symbol)
    row=db.get(InsightsSnapshot,key,with_for_update=True)
    if row is not None:
        prior=json.loads(row.payload_json)
        old={k:v for k,v in prior.items() if k!='observed_at'}
        new={k:v for k,v in result.items() if k!='observed_at'}
        if row.source==result['source'] and old==new:
            return prior
    if row is None:
        row=InsightsSnapshot(kind=key,source=result['source'],fetched_at=observed_at,payload_json='{}')
        db.add(row)
    row.source=result['source']; row.fetched_at=observed_at; row.payload_json=dumps(result)
    db.flush()
    return result
