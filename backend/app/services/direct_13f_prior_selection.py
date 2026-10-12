"""Pure prior-discovery receipt selection shared by ORM and idle preflight."""
from datetime import datetime, timedelta, timezone
import hashlib
import json


def normalize_candidates(candidates):
    if len(candidates) > 20000:
        raise ValueError('Prior candidate window exceeds reviewed filing bound')
    return [(document_id, source_hash, metadata_json,
             json.loads(receipt) if isinstance(receipt, str) else receipt)
            for document_id, source_hash, metadata_json, receipt in candidates]


def select_priors(candidates, priors, *, now, start, limit, recheck_hours):
    cutoff = now - timedelta(hours=recheck_hours)
    eligible = []
    for document_id, source_hash, metadata_json, receipt in candidates:
        priority, checked = 0, datetime.min.replace(tzinfo=timezone.utc)
        if receipt and (receipt.get('current_source_hash'), receipt.get('current_metadata_hash')) == (
            source_hash, hashlib.sha256(metadata_json.encode()).hexdigest()):
            checked = datetime.fromisoformat(receipt['checked_at'])
            if receipt.get('status') == 'collected':
                prior = priors.get(receipt['prior_document_id'])
                valid = prior and prior[0] == 'parsed' and prior[1] == receipt.get('content_hash') and (
                    hashlib.sha256(prior[2].encode()).hexdigest() == receipt.get('prior_metadata_hash'))
                if valid and checked >= now - timedelta(days=7):
                    continue
                priority = 1 if valid else 0
            else:
                if checked >= cutoff:
                    continue
                priority = 2
        eligible.append((priority, checked, document_id))
    eligible.sort()
    return {'document_ids': [item[2] for item in eligible[:limit]], 'eligible': len(eligible), 'candidates': len(candidates),
            'cooling_down': len(candidates) - len(eligible), 'since': start.isoformat()}
