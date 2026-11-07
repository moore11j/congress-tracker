"""Identity-only evidence when SEC financial qualification fails.

No statement amount, period, currency assumption or market value survives.
Only the free provider may supplement this base with its own normalized data.
"""
import hashlib
import json

from app.services.sec_fundamentals import SecFundamentalsError

VERSION = 'independent_fundamental_metrics_v1'


class IndependentMetricsEligible(SecFundamentalsError):
    def __init__(self, *, symbol, cik, company_raw, facts_raw, observed_at, reason):
        from app.services.sec_directory import parse_company
        cik = str(cik).zfill(10)
        if not cik.isdigit() or not 0 < int(cik) < 10**10:
            raise SecFundamentalsError('Invalid independent issuer identity')
        metadata = parse_company(json.loads(company_raw), cik=cik, symbol=symbol)
        if facts_raw is not None:
            facts = json.loads(facts_raw)
            if not isinstance(facts, dict) or str(facts.get('cik', '')).zfill(10) != cik:
                raise SecFundamentalsError('Independent companyfacts identity mismatch')
        evidence = {
            'methodology': VERSION, 'cik': cik, 'financial_status': 'unavailable',
            'financial_error': reason, 'identity_observed_at': observed_at.isoformat(),
            'company': {'url': f'https://data.sec.gov/submissions/CIK{cik}.json',
                        'sha256': hashlib.sha256(company_raw).hexdigest()},
            'current': {}, 'prior': {}, 'instant': {}, 'calculations': {},
            'independent_metrics': VERSION,
            'limitations': ['SEC identity verified; no SEC financial amounts or periods admitted.'],
        }
        if facts_raw is not None:
            evidence['facts'] = {'url': f'https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json',
                                 'sha256': hashlib.sha256(facts_raw).hexdigest()}
        self.values = {'symbol': symbol, 'provider': 'sec_edgar',
            'company_name': metadata['name'], 'exchange': metadata['exchange'],
            'fetched_at': observed_at, 'period_date': None, 'status': 'ok', 'error': None,
            'source_evidence_json': json.dumps(evidence, sort_keys=True)}
        super().__init__(reason)
