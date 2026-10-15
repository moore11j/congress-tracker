"""Reviewed foreign filing forms and accounting bases, scoped to the exact issuer.

No foreign-currency conversion, generic 6-K acceptance or ADR EPS assumption.
"""
from copy import deepcopy

VERSION = 'sec_arm_usd_reporting_v1'
DOMESTIC_FORMS = frozenset({'10-K', '10-Q', '10-K/A', '10-Q/A'})
PROFILES = {
    'NVS': {'cik': '0001114448', 'currency': 'USD', 'taxonomy': 'ifrs-full',
        'forms': ['20-F', '20-F/A', '6-K', '6-K/A'],
        'reviewed_at': '2026-10-15', 'version': 'sec_nvs_ifrs_interim_v1'},
    'ARM': {'cik': '0001973239', 'currency': 'USD', 'taxonomy': 'us-gaap',
        'forms': ['20-F', '20-F/A', '6-K', '6-K/A'],
        'annual_proof_url': 'https://www.sec.gov/Archives/edgar/data/1973239/000197323926000097/arm-20260331.htm',
        'annual_proof_sha256': '95ea3f0d24a85deeda32984663de9dafebe29659e98344194a9d752ed2d2c601',
        'interim_proof_url': 'https://www.sec.gov/Archives/edgar/data/1973239/000197323926000114/arm-20260630.htm',
        'interim_proof_sha256': '0d280b51d3bc153599ff69979b582f5042d727caab9700b4613f72f1c1e1c1ad',
        'reviewed_at': '2026-10-14', 'version': VERSION},
}


def reviewed(symbol, cik):
    profile = PROFILES.get(symbol)
    if profile is None:
        return None
    if str(cik).zfill(10) != profile['cik']:
        raise ValueError('reviewed_reporting_cik_mismatch')
    return deepcopy(profile)


def forms_for(symbol, cik):
    profile = reviewed(symbol, cik)
    return frozenset(profile['forms']) if profile else DOMESTIC_FORMS


def periodic_document(symbol, cik, form, report, filename):
    profile = reviewed(symbol, cik)
    if profile is None:
        return form in DOMESTIC_FORMS
    if symbol == 'NVS':
        return (form in profile['forms'] and len(report) == 10
            and report[4:] in ('-03-31', '-06-30', '-09-30', '-12-31')
            and (form not in ('20-F', '20-F/A') or report[4:] == '-12-31'))
    # Arm also files event-only 6-Ks. Only its reviewed quarterly filing
    # convention may trigger an inline financial download.
    return (form in profile['forms'] and len(report) == 10
        and report[4:] in ('-03-31', '-06-30', '-09-30', '-12-31')
        and (form not in ('20-F', '20-F/A') or report[4:] == '-03-31')
        and filename == 'arm-' + report.replace('-', '') + '.htm')
