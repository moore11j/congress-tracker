"""Reviewed issuer identities; importing this registry has no database/parser cost."""
PROFILES = {
    'AZO': {'cik':'0000866787', 'accession':'0001171843-26-006159',
        'url':'https://www.sec.gov/Archives/edgar/data/866787/000117184326006159/exh_991.htm',
        'filename':'exh_991.htm'},
    'ACN': {'cik':'0001467373', 'accession':'0001467373-26-000037',
        'url':'https://www.sec.gov/Archives/edgar/data/1467373/000146737326000037/q4fy26earnings8-kexhibit.htm',
        'filename':'q4fy26earnings8-kexhibit.htm'},
}


def key_for(symbol):
    return 'issuer-financials:'+symbol+':'+PROFILES[symbol]['accession']+':v1'
