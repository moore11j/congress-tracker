"""Reproducible static FMP references; no imports, credentials, DB or network.

This inventory is not a runtime call graph, live usage report or migration score.
Literal helpers, optional paths and registry defaults are included. Dynamic DB
endpoint overrides require a separate configuration/runtime acceptance check.
"""
from __future__ import annotations

import argparse
import ast
import hashlib
import json
import re
from pathlib import Path

GROUPS = {'Congress disclosures': 'house-latest senate-latest', 'Insider trades': 'insider-trading/latest insider-trading/search', 'Institutional filings and analytics': 'institutional-ownership/latest institutional-ownership/extract institutional-ownership/dates institutional-ownership/symbol-positions-summary institutional-ownership/extract-analytics/holder institutional-ownership/holder-performance-summary institutional-ownership/holder-industry-breakdown institutional-ownership/industry-summary shares-float institutional-holder/latest institutional-holdings/latest', 'Prices and market snapshots': 'historical-price-eod/light historical-price-eod/full historical-chart/1min batch-quote batch-index-quotes batch-commodity-quotes', 'Corporate actions': 'dividends splits', 'Company and security reference': 'profile search-symbol search-cik company-screener market-capitalization market-capitalization-batch', 'Index membership': 'sp500-constituent nasdaq-constituent', 'Statements and calculated fundamentals': 'income-statement balance-sheet-statement cash-flow-statement income-statement-ttm balance-sheet-statement-ttm cash-flow-statement-ttm income-statement-growth balance-sheet-statement-growth cash-flow-statement-growth ratios ratios-ttm key-metrics-ttm', 'Earnings and forecasts': 'earnings analyst-estimates', 'Valuation': 'custom-discounted-cash-flow', 'Analysts': 'grades grades-consensus grades-historical price-target-consensus price-target-summary price-target-news upgrades-downgrades-consensus-bulk price-target-summary-bulk', 'Calendars': 'economic-calendar earnings-calendar dividends-calendar ipos-calendar splits-calendar', 'News and press releases': 'news/general-latest news/stock-latest news/stock news/press-releases news/crypto-latest news/forex-latest', 'SEC filing search': 'sec-filings-search/symbol sec-filings-company-search/symbol', 'Earnings transcripts': 'earning-call-transcript-dates earning-call-transcript', 'Macro and sectors': 'economic-indicators treasury-rates sector-performance-snapshot'}
CATALOG = {endpoint: group for group, values in GROUPS.items() for endpoint in values.split()}
REQUEST_HELPERS = {'_request_stable_rows', '_request_rows', '_request_payload',
                   '_request_payload_with_status', 'request_fmp_json'}
MARKERS = ('FMP_BASE', 'financialmodelingprep', 'app.clients.fmp', 'fmp_client import')


class Scan(ast.NodeVisitor):
    def __init__(self, path):
        self.path = path
        self.functions = []
        self.references = {}
        self.transport_sites = []

    def record(self, endpoint, node, kind):
        if endpoint not in CATALOG and not endpoint.startswith('/api/v3/'):
            group = 'Unclassified: review required'
        else:
            group = CATALOG.get(endpoint, 'Legacy company and security reference')
        ref = dict(file=self.path, line=node.lineno,
                   function='.'.join(self.functions) or 'module', kind=kind)
        key = (endpoint, node.lineno, ref['function'])
        self.references[key] = dict(endpoint=endpoint, group=group, **ref)

    def visit_FunctionDef(self, node):
        self.functions.append(node.name)
        self.generic_visit(node)
        self.functions.pop()

    visit_AsyncFunctionDef = visit_FunctionDef

    def visit_Constant(self, node):
        if not isinstance(node.value, str):
            return
        value = node.value
        if value in CATALOG:
            self.record(value, node, 'catalog_literal')
        elif value.startswith('https://financialmodelingprep.com/'):
            path = value.removeprefix('https://financialmodelingprep.com').split('?', 1)[0]
            if path.startswith('/stable/') and len(path) > len('/stable/'):
                self.record(path.removeprefix('/stable/'), node, 'provider_url_literal')
            elif path.startswith('/api/v3/'):
                if path.endswith('/profile/'):
                    path += '{symbol}'
                self.record(path, node, 'legacy_url_literal')
        elif value.startswith('/'):
            candidate = value.lstrip('/').split('?', 1)[0]
            if candidate in CATALOG:
                self.record(candidate, node, 'path_literal')

    def visit_Call(self, node):
        func = node.func
        name = func.id if isinstance(func, ast.Name) else func.attr if isinstance(func, ast.Attribute) else ''
        if name in REQUEST_HELPERS and node.args and isinstance(node.args[0], ast.Constant):
            value = node.args[0].value
            if isinstance(value, str) and re.fullmatch(r'[a-z][a-z0-9-]*(?:/[a-z0-9-]+)*', value):
                self.record(value, node.args[0], 'literal_request_argument')
        if (isinstance(func, ast.Attribute) and isinstance(func.value, ast.Name)
                and func.value.id in {'requests', 'httpx'} and name in {'get', 'post', 'request'}):
            # Some modules also fetch public sources. Do not label every HTTP
            # call FMP merely because the module contains an FMP import.
            self.transport_sites.append(dict(file=self.path, line=node.lineno,
                function='.'.join(self.functions) or 'module', client=func.value.id,
                method=name, provider='requires_call_path_review'))
        self.generic_visit(node)


def audit(root):
    root = Path(root).resolve()
    application = root/'backend/app'
    if not application.is_dir():
        raise ValueError('Expected repository root containing backend/app')
    references, transports, hashes = [], [], {}
    for path in sorted(application.rglob('*.py')):
        source = path.read_text(encoding='utf-8-sig').replace('\r\n', '\n')
        if not any(marker in source for marker in MARKERS):
            continue
        relative = path.relative_to(root).as_posix()
        hashes[relative] = hashlib.sha256(source.encode()).hexdigest()
        scan = Scan(relative)
        scan.visit(ast.parse(source, filename=relative))
        references.extend(scan.references.values())
        transports.extend(scan.transport_sites)
    endpoints = []
    for endpoint in sorted({r['endpoint'] for r in references}):
        refs = sorted((r for r in references if r['endpoint'] == endpoint), key=lambda r:(r['file'],r['line']))
        endpoints.append(dict(endpoint=endpoint, group=refs[0]['group'],
            source_references=[{k:v for k,v in r.items() if k not in {'endpoint','group'}} for r in refs]))
    return dict(schema_version=1, scope='Static references only; includes defaults, optional and unused helpers; does not prove runtime traffic or replacement readiness.',
        known_limitations=['Computed endpoint expressions and database overrides are not fully resolved.',
            'Catalog literals can be documentation/configuration references, not executed calls.',
            'HTTP sites include non-FMP requests and require call-path review.',
            'Primary checkout, merged source and installed runtime can differ.'],
        endpoint_count=len(endpoints), referenced_module_count=len(hashes),
        unclassified_endpoints=[e['endpoint'] for e in endpoints if e['group'].startswith('Unclassified')],
        catalog_without_reference=sorted(set(CATALOG)-{r['endpoint'] for r in references}),
        source_sha256=hashes, endpoints=endpoints, http_transport_sites=transports)


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path(__file__).resolve().parents[3])
    parser.add_argument('--output', type=Path, required=True)
    args=parser.parse_args(argv)
    payload=audit(args.root)
    args.output.write_text(json.dumps(payload, indent=2, sort_keys=True)+'\n', encoding='utf-8')
    print(json.dumps({k:payload[k] for k in ('endpoint_count','referenced_module_count','unclassified_endpoints','catalog_without_reference')}))
    return 0


if __name__=='__main__':
    raise SystemExit(main())
