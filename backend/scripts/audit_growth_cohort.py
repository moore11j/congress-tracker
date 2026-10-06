"""Read-only weekly audit of Walnut's fixed October 2026 growth cohort.

Run from the repository root; writes public evidence only, no analytics identities.
"""
import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from urllib.robotparser import RobotFileParser
import xml.etree.ElementTree as ET

from audit_public_sitemaps import HOSTS, audit_url, fetch

URLS = [
    'https://walnutmarkets.com/insider-trading-tracker',
    'https://walnutmarkets.com/stock-analysis-platform',
    *[f'https://app.walnutmarkets.com/ticker/{symbol}' for symbol in ('NVDA', 'ANET', 'NBIS', 'ALV')],
    'https://app.walnutmarkets.com/departments/nasa',
    'https://app.walnutmarkets.com/departments/department-of-energy',
    'https://walnutmarkets.com/research/who-is-buying-nvidia-stock-in-the-latest-13f-filings',
    'https://walnutmarkets.com/research/nbis-vs-crwv-ai-neoclouds',
]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    robots, inventory, sitemap_status = {}, {}, []
    for host in HOSTS:
        status, _, _, body = fetch(host + '/robots.txt')
        if status != 200:
            raise RuntimeError(f'Robots unavailable: {host}: {status}')
        rules = RobotFileParser()
        rules.parse(body.splitlines())
        robots[host] = rules
    queue = [url for rules in robots.values() for url in (rules.site_maps() or [])]
    seen = set()
    while queue:
        url = queue.pop(0)
        if url in seen:
            continue
        if not any(url.startswith(host + '/') for host in HOSTS):
            raise ValueError('Unexpected sitemap host')
        seen.add(url)
        status, final, _, body = fetch(url)
        if status != 200:
            raise RuntimeError(f'Sitemap unavailable: {url}: {status}')
        tree = ET.fromstring(body)
        sitemap_status.append({'url': url, 'status': status, 'final': final})
        if tree.tag.endswith('sitemapindex'):
            queue.extend(n.text for n in tree.findall('.//{*}loc'))
        else:
            for node in tree.findall('{*}url'):
                inventory.setdefault(node.findtext('{*}loc'), []).append({'sitemap': url, 'lastmod': node.findtext('{*}lastmod')})
    results = []
    for url in URLS:
        result = audit_url(url, robots)
        result['sitemaps'] = inventory.get(url, [])
        if not result['sitemaps']:
            result['issues'].append('missing_from_sitemaps')
        results.append(result)
        print(json.dumps(result), flush=True)
    report = {'checked_at': datetime.now(timezone.utc).isoformat(), 'sitemaps': sitemap_status, 'pages': results,
              'scope': 'HTTP, robots, canonical and sitemap checks; not Google indexing or browser-render verification.'}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + '\n', encoding='utf-8')
    return int(any(result['issues'] for result in results))


if __name__ == '__main__':
    raise SystemExit(main())
