"""Audit public Walnut sitemap URLs without authentication or provider API calls.

Run from the repository root:
    python backend/scripts/audit_public_sitemaps.py --output backend/artifacts/sitemap-audit-new
The JSONL report is flushed per URL. Exit 1 indicates findings; 2 a sitemap error.
"""
from __future__ import annotations

import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from datetime import datetime, timezone
from html.parser import HTMLParser
import json
import time
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import urlsplit
from urllib.request import Request, urlopen
from urllib.robotparser import RobotFileParser
import xml.etree.ElementTree as ET

HOSTS = ('https://walnutmarkets.com', 'https://app.walnutmarkets.com')
USER_AGENT = 'Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)'


class MetadataParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.robots = []
        self.canonicals = []

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if tag == 'meta' and attrs.get('name', '').lower() in ('robots', 'googlebot'):
            self.robots.append(attrs.get('content', ''))
        if tag == 'link' and attrs.get('rel') == 'canonical':
            self.canonicals.append(attrs.get('href'))


def fetch(url):
    try:
        response = urlopen(Request(url, headers={'User-Agent': USER_AGENT}), timeout=45)
    except HTTPError as error:
        response = error
    with response:
        return response.status, response.url, response.headers, response.read().decode('utf-8', errors='replace')


def audit_url(url, robots):
    try:
        status, final, headers, body = fetch(url)
        parser = MetadataParser()
        parser.feed(body)
        directives = parser.robots + [headers.get('X-Robots-Tag', '')]
        issues = []
        if status != 200:
            issues.append(f'http_{status}')
        if final != url:
            issues.append('redirect')
        if any('noindex' in value.lower() or value.lower().strip() == 'none' for value in directives):
            issues.append('noindex')
        # A bare origin and origin + '/' identify the same homepage URL.
        # Keep every other path/query distinction and require exactly one tag.
        def comparable(value):
            parts = urlsplit(value or '')
            return parts._replace(path=parts.path or '/').geturl()
        if [comparable(value) for value in parser.canonicals] != [comparable(url)]:
            issues.append('canonical')
        parts = urlsplit(url)
        robot = robots.get(f'{parts.scheme}://{parts.netloc}')
        if robot and not robot.can_fetch('Googlebot', url):
            issues.append('robots_block')
        return dict(url=url, status=status, final=final, canonical=parser.canonicals,
                    robots=parser.robots, xrobots=headers.get('X-Robots-Tag', ''), issues=issues)
    except Exception as error:
        return dict(url=url, issues=['fetch_error'], error=str(error))


def main():
    args = argparse.ArgumentParser(description=__doc__)
    args.add_argument('--output', type=Path, required=True)
    args.add_argument('--workers', type=int, default=2, choices=range(1, 4))
    args.add_argument('--delay', type=float, default=0.5, help='Delay after each page per worker')
    args.add_argument('--resume', action='store_true', help='Keep successful checks and retry unfinished/failed URLs')
    options = args.parse_args()
    options.output.mkdir(parents=True, exist_ok=True)
    robots, queue, maps, urls = {}, [], {}, {}
    try:
        for host in HOSTS:
            status, _, _, body = fetch(host + '/robots.txt')
            if status != 200:
                raise RuntimeError(f'{host}/robots.txt returned {status}')
            robot = RobotFileParser()
            robot.parse(body.splitlines())
            robots[host] = robot
            queue.extend(robot.site_maps() or [])
        while queue:
            url = queue.pop(0)
            if url in maps:
                continue
            if not any(url.startswith(host + '/') for host in HOSTS):
                raise RuntimeError(f'Unexpected sitemap origin: {url}')
            status, final, _, body = fetch(url)
            if status != 200:
                raise RuntimeError(f'{url} returned {status}')
            tree = ET.fromstring(body)
            locations = [node.text for node in tree.findall('.//{*}loc')]
            kind = tree.tag.split('}')[-1]
            maps[url] = dict(status=status, final=final, kind=kind, count=len(locations))
            if kind == 'sitemapindex':
                queue.extend(locations)
            elif kind == 'urlset':
                for location in locations:
                    if not any(location.startswith(host + '/') for host in HOSTS):
                        raise RuntimeError(f'Unexpected page origin: {location}')
                    urls.setdefault(location, []).append(url)
            else:
                raise RuntimeError(f'Unexpected XML type {kind} at {url}')
        (options.output / 'inventory.json').write_text(json.dumps(dict(maps=maps, urls=urls), indent=2), encoding='utf-8')
    except Exception as error:
        print(json.dumps(dict(error=str(error))), flush=True)
        return 2
    print(json.dumps(dict(sitemaps=len(maps), unique_urls=len(urls))), flush=True)
    report_path = options.output / 'pages.jsonl'
    results = {}
    if options.resume and report_path.exists():
        for line in report_path.read_text(encoding='utf-8').splitlines():
            row = json.loads(line)
            if row['url'] in urls:
                results[row['url']] = row
    remaining = iter(sorted(url for url in urls if url not in results or results[url]['issues']))
    def check(url):
        result = audit_url(url, robots)
        result['checked_at'] = datetime.now(timezone.utc).isoformat()
        time.sleep(max(0, options.delay))
        return result
    consecutive_errors = 0
    stopped = False
    with report_path.open('a' if options.resume else 'w', encoding='utf-8') as report, ThreadPoolExecutor(max_workers=options.workers) as pool:
        pending = {pool.submit(check, url) for url in [next(remaining, None) for _ in range(options.workers)] if url}
        while pending:
            completed, pending = wait(pending, return_when=FIRST_COMPLETED)
            for future in completed:
                result = future.result()
                results[result['url']] = result
                report.write(json.dumps(result) + '\n')
                report.flush()
                failure = result.get('status') != 200 or not result.get('canonical')
                consecutive_errors = consecutive_errors + 1 if failure else 0
                if result['issues']:
                    print(json.dumps(result), flush=True)
                elif len(results) % 100 == 0:
                    print(f'Checked {len(results)} / {len(urls)}', flush=True)
                if consecutive_errors >= 3:
                    stopped = True
                if not stopped:
                    next_url = next(remaining, None)
                    if next_url:
                        pending.add(pool.submit(check, next_url))
    rows = list(results.values())
    summary = dict(checked=len(rows), total=len(urls), stopped_for_errors=stopped,
                   issues=dict(Counter(issue for row in rows for issue in row['issues'])),
                   failed=[row for row in rows if row['issues']])
    (options.output / 'summary.json').write_text(json.dumps(summary, indent=2), encoding='utf-8')
    print(json.dumps(dict(checked=summary['checked'], issues=summary['issues'])), flush=True)
    return 2 if stopped else int(bool(summary['failed']))


if __name__ == '__main__':
    raise SystemExit(main())
