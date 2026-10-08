"""Normal Senate public-notice session and bounded PTR discovery.

No proxy, browser challenge bypass or FMP fallback. Denied sessions fail closed.
"""
import json
import re
import time
from datetime import datetime
from urllib.parse import urljoin, urlsplit
from uuid import UUID

from lxml import html
import requests

from app.clients.direct_sources import DirectSourceError

ORIGIN = 'https://efdsearch.senate.gov'
HOME = ORIGIN + '/search/home/'
SEARCH = ORIGIN + '/search/'
DATA = ORIGIN + '/search/report/data/'


def report_identity(url):
    parts = urlsplit(urljoin(ORIGIN, url))
    if parts.scheme != 'https' or parts.netloc != 'efdsearch.senate.gov' or parts.query or parts.fragment:
        raise DirectSourceError('Unrecognized official Senate report URL')
    match = re.fullmatch(r'/search/view/(ptr|paper)/([0-9a-f-]{36})/', parts.path)
    if not match or str(UUID(match[2])) != match[2]:
        raise DirectSourceError('Unrecognized official Senate report identity')
    return match[2], match[1], ORIGIN + parts.path


def parse_search_row(row, *, start, end):
    if not isinstance(row, list) or len(row) != 5 or not all(isinstance(x, str) for x in row):
        raise DirectSourceError('Senate search row does not have five text columns')
    first, last, filer_type, link, submitted = row
    anchors = html.fragment_fromstring(link, create_parent=True).xpath('.//a')
    if len(anchors) != 1 or not first.strip() or not last.strip():
        raise DirectSourceError('Senate search report link or filer identity is incomplete')
    key, kind, url = report_identity(anchors[0].get('href') or '')
    title = ' '.join(anchors[0].text_content().split())
    match = re.fullmatch(r'Periodic Transaction Report for (\d{2}/\d{2}/\d{4})(?: \(Amendment(?: \d+)?\))?', title)
    if not match:
        raise DirectSourceError('Unexpected Senate report title')
    report_date = datetime.strptime(match[1], '%m/%d/%Y').date()
    filing_date = datetime.strptime(submitted.strip(), '%m/%d/%Y').date()
    # A filer-entered report heading can differ from the portal's submission
    # date. Preserve both; only the submitted date determines discovery scope.
    if not start <= filing_date <= end:
        raise DirectSourceError(f'Senate report dates require review: filed={filing_date}, report={report_date}, window={start}..{end}')
    return {'key': key, 'filing_id': key, 'url': url, 'filing_date': filing_date.isoformat(),
        'report_date': report_date.isoformat(), 'member_name': ' '.join((first.strip(), last.strip())),
        'filer_type': filer_type.strip(), 'form': 'PTR', 'report_kind': kind,
        'report_title': title, 'amendment_flag': '(Amendment' in title}


class SenateEfdClient:
    def __init__(self, *, accept_notice=False, session=None):
        self.accept_notice = accept_notice
        self.session = session or requests.Session()
        self.session.headers.update({'User-Agent': 'Walnut Markets source research contact@walnutmarkets.com'})
        self.ready = False
        self.last_request = 0.0
        self.search_receipt = None
        self.search_pages = []

    def _request(self, method, url, *, data=None, accepted=(200,), referer=SEARCH):
        time.sleep(max(0, 0.5 - (time.monotonic() - self.last_request)))
        try:
            with self.session.request(method, url, data=data, headers={'Referer': referer},
                                      timeout=(10, 40), allow_redirects=False, stream=True) as response:
                self.last_request = time.monotonic()
                if response.status_code not in accepted:
                    # Stop the run on denial/rate limiting. No repeated challenge requests.
                    raise DirectSourceError(f'Source HTTP {response.status_code}: {url}')
                raw = bytearray()
                for chunk in response.iter_content(65536):
                    raw.extend(chunk)
                    if len(raw) > 10_000_000:
                        raise DirectSourceError('Senate response exceeds size limit')
                return bytes(raw), response.headers.get('Location')
        except requests.RequestException as exc:
            raise DirectSourceError(f'Source transport failed: {url}') from exc

    def open(self):
        if self.ready:
            return
        if not self.accept_notice:
            raise DirectSourceError('Senate public-notice acknowledgement is not configured')
        raw, _ = self._request('GET', HOME, referer=HOME)
        tokens = html.fromstring(raw).xpath('//input[@name="csrfmiddlewaretoken"]/@value')
        if len(tokens) != 1 or not tokens[0]:
            raise DirectSourceError('Senate notice form is unavailable')
        _, location = self._request('POST', HOME, data={'csrfmiddlewaretoken': tokens[0],
            'prohibition_agreement': '1'}, accepted=(302,), referer=HOME)
        if urljoin(HOME, location or '') != SEARCH or not self.session.cookies.get('csrftoken'):
            raise DirectSourceError('Senate notice session was not established')
        self.ready = True

    def discover(self, *, start, end, page_size=100, max_pages=20):
        if end < start or (end-start).days > 93 or not 1 <= page_size <= 100 or not 1 <= max_pages <= 20:
            raise ValueError('Invalid Senate discovery bounds')
        self.search_receipt = None
        self.search_pages = []
        self.open()
        rows, seen, expected_total, page_hashes = [], set(), None, []
        import hashlib
        for page in range(max_pages):
            payload = {'start': str(page * page_size), 'length': str(page_size),
                'report_types': '[11]', 'filer_types': '[]',
                'submitted_start_date': f'{start:%m/%d/%Y} 00:00:00',
                'submitted_end_date': f'{end:%m/%d/%Y} 23:59:59',
                'candidate_state': '', 'senator_state': '', 'office_id': '', 'first_name': '', 'last_name': '',
                'csrfmiddlewaretoken': self.session.cookies.get('csrftoken')}
            raw, _ = self._request('POST', DATA, data=payload)
            data = json.loads(raw)
            total = data.get('recordsFiltered')
            if type(total) is not int or total < 0 or not isinstance(data.get('data'), list):
                raise DirectSourceError('Senate search response lacks an explicit filtered total')
            if expected_total is not None and expected_total != total:
                raise DirectSourceError('Senate discovery changed during pagination; retry whole window')
            expected_total = total
            page_rows = data['data']
            if len(page_rows) > page_size or total > page_size * max_pages:
                raise DirectSourceError('Senate discovery exceeds bounded page budget')
            for row in page_rows:
                item = parse_search_row(row, start=start, end=end)
                if item['key'] in seen:
                    raise DirectSourceError('Senate discovery repeated a report across pages')
                seen.add(item['key']); rows.append(item)
            page_hashes.append(hashlib.sha256(raw).hexdigest())
            self.search_pages.append(raw)
            if len(rows) == total:
                self.search_receipt = {'start': start.isoformat(), 'end': end.isoformat(),
                    'reported_total': total, 'discovered': len(rows), 'page_sha256': page_hashes,
                    'complete': True, 'source': DATA}
                return rows
            if len(rows) > total or len(page_rows) != page_size:
                raise DirectSourceError('Senate discovery count differs from search total')
        raise DirectSourceError('Senate discovery page budget exhausted')

    def get(self, url):
        _, _, verified_url = report_identity(url)
        self.open()
        raw, _ = self._request('GET', verified_url)
        return raw
