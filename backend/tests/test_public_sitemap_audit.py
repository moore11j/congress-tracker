import importlib.util
from pathlib import Path

spec = importlib.util.spec_from_file_location('sitemap_audit', Path(__file__).parents[1] / 'scripts' / 'audit_public_sitemaps.py')
audit = importlib.util.module_from_spec(spec)
spec.loader.exec_module(audit)


def test_homepage_bare_origin_is_self_referencing(monkeypatch):
    url = 'https://walnutmarkets.com/'
    monkeypatch.setattr(audit, 'fetch', lambda _: (200, url, {}, '<link rel="canonical" href="https://walnutmarkets.com">'))
    assert audit.audit_url(url, {})['issues'] == []


def test_distinct_paths_still_fail_canonical_check(monkeypatch):
    url = 'https://walnutmarkets.com/faq'
    monkeypatch.setattr(audit, 'fetch', lambda _: (200, url, {}, '<link rel="canonical" href="https://walnutmarkets.com/">'))
    assert audit.audit_url(url, {})['issues'] == ['canonical']


def test_duplicate_canonical_and_header_noindex_are_detected(monkeypatch):
    url = 'https://walnutmarkets.com/'
    body = '<link rel="canonical" href="https://walnutmarkets.com"><link rel="canonical" href="https://walnutmarkets.com/">'
    monkeypatch.setattr(audit, 'fetch', lambda _: (200, url, {'X-Robots-Tag': 'noindex'}, body))
    assert audit.audit_url(url, {})['issues'] == ['noindex', 'canonical']
