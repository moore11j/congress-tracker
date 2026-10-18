"""Explicit conversion language with complete SEC source/consumer regressions."""
from datetime import date, datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
import gzip
import hashlib
import json
import socket
import pytest
from app.services import sec_tabular_reporting as reporting
from app.services import sec_inline_financials as inline, sec_financial_statements as statements
from app.services import sec_fundamentals as core

ROOT = Path(__file__).parent / 'fixtures/sec_currency_wording'
NOW = datetime(2026, 10, 18, 4, 30, tzinfo=timezone.utc)
CIK = '0001549802'


def originals():
    urls = {}
    for url, meta in json.loads((ROOT / 'sources.json').read_text()).items():
        raw = gzip.decompress((ROOT / meta['file']).read_bytes())
        assert len(raw) == meta['bytes'] and hashlib.sha256(raw).hexdigest() == meta['sha256']
        urls[url] = raw
    return (urls[f'https://data.sec.gov/api/xbrl/companyfacts/CIK{CIK}.json'],
            urls[f'https://data.sec.gov/submissions/CIK{CIK}.json'], CIK, urls)


class Client:
    issuer_document_urls = frozenset()
    def __init__(self, urls): self.urls, self.calls = urls, []
    def get(self, url): self.calls.append(url); return self.urls[url]


def declared(phrase, units=('CNY', 'USD')):
    raw = ('<html><p>The financial statements are prepared in accordance with U.S. GAAP.</p>'
           '<p>' + phrase + '</p></html>').encode()
    return reporting.declaration(raw, {'facts': [{'currency': c} for c in units]})


@pytest.mark.parametrize('phrase', [
    'The conversion of Renminbi (RMB) into US$ is for the convenience of the readers.',
    'The conversion of RMB amounts to USD is for the convenience of the reader.',
    'The conversion of Chinese yuan into U.S. dollars is for the convenience of the readers.',
    'Translations from RMB to USD are for the convenience of the readers.',
])
def test_explicit_direction_and_same_currency_alias(phrase):
    proof = declared(phrase)
    assert (proof['currency'], proof['translations'][0]['convenience_currency']) == ('CNY', 'USD')


@pytest.mark.parametrize('phrase', [
    'The conversion of Renminbi (USD) into US$ is for the convenience of the readers.',
    'The conversion of RMB into US$XYZ is for the convenience of the readers.',
    'No conversion of RMB into USD is provided for the convenience of the readers.',
    'There is not a conversion of RMB into USD for the convenience of the readers.',
    'A hypothetical conversion of RMB into USD is for the convenience of the readers.',
    'No translations from RMB to USD are provided for the convenience of the readers.',
    'RMB and USD amounts are for the convenience of the readers.',
    'The conversion of RMB into USD uses a market exchange rate.',
    'The conversion of RMB into USD is for the convenience of the readers. '
    'The conversion of USD into RMB is also for the convenience of the readers.',
])
def test_unproved_conflicting_or_negated_direction_is_held(phrase):
    with pytest.raises(reporting.TabularReportingError): declared(phrase)


def test_unrelated_third_currency_is_held():
    with pytest.raises(reporting.TabularReportingError, match='translation_columns_unproved'):
        declared('The conversion of RMB into USD is for the convenience of the readers.', ('CNY', 'USD', 'EUR'))


def test_complete_original_has_native_values_and_exact_repeat():
    facts, company, cik, urls = originals(); client = Client(urls)
    with patch.object(socket.socket, 'connect', side_effect=AssertionError('No network')):
        supplement = inline.acquire(facts, company, symbol='JD', cik=cik, observed_at=NOW, client=client)
        assert len(client.calls) == len(set(client.calls)) == 5
        proof = supplement['evidence']['table_reporting']
        assert proof['currency'] == 'CNY' and proof['native_observation_count'] == 22
        assert proof['source_observation_count'] == 33 and not proof['conflicts']
        checked = reporting.validate(supplement, company_raw=company, symbol='JD', cik=cik, available_by=date(2026,10,17))
        assert all(p['unit'] == 'CNY' for rows in checked['records'].values() for p in rows.values() if p)
        assets = checked['records']['current_assets'][None, date(2026,6,30)]['value']
        liabilities = checked['records']['current_liabilities'][None, date(2026,6,30)]['value']
        assert assets == 404602000000 and liabilities == 354736000000
        args = dict(symbol='JD', cik=cik, observed_at=NOW, filing_supplement=supplement)
        panel = statements.project(facts, company, **args)
        assert panel == statements.project(facts, company, **args)
        assert [r['revenue'] for r in panel['quarterly']] == [356660000000, 346401000000]
        assert [r['operatingCashFlow'] for r in panel['quarterly']] == [24409000000, 37718000000]
        assert all(r['netIncome'] is None and r['eps'] is None and r['freeCashFlow'] is None for r in panel['quarterly'])
        assert panel['status'] == 'partial' and panel['summary']['revenueTtm'] is None
        values = core.project_fundamentals(facts, company, **args)
        assert values['current_ratio'] == assets/liabilities == panel['summary']['currentRatio']
        row = SimpleNamespace(source='sec_edgar', fetched_at=NOW, payload_json=json.dumps(panel))
        assert statements.cached_row_payload(row, 'JD', now=NOW) == panel
        assert not checked['records']['equity'] and not checked['records']['net_income']


def test_changed_currency_original_and_missing_accounting_basis_are_held():
    facts, company, cik, urls = originals()
    supplement = inline.acquire(facts, company, symbol='JD', cik=cik, observed_at=NOW, client=Client(urls))
    args = supplement['table_args']; raw = supplement['table_original']
    assert b'Renminbi (RMB)' in raw
    with pytest.raises(reporting.TabularReportingError):
        reporting.normalize(raw.replace(b'Renminbi (RMB)', b'Renminbi (USD)'), **args)
    supplement['evidence']['table_reporting']['currency'] = 'USD'
    with pytest.raises(reporting.TabularReportingError, match='evidence_changed'):
        reporting.validate(supplement, company_raw=company, symbol='JD', cik=cik, available_by=date(2026,10,17))
    with pytest.raises(reporting.TabularReportingError, match='accounting_basis_unproved'):
        reporting.declaration(b'<p>GAAP. The conversion of RMB into USD is for the convenience of the readers.</p>', {'facts':[{'currency':'CNY'},{'currency':'USD'}]})


def test_cache_preparation_and_read_repeat_do_not_refetch(monkeypatch):
    from sqlalchemy import create_engine, select, func
    from sqlalchemy.orm import sessionmaker
    from app.models import InsightsSnapshot
    from app.services import sec_fundamentals_preparation as prep, sec_directory
    facts, company, cik, urls = originals(); client = Client(urls)
    monkeypatch.setattr(socket.socket, 'connect', lambda *a, **k: pytest.fail('Unexpected network'))
    monkeypatch.setenv('SEC_FUNDAMENTALS_WARMING_ENABLED', '1')
    monkeypatch.setattr(sec_directory, 'directory', lambda: {'JD': {'cik':cik}})
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None): return NOW
    engine = create_engine('sqlite://'); InsightsSnapshot.__table__.create(engine); factory = sessionmaker(bind=engine)
    with patch.object(prep,'SessionLocal',factory), patch.object(prep,'datetime',Clock), patch.object(prep,'DirectSourceClient',lambda:client):
        payload=prep.prepare('JD'); assert payload['values']=={'current_ratio':404602000000/354736000000}
        count=len(client.calls); assert prep.prepare('JD')==payload and len(client.calls)==count
    with patch.object(statements,'SessionLocal',factory), patch.object(statements,'datetime',Clock), patch.object(statements,'DirectSourceClient',lambda:client):
        panel=statements.prepared('JD'); assert panel['status']=='partial'
        count=len(client.calls); assert statements.prepared('JD')==panel and len(client.calls)==count
    with factory() as db: assert db.scalar(select(func.count()).select_from(InsightsSnapshot))==2
    engine.dispose()


def test_source_score_watchlist_transition_is_idempotent(monkeypatch):
    import test_sec_tagged_foreign_consumers as consumer
    monkeypatch.setattr(consumer,'originals',lambda symbol:originals())
    monkeypatch.setattr(consumer,'EXPECTED',{'JD':{'metrics':{'current_ratio':404602000000/354736000000}}})
    monkeypatch.setattr(consumer,'NOW',NOW)
    consumer.test_original_source_score_and_alert_recovery_repeat_crossing_rollback('JD','current_ratio',monkeypatch)
