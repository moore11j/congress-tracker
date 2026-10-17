"""Reporting currency ambiguity must not become current ratios or USD panels."""
from types import SimpleNamespace
import json
import pytest
from app.services import sec_tagged_foreign as tagged
from app.services import sec_financial_statements as statements
from test_sec_inline_financials import document, context, fact, parse
from test_sec_document_set_cohort import originals, Client, NOW


def proof(currencies=('USD',)):
    return {'currency_observation': {'methodology': 'entity_wide_statement_currencies_v1',
            'current_statement_currencies': list(currencies)},
            'standard_foreign_reporting': {'currency_policy': tagged.CURRENCY_POLICY}}


@pytest.mark.parametrize('variant', ['current', 'bank_revenue', 'balance_only', 'old', 'other_entity', 'segment', 'nil', 'per_share'])
def test_current_entity_currency_observation_excludes_irrelevant_columns(variant):
    ctx = context('foreign')
    if variant == 'old': ctx = ctx.replace('2026-06-30', '2025-06-30')
    elif variant == 'other_entity': ctx = context('foreign', cik='2')
    elif variant == 'segment': ctx = context('foreign', '<xbrli:segment><member>subsidiary</member></xbrli:segment>')
    elif variant == 'balance_only':
        ctx = ctx.replace('<xbrli:startDate>2026-01-01</xbrli:startDate><xbrli:endDate>2026-06-30</xbrli:endDate>', '<xbrli:instant>2026-06-30</xbrli:instant>')
    unit = '<xbrli:unit id="cny"><xbrli:measure>iso4217:CNY</xbrli:measure></xbrli:unit>'
    if variant == 'per_share':
        unit = '<xbrli:unit id="cny"><xbrli:divide><xbrli:unitNumerator><xbrli:measure>iso4217:CNY</xbrli:measure></xbrli:unitNumerator><xbrli:unitDenominator><xbrli:measure>xbrli:shares</xbrli:measure></xbrli:unitDenominator></xbrli:divide></xbrli:unit>'
    attrs = 'xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:nil="true"' if variant == 'nil' else ''
    tag = 'Assets' if variant == 'balance_only' else ('RevenuesNetOfInterestExpense' if variant == 'bank_revenue' else 'Revenues')
    extra = ctx + unit + fact('' if variant == 'nil' else '77', 'foreign', tag=tag, attrs=attrs).replace('unitRef="usd"', 'unitRef="cny"')
    result = parse(document(extra))['evidence']
    if variant in ('current', 'bank_revenue', 'balance_only'):
        assert result['currency_observation']['current_statement_currencies'] == ['CNY', 'USD']
        with pytest.raises(tagged.TaggedForeignError, match='statement_currency'): tagged.verify_currency(result)
    else:
        assert result['currency_observation']['current_statement_currencies'] == ['USD']
        tagged.verify_currency(result)


def test_complete_nami_original_is_held_before_projection():
    from app.services import sec_inline_financials as inline
    facts, company, cik, urls = originals('NAMI')
    assert any(b'convenience' in raw.lower() and b'renminbi' in raw.lower() for raw in urls.values())
    with pytest.raises(tagged.TaggedForeignError, match='unreviewed_foreign_statement_currency'):
        inline.acquire(facts, company, symbol='NAMI', cik=cik, observed_at=NOW, client=Client(urls))


@pytest.mark.parametrize('defect', ['old', 'dual', 'absent', 'empty', 'malformed', 'selection'])
def test_stale_or_tampered_generic_proof_is_rejected(defect):
    source = proof()
    if defect == 'old': source['standard_foreign_reporting'].pop('currency_policy')
    elif defect == 'dual': source['currency_observation']['current_statement_currencies'].append('CNY')
    elif defect == 'absent': source.pop('currency_observation')
    elif defect == 'empty': source['currency_observation']['current_statement_currencies'] = []
    elif defect == 'malformed': source['currency_observation'] = []
    else: source['standard_foreign_reporting'] = None
    assert not tagged.cached_currency_valid({'inline_filing_supplement': source})
    payload = {'source': 'sec_edgar', 'symbol': 'GENERIC', 'sourceEvidence': {
        'standardForeignReporting': source['standard_foreign_reporting'], 'inlineFilingSupplement': source}}
    row = SimpleNamespace(source='sec_edgar', fetched_at=NOW, payload_json=json.dumps(payload))
    assert statements.cached_row_payload(row, 'GENERIC', now=NOW) is None
    from app.services.free_fundamentals import combine
    from app.services.finnhub_metrics import VERSION as METRICS_VERSION
    with pytest.raises(ValueError, match='statement currency'):
        combine({'provider': 'sec_edgar', 'symbol': 'GENERIC', 'source_evidence_json': json.dumps({'inline_filing_supplement': source})}, {'symbol': 'GENERIC', 'source': 'finnhub', 'version': METRICS_VERSION})


def test_current_generic_and_unrelated_cache_remain_usable():
    for source in (proof(), {}):
        assert tagged.cached_currency_valid({'inline_filing_supplement': source})
        evidence = {'inlineFilingSupplement': source}
        if source: evidence['standardForeignReporting'] = source['standard_foreign_reporting']
        payload = {'source': 'sec_edgar', 'symbol': 'GENERIC', 'sourceEvidence': evidence}
        row = SimpleNamespace(source='sec_edgar', fetched_at=NOW, payload_json=json.dumps(payload))
        assert statements.cached_row_payload(row, 'GENERIC', now=NOW) == payload


def test_prepared_generic_cache_requires_current_currency_proof(monkeypatch):
    from app.services import sec_fundamentals_preparation as prep
    source = proof()
    payload = {'source': 'sec_edgar', 'symbol': 'GENERIC', 'observed_at': NOW.isoformat(),
               'version': prep.version_for_symbol('GENERIC'),
               'calculation_evidence': {'inline_filing_supplement': source}}
    row = SimpleNamespace(source='sec_edgar', fetched_at=NOW, payload_json=json.dumps(payload))
    class Session:
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def get(self, *args): return row
    monkeypatch.setattr(prep, 'SessionLocal', Session)
    assert prep._cached('GENERIC', NOW) == payload
    source['standard_foreign_reporting'].pop('currency_policy')
    row.payload_json = json.dumps(payload)
    assert prep._cached('GENERIC', NOW) is None


def test_nami_public_and_prepared_hold_is_cached_without_repeated_fetch(monkeypatch):
    from datetime import datetime
    from unittest.mock import patch
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    from app.models import InsightsSnapshot
    from app.services import sec_fundamentals_preparation as prep, sec_directory
    from app.services import sec_financial_continuity
    from app.request_priority import set_request_context, reset_request_context
    facts, company, cik, urls = originals('NAMI')
    client = Client(urls)
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None): return NOW
    engine = create_engine('sqlite://')
    InsightsSnapshot.__table__.create(engine)
    factory = sessionmaker(bind=engine)
    monkeypatch.setenv('SEC_FUNDAMENTALS_WARMING_ENABLED', '1')
    monkeypatch.setattr(sec_directory, 'directory', lambda: {'NAMI': {'cik': cik}})
    monkeypatch.setattr(sec_financial_continuity, 'fetch_history', lambda *a, **k: None)
    with patch.object(prep, 'SessionLocal', factory), patch.object(prep, 'datetime', Clock), patch.object(prep, 'DirectSourceClient', lambda: client):
        result = prep.prepare('NAMI')
        assert result['status'] == 'unavailable' and result['values'] == {}
        assert result['reason'] == 'unreviewed_foreign_statement_currency'
        count = len(client.calls)
        assert prep.prepare('NAMI') == result and len(client.calls) == count
    with patch.object(statements, 'SessionLocal', factory), patch.object(statements, 'datetime', Clock), patch.object(statements, 'DirectSourceClient', lambda: client):
        token = set_request_context({})
        try:
            result = statements.prepared('NAMI')
            assert result['status'] == 'unavailable'
            count = len(client.calls)
            assert statements.prepared('NAMI') == result and len(client.calls) == count
            assert not result['annual'] and not result['quarterly']
        finally:
            reset_request_context(token)
    engine.dispose()
