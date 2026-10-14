from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine, select, func
from sqlalchemy.orm import sessionmaker
from app.clients.direct_sources import DirectSourceError
from app.db import Base
from app.models import InsightsSnapshot, FundamentalsCache, Event
from app.services import sec_financial_statements as statements, sec_foreign_statements as foreign, sec_directory

NOW = datetime(2026, 10, 14, 15, tzinfo=timezone.utc)


def captured():
    # Unmodified standard-concept rows extracted from SEC companyfacts.
    # Full source SHA256 d719daada14d23a0b63e8be4a4ea1b43ae96f3afd31265d915da937894bca56b.
    facts = json.loads((Path(__file__).parent / 'fixtures/webull_annual_facts.json').read_bytes())
    company = dict(cik='0001866364', name='Webull Corp', tickers=['BULL', 'BULLW'],
        filings={'recent': dict(accessionNumber=['0001213900-26-041653'], form=['20-F'],
            filingDate=['2026-04-09'], reportDate=['2025-12-31'], primaryDocument=['ea0283691-20f_webull.htm'])})
    return facts, company


def project(facts, company):
    return statements.project(json.dumps(facts).encode(), json.dumps(company).encode(),
        symbol='BULL', cik='1866364', observed_at=NOW)


def test_reviewed_webull_annual_values_match_filed_statements_and_keep_unverified_fields_missing():
    facts, company = captured()
    result = project(facts, company)
    assert [r['date'] for r in result['annual']] == [f'{y}-12-31' for y in range(2022, 2026)]
    latest = result['annual'][-1]
    # Original annual report, consolidated statements (not adjusted earnings).
    assert latest['revenue'] == 570996806
    assert latest['netIncome'] == 24770704
    assert latest['operatingCashFlow'] == 566424456
    assert latest['capex'] == 4887272
    assert latest['freeCashFlow'] == 561537184
    assert 'customer account movements' in result['reportingBasis']
    assert 'intangible assets' in result['sourceEvidence']['cashFlowBasis']
    assert 'preferred-share adjustments' in result['reportingBasis']
    assert latest['currency'] == result['reportingCurrency'] == 'USD'
    assert latest['eps'] is latest['grossMargin'] is latest['operatingMargin'] is None
    assert result['quarterly'] == [] and all(v is None for v in result['summary'].values())
    assert result['status'] == 'partial' and not result['unavailable']
    assert result['sourceEvidence']['periods']['2025-12-31']['revenue']['accession'] == '0001213900-26-041653'
    assert result['sourceEvidence']['factsSha256'] == hashlib.sha256(json.dumps(facts).encode()).hexdigest()
    assert project(facts, company) == result


def test_ordinary_shares_profile_does_not_grant_warrants_or_other_issuers():
    assert foreign.profile('BULLW', '1866364') is None
    with pytest.raises(DirectSourceError, match='cik_mismatch'):
        foreign.profile('BULL', '1')
    facts, company = captured()
    company['tickers'] = ['BULLW']
    with pytest.raises(DirectSourceError, match='symbol_identity'):
        project(facts, company)


@pytest.mark.parametrize('mutation', ['currency', 'conflict', 'filing_form'])
def test_webull_invalid_latest_revenue_is_not_published(mutation):
    facts, company = captured()
    units = facts['facts']['us-gaap']['RevenueFromContractWithCustomerIncludingAssessedTax']['units']
    latest = [r for r in units['USD'] if r['end'] == '2025-12-31']
    assert len(latest) == 1
    if mutation == 'currency': units['CNY'] = units.pop('USD')
    if mutation == 'conflict': units['USD'].append({**latest[0], 'val': 1})
    if mutation == 'filing_form': latest[0]['form'] = '6-K'
    assert all(r['date'] != '2025-12-31' for r in project(facts, company)['annual'])


def test_old_webull_unavailable_cache_is_invalidated_then_replaced_once_without_core_writes(monkeypatch):
    facts, company = captured()
    engine = create_engine('sqlite:///:memory:')
    Base.metadata.create_all(engine)
    factory = sessionmaker(bind=engine)
    monkeypatch.setattr(statements, 'SessionLocal', factory)
    monkeypatch.setattr(sec_directory, 'directory', lambda: {'BULL': {'cik': '0001866364'}})
    old = dict(symbol='BULL', source='sec_edgar', status='unavailable', annual=[], quarterly=[])
    assert statements.cached_row_payload(SimpleNamespace(source='sec_edgar', fetched_at=NOW,
        payload_json=json.dumps(old)), 'BULL', now=NOW) is None
    calls = []
    def get(self, url):
        calls.append(url)
        assert '/Archives/' not in url  # current annual facts need no extra original fetch
        return json.dumps(facts if '/companyfacts/' in url else company).encode()
    monkeypatch.setattr(statements.DirectSourceClient, 'get', get)
    first = statements.prepared('BULL')
    assert len(first['annual']) == 4 and len(calls) == 2
    with factory() as db:
        before = [(r.kind, r.payload_json, r.fetched_at) for r in db.scalars(select(InsightsSnapshot))]
    monkeypatch.setattr(statements.DirectSourceClient, 'get', lambda *args: pytest.fail('cached repeat fetched a source'))
    assert statements.prepared('BULL') == first
    with factory() as db:
        assert before == [(r.kind, r.payload_json, r.fetched_at) for r in db.scalars(select(InsightsSnapshot))]
        assert db.scalar(select(func.count()).select_from(FundamentalsCache)) == 0
        assert db.scalar(select(func.count()).select_from(Event)) == 0
    engine.dispose()
