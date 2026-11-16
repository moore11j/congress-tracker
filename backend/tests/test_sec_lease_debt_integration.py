from copy import deepcopy
from datetime import date, datetime, timedelta, timezone
import gzip
import hashlib
import json
from pathlib import Path
import socket

import pytest
from app.services import sec_fundamentals as sec, sec_inline_financials as inline
from app.services import sec_lease_debt_evidence as subject, sec_net_debt_policy as policy

ROOT = Path(__file__).parent/'fixtures/sec_lease_debt'
NOW = datetime(2026, 11, 9, tzinfo=timezone.utc)
META = json.loads((ROOT/'manifest.json').read_text())['AZO']['arguments']


def sources():
    manifest = json.loads((ROOT/'acquisition-manifest.json').read_text())
    result = {}
    for name, item in manifest.items():
        raw = gzip.decompress((ROOT/item['file']).read_bytes())
        assert hashlib.sha256(raw).hexdigest() == item['sha256'] and len(raw) == item['bytes']
        result[name] = raw
    return result['facts'], result['company']


class Originals:
    def __init__(self):
        self.calls = []

    def get(self, url):
        assert url == META['url']
        self.calls.append(url)
        assert len(self.calls) == 1
        return gzip.decompress((ROOT/'AZO.html.gz').read_bytes())


@pytest.fixture(scope='module')
def captured():
    facts, company = sources()
    client = Originals()
    supplement = inline.acquire(facts, company, symbol='AZO', cik=META['cik'], observed_at=NOW,
        client=client, include_debt_precision=True)
    assert client.calls == [META['url']]
    base = sec.project_fundamentals(facts, company, symbol='AZO', cik=META['cik'], observed_at=NOW)
    values = sec.project_fundamentals(facts, company, symbol='AZO', cik=META['cik'], observed_at=NOW,
        filing_supplement=supplement)
    return facts, company, supplement, base, values


def test_actual_acquisition_normalization_and_repeat_change_only_verified_leverage(captured, monkeypatch):
    monkeypatch.setattr(socket.socket, 'connect', lambda *a, **k: pytest.fail('Offline source replay requested network'))
    facts, company, supplement, base, values = captured
    assert base.get('net_debt_to_ebitda') is None
    assert values['net_debt_to_ebitda'] == pytest.approx(2.082666809485135)
    assert {k:v for k,v in values.items() if k not in {'net_debt_to_ebitda','source_evidence_json'}} == {
        k:v for k,v in base.items() if k not in {'net_debt_to_ebitda','source_evidence_json'}}
    repeat = sec.project_fundamentals(facts, company, symbol='AZO', cik=META['cik'], observed_at=NOW,
        filing_supplement=supplement)
    assert repeat == values
    evidence = json.loads(values['source_evidence_json'])
    chosen = evidence['net_debt_policy']['candidate']
    assert chosen['debt'] == 9_542_328_000 and chosen['cash'] == 326_115_000
    assert chosen['net_debt'] == 9_216_213_000
    assert evidence['net_debt_policy']['selected_basis'] == 'borrowings_plus_finance_leases'
    assert evidence['net_debt_policy']['debt_measurement'] == 'gross_borrowing_principal_plus_finance_lease_liabilities'
    assert 'before discounts and debt issuance costs' in evidence['net_debt_policy']['basis']


@pytest.mark.parametrize('change', ['cik', 'hash', 'period', 'future', 'borrowings', 'leases', 'classification', 'instrument'])
def test_mismatched_supplement_never_qualifies_canonical_value(captured, change):
    facts, company, original, base, _ = captured
    supplement = deepcopy(original)
    proof = supplement['evidence']['disjoint_borrowing_leases']
    if change == 'cik': proof['cik'] = '0000000001'
    elif change == 'hash': proof['source_sha256'] = '0'*64
    elif change == 'period': proof['period_date'] = '2026-05-09'
    elif change == 'future': supplement['evidence']['filed'] = '2026-11-10'
    elif change == 'borrowings': proof['borrowings']['total']['value'] += 1
    elif change == 'leases': proof['finance_lease_total'] += 1
    elif change == 'classification': proof['finance_leases']['current']['location'] = 'Borrowings'
    elif change == 'instrument': proof['borrowings']['components'][0]['value'] += 1
    values = sec.project_fundamentals(facts, company, symbol='AZO', cik=META['cik'], observed_at=NOW,
        filing_supplement=supplement)
    assert values.get('net_debt_to_ebitda') is None


def test_conflicting_reported_inclusive_total_keeps_existing_hold(captured):
    _, company, _, _, values = captured
    panel = json.loads(values['source_evidence_json'])['leverage_preparation']
    check = panel['debt_addition_reconciliations']['leverage_borrowings']
    from app.services import sec_leverage as leverage
    instant = {name: None for name in leverage.INSTANT}
    instant.update(dict(zip(subject.NAMES, deepcopy(check['original_inputs']))))
    instant['leverage_finance_leases'] = deepcopy(check['inputs'][1])
    instant['leverage_debt_with_finance_leases'] = {**instant['leverage_borrowings'], 'tag':'DebtAndCapitalLeaseObligations'}
    assert leverage._debt_addition_checks(instant)['leverage_borrowings']['status'] == 'conflicting_debt_addition'


@pytest.mark.parametrize('change', ['cash', 'debt', 'source', 'total', 'lease_input', 'future_period'])
def test_policy_rechecks_bound_original_and_candidate(captured, change):
    _, company, _, _, values = captured
    panel = json.loads(values['source_evidence_json'])['leverage_preparation']
    check = panel['debt_addition_reconciliations']['leverage_borrowings']
    candidate = next(p for p in panel['candidates'] if p['basis'] == 'borrowings_plus_finance_leases')
    if change == 'cash': candidate['cash'] += 1
    elif change == 'debt': candidate['debt'] += 1
    elif change == 'source': check['original_inputs'][0]['source_cik'] = '0000000001'
    elif change == 'total': check['sum'] += 1
    elif change == 'lease_input': candidate['debt_inputs'][1]['value'] += 1
    elif change == 'future_period': candidate['period_date'] = '2026-11-09'
    assert policy.select(panel, json.loads(company))['status'] == 'unavailable'


def test_optional_fetch_is_off_by_default_and_stale_inputs_do_not_fetch(captured):
    facts, company, _, _, _ = captured
    client = Originals()
    assert inline.acquire(facts, company, symbol='AZO', cik=META['cik'], observed_at=NOW, client=client) is None
    assert client.calls == []
    assert subject.acquire(json.loads(facts), company=json.loads(company), cik=META['cik'],
        available_by=date(2027, 8, 30), end=date(2026, 8, 29), forms={'10-K','10-Q'}, client=client) is None
    assert client.calls == []


def test_actual_source_transition_and_rollback_do_not_send_or_duplicate_alerts(captured, monkeypatch):
    from sqlalchemy import create_engine
    from sqlalchemy.orm import Session
    from app.db import Base
    from app.models import (FundamentalsCache, MonitoringAlert, Security, UserAccount,
        Watchlist, WatchlistItem, WatchlistAlertRule)
    from app.services.custom_alert_rules import evaluate_watchlist_custom_alerts, validate_conditions
    from app.services.fundamentals_cache import cached_fundamentals_by_symbol
    monkeypatch.setattr(socket.socket, 'connect', lambda *a, **k: pytest.fail('Consumer replay requested network'))
    monkeypatch.setenv('FMP_PROVIDER_DISABLED', '1')
    values = captured[4]
    engine = create_engine('sqlite://')
    Base.metadata.create_all(engine)
    with Session(engine) as db:
        user = UserAccount(email='lease-replay@example.test')
        security = Security(symbol='AZO', name='AutoZone', asset_class='equity')
        db.add_all([user, security]); db.flush()
        watchlist = Watchlist(name='Source proof', owner_user_id=user.id)
        db.add(watchlist); db.flush()
        db.add(WatchlistItem(watchlist_id=watchlist.id, security_id=security.id, target_type='ticker'))
        db.add(WatchlistAlertRule(user_id=user.id, watchlist_id=watchlist.id, name='Leverage', enabled=True,
            scope_type='any_watchlist_ticker', match_type='all', delivery='both',
            conditions_json=json.dumps(validate_conditions([dict(metric='net_debt_to_ebitda',
                operator='gt', comparison_type='value', comparison_value=2)]))))
        legacy = FundamentalsCache(symbol='AZO', provider='fmp', status='ok', fetched_at=NOW,
            net_debt_to_ebitda=1)
        columns = {c.name for c in FundamentalsCache.__table__.columns}
        replacement = FundamentalsCache(**{**{k:v for k,v in values.items() if k in columns}, 'status':'ok'})
        db.add_all([legacy, replacement]); db.commit()
        for minute, provider, expected in [(0,'fmp',1), (5,'sec_edgar',values['net_debt_to_ebitda']),
                (10,'sec_edgar',values['net_debt_to_ebitda']), (15,'fmp',1)]:
            monkeypatch.setenv('FUNDAMENTALS_PROVIDER', provider)
            assert cached_fundamentals_by_symbol(db, ['AZO'])['AZO'].net_debt_to_ebitda == expected
            result = evaluate_watchlist_custom_alerts(db, user_id=user.id, watchlist_id=watchlist.id,
                now=NOW+timedelta(minutes=minute))
            assert result['triggered'] == 0
            db.commit()
        assert db.query(MonitoringAlert).count() == 0
    engine.dispose()


def test_new_measurement_rebaselines_without_hiding_later_value_changes(captured):
    from types import SimpleNamespace
    from app.services.custom_alert_rules import _fundamental_basis
    evidence = json.loads(captured[4]['source_evidence_json'])
    def basis(e):
        return _fundamental_basis(SimpleNamespace(provider='sec_edgar', source_evidence_json=json.dumps(e)), {'net_debt_to_ebitda'})
    qualified = basis(evidence)
    unqualified = deepcopy(evidence)
    del unqualified['net_debt_policy']['debt_measurement']
    assert basis(unqualified) != qualified
    evidence['net_debt_policy']['value'] += 1
    evidence['net_debt_policy']['period_date'] = '2026-11-21'
    assert basis(evidence) == qualified
