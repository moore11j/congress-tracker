from copy import deepcopy
from datetime import date, datetime, timezone
import json
import pytest
from app.services import finnhub_metrics as m, free_fundamentals as f
from test_finnhub_metrics import payload, sec, factory

NOW = datetime(2026, 10, 11, tzinfo=timezone.utc)


def source(current=120, prior=100):
    raw = payload()
    raw['metric'].update(beta=-.2, ebitdaCagr5Y=999)
    raw['series']['annual'] = {'ebitda': [{'period':'2025-12-31','v':current}, {'period':'2024-12-31','v':prior}]}
    raw['series']['quarterly']['ebitda'] = [{'period':'2026-06-30','v':999}]
    return raw


def test_annual_growth_beta_and_inputs_survive_without_currency_or_ttm_inference():
    raw=source(); before=deepcopy(raw)
    panel=m.normalize(raw,symbol='ABC',observed_at=NOW)
    assert panel['values']['ebitda_growth']==20 and panel['values']['beta']==-.2
    proof=panel['fields']['ebitda_growth']
    assert proof['basis']=='annual_yoy' and proof['period_date']=='2025-12-31'
    assert proof['inputs']==raw['series']['annual']['ebitda']
    assert panel['fields']['beta']['period_date'] is None
    assert panel['fields']['beta']['limitations']
    assert raw==before and panel==m.normalize(raw,symbol='ABC',observed_at=NOW)
    assert 'net_debt_to_ebitda' not in panel['values']


@pytest.mark.parametrize('current,prior,expected', [(0,100,-100),(-20,100,-120),(-50,-100,50),(50,-100,150)])
def test_annual_loss_growth_uses_absolute_prior_base(current,prior,expected):
    assert m.normalize(source(current,prior),symbol='ABC',observed_at=NOW)['values']['ebitda_growth']==expected


@pytest.mark.parametrize('current,prior', [(100,0),(None,100),(100,None)])
def test_missing_or_zero_base_never_becomes_growth(current,prior):
    assert 'ebitda_growth' not in m.normalize(source(current,prior),symbol='ABC',observed_at=NOW)['values']


@pytest.mark.parametrize('change', ['duplicate_conflict','future','malformed','bad_number','nonfinite','huge_integer','overflow','invalid_shape','long_gap','stale','latest_null'])
def test_optional_growth_holds_preserve_existing_metrics(change):
    raw=source();rows=raw['series']['annual']['ebitda']
    if change=='duplicate_conflict':rows.append({'period':'2025-12-31','v':121})
    if change=='future':rows[0]['period']='2027-12-31'
    if change=='malformed':rows[0]['period']='no-date'
    if change=='bad_number':rows[0]['v']=True
    if change=='nonfinite':rows[0]['v']='NaN'
    if change=='huge_integer':rows[0]['v']=10**400
    if change=='overflow':rows[0]['v']=1e308;rows[1]['v']=-1e308
    if change=='invalid_shape':raw['series']['annual']=[]
    if change=='long_gap':rows[1]['period']='2023-12-31'
    if change=='stale':rows[0]['period']='2024-12-31';rows[1]['period']='2023-12-31'
    if change=='latest_null':rows.insert(0,{'period':'2026-09-30','v':None})
    panel=m.normalize(raw,symbol='ABC',observed_at=NOW)
    assert 'ebitda_growth' not in panel['values']
    assert panel['values']['current_ratio']==2


def test_duplicate_order_and_53_week_fiscal_year_are_supported():
    raw=source();rows=raw['series']['annual']['ebitda']
    rows[0]['period']='2025-12-30';rows[1]['period']='2024-12-24';rows.append(deepcopy(rows[0]));rows.reverse()
    assert m.normalize(raw,symbol='ABC',observed_at=NOW)['values']['ebitda_growth']==20


def test_annual_growth_keeps_own_period_beside_sec_quarter_but_not_newer_sec_year():
    panel=m.normalize(source(),symbol='ABC',observed_at=NOW)
    base=sec(); base['period_date']=date(2026,6,30)
    result=f.combine(base,panel)
    assert result['ebitda_growth']==20 and result['period_date']==date(2026,6,30)
    evidence=json.loads(result['source_evidence_json'])
    assert evidence['supplement']['fields']['ebitda_growth']['period_date']=='2025-12-31'
    evidence=json.loads(base['source_evidence_json']);evidence['current']['revenue']['inputs']=[{'start':'2025-07-01','end':'2026-06-30'}]
    base['source_evidence_json']=json.dumps(evidence)
    assert 'ebitda_growth' not in f.combine(base,panel)
    base['ebitda_growth']=8
    assert f.combine(base,panel)['ebitda_growth']==8


def test_annual_history_never_transfers_beta_between_share_classes():
    raw=source();raw['symbol']='BRK.A'
    panel=m.normalize(raw,symbol='BRK-B',observed_at=NOW)
    assert panel['values']['ebitda_growth']==20 and 'beta' not in panel['values']
    assert panel['fields']['ebitda_growth']['source_symbol']=='BRK.A'
    base=sec();base['symbol']='BRK-B';base['source_evidence_json']=json.dumps({'cik':'0001067983','current':{}})
    assert f.combine(base,panel)['ebitda_growth']==20


def test_previous_cache_version_cannot_masquerade_as_new_field_coverage(factory,monkeypatch):
    from app.models import InsightsSnapshot
    raw=source()
    first=m.normalize(raw,symbol='ABC',observed_at=NOW)
    previous=deepcopy(first);previous['version']='finnhub_metrics_v1'
    previous['values'].pop('ebitda_growth');previous['fields'].pop('ebitda_growth')
    with factory() as db:
        db.add(InsightsSnapshot(kind='finnhub:metrics:ABC',source='finnhub',fetched_at=NOW,payload_json=json.dumps(previous)))
        db.commit()
    assert m.cached('ABC',NOW) is None
    with factory() as db:
        db.get(InsightsSnapshot,'finnhub:metrics:ABC').payload_json=json.dumps(first)
        db.commit()
    assert m.cached('ABC',NOW)==first
