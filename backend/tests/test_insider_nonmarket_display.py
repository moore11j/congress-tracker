import copy
import json
from datetime import date, datetime, timezone

import pytest

from app.models import Event, TradeOutcome
from app.routers.events import _insider_trade_row


@pytest.mark.parametrize('code,side,market,derivative', [
    ('F', None, False, False), ('A', None, False, False),
    ('M', None, False, True), ('P', 'purchase', True, True),
    ('S', 'sale', 'false', 'false'), ('F', None, True, False),
    ('F', None, None, False),
])
@pytest.mark.parametrize('return_source', ['quote', 'payload', 'outcome'])
def test_nonmarket_and_derivative_rows_never_display_equity_performance(code, side, market, derivative, return_source):
    payload = dict(symbol='PROV', transaction_date='2026-05-23', filing_date='2026-10-07',
        price=17.23, shares=1081, transaction_code=code, is_market_trade=market,
        is_derivative=derivative, smart_score=80, smart_band='strong')
    if return_source == 'payload':
        payload['pnl_pct'] = 30
    event = Event(id=1,event_type='insider_trade',symbol='PROV',trade_type=side,transaction_type=code,
        source='sec_edgar',ts=datetime(2026,10,9,tzinfo=timezone.utc),payload_json=json.dumps(payload),
        amount_min=18625,amount_max=18625)
    outcome = TradeOutcome(id=2,event_id=1,symbol='PROV',trade_type=side or 'purchase',source='old',
        trade_date=date(2026,5,23),entry_price=17.23,return_pct=30,alpha_pct=25,
        benchmark_return_pct=5,holding_days=100,scoring_status='ok',methodology_version='insider_v1')
    before = copy.deepcopy(payload)
    row = _insider_trade_row(event,payload,outcome=outcome if return_source=='outcome' else None,
        fallback_pnl_pct=30 if return_source=='quote' else None,prefer_fallback_pnl=True)
    for key in ('pnl_pct','pnlPct','pnl','return_pct','returnPct','pnl_source','smart_score','smart_band',
                'alpha_pct','benchmark_return_pct','holding_period_days','outcome_horizon'):
        assert row[key] is None, key
    assert row['reported_price']==17.23 and row['transaction_date']=='2026-05-23'
    assert row['filing_date']=='2026-10-07' and payload==before
    assert outcome.return_pct==30 and outcome.alpha_pct==25


@pytest.mark.parametrize('side', ['purchase','sale'])
def test_market_trade_keeps_existing_quote_return(side):
    event=Event(id=1,event_type='insider_trade',symbol='PROV',trade_type=side,source='sec_edgar',
        ts=datetime(2026,10,9,tzinfo=timezone.utc),amount_max=10000)
    row=_insider_trade_row(event,dict(symbol='PROV',transaction_date='2026-10-07',price=17.23,
        is_market_trade=True,is_derivative=False),fallback_pnl_pct=5,prefer_fallback_pnl=True)
    assert row['pnl_pct']==5 and row['pnl_source']=='quote_cache'
