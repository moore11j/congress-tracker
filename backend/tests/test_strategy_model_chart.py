from datetime import date, datetime, timezone
import pytest
from sqlalchemy import select
from app.models import PriceCache, StrategyEvaluationRun, StrategyTrade
from app.services.strategy_model_chart import simulate, required_prices, load_decisions, refresh_chart, chart_payload
from test_strategy_evaluations import _session, _strategy


def d(day): return date(2026, 10, day)

def bars(days, aapl=True):
    result={('SPY',d(day).isoformat()):(100,100+day) for day in days}
    if aapl:
        result.update({('AAPL',d(day).isoformat()):(100,110) for day in days})
    return result


def test_next_session_entry_weekend_and_cash_exit_accounting():
    prices=bars([1,2,5,6]);prices[('AAPL',d(5).isoformat())]=(120,130)
    points,gaps=simulate(d(1),{d(2):{'AAPL':1},d(5):{}},d(6),prices)
    assert not gaps
    assert [p['date'] for p in points]==['2026-10-01','2026-10-02','2026-10-05','2026-10-06']
    assert [p['strategyValue'] for p in points]==[100000,110000,120000,120000]
    assert points[-1]['cash']==120000 and points[-1]['activeHoldings']==0
    assert points[0]['benchmarkValue']==100000


def test_no_rebalance_on_unchanged_day_and_partial_cash():
    prices=bars([1,2,5]);prices[('AAPL',d(5).isoformat())]=(110,120)
    points,_=simulate(d(1),{d(2):{'AAPL':.5}},d(5),prices)
    assert points[-1]['cash']==50000
    assert points[-1]['strategyValue']==110000


def test_missing_price_stops_curve_without_flat_padding_or_future_substitution():
    prices=bars([1,2,5,6,7,8]);del prices[('AAPL',d(5).isoformat())]
    for day in [6,7,8]:del prices[('AAPL',d(day).isoformat())]
    points,gaps=simulate(d(1),{d(2):{'AAPL':1}},d(8),prices)
    assert points[-1]['date']=='2026-10-07'
    assert gaps[0]['symbol']=='AAPL'
    assert gaps[0]['date']=='2026-10-08'


def test_all_cash_still_marks_every_completed_session_against_spy():
    points,gaps=simulate(d(1),{},d(6),bars([1,2,5,6],False))
    assert len(points)==4 and not gaps
    assert all(p['strategyValue']==100000 and p['activeHoldings']==0 for p in points)
    assert points[-1]['benchmarkValue']>100000


def test_cash_exit_does_not_require_retired_symbols_on_future_days():
    needed=required_prices(d(1),{d(2):{'AAPL':1},d(5):{}},d(6))
    assert needed['AAPL']=={d(2),d(5)}
    assert d(6) in needed['SPY']


def test_invalid_overallocated_weights_are_rejected():
    with pytest.raises(ValueError):simulate(d(1),{d(2):{'AAPL':1.1}},d(2),bars([1,2]))


def test_backdated_decision_cannot_execute_before_recorded_run():
    with _session()() as db:
        s,v=_strategy(db)
        r=StrategyEvaluationRun(strategy_id=s.id,strategy_version_id=v.id,idempotency_key='x',evaluation_date=d(1),executed_at=datetime(2026,10,2,22,tzinfo=timezone.utc),status='completed')
        db.add(r);db.flush()
        db.add(StrategyTrade(strategy_id=s.id,strategy_version_id=v.id,strategy_run_id=r.id,symbol='AAPL',ticker_at_time='AAPL',action='buy',status='open',signal_date=d(1),effective_date=d(2),weight_pct=100))
        db.commit()
        start,schedules,_=load_decisions(db,s.id)
        assert list(schedules)==[d(5)]


def test_cached_chart_refresh_is_idempotent_and_public_metadata_excludes_decision_ids(monkeypatch):
    with _session()() as db:
        s,v=_strategy(db)
        db.add(StrategyEvaluationRun(strategy_id=s.id,strategy_version_id=v.id,idempotency_key='empty',evaluation_date=d(1),status='completed'))
        for day in [1,2]:db.add(PriceCache(symbol='SPY',date=d(day).isoformat(),close=100+day,open_price=100,adjustment_status='split_adjusted_price_return'))
        db.commit()
        first=refresh_chart(db,s.id,as_of=d(2));second=refresh_chart(db,s.id,as_of=d(2))
        assert first['points']==second['points'] and first['status']=='current'
        monkeypatch.setattr('app.services.strategy_model_chart.get_expected_latest_market_date',lambda:d(5))
        public=chart_payload(db,s.id)
        assert public['status']=='stale'
        assert 'runIds' not in public and 'decisionHash' not in public and 'missingPrices' not in public
        assert public['performance']['totalReturnPct']==0


def test_legacy_equal_weight_membership_recovers_omitted_rebalance_rows():
    import json
    with _session()() as db:
        s,v=_strategy(db);v.rules_json=json.dumps({'candidate_source':'disclosure_portfolio'})
        r=StrategyEvaluationRun(strategy_id=s.id,strategy_version_id=v.id,idempotency_key='legacy',evaluation_date=d(1),status='completed',metadata_json=json.dumps({'candidateSymbols':['AAPL','MSFT','N/A']}))
        db.add(r);db.flush()
        for symbol in ['AAPL','MSFT']:
            db.add(StrategyTrade(strategy_id=s.id,strategy_version_id=v.id,strategy_run_id=r.id,symbol=symbol,ticker_at_time=symbol,action='buy',status='open',signal_date=d(1),effective_date=d(2),weight_pct=60))
        db.commit()
        _,targets,_=load_decisions(db,s.id)
        assert targets[d(2)]=={'AAPL':1/3,'MSFT':1/3}
        assert sum(targets[d(2)].values())<1  # Invalid legacy ticker allocation stays cash.


def test_new_recorded_target_weights_preserve_intentional_cash_allocation():
    import json
    with _session()() as db:
        s,v=_strategy(db)
        r=StrategyEvaluationRun(strategy_id=s.id,strategy_version_id=v.id,idempotency_key='weighted',evaluation_date=d(1),status='completed',metadata_json=json.dumps({'targetWeights':{'AAPL':40},'candidateSymbols':['AAPL']}))
        db.add(r);db.flush()
        db.add(StrategyTrade(strategy_id=s.id,strategy_version_id=v.id,strategy_run_id=r.id,symbol='AAPL',ticker_at_time='AAPL',action='buy',status='open',signal_date=d(1),effective_date=d(2),weight_pct=40))
        db.commit()
        _,targets,_=load_decisions(db,s.id)
        assert targets[d(2)]=={'AAPL':.4}


def test_unpriced_entry_stays_cash_without_spending_other_allocations():
    prices=bars([1,2,5]);del prices[('AAPL',d(2).isoformat())]
    unfilled=[]
    points,gaps=simulate(d(1),{d(2):{'AAPL':1}},d(5),prices,unfilled=unfilled)
    assert not gaps and len(unfilled)==1
    assert all(p['strategyValue']==100000 and p['cash']==100000 for p in points)
    assert points[-1]['activeHoldings']==0  # Later price alone does not invent a new decision.


def test_known_open_missing_close_blocks_instead_of_excluding_losing_position():
    prices=bars([1,2]);prices[('AAPL',d(2).isoformat())]=(100,None)
    unfilled=[]
    points,gaps=simulate(d(1),{d(2):{'AAPL':1}},d(2),prices,unfilled=unfilled)
    assert not unfilled and gaps[0]['field']=='close'
    assert len(points)==1


def test_verified_successor_prices_preserve_original_ledger_symbol_and_date_boundary():
    from app.services.strategy_model_chart import load_bars
    with _session()() as db:
        for day in ['2026-07-20','2026-07-22']:
            db.add(PriceCache(symbol='SNFI',date=day,close=2,open_price=1.9,adjustment_status='split_adjusted_price_return'))
        db.commit()
        days={date(2026,7,20),date(2026,7,22)}
        resolved=load_bars(db,{'NRDE':days,'SNFI':days})
        assert ('NRDE','2026-07-20') not in resolved
        assert resolved[('NRDE','2026-07-22')]==(1.9,2)


def test_sparse_quote_is_labeled_and_missing_open_never_creates_a_fill():
    prices=bars([1,2,5,6]);del prices[('AAPL',d(5).isoformat())]
    stale=[];skipped=[]
    points,gaps=simulate(d(1),{d(2):{'AAPL':1},d(5):{}},d(6),prices,stale_marks=stale,skipped_rebalances=skipped)
    assert not gaps
    assert skipped==[{'date':'2026-10-05','symbols':['AAPL']}]
    assert stale==[{'symbol':'AAPL','date':'2026-10-05','priceAsOf':'2026-10-02'}]
    assert points[-1]['activeHoldings']==1 and points[-1]['cash']==0


def test_skipped_exit_keeps_requiring_prices_after_planned_sale_date():
    prices=bars([1,2,5,6]);del prices[('AAPL',d(5).isoformat())]
    schedules={d(2):{'AAPL':1},d(5):{}}
    needed=required_prices(d(1),schedules,d(6))
    assert d(6) not in needed['AAPL']
    simulate(d(1),schedules,d(6),prices,requirements=needed)
    assert d(6) in needed['AAPL']


def test_weekend_activation_does_not_invent_a_prior_session_or_performance():
    with _session()() as db:
        s,v=_strategy(db)
        db.add(StrategyEvaluationRun(strategy_id=s.id,strategy_version_id=v.id,idempotency_key='new-weekend',evaluation_date=d(4),status='completed'))
        db.commit()
        payload=refresh_chart(db,s.id,as_of=d(2))
        assert payload['startedOn']=='2026-10-04'
        assert payload['status']=='awaiting_first_session'
        assert payload['points']==[] and payload['through'] is None
