from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
from app.models import InsightsSnapshot
from app.services import finnhub_paid_readers as reader, finnhub_paid_preparation as prep
from app.services import finnhub_research as transport

NOW = datetime.now(timezone.utc)
PERIOD = (NOW.date()+timedelta(days=60)).isoformat()


def forbidden(*args, **kwargs):
    pytest.fail('Unexpected provider, queue, or database operation')


def policy(kind):
    value = {'currency':'USD', 'unit':'currency', 'accountingBasis':'us_gaap', 'shareBasis':'not_applicable',
             'reviewedAt':(NOW-timedelta(days=1)).isoformat(), 'validUntil':(NOW+timedelta(days=30)).isoformat(),
             'evidenceUrl':'https://finnhub.io/docs/api', 'evidenceSha256':'a'*64}
    if kind != 'targets':
        value.update(statementMethodology='sec_statements_v1',comparisonCompatible=True)
    if kind == 'eps':
        value.update(unit='currency_per_share', shareBasis='listed_diluted_share')
    elif kind == 'targets':
        value.update(unit='currency_per_share', shareBasis='listed_share', accountingBasis='not_applicable')
    return value


@pytest.fixture
def db(monkeypatch):
    monkeypatch.setenv('FINNHUB_PAID_READERS_ENABLED','1')
    monkeypatch.setenv('FINNHUB_PAID_PREPARATION_ENABLED','1')
    monkeypatch.setenv('FINNHUB_SHARED_LIMITER_ENABLED','1')
    monkeypatch.setenv('FMP_PROVIDER_DISABLED','1')
    monkeypatch.setattr(transport.requests,'get',forbidden)
    monkeypatch.setattr(reader,'REVIEWED_BASIS',{('AAPL',kind,freq):policy(kind)
        for kind,freq in [('eps','quarterly'),('eps','annual'),('revenue','quarterly'),('revenue','annual'),('targets',None)]})
    engine=create_engine('sqlite:///:memory:')
    InsightsSnapshot.__table__.create(engine)
    with sessionmaker(bind=engine)() as session:
        yield session
    engine.dispose()


def stage(db, monkeypatch, kind='eps', frequency='quarterly', symbol='AAPL', **changes):
    if kind=='targets':
        frequency=None
        raw={'symbol':symbol.replace('-','.'), 'lastUpdated':NOW.date().isoformat(), 'numberAnalysts':7,
             'targetLow':100, 'targetMean':150, 'targetMedian':145, 'targetHigh':200, **changes}
    else:
        row={'period':PERIOD, 'year':2027, 'quarter':1 if frequency=='quarterly' else 0, 'numberAnalysts':9,
             kind+'Low':-2 if kind=='eps' else 100, kind+'Avg':0 if kind=='eps' else 200,
             kind+'High':3 if kind=='eps' else 300, **changes}
        raw={'symbol':symbol.replace('-','.'), 'freq':frequency, 'data':[row]}
    monkeypatch.setattr(transport,'request_json',lambda *a:deepcopy(raw))
    prep.prepare(db,symbol,kind,frequency=frequency,observed_at=NOW)
    db.commit()
    monkeypatch.setattr(transport,'request_json',forbidden)
    return db.get(InsightsSnapshot,prep.scope(symbol,kind,frequency)[2])


def fingerprint(db):
    return [(r.kind,r.source,r.fetched_at,r.payload_json) for r in db.scalars(select(InsightsSnapshot).order_by(InsightsSnapshot.kind))]


def panel():
    from app.services.ticker_financials import _unavailable
    value=_unavailable('AAPL')
    value.update(source='sec_edgar',status='partial',unavailable=False,annual=[{'date':'2025-09-30','revenue':123}],
                 sourceEvidence={'methodology':'sec_statements_v1','factsSha256':'b'*64})
    return value


def test_disabled_and_unreviewed_do_not_touch_database(monkeypatch):
    monkeypatch.delenv('FINNHUB_PAID_READERS_ENABLED',raising=False)
    assert reader.read(None,'AAPL','targets')['reason']=='disabled'
    monkeypatch.setenv('FINNHUB_PAID_READERS_ENABLED','1')
    monkeypatch.setattr(reader,'REVIEWED_BASIS',{})
    assert reader.read(None,'AAPL','targets')['reason']=='source_basis_unverified'
    assert reader.attach_targets(None,'AAPL',{},include_details=False)=={}


@pytest.mark.parametrize('change', [
    {'currency':'EUR'}, {'shareBasis':'ordinary_share'}, {'unit':'millions'}, {'accountingBasis':'adjusted'},
    {'reviewedAt':(NOW+timedelta(days=1)).isoformat()}, {'validUntil':NOW.isoformat()},
    {'validUntil':(NOW+timedelta(days=365)).isoformat()}, {'reviewedAt':NOW.replace(tzinfo=None).isoformat()},
    {'evidenceUrl':'https://example.com/assumption'}, {'evidenceSha256':''},
    {'evidenceUrl':'https://finnhub.io/docs?token=secret'}, {'comparisonCompatible':1},
    {'comparisonCompatible':False}, {'statementMethodology':'foreign_annual'}])
def test_basis_review_rejects_unsupported_semantics_or_invalid_evidence(db,monkeypatch,change):
    reader.REVIEWED_BASIS['AAPL','eps','quarterly'].update(change)
    assert reader.read(None,'AAPL','eps',frequency='quarterly',now=NOW)['reason']=='source_basis_unverified'


def test_original_time_provenance_and_zero_eps_survive_read_repeat(db,monkeypatch):
    stage(db,monkeypatch)
    before=fingerprint(db)
    result=reader.read(db,'AAPL','eps',frequency='quarterly',now=NOW+timedelta(hours=1))['data']
    assert result['observedAt']==NOW.isoformat()
    assert result['evaluatedAt']==(NOW+timedelta(hours=1)).isoformat()
    assert result['nextPeriod']['average']==0 and result['nextPeriod']['low']==-2
    assert result['publicationEligible'] and result['currency']=='USD'
    assert result['estimateBasis']=='provider_average_includes_proprietary_estimates'
    assert fingerprint(db)==before
    assert not json.loads(before[0][3])['normalized']['publicationEligible']
    assert reader.read(db,'AAPL','eps',frequency='quarterly',now=NOW+timedelta(hours=1))['data']==result


@pytest.mark.parametrize('mutation', ['raw','projection','source','future','stale','oversize','identity','json'])
def test_corrupt_or_expired_cache_is_held_without_refresh(db,monkeypatch,mutation):
    row=stage(db,monkeypatch)
    body=json.loads(row.payload_json)
    if mutation=='raw': body['raw']['data'][0]['epsAvg']=1
    elif mutation=='projection': body['normalized']['items'][0]['average']=999
    elif mutation=='identity': body['normalized']['symbol']='MSFT'
    elif mutation=='source': row.source='fmp'
    elif mutation=='future': row.fetched_at=NOW+timedelta(seconds=1)
    elif mutation=='stale': row.fetched_at=NOW-timedelta(days=1)
    row.payload_json=json.dumps(body)
    if mutation=='json': row.payload_json='['
    if mutation=='oversize': row.payload_json=' '*(transport.MAX_BYTES*3+1)
    db.commit(); before=fingerprint(db)
    assert reader.read(db,'AAPL','eps',frequency='quarterly',now=NOW)['data'] is None
    assert fingerprint(db)==before


def test_alias_retains_class_and_original_caller_proof(db,monkeypatch):
    row=stage(db,monkeypatch,symbol='BRK-B')
    reader.REVIEWED_BASIS['BRK.B','eps','quarterly']=policy('eps')
    result=reader.read(db,'BRK.B','eps',frequency='quarterly',now=NOW)['data']
    assert result['symbol']=='BRK.B' and result['providerSymbol']=='BRK.B'
    assert json.loads(row.payload_json)['normalized']['symbol']=='BRK-B'
    reader.REVIEWED_BASIS['BRK.A','eps','quarterly']=policy('eps')
    assert reader.read(db,'BRK.A','eps',frequency='quarterly',now=NOW)['data'] is None


def test_midnight_never_recycles_historical_forecast(db,monkeypatch):
    stage(db,monkeypatch,period=NOW.date().isoformat())
    before=fingerprint(db)
    next_day=NOW.replace(hour=23,minute=59,second=59,microsecond=0)+timedelta(seconds=1)
    assert reader.forecast(db,'AAPL','quarterly',now=next_day) is None
    assert fingerprint(db)==before


def test_forecasts_join_only_same_period_and_fiscal_identity(db,monkeypatch):
    stage(db,monkeypatch)
    stage(db,monkeypatch,'revenue')
    result=reader.forecast(db,'AAPL','quarterly',now=NOW)
    assert result['period']=='Q1 FY 2027'
    assert result['revenueEstimate']==200 and result['epsEstimate']==0
    assert result['earningsEstimate'] is None and result['forwardValuationEligible'] is False
    assert set(result['sourceEvidence'])=={'eps','revenue'}
    row=db.get(InsightsSnapshot,prep.scope('AAPL','revenue','quarterly')[2]);db.delete(row);db.commit()
    stage(db,monkeypatch,'revenue',year=2028)
    assert reader.forecast(db,'AAPL','quarterly',now=NOW) is None


def test_different_dates_are_not_combined_and_partial_scope_is_retained(db,monkeypatch):
    stage(db,monkeypatch)
    stage(db,monkeypatch,'revenue',period=(NOW.date()+timedelta(days=90)).isoformat())
    result=reader.forecast(db,'AAPL','quarterly',now=NOW)
    assert result['date']==PERIOD and result['epsEstimate']==0 and result['revenueEstimate'] is None


def test_no_analysts_or_missing_average_is_not_published(db,monkeypatch):
    stage(db,monkeypatch,numberAnalysts=0)
    assert reader.forecast(db,'AAPL','quarterly',now=NOW) is None


def test_complete_preparation_to_financial_contract_and_rollback(db,monkeypatch):
    for freq in ('annual','quarterly'):
        for kind in ('eps','revenue'):
            stage(db,monkeypatch,kind,freq)
    before=fingerprint(db); source=panel(); original=deepcopy(source)
    result=reader.attach_financials(db,'AAPL',source,now=NOW)
    assert result['forecasts']['nextQuarter']['epsEstimate']==0
    assert result['forecasts']['nextFiscalYear']['revenueEstimate']==200
    assert result['forecasts']['nextFiscalYear']['period']=='FY 2027'
    assert result['subsections']['analyst_estimates']['data']==result['forecasts']
    assert 'proprietary' in result['forecastBasis']
    for key in ('summary','annual','quarterly','earnings','health','sourceEvidence','updatedAt','valuation_metrics'):
        assert result[key]==source[key]
    assert source==original and fingerprint(db)==before
    assert reader.attach_financials(db,'AAPL',source,now=NOW)==result
    monkeypatch.delenv('FINNHUB_PAID_READERS_ENABLED')
    assert reader.attach_financials(db,'AAPL',source,now=NOW)==original


@pytest.mark.parametrize('change', [{'symbol':'MSFT'}, {'source':'fmp'}, {'reportingCurrency':'CAD'},
    {'sourceEvidence':{'methodology':'sec_foreign_annual_v1'}},
    {'sourceEvidence':{'methodology':'sec_statements_v1','reviewedReportingPolicy':{'share':'ordinary'}}}])
def test_incompatible_financial_panel_cannot_receive_usd_forecasts(db,monkeypatch,change):
    stage(db,monkeypatch)
    source=panel();source.update(change)
    assert reader.attach_financials(None,'AAPL',source,now=NOW)==source


def test_actual_ticker_reader_preserves_statements_on_optional_cache_failure(db,monkeypatch):
    from app.services import ticker_financials as financials, sec_financial_statements as sec
    monkeypatch.setenv('FINANCIAL_STATEMENTS_PROVIDER','sec_edgar')
    source=panel()
    monkeypatch.setattr(sec,'prepared',lambda symbol:deepcopy(source))
    monkeypatch.setattr(financials,'SessionLocal',lambda:db)
    stage(db,monkeypatch)
    result=financials.get_ticker_financials('AAPL')
    assert result['forecasts']['nextQuarter']['epsEstimate']==0
    def broken(*args,**kwargs): raise RuntimeError('optional cache unavailable')
    monkeypatch.setattr(reader,'attach_financials',broken)
    assert financials.get_ticker_financials('AAPL')==source
    monkeypatch.delenv('FINNHUB_PAID_READERS_ENABLED')
    monkeypatch.setattr(financials,'SessionLocal',forbidden)
    assert financials.get_ticker_financials('AAPL')==source


def recommendation(db):
    raw=[{'symbol':'AAPL','period':NOW.date().replace(day=1).isoformat(),'strongBuy':2,'buy':3,'hold':4,'sell':1,'strongSell':0}]
    normalized=transport.normalize_recommendations(raw,'AAPL',observed_at=NOW)
    db.add(InsightsSnapshot(kind='finnhub:recommendations:AAPL',source='finnhub',fetched_at=NOW,payload_json=json.dumps(normalized)))
    db.commit()


def test_actual_analyst_reader_paid_range_free_projection_and_no_events(db,monkeypatch):
    from app.services import analyst_consensus as analyst, data_enrichment_queue as queue
    monkeypatch.setenv('ANALYST_PROVIDER','finnhub')
    monkeypatch.setattr(queue,'enqueue_data_enrichment_job',forbidden)
    recommendation(db);stage(db,monkeypatch,'targets')
    before=fingerprint(db)
    paid=analyst.current_consensus_payload(db,'AAPL',include_details=True)
    view=paid['currentSnapshot']
    assert view['priceTargetRange']=={'high':200,'low':100,'median':145,'consensus':150,'average':150,'analystCount':7}
    assert view['snapshotDate']==NOW.date().replace(day=1).isoformat()
    assert view['targetSourceUpdatedAt']==NOW.date().isoformat()
    assert view['recommendationDistribution']['total']==10 and view['currentPriceAtSnapshot'] is None
    assert view['impliedUpside']=={'medianPct':None,'consensusPct':None}
    free=analyst.current_consensus_payload(db,'AAPL',include_details=False)
    assert 'priceTargetRange' not in free['currentSnapshot'] and 'targetSourceEvidence' not in free['currentSnapshot']
    assert free['access']['detailsLocked'] and free['currentSnapshot']['totalRatingCount']==10
    assert analyst.analyst_consensus_component_inputs(db,'AAPL')['inputs']['freshnessStatus']=='unavailable'
    assert fingerprint(db)==before and set(db.get_bind().dialect.get_table_names(db.connection()))=={'insights_snapshots'}
    monkeypatch.delenv('FINNHUB_PAID_READERS_ENABLED')
    assert analyst.current_consensus_payload(db,'AAPL')['currentSnapshot']['priceTargetRange']['consensus'] is None


def test_target_only_does_not_invent_recommendation_snapshot_date(db,monkeypatch):
    from app.services import replacement_analysts
    monkeypatch.delenv('FINNHUB_API_KEY',raising=False)
    stage(db,monkeypatch,'targets')
    result=replacement_analysts.current_payload(db,'AAPL',include_details=True)
    assert result['currentSnapshot']['snapshotDate'] is None
    assert result['currentSnapshot']['recommendationLabel'] is None
    assert result['currentSnapshot']['targetHorizon']=='unverified'
    assert result['availability']['status']=='partial'


def test_old_targets_are_held_even_when_cache_is_new(db,monkeypatch):
    stage(db,monkeypatch,'targets',lastUpdated=(NOW.date()-timedelta(days=46)).isoformat())
    assert reader.attach_targets(db,'AAPL',{},include_details=True,now=NOW)=={}


def test_research_cannot_turn_unverified_quote_basis_into_forward_pe():
    from app.services.research_briefs import _deterministic_valuation
    values={'forecasts':{'nextFiscalYear':{'epsEstimate':4,'period':'FY 2027','forwardValuationEligible':False}}}
    assert 'forward_pe' not in _deterministic_valuation({'price':100},values,{})
    del values['forecasts']['nextFiscalYear']['forwardValuationEligible']
    assert _deterministic_valuation({'price':100},values,{})['forward_pe']['value']==25


def test_preparation_reuses_share_class_alias_without_paid_refetch(db,monkeypatch):
    stage(db,monkeypatch,symbol='BRK-B')
    before=fingerprint(db)
    assert prep.prepare(db,'BRK.B','eps',frequency='quarterly',observed_at=NOW)['status']=='cached'
    assert fingerprint(db)==before


def test_annual_zero_and_absent_quarter_are_same_fiscal_period(db,monkeypatch):
    stage(db,monkeypatch,'eps','annual',quarter=None)
    stage(db,monkeypatch,'revenue','annual',quarter=0)
    result=reader.forecast(db,'AAPL','annual',now=NOW)
    assert result['epsEstimate']==0 and result['revenueEstimate']==200 and result['fiscalQuarter'] is None


def test_research_reads_same_source_and_does_not_call_blended_average_consensus(db,monkeypatch):
    from app.services import research_briefs as research
    monkeypatch.setenv('FINANCIAL_STATEMENTS_PROVIDER','sec_edgar')
    source=panel()
    db.add(InsightsSnapshot(kind='sec-financials:AAPL:v1',source='sec_edgar',fetched_at=NOW,payload_json=json.dumps(source)))
    db.commit()
    stage(db,monkeypatch,'eps','annual',epsLow=2,epsAvg=3,epsHigh=4)
    stage(db,monkeypatch,'revenue','annual')
    before=fingerprint(db)
    result=research._cached_financials_snapshot(db,'AAPL')
    forecast=result['forecasts']['nextFiscalYear']
    assert forecast['epsEstimate']==3
    assert forecast['sourceEvidence']['eps']['sourceSha256']
    assert forecast['sourceEvidence']['eps']['observedAt']==NOW.isoformat()
    assert forecast['forwardValuationEligible'] is False
    assert research._deterministic_valuation({'price':100},result,{})=={}
    available=research._research_data_availability({'financials':result},{})
    assert available['revenue'] and not available['revenue consensus'] and not available['eps consensus']
    assert fingerprint(db)==before
    monkeypatch.delenv('FINNHUB_PAID_READERS_ENABLED')
    assert research._cached_financials_snapshot(db,'AAPL')['forecasts']['nextFiscalYear'] is None


def test_compare_reader_keeps_target_entitlement_and_symbol_deduplication(db,monkeypatch):
    from app.services import analyst_consensus as analyst
    monkeypatch.setenv('ANALYST_PROVIDER','finnhub')
    recommendation(db);stage(db,monkeypatch,'targets')
    before=fingerprint(db)
    paid=analyst.compare_consensus_payload(db,['AAPL','aapl'],include_details=True)
    assert paid['symbols']==['AAPL'] and paid['items']['AAPL']['currentSnapshot']['priceTargetRange']['consensus']==150
    free=analyst.compare_consensus_payload(db,['AAPL','aapl'],include_details=False)
    assert 'priceTargetRange' not in free['items']['AAPL']['currentSnapshot'] and free['access']['detailsLocked']
    assert fingerprint(db)==before


def test_unlabelled_legacy_consensus_and_direct_official_facts_keep_availability():
    from app.services.research_briefs import _research_data_availability
    primary={'financials':{'forecasts':{'nextQuarter':{'epsEstimate':0,'revenueEstimate':200}}}}
    available=_research_data_availability(primary,{})
    assert available['eps consensus'] and available['revenue consensus']
    official={'official_facts':{'current_eps_consensus':{'value':0},'current_revenue_consensus':{'value':200}}}
    available=_research_data_availability({},official)
    assert available['eps consensus'] and available['revenue consensus']
