from copy import deepcopy
import json
import pytest
from test_sec_foreign_statements import sources, project, NOW
from app.services import sec_foreign_statements as foreign, sec_financial_statements as statements
from app.clients.direct_sources import DirectSourceError
from types import SimpleNamespace


@pytest.mark.parametrize('symbol,currency', [('ASML','EUR'),('SAP','EUR'),('NVS','USD')])
def test_reviewed_currency_and_definition_preserve_partial_contract(symbol,currency):
    data,company,basis=sources(symbol)
    # The common fixture adds USD convenience figures; NVS actually reports USD.
    if currency=='USD':
        for metric,tags in basis['tags'].items():
            expected=dict(revenue=100,net_income=20,gross_profit=60,operating_income=30,operating_cash_flow=40,capex=10)[metric]
            for row in data['facts'][basis['namespace']][tags[0]]['units']['USD']:
                row['val']=expected if row['end']=='2025-12-31' else expected*.8
    original=deepcopy(data)
    result=project(data,company,symbol)
    assert result['annual'][-1]['revenue']==100
    assert result['annual'][-1]['freeCashFlow']==30
    assert result['reportingCurrency']==currency
    assert result['annual'][-1]['currency']==currency
    assert not result['quarterly'] and all(v is None for v in result['summary'].values())
    assert result['annual'][-1]['eps'] is None
    assert result['sourceEvidence']['reviewedReportingBasis']['proof_sha256']==basis['proof_sha256']
    assert project(data,company,symbol)==result and data==original
    row=SimpleNamespace(source='sec_edgar',fetched_at=NOW,payload_json=json.dumps(result))
    assert statements.cached_row_payload(row,symbol,now=NOW)==result
    result.pop('preparationVersion');row.payload_json=json.dumps(result)
    assert statements.cached_row_payload(row,symbol,now=NOW) is None


@pytest.mark.parametrize('symbol', ['ASML','SAP','NVS'])
def test_wrong_issuer_cannot_reuse_review(symbol):
    with pytest.raises(DirectSourceError): foreign.profile(symbol,'1')


def test_overrides_do_not_change_other_issuers_or_returned_profiles():
    original=deepcopy(foreign.IFRS)
    sap=foreign.profile('SAP','1000184');nvs=foreign.profile('NVS','1114448')
    assert sap['tags']['capex']!=original['capex']
    assert nvs['tags']['revenue']==('RevenueFromSaleOfGoods',)
    assert nvs['tags']['operating_cash_flow']==('CashFlowsFromUsedInOperatingActivitiesContinuingOperations',)
    sap['tag_overrides']['capex'].append('Revenue')
    sap['tags']['revenue']=('OtherRevenue',)
    assert foreign.IFRS==original
    assert foreign.profile('NVO','353278')['tags']==original
    assert 'Revenue' not in foreign.profile('SAP','1000184')['tag_overrides']['capex']


@pytest.mark.parametrize('symbol', ['SAP','NVS'])
def test_consolidated_cash_never_fills_missing_continuing_cash(symbol):
    data,company,basis=sources(symbol)
    tags=data['facts'][basis['namespace']]
    cash=tags.pop('CashFlowsFromUsedInOperatingActivitiesContinuingOperations')
    tags['CashFlowsFromUsedInOperatingActivities']=cash
    row=project(data,company,symbol)['annual'][-1]
    assert row['operatingCashFlow'] is None and row['freeCashFlow'] is None


def test_novartis_other_revenue_cannot_become_net_sales():
    data,company,basis=sources('NVS')
    tags=data['facts']['ifrs-full']
    tags['OtherRevenue']=tags.pop('RevenueFromSaleOfGoods')
    assert project(data,company,'NVS')['annual']==[]


def test_sap_combined_purchases_do_not_fall_back_to_different_capex_basis():
    data,company,basis=sources('SAP')
    tags=data['facts']['ifrs-full']
    tags[foreign.IFRS['capex'][0]]=tags.pop(basis['tags']['capex'][0])
    row=project(data,company,'SAP')['annual'][-1]
    assert row['capex'] is None and row['freeCashFlow'] is None
