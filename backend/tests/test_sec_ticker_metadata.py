import copy
import pytest
from test_sec_metadata_consumers import db, seed
from test_ticker_context_canonical_projection import _canonical_payload
import app.main as main


def forbidden(*args, **kwargs):
    pytest.fail('Legacy identity or live provider must not be read')


def test_selected_shell_and_peer_do_not_mix_legacy_classification(db, monkeypatch):
    seed(db)
    monkeypatch.setattr(main, '_optional_identity_row', forbidden)
    monkeypatch.setattr(main, '_ticker_content_profile_identity_row', forbidden)
    monkeypatch.setattr(main, '_peer_compare_ticker_meta_row', forbidden)
    monkeypatch.setattr(main, '_company_profile_snapshot_from_fmp', forbidden)
    result=main._ticker_shell_identity_fields(db=db,symbol='ZBEX',security=None,meta=None,
        fundamentals=None,profile_snapshot={'sector':'Legacy sector','industry':'Legacy industry'})
    assert result['sector'] is result['country'] is None
    assert result['industry']=='EXAMPLE SIC INDUSTRY'
    assert result['industry_source']=='SEC SIC'
    assert result['display_market_chain']=='SEC SIC: EXAMPLE SIC INDUSTRY / Nasdaq'
    assert main._resolve_ticker_company_metadata(db,'ZBEX')['industry']==result['industry']
    assert main._peer_compare_identity(db,'ZBEX',None)['company_name']=='Zebra Example Inc'
    assert main._resolve_ticker_page_name(db,'ZBEX')=='Zebra Example Inc'


@pytest.mark.parametrize('symbol,company,expected',[('ZBEX',True,'EXAMPLE SIC INDUSTRY'),('ZBEX',False,None),('MISSING',True,None)])
def test_cached_context_overlay_keeps_price_and_access_and_original_cache(db,symbol,company,expected):
    seed(db,company=company)
    canonical=_canonical_payload()
    canonical['ticker']={'symbol':symbol,'name':'Legacy','sector':'Legacy sector','industry':'Legacy industry','price':123}
    canonical['identity']={'symbol':symbol,'company_name':'Legacy','sector':'Legacy sector','market_cap':999}
    original=copy.deepcopy(canonical)
    result=main._project_ticker_context_bundle_for_entitlements(canonical,symbol=symbol,db=db,
        source_entitlements=main._ticker_context_source_entitlements(None,authenticated=False))
    assert result['ticker']['sector'] is None and result['ticker']['industry']==expected
    assert result['ticker']['price']==123 and result['identity']['market_cap']==999
    assert result['identity']['company_name']==('Zebra Example Inc' if symbol=='ZBEX' else symbol)
    assert result['signals_summary']['items']==[]
    assert canonical==original
    assert main._apply_selected_ticker_identity(result,db,symbol)==result


def test_inactive_selection_leaves_payload_unchanged(db,monkeypatch):
    monkeypatch.setenv('COMPANY_METADATA_PROVIDER','fmp')
    original={'ticker':{'symbol':'ZBEX','name':'Legacy','industry':'Legacy'}}
    assert main._apply_selected_ticker_identity(original,db,'ZBEX') is original
