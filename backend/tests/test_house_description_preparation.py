"""Original-bound description enrichment preserves economics, identity and holds."""
import copy
import json
import pytest
from app.clients import house_ptr_descriptions as descriptions
from app.clients import house_ptr_preparation as prep
from app.clients.direct_sources import DirectSourceError
from app.services import direct_feed_collection as collection
from test_house_scan_preparation import source


def test_original_review_corrects_descriptions_without_inventing_stock_identity(source, monkeypatch):
    raw, meta, _ = source
    _, enriched, holds = collection.parse_document('house_ptr', raw, meta)
    assert holds == [prep.HOLD]
    rows = {r['source_line_ref']: r for r in enriched['source_report']['transactions']}
    assert rows['p13:r6']['assetDescription'] == 'ROLLINS INC REGOFF SCTASK0795638'
    assert rows['p20:r9']['assetDescription'] == 'LENNOX INTERNATIONAL INC CMN'
    for key in ('p23:r9', 'p29:r7'):
        assert rows[key]['description_reviewed'] is False
        assert rows[key]['description_status'] == 'unclear_printed_characters'
        assert rows[key]['assetDescription'] == rows[key]['candidate_asset_description']
    assert sum(r['description_reviewed'] for r in rows.values()) == 457
    assert rows['p2:r7']['disclosed_instrument_indicator'] == 'option'
    assert rows['p25:r16']['disclosed_instrument_indicator'] == 'exchange_traded_fund'
    for number in range(1, 5):
        assert rows[f'p30:r{number+7}']['disclosed_trust_heading'] != rows[f'p31:r{number}']['disclosed_trust_heading']
        assert rows[f'p30:r{number+7}']['disclosed_account_heading'] == rows[f'p31:r{number}']['disclosed_account_heading'] == 'MURA Holdings'
    assert collection.parse_document('house_ptr', raw, meta)[1] == enriched
    monkeypatch.setattr(descriptions, 'reviewed_descriptions', lambda _: ({}, None))
    _, baseline, baseline_holds = collection.parse_document('house_ptr', raw, meta)
    assert baseline_holds == holds
    # Compare every actual normalized row, not just counts. Only two original
    # name corrections may differ; no amount/date/owner/hash/ticker may change.
    changed = []
    for before, after in zip(baseline['transactions'], enriched['transactions'], strict=True):
        differences = {k for k in before.keys() | after.keys() if before.get(k) != after.get(k)}
        if differences:
            assert differences <= {'issuer_name_raw', 'security_name_raw'}
            changed.append(after['source_line_ref'])
        assert after['ticker_normalized'] is None
        assert after['asset_type_normalized'] == 'unresolved'
    assert changed == ['p13:r6', 'p20:r9']


@pytest.mark.parametrize('mutation', ['metadata','page_count','missing_row','duplicate_row','candidate',
    'position','unknown_status','unclear_guess','matched_changed','correction_no_note','ticker',
    'identity','eligible','scope','kind','trust','mapping_count','bool_mapping_count'])
def test_invalid_description_review_cannot_enrich_or_release_source(source,tmp_path,monkeypatch,mutation):
    raw,meta,_=source
    review=json.loads(descriptions.REGISTRY.read_bytes())
    row=review['rows'][0]
    if mutation=='metadata':review['metadata']['filing_id']='different'
    elif mutation=='page_count':review['page_count']=30
    elif mutation=='missing_row':review['rows'].pop()
    elif mutation=='duplicate_row':review['rows'][1]=copy.deepcopy(row)
    elif mutation=='candidate':row['candidate_description']='Other issuer'
    elif mutation=='position':row['source_line_ref']='p2:r2'
    elif mutation=='unknown_status':row['description_status']='approved_stock'
    elif mutation=='unclear_guess':row['description_status']='unclear_printed_characters'
    elif mutation=='matched_changed':row['reviewed_description']='Other issuer'
    elif mutation=='correction_no_note':row.update(description_status='corrected_from_original',reviewed_description='Other issuer',description_note=None)
    elif mutation=='ticker':row['mapped_ticker']='MSFT'
    elif mutation=='identity':row['security_identity_status']='verified'
    elif mutation=='eligible':row['publication_eligible']=True
    elif mutation=='scope':review['review_scope']='complete'
    elif mutation=='kind':row['disclosed_instrument_indicator']='stock'
    elif mutation=='trust':row['trust_heading']=''
    elif mutation=='mapping_count':review['security_mappings_approved']=1
    else:review['security_mappings_approved']=False
    path=tmp_path/'descriptions.json';path.write_text(json.dumps(review),encoding='utf-8')
    monkeypatch.setattr(descriptions,'REGISTRY',path)
    with pytest.raises(DirectSourceError):collection.parse_document('house_ptr',raw,meta)


def test_other_original_never_inherits_description_review(source):
    _,_,registry=source
    other=copy.deepcopy(registry['preparations'][0]);other['source_sha256']='a'*64
    assert descriptions.reviewed_descriptions(other) == ({},None)
