"""Shared rounding reconciliation keeps original values and rejects real conflicts."""
from itertools import permutations
from pathlib import Path
import gzip
import hashlib
import json
import pytest
from app.services import sec_inline_financials as inline, sec_document_sets as sets
from test_sec_inline_financials import document, fact, parse, TAG


def numeric(value, decimals, *, tag=TAG, identifier="fact", ctx="parent"):
    sign = ' sign="-"' if value < 0 else ''
    accuracy = '' if decimals is None else f' decimals="{decimals}"'
    return fact(str(abs(value)), tag=tag, ctx=ctx, attrs=f'id="{identifier}"{accuracy}{sign}')


@pytest.mark.parametrize('tag',[TAG,'NetIncomeLoss','AssetsCurrent','StockholdersEquity','NetCashProvidedByUsedInOperatingActivities'])
@pytest.mark.parametrize('reverse',[False,True])
def test_rounded_core_duplicate_uses_exact_source_value_with_original_provenance(tag,reverse):
    balance=tag in {'AssetsCurrent','StockholdersEquity'}
    ctx='balance' if balance else 'parent'
    rows=[numeric(-139478000,'-3',tag=tag,identifier='statement',ctx=ctx),numeric(-139500000,'-5',tag=tag,identifier='narrative',ctx=ctx)]
    if reverse:rows.reverse()
    balance_context='<xbrli:context id="balance"><xbrli:entity><xbrli:identifier scheme="http://www.sec.gov/CIK">1</xbrli:identifier></xbrli:entity><xbrli:period><xbrli:instant>2026-06-30</xbrli:instant></xbrli:period></xbrli:context>' if balance else ''
    raw=document(balance_context+''.join(rows)) if tag!=TAG else document(primary=''.join(rows))
    result=parse(raw)
    assert result['facts'][tag]['units']['USD'][0]['val']==-139478000
    proof=result['provenance'][(tag,'USD',None if balance else '2026-01-01','2026-06-30')]
    assert proof['fact_id']=='statement' and proof['source_sha256']==hashlib.sha256(raw).hexdigest()
    evidence=result['evidence']['core_rounded_duplicates']
    assert evidence['methodology']=='contained_reported_precision_v1'
    assert evidence['comparisons'][0]['selected_value']==-139478000
    assert 'auxiliary_rounded_duplicates' not in result['evidence']


@pytest.mark.parametrize('order',list(permutations([(139478000,'-3'),(139480000,'-4'),(139500000,'-5')])))
def test_three_precisions_do_not_depend_on_document_order(order):
    result=parse(document(primary=''.join(numeric(v,d,identifier=str(i)) for i,(v,d) in enumerate(order))))
    assert result['facts'][TAG]['units']['USD'][0]['val']==139478000


@pytest.mark.parametrize('order',list(permutations([(100,'INF'),(100,'-2'),(101,'-2')])))
def test_precise_middle_fact_cannot_mask_conflicting_coarse_values(order):
    with pytest.raises(inline.InlineFactsError,match='conflicting_reported_facts'):
        parse(document(primary=''.join(numeric(v,d) for v,d in order)))


@pytest.mark.parametrize('other,decimals',[(139600000,'-5'),(139500000,'-3'),(139500000,None),
    (139500000,'NaN'),(139500000,'-21'),(139500000,'INF'),(-139500000,'-5')])
@pytest.mark.parametrize('reverse',[False,True])
def test_unknown_precision_true_conflict_or_wrong_sign_never_becomes_a_tolerance(other,decimals,reverse):
    rows=[numeric(139478000,'-3'),numeric(other,decimals)]
    if reverse:rows.reverse()
    with pytest.raises(inline.InlineFactsError,match='conflicting_reported_facts'):
        parse(document(primary=''.join(rows)))


@pytest.mark.parametrize('order',list(permutations([(100,None),(100,'INF'),(101,'-2')])))
def test_unknown_accuracy_cannot_be_discarded_before_conflicting_duplicate(order):
    with pytest.raises(inline.InlineFactsError,match='conflicting_reported_facts'):
        parse(document(primary=''.join(numeric(v,d) for v,d in order)))


@pytest.mark.parametrize('order',list(permutations([(100,'-2'),(100,'INF'),(100,'0')])))
def test_equal_value_keeps_finest_source_provenance(order):
    result=parse(document(primary=''.join(numeric(v,d,identifier=d) for v,d in order)))
    assert next(iter(result['provenance'].values()))['fact_id']=='INF'
    assert 'core_rounded_duplicates' not in result['evidence']


def test_partial_overlap_is_still_held_by_conservative_containment_policy():
    # XBRL allows these overlapping intervals; this reader intentionally requires
    # containment before replacing a source value with a finer occurrence.
    with pytest.raises(inline.InlineFactsError,match='conflicting_reported_facts'):
        parse(document(primary=numeric(2500,'-2')+numeric(2000,'-3')))


def originals():
    folder=Path(__file__).parent/'fixtures/sec_core_precision'
    manifest=json.loads((folder/'sources.json').read_text())
    result={}
    for name,meta in manifest.items():
        raw=gzip.decompress((folder/(name+'.htm.gz')).read_bytes())
        assert hashlib.sha256(raw).hexdigest()==meta['sha256'] and len(raw)==meta['bytes']
        result[name]=raw
    return manifest,result


def test_complete_original_recovers_rounded_core_facts_without_loosening_source_discovery():
    manifest,raw=originals();m=manifest['RZLV-statement']
    args={k:m[k] for k in ('cik','accession','filed','report_date','form','url')}
    args.update(cover_raw=raw['RZLV-cover'],cover_url=manifest['RZLV-cover']['url'],document_set=True)
    result=inline.parse(raw['RZLV-statement'],**args)
    assert result['evidence']['reported_facts']==35
    losses=result['facts']['NetIncomeLoss']['units']['USD']
    assert next(x['val'] for x in losses if x.get('start')=='2026-01-01' and x['end']=='2026-06-30')==-139478000
    assert result['evidence']['core_rounded_duplicates']['comparisons']
    assert inline.parse(raw['RZLV-statement'],**args)==result
    # Multiple issuer/acquisition exhibits still require a separately proved
    # selection rule. This parser repair alone does not activate this source.
    links=sets.statement_exhibits(raw['RZLV-cover'],{'cover_url':manifest['RZLV-cover']['url']})
    assert len(links)>1


def test_complete_original_real_conflict_remains_held():
    manifest,raw=originals();m=manifest['RZLV-statement']
    args={k:m[k] for k in ('cik','accession','filed','report_date','form','url')}
    args.update(cover_raw=raw['RZLV-cover'],cover_url=manifest['RZLV-cover']['url'],document_set=True)
    assert b'>139.5<' in raw['RZLV-statement']
    damaged=raw['RZLV-statement'].replace(b'>139.5<',b'>140.5<')
    with pytest.raises(inline.InlineFactsError,match='conflicting_reported_facts'):
        inline.parse(damaged,**args)
