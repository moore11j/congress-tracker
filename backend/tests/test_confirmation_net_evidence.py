from copy import deepcopy

import pytest

from app.services.confirmation_score import confirmation_score_bundle_from_source_payloads


def source(side, strength=100, quality=100, age=1):
    return dict(present=True, direction=side, strength=strength, quality=quality,
                freshness_days=age, score_contribution=0, label='Fixture')


def calculate(sources):
    return confirmation_score_bundle_from_source_payloads('TEST', sources_payload=sources)


@pytest.mark.parametrize('side,opposite', [('bullish', 'bearish'), ('bearish', 'bullish')])
@pytest.mark.parametrize('key', ['congress', 'insiders', 'signals', 'price_volume',
                                  'options_flow', 'macro_positioning', 'government_contracts'])
def test_opposition_never_adds_points_and_strengthening_it_never_raises_confirmation(side, opposite, key):
    inputs = {k: source(side) for k in ('fundamentals', 'institutional_activity', 'analysts')}
    baseline = calculate(inputs)
    inputs[key] = source(opposite, strength=40, quality=40)
    weak = calculate(inputs)
    inputs[key] = source(opposite)
    strong = calculate(inputs)
    assert baseline['direction'] == weak['direction'] == strong['direction'] == side
    assert strong['score'] <= weak['score'] <= baseline['score']
    assert strong['score_calculation']['raw_score'] < weak['score_calculation']['raw_score'] < baseline['score_calculation']['raw_score']
    assert strong['sources'][key]['confirmation_contribution'] < 0
    assert strong['sources'][key]['confirmation_evidence_weight'] > 0


@pytest.mark.parametrize('state', ['mixed', 'neutral', 'missing', 'stale', 'future'])
def test_non_directional_and_ineligible_sources_add_no_credit(state):
    inputs = {'fundamentals': source('bullish'), 'institutional_activity': source('bullish'),
              'congress': source('bearish')}
    baseline = calculate(inputs)
    extra = source(state if state in ('mixed', 'neutral') else 'bullish')
    if state == 'missing':
        extra['present'] = False
    if state in ('stale', 'future'):
        extra['freshness_days'] = 91 if state == 'stale' else -1
    inputs['price_volume'] = extra
    result = calculate(inputs)
    assert result['score'] == baseline['score']
    assert result['sources']['price_volume']['confirmation_contribution'] == 0


def test_ba_release_inputs_have_signed_auditable_net_score():
    inputs = {
        'fundamentals': source('bullish', 49, 67, 2),
        'institutional_activity': source('bullish', 100, 92, 21),
        'analysts': source('bullish', 42, 69, 0),
        'congress': source('bearish', 81, 92, 14),
        'macro_positioning': source('bearish', 50, 61, 22),
        'price_volume': source('mixed', 25, 82, 0),
    }
    result = calculate(inputs)
    math = result['score_calculation']
    assert result['direction'] == 'bullish'
    assert result['score'] == 48
    assert math['net_weight'] == pytest.approx(math['aligned_weight'] - math['opposing_weight'], abs=.0001)
    assert sum(s['confirmation_contribution'] for s in result['sources'].values()) == pytest.approx(math['net_weight'], abs=.001)
    assert result['sources']['congress']['confirmation_contribution'] < 0
    assert result['sources']['macro_positioning']['confirmation_contribution'] < 0
    assert result['sources']['price_volume']['confirmation_contribution'] == 0
    rebuilt = calculate(deepcopy(result['sources']))
    assert rebuilt['score_calculation'] == math


def test_single_source_cannot_claim_full_confirmation_and_mixed_only_has_none():
    single = calculate({'fundamentals': source('bullish')})
    assert single['score'] == 39
    assert single['score_calculation']['single_source_cap_applied'] is True
    mixed = calculate({'fundamentals': source('mixed'), 'price_volume': source('mixed')})
    assert mixed['score'] == 0
    assert all(s['confirmation_contribution'] == 0 for s in mixed['sources'].values())


def test_source_availability_rebuild_refreshes_signed_contributions():
    from app.main import _mark_institutional_unavailable_in_confirmation_bundle
    inputs = {'fundamentals': source('bullish'), 'institutional_activity': source('bullish'),
              'congress': source('bearish')}
    before = calculate(inputs)
    after = _mark_institutional_unavailable_in_confirmation_bundle(
        before, {'status': 'unavailable'}, {'institutional_activity': {'locked': False}})
    assert after['score'] < before['score']
    assert after['sources']['institutional_activity']['confirmation_contribution'] == 0
    assert after['sources']['institutional_activity']['status'] == 'unavailable'
    assert after['sources']['congress']['confirmation_contribution'] == before['sources']['congress']['confirmation_contribution']
    assert sum(s['confirmation_contribution'] for s in after['sources'].values()) == pytest.approx(after['score_calculation']['net_weight'], abs=.001)


def test_full_confirmation_requires_every_weighted_source_at_full_strength():
    from app.services.confirmation_evidence import SOURCE_MAX_POINTS
    assert sum(SOURCE_MAX_POINTS.values()) == 100
    inputs = {key: source('bullish') for key in SOURCE_MAX_POINTS}
    assert calculate(inputs)['score'] == 100
    for key, maximum in SOURCE_MAX_POINTS.items():
        missing = deepcopy(inputs)
        del missing[key]
        assert calculate(missing)['score'] == min(99, round(100 * ((100 - maximum) / 100) ** .5))
        for state in ('mixed', 'neutral'):
            incomplete = deepcopy(inputs)
            incomplete[key]['direction'] = state
            assert calculate(incomplete)['score'] == calculate(missing)['score']
    almost = deepcopy(inputs)
    almost['government_contracts']['strength'] = 99
    assert calculate(almost)['score'] == 99  # Rounding cannot manufacture 100.


def test_two_unanimous_low_weight_sources_cannot_compete_with_broad_confirmation():
    thin = calculate({'analysts': source('bullish'), 'macro_positioning': source('bullish')})
    broad = calculate({key: source('bullish') for key in ('fundamentals', 'institutional_activity', 'price_volume', 'congress', 'insiders', 'analysts')})
    assert thin['score'] == 33
    assert broad['score'] == 94
    assert thin['score_calculation']['aligned_source_count'] == 2
    assert broad['score_calculation']['aligned_source_count'] == 6


def test_paid_redaction_preserves_canonical_score_but_removes_locked_math():
    from app.services.confirmation_score import redact_confirmation_bundle_sources
    bundle = calculate({key: source('bullish') for key in ('fundamentals', 'institutional_activity', 'price_volume')})
    redacted = redact_confirmation_bundle_sources(bundle, {'institutional_activity'})
    assert redacted['score'] == bundle['score'] == 79
    assert redacted['score_calculation'] is None
    assert redacted['sources']['institutional_activity']['strength'] is None


def test_fundamentals_transfer_is_equal_across_all_other_sources():
    from app.services.confirmation_evidence import SOURCE_MAX_POINTS
    previous = dict(fundamentals=20, institutional_activity=20, price_volume=15,
                    congress=10, insiders=10, analysts=8, signals=5, options_flow=5,
                    macro_positioning=5, government_contracts=2)
    assert SOURCE_MAX_POINTS['fundamentals'] == 30
    assert sum(SOURCE_MAX_POINTS.values()) == pytest.approx(100)
    for key, maximum in previous.items():
        if key != 'fundamentals':
            assert maximum - SOURCE_MAX_POINTS[key] == pytest.approx(10 / 9)
    full = calculate({key: source('bullish') for key in previous})
    assert full['score_calculation']['raw_score'] == 100
    assert full['score'] == 100


@pytest.mark.parametrize('symbol, inputs, expected', [
    ('TSM', dict(fundamentals=('bullish',64,69,0), price_volume=('bullish',75,82,1),
                 congress=('bullish',50,57,3), insiders=('bullish',93,92,19),
                 analysts=('bullish',44,69,0), macro_positioning=('bullish',70,77,11)), 80),
    ('BRK-B', dict(fundamentals=('bullish',55,67,5), price_volume=('mixed',25,82,1),
                   congress=('bullish',50,57,16), insiders=('bullish',100,92,5),
                   analysts=('bullish',25,58,0), macro_positioning=('bullish',70,77,32),
                   signals=('bullish',66,82,5)), 74),
    ('AZO', dict(analysts=('bullish',42,69,0), macro_positioning=('bullish',80,85,32),
                 fundamentals=('mixed',29,57,12), price_volume=('mixed',25,82,1)), 30),
])
def test_approved_saved_examples_use_same_formula_for_every_symbol(symbol, inputs, expected):
    payload = {key: source(*values) for key, values in inputs.items()}
    bundle = confirmation_score_bundle_from_source_payloads(symbol, sources_payload=payload)
    assert bundle['score'] == expected
    assert calculate(payload)['score'] == expected
    assert bundle['score_calculation']['agreement'] == 1


def test_empty_and_conflicted_sources_cannot_earn_agreement_or_coverage():
    from app.services.confirmation_evidence import net_confirmation
    for direction in ('neutral', 'mixed'):
        result = net_confirmation({'fundamentals': source('bullish'), 'institutional_activity': source('bearish')}, direction)
        assert result['score'] == result['agreement'] == result['weighted_coverage'] == 0
    assert net_confirmation({}, 'bullish')['score'] == 0


def test_stronger_aligned_evidence_improves_quality_without_inventing_coverage():
    weak = calculate({'fundamentals': source('bullish', 40, 40), 'price_volume': source('bullish', 40, 40)})
    strong = calculate({'fundamentals': source('bullish'), 'price_volume': source('bullish')})
    assert strong['score'] > weak['score']
    assert strong['score_calculation']['weighted_coverage'] == weak['score_calculation']['weighted_coverage']
    assert strong['score_calculation']['evidence_quality'] > weak['score_calculation']['evidence_quality']
