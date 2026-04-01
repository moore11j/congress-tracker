from datetime import datetime, timezone

from app.services.government_exposure import GovernmentExposureSummary, government_exposure_signal_boost
from app.services.signal_score import MAX_GOVERNMENT_EXPOSURE_BONUS, calculate_smart_score


def _summary(
    *,
    has_exposure: bool,
    level: str | None,
    recent_awards: bool | None,
) -> GovernmentExposureSummary:
    return GovernmentExposureSummary(
        symbol="PLTR",
        has_government_exposure=has_exposure,
        contract_exposure_level=level,
        recent_award_activity=recent_awards,
        summary_label="",
        source_context="",
        confidence="observed" if has_exposure else "none",
        as_of="2026-03-31",
    )


def test_government_exposure_boost_ranks_otherwise_equal_signal_slightly_higher() -> None:
    ts = datetime.now(timezone.utc)
    base_score, _ = calculate_smart_score(unusual_multiple=5.0, amount_max=500_000, ts=ts)

    government_boost = government_exposure_signal_boost(
        _summary(has_exposure=True, level=None, recent_awards=None)
    )
    boosted_score, _ = calculate_smart_score(
        unusual_multiple=5.0,
        amount_max=500_000,
        ts=ts,
        government_exposure_signal_boost=government_boost,
    )

    assert boosted_score > base_score
    assert boosted_score - base_score == 1


def test_government_exposure_bonus_is_modest_and_bounded() -> None:
    ts = datetime.now(timezone.utc)
    base_score, _ = calculate_smart_score(unusual_multiple=5.0, amount_max=500_000, ts=ts)

    max_raw_boost = government_exposure_signal_boost(
        _summary(has_exposure=True, level="high", recent_awards=True)
    )
    boosted_score, _ = calculate_smart_score(
        unusual_multiple=5.0,
        amount_max=500_000,
        ts=ts,
        government_exposure_signal_boost=max_raw_boost,
    )

    assert boosted_score - base_score <= MAX_GOVERNMENT_EXPOSURE_BONUS
    assert boosted_score - base_score == MAX_GOVERNMENT_EXPOSURE_BONUS


def test_government_exposure_bonus_not_applied_without_exposure() -> None:
    ts = datetime.now(timezone.utc)
    base_score, _ = calculate_smart_score(unusual_multiple=3.0, amount_max=100_000, ts=ts)

    no_exposure_boost = government_exposure_signal_boost(
        _summary(has_exposure=False, level=None, recent_awards=False)
    )
    no_exposure_score, _ = calculate_smart_score(
        unusual_multiple=3.0,
        amount_max=100_000,
        ts=ts,
        government_exposure_signal_boost=no_exposure_boost,
    )

    assert no_exposure_boost == 0.0
    assert no_exposure_score == base_score


def test_government_exposure_bonus_respects_score_cap() -> None:
    ts = datetime.now(timezone.utc)
    max_raw_boost = government_exposure_signal_boost(
        _summary(has_exposure=True, level="high", recent_awards=True)
    )

    score, band = calculate_smart_score(
        unusual_multiple=30.0,
        amount_max=1_000_000,
        ts=ts,
        confirmation_30d={
            "cross_source_confirmed_30d": True,
            "repeat_insider_30d": True,
            "repeat_congress_30d": True,
        },
        government_exposure_signal_boost=max_raw_boost,
    )

    assert score == 100
    assert band == "strong"
