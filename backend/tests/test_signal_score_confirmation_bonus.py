from datetime import datetime, timezone

from app.services.signal_score import calculate_smart_score


def test_confirmation_bonus_applies_additively() -> None:
    ts = datetime.now(timezone.utc)
    base_score, _ = calculate_smart_score(unusual_multiple=5.0, amount_max=500_000, ts=ts)

    confirmed_score, _ = calculate_smart_score(
        unusual_multiple=5.0,
        amount_max=500_000,
        ts=ts,
        confirmation_30d={
            "cross_source_confirmed_30d": True,
            "repeat_insider_30d": True,
            "repeat_congress_30d": True,
        },
    )

    assert confirmed_score == base_score + 10


def test_repeat_flags_without_cross_source_get_smaller_bonus() -> None:
    ts = datetime.now(timezone.utc)
    base_score, _ = calculate_smart_score(unusual_multiple=3.0, amount_max=100_000, ts=ts)

    repeated_score, _ = calculate_smart_score(
        unusual_multiple=3.0,
        amount_max=100_000,
        ts=ts,
        confirmation_30d={
            "cross_source_confirmed_30d": False,
            "repeat_insider_30d": True,
            "repeat_congress_30d": True,
        },
    )

    assert repeated_score == base_score + 4


def test_confirmation_bonus_is_capped_by_max_score() -> None:
    ts = datetime.now(timezone.utc)

    score, band = calculate_smart_score(
        unusual_multiple=30.0,
        amount_max=1_000_000,
        ts=ts,
        confirmation_30d={
            "cross_source_confirmed_30d": True,
            "repeat_insider_30d": True,
            "repeat_congress_30d": True,
        },
    )

    assert score == 100
    assert band == "strong"
