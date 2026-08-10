from __future__ import annotations

from app.strategy_research.candidate_strategy_validation import walnut_strategy_score
from app.strategy_research.candidate_strategy_validation import walnut_strategy_score_from_validation_result
from app.strategy_research.candidate_strategy_validation import _candidate_lookup


def _perf(
    *,
    cagr: float,
    alpha: float,
    lots: int,
    sharpe: float = 1.0,
    drawdown: float = -20.0,
    rolling: float = 60.0,
    status: str = "ok",
) -> dict[str, object]:
    return {
        "status": status,
        "cagr_pct": cagr,
        "alpha_cagr_pct": alpha,
        "lots": lots,
        "sharpe": sharpe,
        "max_drawdown_pct": drawdown,
        "rolling_12m_beating_spy_pct": rolling,
    }


def test_walnut_strategy_score_rewards_clean_out_of_sample_results():
    score = walnut_strategy_score(
        full=_perf(cagr=24, alpha=10, lots=1600, sharpe=1.1),
        validation=_perf(cagr=20, alpha=8, lots=400, sharpe=1.0),
        test=_perf(cagr=22, alpha=9, lots=300, sharpe=1.2, rolling=70),
        diagnostics={"concentration_flags": []},
    )

    assert score["score"] > 60
    assert score["penalties"] == []
    assert score["components"]["concentration_quality"] == 100.0


def test_walnut_strategy_score_penalizes_concentration_and_negative_test_alpha():
    score = walnut_strategy_score(
        full=_perf(cagr=35, alpha=15, lots=70, sharpe=1.4),
        validation=_perf(cagr=12, alpha=-2, lots=80, sharpe=0.7),
        test=_perf(cagr=8, alpha=-4, lots=40, sharpe=0.4, rolling=35),
        diagnostics={"concentration_flags": ["sample_size_below_100_lots", "top_month_exceeds_25pct_of_lots"]},
    )

    reasons = {penalty["reason"] for penalty in score["penalties"]}
    assert "test_sample_below_12_lots" not in reasons
    assert "validation_sample_below_12_lots" not in reasons
    assert "negative_test_alpha" in reasons
    assert "concentration_flags" in reasons
    assert score["score"] < 25


def test_walnut_strategy_score_treats_monthly_selective_signals_as_meaningful_evidence():
    score = walnut_strategy_score(
        full={
            **_perf(cagr=18, alpha=6, lots=36),
            "start_date": "2023-01-01",
            "end_date": "2026-01-01",
        },
        validation=_perf(cagr=15, alpha=4, lots=12),
        test=_perf(cagr=16, alpha=5, lots=12),
        diagnostics={"concentration_flags": []},
    )

    assert score["score_version"] == "walnut_strategy_score_v2"
    assert score["components"]["sample_size"] > 75
    reasons = {penalty["reason"] for penalty in score["penalties"]}
    assert "test_sample_below_12_lots" not in reasons
    assert "validation_sample_below_12_lots" not in reasons


def test_walnut_strategy_score_can_be_recalculated_from_persisted_validation_periods():
    score = walnut_strategy_score_from_validation_result(
        {
            "periods": {
                "full": {
                    "performance": {
                        **_perf(cagr=18, alpha=6, lots=36),
                        "start_date": "2023-01-01",
                        "end_date": "2026-01-01",
                    },
                    "diagnostics": {"concentration_flags": []},
                },
                "validation": {"performance": _perf(cagr=15, alpha=4, lots=12)},
                "test": {"performance": _perf(cagr=16, alpha=5, lots=12)},
            }
        }
    )

    assert score is not None
    assert score["score_version"] == "walnut_strategy_score_v2"


def test_default_candidate_lookup_includes_primary_and_contract_expansion_candidates():
    lookup = _candidate_lookup()

    assert lookup["congress-buys-90d"].strategy_kind == "primary"
    assert lookup["congress-buys-180d"].strategy_kind == "primary"
    assert lookup["insider-open-market-buys-90d"].strategy_kind == "primary"
    assert lookup["congress-contracts-confirmation-90d"].pair == "congress_contracts"
    assert lookup["insider-contracts-confirmation-90d"].pair == "insider_contracts"
