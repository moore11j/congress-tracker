from copy import deepcopy

from conflict_ceiling import directional_score_ceiling
from evaluate_conflict_ceiling import compare, revised_score


def source(direction, age=1):
    return {"present": True, "direction": direction, "strength": 100, "quality": 100, "score_contribution": 10, "freshness_days": age}


def test_ceiling_is_symmetric_and_does_not_treat_stale_or_future_evidence_as_current():
    sources = {"a": source("bullish"), "b": source("bearish")}
    assert directional_score_ceiling(sources, "bullish") == 50
    assert directional_score_ceiling(sources, "bearish") == 50
    for age in (91, -1):
        sources["b"]["freshness_days"] = age
        assert directional_score_ceiling(sources, "bullish") == 100


def test_frozen_freshness_and_outcomes_are_preserved_and_abstention_is_reported():
    rows = [
        {"score": 100, "direction": "bullish", "security_id": 1, "sources": {"a": source("bullish"), "b": source("bearish")},
         "outcomes": {"30": {"correct": True, "raw_correct": True}}},
        {"score": 70, "direction": "bullish", "security_id": 2, "sources": {"a": source("bullish"), "b": source("bearish")},
         "freshness": {"b": {"freshness_days": 91}}, "outcomes": {"30": {"correct": False, "raw_correct": False}}},
    ]
    original = deepcopy(rows)
    assert revised_score(rows[1]) == 70
    result = compare(rows, "30", 60)
    assert rows == original
    assert result["baseline"]["accuracy_pct"] == 50
    assert result["revised"]["accuracy_pct"] == 0
    assert result["removed"]["wins"] == 1
    assert result["retained_pct"] == 50
