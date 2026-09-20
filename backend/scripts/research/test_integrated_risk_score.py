import copy
import unittest

from integrated_risk_score import adjusted_score, issuer_partition, select
from evaluate_integrated_risk import choose_weight, compare, decision_date


class IntegratedRiskTests(unittest.TestCase):
    def test_score_bounds_monotonicity_and_nonbullish_preservation(self):
        for original in range(101):
            scores = [adjusted_score(original, "bullish", loss, 2)["score"] for loss in [0, .25, 1, 10, 100]]
            self.assertEqual(scores, sorted(scores, reverse=True))
            self.assertTrue(all(0 <= s <= original for s in scores))
            for direction in ["bearish", "mixed", "inactive"]:
                self.assertEqual(adjusted_score(original, direction, 100, 8)["score"], original)

    def test_unknown_is_distinct_from_zero_and_stale_is_unknown(self):
        for value in [None, float("nan"), float("inf"), -1, True]:
            result = adjusted_score(80, "bullish", value, 2)
            self.assertEqual(result["score"], 80)
            self.assertEqual(result["status"], "unknown_input_fallback")
        self.assertEqual(adjusted_score(80, "bullish", 0, 2)["status"], "adjusted")
        self.assertEqual(adjusted_score(80, "bullish", 2, 2, usable=False)["status"], "unknown_input_fallback")

    def test_invalid_original_or_weight_fails_instead_of_hiding_bad_input(self):
        for score, weight in [(101, 1), (-1, 1), (80.5, 1), (True, 1), (80, -1), (80, float("nan"))]:
            with self.assertRaises(ValueError):
                adjusted_score(score, "bullish", 2, weight)

    def test_integer_rounding_ties_and_no_return_access(self):
        self.assertEqual(adjusted_score(80, "bullish", .5, 1)["score"], 80)
        rows = [{"id": i, "ticker": ticker, "direction": "bullish", "score": 80,
                 "predicted_loss": 1, "raw_return": returns}
                for i, ticker, returns in [(1, "B", 100), (2, "A", -100), (3, "C", 0), (4, "D", 0)]]
        self.assertEqual([r["ticker"] for r in select(rows, 2)], ["A"])
        changed = copy.deepcopy(rows)
        for r in changed:
            r["raw_return"] = -r["raw_return"]
        self.assertEqual([r["id"] for r in select(rows, 2)], [r["id"] for r in select(changed, 2)])

    def test_issuer_partition_requires_stable_normalized_identity(self):
        self.assertEqual(issuer_partition("0000320193"), issuer_partition("0000320193"))
        for cik in ["", "320193", "not-an-issuer"]:
            with self.assertRaises(ValueError):
                issuer_partition(cik)

    def test_timestamp_and_snapshot_market_date_both_bound_features(self):
        calendar = ["2026-08-03", "2026-08-04", "2026-08-05"]
        r = {"calculated_at": "2026-08-05T19:00:00+00:00", "market_date": "2026-08-05"}
        self.assertEqual(decision_date(r, calendar), "2026-08-04")
        r["calculated_at"] = "2026-08-05T21:00:00+00:00"
        r["market_date"] = "2026-08-03"
        self.assertEqual(decision_date(r, calendar), "2026-08-03")
        r["calculated_at"] = "2026-08-10T21:00:00+00:00"
        self.assertIsNone(decision_date(r, calendar))

    @staticmethod
    def sample():
        return [{"id": d * 100 + i, "ticker": f"T{i:02}", "cik": str(i).zfill(10),
                 "security_id": i, "entry_date": f"2026-08-{d+3:02}", "direction": "bullish",
                 "score": 90 - i, "predicted_loss": 10 if i < 7 else 1,
                 "raw_return": -12 if i < 7 else 3, "excess_return": -12 if i < 7 else 3,
                 "public_correct": i >= 7}
                for d in range(4) for i in range(60)]

    def test_matched_coverage_and_first_minimal_candidate(self):
        rows = self.sample()
        selected = choose_weight(rows)
        self.assertEqual(selected["status"], "exploratory_candidate")
        self.assertGreater(selected["weight"], 0)
        comparison = compare(rows, selected["weight"])
        self.assertEqual(comparison["baseline"]["n_selected"], comparison["integrated"]["n_selected"])
        self.assertEqual(comparison["baseline"]["n_selected"], 60)
        self.assertGreater(comparison["improvements"]["downside"], 0)

    def test_small_sample_or_no_improvement_keeps_zero(self):
        rows = self.sample()
        self.assertEqual(choose_weight(rows[:20])["weight"], 0)
        for r in rows:
            r.update(raw_return=3, excess_return=3, public_correct=True)
        self.assertEqual(choose_weight(rows)["weight"], 0)


if __name__ == "__main__":
    unittest.main()
