import copy
import unittest

import numpy as np

from analyze_confirmation_models import correct, features, metrics, split_for, wilson


class ResearchInvariants(unittest.TestCase):
    def row(self):
        return {'id': 1, 'security_id': 43, 'ticker': 'ABC', 'direction': 'bullish',
                'score': 80, 'active_source_count': 2, 'sources': {'price_volume': {
                    'present': True, 'direction': 'bullish', 'strength': 70, 'quality': 80}},
                'freshness': {'price_volume': {'freshness_days': 2}},
                'outcomes': {'30': {'raw_return': -0.5, 'spy_return': -1.0}}}

    def test_grading_preserves_raw_or_benchmark_rule(self):
        row = self.row()
        self.assertTrue(correct(row, 1))
        self.assertTrue(correct(row, -1))  # Both can win under the existing OR definition.
        row['outcomes']['30'] = {'raw_return': 0., 'spy_return': 0.}
        self.assertFalse(correct(row, 1))
        self.assertFalse(correct(row, -1))

    def test_features_exclude_future_identity_and_execution_information(self):
        row = self.row()
        before = copy.deepcopy(row)
        a = features(row)
        row.update(id=999, ticker='XYZ', security_id=100, entry_price=9000,
                   entry_date='2099-01-01', closed_at='2099-03-01', methodology_id=999)
        row['outcomes']['30']['raw_return'] = 9999
        row['sources']['__v2_features'] = {'regime_context': {'future_hint': 1}}
        b = features(row)
        self.assertEqual(list(a), list(b))
        np.testing.assert_allclose(list(a.values()), list(b.values()), equal_nan=True)
        self.assertNotIn('freshness_days', before['sources']['price_volume'])

    def test_one_security_stays_in_one_split_despite_new_ticker_or_event(self):
        a = self.row()
        b = dict(a, id=2, ticker='RENAMED', direction='bearish')
        self.assertEqual(split_for(a), split_for(b))

    def test_coverage_accounts_for_abstentions_without_changing_rows(self):
        rows = [self.row(), self.row()]
        before = copy.deepcopy(rows)
        result = metrics(rows, [1, 0])
        self.assertEqual(result['n'], 1)
        self.assertEqual(result['coverage'], 50.)
        self.assertEqual(result['accuracy'], 100.)
        self.assertEqual(rows, before)

    def test_research_never_clips_extreme_stored_returns(self):
        row = self.row()
        row['outcomes']['30']['raw_return'] = 196.06
        result = metrics([row], [-1])
        self.assertEqual(result['mean_directional_return'], -196.06)
        self.assertEqual(result['accuracy'], 0.)

    def test_intervals_and_empty_cohorts(self):
        self.assertEqual(wilson(0, 0), [None, None])
        lo, hi = wilson(55, 84)
        self.assertAlmostEqual(lo, 54.83, places=2)
        self.assertAlmostEqual(hi, 74.76, places=2)
        self.assertIsNone(metrics([], [])['accuracy'])


if __name__ == '__main__':
    unittest.main()
