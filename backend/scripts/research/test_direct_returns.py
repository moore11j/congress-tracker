import unittest
import numpy as np
from analyze_direct_returns import target_return, training_rows, paired_block_interval, evaluate


class DirectReturnInvariants(unittest.TestCase):
    def test_losing_stock_that_beats_market_is_not_positive_return(self):
        row = {'outcomes':{'30':{'raw_return':-2.,'spy_return':-5.}}}
        self.assertEqual(target_return(row,'raw'),-2.)
        self.assertEqual(target_return(row,'excess'),3.)

    def test_training_requires_matured_labels_strictly_before_cutoff(self):
        rows = [{'outcomes':{'30':{'target_date':d}}} for d in ['2025-03-31','2025-04-01','2025-04-02']]
        self.assertEqual(training_rows(rows,'2025-04-01'),rows[:1])

    def test_paired_intervals_preserve_same_day_dependence(self):
        dates = ['2025-01-06']*10+['2025-01-13']*10
        self.assertEqual(paired_block_interval(dates,np.ones(20)),[1.,1.])
        self.assertEqual(paired_block_interval(dates,np.zeros(20)),[0.,0.])

    def test_zero_return_not_a_positive_outcome_and_no_bull_calls_is_defined(self):
        records = [{'ticker':str(i),'entry_date':'2025-01-06', 'raw':{'actual':y,'prevalence':.4,'predictions':{'logistic':.2}}} for i,y in enumerate([0.,-1.,2.])]
        result = evaluate(records,'raw','logistic')
        self.assertAlmostEqual(result['direction_accuracy_pct'],200/3)
        self.assertEqual(result['bullish_coverage_pct'],0)
        self.assertIsNone(result['bullish_precision_pct'])
        self.assertEqual(result['accuracy_gain_pp'],0)


if __name__ == '__main__': unittest.main()
