import copy
import unittest
import numpy as np
from rank_calibration_model import add_cross_sectional_ranks, partition, half_year_before, date_weights, fit_calibrator, calibrate, forward_ranks
from evaluate_rank_calibration import advancement, selections


class RankCalibrationInvariants(unittest.TestCase):
    def test_calendar_half_year_and_strict_label_purges(self):
        self.assertEqual(half_year_before('2025-01-01'),'2024-07-01')
        self.assertEqual(half_year_before('2025-10-01'),'2025-04-01')
        dates=[('2024-05-20','2024-06-20'),('2024-06-01','2024-07-01'),('2024-07-01','2024-08-01'),('2024-12-02','2025-01-01')]
        rows=[{'entry_date':a,'outcomes':{'30':{'target_date':b}}} for a,b in dates]
        fit,cal=partition(rows,'2025-01-01')
        self.assertEqual(fit,[rows[0]])
        self.assertEqual(cal,[rows[2]])

    def test_feature_ranks_do_not_depend_on_outcome_or_missingness(self):
        rows=[{'features':{'momentum':v},'outcomes':{}} for v in [10,20,30]]
        add_cross_sectional_ranks(rows)
        expected=[r['ranks'].copy() for r in rows]
        altered=copy.deepcopy(rows)
        altered[0]['outcomes']={'30':{'raw_return':10000}}
        altered[2]['outcomes']={'30':{'raw_return':-99}}
        add_cross_sectional_ranks(altered)
        self.assertEqual(expected,[r['ranks'] for r in altered])
        self.assertAlmostEqual(rows[1]['ranks']['momentum'],.5)
        self.assertAlmostEqual(rows[2]['ranks']['momentum'],5/6)

    def test_missing_features_are_neutral_and_ties_equal(self):
        rows=[{'features':{'x':v}} for v in [1.,1.,np.nan]]
        add_cross_sectional_ranks(rows)
        self.assertEqual([r['ranks']['x'] for r in rows],[.5,.5,.5])

    def test_each_date_has_equal_total_training_weight(self):
        rows=[{'entry_date':d} for d in ['a','b','b','b']]
        w=date_weights(rows)
        self.assertAlmostEqual(w[0],w[1:].sum())
        self.assertAlmostEqual(w.mean(),1.)

    def test_calibration_cannot_reverse_ranking_or_leave_probability_range(self):
        p=np.linspace(.05,.95,50); y=p<.5
        coef=fit_calibrator(p,y,np.ones(50))
        self.assertGreaterEqual(coef[0],0.)
        self.assertLessEqual(coef[0],2.)
        calibrated=calibrate(p,coef)
        self.assertTrue(np.all(np.diff(calibrated)>=0))
        self.assertTrue(np.all((calibrated>0)&(calibrated<1)))

    def test_forward_ranks_use_each_date_not_market_drift(self):
        rows=[{'entry_date':d,'outcomes':{'30':{'raw_return':v,'spy_return':0}}} for d,v in [('a',-5),('a',-2),('b',10),('b',20)]]
        np.testing.assert_allclose(forward_ranks(rows),[.25,.75,.25,.75])

    def test_selection_uses_only_forecast_and_date(self):
        rows=[{'ticker':s,'entry_date':'a','raw':{'actual':v}} for s,v in [('A',1000),('B',-99),('C',50),('D',-30)]]
        _,selected=selections(rows,np.array([.1,.9,.2,.3]))
        self.assertEqual(selected,{'a':[1]})

    def test_advancement_cannot_be_won_by_accuracy_alone(self):
        model={'brier':.24,'brier_improvement_block95':[-.01,.01],
               'accuracy_comparisons':{b:{'gain_block95_pp':[1.,3.]} for b in ['stock_trend','always_nonpositive']},
               'quarters':{str(i):{'brier_improvement':.01} for i in range(6)},
               'top_quartile':{'dates':78,'selected':4000,'hit_advantage_block95_pp':[1.,3.],'return_advantage_block95_pp':[-1.,2.],
                               'quarters':{},'vs_fixed_rankings':{}}}
        self.assertIsNone(advancement({'rank_logistic_calibrated':model})['selected'])
        model['brier_improvement_block95']=[.001,.01]
        self.assertTrue(advancement({'rank_logistic_calibrated':model})['selected']['directional'])
        self.assertIsNone(advancement({'rank_return_hist_uncalibrated':model})['selected'])


if __name__=='__main__': unittest.main()
