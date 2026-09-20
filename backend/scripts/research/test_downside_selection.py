import copy
import unittest
import numpy as np
from downside_selection import choose, risk_targets, payoff, measures, pair_metrics, advancement, POLICIES


class DownsideInvariants(unittest.TestCase):
    def rows(self):
        return [{'ticker':str(i),'base_probability':1-i/10,'prior_volatility':10-i,'predicted_loss':10-i,
                 'predicted_large_loss':1-i/10,'predicted_lower_tail':i,'predicted_payoff':i,
                 'raw_return':-99 if i==3 else 100,'excess_return':0} for i in range(8)]

    def test_preserves_coverage_and_top_half_constraint_without_outcomes(self):
        rows=self.rows()
        for policy in POLICIES:
            selected=choose(rows,policy)
            self.assertEqual(len(selected),2)
            self.assertTrue(set(selected).issubset({0,1,2,3}))
            altered=copy.deepcopy(rows)
            for r in altered:r['raw_return']=-r['raw_return']
            self.assertEqual(choose(altered,policy),selected)

    def test_forecast_signs_and_deterministic_ties(self):
        rows=self.rows()
        self.assertEqual(choose(rows,'baseline'),[0,1])
        self.assertEqual(choose(rows,'expected_loss'),[3,2])
        self.assertEqual(choose(rows,'lower_tail'),[3,2])
        self.assertEqual(choose(rows,'expected_payoff'),[3,2])
        for r in rows:r['predicted_loss']=1
        self.assertEqual(choose(rows,'expected_loss'),[0,1])

    def test_zero_is_not_a_win_and_loss_threshold_is_inclusive(self):
        loss,large=risk_targets([-10,-9,0,15])
        np.testing.assert_array_equal(loss,[10,9,0,0])
        np.testing.assert_array_equal(large,[True,False,False,False])
        rows=[{'raw_return':r,'excess_return':r} for r in [-10,0,10]]
        m=measures(rows)
        self.assertAlmostEqual(m['hit_rate'],1/3)
        self.assertAlmostEqual(m['downside'],10/3)

    def test_expected_payoff_keeps_both_loss_size_and_probability(self):
        np.testing.assert_allclose(payoff(np.array([.6,.8]),np.array([10,10]),np.array([20,20])),[-2,4])

    def test_improvement_direction_rewards_smaller_losses(self):
        b=[{'date':'a','hit_rate':.6,'return':2.,'excess_return':1.,'downside':4.,'large_loss_rate':.2}]
        a=[{**b[0],'downside':3.,'large_loss_rate':.1}]
        m=pair_metrics(a,b)
        self.assertEqual(m['downside']['improvement'],1.)
        self.assertEqual(m['large_loss_rate']['improvement'],10.)
        self.assertEqual(m['return']['block95'],[0.,0.])

    def test_lower_risk_alone_does_not_pass_return_criteria(self):
        baseline={'quarterly':{str(i):{'return':2.} for i in range(5)}}
        candidate={'dates':64,'n_selected':1331,'quarterly':{str(i):{'return':3.} for i in range(5)},
                   'vs_baseline':{'hit_rate':{'block95':[-1,2]},'return':{'block95':[-.1,.5]},
                                  'downside':{'block95':[.1,.5]},'large_loss_rate':{'improvement':2}},
                   'vs_universe':{'return':{'block95':[.1,.5]}},
                   'vs_low_volatility':{'return':{'improvement':.1},'downside':{'improvement':.1}}}
        report={'policies':{'baseline':baseline,**{n:copy.deepcopy(candidate) for n in POLICIES[2:]}}}
        self.assertIsNone(advancement(report)['selected'])
        report['policies']['expected_loss']['vs_baseline']['return']['block95']=[.1,.5]
        self.assertEqual(advancement(report)['selected']['name'],'expected_loss')


if __name__=='__main__':unittest.main()
