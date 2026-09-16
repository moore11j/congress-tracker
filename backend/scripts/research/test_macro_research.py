import copy
import unittest
from analyze_macro_hypothesis import market_features, risk_state, macro_headwind, direction_under, classifier_for


class MacroResearchTests(unittest.TestCase):
    def test_benchmark_uses_only_closes_available_before_score(self):
        row={'calculated_at':'2026-08-10 14:00:00+00:00'}
        prices=[{'symbol':'SPY','date':d,'source':'provider','raw_close':p}
                for d,p in [('2026-07-31',100),('2026-08-07',90),('2026-08-10',500)]]
        features=market_features(row,prices)
        self.assertEqual(features['SPY_asof'],'2026-08-07')
        self.assertAlmostEqual(features['SPY_7'],-10)

    def test_missing_market_data_remains_unknown(self):
        self.assertIsNone(risk_state({'SPY_7':-1}))
        self.assertTrue(risk_state({'SPY_7':-1,'QQQ_7':-2}))
        self.assertFalse(risk_state({'SPY_7':-1,'QQQ_7':2}))

    def test_stale_macro_cannot_trigger_fresh_headwind_gate(self):
        row={'sources':{'macro_positioning':{'present':True,'direction':'bearish'}},
             'freshness':{'macro_positioning':{'freshness_days':11}}}
        self.assertFalse(macro_headwind(row))
        row['freshness']['macro_positioning']['freshness_days']=10
        self.assertTrue(macro_headwind(row))

    def test_counterfactual_strength_does_not_modify_frozen_source(self):
        row={'sources':{'macro_positioning':{'present':True,'direction':'bearish','strength':50,'quality':61}},
             'freshness':{'macro_positioning':{'freshness_days':2}}}
        before=copy.deepcopy(row)
        self.assertEqual(direction_under(row,'symmetric_moderate'),'bearish')
        self.assertEqual(row,before)
        self.assertIs(classifier_for('current'),classifier_for('current'))
        self.assertIsNot(classifier_for('current'),classifier_for('symmetric_moderate'))


if __name__=='__main__': unittest.main()
