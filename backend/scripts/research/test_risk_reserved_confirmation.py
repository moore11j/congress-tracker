import copy
import unittest
from risk_reserved_confirmation import issuer_selection,decision


class ReservedRiskInvariants(unittest.TestCase):
    def test_reserved_issuers_exclude_earlier_and_duplicate_share_classes(self):
        ordered=[f'T{i:04}' for i in range(931)]
        priors=[{'symbols':ordered[i:i+256]+['SPY','QQQ']} for i in [0,256,512]]
        mapping={str(i):{'ticker':s,'cik_str':c} for i,(s,c) in enumerate([(ordered[0],1),(ordered[768],1),(ordered[769],2),(ordered[770],2),(ordered[771],3)])}
        result=issuer_selection({'ordered_eligible':ordered},mapping,priors)
        self.assertEqual([r['ticker'] for r in result['selected']],[ordered[769],ordered[771]])
        self.assertEqual(result['raw_reserved_count'],163)
        self.assertEqual(result['excluded'][0]['reason'],'earlier_issuer')
        self.assertEqual(result['excluded'][1]['reason'],'duplicate_reserved_issuer')

    def test_changed_earlier_sample_is_rejected(self):
        ordered=[f'T{i:04}' for i in range(931)]
        priors=[{'symbols':ordered[i:i+256]} for i in [0,256,512]]
        priors[0]['symbols'][0]='CHANGED'
        with self.assertRaisesRegex(ValueError,'membership changed'):
            issuer_selection({'ordered_eligible':ordered},{},priors)

    def report(self):
        baseline={'quarterly':{str(i):{'downside':3.} for i in range(5)}}
        candidate={'n_selected':1200,'dates':60,'return':-1.,'quarterly':{str(i):{'downside':2.} for i in range(5)},
                   'vs_baseline':{'hit_rate':{'block95':[-1.9,1.]},'downside':{'block95':[.1,.5]},'large_loss_rate':{'block95':[.1,2.]}}}
        return {'issuers':60,'policies':{'baseline':baseline,'expected_loss':candidate}}

    def test_risk_only_pass_does_not_claim_return_improvement_or_deployment(self):
        result=decision(self.report())
        self.assertEqual(result['status'],'pass_risk_only')
        self.assertFalse(result['higher_returns_tested_as_success_criterion'])
        self.assertFalse(result['deployment_authorized'])

    def test_boundary_accuracy_or_loss_intervals_do_not_pass(self):
        for metric,value in [('hit_rate',-2.),('downside',0.),('large_loss_rate',0.)]:
            report=self.report();report['policies']['expected_loss']['vs_baseline'][metric]['block95'][0]=value
            self.assertEqual(decision(report)['status'],'not_confirmed')

    def test_small_sample_cannot_pass_and_quarter_consistency_is_required(self):
        report=self.report();report['issuers']=49
        self.assertEqual(decision(report)['status'],'insufficient_sample')
        report=self.report()
        for i in range(2):report['policies']['expected_loss']['quarterly'][str(i)]['downside']=4.
        self.assertEqual(decision(report)['status'],'not_confirmed')


if __name__=='__main__':unittest.main()
