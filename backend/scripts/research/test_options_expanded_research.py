from datetime import date,datetime,timedelta,timezone
import json
import unittest

from options_activity_expanded import select_pair,weekly_entries,PROVIDERS
from build_options_activity_panel import event_features,label,model_split
from analyze_options_activity_expanded import rank_half,probability_metrics,bootstrap_difference
from audit_options_activity_expanded import cached_pages,identity


def contract(side,expiry,strike=100):
    return {'ticker':'O:TEST'+expiry+side+str(strike),'underlying_ticker':'TEST','contract_type':side,
            'expiration_date':expiry,'strike_price':strike,'shares_per_contract':100}


class ExpandedOptionsInvariants(unittest.TestCase):
    def test_weekly_observations_start_after_contract_selection(self):
        sessions=['2025-01-02','2025-01-03','2025-01-06','2025-01-07','2025-01-13']
        result=weekly_entries(sessions,'2025-01-02','2025-01-31')
        self.assertEqual(result,[{'entry_date':'2025-01-06','cutoff':'2025-01-03'},
                                 {'entry_date':'2025-01-13','cutoff':'2025-01-07'}])

    def test_historical_pair_uses_anchor_price_not_future_activity(self):
        rows=[contract(side,expiry,strike) for side in ['call','put'] for expiry in ['2025-03-21','2025-04-18'] for strike in [95,100,105]]
        pair=select_pair(rows,'2025-01-02',100,'TEST')
        self.assertEqual({r['expiration_date'] for r in pair},{'2025-03-21'})
        self.assertEqual({r['strike_price'] for r in pair},{100})
        self.assertEqual(select_pair(rows,'2025-01-02',100,'OTHER'),[])

    def test_weekly_feature_cannot_use_future_bars_in_downloaded_window(self):
        first=date(2024,9,1);ss=[(first+timedelta(days=i)).isoformat() for i in range(140) if (first+timedelta(days=i)).weekday()<5]
        cutoff='2025-01-03';prices={s:{d:[s,d,100,100,100,1000,PROVIDERS[0],None,None] for d in ss} for s in ['TEST','SPY']}
        pair=[]
        for side in ['call','put']:
            bars=[{'t':int(datetime.fromisoformat(d+'T05:00:00+00:00').timestamp()*1000),'v':10,'vw':2} for d in ss]
            pair.append({'contract':contract(side,'2025-03-21'),'fetch_complete':True,'bars':bars})
        group={'ticker':'TEST','anchor':'2025-01-02'};decision={'cutoff':cutoff}
        a,_=event_features(group,pair,decision,prices,ss)
        for item in pair:
            for bar in item['bars']:
                if datetime.fromtimestamp(bar['t']/1000,timezone.utc).date().isoformat()>cutoff:bar['v']=10**12
        b,audit=event_features(group,pair,decision,prices,ss)
        self.assertEqual(a,b);self.assertGreater(audit['future_bars_ignored'],0)
        self.assertEqual(event_features(group,pair,{'cutoff':'2025-01-01'},prices,ss)[1]['status'],'before_contract_selection')

    def test_return_basis_is_consistent_per_asset_not_required_across_assets(self):
        days=['2025-01-06','2025-02-05'];by={p:{} for p in PROVIDERS}
        by[PROVIDERS[0]]['TEST']={days[0]:['TEST',days[0],100,50,50],days[1]:['TEST',days[1],110,110,110]}
        by[PROVIDERS[1]]['SPY']={days[0]:['SPY',days[0],200,200,200],days[1]:['SPY',days[1],180,180,180]}
        out=label('TEST',days[0],30,days,by)
        self.assertAlmostEqual(out['raw_return'],10)
        self.assertAlmostEqual(out['spy_return'],-10)
        self.assertTrue(out['correct'])
        by[PROVIDERS[1]]['TEST']={days[1]:by[PROVIDERS[0]]['TEST'].pop(days[1])}
        self.assertEqual(label('TEST',days[0],30,days,by)['status'],'missing_consistent_prices')

    def test_maturity_boundary_prevents_training_on_future_validation_labels(self):
        row={'split':'train','outcomes':{'30':{'status':'measured','target_date':'2026-01-01'}}}
        self.assertIsNone(model_split(row))
        row['outcomes']['30']['target_date']='2025-12-31';self.assertEqual(model_split(row),'train')
        row['split']='validation';row['outcomes']['30']['target_date']='2026-04-01';self.assertIsNone(model_split(row))

    def test_ranking_is_fixed_before_measuring_outcomes_and_preserves_ties(self):
        rows=[{'id':s,'ticker':s,'entry_date':'2026-05-04','outcomes':{'30':{'status':'measured','correct':s=='A'}}} for s in ['C','B','A']]
        probs={s:.5 for s in ['A','B','C']};before=json.dumps(rows)
        self.assertEqual(rank_half(rows,probs),{'A','B'})
        self.assertEqual(json.dumps(rows),before)
        for r in rows:r['outcomes']['30']['correct']=not r['outcomes']['30']['correct']
        self.assertEqual(rank_half(rows,probs),{'A','B'})

    def test_identical_models_have_zero_paired_bootstrap_difference(self):
        rows=[{'id':str(i),'ticker':'A','entry_date':str(i),'outcomes':{'30':{'status':'measured','correct':i%2==0}}} for i in range(4)]
        ids={r['id'] for r in rows};out=bootstrap_difference(rows,ids,ids,'entry_date')
        self.assertEqual(out['difference_pp95'],[0,0])
        self.assertIsNone(bootstrap_difference(rows,ids,ids,'ticker'))

    def test_probability_metrics_do_not_count_missing_or_unmatured_labels(self):
        rows=[{'id':'A','outcomes':{'30':{'status':'measured','correct':True}}},
              {'id':'B','outcomes':{'30':{'status':'immature'}}}]
        out=probability_metrics(rows,{'A':.8,'B':.99})
        self.assertEqual(out['n'],1);self.assertAlmostEqual(out['brier'],.04)

    def test_cached_reference_audit_requires_every_page(self):
        first={'path':'/reference','params':{'as_of':'2025-01-02'},'results':[{'id':1}],
               'next_request':['/reference',{'cursor':'page2'}]}
        second={'path':'/reference','params':{'cursor':'page2'},'results':[{'id':2}],'next_request':None}
        cache={identity(p['path'],p['params']):p for p in [first,second]}
        self.assertEqual(cached_pages(first,cache),[{'id':1},{'id':2}])
        del cache[identity(second['path'],second['params'])]
        with self.assertRaises(KeyError):cached_pages(first,cache)

    def test_cached_reference_audit_rejects_pagination_cycle(self):
        page={'path':'/reference','params':{'cursor':'same'},'results':[],
              'next_request':['/reference',{'cursor':'same'}]}
        with self.assertRaisesRegex(ValueError,'cycle'):
            cached_pages(page,{identity(page['path'],page['params']):page})


if __name__=='__main__':unittest.main()
