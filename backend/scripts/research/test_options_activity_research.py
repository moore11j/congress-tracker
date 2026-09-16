from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import options_activity_pilot as collector
from options_activity_pilot import cutoff_session, safe_next, select_pairs, CachedClient, StopCollection
from analyze_options_activity_pilot import activity_features, dated_bars, fixed_gate, summarize


def sessions():
    first = datetime(2026, 6, 1)
    return [(first+timedelta(days=i)).date().isoformat() for i in range(50) if (first+timedelta(days=i)).weekday()<5][:25]


def contract(side, expiry='2026-10-16', strike=100, ticker=None):
    return {'ticker': ticker or 'O:TEST'+side, 'contract_type': side, 'expiration_date': expiry,
            'strike_price': strike, 'shares_per_contract': 100}


def bar(day, volume=10, vwap=2):
    stamp = datetime.fromisoformat(day+'T04:00:00+00:00')
    return {'t': int(stamp.timestamp()*1000), 'v': volume, 'vw': vwap}


def pair():
    return [{'contract': contract(side), 'fetch_complete': True,
             'bars': [bar(d) for d in sessions()]} for side in ['call','put']]


class OptionsActivityInvariants(unittest.TestCase):
    def test_eod_data_is_withheld_even_after_close_and_dates_use_new_york(self):
        days=['2026-08-03','2026-08-04','2026-08-05']
        self.assertEqual(cutoff_session('2026-08-06T03:21:00+00:00', days), '2026-08-04')
        self.assertEqual(cutoff_session('2026-08-05T21:00:00+00:00', days), '2026-08-04')

    def test_selection_requires_matched_standard_pair_and_ignores_activity(self):
        rows=[contract(s, strike=k, ticker=f'O:{s}{k}') for k in [95,100,105] for s in ['call','put']]
        rows += [{**contract(s, strike=101, ticker=f'O:bad{s}'), 'shares_per_contract': 10} for s in ['call','put']]
        a=select_pairs(rows, '2026-08-04', 100)
        b=select_pairs([{**r,'volume':1000000000} for r in reversed(rows)], '2026-08-04', 100)
        self.assertEqual([r['ticker'] for r in a['30_90']], [r['ticker'] for r in b['30_90']])
        self.assertTrue(all(r['strike_price']==100 for r in a['30_90']))

    def test_future_bars_cannot_affect_features(self):
        rows=pair(); original=activity_features(rows,sessions()[-1],sessions())
        rows[0]['bars'].append(bar('2026-08-01',1_000_000,1000))
        changed=activity_features(rows,sessions()[-1],sessions())
        self.assertEqual(original['premium_activity_ratio'],changed['premium_activity_ratio'])
        self.assertEqual(changed['audit']['call']['discarded_future_bars'],1)

    def test_new_contract_and_failed_request_are_unknown_not_zero(self):
        rows=pair(); rows[0]['bars']=rows[0]['bars'][10:]
        self.assertEqual(activity_features(rows,sessions()[-1],sessions())['status'],'insufficient_contract_history')
        rows[0]['fetch_complete']=False
        self.assertIsNone(fixed_gate(activity_features(rows,sessions()[-1],sessions())))

    def test_missing_vwap_does_not_use_close_as_invented_turnover(self):
        rows=pair(); rows[0]['bars'][-1]['vw']=None; rows[0]['bars'][-1]['c']=5
        self.assertEqual(activity_features(rows,sessions()[-1],sessions())['status'],'missing_vwap')

    def test_activity_compares_daily_rates_not_unequal_window_totals(self):
        rows=pair(); out=activity_features(rows,sessions()[-1],sessions())
        self.assertAlmostEqual(out['premium_activity_ratio'],1)
        self.assertFalse(fixed_gate(out))
        for r in rows[0]['bars'][-5:]: r['v']=50
        out=activity_features(rows,sessions()[-1],sessions())
        self.assertAlmostEqual(out['premium_activity_ratio'],3)
        self.assertTrue(fixed_gate(out))
        self.assertEqual(out['semantic_type'],'unsigned_gross_activity_sample')

    def test_mismatched_strike_does_not_create_skew(self):
        rows=pair(); rows[1]['contract']['strike_price']=105
        self.assertEqual(activity_features(rows,sessions()[-1],sessions())['status'],'unmatched_contracts')

    def test_pagination_removes_credentials_and_rejects_external_origin(self):
        path,params=safe_next('https://api.massive.com/v3/reference/options/contracts?cursor=abc&apiKey=SECRET')
        self.assertEqual(params,{'cursor':'abc'})
        with self.assertRaises(ValueError): safe_next('https://example.com/path?apiKey=SECRET')

    def test_cache_hit_never_repeats_request_and_budget_never_calls_network(self):
        with tempfile.TemporaryDirectory() as temp, patch.object(collector,'ROOT',Path(temp)):
            client=CachedClient('SECRET',0)
            path='/v3/reference/options/contracts'; params={'limit':1}
            identity=json.dumps([path,params],sort_keys=True)
            target=client.cache/(collector.hashlib.sha256(identity.encode()).hexdigest()+'.json')
            collector.save(target,{'results':[],'next_request':None})
            with patch.object(collector,'urlopen',side_effect=AssertionError('Network not allowed')):
                self.assertEqual(client.get(path,params)['results'],[])
                with self.assertRaises(StopCollection): client.get(path,{'limit':2})
                with self.assertRaises(ValueError): client.get('/v3/snapshot/options/TSM',{})
            self.assertEqual(client.calls,0)

    def test_conflicting_duplicate_bars_are_not_summed(self):
        raw=[bar(sessions()[0]),bar(sessions()[0],volume=999)]
        _,audit=dated_bars(raw,sessions()[-1])
        self.assertEqual(audit['error'],'conflicting_duplicate_day')

    def test_comparison_preserves_original_outcome_and_reports_abstentions(self):
        rows=[{'direction':'bearish','entry_date':'2026-08-05','options_gate':None,
               'outcomes':{'30':{'correct':True,'raw_return':-10,'spy_return':0}},
               'retained':{'original':True,'abstain':False}}]
        before=json.dumps(rows,sort_keys=True)
        baseline=summarize(rows,'30','original'); abstain=summarize(rows,'30','abstain')
        self.assertEqual(baseline['correct'],1)
        self.assertEqual(baseline['raw_directional_correct'],1)
        self.assertEqual(baseline['unknown_options_fallback'],1)
        self.assertEqual(abstain['retained'],0)
        self.assertIsNone(abstain['accuracy'])
        self.assertEqual(json.dumps(rows,sort_keys=True),before)


if __name__ == '__main__': unittest.main()
