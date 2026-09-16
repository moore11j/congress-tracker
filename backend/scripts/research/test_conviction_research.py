import unittest
from conviction_features import build_trades,ConvictionFeatures


def event(id=1,kind='insider_trade',filing='2026-02-10',side='purchase',shares=10,price=100,following=110,low=None,high=None):
    return {'id':id,'type':kind,'ticker':'TEST','side':side,'actor':'person','filing_date':filing,'transaction_date':filing,'shares':shares,'price':price,'shares_following':following,'amount_min':low,'amount_max':high,'role':'Chief Executive Officer','ownership':'D','market_trade':True}


class ConvictionInvariants(unittest.TestCase):
    def test_duplicates_cannot_double_dollars_or_buyer_count(self):
        r=event();trades,audit=build_trades({'events':[r,{**r,'id':2}],'normalized':[]})
        self.assertEqual(len(trades),1);self.assertEqual(trades[0]['low'],1000)
        f=ConvictionFeatures(trades).features('TEST','2026-02-11')
        self.assertEqual(f['conviction:insider:30:buy_actors'],1)
        self.assertEqual(audit['duplicate_event_rows'],1)

    def test_dollars_can_disagree_with_trade_count(self):
        rows=[event(kind='congress_trade',low=1000000,high=5000000)]
        rows += [{**event(id=i+2,kind='congress_trade',side='sale',low=1,high=1000),'actor':f'seller{i}'} for i in range(10)]
        trades,_=build_trades({'events':rows,'normalized':[]});f=ConvictionFeatures(trades).features('TEST','2026-02-11')
        self.assertEqual(f['conviction:congress:30:buy_actors'],1)
        self.assertEqual(f['conviction:congress:30:sell_actors'],10)
        self.assertGreater(f['conviction:congress:30:net_lower_ratio'],0)

    def test_current_and_future_filings_not_visible(self):
        trades,_=build_trades({'events':[event()],'normalized':[]});engine=ConvictionFeatures(trades)
        self.assertEqual(engine.features('TEST','2026-02-10')['conviction:insider:30:buy_filings'],0)
        self.assertEqual(engine.features('TEST','2026-02-11')['conviction:insider:30:buy_filings'],1)

    def test_ownership_uses_pre_purchase_holdings_and_prior_behavior_only(self):
        rows=[event(id=i,filing=d) for i,d in enumerate(['2026-01-01','2026-01-10','2026-02-01'])]
        rows.append(event(id=5,shares=20,following=120))
        trades,_=build_trades({'events':rows,'normalized':[]})
        self.assertEqual(trades[-1]['unusual_ratio'],2)
        self.assertEqual(trades[-1]['ownership_increase'],.2)
        self.assertIsNone(trades[0]['unusual_ratio'])

    def test_member_track_record_excludes_unmatured_results(self):
        past=[{'maturity':'2026-01-10','correct':False} for _ in range(5)]
        history={'member':past+[{'maturity':'2026-03-01','correct':True} for _ in range(100)]}
        engine=ConvictionFeatures([],history)
        self.assertEqual(engine.member_score('member','2026-02-01'),(5/15,5))

    def test_options_do_not_count_as_common_stock_buys(self):
        r=event(kind='congress_trade',low=1000,high=15000)
        trades,audit=build_trades({'events':[r],'normalized':[]},{'congress':{'1':{'asset_class':'Stock Option'}}})
        self.assertEqual(trades,[]);self.assertEqual(audit['excluded_congress_security_type'],1)

    def test_decimal_error_cannot_manufacture_large_purchase(self):
        rows=[event(id=1,price=748119),event(id=2,price=79.019),event(id=3,price=77.6356)]
        trades,audit=build_trades({'events':rows,'normalized':[]})
        self.assertIsNone(trades[0]['low'])
        self.assertEqual(audit['quarantined_same_filing_price_outliers'],1)
        self.assertEqual(rows[0]['price'],748119)


if __name__=='__main__':unittest.main()
