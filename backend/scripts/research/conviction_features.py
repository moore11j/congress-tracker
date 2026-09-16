"""Offline public-trade conviction features. All joins are dated; no app imports."""
from bisect import bisect_left,bisect_right
from collections import Counter,defaultdict
from datetime import date,timedelta
from functools import lru_cache
from statistics import median
import json
import math
from pathlib import Path

BASE=Path('frontend/test-results/confirmation-research')


def number(value):
    try:
        v=float(str(value).replace(',','').replace('$',''))
        return v if math.isfinite(v) and v>=0 else None
    except (ValueError,TypeError):return None


def day(value):
    try:return date.fromisoformat(str(value)[:10]).isoformat()
    except ValueError:return None


def signature(r):
    return (r['source'],r['ticker'],r['actor'],r['filing'],r['transaction'],r['side'],
        round(r['shares'],4) if r['shares'] is not None else None,
        round(r['price'],5) if r['price'] is not None else None,
        r['low'] if r['source']=='congress' else None,r['high'] if r['source']=='congress' else None)


def normalize_event(r):
    filing=day(r.get('filing_date'));transaction=day(r.get('transaction_date'))
    side=str(r.get('side') or '').lower()
    side='buy' if side in ['purchase','p-purchase','buy','p'] else 'sell' if 'sale' in side or side in ['sell','s'] else None
    if not filing or not transaction or transaction>filing or not side or r.get('market_trade') is False:return None
    source='congress' if r['type']=='congress_trade' else 'insider'
    shares=number(r.get('shares'));price=number(r.get('price'))
    value=shares*price if shares and price else None
    low=number(r.get('amount_min')) if source=='congress' else value
    high=number(r.get('amount_max')) if source=='congress' else value
    if source=='congress' and low is not None and high is not None and high<low:high=None
    return {'source':source,'ticker':r['ticker'].upper().strip(),'actor':r.get('actor'),'filing':filing,'transaction':transaction,'side':side,'shares':shares,'price':price,'low':low,'high':high,'following':number(r.get('shares_following')),'role':str(r.get('role') or '').lower(),'plan':None,'ownership':r.get('ownership')}


def build_trades(payload,security_types=None):
    security_types=security_types or {}
    audit=Counter();eventmap={}
    for r in payload['events']:
        if security_types and r['type']=='congress_trade':
            asset=str(security_types.get('congress',{}).get(str(r['id']),{}).get('asset_class') or '').lower()
            if asset not in ['stock','equity','reit','etf','etf_fund']:
                audit['excluded_congress_security_type']+=1;continue
        n=normalize_event(r)
        if n is None:audit['invalid_event_rows']+=1;continue
        k=signature(n)
        if k in eventmap:audit['duplicate_event_rows']+=1
        else:eventmap[k]=n
    records={};blocked=set()
    for r in payload['normalized']:
        fake={'type':'insider_trade','ticker':r['ticker_normalized'],'side':'purchase' if r['transaction_type_normalized']=='open_market_purchase' else 'sale','actor':r['actor'],'filing_date':r['filing_date'],'transaction_date':r['transaction_date'],'shares':r['shares'],'price':r['price'],'shares_following':r['shares_owned_following'],'role':r.get('officer_title') or ('officer' if r['is_officer'] else 'director' if r['is_director'] else ''),'ownership':r['direct_or_indirect']}
        if not fake['ticker']:continue
        n=normalize_event(fake)
        if n is None:audit['invalid_normalized_rows']+=1;continue
        title=str(security_types.get('insider',{}).get(r['normalized_hash']) or '').lower()
        if any(t in title for t in ['preferred','warrant','option','debenture','convertible note']):
            blocked.add(signature(n));audit['excluded_insider_security_type']+=1;continue
        k=signature(n);fallback=eventmap.get(k,{})
        if n['following'] is None:n['following']=fallback.get('following')
        n['role']=n['role']+' '+fallback.get('role','')
        n['plan']=bool(r['ten_b5_1_flag'])
        # Exact source duplicates collapse; matching display events cannot add volume.
        if k in records:audit['duplicate_normalized_signatures']+=1
        else:records[k]=n
    for k,n in eventmap.items():
        if k in blocked:continue
        if k not in records:records[k]=n
        else:audit['matched_event_normalized_rows']+=1
    # A decimal/units error must not become conviction. Use only nearby
    # transactions already disclosed in the same actor/symbol/filing batch.
    # No correction is guessed; suspect monetary values remain unknown.
    peers=defaultdict(list)
    for r in records.values():
        if r['source']=='insider':peers[(r['ticker'],r['actor'],r['filing'])].append(r)
    for rr in peers.values():
        ps=[r['price'] for r in rr if r['price'] and r['price']>0]
        if len(ps)<3:continue
        if (date.fromisoformat(max(r['transaction'] for r in rr))-date.fromisoformat(min(r['transaction'] for r in rr))).days>7:continue
        center=median(ps)
        if sum(center/2<=p<=center*2 for p in ps)<2:continue
        for r in rr:
            if r['price'] and (r['price']>center*10 or r['price']<center/10):
                r['price']=None;r['low']=None;r['high']=None
                audit['quarantined_same_filing_price_outliers']+=1
    grouped={}
    for r in records.values():
        key=(r['source'],r['ticker'],r['actor'],r['filing'],r['side'])
        if key not in grouped:grouped[key]={**r,'transaction':r['transaction'],'low':0.,'high':0.,'shares':0.,'following':None,'role':'','plan':False,'plan_known':True,'transactions':0,'lag':0}
        g=grouped[key];g['transactions']+=1
        for field in ['low','high','shares']:
            g[field]=g[field]+r[field] if g[field] is not None and r[field] is not None else None
        if r['following'] is not None:g['following']=max(g['following'] or 0,r['following'])
        g['role']+=' '+r['role'];g['plan']=g['plan'] or bool(r['plan']);g['plan_known']=g['plan_known'] and r['plan'] is not None
        g['transaction']=min(g['transaction'],r['transaction']);g['lag']=max(g['lag'],(date.fromisoformat(r['filing'])-date.fromisoformat(r['transaction'])).days)
    trades=sorted(grouped.values(),key=lambda r:(r['filing'],r['ticker'],r['actor'] or '',r['side']))
    opposite={(r['source'],r['ticker'],r['actor'],r['filing'],r['side']) for r in trades}
    prior=defaultdict(list)
    for r in trades:
        r['ownership_increase']=None;r['new_position']=False;r['unusual_ratio']=None;r['prior_purchase_count']=0
        if r['side']=='buy' and r['source']=='insider':
            key=(r['actor'],r['ticker']);past=[p for p in prior[key] if p['filing']<r['filing'] and (date.fromisoformat(r['filing'])-date.fromisoformat(p['filing'])).days<=1095 and p['low'] and p['low']>0]
            r['prior_purchase_count']=len(past)
            if len(past)>=3 and r['low'] is not None:r['unusual_ratio']=r['low']/median(p['low'] for p in past)
            if (r['source'],r['ticker'],r['actor'],r['filing'],'sell') not in opposite and r['shares'] and r['following'] is not None and r['following']>=r['shares']:
                before=r['following']-r['shares'];r['new_position']=before==0
                if before>0:r['ownership_increase']=r['shares']/before
            prior[key].append(r)
    audit['trades_after_dedup']=len(records);audit['actor_symbol_filing_side_groups']=len(trades)
    for source in ['congress','insider']:
        buys=[r for r in trades if r['source']==source and r['side']=='buy']
        audit[source+'_buy_groups']=len(buys);audit[source+'_buy_known_dollars']=sum(r['low'] is not None for r in buys)
    buys=[r for r in trades if r['source']=='insider' and r['side']=='buy']
    audit['insider_known_ownership_increase']=sum(r['ownership_increase'] is not None for r in buys)
    audit['insider_known_unusual_ratio']=sum(r['unusual_ratio'] is not None for r in buys)
    audit['insider_known_plan_flag']=sum(r['plan_known'] for r in buys)
    return trades,dict(audit)


def load_prices():
    prices=defaultdict(dict)
    for name in ['historical-panel.json','ledger-trailing-prices.json','recent-provider-prices.json']:
        for r in json.loads((BASE/name).read_text())['rows']:
            if r[2] and r[3] and r[4] and r[6] in ['fmp:historical-price-eod/full+corporate_actions','massive:grouped-daily-adjusted'] and date.fromisoformat(r[1]).weekday()<5:prices[r[0]][r[1]]=r
    return dict(prices)


def build_member_history(trades,prices):
    """Only disclosure-following returns with a recorded maturity date.
    The caller must use maturity < decision date, never today's member ranking.
    """
    spy=prices['SPY'];days=sorted(spy);dd=[date.fromisoformat(d) for d in days];history=defaultdict(list);audit=Counter()
    for r in trades:
        if r['source']!='congress' or r['side']!='buy' or not r['actor']:continue
        i=bisect_right(days,r['filing'])
        if i>=len(days):continue
        target=dd[i]+timedelta(days=30);j=bisect_left(dd,target)
        stock=prices.get(r['ticker'],{})
        if j>=len(days) or days[i] not in stock or days[j] not in stock:continue
        if (dd[i]-date.fromisoformat(r['filing'])).days>4:continue
        e=stock[days[i]];b=spy[days[i]];ep=e[4]*e[2]/e[3];bp=b[4]*b[2]/b[3]
        if ep<=0 or bp<=0:continue
        rr=100*(stock[days[j]][2]/ep-1);br=100*(spy[days[j]][2]/bp-1)
        history[r['actor']].append({'maturity':days[j],'correct':rr>0 or round(rr-br,2)>0,'excess':max(-100,min(100,rr-br))})
        audit['measured_past_member_purchases']+=1
    return {k:sorted(v,key=lambda r:r['maturity']) for k,v in history.items()},dict(audit)


class ConvictionFeatures:
    def __init__(self,trades,member_history=None):
        self.by_symbol=defaultdict(list);self.history=member_history or {}
        for r in trades:self.by_symbol[r['ticker']].append(r)
        self.dates={s:[r['filing'] for r in rr] for s,rr in self.by_symbol.items()}

    def member_score(self,actor,asof):
        past=[r for r in self.history.get(actor,[]) if r['maturity']<asof]
        # Shrink toward neutral until substantial matured data exist.
        return ((sum(r['correct'] for r in past)+5)/(len(past)+10),len(past)) if len(past)>=5 else (None,len(past))

    @lru_cache(maxsize=100000)
    def features(self,ticker,asof):
        dates=self.dates.get(ticker,[]);records=self.by_symbol.get(ticker,[])
        end=bisect_left(dates,asof);target=date.fromisoformat(asof);f={}
        for n in [30,90]:
            start=bisect_left(dates,(target-timedelta(days=n)).isoformat());rr=records[start:end]
            for source in ['congress','insider']:
                prefix=f'conviction:{source}:{n}:';rows=[r for r in rr if r['source']==source];buys=[r for r in rows if r['side']=='buy'];sells=[r for r in rows if r['side']=='sell']
                for side,ss in [('buy',buys),('sell',sells)]:
                    low=sum(r['low'] or 0 for r in ss);high=sum(r['high'] or 0 for r in ss)
                    f[prefix+side+'_log_dollars_lower']=math.log1p(low)
                    f[prefix+side+'_log_dollars_upper']=math.log1p(high)
                    f[prefix+side+'_missing_amount_fraction']=sum(r['low'] is None or r['high'] is None for r in ss)/len(ss) if ss else 0.
                    f[prefix+side+'_actors']=len(set(r['actor'] for r in ss if r['actor']))
                    f[prefix+side+'_filings']=len(ss)
                complete=all(r['low'] is not None and r['high'] is not None for r in rows)
                net=sum(r['low'] or 0 for r in buys)-sum(r['high'] or 0 for r in sells)
                gross=sum(r['low'] or 0 for r in buys)+sum(r['high'] or 0 for r in sells)
                f[prefix+'net_lower_log']=math.copysign(math.log1p(abs(net)),net) if complete else math.nan
                f[prefix+'net_lower_ratio']=net/gross if gross and complete else 0. if complete else math.nan
                f[prefix+'buy_age']=min((target-date.fromisoformat(r['filing'])).days for r in buys) if buys else 366.
                f[prefix+'max_buy_log']=math.log1p(max((r['low'] or 0 for r in buys),default=0))
                f[prefix+'quick_buyers']=len(set(r['actor'] for r in buys if r['lag']<=7 and r['actor']))
                f[prefix+'mean_disclosure_lag']=sum(r['lag'] for r in buys)/len(buys) if buys else math.nan
                if source=='insider':
                    for attr in ['ownership_increase','unusual_ratio']:
                        vals=[r[attr] for r in buys if r[attr] is not None]
                        f[prefix+attr+'_max_log']=math.log1p(min(max(vals),100)) if vals else math.nan
                        f[prefix+attr+'_known_fraction']=len(vals)/len(buys) if buys else 0.
                    f[prefix+'new_position_buyers']=len(set(r['actor'] for r in buys if r['new_position'] and r['actor']))
                    execs=[r for r in buys if any(s in r['role'] for s in ['chief executive','chief financial','ceo','cfo'])]
                    f[prefix+'executive_buy_log_dollars']=math.log1p(sum(r['low'] or 0 for r in execs))
                    f[prefix+'executive_buyers']=len(set(r['actor'] for r in execs if r['actor']))
                    f[prefix+'known_plan_buys']=sum(r['plan'] for r in buys)
                    f[prefix+'known_plan_fraction']=sum(r['plan_known'] for r in buys)/len(buys) if buys else 0.
                else:
                    actor_scores=[self.member_score(a,asof) for a in set(r['actor'] for r in buys if r['actor'])]
                    known=[s for s,count in actor_scores if s is not None]
                    f[prefix+'member_past_accuracy']=sum(known)/len(known) if known else math.nan
                    f[prefix+'member_past_coverage']=len(known)/len(actor_scores) if actor_scores else 0.
                    weighted=[((self.member_score(r['actor'],asof)[0] or .5)-.5)*(r['low'] or 0) for r in buys]
                    f[prefix+'member_weighted_buy_edge']=sum(weighted)/sum(r['low'] or 0 for r in buys) if sum(r['low'] or 0 for r in buys)>0 else math.nan
        return f
