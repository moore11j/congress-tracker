"""Point-in-time expanded options features and separate research labels."""
from bisect import bisect_left, bisect_right
from collections import Counter
from datetime import date, timedelta
import json
import math
from statistics import mean, pstdev

from analyze_options_activity_pilot import activity_features
from options_activity_expanded import ROOT, BASE, PROVIDERS, LEDGER_HASH, load_stock_prices
from options_activity_pilot import digest, save


def price_features(ticker,cutoff,prices):
    sessions=sorted(prices['SPY']); days=sessions[:bisect_right(sessions,cutoff)][-61:]
    if len(days)!=61 or days[-1]!=cutoff:return None,'insufficient_price_history'
    f={}
    for prefix,symbol in [('stock',ticker),('market','SPY')]:
        if any(d not in prices.get(symbol,{}) for d in days):return None,'missing_price_session'
        c=[prices[symbol][d][2] for d in days]
        if min(c)<=0 or any(not math.isfinite(v) for v in c):return None,'invalid_price'
        lr=[math.log(c[i]/c[i-1]) for i in range(1,len(c))]
        if max(abs(r) for r in lr)>math.log(3):return None,'input_price_discontinuity'
        for n in [5,20,60]:f[prefix+':momentum_'+str(n)]=c[-1]/c[-1-n]-1
        for n in [20,60]:f[prefix+':sma_'+str(n)]=c[-1]/mean(c[-n:])-1
        f[prefix+':volatility_20']=pstdev(lr[-20:])*math.sqrt(252)
        f[prefix+':drawdown_20']=c[-1]/max(c[-20:])-1
    for n in [5,20,60]:f['relative:momentum_'+str(n)]=f['stock:momentum_'+str(n)]-f['market:momentum_'+str(n)]
    return f,'ok'


def option_inputs(f):
    """Predeclared continuous inputs; no activity threshold or signed-flow inference."""
    def logratio(name):
        x=f.get(name)
        return math.log1p(min(max(x,0),100)) if x is not None else None
    return {'options:log_premium_activity_ratio':logratio('premium_activity_ratio'),
            'options:log_volume_activity_ratio':logratio('volume_activity_ratio'),
            'options:call_premium_share':f['recent_call_premium_share'],
            'options:baseline_call_premium_share':f['baseline_call_premium_share'],
            'options:call_premium_share_change':f['call_premium_share_change'],
            'options:call_volume_share':f['recent_call_volume_share'],
            'options:baseline_call_volume_share':f['baseline_call_volume_share'],
            'options:log_recent_premium':math.log1p(f['recent_call_premium']+f['recent_put_premium']),
            'options:log_recent_volume':math.log1p(f['recent_call_volume']+f['recent_put_volume'])}


def event_features(group,pair,decision,prices,sessions):
    cutoff=decision['cutoff']
    if cutoff<group['anchor']:return None,{'status':'before_contract_selection'}
    if not pair:return None,{'status':'no_matched_pair'}
    expiry=pair[0]['contract']['expiration_date']; dte=(date.fromisoformat(expiry)-date.fromisoformat(cutoff)).days
    if not 30<=dte<=90:return None,{'status':'outside_30_90_dte','dte':dte}
    f=activity_features(pair,cutoff,sessions)
    if f['status']!='ok':return None,f
    controls,status=price_features(group['ticker'],cutoff,prices)
    if controls is None:return None,{'status':status}
    controls['contract:days_to_expiry']=dte
    controls['contract:moneyness']=prices[group['ticker']][cutoff][2]/pair[0]['contract']['strike_price']-1
    return {**controls,**option_inputs(f)},{'status':'ok','contracts':f['sample_contracts'],
            'expiry':expiry,'strike':f['strike'],'option_window_start':f['window_start'],
            'future_bars_ignored':sum(a.get('discarded_future_bars',0) for a in f['audit'].values())}


def asset_return(symbol,entry,target,by_provider):
    for provider in PROVIDERS:
        book=by_provider[provider].get(symbol,{})
        if entry not in book or target not in book:continue
        e=book[entry]; opening=e[4]*e[2]/e[3]
        if opening<=0:continue
        return {'return':100*(book[target][2]/opening-1),'provider':provider,
                'entry_price':opening,'target_price':book[target][2]}
    return None


def label(ticker,entry,horizon,sessions,by_provider):
    due=(date.fromisoformat(entry)+timedelta(days=horizon)).isoformat();ix=bisect_left(sessions,due)
    if ix>=len(sessions):return {'status':'immature','due_date':due}
    target=sessions[ix]
    if (date.fromisoformat(target)-date.fromisoformat(due)).days>4:return {'status':'calendar_gap','due_date':due}
    stock=asset_return(ticker,entry,target,by_provider);spy=asset_return('SPY',entry,target,by_provider)
    if stock is None or spy is None:return {'status':'missing_consistent_prices','target_date':target}
    raw=stock['return'];br=spy['return'];excess=round(raw-br,2)
    return {'status':'measured','target_date':target,'raw_return':raw,'spy_return':br,'excess_return':excess,
            'correct':raw>0 or excess>0,'raw_correct':raw>0,'stock_prices':stock,'spy_prices':spy}


def model_split(row):
    h=row['outcomes']['30']
    if h['status']!='measured':return None
    if row['split']=='train' and h['target_date']<'2026-01-01':return 'train'
    if row['split']=='validation' and h['target_date']<'2026-04-01':return 'validation'
    if row['split']=='test' and h['target_date']<='2026-09-11':return 'test'
    return None


def main():
    assert digest(BASE/'cohort.json')==LEDGER_HASH
    config=json.loads((ROOT/'cohort.json').read_text())
    missing=[g['id'] for g in config['groups'] if not (ROOT/'groups'/(g['id']+'.json')).exists()]
    if missing:raise SystemExit('Collection incomplete: '+str(len(missing))+' groups missing')
    prices,by_provider=load_stock_prices();sessions=sorted(prices['SPY']);rows=[];groups={}
    for group in config['groups']:
        data=json.loads((ROOT/'groups'/(group['id']+'.json')).read_text())
        if data['group']!=group:raise ValueError('Group identity mismatch')
        groups[group['id']]=data
        for decision in group['decisions']:
            f,audit=event_features(group,data['pair'],decision,prices,sessions)
            rows.append({'id':group['id']+'_'+decision['entry_date'],'ticker':group['ticker'],
                         'anchor':group['anchor'],'split':group['split'],**decision,'features':f,'audit':audit})
    # Persist every planned observation's availability before generating any label.
    save(ROOT/'features.json',{'rows':rows,'cohort_sha256':digest(ROOT/'cohort.json')})
    labeled=[{**r,'outcomes':{str(h):label(r['ticker'],r['entry_date'],h,sessions,by_provider) for h in [30,7]}} for r in rows]
    save(ROOT/'panel.json',{'rows':labeled,'features_sha256':digest(ROOT/'features.json')})
    # Apply the latest already-selected anchor basket to each original ledger event.
    ledger=json.loads((BASE/'cohort.json').read_text())['events'];transfer=[]
    from options_activity_pilot import cutoff_session
    for original in ledger:
        candidates=[g for g in config['groups'] if g['ticker']==original['ticker']]
        if not candidates:continue
        cutoff=cutoff_session(original['calculated_at'],sessions)
        candidates=[g for g in candidates if g['anchor']<=cutoff<=g['window_end']]
        if not candidates:
            transfer.append({'id':original['id'],'ticker':original['ticker'],'cutoff':cutoff,'features':None,'audit':{'status':'outside_selected_windows'}});continue
        g=max(candidates,key=lambda r:r['anchor'])
        f,audit=event_features(g,groups[g['id']]['pair'],{'cutoff':cutoff},prices,sessions)
        transfer.append({'id':original['id'],'ticker':original['ticker'],'cutoff':cutoff,'features':f,'audit':audit})
    save(ROOT/'ledger-features.json',{'rows':transfer,'ledger_sha256':LEDGER_HASH})
    status={'planned':len(rows),'feature_coverage':dict(Counter(r['audit']['status'] for r in rows)),
            'model_rows':{s:sum(r['features'] is not None and model_split(r)==s for r in labeled) for s in ['train','validation','test']},
            'labels':{h:dict(Counter(r['outcomes'][h]['status'] for r in labeled)) for h in ['30','7']},
            'ledger_coverage':dict(Counter(r['audit']['status'] for r in transfer))}
    save(ROOT/'panel-audit.json',status);print(json.dumps(status,indent=2))
    assert digest(BASE/'cohort.json')==LEDGER_HASH


if __name__=='__main__':main()
