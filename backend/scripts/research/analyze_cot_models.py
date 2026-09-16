"""Exploratory raw-COT follow-up. Previously seen test period is not fresh validation."""
import csv
import io
import json
import zipfile
from datetime import date,timedelta
from functools import lru_cache
from pathlib import Path
import numpy as np
from analyze_historical_panel import model_configs, split_for, predictions
from analyze_confirmation_models import correct, metrics, original
from evaluate_historical_transfer import frozen_features

BASE=Path('frontend/test-results/confirmation-research')
CODES={'sp':'13874A','nasdaq':'209742','russell':'239742','dollar':'098662','treasury':'043602'}
PARTICIPANTS=['Dealer','Asset_Mgr','Lev_Money']


def available_date(report):
    """Conservative one-week lag plus known extended disruptions.

    Backlog windows are withheld until after catch-up, rather than inventing
    normal Friday availability. Current archive revisions remain a limitation.
    """
    d=date.fromisoformat(report);available=d+timedelta(days=7)
    for first,last,resume in [('2013-10-01','2013-10-31','2013-12-01'),('2018-12-18','2019-03-05','2019-04-01'),('2023-01-31','2023-03-14','2023-04-01'),('2025-09-30','2025-12-23','2026-01-06')]:
        if first<=report<=last:available=max(available,date.fromisoformat(resume))
    return available.isoformat()


def load_cot():
    data={k:{} for k in CODES}
    reverse={v:k for k,v in CODES.items()}
    for path in sorted((BASE/'cot').glob('*.zip')):
        with zipfile.ZipFile(path) as z:
            for r in csv.DictReader(io.StringIO(z.read(z.namelist()[0]).decode('utf-8-sig'))):
                code=r['CFTC_Contract_Market_Code'].strip()
                if code not in reverse:continue
                day=r.get('Report_Date_as_YYYY-MM-DD') or r.get('Report_Date_as_MM_DD_YYYY')
                if '/' in day:
                    m,d,y=day.split('/');day=f'{int(y):04}-{int(m):02}-{int(d):02}'
                oi=float(r['Open_Interest_All'].replace(',',''))
                if oi<=0:continue
                def number(name):return float(r[name].replace(',',''))
                values={p:(number(p+'_Positions_Long_All')-number(p+'_Positions_Short_All'))/oi for p in PARTICIPANTS}
                data[reverse[code]][day]=values
    return data


COT=load_cot()


@lru_cache(maxsize=3000)
def cot_features(asof):
    out={}
    for asset,history in COT.items():
        days=sorted(d for d in history if available_date(d)<=asof)
        fresh=bool(days and (date.fromisoformat(asof)-date.fromisoformat(days[-1])).days<=28)
        for p in PARTICIPANTS:
            vals=np.array([history[d][p] for d in days[-53:]])
            key=f'cot:{asset}:{p}'
            out[key+':net_oi']=vals[-1] if fresh else np.nan
            for n in [4,13]:out[key+f':change_{n}w']=vals[-1]-vals[-1-n] if fresh and len(vals)>n else np.nan
            out[key+':percentile_52w']=float(np.mean(vals<=vals[-1])) if fresh and len(vals)>=40 else np.nan
    return out


def augment(row,price=True):
    asof=row.get('decision_date') or row['calculated_at'][:10]
    f=dict(row['features']) if price else {}
    f.update(cot_features(asof))
    if price:
        for asset in ['sp','nasdaq']:
            for p in ['Asset_Mgr','Lev_Money']:
                f[f'cot_interaction:{asset}:{p}']=f[f'cot:{asset}:{p}:change_4w']*f['relative_strength_20']
    return f


def main():
    rows=json.loads((BASE/'historical-results.panel.json').read_text());groups={s:[r for r in rows if split_for(r)==s] for s in ['train','validation','test']}
    trained={};candidates=[];names_by_family={}
    for family in ['cot_only','price_plus_cot']:
        price=family=='price_plus_cot';names=sorted(augment(groups['train'][0],price));names_by_family[family]=names
        x={s:np.array([[f[n] for n in names] for r in rr for f in [augment(r,price)]]) for s,rr in groups.items() if s!='test'}
        for name,factory in model_configs():
            key=family+'|'+name;models=[factory(),factory()]
            for side,m in zip([1,-1],models):m.fit(x['train'],[correct(r,side) for r in groups['train']])
            trained[key]=models;prob=np.column_stack([m.predict_proba(x['validation'])[:,1] for m in models])
            for mode in ['bull_gate','directional','selective']:
                for threshold in ([0.] if mode=='directional' else [.45,.50,.55,.60,.65,.70,.75,.80]):
                    pred=predictions(prob,mode,threshold);metric=metrics(groups['validation'],pred)
                    if metric['coverage']>=50:candidates.append({'key':key,'family':family,'mode':mode,'threshold':threshold,'validation':metric})
            print('Validation',key,flush=True)
    selected={mode:max([c for c in candidates if c['mode']==mode],key=lambda c:(c['validation']['wilson95'][0],c['validation']['coverage'])) for mode in ['bull_gate','directional','selective']}
    (BASE/'cot-selection.json').write_text(json.dumps(selected,indent=2))
    result={'availability_policy':'report + 7 calendar days; disruption backlogs withheld; revised archives, exploratory only','cot_coverage':{k:{'n':len(v),'first':min(v),'last':max(v)} for k,v in COT.items()},'selected':selected,'validation_candidates':candidates,'historical_test':{},'ledger':{}}
    test=groups['test']
    cohort=json.loads((BASE/'cohort.json').read_text())['events'];payload=json.loads((BASE/'ledger-trailing-prices.json').read_text());prices={s:{} for s in payload['symbols']}
    for r in payload['rows']:prices[r[0]][r[1]]=r
    for r in json.loads((BASE/'recent-provider-prices.json').read_text())['rows']:
        if r[7]=='split_adjusted_price_return' and r[2]==r[3]:prices.setdefault(r[0],{})[r[1]]=r
    covered=[]
    for r in cohort:
        f=frozen_features(r,prices)
        if f is not None:covered.append({**r,'features':f})
    for mode,c in selected.items():
        names=names_by_family[c['family']];price=c['family']=='price_plus_cot';models=trained[c['key']]
        x=np.array([[f[n] for n in names] for r in test for f in [augment(r,price)]]);prob=np.column_stack([m.predict_proba(x)[:,1] for m in models]);p=predictions(prob,mode,c['threshold'])
        result['historical_test'][mode]={h:metrics(test,p,h) for h in ['30','7']}
        eligible=covered if price else cohort
        x=np.array([[f[n] for n in names] for r in eligible for f in [augment(r,price)]]);prob=np.column_stack([m.predict_proba(x)[:,1] for m in models]);p=predictions(prob,mode,c['threshold'])
        if mode=='bull_gate':p=np.where(original(eligible)==-1,-1,p)
        by_id={r['id']:int(pred) for r,pred in zip(eligible,p)}
        result['ledger'][mode]={}
        for h in ['30','7']:
            full=[r for r in cohort if h in r['outcomes']]
            pp=np.array([by_id.get(r['id'],1 if r['direction']=='bullish' else -1) for r in full])
            result['ledger'][mode][h]=metrics(full,pp,h)
    (BASE/'cot-results.json').write_text(json.dumps(result,indent=2,allow_nan=False))
    print(json.dumps({'selected':selected,'historical_test':result['historical_test'],'ledger':result['ledger']},indent=2))


if __name__=='__main__':main()

