"""Exploratory coverage-stable gate; fixed earlier selected models, no ledger fitting."""
import json
from collections import defaultdict
import numpy as np
from analyze_cot_models import BASE, augment
from analyze_historical_panel import model_configs,split_for
from analyze_confirmation_models import correct,metrics,original
from evaluate_historical_transfer import frozen_features


def ranked_gate(rows,prob,keep_bears=False):
    pred=-np.ones(len(rows),dtype=int) if keep_bears else np.zeros(len(rows),dtype=int)
    groups=defaultdict(list)
    for i,r in enumerate(rows):
        if keep_bears and r['direction']=='bearish':continue
        groups[r['entry_date']].append(i);pred[i]=0
    for indices in groups.values():
        # Ranking depends only on forecast probabilities, never outcomes.
        selected=sorted(indices,key=lambda i:(-prob[i],rows[i]['ticker']))[:(len(indices)+1)//2]
        pred[selected]=1
    return pred


def main():
    rows=json.loads((BASE/'historical-results.panel.json').read_text());train=[r for r in rows if split_for(r)=='train']
    selected=json.loads((BASE/'cot-selection.json').read_text())['bull_gate'];name=selected['key'].split('|')[1]
    names=sorted(augment(train[0],True));x=np.array([[f[n] for n in names] for r in train for f in [augment(r,True)]])
    model=dict(model_configs())[name]();model.fit(x,[correct(r,1) for r in train])
    result={'policy':'Top half of bullish opportunities per entry-date batch; earlier selected price+COT model; exploratory, requires contemporaneous batch universe','historical':{},'ledger':{}}
    for split in ['validation','test']:
        rr=[r for r in rows if split_for(r)==split];x=np.array([[f[n] for n in names] for r in rr for f in [augment(r,True)]])
        p=ranked_gate(rr,model.predict_proba(x)[:,1]);result['historical'][split]={h:metrics(rr,p,h) for h in ['30','7']}
    payload=json.loads((BASE/'ledger-trailing-prices.json').read_text());prices={s:{} for s in payload['symbols']}
    for r in payload['rows']:prices[r[0]][r[1]]=r
    for r in json.loads((BASE/'recent-provider-prices.json').read_text())['rows']:
        if r[7]=='split_adjusted_price_return' and r[2]==r[3]:prices.setdefault(r[0],{})[r[1]]=r
    cohort=json.loads((BASE/'cohort.json').read_text())['events'];covered=[]
    for r in cohort:
        f=frozen_features(r,prices)
        if f is not None:covered.append({**r,'features':f})
    x=np.array([[f[n] for n in names] for r in covered for f in [augment(r,True)]])
    p=ranked_gate(covered,model.predict_proba(x)[:,1],True);by_id={r['id']:int(v) for r,v in zip(covered,p)}
    for h in ['30','7']:
        full=[r for r in cohort if h in r['outcomes']];pp=np.array([by_id.get(r['id'],1 if r['direction']=='bullish' else -1) for r in full])
        result['ledger'][h]=metrics(full,pp,h)
    (BASE/'ranked-bullish-results.json').write_text(json.dumps(result,indent=2));print(json.dumps(result,indent=2))


if __name__=='__main__':main()
