"""Freeze, acquire, then evaluate one risk filter on reserved issuers. Local research."""
import argparse
import hashlib
import json
import math
import urllib.error
import sys
from collections import Counter,defaultdict
from datetime import datetime,timezone
from pathlib import Path
import joblib
import numpy as np
import sklearn
from threadpoolctl import threadpool_limits
from analyze_direct_returns import QUARTERS,target_return
from rank_calibration_model import partition,date_weights,half_year_before,prepare
from sec_filing_model import make_model
from sec_filing_features import extract_series,build_features
from downside_selection import tree,fit_tree,choose,measures,pair_metrics
from fetch_sec_fundamentals_pilot import fetch

BASE=Path('frontend/test-results/confirmation-research')
PILOT=BASE/'sec-filing-pilot-2026-09-15'
PROTOCOL=Path('docs/confirmation-risk-holdout-protocol-2026-09-15.md')
SCRIPTS=Path('backend/scripts/research')
POLICIES=('baseline','expected_loss','low_volatility')


def sha(path):return hashlib.sha256(path.read_bytes()).hexdigest()


def risk_matrix(rows,pn,fn):
    return np.array([[r['ranks'][n] for n in pn]+[r['fundamentals'][n] for n in fn]+[r['features']['volatility_20'],r['features']['volatility_60']] for r in rows])


def base_matrix(rows,fn):return np.array([[r['fundamentals'][n] for n in fn] for r in rows])


def verify_freeze(out):
    frozen=json.loads((out/'freeze.json').read_bytes())
    if frozen['versions']!={'python':sys.version,'numpy':np.__version__,'sklearn':sklearn.__version__,'joblib':joblib.__version__}:
        raise ValueError('Frozen dependency versions changed')
    for name,expected in frozen['sha256'].items():
        if sha(Path(name))!=expected:raise ValueError('Frozen dependency changed: '+name)
    return frozen


def freeze(out):
    if (out/'freeze.json').exists():verify_freeze(out);print('Existing freeze verified; no models changed',flush=True);return
    if (out/'reserved-prices.json').exists():raise ValueError('Cannot freeze after reserved data acquisition')
    groups=json.loads((PILOT/'prepared.json').read_bytes())['groups']
    prior=json.loads((BASE/'downside-selection-2026-09-15/predictions.json').read_bytes())
    old={(r['group'],r['ticker'],r['entry_date']):r for r in prior}
    pn=sorted(groups['development'][0]['ranks']);fn=sorted(groups['development'][0]['fundamentals'])
    models={};folds=[]
    with threadpool_limits(limits=4):
        for start,end in QUARTERS[1:]:
            fit,_=partition(groups['development'],start);w=date_weights(fit);y=np.array([target_return(r,'raw') for r in fit])
            baseline=make_model('trees');baseline.fit(base_matrix(fit,fn),y>0,extratreesclassifier__sample_weight=w)
            risk=fit_tree(tree(),risk_matrix(fit,pn,fn),np.maximum(-y,0),w)
            check=[r for rr in groups.values() for r in rr if start<=r['entry_date']<end]
            expected=[old[(r['group'],r['ticker'],r['entry_date'])] for r in check]
            bp=baseline.predict_proba(base_matrix(check,fn))[:,1];rp=risk.predict(risk_matrix(check,pn,fn))
            np.testing.assert_allclose(bp,[r['base_probability'] for r in expected],rtol=0,atol=1e-12)
            np.testing.assert_allclose(rp,[r['predicted_loss'] for r in expected],rtol=0,atol=1e-12)
            models[start]={'baseline':baseline,'risk':risk}
            folds.append({'start':start,'end':end,'fit':len(fit),'latest_fit_target':max(r['outcomes']['30']['target_date'] for r in fit),'reproduced_predictions':len(check)})
            print('Reproduced and froze',start,len(check),flush=True)
    model_path=out/'models.joblib';joblib.dump({'models':models,'price_names':pn,'fundamental_names':fn},model_path)
    files=[PROTOCOL,model_path,PILOT/'prepared.json',PILOT/'company-tickers.json',BASE/'downside-selection-2026-09-15/predictions.json']
    files += [SCRIPTS/n for n in ['risk_reserved_confirmation.py','export_risk_reserved.py','downside_selection.py','sec_filing_model.py','sec_filing_features.py','rank_calibration_model.py','analyze_historical_panel.py','analyze_direct_returns.py','fetch_sec_fundamentals_pilot.py']]
    (out/'freeze.json').write_text(json.dumps({'frozen_at_utc':datetime.now(timezone.utc).isoformat(),'candidate':'expected_loss','objective':'accuracy preservation and endpoint loss reduction; not higher returns','folds':folds,
        'versions':{'python':sys.version,'numpy':np.__version__,'sklearn':sklearn.__version__,'joblib':joblib.__version__},'sha256':{str(p):sha(p) for p in files}},indent=2))
    print('Source, protocol, training data and five fitted model pairs frozen before holdout acquisition.',flush=True)


def issuer_selection(payload,mapping,prior_payloads):
    ordered=payload['ordered_eligible']
    if len(ordered)!=931:raise ValueError('Changed eligible population')
    for i,old in enumerate(prior_payloads):
        if set(ordered[i*256:(i+1)*256])-{'SPY','QQQ'} != set(old['symbols'])-{'SPY','QQQ'}:
            raise ValueError('Earlier hash-block membership changed')
    mapping={r['ticker'].upper():str(r['cik_str']).zfill(10) for r in mapping.values()}
    earlier_ciks={mapping[s] for s in ordered[:768] if s in mapping}
    selected=[];excluded=[];seen=set()
    for ticker in sorted(set(ordered[768:])-{'SPY','QQQ'}):
        cik=mapping.get(ticker)
        reason='unmapped' if not cik else 'earlier_issuer' if cik in earlier_ciks else 'duplicate_reserved_issuer' if cik in seen else None
        if reason:excluded.append({'ticker':ticker,'cik':cik,'reason':reason});continue
        selected.append({'ticker':ticker,'cik':cik});seen.add(cik)
    return {'selected':selected,'excluded':excluded,'raw_reserved_count':len(ordered[768:]),'reserved_stock_count':len(set(ordered[768:])-{'SPY','QQQ'})}


def collect(out):
    verify_freeze(out)
    payload=json.loads((out/'reserved-prices.json').read_bytes())
    priors=[json.loads(p.read_bytes()) for p in [BASE/'historical-panel.json',BASE/'direct-return-2026-09-15/extension.json',BASE/'rank-calibration-2026-09-15/validation-prices.json']]
    selection=issuer_selection(payload,json.loads((PILOT/'company-tickers.json').read_bytes()),priors)
    path=out/'issuer-selection.json'
    if path.exists() and json.loads(path.read_bytes())!=selection:raise ValueError('Issuer selection changed')
    if not path.exists():path.write_text(json.dumps(selection,indent=2))
    symbols={r['ticker'] for r in selection['selected']}|{'SPY','QQQ'}
    filtered={**payload,'symbols':sorted(symbols),'rows':[r for r in payload['rows'] if r[0] in symbols]}
    (out/'issuer-prices.json').write_text(json.dumps(filtered,separators=(',',':')))
    (out/'facts').mkdir(exist_ok=True)
    print('Frozen reserved issuer selection',len(selection['selected']),'excluded',len(selection['excluded']),flush=True)
    audit=[]
    for i,item in enumerate(selection['selected']):
        try:
            p=fetch('https://data.sec.gov/api/xbrl/companyfacts/CIK'+item['cik']+'.json',out/'facts'/(item['cik']+'.json'))
            status='ok' if p.get('facts',{}).get('us-gaap') else 'no_us_gaap'
        except urllib.error.HTTPError as exc:
            if exc.code in [403,429]:raise SystemExit(f'SEC access/rate limit {exc.code}; stop without bypass')
            status=f'http_{exc.code}'
        except (urllib.error.URLError,TimeoutError) as exc:status=type(exc).__name__
        audit.append({**item,'status':status});(out/'fetch-audit.json').write_text(json.dumps(audit,indent=2))
        if (i+1)%8==0:print('Collected reserved SEC facts',i+1,'of',len(selection['selected']),flush=True)
    print('Reserved SEC acquisition complete',dict(Counter(r['status'] for r in audit)),flush=True)


def decision(report):
    b=report['policies']['baseline'];c=report['policies']['expected_loss'];paired=c['vs_baseline']
    tests={'adequate_sample':report['issuers']>=50 and c['dates']>=50 and c['n_selected']>=1000,
           'hit_rate_noninferiority':paired['hit_rate']['block95'][0]>-2,
           'lower_mean_downside':paired['downside']['block95'][0]>0,
           'fewer_large_losses':paired['large_loss_rate']['block95'][0]>0,
           'four_quarters_lower_downside':sum(q['downside']<b['quarterly'][d]['downside'] for d,q in c['quarterly'].items())>=4}
    return {'checks':tests,'status':'pass_risk_only' if all(tests.values()) else 'insufficient_sample' if not tests['adequate_sample'] else 'not_confirmed',
            'higher_returns_tested_as_success_criterion':False,'deployment_authorized':False}


def evaluate(records):
    groups=defaultdict(list)
    for r in records:groups[r['entry_date']].append(r)
    daily={};policies={}
    universe=[{'date':d,**measures(rr)} for d,rr in sorted(groups.items())]
    for policy in POLICIES:
        dd=[];pool=[]
        for d,rr in sorted(groups.items()):
            ix=choose(rr,policy);chosen=[rr[i] for i in ix];pool.extend(chosen)
            dd.append({'date':d,'n':len(chosen),'overlap':len(set(ix)&set(choose(rr,'baseline')))/len(ix),**measures(chosen)})
        raw=np.array([r['raw_return'] for r in pool]);losers=raw[raw<0]
        s={n:float(np.mean([r[n] for r in dd])) for n in ['hit_rate','return','excess_return','downside','large_loss_rate','overlap']}
        s.update(n_selected=len(pool),dates=len(dd),coverage_pct=100*len(pool)/len(records),average_losing_return=float(losers.mean()) if len(losers) else None,
                 worst_decile_mean_return=float(np.sort(raw)[:math.ceil(len(raw)/10)].mean()),quarterly={})
        for start,end in QUARTERS[1:]:
            qq=[r for r in dd if start<=r['date']<end]
            if qq:s['quarterly'][start]={n:float(np.mean([r[n] for r in qq])) for n in ['hit_rate','return','downside','large_loss_rate']}
        daily[policy]=dd;policies[policy]=s
    for policy in POLICIES:
        for label,ref in [('baseline',daily['baseline']),('low_volatility',daily['low_volatility']),('universe',universe)]:
            policies[policy]['vs_'+label]=pair_metrics(daily[policy],ref)
    return {'n':len(records),'issuers':len(set(r['cik'] for r in records)),'policies':policies,'daily':daily,
            'universe':{n:float(np.mean([r[n] for r in universe])) for n in ['hit_rate','return','excess_return','downside','large_loss_rate']}}


def run_test(out):
    frozen=verify_freeze(out)
    if (out/'results.json').exists():raise ValueError('Reserved evaluation already complete; inspect saved results, do not rerun')
    audit=json.loads((out/'fetch-audit.json').read_bytes());selection=json.loads((out/'issuer-selection.json').read_bytes())
    if len(audit)!=len(selection['selected']):raise ValueError('Reserved acquisition incomplete')
    rows,price_audit=prepare([out/'issuer-prices.json'],'reserved')
    lookup={};cik_by_symbol={r['ticker']:r['cik'] for r in selection['selected']}
    for item in selection['selected']:
        p=out/'facts'/(item['cik']+'.json')
        if p.exists():lookup[item['ticker']]=extract_series(json.loads(p.read_bytes()))
    covered=[];counts=Counter();references=0
    for r in rows:
        if r['ticker'] not in lookup:counts['missing_company_facts_rows']+=1;continue
        f,usable,refs=build_features(lookup[r['ticker']],r['decision_date'])
        if not usable:counts['no_current_financials']+=1;continue
        if '30' not in r['outcomes']:counts['missing_outcome']+=1;continue
        if any(x['filed']>=r['decision_date'] for x in refs):raise AssertionError('SEC date leak')
        references+=len(refs);covered.append({**r,'fundamentals':f,'cik':cik_by_symbol[r['ticker']],'filing_provenance':refs})
    (out/'prepared.json').write_text(json.dumps({'rows':covered,'price_audit':price_audit,'coverage':dict(counts),'filing_references_verified':references},allow_nan=True))
    models=joblib.load(out/'models.joblib');pn=models['price_names'];fn=models['fundamental_names'];records=[]
    with threadpool_limits(limits=4):
        for start,end in QUARTERS[1:]:
            rr=[r for r in covered if start<=r['entry_date']<end]
            if not rr:continue
            pair=models['models'][start];p=pair['baseline'].predict_proba(base_matrix(rr,fn))[:,1];loss=pair['risk'].predict(risk_matrix(rr,pn,fn))
            for r,b,l in zip(rr,p,loss):
                records.append({'ticker':r['ticker'],'cik':r['cik'],'entry_date':r['entry_date'],'target_date':r['outcomes']['30']['target_date'],
                    'raw_return':target_return(r,'raw'),'excess_return':target_return(r,'excess'),'base_probability':float(b),'predicted_loss':float(l),
                    'prior_volatility':r['features']['volatility_60']})
            print('Applied frozen models',start,len(rr),flush=True)
    if not records:raise ValueError('No measurable holdout; cannot test')
    (out/'predictions.json').write_text(json.dumps(records,allow_nan=False))
    report=evaluate(records);report.update(decision=decision(report),price_audit=price_audit,coverage=dict(counts),filing_references_verified=references,
        frozen_at_utc=frozen['frozen_at_utc'],evaluated_at_utc=datetime.now(timezone.utc).isoformat(),
        data_sha256={str(p):sha(p) for p in [out/'reserved-prices.json',out/'issuer-selection.json',out/'issuer-prices.json',out/'fetch-audit.json']+list((out/'facts').glob('*.json'))})
    (out/'results.json').write_text(json.dumps(report,indent=2,allow_nan=False))
    print(json.dumps({'n':report['n'],'issuers':report['issuers'],'policies':{n:{k:s[k] for k in ['hit_rate','return','downside','large_loss_rate','n_selected','dates']} for n,s in report['policies'].items()},'decision':report['decision']},indent=2),flush=True)


def main():
    ap=argparse.ArgumentParser();ap.add_argument('directory',type=Path);ap.add_argument('action',choices=['freeze','collect','evaluate']);args=ap.parse_args()
    args.directory.mkdir(parents=True,exist_ok=True)
    {'freeze':freeze,'collect':collect,'evaluate':run_test}[args.action](args.directory)


if __name__=='__main__':main()
