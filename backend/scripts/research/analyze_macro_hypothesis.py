"""Fixed, exploratory macro/market gates. No training, application writes, or deployment.

The prior holdout has already been inspected: its results here are diagnostics,
not a new untouched test. Benchmark history is a reconstruction, not vintage data.
"""
from __future__ import annotations
import ast
import hashlib
import json
from collections import Counter
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from pathlib import Path
from types import SimpleNamespace

from analyze_confirmation_models import SOURCES, correct, metrics, original, split_for

BASE = Path('frontend/test-results/confirmation-research')
SCORE_SOURCE = Path('backend/app/services/confirmation_score.py')


def load_direction_classifier():
    source = SCORE_SOURCE.read_text(encoding='utf-8')
    functions = {'classify_confirmation_direction','_directional_evidence_weight',
                 '_freshness_score','_is_material_directional_evidence','_classification_strength'}
    constants = {'SOURCE_ORDER','SUPPORT_ONLY_SOURCE_KEYS','MATERIAL_DIRECTIONAL_EVIDENCE_MIN',
                 'DEFENSIBLE_DIRECTIONAL_MARGIN','CONFLICT_DIRECTIONAL_MARGIN',
                 'CONFLICT_DIRECTIONAL_EDGE_RATIO','MATERIAL_EVIDENCE_MAX_FRESHNESS_DAYS',
                 'THIRTY_DAY_DURABLE_SOURCES','SHORT_HORIZON_SOURCES'}
    nodes=[]
    for node in ast.parse(source).body:
        if isinstance(node,ast.FunctionDef) and node.name in functions: nodes.append(node)
        if isinstance(node,ast.Assign) and any(isinstance(t,ast.Name) and t.id in constants for t in node.targets): nodes.append(node)
        if isinstance(node,ast.AnnAssign) and isinstance(node.target,ast.Name) and node.target.id in constants: nodes.append(node)
    ns={'ConfirmationClassification':SimpleNamespace}
    exec('from __future__ import annotations\n'+ast.unparse(ast.Module(body=nodes,type_ignores=[])),ns)
    return ns


def freshness(row,key):
    value=row.get('freshness',{}).get(key,{})
    return value.get('freshness_days') if isinstance(value,dict) else value


def sources_for(row):
    return {k:SimpleNamespace(present=bool((s:=row['sources'].get(k,{})).get('present')),
              direction=s.get('direction','neutral'),strength=s.get('strength',0) or 0,
              quality=s.get('quality',0) or 0,score_contribution=s.get('score_contribution',0) or 0,
              freshness_days=freshness(row,k)) for k in SOURCES}


@lru_cache(maxsize=4)
def classifier_for(variant):
    ns=load_direction_classifier()
    base_weight=ns['_directional_evidence_weight']
    def weight(key,source):
        if key=='macro_positioning' and source.direction=='bearish' and source.present:
            if variant=='symmetric_moderate' and source.strength==50 and source.quality==61:
                source=SimpleNamespace(**{**vars(source),'strength':70,'quality':77})
                return base_weight(key,source)*(1.2/1.08)
            if variant in ['macro_1.5x','macro_2x']:
                return base_weight(key,source)*(1.5 if variant=='macro_1.5x' else 2.)
        return base_weight(key,source)
    ns['_directional_evidence_weight']=weight
    return ns['classify_confirmation_direction']


def direction_under(row,variant):
    return classifier_for(variant)(sources_for(row)).direction


def market_features(row,prices):
    cutoff=datetime.fromisoformat(row['calculated_at'].replace('Z','+00:00'))
    if cutoff.tzinfo is None: cutoff=cutoff.replace(tzinfo=timezone.utc)
    result={}
    # Export spans July-September only, when US session closes are 20:00 UTC.
    # No holiday close is invented; only cache rows with provider attribution qualify.
    for symbol in ['SPY','QQQ']:
        valid=sorted([p for p in prices if p['symbol']==symbol and p['source']
                      and datetime.fromisoformat(p['date']+'T20:00:00+00:00')<=cutoff
                      and datetime.fromisoformat(p['date']).weekday()<5
                      and (p.get('raw_close') or 0)>0],key=lambda p:p['date'])
        if not valid: continue
        end=valid[-1]; end_date=datetime.fromisoformat(end['date'])
        if (cutoff.date()-end_date.date()).days>4: continue
        result[symbol+'_asof']=end['date']
        for horizon in [7,30]:
            start_target=end_date-timedelta(days=horizon)
            previous=[p for p in valid if p['date']<=start_target.date().isoformat()]
            if previous:
                start=previous[-1]
                if (start_target-datetime.fromisoformat(start['date'])).days<=4:
                    result[f'{symbol}_{horizon}']=100*(end['raw_close']/start['raw_close']-1)
    return result


def macro_headwind(row):
    s=row['sources'].get('macro_positioning',{})
    days=freshness(row,'macro_positioning')
    return bool(s.get('present') and s.get('direction')=='bearish' and days is not None and days<=10)


def stock_override(row):
    price=row['sources'].get('price_volume',{})
    price_age=freshness(row,'price_volume')
    evidence=sum(1 for k,s in row['sources'].items() if k in SOURCES
                 and k not in ['macro_positioning','price_volume','government_contracts']
                 and s.get('present') and s.get('direction')=='bullish'
                 and freshness(row,k) is not None and freshness(row,k)<=30)
    return bool(price.get('present') and price.get('direction')=='bullish'
                and price_age is not None and price_age<=7 and evidence>=2)


def risk_state(context,confirmed=False):
    keys=[f'{s}_{h}' for s in ['SPY','QQQ'] for h in ([7,30] if confirmed else [7])]
    if not all(k in context for k in keys): return None
    return all(context[k]<0 for k in keys)


def main():
    raw=(BASE/'cohort.json').read_bytes(); prices_raw=(BASE/'market-context.json').read_bytes()
    rows=json.loads(raw)['events'];prices=json.loads(prices_raw)
    contexts={r['id']:market_features(r,prices) for r in rows}
    # Fixed hypotheses; no sweep for a maximum and no test-driven threshold tuning.
    rules={
      'original':lambda r:True,
      'current_direction_gate':lambda r:direction_under(r,'current')=='bullish',
      'symmetric_moderate_macro_gate':lambda r:direction_under(r,'symmetric_moderate')=='bullish',
      'bearish_macro_1.5x_gate':lambda r:direction_under(r,'macro_1.5x')=='bullish',
      'bearish_macro_2x_gate':lambda r:direction_under(r,'macro_2x')=='bullish',
      'fresh_bearish_macro_veto':lambda r:not macro_headwind(r),
      'fresh_macro_requires_stock_confirmation':lambda r:not macro_headwind(r) or stock_override(r),
      'weak_7d_market_requires_stock_confirmation':lambda r:risk_state(contexts[r['id']]) is not True or stock_override(r),
      'confirmed_7d_30d_market_requires_stock_confirmation':lambda r:risk_state(contexts[r['id']],True) is not True or stock_override(r),
      'macro_or_weak_market_requires_stock_confirmation':lambda r:not (macro_headwind(r) or risk_state(contexts[r['id']]) is True) or stock_override(r),
    }
    predictions={name:[-1 if r['direction']=='bearish' else 1 if gate(r) else 0 for r in rows] for name,gate in rules.items()}
    report={'cohort_sha256':hashlib.sha256(raw).hexdigest(),'price_export_sha256':hashlib.sha256(prices_raw).hexdigest(),
            'classifier_sha256':hashlib.sha256(SCORE_SOURCE.read_bytes()).hexdigest(),
            'status':'exploratory fixed rules; earlier holdout is no longer untouched; no production changes',
            'results':{},'market_context_by_event':contexts}
    for h in ['30','7']:
        ii=[i for i,r in enumerate(rows) if h in r['outcomes']]
        cohort=[rows[i] for i in ii]
        day_groups={}
        for r in cohort:
            ctx=contexts[r['id']]
            key='|'.join(ctx.get(s+'_asof','missing') for s in ['SPY','QQQ'])
            d=day_groups.setdefault(key,{'events':0,'context':ctx})
            d['events']+=1
        report['results'][h]={'rules':{},'market_days':day_groups,'risk_counts':{
            'weak':dict(Counter(str(risk_state(contexts[r['id']])) for r in cohort)),
            'confirmed':dict(Counter(str(risk_state(contexts[r['id']],True)) for r in cohort)),
            'fresh_macro_headwind':sum(macro_headwind(r) for r in cohort)}}
        for name,pred in predictions.items():
            selected=[pred[i] for i in ii]
            previous=[i for i in ii if split_for(rows[i])=='test']
            dropped=[rows[i] for i in ii if pred[i]==0]
            assert all(pred[i]==-1 for i in ii if rows[i]['direction']=='bearish')
            report['results'][h]['rules'][name]={'all':metrics(cohort,selected,h),
                'previous_test_diagnostic':metrics([rows[i] for i in previous],[pred[i] for i in previous],h),
                'dropped_events':len(dropped),'dropped_original_wins':sum(r['outcomes'][h]['correct'] for r in dropped)}
    (BASE/'macro-results.json').write_text(json.dumps(report,indent=2,allow_nan=False),encoding='utf-8')
    for h in ['30','7']:
        print('HORIZON',h,'RISK',report['results'][h]['risk_counts'])
        for name,x in report['results'][h]['rules'].items():
            a=x['all'];b=x['previous_test_diagnostic']
            print(name,'accuracy',a['accuracy'],'n',a['n'],'bullish',a['bullish'],'prior_test',b['accuracy'])


if __name__=='__main__': main()
