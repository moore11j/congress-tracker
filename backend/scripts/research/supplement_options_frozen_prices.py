"""Coverage-only supplement from a forward capture; never refit or overwrite prior research."""
import json
from pathlib import Path
import sys
from options_activity_expanded import ROOT as OPTIONS,load_stock_prices,PROVIDERS
from options_activity_pilot import digest
from build_options_activity_panel import label
from analyze_options_activity_expanded import outcome_stats
from forward_bullish_study import ROOT,append


def main():
    source=Path(sys.argv[1]);original=json.loads((OPTIONS/'results.json').read_text())
    if any(digest(Path(p))!=sha for p,sha in original['manifest'].items()):raise ValueError('Frozen options manifest changed')
    payload=json.loads(source.read_text());_,books=load_stock_prices();added=0
    for row in payload['prices']:
        if row[6] not in books or row[1]>'2026-09-11':continue
        if not all(isinstance(row[i],(int,float)) and row[i]>0 for i in [2,3,4]):continue
        book=books[row[6]].setdefault(row[0],{})
        if row[1] not in book:book[row[1]]=row;added+=1
    sessions=sorted({d for p in books.values() for d in p.get('SPY',{}) if d<='2026-09-11'})
    panel=json.loads((OPTIONS/'panel.json').read_text())['rows'];repaired=[]
    for row in panel:
        for h in ['30','7']:
            old=row['outcomes'][h]
            if old['status']!='missing_consistent_prices':continue
            new=label(row['ticker'],row['entry_date'],int(h),sessions,books)
            if new['status']=='measured' and new['target_date']==old['target_date']:
                repaired.append({'id':row['id'],'horizon':h,'outcome':new});row['outcomes'][h]=new
    test=[r for r in panel if r['split']=='test' and r['features'] is not None]
    frozen=json.loads((OPTIONS/'test-predictions.json').read_text())
    results={'supplement_only':True,'predictions_refitted':False,'original_results_changed':False,
             'source':str(source),'source_sha256':digest(source),'original_results_sha256':digest(OPTIONS/'results.json'),
             'predictions_sha256':digest(OPTIONS/'test-predictions.json'),'new_price_endpoints':added,
             'repaired_measurements':repaired,
             'results':{f:{p:{h:outcome_stats(test,set(ids),h) for h in ['30','7']} for p,ids in policies.items()} for f,policies in frozen['policies'].items()}}
    target=ROOT/'supplements'/('options-price-coverage-'+digest(source)[:12]+'.json');append(target,results)
    print(json.dumps({'report':str(target),'repaired':len(repaired),'new_price_endpoints':added}))


if __name__=='__main__':main()
