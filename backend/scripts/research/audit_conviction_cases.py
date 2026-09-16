"""Final local case audit; no fitting or application writes."""
import json,math
from datetime import date
from pathlib import Path
BASE=Path('frontend/test-results/confirmation-research')
ledger=json.loads((BASE/'conviction-feature-cache.json').read_text())['ledger']
trades=json.loads((BASE/'conviction-trades.json').read_text())['trades']
unusual=[];same_buyer=[]
for r in ledger:
    if r['direction']!='bullish' or '30' not in r['outcomes']:continue
    asof=date.fromisoformat(r['calculated_at'][:10])
    if not r['conviction']['conviction:insider:90:net_lower_ratio']>0:continue
    eligible=[t for t in trades if t['ticker']==r['ticker'] and t['source']=='insider' and t['side']=='buy' and 0<(asof-date.fromisoformat(t['filing'])).days<=90]
    matching=[t for t in eligible if t['unusual_ratio'] is not None and t['unusual_ratio']>=2]
    if matching:unusual.append({'ticker':r['ticker'],'entry_date':r['entry_date'],'correct_30':r['outcomes']['30']['correct'],'raw_return_30':r['outcomes']['30']['raw_return'],'triggers':[{k:t[k] for k in ['filing','transaction','low','unusual_ratio','prior_purchase_count','role']} for t in matching]})
    if any(t['ownership_increase'] is not None and t['ownership_increase']>=.1 and (t['low'] or 0)>=100000 for t in eligible):same_buyer.append(r)
result={'unusual_cases':unusual,'same_buyer_ownership_10pct_and_purchase_100k':{'n':len(same_buyer),'correct':sum(r['outcomes']['30']['correct'] for r in same_buyer),'tickers':[r['ticker'] for r in same_buyer]}}
(BASE/'conviction-case-audit.json').write_text(json.dumps(result,indent=2))
print(json.dumps({'unusual_n':len(unusual),'unusual_correct':sum(r['correct_30'] for r in unusual),'same_buyer':result['same_buyer_ownership_10pct_and_purchase_100k']},indent=2))
