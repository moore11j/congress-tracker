"""Refresh daily strategy model charts and rotate through missing canonical prices."""
import argparse
import json
from sqlalchemy import select
from app.db import SessionLocal
from app.models import AppSetting, StrategyDefinition, StrategyVersion
from app.services.strategy_model_chart import chart_price_requirements, load_bars, refresh_chart
from app.services.price_lookup import get_expected_latest_market_date, hydrate_split_adjusted_ohlc


def refresh_model_charts(db, *, hydrate_limit=25):
    end = get_expected_latest_market_date()
    ids = db.execute(select(StrategyDefinition.id).join(StrategyVersion,
        StrategyVersion.strategy_id == StrategyDefinition.id).where(
        StrategyDefinition.status == 'published', StrategyVersion.status == 'active')).scalars().unique().all()
    needed = {}
    for sid in ids:
        for symbol, days in chart_price_requirements(db, sid, end).items():
            needed.setdefault(symbol, set()).update(days)
    bars = load_bars(db, needed)
    missing = [s for s in sorted(needed) if any((s,d.isoformat()) not in bars or any(v is None for v in bars[(s,d.isoformat())]) for d in needed[s])]
    key='strategy_model_chart_price_cursor';cursor=db.get(AppSetting,key)
    after=cursor.value if cursor else ''
    rotated=[s for s in missing if s>after]+[s for s in missing if s<=after]
    repaired=failed=0
    for symbol in rotated[:max(0,hydrate_limit)]:
        try:
            days=needed[symbol]
            repaired+=bool(hydrate_split_adjusted_ohlc(db,symbol,min(days).isoformat(),max(days).isoformat()))
        except Exception:
            db.rollback();failed+=1
        cursor=db.get(AppSetting,key)
        if cursor is None:
            cursor=AppSetting(key=key);db.add(cursor)
        cursor.value=symbol;db.commit()
    results=[]
    for sid in ids:
        try:
            p=refresh_chart(db,sid,as_of=end)
            results.append({'strategyId':sid,'status':p['status'],'through':p['through'],'missingPrices':p['missingPrices']})
        except Exception as exc:
            db.rollback();results.append({'strategyId':sid,'status':'error','error':type(exc).__name__})
    return {'expectedThrough':str(end),'missingSymbolsBefore':len(missing),'refreshedSymbols':repaired,'priceFailures':failed,'results':results}


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--hydrate-limit',type=int,default=25)
    args=parser.parse_args()
    with SessionLocal() as db:
        print(json.dumps(refresh_model_charts(db,hydrate_limit=args.hydrate_limit)),flush=True)
