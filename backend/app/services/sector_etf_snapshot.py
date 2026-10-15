"""Explicit EOD sector ETF price returns, never an all-company sector average."""
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
import math
import os
from sqlalchemy import select
from app.models import AppSetting, PriceCache

VERSION = 'sector_etf_eod_v1'
SOURCE = 'eod_etf_proxy'
TARGETS = (('Technology','XLK'),('Financials','XLF'),('Energy','XLE'),
    ('Health Care','XLV'),('Consumer Discretionary','XLY'),('Consumer Staples','XLP'),
    ('Industrials','XLI'),('Utilities','XLU'),('Materials','XLB'),
    ('Real Estate','XLRE'),('Communication Services','XLC'))
CURSOR_KEY = 'sector_etf_warming_cursor_v1'


def selected():
    return os.getenv('INSIGHTS_SECTOR_PROVIDER','fmp').strip().lower() == 'etf_proxy'


def snapshot_kind(kind):
    return f'{kind}:sectors:{VERSION}' if kind == 'macro-snapshot' and selected() else kind


def _positive(value):
    try:
        return type(value) in (int,float) and math.isfinite(value) and value > 0
    except OverflowError:
        return False


def build(db, *, now=None):
    from app.services.price_lookup import get_expected_latest_market_date, previous_market_trading_day
    end=get_expected_latest_market_date(now)
    prior=previous_market_trading_day(end)
    dates=(prior.isoformat(),end.isoformat())
    rows=db.execute(select(PriceCache).where(PriceCache.symbol.in_([symbol for _,symbol in TARGETS]),
        PriceCache.date.in_(dates))).scalars().all()
    prices={(row.symbol,row.date):row for row in rows}
    items=[]
    for sector,symbol in TARGETS:
        left,right=(prices.get((symbol,day)) for day in dates)
        item={'sector':f'{sector} ({symbol} ETF proxy)','symbol':symbol,'change_pct':None,
            'date':dates[1],'previous_date':dates[0],'unit_label':'%', 'source':SOURCE,
            'is_proxy':True,'methodology':VERSION,'return_basis':'split_adjusted_price_return',
            'timeframe_label':'Completed-session price change; excludes dividends',
            'status':'unavailable','reason':'missing_completed_sessions'}
        if left is not None and right is not None:
            if (any(row.adjustment_status != 'split_adjusted_price_return' or not _positive(row.close)
                    or row.provider_symbol != symbol or row.price_source not in {'massive','massive+corporate_actions'} for row in (left,right))
                    or left.price_source != right.price_source):
                item['reason']='unverified_or_mixed_price_basis'
            else:
                value=(right.close/left.close-1)*100
                if math.isfinite(value):
                    item.update(change_pct=value,status='ok',reason=None,price_source=right.price_source,
                        previous_close=left.close,close=right.close)
                else:
                    item['reason']='nonfinite_return'
        items.append(item)
    available=sum(item['status']=='ok' for item in items)
    coverage={'status':'ok' if available==len(TARGETS) else 'partial' if available else 'unavailable',
        'source':SOURCE,'methodology':VERSION,'expected_count':len(TARGETS),'available_count':available,
        'missing_count':len(TARGETS)-available,'date':dates[1],'previous_date':dates[0]}
    return items,coverage


def _hydrate(db, symbol, start, end):
    """Use split-adjusted Massive daily bars directly, with no FMP fallback."""
    from app.clients.massive_stocks import MassiveStocksClient, selected as massive_selected
    if not massive_selected() or symbol not in {value for _,value in TARGETS}:
        raise ValueError('sector_price_provider_not_ready')
    payload=MassiveStocksClient().get(f'/v2/aggs/ticker/{symbol}/range/1/day/{start}/{end}',
        {'adjusted':'true','sort':'asc','limit':50000})
    rows=payload.get('results')
    if (payload.get('ticker') != symbol or payload.get('adjusted') is not True
            or payload.get('next_url') or not isinstance(rows,list) or len(rows)!=2):
        raise ValueError('incomplete_or_unverified_sector_bars')
    prepared={}
    for bar in rows:
        if not isinstance(bar,dict) or not _positive(bar.get('t')):
            raise ValueError('invalid_sector_bar')
        stamp=datetime.fromtimestamp(bar['t']/1000,tz=timezone.utc).astimezone(ZoneInfo('America/New_York'))
        day=stamp.date().isoformat()
        if (day not in {start,end} or day in prepared or any((stamp.hour,stamp.minute,stamp.second,stamp.microsecond))
                or any(not _positive(bar.get(key)) for key in ('o','h','l','c'))
                or not bar['l'] <= min(bar['o'],bar['c']) <= max(bar['o'],bar['c']) <= bar['h']
                or type(bar.get('v')) not in (int,float) or not math.isfinite(bar['v']) or bar['v']<0
                or not math.isfinite(bar['v']*bar['c'])):
            raise ValueError('invalid_sector_daily_bar')
        prepared[day]=bar
    if set(prepared)!={start,end}:
        raise ValueError('missing_sector_session')
    # Validate the entire pair before touching shared canonical rows.
    for day,bar in prepared.items():
        row=db.get(PriceCache,(symbol,day))
        if row is None:
            row=PriceCache(symbol=symbol,date=day,close=bar['c'])
            db.add(row)
        row.close=row.adjusted_close=bar['c']
        row.raw_close=row.split_coefficient=row.dividend_amount=None
        row.open_price,row.high_price,row.low_price=bar['o'],bar['h'],bar['l']
        row.volume=row.day_volume=bar['v']
        row.dollar_volume=bar['v']*bar['c']
        row.price_source='massive'
        row.provider_symbol=symbol
        row.adjustment_status='split_adjusted_price_return'
    db.commit()
    return 2


def warm(db, *, now=None):
    """At most two existing price hydrations per background refresh; fair retries."""
    from app.request_priority import get_request_context
    if (not selected() or os.getenv('INSIGHTS_SECTOR_WARMING_ENABLED','false').lower() != 'true'
            or str((get_request_context() or {}).get('path') or '').startswith('/api/')):
        return {'status':'disabled','attempted':0}
    from app.clients.massive_stocks import selected as massive_selected
    if not massive_selected():
        return {'status':'awaiting_massive_selection','attempted':0}
    items,_=build(db,now=now)
    missing={item['symbol']:item for item in items if item['status']!='ok'}
    cursor=db.get(AppSetting,CURSOR_KEY)
    symbols=[symbol for _,symbol in TARGETS]
    offset=(symbols.index(cursor.value)+1)%len(symbols) if cursor and cursor.value in symbols else 0
    ordered=symbols[offset:]+symbols[:offset]
    result={'status':'ok','attempted':0,'failed':0}
    for symbol in [value for value in ordered if value in missing][:2]:
        item=missing[symbol]
        try:
            _hydrate(db,symbol,item['previous_date'],item['date'])
        except Exception:
            db.rollback()
            result['failed']+=1
        cursor=db.get(AppSetting,CURSOR_KEY)
        if cursor is None:
            cursor=AppSetting(key=CURSOR_KEY)
            db.add(cursor)
        cursor.value=symbol
        db.commit()
        result['attempted']+=1
    return result
