"""Explicit unpublished Finnhub rating-change preparation; no scheduled collection."""
import argparse
import json

def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--symbol',required=True)
    parser.add_argument('--from',dest='start',required=True)
    parser.add_argument('--to',dest='end',required=True)
    args=parser.parse_args(argv)
    from app.services.finnhub_research import require_paid_preparation,FinnhubUnavailable
    try:
        require_paid_preparation()
        from datetime import datetime,timezone
        from app.services.finnhub_rating_preparation import scope,prepare
        scope(args.symbol,args.start,args.end,datetime.now(timezone.utc))
        from app.db import SessionLocal
        with SessionLocal() as db:
            result=prepare(db,args.symbol,start=args.start,end=args.end);db.commit()
        print(json.dumps(result,sort_keys=True));return 0
    except FinnhubUnavailable as exc:
        print(json.dumps({'status':'unavailable','reason':str(exc)}));return 1
    except Exception:
        print(json.dumps({'status':'unavailable','reason':'rating_preparation_failed'}));return 1

if __name__=='__main__':raise SystemExit(main())
