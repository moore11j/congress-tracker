"""Manually collect one bounded Yahoo statement into isolated evidence storage.

Run from backend: python -m scripts.prepare_yahoo_financial_evidence SYMBOL STATEMENT
Requires YAHOO_FINANCIAL_PREPARATION_ENABLED=1 and existing staging schema.
This command does not register a schedule or enable any public data consumer.
"""
import argparse
from collections import Counter
import json

from app.services.yahoo_financial_collection import collect, YahooCollectionUnavailable


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('symbol')
    parser.add_argument('statement',choices=('income','balance','cash_flow'))
    args=parser.parse_args()
    try:
        result=collect(args.symbol,args.statement)
    except YahooCollectionUnavailable as exc:
        print(json.dumps({'status':'held','reason':str(exc),'canonical_eligible':False}))
        return 1
    print(json.dumps({'status':result['status'],'symbol':result['symbol'],'statement':result['statement'],
        'capture':result['capture'],'canonical_eligible':False,
        'observations':len(result['bundle']['observations']),
        'comparisons':dict(Counter(row['status'] for row in result['bundle']['comparisons']))}))
    return 0


if __name__=='__main__':
    raise SystemExit(main())
