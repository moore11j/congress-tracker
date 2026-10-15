"""Explicit one-scope preparation. No recurring schedule or publication switch."""
import argparse
import json


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--symbol', required=True)
    parser.add_argument('--kind', choices=('eps', 'revenue', 'targets'), required=True)
    parser.add_argument('--frequency', choices=('annual', 'quarterly'))
    args = parser.parse_args(argv)
    from app.services.finnhub_research import FinnhubUnavailable, require_paid_preparation
    try:
        # Check before ORM/database imports. Running the command cannot opt in.
        require_paid_preparation()
        from app.services.finnhub_paid_preparation import prepare, scope
        scope(args.symbol, args.kind, args.frequency)
        from app.db import SessionLocal
        with SessionLocal() as db:
            result = prepare(db, args.symbol, args.kind, frequency=args.frequency)
            db.commit()
        print(json.dumps(result, sort_keys=True))
        return 0
    except FinnhubUnavailable as exc:
        # Adapter exceptions contain stable reason codes, never provider bodies.
        print(json.dumps({'status': 'unavailable', 'reason': str(exc)}))
        return 1
    except Exception:
        print(json.dumps({'status': 'unavailable', 'reason': 'preparation_failed'}))
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
