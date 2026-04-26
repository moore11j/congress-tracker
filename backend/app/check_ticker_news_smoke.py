from __future__ import annotations

import argparse
import json
import sys

import requests


def main() -> int:
    parser = argparse.ArgumentParser(description="Smoke-test the local ticker news API route.")
    parser.add_argument("--base-url", default="http://127.0.0.1:8080", help="Local backend base URL.")
    parser.add_argument("--symbol", default="AAPL", help="Ticker symbol to request.")
    parser.add_argument("--limit", type=int, default=5, help="Page size to request.")
    args = parser.parse_args()

    url = f"{args.base_url.rstrip('/')}/api/tickers/{args.symbol}/news"
    params = {"limit": args.limit}
    print(f"GET {url}?limit={args.limit}")
    try:
        response = requests.get(url, params=params, timeout=15)
    except requests.RequestException as exc:
        print(f"request_failed: {exc}", file=sys.stderr)
        return 1

    print(f"status_code={response.status_code}")
    try:
        payload = response.json()
    except ValueError:
        print(response.text)
        return 1

    print(json.dumps(payload, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
