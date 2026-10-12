"""Prepare a reviewed saved public earnings release, without provider or DB calls.

Run from backend: python -m scripts.prepare_issuer_financial_release ORIGINAL --output RECEIPT
"""
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path

from app.services.issuer_release_financials import AZO_URL, prepare_autozone_annual


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('original', type=Path)
    parser.add_argument('--source-url', default=AZO_URL)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if args.original.resolve() == args.output.resolve():
        parser.error('Output must not replace the original source')
    with args.original.open('rb') as stream:
        raw = stream.read(3_000_001)
    receipt = prepare_autozone_annual(raw, source_url=args.source_url,
                                    observed_at=datetime.now(timezone.utc))
    # Keep prior observations immutable, including accidental repeated commands.
    with args.output.open('x', encoding='utf-8') as stream:
        json.dump(receipt, stream, indent=2, sort_keys=True)
    print(json.dumps({'output':str(args.output), 'symbol':receipt['symbol'],
        'period_end':receipt['period_end'], 'source_sha256':receipt['source_sha256'],
        'values':receipt['values'], 'publication_eligible':receipt['publication_eligible']}))


if __name__ == '__main__':
    main()
